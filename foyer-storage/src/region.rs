//  Copyright 2023 MrCroxx
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::{collections::HashMap, fmt::Debug, ops::RangeBounds, pin::Pin, sync::Arc, task::Waker};

use futures::future::BoxFuture;
use tokio::sync::{OwnedRwLockWriteGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use tracing::instrument;

use crate::{
    device::{BufferAllocator, Device},
    error::Result,
    slice::{Slice, SliceMut},
};

pub type RegionId = u32;
/// 0 matches any version
pub type Version = u32;

pub enum AllocateResult {
    Ok(WriteSlice),
    Full { slice: WriteSlice, remain: usize },
    None,
}

impl AllocateResult {
    pub fn unwrap(self) -> WriteSlice {
        match self {
            AllocateResult::Ok(slice) => slice,
            AllocateResult::Full { .. } => unreachable!(),
            AllocateResult::None => unreachable!(),
        }
    }
}

#[derive(Debug)]
pub struct RegionInner<A>
where
    A: BufferAllocator,
{
    version: Version,

    buffer: Option<Vec<u8, A>>,
    len: usize,
    capacity: usize,

    writers: usize,
    buffered_readers: usize,
    physical_readers: usize,

    wakers: HashMap<usize, Waker>,
}

#[derive(Debug, Clone)]
pub struct Region<D>
where
    D: Device,
{
    id: RegionId,

    inner: ErwLock<D::IoBufferAllocator>,

    device: D,
}

/// [`Region`] represents a contiguous aligned range on device and its optional dirty buffer.
///
/// [`Region`] may be in one of the following states:
///
/// - buffered write : append-only buffer write, written parts can be read concurrently.
/// - buffered read  : happenes if the region is dirty with an attached dirty buffer
/// - physical read  : happenes if the region is clean, read directly from the devie
/// - flush          : happenes after the region dirty buffer is full, there are 2 steps when flushing
///                    step 1 writes dirty buffer to device, must guarantee there is no writers or physical readers
///                    step 2 detaches dirty buffer, must guarantee there is no buffer readers
/// - reclaim        : happens after the region is evicted, must guarantee there is no writers, buffer readers or physical readers,
///                    *or in-flight writers or readers* (verify by version)
impl<D> Region<D>
where
    D: Device,
{
    pub fn new(id: RegionId, device: D) -> Self {
        let inner = RegionInner {
            version: 0,

            buffer: None,
            len: 0,
            capacity: device.region_size(),

            writers: 0,
            buffered_readers: 0,
            physical_readers: 0,

            wakers: HashMap::default(),
        };
        Self {
            id,
            inner: ErwLock::new(inner),
            device,
        }
    }

    pub async fn allocate(&self, size: usize) -> AllocateResult {
        let future = {
            let inner = self.inner.clone();
            async move {
                let mut guard = inner.write().await;
                guard.writers -= 1;
                guard.wake_all();
            }
        };

        let mut inner = self.inner.write().await;

        inner.writers += 1;
        let version = inner.version;
        let offset = inner.len;
        let region_id = self.id;

        // reserve 1 align size for region footer
        if inner.len + size + self.device.align() > inner.capacity {
            // if full, return the reserved 1 aligen write buf
            let remain = self.device.region_size() - inner.len;
            inner.len = self.device.region_size();
            let range = inner.len - self.device.align()..inner.len;

            let buffer = inner.buffer.as_mut().unwrap();
            let slice = unsafe { SliceMut::new(&mut buffer[range]) };

            let slice = WriteSlice {
                slice,
                region_id,
                version,
                offset,
                future: Some(Box::pin(future)),
            };
            AllocateResult::Full { slice, remain }
        } else {
            inner.len += size;

            let buffer = inner.buffer.as_mut().unwrap();
            let slice = unsafe { SliceMut::new(&mut buffer[offset..offset + size]) };

            let slice = WriteSlice {
                slice,
                region_id,
                version,
                offset,
                future: Some(Box::pin(future)),
            };
            AllocateResult::Ok(slice)
        }
    }

    #[tracing::instrument(skip(self, range), fields(start, end))]
    pub async fn load(
        &self,
        range: impl RangeBounds<usize>,
        version: Version,
    ) -> Result<Option<ReadSlice<D::IoBufferAllocator>>> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(i) => *i,
            std::ops::Bound::Excluded(i) => *i + 1,
            std::ops::Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            std::ops::Bound::Included(i) => *i + 1,
            std::ops::Bound::Excluded(i) => *i,
            std::ops::Bound::Unbounded => self.device.region_size(),
        };

        // restrict guard lifetime
        {
            let mut inner = self.inner.write().await;

            if version != 0 && version != inner.version {
                return Ok(None);
            }

            // if buffer attached, buffered read

            if inner.buffer.is_some() {
                inner.buffered_readers += 1;
                let allocator = inner.buffer.as_ref().unwrap().allocator().clone();
                let slice = unsafe { Slice::new(&inner.buffer.as_ref().unwrap()[start..end]) };
                let future = {
                    let inner = self.inner.clone();
                    async move {
                        let mut guard = inner.write().await;
                        guard.buffered_readers -= 1;
                        guard.wake_all();
                    }
                };
                return Ok(Some(ReadSlice::Slice {
                    slice,
                    allocator: Some(allocator),
                    future: Some(Box::pin(future)),
                }));
            }

            // if buffer detached, physical read
            inner.physical_readers += 1;
            drop(inner);
        }

        let region = self.id;
        let mut buf = self.device.io_buffer(end - start, end - start);

        let mut offset = 0;
        while start + offset < end {
            let len = std::cmp::min(self.device.io_size(), end - start - offset);
            tracing::trace!(
                "physical read region {} [{}..{}]",
                region,
                start + offset,
                start + offset + len
            );
            let s = unsafe { SliceMut::new(&mut buf[offset..offset + len]) };
            if self
                .device
                .read(s, region, (start + offset) as u64, len)
                .await?
                != len
            {
                let mut inner = self.inner.write().await;
                inner.physical_readers -= 1;
                inner.wake_all();
                return Ok(None);
            }
            offset += len;
        }

        let future = {
            let inner = self.inner.clone();
            async move {
                let mut guard = inner.write().await;
                guard.physical_readers -= 1;
                guard.wake_all();
            }
        };
        Ok(Some(ReadSlice::Owned {
            buf: Some(buf),
            future: Some(Box::pin(future)),
        }))
    }

    pub async fn attach_buffer(&self, buf: Vec<u8, D::IoBufferAllocator>) {
        let mut inner = self.inner.write().await;

        assert_eq!(inner.writers, 0);
        assert_eq!(inner.buffered_readers, 0);

        inner.attach_buffer(buf);
    }

    pub async fn detach_buffer(&self) -> Vec<u8, D::IoBufferAllocator> {
        let mut inner = self.inner.write().await;

        inner.detach_buffer()
    }

    pub async fn has_buffer(&self) -> bool {
        let inner = self.inner.read().await;
        inner.has_buffer()
    }

    #[instrument(skip(self))]
    pub async fn exclusive(
        &self,
        can_write: bool,
        can_buffered_read: bool,
        can_physical_read: bool,
    ) -> OwnedRwLockWriteGuard<RegionInner<D::IoBufferAllocator>> {
        self.inner
            .exclusive(can_write, can_buffered_read, can_physical_read)
            .await
    }

    pub fn id(&self) -> RegionId {
        self.id
    }

    pub fn device(&self) -> &D {
        &self.device
    }

    pub async fn version(&self) -> Version {
        self.inner.read().await.version
    }

    pub async fn advance(&self) -> Version {
        let mut inner = self.inner.write().await;
        let res = inner.version;
        inner.version += 1;
        res
    }
}

impl<A> RegionInner<A>
where
    A: BufferAllocator,
{
    pub fn attach_buffer(&mut self, buf: Vec<u8, A>) {
        assert!(self.buffer.is_none());
        assert_eq!(buf.len(), buf.capacity());
        assert_eq!(buf.capacity(), self.capacity);
        self.buffer = Some(buf);
        self.len = 0;
    }

    pub fn detach_buffer(&mut self) -> Vec<u8, A> {
        self.buffer.take().unwrap()
    }

    pub fn has_buffer(&self) -> bool {
        self.buffer.is_some()
    }

    pub fn writers(&self) -> usize {
        self.writers
    }

    pub fn buffered_readers(&self) -> usize {
        self.buffered_readers
    }

    pub fn physical_readers(&self) -> usize {
        self.physical_readers
    }

    fn wake_all(&self) {
        for waker in self.wakers.values() {
            waker.wake_by_ref();
        }
    }
}

// read & write slice

#[pin_project::pin_project(project = WriteSliceProj, PinnedDrop)]
pub struct WriteSlice {
    slice: SliceMut,
    region_id: RegionId,
    version: Version,
    offset: usize,
    #[pin]
    future: Option<BoxFuture<'static, ()>>,
}

impl Debug for WriteSlice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteSlice")
            .field("slice", &self.slice)
            .field("region_id", &self.region_id)
            .field("version", &self.version)
            .field("offset", &self.offset)
            .finish()
    }
}

impl WriteSlice {
    pub fn region_id(&self) -> RegionId {
        self.region_id
    }

    pub fn version(&self) -> Version {
        self.version
    }

    pub fn offset(&self) -> usize {
        self.offset
    }

    pub fn len(&self) -> usize {
        self.slice.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// # Safety
    ///
    /// `destroy` MUST be called before actually drop if `future` is set.
    pub async fn destroy(mut self) {
        if let Some(future) = self.future.take() {
            future.await;
        }
    }
}

impl AsRef<[u8]> for WriteSlice {
    fn as_ref(&self) -> &[u8] {
        self.slice.as_ref()
    }
}

impl AsMut<[u8]> for WriteSlice {
    fn as_mut(&mut self) -> &mut [u8] {
        self.slice.as_mut()
    }
}

#[pin_project::pinned_drop]
impl PinnedDrop for WriteSlice {
    fn drop(self: Pin<&mut Self>) {
        let mut this = self.project();
        if let Some(future) = this.future.take() {
            tracing::error!("future is not consumed. This may be caused by error early return. If there's not, check if there's slice not destroyed. {:?}", this);
            tokio::spawn(future);
        }
    }
}

impl<'pin> Debug for WriteSliceProj<'pin> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteSlice")
            .field("slice", &self.slice)
            .field("region_id", &self.region_id)
            .field("version", &self.version)
            .field("offset", &self.offset)
            .finish()
    }
}

#[pin_project::pin_project(project = ReadSliceProj, PinnedDrop)]
pub enum ReadSlice<A>
where
    A: BufferAllocator,
{
    Slice {
        slice: Slice,
        allocator: Option<A>,
        #[pin]
        future: Option<BoxFuture<'static, ()>>,
    },
    Owned {
        buf: Option<Vec<u8, A>>,
        #[pin]
        future: Option<BoxFuture<'static, ()>>,
    },
}

impl<A> Debug for ReadSlice<A>
where
    A: BufferAllocator,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Slice {
                slice, allocator, ..
            } => f
                .debug_struct("ReadSlice::Slice")
                .field("slice", slice)
                .field("allocator", allocator)
                .finish(),
            Self::Owned { buf, .. } => f
                .debug_struct("ReadSlice::Owned")
                .field("buf", buf)
                .finish(),
        }
    }
}

impl<A> ReadSlice<A>
where
    A: BufferAllocator,
{
    pub fn len(&self) -> usize {
        match self {
            Self::Slice { slice, .. } => slice.len(),
            Self::Owned { buf, .. } => buf.as_ref().unwrap().len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// # Safety
    ///
    /// `destroy` MUST be called before actually drop if `future` is set.
    pub async fn destroy(mut self) {
        if let Some(future) = match &mut self {
            ReadSlice::Slice { future, .. } => future.take(),
            ReadSlice::Owned { future, .. } => future.take(),
        } {
            future.await;
        }
    }
}

impl<A> AsRef<[u8]> for ReadSlice<A>
where
    A: BufferAllocator,
{
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Slice { slice, .. } => slice.as_ref(),
            Self::Owned { buf, .. } => buf.as_ref().unwrap(),
        }
    }
}

#[pin_project::pinned_drop]
impl<A> PinnedDrop for ReadSlice<A>
where
    A: BufferAllocator,
{
    fn drop(self: Pin<&mut Self>) {
        let mut this = self.project();
        if let Some(future) = match &mut this {
            ReadSliceProj::Slice { future, .. } => future.take(),
            ReadSliceProj::Owned { future, .. } => future.take(),
        } {
            tracing::error!("future is not consumed. This may be caused by error early return. If there's not, check if there's slice not destroyed. {:?}", this);
            tokio::spawn(future);
        }
    }
}

impl<'pin, A: BufferAllocator> Debug for ReadSliceProj<'pin, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Slice {
                slice, allocator, ..
            } => f
                .debug_struct("Slice")
                .field("slice", slice)
                .field("allocator", allocator)
                .finish(),
            Self::Owned { buf, .. } => f.debug_struct("Owned").field("buf", buf).finish(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ErwLock<A: BufferAllocator> {
    inner: Arc<RwLock<RegionInner<A>>>,
}

impl<A: BufferAllocator> ErwLock<A> {
    pub fn new(inner: RegionInner<A>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    pub async fn read(&self) -> RwLockReadGuard<'_, RegionInner<A>> {
        self.inner.read().await
    }

    pub async fn write(&self) -> RwLockWriteGuard<'_, RegionInner<A>> {
        self.inner.write().await
    }

    pub async fn exclusive(
        &self,
        can_write: bool,
        can_buffered_read: bool,
        can_physical_read: bool,
    ) -> OwnedRwLockWriteGuard<RegionInner<A>> {
        loop {
            {
                let guard = self.inner.clone().write_owned().await;
                let is_ready = (can_write || guard.writers == 0)
                    && (can_buffered_read || guard.buffered_readers == 0)
                    && (can_physical_read || guard.physical_readers == 0);
                if is_ready {
                    return guard;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }
}
