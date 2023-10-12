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

use bytes::{Buf, BufMut};
use foyer_common::erwlock::{ErwLock, ErwLockInner};
use parking_lot::{lock_api::ArcRwLockWriteGuard, RawRwLock, RwLockWriteGuard};
use std::{
    collections::btree_map::{BTreeMap, Entry},
    fmt::Debug,
    ops::RangeBounds,
    sync::Arc,
};
use tokio::sync::oneshot;
use tracing::instrument;

use crate::{
    device::{BufferAllocator, Device},
    error::Result,
    slice::{Slice, SliceMut},
};

pub type RegionId = u32;
/// 0 matches any version
pub type Version = u32;

#[derive(Debug)]
pub enum AllocateResult {
    Ok(WriteSlice),
    NotEnough(WriteSlice),
}

impl AllocateResult {
    pub fn unwrap(self) -> WriteSlice {
        match self {
            AllocateResult::Ok(slice) => slice,
            AllocateResult::NotEnough { .. } => unreachable!(),
        }
    }
}

pub const REGION_MAGIC: u64 = 0x19970327;

#[derive(Debug)]
pub struct RegionHeader {
    /// magic number to decide a valid region
    pub magic: u64,
}

impl RegionHeader {
    pub fn write(&self, buf: &mut [u8]) {
        (&mut buf[..]).put_u64(self.magic);
    }

    pub fn read(buf: &[u8]) -> Self {
        let magic = (&buf[..]).get_u64();
        Self { magic }
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

    #[expect(clippy::type_complexity)]
    waits: BTreeMap<(usize, usize), Vec<oneshot::Sender<Result<ReadSlice<A>>>>>,
}

#[derive(Debug, Clone)]
pub struct RegionInnerExclusiveRequire {
    can_write: bool,
    can_buffered_read: bool,
    can_physical_read: bool,
}

impl<A: BufferAllocator> ErwLockInner for RegionInner<A> {
    type R = RegionInnerExclusiveRequire;

    fn is_exclusive(&self, require: &Self::R) -> bool {
        (require.can_write || self.writers == 0)
            && (require.can_buffered_read || self.buffered_readers == 0)
            && (require.can_physical_read || self.physical_readers == 0)
    }
}

#[derive(Debug, Clone)]
pub struct Region<D>
where
    D: Device,
{
    id: RegionId,

    inner: ErwLock<RegionInner<D::IoBufferAllocator>>,

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

            waits: BTreeMap::new(),
        };
        Self {
            id,
            inner: ErwLock::new(inner),
            device,
        }
    }

    /// If there is enough buffer, return `AllocateResult::Ok(slice)``.
    /// Else, return `AllocateResult::NotEnough(slice)`. `slice` is the remaining buffer.
    #[tracing::instrument(skip(self))]
    pub fn allocate(&self, size: usize) -> AllocateResult {
        let cleanup = {
            let inner = self.inner.clone();
            let f = move || {
                let mut guard = inner.write();
                guard.writers -= 1;
            };
            Box::new(f)
        };

        let mut inner = self.inner.write();

        inner.writers += 1;
        let version = inner.version;
        let offset = inner.len;
        let region_id = self.id;

        if inner.len + size > inner.capacity {
            inner.len = self.device.region_size();

            let buffer = inner.buffer.as_mut().unwrap();
            let slice = unsafe { SliceMut::new(&mut buffer[offset..]) };

            let slice = WriteSlice {
                slice,
                region_id,
                version,
                offset,
                cleanup: Some(cleanup),
            };
            AllocateResult::NotEnough(slice)
        } else {
            inner.len += size;

            let buffer = inner.buffer.as_mut().unwrap();
            let slice = unsafe { SliceMut::new(&mut buffer[offset..offset + size]) };

            let slice = WriteSlice {
                slice,
                region_id,
                version,
                offset,
                cleanup: Some(cleanup),
            };
            AllocateResult::Ok(slice)
        }
    }

    /// Load region data into a [`ReadSlice`].
    ///
    /// Data may be loaded ether from physical device or from dirty buffer.
    ///
    /// Use version `0` to skip version check.
    ///
    /// Returns `None` if verion mismatch or given range cannot be fully filled.
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

        // case 1: read from dirty buffer

        // restrict guard lifetime
        let rx = {
            let mut inner = self.inner.write();

            if version != 0 && version != inner.version {
                return Ok(None);
            }

            // if buffer attached, buffered read

            if inner.buffer.is_some() {
                inner.buffered_readers += 1;
                let allocator = inner.buffer.as_ref().unwrap().allocator().clone();
                let slice = unsafe { Slice::new(&inner.buffer.as_ref().unwrap()[start..end]) };
                let cleanup = {
                    let inner = self.inner.clone();
                    let f = move || {
                        let mut guard = inner.write();
                        guard.buffered_readers -= 1;
                    };
                    Box::new(f)
                };
                return Ok(Some(ReadSlice::Slice {
                    slice,
                    allocator: Some(allocator),
                    cleanup: Some(cleanup),
                }));
            }

            // case 2: join wait map if exists
            let rx = match inner.waits.entry((start, end)) {
                Entry::Vacant(v) => {
                    v.insert(vec![]);
                    None
                }
                Entry::Occupied(mut o) => {
                    let (tx, rx) = oneshot::channel();
                    o.get_mut().push(tx);
                    Some(rx)
                }
            };

            inner.physical_readers += 1;
            drop(inner);

            rx
        };

        // case 3: wait for result
        if let Some(rx) = rx {
            return rx.await.map_err(anyhow::Error::from)?.map(Some);
        }

        // case 4: read from device
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
            let read = match self
                .device
                .read(s, region, (start + offset) as u64, len)
                .await
            {
                Ok(bytes) => bytes,
                Err(e) => {
                    let mut inner = self.inner.write();
                    self.cleanup(&mut inner, start, end)?;
                    inner.physical_readers -= 1;
                    return Err(e.into());
                }
            };
            if read != len {
                let mut inner = self.inner.write();
                self.cleanup(&mut inner, start, end)?;
                inner.physical_readers -= 1;
                return Ok(None);
            }
            offset += len;
        }
        let buf = Arc::new(buf);

        let cleanup = {
            let inner = self.inner.clone();
            let f = move || {
                let mut guard = inner.write();
                guard.physical_readers -= 1;
            };
            Box::new(f)
        };

        if let Some(txs) = self.inner.write().waits.remove(&(start, end)) {
            // TODO: handle error !!!!!!!!!!!
            for tx in txs {
                tx.send(Ok(ReadSlice::Shared {
                    buf: buf.clone(),
                    cleanup: Some(cleanup.clone()),
                }))
                .map_err(|_| anyhow::anyhow!("fail to send load result"))?;
            }
        }

        Ok(Some(ReadSlice::Shared {
            buf,
            cleanup: Some(cleanup),
        }))
    }

    pub async fn attach_buffer(&self, buf: Vec<u8, D::IoBufferAllocator>) {
        let mut inner = self.inner.write();

        assert_eq!(inner.writers, 0);
        assert_eq!(inner.buffered_readers, 0);

        inner.attach_buffer(buf);
        let buffer = inner.buffer.as_deref_mut().unwrap();
        let header = RegionHeader {
            magic: REGION_MAGIC,
        };
        header.write(buffer);
        inner.len = self.device.align();
    }

    pub async fn detach_buffer(&self) -> Vec<u8, D::IoBufferAllocator> {
        let mut inner = self.inner.write();

        inner.detach_buffer()
    }

    pub async fn has_buffer(&self) -> bool {
        let inner = self.inner.read();
        inner.has_buffer()
    }

    #[instrument(skip(self))]
    pub async fn exclusive(
        &self,
        can_write: bool,
        can_buffered_read: bool,
        can_physical_read: bool,
    ) -> ArcRwLockWriteGuard<RawRwLock, RegionInner<D::IoBufferAllocator>> {
        self.inner
            .exclusive(&RegionInnerExclusiveRequire {
                can_write,
                can_buffered_read,
                can_physical_read,
            })
            .await
    }

    pub fn id(&self) -> RegionId {
        self.id
    }

    pub fn device(&self) -> &D {
        &self.device
    }

    pub async fn version(&self) -> Version {
        self.inner.read().version
    }

    pub async fn advance(&self) -> Version {
        let mut inner = self.inner.write();
        let res = inner.version;
        inner.version += 1;
        res
    }

    /// Cleanup waits.
    fn cleanup(
        &self,
        guard: &mut RwLockWriteGuard<'_, RegionInner<D::IoBufferAllocator>>,
        start: usize,
        end: usize,
    ) -> Result<()> {
        if let Some(txs) = guard.waits.remove(&(start, end)) {
            guard.writers -= txs.len();
            for tx in txs {
                tx.send(Err(anyhow::anyhow!("cancelled by previous error").into()))
                    .map_err(|_| anyhow::anyhow!("fail to cleanup waits"))?;
            }
        }
        Ok(())
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
}

// read & write slice

pub trait CleanupFn = FnOnce() + Send + Sync + 'static;

pub struct WriteSlice {
    slice: SliceMut,
    region_id: RegionId,
    version: Version,
    offset: usize,
    cleanup: Option<Box<dyn CleanupFn>>,
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

impl Drop for WriteSlice {
    fn drop(&mut self) {
        if let Some(f) = self.cleanup.take() {
            f()
        }
    }
}

pub enum ReadSlice<A>
where
    A: BufferAllocator,
{
    Slice {
        slice: Slice,
        allocator: Option<A>,
        cleanup: Option<Box<dyn CleanupFn>>,
    },
    Shared {
        buf: Arc<Vec<u8, A>>,
        cleanup: Option<Box<dyn CleanupFn>>,
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
            Self::Shared { buf, .. } => f
                .debug_struct("ReadSlice::Shared")
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
            Self::Shared { buf, .. } => buf.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<A> AsRef<[u8]> for ReadSlice<A>
where
    A: BufferAllocator,
{
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Slice { slice, .. } => slice.as_ref(),
            Self::Shared { buf, .. } => buf.as_ref(),
        }
    }
}

impl<A> Drop for ReadSlice<A>
where
    A: BufferAllocator,
{
    fn drop(&mut self) {
        if let Some(f) = match self {
            ReadSlice::Slice { cleanup, .. } => cleanup.take(),
            ReadSlice::Shared { cleanup, .. } => cleanup.take(),
        } {
            f();
        }
    }
}
