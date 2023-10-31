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
use parking_lot::{Mutex, MutexGuard};
use std::{
    collections::btree_map::{BTreeMap, Entry},
    fmt::Debug,
    ops::RangeBounds,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::sync::oneshot;

use crate::{
    device::{BufferAllocator, Device},
    error::Result,
    slice::SliceMut,
};

pub type RegionId = u32;

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
    #[expect(clippy::type_complexity)]
    waits: BTreeMap<(usize, usize), Vec<oneshot::Sender<Result<Arc<Vec<u8, A>>>>>>,
}

#[derive(Debug, Clone)]
pub struct Region<D>
where
    D: Device,
{
    id: RegionId,

    inner: Arc<Mutex<RegionInner<D::IoBufferAllocator>>>,

    device: D,

    refs: Arc<AtomicUsize>,
}

/// [`Region`] represents a contiguous aligned range on device and its optional dirty buffer.
///
/// [`Region`] may be in one of the following states:
///
/// - physical write : written by flushers with append pattern
/// - physical read  : read if entry is not in ring buffer
/// - reclaim        : happens after the region is evicted, must guarantee there is no writers or readers,
///                    *or in-flight writers or readers*
impl<D> Region<D>
where
    D: Device,
{
    pub fn new(id: RegionId, device: D) -> Self {
        let inner = RegionInner {
            waits: BTreeMap::new(),
        };
        Self {
            id,
            inner: Arc::new(Mutex::new(inner)),
            device,
            refs: Arc::new(AtomicUsize::default()),
        }
    }

    pub fn view(&self, offset: u32, len: u32, key_len: u32, value_len: u32) -> RegionView {
        self.refs.fetch_add(1, Ordering::SeqCst);
        RegionView {
            id: self.id,
            offset,
            len,
            key_len,
            value_len,
            refs: Arc::clone(&self.refs),
        }
    }

    pub fn refs(&self) -> &Arc<AtomicUsize> {
        &self.refs
    }

    /// Load region data by view from device.
    #[expect(clippy::type_complexity)]
    #[tracing::instrument(skip(self, view))]
    pub async fn load(
        &self,
        view: RegionView,
    ) -> Result<Option<Arc<Vec<u8, D::IoBufferAllocator>>>> {
        let res = self
            .load_range(view.offset as usize..view.offset as usize + view.len as usize)
            .await;
        // drop view after load finish
        drop(view);
        res
    }

    /// Load region data with given `range` from device.
    #[expect(clippy::type_complexity)]
    #[tracing::instrument(skip(self, range), fields(start, end))]
    pub async fn load_range(
        &self,
        range: impl RangeBounds<usize>,
    ) -> Result<Option<Arc<Vec<u8, D::IoBufferAllocator>>>> {
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

        let rx = {
            let mut inner = self.inner.lock();

            // join wait map if exists
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

            drop(inner);

            rx
        };

        // wait for result if joined into wait map
        if let Some(rx) = rx {
            return rx.await.map_err(anyhow::Error::from)?.map(Some);
        }

        // otherwise, read from device
        let region = self.id;
        let mut buf = self.device.io_buffer(end - start, end - start);

        let mut offset = 0;
        while start + offset < end {
            let len = std::cmp::min(self.device.io_size(), end - start - offset);
            tracing::trace!(
                "read region {} [{}..{}]",
                region,
                start + offset,
                start + offset + len
            );
            let s = unsafe { SliceMut::new(&mut buf[offset..offset + len]) };
            let (res, _s) = self
                .device
                .read(s, .., region, (start + offset) as u64)
                .await;
            let read = match res {
                Ok(bytes) => bytes,
                Err(e) => {
                    let mut inner = self.inner.lock();
                    self.cleanup(&mut inner, start, end)?;
                    return Err(e.into());
                }
            };
            if read != len {
                let mut inner = self.inner.lock();
                self.cleanup(&mut inner, start, end)?;
                // TODO(MrCroxx): return err?
                return Ok(None);
            }
            offset += len;
        }
        let buf = Arc::new(buf);

        if let Some(txs) = self.inner.lock().waits.remove(&(start, end)) {
            // TODO: handle error !!!!!!!!!!!
            for tx in txs {
                tx.send(Ok(buf.clone()))
                    .map_err(|_| anyhow::anyhow!("fail to send load result"))?;
            }
        }

        Ok(Some(buf))
    }

    pub fn id(&self) -> RegionId {
        self.id
    }

    pub fn device(&self) -> &D {
        &self.device
    }

    /// Cleanup waits.
    fn cleanup(
        &self,
        guard: &mut MutexGuard<'_, RegionInner<D::IoBufferAllocator>>,
        start: usize,
        end: usize,
    ) -> Result<()> {
        if let Some(txs) = guard.waits.remove(&(start, end)) {
            for tx in txs {
                tx.send(Err(anyhow::anyhow!("cancelled by previous error").into()))
                    .map_err(|_| anyhow::anyhow!("fail to cleanup waits"))?;
            }
        }
        Ok(())
    }
}

// read & write slice

pub trait CleanupFn = FnOnce() + Send + Sync + 'static;

#[derive(Debug)]
pub struct RegionView {
    id: RegionId,
    offset: u32,
    len: u32,
    key_len: u32,
    value_len: u32,
    refs: Arc<AtomicUsize>,
}

impl Clone for RegionView {
    fn clone(&self) -> Self {
        self.refs.fetch_add(1, Ordering::SeqCst);
        Self {
            id: self.id,
            offset: self.offset,
            len: self.len,
            key_len: self.key_len,
            value_len: self.value_len,
            refs: Arc::clone(&self.refs),
        }
    }
}

impl Drop for RegionView {
    fn drop(&mut self) {
        self.refs.fetch_sub(1, Ordering::SeqCst);
    }
}

impl RegionView {
    pub fn id(&self) -> &RegionId {
        &self.id
    }

    pub fn offset(&self) -> &u32 {
        &self.offset
    }

    pub fn len(&self) -> &u32 {
        &self.len
    }

    pub fn key_len(&self) -> &u32 {
        &self.key_len
    }

    pub fn value_len(&self) -> &u32 {
        &self.value_len
    }

    pub fn refs(&self) -> &Arc<AtomicUsize> {
        &self.refs
    }
}
