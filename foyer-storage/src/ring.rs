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

use crate::{catalog::Sequence, device::BufferAllocator};
use foyer_common::{bits::align_up, continuum::ContinuumUsize};
use itertools::Itertools;
use std::{
    alloc::Global,
    fmt::Debug,
    ops::{Deref, DerefMut, Range},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

pub struct RingBuffer<A = Global>
where
    A: BufferAllocator,
{
    data: Vec<u8, A>,
    align: usize,
    capacity: usize,
    blocks: usize,

    allocated: AtomicUsize,

    refs: Vec<Arc<AtomicUsize>>,

    continuum: Arc<ContinuumUsize>,
}

impl<A> Debug for RingBuffer<A>
where
    A: BufferAllocator,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RingBuffer")
            .field("align", &self.align)
            .field("capacity", &self.capacity)
            .field("blocks", &self.blocks)
            .field("allocated", &self.allocated)
            .finish()
    }
}

impl RingBuffer<Global> {
    /// `align` must be power of 2.
    pub fn new(align: usize, capacity: usize) -> Self {
        Self::new_in(align, capacity, Global)
    }
}

impl<A> RingBuffer<A>
where
    A: BufferAllocator,
{
    /// `align` must be power of 2.
    pub fn new_in(align: usize, capacity: usize, alloc: A) -> Self {
        assert!(align.is_power_of_two());
        let capacity = align_up(align, capacity);
        let blocks = capacity / align;

        let mut data = Vec::with_capacity_in(capacity, alloc);
        unsafe { data.set_len(capacity) };

        let allocated = AtomicUsize::new(0);

        let continuum = Arc::new(ContinuumUsize::new(blocks));
        let refs = (0..blocks)
            .map(|_| Arc::new(AtomicUsize::default()))
            .collect_vec();

        Self {
            data,
            align,
            capacity,
            blocks,
            allocated,
            refs,
            continuum,
        }
    }

    /// Allocate a mutable view with required size on the ring buffer.
    ///
    /// Returns `None` when the allocated buffer cross the boundary.
    ///
    /// When all views from an allocation are dropped, the buffer will be released.
    pub async fn allocate(self: &Arc<Self>, len: usize, sequence: Sequence) -> ViewMut {
        loop {
            if let Some(view) = self.allocate_inner(len, sequence).await {
                return view;
            }
        }
    }

    async fn allocate_inner(self: &Arc<Self>, len: usize, sequence: Sequence) -> Option<ViewMut> {
        let len = align_up(self.align, len);
        let offset = self.allocated.fetch_add(len, Ordering::SeqCst);

        debug_assert_eq!(offset & (self.align - 1), 0);

        loop {
            self.continuum.advance();
            if self.continuum.continuum() * self.align + self.capacity >= offset + len {
                break;
            }
            tokio::time::sleep(Duration::from_micros(100)).await;
        }

        if offset / self.capacity != (offset + len) / self.capacity {
            debug_assert!(self.continuum.is_vacant(offset / self.align));

            self.continuum
                .submit((offset / self.align)..((offset + len) / self.align));
            return None;
        }

        let refs = self.refs(sequence);

        let ring = Arc::clone(self);
        let ring: Arc<dyn Ring> = ring;
        Some(ViewMut::new(ring, offset, len, refs))
    }

    fn refs(&self, sequence: Sequence) -> &Arc<AtomicUsize> {
        &self.refs[sequence as usize % self.blocks]
    }

    pub fn continuum(&self) -> usize {
        self.continuum.continuum() * self.align
    }

    pub fn advance(&self) {
        self.continuum.advance();
    }
}

pub trait Ring: Send + Sync + 'static + Debug {
    fn align(&self) -> usize;

    fn ptr(&self, offset: usize) -> *mut u8;

    fn release(&self, range: Range<usize>);
}

impl<A> Ring for RingBuffer<A>
where
    A: BufferAllocator,
{
    fn align(&self) -> usize {
        self.align
    }

    fn ptr(&self, offset: usize) -> *mut u8 {
        (self.data.as_ptr() as usize + (offset % self.capacity)) as *mut u8
    }

    fn release(&self, range: Range<usize>) {
        let start = range.start / self.align;
        let end = range.end / self.align;
        debug_assert!(self.continuum.is_vacant(start));
        self.continuum.submit(start..end);
    }
}

/// # Safety
///
/// The underlying buffer of [`ViewMut`] must be valid during its lifetime.
#[derive(Debug)]
pub struct ViewMut {
    ring: Arc<dyn Ring>,
    ptr: *mut u8,
    offset: usize,
    len: usize,
    capacity: usize,
    refs: Arc<AtomicUsize>,
}

impl ViewMut {
    fn new(ring: Arc<dyn Ring>, offset: usize, len: usize, refs: &Arc<AtomicUsize>) -> Self {
        refs.fetch_add(1, Ordering::AcqRel);
        let ptr = ring.ptr(offset);
        Self {
            ring,
            ptr,
            offset,
            len,
            capacity: len,
            refs: Arc::clone(refs),
        }
    }

    /// Shrink the accessable area of [`ViewMut`].
    ///
    /// `shrink` will not release the underlying buffer size.
    pub fn shrink_to(&mut self, len: usize) {
        debug_assert!(len <= self.capacity);
        self.len = len;
    }

    pub fn aligned(&self) -> usize {
        align_up(self.ring.align(), self.len)
    }

    pub fn freeze(self) -> View {
        View::from(self)
    }
}

impl Deref for ViewMut {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { core::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl DerefMut for ViewMut {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { core::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

impl Drop for ViewMut {
    fn drop(&mut self) {
        if self.refs.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.ring.release(self.offset..self.offset + self.capacity);
        }
    }
}

unsafe impl Send for ViewMut {}
unsafe impl Sync for ViewMut {}

/// # Safety
///
/// The underlying buffer of [`View`] must be valid during its lifetime.
#[derive(Debug)]
pub struct View {
    view: ViewMut,
}

impl From<ViewMut> for View {
    fn from(view: ViewMut) -> Self {
        Self { view }
    }
}

impl Deref for View {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.view.deref()
    }
}

impl Clone for View {
    fn clone(&self) -> Self {
        self.view.refs.fetch_add(1, Ordering::AcqRel);
        let view = ViewMut {
            ring: self.view.ring.clone(),
            ptr: self.view.ptr,
            offset: self.view.offset,
            len: self.view.len,
            capacity: self.view.capacity,
            refs: self.view.refs.clone(),
        };
        Self { view }
    }
}

impl View {
    pub fn aligned(&self) -> usize {
        self.view.aligned()
    }
}

unsafe impl Send for View {}
unsafe impl Sync for View {}

#[cfg(test)]
mod tests {

    use std::{
        collections::BTreeMap,
        future::{poll_fn, Future},
        ops::Range,
        pin::pin,
        sync::atomic::AtomicU64,
        task::{Poll, Poll::Pending},
        thread::available_parallelism,
        time::Instant,
    };

    use bytes::BufMut;
    use rand::{rngs::OsRng, Rng};

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_ring() {
        const ALIGN: usize = 4096; // 4 KiB
        const CAPACITY: usize = 16 * 1024 * 1024; // 16 MiB

        let ring = Arc::new(RingBuffer::new(ALIGN, CAPACITY));
        let sequence = Arc::new(AtomicU64::default());

        let mut views = BTreeMap::new();
        for i in 0..15 {
            let seq = sequence.fetch_add(1, Ordering::Relaxed);
            let view = ring
                .allocate_inner(1024 * 1024 - 3000 /* ~ 1 MiB */, seq)
                .await
                .unwrap();
            assert_eq!(view.offset, i * 1024 * 1024);
            assert_eq!(view.capacity, 1024 * 1024);
            views.insert(seq, view);
        }
        let seq = sequence.fetch_add(1, Ordering::Relaxed);

        let mut future = pin!(ring.allocate_inner(2 * 1024 * 1024 - 3000 /* ~ 2 MiB */, seq));
        assert!(matches! { poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await, Pending });
        views.remove(&0).unwrap();
        let res = future.await;
        assert!(res.is_none());

        let mut future = pin!(ring.allocate_inner(2 * 1024 * 1024 - 3000 /* ~ 2 MiB */, seq));
        assert!(matches! { poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await, Pending });
        views.remove(&2).unwrap();
        views.remove(&1).unwrap();
        let view = future.await.unwrap();
        assert_eq!(view.offset, 17 * 1024 * 1024);
        assert_eq!(view.capacity, 2 * 1024 * 1024);
        views.insert(seq, view);

        drop(views);
    }

    async fn test_ring_concurrent_case(capacity: usize, concurrency: usize, loops: usize) {
        const ALIGN: usize = 4096; // 4 KiB
        const SIZE: Range<usize> = 16 * 1024..256 * 1024; // 16 KiB ~ 128 KiB

        let ring = Arc::new(RingBuffer::new(ALIGN, capacity));
        let sequence = Arc::new(AtomicU64::default());

        let tasks = (0..concurrency)
            .map(|_| {
                let ring = ring.clone();
                let sequence = sequence.clone();
                async move {
                    for i in 0..loops {
                        let seq = sequence.fetch_add(1, Ordering::Relaxed);
                        let size = OsRng.gen_range(SIZE);
                        let mut view = ring.allocate(size, seq).await;
                        tokio::time::sleep(Duration::from_millis(OsRng.gen_range(0..10))).await;
                        let data = vec![i as u8; size];
                        view.as_mut().put_slice(&data);
                        let view = view.freeze();
                        assert!(view[..size] == data);
                        drop(view);
                    }
                }
            })
            .collect_vec();
        let handles = tasks.into_iter().map(tokio::spawn).collect_vec();
        for handle in handles {
            handle.await.unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_ring_concurrent_small() {
        test_ring_concurrent_case(16 * 1024 * 1024, 16, 100).await;
    }

    #[ignore]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_ring_concurrent_large() {
        let concurrency = available_parallelism().unwrap().get() * 64;

        let now = Instant::now();
        test_ring_concurrent_case(
            128 * 1024 * 1024,
            available_parallelism().unwrap().get() * 64,
            1000,
        )
        .await;
        let elapsed = now.elapsed();

        println!("========== ring current fuzzy begin ==========");
        println!("concurrency: {concurrency}");
        println!("elapsed: {elapsed:?}");
        println!("=========== ring current fuzzy end ===========");
    }
}
