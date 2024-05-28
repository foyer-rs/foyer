//  Copyright 2024 Foyer Project Authors
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

use std::{
    collections::{HashSet, VecDeque},
    fmt::Debug,
    ptr::NonNull,
};

use foyer_common::{assert::OptionExt, strict_assert, strict_assert_eq};
use foyer_intrusive::{
    dlist::{Dlist, DlistLink},
    intrusive_adapter,
};
use serde::{Deserialize, Serialize};

use crate::{
    eviction::Eviction,
    handle::{BaseHandle, Handle},
    CacheContext,
};

const MAX_FREQ: u8 = 3;

#[derive(Debug, Clone)]
pub struct S3FifoContext(CacheContext);

impl From<CacheContext> for S3FifoContext {
    fn from(context: CacheContext) -> Self {
        Self(context)
    }
}

impl From<S3FifoContext> for CacheContext {
    fn from(context: S3FifoContext) -> Self {
        context.0
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Queue {
    None,
    Main,
    Small,
}

pub struct S3FifoHandle<T>
where
    T: Send + Sync + 'static,
{
    link: DlistLink,
    base: BaseHandle<T, S3FifoContext>,
    freq: u8,
    queue: Queue,
}

impl<T> Debug for S3FifoHandle<T>
where
    T: Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3FifoHandle").finish()
    }
}

intrusive_adapter! { S3FifoHandleDlistAdapter<T> = NonNull<S3FifoHandle<T>>: S3FifoHandle<T> { link: DlistLink } where T: Send + Sync + 'static }

impl<T> S3FifoHandle<T>
where
    T: Send + Sync + 'static,
{
    #[inline(always)]
    pub fn freq_inc(&mut self) {
        self.freq = std::cmp::min(self.freq + 1, MAX_FREQ);
    }

    #[inline(always)]
    pub fn freq_dec(&mut self) {
        self.freq = self.freq.saturating_sub(1);
    }
}

impl<T> Default for S3FifoHandle<T>
where
    T: Send + Sync + 'static,
{
    fn default() -> Self {
        Self {
            link: DlistLink::default(),
            freq: 0,
            base: BaseHandle::new(),
            queue: Queue::None,
        }
    }
}

impl<T> Handle for S3FifoHandle<T>
where
    T: Send + Sync + 'static,
{
    type Data = T;
    type Context = S3FifoContext;

    fn base(&self) -> &BaseHandle<Self::Data, Self::Context> {
        &self.base
    }

    fn base_mut(&mut self) -> &mut BaseHandle<Self::Data, Self::Context> {
        &mut self.base
    }
}

/// S3FIFO eviction algorithm config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3FifoConfig {
    /// Capacity ratio of the small S3FIFO queue.
    pub small_queue_capacity_ratio: f64,
    /// Capacity ratio of the ghost S3FIFO queue.
    pub ghost_queue_capacity_ratio: f64,
    /// Minimum access times when population entry from small queue to main queue.
    pub small_to_main_freq_threshold: u8,
}

impl Default for S3FifoConfig {
    fn default() -> Self {
        Self {
            small_queue_capacity_ratio: 0.1,
            ghost_queue_capacity_ratio: 1.0,
            small_to_main_freq_threshold: 1,
        }
    }
}

pub struct S3Fifo<T>
where
    T: Send + Sync + 'static,
{
    ghost_queue: GhostQueue,
    small_queue: Dlist<S3FifoHandleDlistAdapter<T>>,
    main_queue: Dlist<S3FifoHandleDlistAdapter<T>>,

    small_weight_capacity: usize,

    small_weight: usize,
    main_weight: usize,

    small_to_main_freq_threshold: u8,
}

impl<T> S3Fifo<T>
where
    T: Send + Sync + 'static,
{
    unsafe fn evict(&mut self) -> Option<NonNull<S3FifoHandle<T>>> {
        // TODO(MrCroxx): Use `let_chains` here after it is stable.
        if self.small_weight > self.small_weight_capacity {
            if let Some(ptr) = self.evict_small() {
                return Some(ptr);
            }
        }
        if let Some(ptr) = self.evict_main() {
            return Some(ptr);
        }
        self.evict_small_force()
    }

    // TODO(MrCroxx): FIX ME!!! clippy false positive
    #[allow(clippy::never_loop)]
    unsafe fn evict_small_force(&mut self) -> Option<NonNull<S3FifoHandle<T>>> {
        while let Some(mut ptr) = self.small_queue.pop_front() {
            let handle = ptr.as_mut();
            handle.queue = Queue::None;
            handle.freq = 0;
            self.small_weight -= handle.base().weight();
            return Some(ptr);
        }
        None
    }

    unsafe fn evict_small(&mut self) -> Option<NonNull<S3FifoHandle<T>>> {
        while let Some(mut ptr) = self.small_queue.pop_front() {
            let handle = ptr.as_mut();
            if handle.freq >= self.small_to_main_freq_threshold {
                handle.queue = Queue::Main;
                self.main_queue.push_back(ptr);
                self.small_weight -= handle.base().weight();
                self.main_weight += handle.base().weight();
            } else {
                handle.queue = Queue::None;
                handle.freq = 0;
                self.small_weight -= handle.base().weight();

                self.ghost_queue.push(handle.base().hash(), handle.base().weight());

                return Some(ptr);
            }
        }
        None
    }

    unsafe fn evict_main(&mut self) -> Option<NonNull<S3FifoHandle<T>>> {
        while let Some(mut ptr) = self.main_queue.pop_front() {
            let handle = ptr.as_mut();
            if handle.freq > 0 {
                handle.freq_dec();
                self.main_queue.push_back(ptr);
            } else {
                handle.queue = Queue::None;
                self.main_weight -= handle.base.weight();
                return Some(ptr);
            }
        }
        None
    }
}

impl<T> Eviction for S3Fifo<T>
where
    T: Send + Sync + 'static,
{
    type Handle = S3FifoHandle<T>;
    type Config = S3FifoConfig;

    unsafe fn new(capacity: usize, config: &Self::Config) -> Self
    where
        Self: Sized,
    {
        let ghost_queue = GhostQueue::new((capacity as f64 * config.ghost_queue_capacity_ratio) as usize);
        let small_weight_capacity = (capacity as f64 * config.small_queue_capacity_ratio) as usize;
        Self {
            ghost_queue,
            small_queue: Dlist::new(),
            main_queue: Dlist::new(),
            small_weight_capacity,
            small_weight: 0,
            main_weight: 0,
            small_to_main_freq_threshold: config.small_to_main_freq_threshold.min(MAX_FREQ),
        }
    }

    unsafe fn push(&mut self, mut ptr: NonNull<Self::Handle>) {
        let handle = ptr.as_mut();
        strict_assert_eq!(handle.freq, 0);
        strict_assert_eq!(handle.queue, Queue::None);

        if self.ghost_queue.contains(handle.base().hash()) {
            handle.queue = Queue::Main;
            self.main_queue.push_back(ptr);
            self.main_weight += handle.base().weight();
        } else {
            handle.queue = Queue::Small;
            self.small_queue.push_back(ptr);
            self.small_weight += handle.base().weight();
        }

        handle.base_mut().set_in_eviction(true);
    }

    // TODO: FIX ME! clippy false positive
    // #[allow(clippy::never_loop)]
    unsafe fn pop(&mut self) -> Option<NonNull<Self::Handle>> {
        if let Some(mut ptr) = self.evict() {
            let handle = ptr.as_mut();
            // `handle.queue` has already been set with `evict()`
            handle.base_mut().set_in_eviction(false);
            Some(ptr)
        } else {
            strict_assert!(self.small_queue.is_empty());
            strict_assert!(self.main_queue.is_empty());
            None
        }
    }

    unsafe fn release(&mut self, _: NonNull<Self::Handle>) {}

    unsafe fn acquire(&mut self, ptr: NonNull<Self::Handle>) {
        let mut ptr = ptr;
        ptr.as_mut().freq_inc();
    }

    unsafe fn remove(&mut self, mut ptr: NonNull<Self::Handle>) {
        let handle = ptr.as_mut();

        match handle.queue {
            Queue::None => unreachable!(),
            Queue::Main => {
                let p = self
                    .main_queue
                    .iter_mut_from_raw(ptr.as_mut().link.raw())
                    .remove()
                    .strict_unwrap_unchecked();
                strict_assert_eq!(p, ptr);

                handle.queue = Queue::None;
                handle.freq = 0;
                handle.base_mut().set_in_eviction(false);

                self.main_weight -= handle.base().weight();
            }
            Queue::Small => {
                let p = self
                    .small_queue
                    .iter_mut_from_raw(ptr.as_mut().link.raw())
                    .remove()
                    .strict_unwrap_unchecked();
                strict_assert_eq!(p, ptr);

                handle.queue = Queue::None;
                handle.freq = 0;
                handle.base_mut().set_in_eviction(false);

                self.small_weight -= handle.base().weight();
            }
        }
    }

    unsafe fn clear(&mut self) -> Vec<NonNull<Self::Handle>> {
        let mut res = Vec::with_capacity(self.len());
        while let Some(ptr) = self.pop() {
            res.push(ptr);
        }
        res
    }

    fn len(&self) -> usize {
        self.small_queue.len() + self.main_queue.len()
    }

    fn is_empty(&self) -> bool {
        self.small_queue.is_empty() && self.main_queue.is_empty()
    }
}

unsafe impl<T> Send for S3Fifo<T> where T: Send + Sync + 'static {}
unsafe impl<T> Sync for S3Fifo<T> where T: Send + Sync + 'static {}

struct GhostQueue {
    counts: HashSet<u64>,
    queue: VecDeque<(u64, usize)>,
    capacity: usize,
    weight: usize,
}

impl GhostQueue {
    fn new(capacity: usize) -> Self {
        Self {
            counts: HashSet::default(),
            queue: VecDeque::new(),
            capacity,
            weight: 0,
        }
    }

    fn push(&mut self, hash: u64, weight: usize) {
        if self.capacity == 0 {
            return;
        }
        while self.weight + weight > self.capacity && self.weight > 0 {
            self.pop();
        }
        self.queue.push_back((hash, weight));
        self.counts.insert(hash);
        self.weight += weight;
    }

    fn pop(&mut self) {
        if let Some((hash, weight)) = self.queue.pop_front() {
            self.weight -= weight;
            self.counts.remove(&hash);
        }
    }

    fn contains(&self, hash: u64) -> bool {
        self.counts.contains(&hash)
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use itertools::Itertools;

    use super::*;
    use crate::{eviction::test_utils::TestEviction, handle::HandleExt};

    impl<T> TestEviction for S3Fifo<T>
    where
        T: Send + Sync + 'static + Clone,
    {
        fn dump(&self) -> Vec<T> {
            self.small_queue
                .iter()
                .chain(self.main_queue.iter())
                .map(|handle| handle.base().data_unwrap_unchecked().clone())
                .collect_vec()
        }
    }

    type TestS3Fifo = S3Fifo<u64>;
    type TestS3FifoHandle = S3FifoHandle<u64>;

    fn assert_test_s3fifo(s3fifo: &TestS3Fifo, small: Vec<u64>, main: Vec<u64>) {
        let mut s = s3fifo.dump().into_iter().collect_vec();
        assert_eq!(s.len(), s3fifo.small_queue.len() + s3fifo.main_queue.len());
        let m = s.split_off(s3fifo.small_queue.len());
        assert_eq!((&s, &m), (&small, &main));
        assert_eq!(s3fifo.small_weight, s.len());
        assert_eq!(s3fifo.main_weight, m.len());
        assert_eq!(s3fifo.len(), s3fifo.small_queue.len() + s3fifo.main_queue.len());
    }

    fn assert_count(ptrs: &[NonNull<TestS3FifoHandle>], range: Range<usize>, count: u8) {
        unsafe {
            ptrs[range].iter().for_each(|ptr| assert_eq!(ptr.as_ref().freq, count));
        }
    }

    #[test]
    fn test_s3fifo() {
        unsafe {
            let ptrs = (0..100)
                .map(|i| {
                    let mut handle = Box::<TestS3FifoHandle>::default();
                    handle.init(i, i, 1, S3FifoContext(CacheContext::Default));
                    NonNull::new_unchecked(Box::into_raw(handle))
                })
                .collect_vec();

            // capacity: 8, small: 2, ghost: 80
            let config = S3FifoConfig {
                small_queue_capacity_ratio: 0.25,
                ghost_queue_capacity_ratio: 10.0,
                small_to_main_freq_threshold: 2,
            };
            let mut s3fifo = TestS3Fifo::new(8, &config);

            assert_eq!(s3fifo.small_weight_capacity, 2);

            s3fifo.push(ptrs[0]);
            s3fifo.push(ptrs[1]);
            assert_test_s3fifo(&s3fifo, vec![0, 1], vec![]);

            s3fifo.push(ptrs[2]);
            s3fifo.push(ptrs[3]);
            assert_test_s3fifo(&s3fifo, vec![0, 1, 2, 3], vec![]);

            assert_count(&ptrs, 0..4, 0);

            (0..4).for_each(|i| s3fifo.acquire(ptrs[i]));
            s3fifo.acquire(ptrs[1]);
            s3fifo.acquire(ptrs[2]);
            assert_count(&ptrs, 0..1, 1);
            assert_count(&ptrs, 1..3, 2);
            assert_count(&ptrs, 3..4, 1);

            let p0 = s3fifo.pop().unwrap();
            let p3 = s3fifo.pop().unwrap();
            assert_eq!(p0, ptrs[0]);
            assert_eq!(p3, ptrs[3]);
            assert_test_s3fifo(&s3fifo, vec![], vec![1, 2]);
            assert_count(&ptrs, 0..1, 0);
            assert_count(&ptrs, 1..3, 2);
            assert_count(&ptrs, 3..4, 0);

            let p1 = s3fifo.pop().unwrap();
            assert_eq!(p1, ptrs[1]);
            assert_test_s3fifo(&s3fifo, vec![], vec![2]);
            assert_count(&ptrs, 0..4, 0);

            assert_eq!(s3fifo.clear(), [2].into_iter().map(|i| ptrs[i]).collect_vec());

            for ptr in ptrs {
                let _ = Box::from_raw(ptr.as_ptr());
            }
        }
    }
}
