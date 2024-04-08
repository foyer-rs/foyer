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

use std::{fmt::Debug, ptr::NonNull};

use foyer_intrusive::{
    collections::dlist::{Dlist, DlistLink},
    intrusive_adapter,
};

use crate::{
    eviction::Eviction,
    handle::{BaseHandle, Handle},
    CacheContext, Key, Value,
};

#[derive(Debug, Clone)]
pub struct S3FifoContext;

impl From<CacheContext> for S3FifoContext {
    fn from(_: CacheContext) -> Self {
        Self
    }
}

impl From<S3FifoContext> for CacheContext {
    fn from(_: S3FifoContext) -> Self {
        CacheContext::Default
    }
}

enum Queue {
    None,
    Main,
    Small,
}

pub struct S3FifoHandle<K, V>
where
    K: Key,
    V: Value,
{
    link: DlistLink,
    base: BaseHandle<K, V, S3FifoContext>,
    freq: u8,
    queue: Queue,
}

impl<K, V> Debug for S3FifoHandle<K, V>
where
    K: Key,
    V: Value,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3FifoHandle").finish()
    }
}

intrusive_adapter! { S3FifoHandleDlistAdapter<K, V> = NonNull<S3FifoHandle<K, V>>: S3FifoHandle<K, V> { link: DlistLink } where K: Key, V: Value }

impl<K, V> S3FifoHandle<K, V>
where
    K: Key,
    V: Value,
{
    #[inline(always)]
    pub fn inc(&mut self) {
        self.freq = std::cmp::min(self.freq + 1, 3);
    }

    #[inline(always)]
    pub fn dec(&mut self) {
        self.freq = self.freq.saturating_sub(1);
    }

    #[inline(always)]
    pub fn reset(&mut self) {
        self.freq = 0;
    }
}

impl<K, V> Handle for S3FifoHandle<K, V>
where
    K: Key,
    V: Value,
{
    type Key = K;
    type Value = V;
    type Context = S3FifoContext;

    fn new() -> Self {
        Self {
            link: DlistLink::default(),
            freq: 0,
            base: BaseHandle::new(),
            queue: Queue::None,
        }
    }

    fn init(&mut self, hash: u64, key: Self::Key, value: Self::Value, charge: usize, context: Self::Context) {
        self.base.init(hash, key, value, charge, context);
    }

    fn base(&self) -> &BaseHandle<Self::Key, Self::Value, Self::Context> {
        &self.base
    }

    fn base_mut(&mut self) -> &mut BaseHandle<Self::Key, Self::Value, Self::Context> {
        &mut self.base
    }
}

#[derive(Debug, Clone)]
pub struct S3FifoConfig {
    pub small_queue_capacity_ratio: f64,
}

pub struct S3Fifo<K, V>
where
    K: Key,
    V: Value,
{
    small_queue: Dlist<S3FifoHandleDlistAdapter<K, V>>,
    main_queue: Dlist<S3FifoHandleDlistAdapter<K, V>>,

    small_capacity: usize,

    small_charges: usize,
    main_charges: usize,
}

impl<K, V> S3Fifo<K, V>
where
    K: Key,
    V: Value,
{
    unsafe fn evict(&mut self) -> Option<NonNull<<S3Fifo<K, V> as Eviction>::Handle>> {
        if self.small_charges > self.small_capacity
            && let Some(ptr) = self.evict_small()
        {
            Some(ptr)
        } else {
            self.evict_main()
        }
    }

    unsafe fn evict_small(&mut self) -> Option<NonNull<<S3Fifo<K, V> as Eviction>::Handle>> {
        while let Some(mut ptr) = self.small_queue.pop_front() {
            let handle = ptr.as_mut();
            if handle.freq > 1 {
                self.main_queue.push_back(ptr);
                handle.queue = Queue::Main;
                self.small_charges -= handle.base().charge();
                self.main_charges += handle.base().charge();
            } else {
                handle.queue = Queue::None;
                handle.reset();
                self.small_charges -= handle.base().charge();
                return Some(ptr);
            }
        }
        None
    }

    unsafe fn evict_main(&mut self) -> Option<NonNull<<S3Fifo<K, V> as Eviction>::Handle>> {
        while let Some(mut ptr) = self.main_queue.pop_front() {
            let handle = ptr.as_mut();
            if handle.freq > 0 {
                self.main_queue.push_back(ptr);
                handle.dec();
            } else {
                handle.queue = Queue::None;
                self.main_charges -= handle.base.charge();
                return Some(ptr);
            }
        }
        None
    }
}

impl<K, V> Eviction for S3Fifo<K, V>
where
    K: Key,
    V: Value,
{
    type Handle = S3FifoHandle<K, V>;
    type Config = S3FifoConfig;

    unsafe fn new(capacity: usize, config: &Self::Config) -> Self
    where
        Self: Sized,
    {
        let small_capacity = (capacity as f64 * config.small_queue_capacity_ratio) as usize;
        Self {
            small_queue: Dlist::new(),
            main_queue: Dlist::new(),
            small_capacity,
            small_charges: 0,
            main_charges: 0,
        }
    }

    unsafe fn push(&mut self, mut ptr: NonNull<Self::Handle>) {
        let handle = ptr.as_mut();

        self.small_queue.push_back(ptr);
        handle.queue = Queue::Small;
        self.small_charges += handle.base().charge();

        handle.base_mut().set_in_eviction(true);
    }

    unsafe fn pop(&mut self) -> Option<NonNull<Self::Handle>> {
        if let Some(mut ptr) = self.evict() {
            let handle = ptr.as_mut();
            // `handle.queue` has already been set with `evict()`
            handle.base_mut().set_in_eviction(false);
            Some(ptr)
        } else {
            debug_assert!(self.is_empty());
            None
        }
    }

    unsafe fn reinsert(&mut self, _: NonNull<Self::Handle>) {}

    unsafe fn access(&mut self, ptr: NonNull<Self::Handle>) {
        let mut ptr = ptr;
        ptr.as_mut().inc();
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
                    .unwrap_unchecked();
                debug_assert_eq!(p, ptr);

                handle.queue = Queue::None;
                handle.base_mut().set_in_eviction(false);

                self.main_charges -= handle.base().charge();
            }
            Queue::Small => {
                let p = self
                    .small_queue
                    .iter_mut_from_raw(ptr.as_mut().link.raw())
                    .remove()
                    .unwrap_unchecked();
                debug_assert_eq!(p, ptr);

                handle.queue = Queue::None;
                handle.base_mut().set_in_eviction(false);

                self.small_charges -= handle.base().charge();
            }
        }
    }

    unsafe fn clear(&mut self) -> Vec<NonNull<Self::Handle>> {
        let mut res = Vec::with_capacity(self.len());
        while let Some(mut ptr) = self.small_queue.pop_front() {
            let handle = ptr.as_mut();
            handle.base_mut().set_in_eviction(false);
            handle.queue = Queue::None;
            res.push(ptr);
        }
        while let Some(mut ptr) = self.main_queue.pop_front() {
            let handle = ptr.as_mut();
            handle.base_mut().set_in_eviction(false);
            handle.queue = Queue::None;
            res.push(ptr);
        }
        res
    }

    unsafe fn len(&self) -> usize {
        self.small_queue.len() + self.main_queue.len()
    }

    unsafe fn is_empty(&self) -> bool {
        self.small_queue.is_empty() && self.main_queue.is_empty()
    }
}

unsafe impl<K, V> Send for S3Fifo<K, V>
where
    K: Key,
    V: Value,
{
}
unsafe impl<K, V> Sync for S3Fifo<K, V>
where
    K: Key,
    V: Value,
{
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use itertools::Itertools;

    use super::*;
    use crate::eviction::test_utils::TestEviction;

    impl<K, V> TestEviction for S3Fifo<K, V>
    where
        K: Key + Clone,
        V: Value + Clone,
    {
        fn dump(&self) -> Vec<(<Self::Handle as Handle>::Key, <Self::Handle as Handle>::Value)> {
            self.small_queue
                .iter()
                .chain(self.main_queue.iter())
                .map(|handle| (handle.base().key().clone(), handle.base().value().clone()))
                .collect_vec()
        }
    }

    type TestS3Fifo = S3Fifo<u64, u64>;
    type TestS3FifoHandle = S3FifoHandle<u64, u64>;

    fn assert_test_s3fifo(s3fifo: &TestS3Fifo, small: Vec<u64>, main: Vec<u64>) {
        let mut s = s3fifo
            .dump()
            .into_iter()
            .map(|(k, v)| {
                assert_eq!(k, v);
                k
            })
            .collect_vec();
        assert_eq!(s.len(), s3fifo.small_queue.len() + s3fifo.main_queue.len());
        let m = s.split_off(s3fifo.small_queue.len());
        assert_eq!((&s, &m), (&small, &main));
        assert_eq!(s3fifo.small_charges, s.len());
    }

    fn assert_count(ptrs: &[NonNull<TestS3FifoHandle>], range: Range<usize>, count: u8) {
        unsafe {
            ptrs[range].iter().for_each(|ptr| assert_eq!(ptr.as_ref().freq, count));
        }
    }

    #[test]
    fn test_lfu() {
        unsafe {
            let ptrs = (0..100)
                .map(|i| {
                    let mut handle = Box::new(TestS3FifoHandle::new());
                    handle.init(i, i, i, 1, S3FifoContext);
                    NonNull::new_unchecked(Box::into_raw(handle))
                })
                .collect_vec();

            // window: 2, probation: 2, protected: 6
            let config = S3FifoConfig {
                small_queue_capacity_ratio: 0.25,
            };
            let mut s3fifo = TestS3Fifo::new(8, &config);

            assert_eq!(s3fifo.small_capacity, 2);

            s3fifo.push(ptrs[0]);
            s3fifo.push(ptrs[1]);
            assert_test_s3fifo(&s3fifo, vec![0, 1], vec![]);

            s3fifo.push(ptrs[2]);
            s3fifo.push(ptrs[3]);
            assert_test_s3fifo(&s3fifo, vec![0, 1, 2, 3], vec![]);

            assert_count(&ptrs, 0..4, 0);

            (0..4).for_each(|i| s3fifo.access(ptrs[i]));
            s3fifo.access(ptrs[1]);
            s3fifo.access(ptrs[2]);
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
