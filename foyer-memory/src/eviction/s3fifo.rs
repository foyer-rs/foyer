//  Copyright 2024 foyer Project Authors
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
    ptr::NonNull,
    sync::atomic::{AtomicU8, Ordering},
};

use foyer_common::{
    assert::OptionExt,
    code::{Key, Value},
    strict_assert, strict_assert_eq,
};
use foyer_intrusive::{
    dlist::{Dlist, DlistLink},
    intrusive_adapter,
};
use serde::{Deserialize, Serialize};

use super::{Eviction, Operator};
use crate::record::{CacheHint, Record};

/// S3Fifo eviction algorithm config.
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

/// S3Fifo eviction algorithm hint.
#[derive(Debug, Clone, Default)]
pub struct S3FifoHint;

impl From<CacheHint> for S3FifoHint {
    fn from(_: CacheHint) -> Self {
        S3FifoHint
    }
}

impl From<S3FifoHint> for CacheHint {
    fn from(_: S3FifoHint) -> Self {
        CacheHint::Normal
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Queue {
    None,
    Main,
    Small,
}

impl Default for Queue {
    fn default() -> Self {
        Self::None
    }
}

/// S3Fifo eviction algorithm hint.
#[derive(Debug, Default)]
pub struct S3FifoState {
    link: DlistLink,
    frequency: AtomicU8,
    queue: Queue,
}

impl S3FifoState {
    const MAX_FREQUENCY: u8 = 3;

    fn frequency(&self) -> u8 {
        self.frequency.load(Ordering::Acquire)
    }

    fn set_frequency(&self, val: u8) {
        self.frequency.store(val, Ordering::Release)
    }

    fn inc_frequency(&self) -> u8 {
        self.frequency
            .fetch_update(Ordering::Release, Ordering::Acquire, |v| {
                Some(std::cmp::min(Self::MAX_FREQUENCY, v + 1))
            })
            .unwrap()
    }

    fn dec_frequency(&self) -> u8 {
        self.frequency
            .fetch_update(Ordering::Release, Ordering::Acquire, |v| Some(v.saturating_sub(1)))
            .unwrap()
    }
}

intrusive_adapter! { Adapter<K, V> = Record<S3Fifo<K, V>> { state.link => DlistLink } where K: Key, V: Value }

pub struct S3Fifo<K, V>
where
    K: Key,
    V: Value,
{
    ghost_queue: GhostQueue,
    small_queue: Dlist<Adapter<K, V>>,
    main_queue: Dlist<Adapter<K, V>>,

    small_weight_capacity: usize,

    small_weight: usize,
    main_weight: usize,

    small_to_main_freq_threshold: u8,
}

impl<K, V> S3Fifo<K, V>
where
    K: Key,
    V: Value,
{
    fn evict(&mut self) -> Option<NonNull<Record<S3Fifo<K, V>>>> {
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

    #[expect(clippy::never_loop)]
    fn evict_small_force(&mut self) -> Option<NonNull<Record<S3Fifo<K, V>>>> {
        while let Some(mut ptr) = self.small_queue.pop_front() {
            let record = unsafe { ptr.as_mut() };
            record.state.queue = Queue::None;
            record.state.set_frequency(0);
            self.small_weight -= record.weight();
            return Some(ptr);
        }
        None
    }

    fn evict_small(&mut self) -> Option<NonNull<Record<S3Fifo<K, V>>>> {
        while let Some(mut ptr) = self.small_queue.pop_front() {
            let record = unsafe { ptr.as_mut() };
            if record.state.frequency() >= self.small_to_main_freq_threshold {
                record.state.queue = Queue::Main;
                self.main_queue.push_back(ptr);
                self.small_weight -= record.weight();
                self.main_weight += record.weight();
            } else {
                record.state.queue = Queue::None;
                record.state.set_frequency(0);
                self.small_weight -= record.weight();

                self.ghost_queue.push(record.hash(), record.weight());

                return Some(ptr);
            }
        }
        None
    }

    fn evict_main(&mut self) -> Option<NonNull<Record<S3Fifo<K, V>>>> {
        while let Some(mut ptr) = self.main_queue.pop_front() {
            let record = unsafe { ptr.as_mut() };

            if record.state.dec_frequency() > 0 {
                self.main_queue.push_back(ptr);
            } else {
                record.state.queue = Queue::None;
                self.main_weight -= record.weight();
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
    type Config = S3FifoConfig;
    type Key = K;
    type Value = V;
    type Hint = S3FifoHint;
    type State = S3FifoState;

    fn new(capacity: usize, config: &Self::Config) -> Self
    where
        Self: Sized,
    {
        let ghost_queue_capacity = (capacity as f64 * config.ghost_queue_capacity_ratio) as usize;
        let ghost_queue = GhostQueue::new(ghost_queue_capacity);
        let small_weight_capacity = (capacity as f64 * config.small_queue_capacity_ratio) as usize;
        Self {
            ghost_queue,
            small_queue: Dlist::new(),
            main_queue: Dlist::new(),
            small_weight_capacity,
            small_weight: 0,
            main_weight: 0,
            small_to_main_freq_threshold: config.small_to_main_freq_threshold.min(S3FifoState::MAX_FREQUENCY),
        }
    }

    fn update(&mut self, capacity: usize, config: &Self::Config) {
        let ghost_queue_capacity = (capacity as f64 * config.ghost_queue_capacity_ratio) as usize;
        let small_weight_capacity = (capacity as f64 * config.small_queue_capacity_ratio) as usize;
        self.ghost_queue.update(ghost_queue_capacity);
        self.small_weight_capacity = small_weight_capacity;
    }

    fn push(&mut self, mut ptr: NonNull<Record<Self>>) {
        let record = unsafe { ptr.as_mut() };
        strict_assert_eq!(record.state.frequency(), 0);
        strict_assert_eq!(record.state.queue, Queue::None);

        if self.ghost_queue.contains(record.hash()) {
            record.state.queue = Queue::Main;
            self.main_queue.push_back(ptr);
            self.main_weight += record.weight();
        } else {
            record.state.queue = Queue::Small;
            self.small_queue.push_back(ptr);
            self.small_weight += record.weight();
        }

        record.set_in_eviction(true);
    }

    fn pop(&mut self) -> Option<NonNull<Record<Self>>> {
        if let Some(mut ptr) = self.evict() {
            let record = unsafe { ptr.as_mut() };
            // `handle.queue` has already been set with `evict()`
            record.set_in_eviction(false);
            Some(ptr)
        } else {
            strict_assert!(self.small_queue.is_empty());
            strict_assert!(self.main_queue.is_empty());
            None
        }
    }

    fn remove(&mut self, mut ptr: NonNull<Record<Self>>) {
        let record = unsafe { ptr.as_mut() };

        match record.state.queue {
            Queue::None => unreachable!(),
            Queue::Main => {
                let p = unsafe {
                    self.main_queue
                        .iter_mut_with_ptr(ptr)
                        .remove()
                        .strict_unwrap_unchecked()
                };

                strict_assert_eq!(p, ptr);

                record.state.queue = Queue::None;
                record.state.set_frequency(0);
                record.set_in_eviction(false);

                self.main_weight -= record.weight();
            }
            Queue::Small => {
                let p = unsafe {
                    self.small_queue
                        .iter_mut_with_ptr(ptr)
                        .remove()
                        .strict_unwrap_unchecked()
                };
                strict_assert_eq!(p, ptr);

                record.state.queue = Queue::None;
                record.state.set_frequency(0);
                record.set_in_eviction(false);

                self.small_weight -= record.weight();
            }
        }
    }

    fn clear(&mut self) {
        while self.pop().is_some() {}
    }

    fn len(&self) -> usize {
        self.small_queue.len() + self.main_queue.len()
    }

    fn acquire_operator() -> super::Operator {
        Operator::Immutable
    }

    fn acquire_immutable(&self, ptr: NonNull<Record<Self>>) {
        unsafe { ptr.as_ref() }.state.inc_frequency();
    }

    fn acquire_mutable(&mut self, _ptr: NonNull<Record<Self>>) {
        unreachable!()
    }

    fn release(&mut self, _ptr: NonNull<Record<Self>>) {}
}

// TODO(MrCroxx): use ordered hash map?
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

    fn update(&mut self, capacity: usize) {
        self.capacity = capacity;
        if self.capacity == 0 {
            return;
        }
        while self.weight > self.capacity && self.weight > 0 {
            self.pop();
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
    use crate::{eviction::test_utils::TestEviction, record::Data};

    impl<K, V> TestEviction for S3Fifo<K, V>
    where
        K: Key + Clone,
        V: Value + Clone,
    {
        fn dump(&self) -> Vec<NonNull<Record<Self>>> {
            self.small_queue
                .iter_ptr()
                .chain(self.main_queue.iter_ptr())
                .collect_vec()
        }
    }

    type TestS3Fifo = S3Fifo<u64, u64>;

    fn assert_test_s3fifo(
        s3fifo: &TestS3Fifo,
        small: Vec<NonNull<Record<TestS3Fifo>>>,
        main: Vec<NonNull<Record<TestS3Fifo>>>,
    ) {
        let mut s = s3fifo.dump().into_iter().collect_vec();
        assert_eq!(s.len(), s3fifo.small_queue.len() + s3fifo.main_queue.len());
        let m = s.split_off(s3fifo.small_queue.len());
        assert_eq!((&s, &m), (&small, &main));
        assert_eq!(s3fifo.small_weight, s.len());
        assert_eq!(s3fifo.main_weight, m.len());
        assert_eq!(s3fifo.len(), s3fifo.small_queue.len() + s3fifo.main_queue.len());
    }

    fn assert_count(ptrs: &[NonNull<Record<TestS3Fifo>>], range: Range<usize>, count: u8) {
        ptrs[range]
            .iter()
            .for_each(|ptr| assert_eq!(unsafe { ptr.as_ref() }.state.frequency(), count));
    }

    #[test]
    fn test_s3fifo() {
        unsafe {
            let ptrs = (0..100)
                .map(|i| {
                    let handle = Box::new(Record::new(Data {
                        key: i,
                        value: i,
                        hint: S3FifoHint,
                        state: Default::default(),
                        hash: i,
                        weight: 1,
                    }));
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

            let ps = |indices: &[usize]| indices.iter().map(|&i| ptrs[i]).collect_vec();

            assert_eq!(s3fifo.small_weight_capacity, 2);

            s3fifo.push(ptrs[0]);
            s3fifo.push(ptrs[1]);
            assert_test_s3fifo(&s3fifo, ps(&[0, 1]), vec![]);

            s3fifo.push(ptrs[2]);
            s3fifo.push(ptrs[3]);
            assert_test_s3fifo(&s3fifo, ps(&[0, 1, 2, 3]), vec![]);

            assert_count(&ptrs, 0..4, 0);

            (0..4).for_each(|i| s3fifo.acquire_immutable(ptrs[i]));
            s3fifo.acquire_immutable(ptrs[1]);
            s3fifo.acquire_immutable(ptrs[2]);
            assert_count(&ptrs, 0..1, 1);
            assert_count(&ptrs, 1..3, 2);
            assert_count(&ptrs, 3..4, 1);

            let p0 = s3fifo.pop().unwrap();
            let p3 = s3fifo.pop().unwrap();
            assert_eq!(p0, ptrs[0]);
            assert_eq!(p3, ptrs[3]);
            assert_test_s3fifo(&s3fifo, vec![], ps(&[1, 2]));
            assert_count(&ptrs, 0..1, 0);
            assert_count(&ptrs, 1..3, 2);
            assert_count(&ptrs, 3..4, 0);

            let p1 = s3fifo.pop().unwrap();
            assert_eq!(p1, ptrs[1]);
            assert_test_s3fifo(&s3fifo, vec![], ps(&[2]));
            assert_count(&ptrs, 0..4, 0);

            s3fifo.clear();
            assert_test_s3fifo(&s3fifo, vec![], vec![]);

            for ptr in ptrs {
                let _ = Box::from_raw(ptr.as_ptr());
            }
        }
    }
}
