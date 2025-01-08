// Copyright 2025 foyer Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    collections::{HashSet, VecDeque},
    mem::offset_of,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
};

use foyer_common::{
    code::{Key, Value},
    strict_assert, strict_assert_eq,
};
use intrusive_collections::{intrusive_adapter, LinkedList, LinkedListAtomicLink};
use serde::{Deserialize, Serialize};

use super::{Eviction, Op};
use crate::{
    error::{Error, Result},
    record::{CacheHint, Record},
};

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
    link: LinkedListAtomicLink,
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

intrusive_adapter! { Adapter<K, V> = Arc<Record<S3Fifo<K, V>>>: Record<S3Fifo<K, V>> { ?offset = Record::<S3Fifo<K, V>>::STATE_OFFSET + offset_of!(S3FifoState, link) => LinkedListAtomicLink } where K: Key, V: Value }

pub struct S3Fifo<K, V>
where
    K: Key,
    V: Value,
{
    ghost_queue: GhostQueue,
    small_queue: LinkedList<Adapter<K, V>>,
    main_queue: LinkedList<Adapter<K, V>>,

    small_weight_capacity: usize,

    small_weight: usize,
    main_weight: usize,

    small_to_main_freq_threshold: u8,

    config: S3FifoConfig,
}

impl<K, V> S3Fifo<K, V>
where
    K: Key,
    V: Value,
{
    fn evict(&mut self) -> Option<Arc<Record<S3Fifo<K, V>>>> {
        // TODO(MrCroxx): Use `let_chains` here after it is stable.
        if self.small_weight > self.small_weight_capacity {
            if let Some(record) = self.evict_small() {
                return Some(record);
            }
        }
        if let Some(record) = self.evict_main() {
            return Some(record);
        }
        self.evict_small_force()
    }

    #[expect(clippy::never_loop)]
    fn evict_small_force(&mut self) -> Option<Arc<Record<S3Fifo<K, V>>>> {
        while let Some(record) = self.small_queue.pop_front() {
            let state = unsafe { &mut *record.state().get() };
            state.queue = Queue::None;
            state.set_frequency(0);
            self.small_weight -= record.weight();
            return Some(record);
        }
        None
    }

    fn evict_small(&mut self) -> Option<Arc<Record<S3Fifo<K, V>>>> {
        while let Some(record) = self.small_queue.pop_front() {
            let state = unsafe { &mut *record.state().get() };
            if state.frequency() >= self.small_to_main_freq_threshold {
                state.queue = Queue::Main;
                self.small_weight -= record.weight();
                self.main_weight += record.weight();
                self.main_queue.push_back(record);
            } else {
                state.queue = Queue::None;
                state.set_frequency(0);
                self.small_weight -= record.weight();

                self.ghost_queue.push(record.hash(), record.weight());

                return Some(record);
            }
        }
        None
    }

    fn evict_main(&mut self) -> Option<Arc<Record<S3Fifo<K, V>>>> {
        while let Some(record) = self.main_queue.pop_front() {
            let state = unsafe { &mut *record.state().get() };
            if state.dec_frequency() > 0 {
                self.main_queue.push_back(record);
            } else {
                state.queue = Queue::None;
                self.main_weight -= record.weight();
                return Some(record);
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
        assert!(
            config.small_queue_capacity_ratio > 0.0 && config.small_queue_capacity_ratio < 1.0,
            "small_queue_capacity_ratio must be in (0, 1), given: {}",
            config.small_queue_capacity_ratio
        );

        let config = config.clone();

        let ghost_queue_capacity = (capacity as f64 * config.ghost_queue_capacity_ratio) as usize;
        let ghost_queue = GhostQueue::new(ghost_queue_capacity);
        let small_weight_capacity = (capacity as f64 * config.small_queue_capacity_ratio) as usize;

        Self {
            ghost_queue,
            small_queue: LinkedList::new(Adapter::new()),
            main_queue: LinkedList::new(Adapter::new()),
            small_weight_capacity,
            small_weight: 0,
            main_weight: 0,
            small_to_main_freq_threshold: config.small_to_main_freq_threshold.min(S3FifoState::MAX_FREQUENCY),
            config,
        }
    }

    fn update(&mut self, capacity: usize, config: Option<&Self::Config>) -> Result<()> {
        if let Some(config) = config {
            if config.small_queue_capacity_ratio > 0.0 && config.small_queue_capacity_ratio < 1.0 {
                return Err(Error::ConfigError(format!(
                    "small_queue_capacity_ratio must be in (0, 1), given: {}",
                    config.small_queue_capacity_ratio
                )));
            }
            self.config = config.clone();
        }

        let ghost_queue_capacity = (capacity as f64 * self.config.ghost_queue_capacity_ratio) as usize;
        let small_weight_capacity = (capacity as f64 * self.config.small_queue_capacity_ratio) as usize;
        self.ghost_queue.update(ghost_queue_capacity);
        self.small_weight_capacity = small_weight_capacity;

        Ok(())
    }

    fn push(&mut self, record: Arc<Record<Self>>) {
        let state = unsafe { &mut *record.state().get() };

        strict_assert_eq!(state.frequency(), 0);
        strict_assert_eq!(state.queue, Queue::None);

        record.set_in_eviction(true);

        if self.ghost_queue.contains(record.hash()) {
            state.queue = Queue::Main;
            self.main_weight += record.weight();
            self.main_queue.push_back(record);
        } else {
            state.queue = Queue::Small;
            self.small_weight += record.weight();
            self.small_queue.push_back(record);
        }
    }

    fn pop(&mut self) -> Option<Arc<Record<Self>>> {
        if let Some(record) = self.evict() {
            // `handle.queue` has already been set with `evict()`
            record.set_in_eviction(false);
            Some(record)
        } else {
            strict_assert!(self.small_queue.is_empty());
            strict_assert!(self.main_queue.is_empty());
            None
        }
    }

    fn remove(&mut self, record: &Arc<Record<Self>>) {
        let state = unsafe { &mut *record.state().get() };

        match state.queue {
            Queue::None => unreachable!(),
            Queue::Main => {
                unsafe { self.main_queue.remove_from_ptr(Arc::as_ptr(record)) };

                state.queue = Queue::None;
                state.set_frequency(0);
                record.set_in_eviction(false);

                self.main_weight -= record.weight();
            }
            Queue::Small => {
                unsafe { self.small_queue.remove_from_ptr(Arc::as_ptr(record)) };

                state.queue = Queue::None;
                state.set_frequency(0);
                record.set_in_eviction(false);

                self.small_weight -= record.weight();
            }
        }
    }

    fn acquire() -> Op<Self> {
        Op::immutable(|_: &Self, record| {
            let state = unsafe { &mut *record.state().get() };
            state.inc_frequency();
        })
    }

    fn release() -> Op<Self> {
        Op::noop()
    }
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
    use crate::{
        eviction::test_utils::{assert_ptr_eq, assert_ptr_vec_vec_eq, Dump, OpExt},
        record::Data,
    };

    impl<K, V> Dump for S3Fifo<K, V>
    where
        K: Key + Clone,
        V: Value + Clone,
    {
        type Output = Vec<Vec<Arc<Record<Self>>>>;

        fn dump(&self) -> Self::Output {
            let mut small = vec![];
            let mut main = vec![];

            let mut cursor = self.small_queue.cursor();
            loop {
                cursor.move_next();
                match cursor.clone_pointer() {
                    Some(record) => small.push(record),
                    None => break,
                }
            }

            let mut cursor = self.main_queue.cursor();
            loop {
                cursor.move_next();
                match cursor.clone_pointer() {
                    Some(record) => main.push(record),
                    None => break,
                }
            }

            vec![small, main]
        }
    }

    type TestS3Fifo = S3Fifo<u64, u64>;

    fn assert_frequencies(rs: &[Arc<Record<TestS3Fifo>>], range: Range<usize>, count: u8) {
        rs[range]
            .iter()
            .for_each(|r| assert_eq!(unsafe { &*r.state().get() }.frequency(), count));
    }

    #[test]
    fn test_s3fifo() {
        let rs = (0..100)
            .map(|i| {
                Arc::new(Record::new(Data {
                    key: i,
                    value: i,
                    hint: S3FifoHint,
                    hash: i,
                    weight: 1,
                }))
            })
            .collect_vec();
        let r = |i: usize| rs[i].clone();

        // capacity: 8, small: 2, ghost: 80
        let config = S3FifoConfig {
            small_queue_capacity_ratio: 0.25,
            ghost_queue_capacity_ratio: 10.0,
            small_to_main_freq_threshold: 2,
        };
        let mut s3fifo = TestS3Fifo::new(8, &config);

        assert_eq!(s3fifo.small_weight_capacity, 2);

        s3fifo.push(r(0));
        s3fifo.push(r(1));
        assert_ptr_vec_vec_eq(s3fifo.dump(), vec![vec![r(0), r(1)], vec![]]);

        s3fifo.push(r(2));
        s3fifo.push(r(3));
        assert_ptr_vec_vec_eq(s3fifo.dump(), vec![vec![r(0), r(1), r(2), r(3)], vec![]]);

        assert_frequencies(&rs, 0..4, 0);

        (0..4).for_each(|i| s3fifo.acquire_immutable(&rs[i]));
        s3fifo.acquire_immutable(&rs[1]);
        s3fifo.acquire_immutable(&rs[2]);
        assert_frequencies(&rs, 0..1, 1);
        assert_frequencies(&rs, 1..3, 2);
        assert_frequencies(&rs, 3..4, 1);

        let r0 = s3fifo.pop().unwrap();
        let r3 = s3fifo.pop().unwrap();
        assert_ptr_eq(&rs[0], &r0);
        assert_ptr_eq(&rs[3], &r3);
        assert_ptr_vec_vec_eq(s3fifo.dump(), vec![vec![], vec![r(1), r(2)]]);
        assert_frequencies(&rs, 0..1, 0);
        assert_frequencies(&rs, 1..3, 2);
        assert_frequencies(&rs, 3..4, 0);

        let r1 = s3fifo.pop().unwrap();
        assert_ptr_eq(&rs[1], &r1);
        assert_ptr_vec_vec_eq(s3fifo.dump(), vec![vec![], vec![r(2)]]);
        assert_frequencies(&rs, 0..4, 0);

        s3fifo.clear();
        assert_ptr_vec_vec_eq(s3fifo.dump(), vec![vec![], vec![]]);
    }
}
