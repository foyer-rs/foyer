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

use std::{mem::offset_of, sync::Arc};

use cmsketch::CMSketchU16;
use foyer_common::{
    code::{Context, Key, Value},
    strict_assert, strict_assert_eq, strict_assert_ne,
};
use intrusive_collections::{intrusive_adapter, LinkedList, LinkedListAtomicLink};
use serde::{Deserialize, Serialize};

use super::{Eviction, Op};
use crate::{
    error::{Error, Result},
    record::{CacheHint, Record},
};

/// w-TinyLFU eviction algorithm config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LfuConfig {
    /// `window` capacity ratio of the total cache capacity.
    ///
    /// Must be in (0, 1).
    ///
    /// Must guarantee `window_capacity_ratio + protected_capacity_ratio < 1`.
    pub window_capacity_ratio: f64,
    /// `protected` capacity ratio of the total cache capacity.
    ///
    /// Must be in (0, 1).
    ///
    /// Must guarantee `window_capacity_ratio + protected_capacity_ratio < 1`.
    pub protected_capacity_ratio: f64,

    /// Error of the count-min sketch.
    ///
    /// See [`CMSketchU16::new`].
    pub cmsketch_eps: f64,

    /// Confidence of the count-min sketch.
    ///
    /// See [`CMSketchU16::new`].
    pub cmsketch_confidence: f64,
}

impl Default for LfuConfig {
    fn default() -> Self {
        Self {
            window_capacity_ratio: 0.1,
            protected_capacity_ratio: 0.8,
            cmsketch_eps: 0.001,
            cmsketch_confidence: 0.9,
        }
    }
}

/// w-TinyLFU eviction algorithm hint.
#[derive(Debug, Clone, Default)]
pub struct LfuHint;

impl From<CacheHint> for LfuHint {
    fn from(_: CacheHint) -> Self {
        LfuHint
    }
}

impl From<LfuHint> for CacheHint {
    fn from(_: LfuHint) -> Self {
        CacheHint::Normal
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum Queue {
    None,
    Window,
    Probation,
    Protected,
}

impl Default for Queue {
    fn default() -> Self {
        Self::None
    }
}

/// w-TinyLFU eviction algorithm hint.
#[derive(Debug, Default)]
pub struct LfuState {
    link: LinkedListAtomicLink,
    queue: Queue,
}

intrusive_adapter! { Adapter<K, V, C> = Arc<Record<Lfu<K, V, C>>>: Record<Lfu<K, V, C>> { ?offset = Record::<Lfu<K, V, C>>::STATE_OFFSET + offset_of!(LfuState, link) => LinkedListAtomicLink } where K: Key, V: Value, C: Context }

/// This implementation is inspired by [Caffeine](https://github.com/ben-manes/caffeine) under Apache License 2.0
///
/// A new and hot entry is kept in `window`.
///
/// When `window` is full, entries from it will overflow to `probation`.
///
/// When a entry in `probation` is accessed, it will be promoted to `protected`.
///
/// When `protected` is full, entries from it will overflow to `probation`.
///
/// When evicting, the entry with a lower frequency from `window` or `probation` will be evicted first, then from
/// `protected`.
pub struct Lfu<K, V, C>
where
    K: Key,
    V: Value,
    C: Context,
{
    window: LinkedList<Adapter<K, V, C>>,
    probation: LinkedList<Adapter<K, V, C>>,
    protected: LinkedList<Adapter<K, V, C>>,

    window_weight: usize,
    probation_weight: usize,
    protected_weight: usize,

    window_weight_capacity: usize,
    protected_weight_capacity: usize,

    // TODO(MrCroxx): use a count-min-sketch impl with atomic u16
    frequencies: CMSketchU16,

    step: usize,
    decay: usize,

    config: LfuConfig,
}

impl<K, V, C> Lfu<K, V, C>
where
    K: Key,
    V: Value,
    C: Context,
{
    fn increase_queue_weight(&mut self, queue: Queue, weight: usize) {
        match queue {
            Queue::None => unreachable!(),
            Queue::Window => self.window_weight += weight,
            Queue::Probation => self.probation_weight += weight,
            Queue::Protected => self.protected_weight += weight,
        }
    }

    fn decrease_queue_weight(&mut self, queue: Queue, weight: usize) {
        match queue {
            Queue::None => unreachable!(),
            Queue::Window => self.window_weight -= weight,
            Queue::Probation => self.probation_weight -= weight,
            Queue::Protected => self.protected_weight -= weight,
        }
    }

    fn update_frequencies(&mut self, hash: u64) {
        self.frequencies.inc(hash);
        self.step += 1;
        if self.step >= self.decay {
            self.step >>= 1;
            self.frequencies.halve();
        }
    }
}

impl<K, V, C> Eviction for Lfu<K, V, C>
where
    K: Key,
    V: Value,
    C: Context,
{
    type Config = LfuConfig;
    type Key = K;
    type Value = V;
    type Context = C;
    type Hint = LfuHint;
    type State = LfuState;

    fn new(capacity: usize, config: &Self::Config) -> Self
    where
        Self: Sized,
    {
        assert!(
            config.window_capacity_ratio > 0.0 && config.window_capacity_ratio < 1.0,
            "window_capacity_ratio must be in (0, 1), given: {}",
            config.window_capacity_ratio
        );

        assert!(
            config.protected_capacity_ratio > 0.0 && config.protected_capacity_ratio < 1.0,
            "protected_capacity_ratio must be in (0, 1), given: {}",
            config.protected_capacity_ratio
        );

        assert!(
            config.window_capacity_ratio + config.protected_capacity_ratio < 1.0,
            "must guarantee: window_capacity_ratio + protected_capacity_ratio < 1, given: {}",
            config.window_capacity_ratio + config.protected_capacity_ratio
        );

        let config = config.clone();

        let window_weight_capacity = (capacity as f64 * config.window_capacity_ratio) as usize;
        let protected_weight_capacity = (capacity as f64 * config.protected_capacity_ratio) as usize;
        let frequencies = CMSketchU16::new(config.cmsketch_eps, config.cmsketch_confidence);
        let decay = frequencies.width();

        Self {
            window: LinkedList::new(Adapter::new()),
            probation: LinkedList::new(Adapter::new()),
            protected: LinkedList::new(Adapter::new()),
            window_weight: 0,
            probation_weight: 0,
            protected_weight: 0,
            window_weight_capacity,
            protected_weight_capacity,
            frequencies,
            step: 0,
            decay,
            config,
        }
    }

    fn update(&mut self, capacity: usize, config: Option<&Self::Config>) -> Result<()> {
        if let Some(config) = config {
            let mut msgs = vec![];
            if config.window_capacity_ratio <= 0.0 || config.window_capacity_ratio >= 1.0 {
                msgs.push(format!(
                    "window_capacity_ratio must be in (0, 1), given: {}, new config ignored",
                    config.window_capacity_ratio
                ));
            }
            if config.protected_capacity_ratio <= 0.0 || config.protected_capacity_ratio >= 1.0 {
                msgs.push(format!(
                    "protected_capacity_ratio must be in (0, 1), given: {}, new config ignored",
                    config.protected_capacity_ratio
                ));
            }
            if config.window_capacity_ratio + config.protected_capacity_ratio >= 1.0 {
                msgs.push(format!(
                    "must guarantee: window_capacity_ratio + protected_capacity_ratio < 1, given: {}, new config ignored",
                    config.window_capacity_ratio + config.protected_capacity_ratio
                ));
            }

            if !msgs.is_empty() {
                return Err(Error::ConfigError(msgs.join(" | ")));
            }

            self.config = config.clone();
        }

        // TODO(MrCroxx): Raise a warn log the cmsketch args updates is not supported yet if it is modified.

        let window_weight_capacity = (capacity as f64 * self.config.window_capacity_ratio) as usize;
        let protected_weight_capacity = (capacity as f64 * self.config.protected_capacity_ratio) as usize;

        self.window_weight_capacity = window_weight_capacity;
        self.protected_weight_capacity = protected_weight_capacity;

        Ok(())
    }

    /// Push a new record to `window`.
    ///
    /// Overflow record from `window` to `probation` if needed.
    fn push(&mut self, record: Arc<Record<Self>>) {
        let state = unsafe { &mut *record.state().get() };

        strict_assert!(!state.link.is_linked());
        strict_assert!(!record.is_in_eviction());
        strict_assert_eq!(state.queue, Queue::None);

        record.set_in_eviction(true);
        state.queue = Queue::Window;
        self.increase_queue_weight(Queue::Window, record.weight());
        self.update_frequencies(record.hash());
        self.window.push_back(record);

        // If `window` weight exceeds the capacity, overflow entry from `window` to `probation`.
        while self.window_weight > self.window_weight_capacity {
            strict_assert!(!self.window.is_empty());
            let r = self.window.pop_front().unwrap();
            let s = unsafe { &mut *r.state().get() };
            self.decrease_queue_weight(Queue::Window, r.weight());
            s.queue = Queue::Probation;
            self.increase_queue_weight(Queue::Probation, r.weight());
            self.probation.push_back(r);
        }
    }

    fn pop(&mut self) -> Option<Arc<Record<Self>>> {
        // Compare the frequency of the front element of `window` and `probation` queue, and evict the lower one.
        // If both `window` and `probation` are empty, try evict from `protected`.
        let mut cw = self.window.front_mut();
        let mut cp = self.probation.front_mut();
        let record = match (cw.get(), cp.get()) {
            (None, None) => None,
            (None, Some(_)) => cp.remove(),
            (Some(_), None) => cw.remove(),
            (Some(w), Some(p)) => {
                if self.frequencies.estimate(w.hash()) < self.frequencies.estimate(p.hash()) {
                    cw.remove()

                    // TODO(MrCroxx): Rotate probation to prevent a high frequency but cold head holds back promotion
                    // too long like CacheLib does?
                } else {
                    cp.remove()
                }
            }
        }
        .or_else(|| self.protected.pop_front())?;

        let state = unsafe { &mut *record.state().get() };

        strict_assert!(!state.link.is_linked());
        strict_assert!(record.is_in_eviction());
        strict_assert_ne!(state.queue, Queue::None);

        self.decrease_queue_weight(state.queue, record.weight());
        state.queue = Queue::None;
        record.set_in_eviction(false);

        Some(record)
    }

    fn remove(&mut self, record: &Arc<Record<Self>>) {
        let state = unsafe { &mut *record.state().get() };

        strict_assert!(state.link.is_linked());
        strict_assert!(record.is_in_eviction());
        strict_assert_ne!(state.queue, Queue::None);

        match state.queue {
            Queue::None => unreachable!(),
            Queue::Window => unsafe { self.window.remove_from_ptr(Arc::as_ptr(record)) },
            Queue::Probation => unsafe { self.probation.remove_from_ptr(Arc::as_ptr(record)) },
            Queue::Protected => unsafe { self.protected.remove_from_ptr(Arc::as_ptr(record)) },
        };

        strict_assert!(!state.link.is_linked());

        self.decrease_queue_weight(state.queue, record.weight());
        state.queue = Queue::None;
        record.set_in_eviction(false);
    }

    fn clear(&mut self) {
        while let Some(record) = self.pop() {
            let state = unsafe { &*record.state().get() };
            strict_assert!(!record.is_in_eviction());
            strict_assert!(!state.link.is_linked());
            strict_assert_eq!(state.queue, Queue::None);
        }
    }

    fn acquire() -> Op<Self> {
        Op::mutable(|this: &mut Self, record| {
            // Update frequency by access.
            this.update_frequencies(record.hash());

            if !record.is_in_eviction() {
                return;
            }

            let state = unsafe { &mut *record.state().get() };

            strict_assert!(state.link.is_linked());

            match state.queue {
                Queue::None => unreachable!(),
                Queue::Window => {
                    // Move to MRU position of `window`.
                    let r = unsafe { this.window.remove_from_ptr(Arc::as_ptr(record)) };
                    this.window.push_back(r);
                }
                Queue::Probation => {
                    // Promote to MRU position of `protected`.
                    let r = unsafe { this.probation.remove_from_ptr(Arc::as_ptr(record)) };
                    this.decrease_queue_weight(Queue::Probation, record.weight());
                    state.queue = Queue::Protected;
                    this.increase_queue_weight(Queue::Protected, record.weight());
                    this.protected.push_back(r);

                    // If `protected` weight exceeds the capacity, overflow entry from `protected` to `probation`.
                    while this.protected_weight > this.protected_weight_capacity {
                        strict_assert!(!this.protected.is_empty());
                        let r = this.protected.pop_front().unwrap();
                        let s = unsafe { &mut *r.state().get() };
                        this.decrease_queue_weight(Queue::Protected, r.weight());
                        s.queue = Queue::Probation;
                        this.increase_queue_weight(Queue::Probation, r.weight());
                        this.probation.push_back(r);
                    }
                }
                Queue::Protected => {
                    // Move to MRU position of `protected`.
                    let r = unsafe { this.protected.remove_from_ptr(Arc::as_ptr(record)) };
                    this.protected.push_back(r);
                }
            }
        })
    }

    fn release() -> Op<Self> {
        Op::noop()
    }
}

#[cfg(test)]
mod tests {

    use itertools::Itertools;

    use super::*;
    use crate::{
        eviction::test_utils::{assert_ptr_eq, assert_ptr_vec_vec_eq, Dump, OpExt},
        record::Data,
    };

    impl<K, V> Dump for Lfu<K, V, ()>
    where
        K: Key + Clone,
        V: Value + Clone,
    {
        type Output = Vec<Vec<Arc<Record<Self>>>>;
        fn dump(&self) -> Self::Output {
            let mut window = vec![];
            let mut probation = vec![];
            let mut protected = vec![];

            let mut cursor = self.window.cursor();
            loop {
                cursor.move_next();
                match cursor.clone_pointer() {
                    Some(record) => window.push(record),
                    None => break,
                }
            }

            let mut cursor = self.probation.cursor();
            loop {
                cursor.move_next();
                match cursor.clone_pointer() {
                    Some(record) => probation.push(record),
                    None => break,
                }
            }

            let mut cursor = self.protected.cursor();
            loop {
                cursor.move_next();
                match cursor.clone_pointer() {
                    Some(record) => protected.push(record),
                    None => break,
                }
            }

            vec![window, probation, protected]
        }
    }

    type TestLfu = Lfu<u64, u64, ()>;

    #[test]
    fn test_lfu() {
        let rs = (0..100)
            .map(|i| {
                Arc::new(Record::new(Data {
                    key: i,
                    value: i,
                    hint: LfuHint,
                    hash: i,
                    weight: 1,
                    location: Default::default(),
                }))
            })
            .collect_vec();
        let r = |i: usize| rs[i].clone();

        // window: 2, probation: 2, protected: 6
        let config = LfuConfig {
            window_capacity_ratio: 0.2,
            protected_capacity_ratio: 0.6,
            cmsketch_eps: 0.01,
            cmsketch_confidence: 0.95,
        };
        let mut lfu = TestLfu::new(10, &config);

        assert_eq!(lfu.window_weight_capacity, 2);
        assert_eq!(lfu.protected_weight_capacity, 6);

        lfu.push(r(0));
        lfu.push(r(1));
        assert_ptr_vec_vec_eq(lfu.dump(), vec![vec![r(0), r(1)], vec![], vec![]]);

        lfu.push(r(2));
        lfu.push(r(3));
        assert_ptr_vec_vec_eq(lfu.dump(), vec![vec![r(2), r(3)], vec![r(0), r(1)], vec![]]);

        (4..10).for_each(|i| lfu.push(r(i)));
        assert_ptr_vec_vec_eq(
            lfu.dump(),
            vec![
                vec![r(8), r(9)],
                vec![r(0), r(1), r(2), r(3), r(4), r(5), r(6), r(7)],
                vec![],
            ],
        );

        // [8, 9] [1, 2, 3, 4, 5, 6, 7]
        let r0 = lfu.pop().unwrap();
        assert_ptr_eq(&rs[0], &r0);

        // [9, 0] [1, 2, 3, 4, 5, 6, 7, 8]
        lfu.push(r(0));
        assert_ptr_vec_vec_eq(
            lfu.dump(),
            vec![
                vec![r(9), r(0)],
                vec![r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8)],
                vec![],
            ],
        );

        // [0, 9] [1, 2, 3, 4, 5, 6, 7, 8]
        lfu.acquire_mutable(&rs[9]);
        assert_ptr_vec_vec_eq(
            lfu.dump(),
            vec![
                vec![r(0), r(9)],
                vec![r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8)],
                vec![],
            ],
        );

        // [0, 9] [1, 2, 7, 8] [3, 4, 5, 6]
        (3..7).for_each(|i| lfu.acquire_mutable(&rs[i]));
        assert_ptr_vec_vec_eq(
            lfu.dump(),
            vec![
                vec![r(0), r(9)],
                vec![r(1), r(2), r(7), r(8)],
                vec![r(3), r(4), r(5), r(6)],
            ],
        );

        // [0, 9] [1, 2, 7, 8] [5, 6, 3, 4]
        (3..5).for_each(|i| lfu.acquire_mutable(&rs[i]));
        assert_ptr_vec_vec_eq(
            lfu.dump(),
            vec![
                vec![r(0), r(9)],
                vec![r(1), r(2), r(7), r(8)],
                vec![r(5), r(6), r(3), r(4)],
            ],
        );

        // [0, 9] [5, 6] [3, 4, 1, 2, 7, 8]
        [1, 2, 7, 8].into_iter().for_each(|i| lfu.acquire_mutable(&rs[i]));
        assert_ptr_vec_vec_eq(
            lfu.dump(),
            vec![
                vec![r(0), r(9)],
                vec![r(5), r(6)],
                vec![r(3), r(4), r(1), r(2), r(7), r(8)],
            ],
        );

        // [0, 9] [6] [3, 4, 1, 2, 7, 8]
        let r5 = lfu.pop().unwrap();
        assert_ptr_eq(&rs[5], &r5);
        assert_ptr_vec_vec_eq(
            lfu.dump(),
            vec![vec![r(0), r(9)], vec![r(6)], vec![r(3), r(4), r(1), r(2), r(7), r(8)]],
        );

        // [11, 12] [6, 0, 9, 10] [3, 4, 1, 2, 7, 8]
        (10..13).for_each(|i| lfu.push(r(i)));
        assert_ptr_vec_vec_eq(
            lfu.dump(),
            vec![
                vec![r(11), r(12)],
                vec![r(6), r(0), r(9), r(10)],
                vec![r(3), r(4), r(1), r(2), r(7), r(8)],
            ],
        );

        // 0: high freq
        // [11, 12] [6, 9, 10, 3] [4, 1, 2, 7, 8, 0]
        (0..10).for_each(|_| lfu.acquire_mutable(&rs[0]));
        assert_ptr_vec_vec_eq(
            lfu.dump(),
            vec![
                vec![r(11), r(12)],
                vec![r(6), r(9), r(10), r(3)],
                vec![r(4), r(1), r(2), r(7), r(8), r(0)],
            ],
        );

        // 0: high freq
        // [11, 12] [0, 6, 9, 10] [3, 4, 1, 2, 7, 8]
        lfu.acquire_mutable(&rs[6]);
        lfu.acquire_mutable(&rs[9]);
        lfu.acquire_mutable(&rs[10]);
        lfu.acquire_mutable(&rs[3]);
        lfu.acquire_mutable(&rs[4]);
        lfu.acquire_mutable(&rs[1]);
        lfu.acquire_mutable(&rs[2]);
        lfu.acquire_mutable(&rs[7]);
        lfu.acquire_mutable(&rs[8]);
        assert_ptr_vec_vec_eq(
            lfu.dump(),
            vec![
                vec![r(11), r(12)],
                vec![r(0), r(6), r(9), r(10)],
                vec![r(3), r(4), r(1), r(2), r(7), r(8)],
            ],
        );

        // evict 11, 12 because freq(11) < freq(0), freq(12) < freq(0)
        // [12] [0, 9, 10] [3, 4, 1, 2, 7, 8]
        assert!(lfu.frequencies.estimate(0) > lfu.frequencies.estimate(11));
        assert!(lfu.frequencies.estimate(0) > lfu.frequencies.estimate(12));
        let r11 = lfu.pop().unwrap();
        let r12 = lfu.pop().unwrap();
        assert_ptr_eq(&rs[11], &r11);
        assert_ptr_eq(&rs[12], &r12);
        assert_ptr_vec_vec_eq(
            lfu.dump(),
            vec![
                vec![],
                vec![r(0), r(6), r(9), r(10)],
                vec![r(3), r(4), r(1), r(2), r(7), r(8)],
            ],
        );

        // evict 0, high freq but cold
        // [] [6, 9, 10] [3, 4, 1, 2, 7, 8]
        let r0 = lfu.pop().unwrap();
        assert_ptr_eq(&rs[0], &r0);
        assert_ptr_vec_vec_eq(
            lfu.dump(),
            vec![
                vec![],
                vec![r(6), r(9), r(10)],
                vec![r(3), r(4), r(1), r(2), r(7), r(8)],
            ],
        );

        lfu.clear();
        assert_ptr_vec_vec_eq(lfu.dump(), vec![vec![], vec![], vec![]]);
    }
}
