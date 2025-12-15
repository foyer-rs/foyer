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

use foyer_common::{
    code::{Key, Value},
    error::{Error, ErrorKind, Result},
    properties::{Hint, Properties},
    strict_assert,
};
use intrusive_collections::{intrusive_adapter, LinkedList, LinkedListAtomicLink};
use serde::{Deserialize, Serialize};

use super::{Eviction, Op};
use crate::record::Record;

/// Lru eviction algorithm config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LruConfig {
    /// The ratio of the high priority pool occupied.
    ///
    /// `Lru` guarantees that the high priority weight are always as larger as
    /// but no larger that the capacity * high priority pool ratio.
    ///
    /// # Panic
    ///
    /// Panics if the value is not in [0, 1.0].
    pub high_priority_pool_ratio: f64,
}

impl Default for LruConfig {
    fn default() -> Self {
        Self {
            high_priority_pool_ratio: 0.9,
        }
    }
}

/// Lru eviction algorithm state.
#[derive(Debug, Default)]
pub struct LruState {
    link: LinkedListAtomicLink,
    in_high_priority_pool: bool,
    is_pinned: bool,
}

intrusive_adapter! { Adapter<K, V, P> = Arc<Record<Lru<K, V, P>>>: Record<Lru<K, V, P>> { ?offset = Record::<Lru<K, V, P>>::STATE_OFFSET + offset_of!(LruState, link) => LinkedListAtomicLink } where K: Key, V: Value, P: Properties }

pub struct Lru<K, V, P>
where
    K: Key,
    V: Value,
    P: Properties,
{
    high_priority_list: LinkedList<Adapter<K, V, P>>,
    list: LinkedList<Adapter<K, V, P>>,
    pin_list: LinkedList<Adapter<K, V, P>>,

    high_priority_weight: usize,
    high_priority_weight_capacity: usize,

    config: LruConfig,
}

impl<K, V, P> Lru<K, V, P>
where
    K: Key,
    V: Value,
    P: Properties,
{
    fn may_overflow_high_priority_pool(&mut self) {
        while self.high_priority_weight > self.high_priority_weight_capacity {
            strict_assert!(!self.high_priority_list.is_empty());

            // overflow last entry in high priority pool to low priority pool
            let record = self.high_priority_list.pop_front().unwrap();
            let state = unsafe { &mut *record.state().get() };
            strict_assert!(state.in_high_priority_pool);
            state.in_high_priority_pool = false;
            self.high_priority_weight -= record.weight();
            self.list.push_back(record);
        }
    }
}

impl<K, V, P> Eviction for Lru<K, V, P>
where
    K: Key,
    V: Value,
    P: Properties,
{
    type Config = LruConfig;
    type Key = K;
    type Value = V;
    type Properties = P;
    type State = LruState;

    fn new(capacity: usize, config: &Self::Config) -> Self
    where
        Self: Sized,
    {
        assert!(
            (0.0..=1.0).contains(&config.high_priority_pool_ratio),
            "high_priority_pool_ratio_percentage must be in 0.0..=1.0, given: {}",
            config.high_priority_pool_ratio
        );

        let config = config.clone();

        let high_priority_weight_capacity = (capacity as f64 * config.high_priority_pool_ratio) as usize;

        Self {
            high_priority_list: LinkedList::new(Adapter::new()),
            list: LinkedList::new(Adapter::new()),
            pin_list: LinkedList::new(Adapter::new()),
            high_priority_weight: 0,
            high_priority_weight_capacity,
            config,
        }
    }

    fn update(&mut self, capacity: usize, config: Option<&Self::Config>) -> Result<()> {
        if let Some(config) = config {
            if !(0.0..=1.0).contains(&config.high_priority_pool_ratio) {
                return Err(
                    Error::new(ErrorKind::Config, "update LRU config failed")
                        .with_context("reason", format!(
                            "high_priority_pool_ratio_percentage must be in 0.0..=1.0, given: {}, new configuration ignored",
                            config.high_priority_pool_ratio
                        ))
                );
            }
            self.config = config.clone();
        }

        let high_priority_weight_capacity = (capacity as f64 * self.config.high_priority_pool_ratio) as usize;
        self.high_priority_weight_capacity = high_priority_weight_capacity;

        self.may_overflow_high_priority_pool();

        Ok(())
    }

    fn push(&mut self, record: Arc<Record<Self>>) {
        let state = unsafe { &mut *record.state().get() };

        strict_assert!(!state.link.is_linked());

        record.set_in_eviction(true);

        match record.properties().hint().unwrap_or_default() {
            Hint::Normal => {
                state.in_high_priority_pool = true;
                self.high_priority_weight += record.weight();
                self.high_priority_list.push_back(record);

                self.may_overflow_high_priority_pool();
            }
            Hint::Low => {
                state.in_high_priority_pool = false;
                self.list.push_back(record);
            }
        }
    }

    fn pop(&mut self) -> Option<Arc<Record<Self>>> {
        let record = self.list.pop_front().or_else(|| self.high_priority_list.pop_front())?;

        let state = unsafe { &mut *record.state().get() };

        strict_assert!(!state.link.is_linked());

        if state.in_high_priority_pool {
            self.high_priority_weight -= record.weight();
            state.in_high_priority_pool = false;
        }

        record.set_in_eviction(false);

        Some(record)
    }

    fn remove(&mut self, record: &Arc<Record<Self>>) {
        let state = unsafe { &mut *record.state().get() };

        strict_assert!(state.link.is_linked());

        match (state.is_pinned, state.in_high_priority_pool) {
            (true, false) => unsafe { self.pin_list.remove_from_ptr(Arc::as_ptr(record)) },
            (true, true) => unsafe {
                self.high_priority_weight -= record.weight();
                state.in_high_priority_pool = false;
                self.pin_list.remove_from_ptr(Arc::as_ptr(record))
            },
            (false, true) => {
                self.high_priority_weight -= record.weight();
                state.in_high_priority_pool = false;
                unsafe { self.high_priority_list.remove_from_ptr(Arc::as_ptr(record)) }
            }
            (false, false) => unsafe { self.list.remove_from_ptr(Arc::as_ptr(record)) },
        };

        #[cfg(not(miri))]
        strict_assert!(!state.link.is_linked());

        record.set_in_eviction(false);
    }

    fn clear(&mut self) {
        while self.pop().is_some() {}

        // Clear pin list to prevent from memory leak.
        while let Some(record) = self.pin_list.pop_front() {
            let state = unsafe { &mut *record.state().get() };
            strict_assert!(!state.link.is_linked());

            if state.in_high_priority_pool {
                self.high_priority_weight -= record.weight();
                state.in_high_priority_pool = false;
            }

            record.set_in_eviction(false);
        }

        assert!(self.list.is_empty());
        assert!(self.high_priority_list.is_empty());
        assert!(self.pin_list.is_empty());
        assert_eq!(self.high_priority_weight, 0);
    }

    fn acquire() -> Op<Self> {
        Op::mutable(|this: &mut Self, record| {
            if !record.is_in_eviction() {
                return;
            }

            let state = unsafe { &mut *record.state().get() };
            assert!(state.link.is_linked());

            if state.is_pinned {
                return;
            }

            // Pin the record by moving it to the pin list.

            let r = if state.in_high_priority_pool {
                unsafe { this.high_priority_list.remove_from_ptr(Arc::as_ptr(record)) }
            } else {
                unsafe { this.list.remove_from_ptr(Arc::as_ptr(record)) }
            };

            this.pin_list.push_back(r);

            state.is_pinned = true;
        })
    }

    fn release() -> Op<Self> {
        Op::mutable(|this: &mut Self, record| {
            if !record.is_in_eviction() {
                return;
            }

            let state = unsafe { &mut *record.state().get() };
            assert!(state.link.is_linked());

            if !state.is_pinned {
                return;
            }

            // Unpin the record by moving it from the pin list.

            unsafe { this.pin_list.remove_from_ptr(Arc::as_ptr(record)) };

            if state.in_high_priority_pool {
                this.high_priority_list.push_back(record.clone());
            } else {
                this.list.push_back(record.clone());
            }

            state.is_pinned = false;
        })
    }
}

#[cfg(test)]
pub mod tests {

    use itertools::Itertools;

    use super::*;
    use crate::{
        eviction::test_utils::{assert_ptr_eq, assert_ptr_vec_vec_eq, Dump, OpExt, TestProperties},
        record::Data,
    };

    impl<K, V> Dump for Lru<K, V, TestProperties>
    where
        K: Key + Clone,
        V: Value + Clone,
    {
        type Output = Vec<Vec<Arc<Record<Self>>>>;
        fn dump(&self) -> Self::Output {
            let mut low = vec![];
            let mut high = vec![];
            let mut pin = vec![];

            let mut cursor = self.list.cursor();
            loop {
                cursor.move_next();
                match cursor.clone_pointer() {
                    Some(record) => low.push(record),
                    None => break,
                }
            }

            let mut cursor = self.high_priority_list.cursor();
            loop {
                cursor.move_next();
                match cursor.clone_pointer() {
                    Some(record) => high.push(record),
                    None => break,
                }
            }

            let mut cursor = self.pin_list.cursor();
            loop {
                cursor.move_next();
                match cursor.clone_pointer() {
                    Some(record) => pin.push(record),
                    None => break,
                }
            }

            vec![low, high, pin]
        }
    }

    type TestLru = Lru<u64, u64, TestProperties>;

    #[test]
    fn test_lru() {
        let rs = (0..20)
            .map(|i| {
                Arc::new(Record::new(Data {
                    key: i,
                    value: i,
                    properties: if i < 10 {
                        TestProperties::default().with_hint(Hint::Normal)
                    } else {
                        TestProperties::default().with_hint(Hint::Low)
                    },
                    hash: i,
                    weight: 1,
                }))
            })
            .collect_vec();
        let r = |i: usize| rs[i].clone();

        let config = LruConfig {
            high_priority_pool_ratio: 0.5,
        };
        let mut lru = TestLru::new(8, &config);

        assert_eq!(lru.high_priority_weight_capacity, 4);

        // [0, 1, 2, 3]
        lru.push(r(0));
        lru.push(r(1));
        lru.push(r(2));
        lru.push(r(3));
        assert_ptr_vec_vec_eq(lru.dump(), vec![vec![], vec![r(0), r(1), r(2), r(3)], vec![]]);

        // 0, [1, 2, 3, 4]
        lru.push(r(4));
        assert_ptr_vec_vec_eq(lru.dump(), vec![vec![r(0)], vec![r(1), r(2), r(3), r(4)], vec![]]);

        // 0, 10, [1, 2, 3, 4]
        lru.push(r(10));
        assert_ptr_vec_vec_eq(
            lru.dump(),
            vec![vec![r(0), r(10)], vec![r(1), r(2), r(3), r(4)], vec![]],
        );

        // 10, [1, 2, 3, 4]
        let r0 = lru.pop().unwrap();
        assert_ptr_eq(&r(0), &r0);
        assert_ptr_vec_vec_eq(lru.dump(), vec![vec![r(10)], vec![r(1), r(2), r(3), r(4)], vec![]]);

        // 10, [1, 3, 4]
        lru.remove(&rs[2]);
        assert_ptr_vec_vec_eq(lru.dump(), vec![vec![r(10)], vec![r(1), r(3), r(4)], vec![]]);

        // 10, 11, [1, 3, 4]
        lru.push(r(11));
        assert_ptr_vec_vec_eq(lru.dump(), vec![vec![r(10), r(11)], vec![r(1), r(3), r(4)], vec![]]);

        // 10, 11, 1, [3, 4, 5, 6]
        lru.push(r(5));
        lru.push(r(6));
        assert_ptr_vec_vec_eq(
            lru.dump(),
            vec![vec![r(10), r(11), r(1)], vec![r(3), r(4), r(5), r(6)], vec![]],
        );

        // 10, 11, 1, 3, [4, 5, 6, 0]
        lru.push(r(0));
        assert_ptr_vec_vec_eq(
            lru.dump(),
            vec![vec![r(10), r(11), r(1), r(3)], vec![r(4), r(5), r(6), r(0)], vec![]],
        );

        lru.clear();
        assert_ptr_vec_vec_eq(lru.dump(), vec![vec![], vec![], vec![]]);
    }

    #[test]
    fn test_lru_pin() {
        let rs = (0..20)
            .map(|i| {
                Arc::new(Record::new(Data {
                    key: i,
                    value: i,
                    properties: if i < 10 {
                        TestProperties::default().with_hint(Hint::Normal)
                    } else {
                        TestProperties::default().with_hint(Hint::Low)
                    },
                    hash: i,
                    weight: 1,
                }))
            })
            .collect_vec();
        let r = |i: usize| rs[i].clone();

        let config = LruConfig {
            high_priority_pool_ratio: 0.5,
        };
        let mut lru = TestLru::new(8, &config);

        assert_eq!(lru.high_priority_weight_capacity, 4);

        // 10, 11, [0, 1]
        lru.push(r(0));
        lru.push(r(1));
        lru.push(r(10));
        lru.push(r(11));
        assert_ptr_vec_vec_eq(lru.dump(), vec![vec![r(10), r(11)], vec![r(0), r(1)], vec![]]);

        // pin: [0], 10
        // 11, [1]
        lru.acquire_mutable(&rs[0]);
        lru.acquire_mutable(&rs[10]);
        assert_ptr_vec_vec_eq(lru.dump(), vec![vec![r(11)], vec![r(1)], vec![r(0), r(10)]]);

        // 11, 10, [1, 0]
        lru.release_mutable(&rs[0]);
        lru.release_mutable(&rs[10]);
        assert_ptr_vec_vec_eq(lru.dump(), vec![vec![r(11), r(10)], vec![r(1), r(0)], vec![]]);

        // acquire pinned
        // pin: [0], 11
        // 10, [1]
        lru.acquire_mutable(&rs[0]);
        lru.acquire_mutable(&rs[11]);
        lru.acquire_mutable(&rs[0]);
        lru.acquire_mutable(&rs[11]);
        assert_ptr_vec_vec_eq(lru.dump(), vec![vec![r(10)], vec![r(1)], vec![r(0), r(11)]]);

        // remove pinned (low priority)
        // pin: [0]
        // 10, [1]
        lru.remove(&rs[11]);
        assert_ptr_vec_vec_eq(lru.dump(), vec![vec![r(10)], vec![r(1)], vec![r(0)]]);

        // remove pinned (high priority)
        // step 1:
        //   pin: [0], [2]
        //   10, [1]
        lru.push(r(2));
        lru.acquire_mutable(&rs[2]);
        assert_ptr_vec_vec_eq(lru.dump(), vec![vec![r(10)], vec![r(1)], vec![r(0), r(2)]]);
        // step 2:
        //   pin: [0]
        //   10, [1]
        lru.remove(&rs[2]);
        assert_ptr_vec_vec_eq(lru.dump(), vec![vec![r(10)], vec![r(1)], vec![r(0)]]);

        // release removed
        // pin: [0]
        // 10, [1]
        lru.release_mutable(&rs[11]);
        assert_ptr_vec_vec_eq(lru.dump(), vec![vec![r(10)], vec![r(1)], vec![r(0)]]);

        // release unpinned
        // 10, [1, 0]
        lru.release_mutable(&rs[0]);
        lru.release_mutable(&rs[0]);
        assert_ptr_vec_vec_eq(lru.dump(), vec![vec![r(10)], vec![r(1), r(0)], vec![]]);

        // clear with pinned
        // pin: [1]
        // 10, [0]
        lru.acquire_mutable(&rs[1]);
        assert_ptr_vec_vec_eq(lru.dump(), vec![vec![r(10)], vec![r(0)], vec![r(1)]]);

        lru.clear();
        assert_ptr_vec_vec_eq(lru.dump(), vec![vec![], vec![], vec![]]);
    }
}
