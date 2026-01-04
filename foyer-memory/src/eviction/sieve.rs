// Copyright 2026 foyer Project Authors
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
    mem::offset_of,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use foyer_common::{
    code::{Key, Value},
    error::Result,
    properties::Properties,
};
use intrusive_collections::{intrusive_adapter, LinkedList, LinkedListAtomicLink};
use serde::{Deserialize, Serialize};

use super::{Eviction, Op};
use crate::record::Record;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SieveConfig;

#[derive(Debug, Default)]
pub struct SieveState {
    link: LinkedListAtomicLink,
    visited: AtomicBool,
}

impl SieveState {
    #[inline]
    fn is_visited(&self) -> bool {
        self.visited.load(Ordering::Relaxed)
    }

    fn set_visited(&self, visited: bool) {
        self.visited.store(visited, Ordering::Relaxed);
    }
}

intrusive_adapter! {
    Adapter<K, V, P> = Arc<Record<Sieve<K, V, P>>>: Record<Sieve<K, V, P>> {
        ?offset = Record::<Sieve<K, V, P>>::STATE_OFFSET + offset_of!(SieveState, link) => LinkedListAtomicLink
    }
    where K: Key, V: Value, P: Properties
}

/// Sieve eviction algorithm implementation based on the paper:
/// "SIEVE is Simpler than LRU: an Efficient Turn-Key Eviction Algorithm for Web Caches"
/// (https://junchengyang.com/publication/nsdi24-SIEVE.pdf).
///
/// The Sieve algorithm is specifically designed for web cache workloads, providing
/// a simple and efficient alternative to traditional LRU eviction policies. It leverages
/// a visited bit to distinguish between recently accessed and unaccessed items, enabling
/// efficient eviction decisions with minimal overhead.
///
/// **Note:** Due to its lack of scan resistance, Sieve is not recommended for use in
/// block cache workloads or environments where scan-resistant eviction is required.
/// It is best suited for web workloads where access patterns align with the algorithm's
/// design assumptions.
pub struct Sieve<K, V, P>
where
    K: Key,
    V: Value,
    P: Properties,
{
    queue: LinkedList<Adapter<K, V, P>>,
    /// Hand pointer for eviction scanning, points to the next candidate to examine
    hand: Option<Arc<Record<Sieve<K, V, P>>>>,
}

impl<K, V, P> Eviction for Sieve<K, V, P>
where
    K: Key,
    V: Value,
    P: Properties,
{
    type Config = SieveConfig;
    type Key = K;
    type Value = V;
    type Properties = P;
    type State = SieveState;

    fn new(_capacity: usize, _config: &Self::Config) -> Self
    where
        Self: Sized,
    {
        Self {
            queue: LinkedList::new(Adapter::new()),
            hand: None,
        }
    }

    fn update(&mut self, _: usize, _: Option<&Self::Config>) -> Result<()> {
        Ok(())
    }

    fn push(&mut self, record: Arc<Record<Self>>) {
        record.set_in_eviction(true);
        self.queue.push_back(record);
    }

    fn pop(&mut self) -> Option<Arc<Record<Self>>> {
        let mut candidate = if let Some(ref hand_ptr) = self.hand {
            unsafe { self.queue.cursor_mut_from_ptr(Arc::as_ptr(hand_ptr)) }
        } else {
            self.queue.front_mut()
        };

        loop {
            if let Some(record) = candidate.get() {
                let state = unsafe { &*record.state().get() };
                if !state.is_visited() {
                    break;
                } else {
                    state.set_visited(false);
                    if candidate.peek_next().is_null() {
                        candidate = self.queue.front_mut();
                    } else {
                        candidate.move_next();
                    }
                }
            } else {
                // Queue is empty, no record to evict
                return None;
            }
        }

        self.hand = candidate.peek_next().clone_pointer();
        candidate.remove().inspect(|record| record.set_in_eviction(false))
    }

    fn remove(&mut self, record: &Arc<Record<Self>>) {
        if let Some(ref hand_ptr) = self.hand {
            if Arc::ptr_eq(hand_ptr, record) {
                // Reset hand if we are removing the current hand pointer
                self.hand = None;
            }
        }

        unsafe { self.queue.remove_from_ptr(Arc::as_ptr(record)) };
        record.set_in_eviction(false);
    }

    fn acquire() -> Op<Self> {
        Op::immutable(|_: &Self, record| {
            let state = unsafe { &*record.state().get() };
            state.set_visited(true);
        })
    }

    fn release() -> Op<Self> {
        Op::noop()
    }
}

#[cfg(test)]
pub mod tests {
    use itertools::Itertools;

    use super::*;
    use crate::{
        eviction::test_utils::{assert_ptr_eq, assert_ptr_vec_eq, Dump, OpExt, TestProperties},
        record::Data,
    };

    impl<K, V> Dump for Sieve<K, V, TestProperties>
    where
        K: Key + Clone,
        V: Value + Clone,
    {
        type Output = Vec<Arc<Record<Self>>>;
        fn dump(&self) -> Self::Output {
            let mut res = vec![];
            let mut cursor = self.queue.cursor();
            loop {
                cursor.move_next();
                match cursor.clone_pointer() {
                    Some(record) => res.push(record),
                    None => break,
                }
            }
            res
        }
    }

    type TestSieve = Sieve<u64, u64, TestProperties>;

    #[test]
    fn test_sieve_basic() {
        let rs = (0..8)
            .map(|i| {
                Arc::new(Record::new(Data {
                    key: i,
                    value: i,
                    properties: TestProperties::default(),
                    hash: i,
                    weight: 1,
                }))
            })
            .collect_vec();
        let r = |i: usize| rs[i].clone();
        let mut sieve = TestSieve::new(100, &SieveConfig {});

        // 0, 1, 2, 3
        sieve.push(r(0));
        sieve.push(r(1));
        sieve.push(r(2));
        sieve.push(r(3));
        assert_ptr_vec_eq(sieve.dump(), vec![r(0), r(1), r(2), r(3)]);

        sieve.acquire_immutable(&r(1));
        sieve.acquire_immutable(&r(3));

        // 0 is oldest, and not visited, so it should be evicted first
        let r0 = sieve.pop().unwrap();
        assert_ptr_eq(&rs[0], &r0);

        // 1 is visited, so it will not be evicted in this round
        let r2 = sieve.pop().unwrap();
        assert_ptr_eq(&rs[2], &r2);

        // Now we only have 1 and 3 in the sieve, and 1 is unvisited, 3 is visited
        // and hand points to 3
        assert_ptr_vec_eq(sieve.dump(), vec![r(1), r(3)]);

        // 1 is unvisited, so it will be evicted
        let r1 = sieve.pop().unwrap();
        assert_ptr_eq(&rs[1], &r1);

        assert_ptr_vec_eq(sieve.dump(), vec![r(3)]);

        sieve.remove(&r(3));
        assert_ptr_vec_eq(sieve.dump(), vec![]);

        // clear
        sieve.push(r(4));
        sieve.push(r(5));
        sieve.push(r(6));
        sieve.clear();
        assert_ptr_vec_eq(sieve.dump(), vec![]);
    }
}
