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

use foyer_common::code::{Key, Value};
use intrusive_collections::{LinkedList, LinkedListAtomicLink, intrusive_adapter};
use serde::{Deserialize, Serialize};

use super::{Eviction, Op};
use crate::{
    error::Result,
    record::{CacheHint, Record},
};

/// Fifo eviction algorithm config.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FifoConfig {}

/// Fifo eviction algorithm hint.
#[derive(Debug, Clone, Default)]
pub struct FifoHint;

impl From<CacheHint> for FifoHint {
    fn from(_: CacheHint) -> Self {
        FifoHint
    }
}

impl From<FifoHint> for CacheHint {
    fn from(_: FifoHint) -> Self {
        CacheHint::Normal
    }
}

/// Fifo eviction algorithm state.
#[derive(Debug, Default)]
pub struct FifoState {
    link: LinkedListAtomicLink,
}

intrusive_adapter! { Adapter<K, V> = Arc<Record<Fifo<K, V>>>: Record<Fifo<K, V>> { ?offset = Record::<Fifo<K, V>>::STATE_OFFSET + offset_of!(FifoState, link) => LinkedListAtomicLink } where K: Key, V: Value }

pub struct Fifo<K, V>
where
    K: Key,
    V: Value,
{
    queue: LinkedList<Adapter<K, V>>,
}

impl<K, V> Eviction for Fifo<K, V>
where
    K: Key,
    V: Value,
{
    type Config = FifoConfig;
    type Key = K;
    type Value = V;
    type Hint = FifoHint;
    type State = FifoState;

    fn new(_capacity: usize, _config: &Self::Config) -> Self
    where
        Self: Sized,
    {
        Self {
            queue: LinkedList::new(Adapter::new()),
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
        self.queue.pop_front().inspect(|record| record.set_in_eviction(false))
    }

    fn remove(&mut self, record: &Arc<Record<Self>>) {
        unsafe { self.queue.remove_from_ptr(Arc::as_ptr(record)) };
        record.set_in_eviction(false);
    }

    fn acquire() -> Op<Self> {
        Op::noop()
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
        eviction::test_utils::{Dump, assert_ptr_eq, assert_ptr_vec_eq},
        record::Data,
    };

    impl<K, V> Dump for Fifo<K, V>
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

    type TestFifo = Fifo<u64, u64>;

    #[test]
    fn test_fifo() {
        let rs = (0..8)
            .map(|i| {
                Arc::new(Record::new(Data {
                    key: i,
                    value: i,
                    hint: FifoHint,
                    hash: i,
                    weight: 1,
                    location: Default::default(),
                }))
            })
            .collect_vec();
        let r = |i: usize| rs[i].clone();

        let mut fifo = TestFifo::new(100, &FifoConfig {});

        // 0, 1, 2, 3
        fifo.push(r(0));
        fifo.push(r(1));
        fifo.push(r(2));
        fifo.push(r(3));

        // 2, 3
        let r0 = fifo.pop().unwrap();
        let r1 = fifo.pop().unwrap();
        assert_ptr_eq(&rs[0], &r0);
        assert_ptr_eq(&rs[1], &r1);

        // 2, 3, 4, 5, 6
        fifo.push(r(4));
        fifo.push(r(5));
        fifo.push(r(6));

        // 2, 6
        fifo.remove(&rs[3]);
        fifo.remove(&rs[4]);
        fifo.remove(&rs[5]);

        assert_ptr_vec_eq(fifo.dump(), vec![r(2), r(6)]);

        fifo.clear();

        assert_ptr_vec_eq(fifo.dump(), vec![]);
    }
}
