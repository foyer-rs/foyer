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

use std::ptr::NonNull;

use foyer_common::code::{Key, Value};
use foyer_intrusive_v2::{
    dlist::{Dlist, DlistLink},
    intrusive_adapter,
};
use serde::{Deserialize, Serialize};

use super::{Eviction, Operator};
use crate::record::{CacheHint, Record};

/// Fifo eviction algorithm config.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FifoConfig;

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
    link: DlistLink,
}

intrusive_adapter! { Adapter<K, V> = Record<Fifo<K, V>> { state.link => DlistLink } where K: Key, V: Value }

pub struct Fifo<K, V>
where
    K: Key,
    V: Value,
{
    queue: Dlist<Adapter<K, V>>,
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
        Self { queue: Dlist::new() }
    }

    fn update(&mut self, _: usize, _: &Self::Config) {}

    fn push(&mut self, ptr: NonNull<Record<Self>>) {
        self.queue.push_back(ptr);
        unsafe { ptr.as_ref().set_in_eviction(true) };
    }

    fn pop(&mut self) -> Option<NonNull<Record<Self>>> {
        self.queue
            .pop_front()
            .inspect(|ptr| unsafe { ptr.as_ref().set_in_eviction(false) })
    }

    fn remove(&mut self, ptr: NonNull<Record<Self>>) {
        let p = self.queue.remove(ptr);
        assert_eq!(p, ptr);
        unsafe { ptr.as_ref().set_in_eviction(false) };
    }

    fn clear(&mut self) {
        while self.pop().is_some() {}
    }

    fn len(&self) -> usize {
        self.queue.len()
    }

    fn acquire_operator() -> super::Operator {
        Operator::Immutable
    }

    fn acquire_immutable(&self, _ptr: NonNull<Record<Self>>) {}

    fn acquire_mutable(&mut self, _ptr: NonNull<Record<Self>>) {
        unreachable!()
    }

    fn release(&mut self, _ptr: NonNull<Record<Self>>) {}
}

#[cfg(test)]
pub mod tests {

    use itertools::Itertools;

    use super::*;
    use crate::{eviction::test_utils::TestEviction, record::Data};

    impl<K, V> TestEviction for Fifo<K, V>
    where
        K: Key + Clone,
        V: Value + Clone,
    {
        fn dump(&self) -> Vec<NonNull<Record<Self>>> {
            self.queue.iter_ptr().collect_vec()
        }
    }

    type TestFifo = Fifo<u64, u64>;

    unsafe fn new_test_fifo_handle_ptr(data: u64) -> NonNull<Record<TestFifo>> {
        let handle = Box::new(Record::new(Data {
            key: data,
            value: data,
            hint: FifoHint,
            state: Default::default(),
            hash: 0,
            weight: 1,
        }));
        NonNull::new_unchecked(Box::into_raw(handle))
    }

    unsafe fn del_test_fifo_handle_ptr(ptr: NonNull<Record<TestFifo>>) {
        let _ = Box::from_raw(ptr.as_ptr());
    }

    #[test]
    fn test_fifo() {
        unsafe {
            let ptrs = (0..8).map(|i| new_test_fifo_handle_ptr(i)).collect_vec();

            let mut fifo = TestFifo::new(100, &FifoConfig {});

            // 0, 1, 2, 3
            fifo.push(ptrs[0]);
            fifo.push(ptrs[1]);
            fifo.push(ptrs[2]);
            fifo.push(ptrs[3]);

            // 2, 3
            let p0 = fifo.pop().unwrap();
            let p1 = fifo.pop().unwrap();
            assert_eq!(ptrs[0], p0);
            assert_eq!(ptrs[1], p1);

            // 2, 3, 4, 5, 6
            fifo.push(ptrs[4]);
            fifo.push(ptrs[5]);
            fifo.push(ptrs[6]);

            // 2, 6
            fifo.remove(ptrs[3]);
            fifo.remove(ptrs[4]);
            fifo.remove(ptrs[5]);

            assert_eq!(fifo.dump(), vec![ptrs[2], ptrs[6]]);

            fifo.clear();

            assert_eq!(fifo.dump(), vec![]);

            for ptr in ptrs {
                del_test_fifo_handle_ptr(ptr);
            }
        }
    }
}
