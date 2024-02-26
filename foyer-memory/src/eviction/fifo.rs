//  Copyright 2024 MrCroxx
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

use std::{hash::BuildHasher, ptr::NonNull};

use foyer_common::removable_queue::{RemovableQueue, Token};

use crate::{
    cache::CacheConfig,
    eviction::Eviction,
    handle::{BaseHandle, Handle},
    Key, Value,
};

pub type FifoContext = ();

pub struct FifoHandle<K, V>
where
    K: Key,
    V: Value,
{
    base: BaseHandle<K, V, FifoContext>,
    token: Option<Token>,
}

impl<K, V> Handle for FifoHandle<K, V>
where
    K: Key,
    V: Value,
{
    type Key = K;
    type Value = V;
    type Context = FifoContext;

    fn new() -> Self {
        Self {
            base: BaseHandle::new(),
            token: None,
        }
    }

    fn init(
        &mut self,
        hash: u64,
        key: Self::Key,
        value: Self::Value,
        charge: usize,
        context: Self::Context,
    ) {
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
pub struct FifoConfig {
    pub default_removable_capacity: usize,
}

pub struct Fifo<K, V>
where
    K: Key,
    V: Value,
{
    queue: RemovableQueue<NonNull<FifoHandle<K, V>>>,
}

impl<K, V> Eviction for Fifo<K, V>
where
    K: Key,
    V: Value,
{
    type Handle = FifoHandle<K, V>;
    type Config = FifoConfig;

    unsafe fn new<S: BuildHasher>(config: &CacheConfig<Self, S>) -> Self
    where
        Self: Sized,
    {
        let capacity = config.eviction_config.default_removable_capacity / config.shards;
        Self {
            queue: RemovableQueue::with_capacity(capacity),
        }
    }

    unsafe fn push(&mut self, mut ptr: NonNull<Self::Handle>) {
        let token = self.queue.push(ptr);
        ptr.as_mut().token = Some(token);
    }

    unsafe fn pop(&mut self) -> Option<NonNull<Self::Handle>> {
        self.queue.pop()
    }

    unsafe fn access(&mut self, _: NonNull<Self::Handle>) {}

    unsafe fn remove(&mut self, mut ptr: NonNull<Self::Handle>) {
        debug_assert!(ptr.as_mut().token.is_some());
        let token = ptr.as_mut().token.take().unwrap_unchecked();
        self.queue.remove(token);
    }

    unsafe fn clear(&mut self) -> Vec<NonNull<Self::Handle>> {
        self.queue.clear()
    }

    unsafe fn len(&self) -> usize {
        self.queue.len()
    }

    unsafe fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

unsafe impl<K, V> Send for Fifo<K, V>
where
    K: Key,
    V: Value,
{
}
unsafe impl<K, V> Sync for Fifo<K, V>
where
    K: Key,
    V: Value,
{
}

#[cfg(test)]
mod tests {
    use ahash::RandomState;
    use itertools::Itertools;

    use super::*;

    type TestFifoHandle = FifoHandle<u64, u64>;
    type TestFifo = Fifo<u64, u64>;

    unsafe fn new_test_fifo_handle_ptr(key: u64, value: u64) -> NonNull<TestFifoHandle> {
        let mut handle = Box::new(TestFifoHandle::new());
        handle.init(0, key, value, 1, ());
        NonNull::new_unchecked(Box::into_raw(handle))
    }

    unsafe fn del_test_fifo_handle_ptr(ptr: NonNull<TestFifoHandle>) {
        let _ = Box::from_raw(ptr.as_ptr());
    }

    #[test]
    fn test_fifo() {
        unsafe {
            let ptrs = (0..8).map(|i| new_test_fifo_handle_ptr(i, i)).collect_vec();

            let config = CacheConfig {
                capacity: 0,
                shards: 1,
                eviction_config: FifoConfig {
                    default_removable_capacity: 4,
                },
                object_pool_capacity: 0,
                hash_builder: RandomState::default(),
            };

            let mut fifo = TestFifo::new(&config);

            fifo.push(ptrs[0]);
            fifo.push(ptrs[1]);
            fifo.push(ptrs[2]);
            fifo.push(ptrs[3]);

            let p0 = fifo.pop().unwrap();
            let p1 = fifo.pop().unwrap();
            assert_eq!(ptrs[0], p0);
            assert_eq!(ptrs[1], p1);

            fifo.push(ptrs[4]);
            fifo.push(ptrs[5]);
            fifo.push(ptrs[6]);

            fifo.remove(ptrs[3]);
            fifo.remove(ptrs[4]);
            fifo.remove(ptrs[5]);

            let p2 = fifo.pop().unwrap();
            let p6 = fifo.pop().unwrap();
            assert_eq!(ptrs[2], p2);
            assert_eq!(ptrs[6], p6);

            for ptr in ptrs {
                del_test_fifo_handle_ptr(ptr);
            }
        }
    }
}
