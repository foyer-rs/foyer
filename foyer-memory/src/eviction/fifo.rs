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
    prev: Option<NonNull<FifoHandle<K, V>>>,
    next: Option<NonNull<FifoHandle<K, V>>>,

    base: BaseHandle<K, V, FifoContext>,
}

unsafe impl<K, V> Send for FifoHandle<K, V>
where
    K: Key,
    V: Value,
{
}
unsafe impl<K, V> Sync for FifoHandle<K, V>
where
    K: Key,
    V: Value,
{
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
            prev: None,
            next: None,
            base: BaseHandle::new(),
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
pub struct FifoConfig {}

pub struct Fifo<K, V>
where
    K: Key,
    V: Value,
{
    /// Dummy head node of a ring linked list.
    head: Box<FifoHandle<K, V>>,

    len: usize,
}

impl<K, V> Fifo<K, V>
where
    K: Key,
    V: Value,
{
    #[inline(always)]
    unsafe fn insert_ptr_after_head(&mut self, ptr: NonNull<FifoHandle<K, V>>) {
        let pos = self.head_nonnull_ptr();
        self.insert_ptr_after(ptr, pos);
    }

    #[inline(always)]
    unsafe fn insert_ptr_after(
        &mut self,
        mut ptr: NonNull<FifoHandle<K, V>>,
        mut pos: NonNull<FifoHandle<K, V>>,
    ) {
        let handle = ptr.as_mut();
        let phandle = pos.as_mut();

        debug_assert!(handle.prev.is_none());
        debug_assert!(handle.next.is_none());
        debug_assert!(phandle.prev.is_some());
        debug_assert!(phandle.next.is_some());

        handle.prev = Some(pos);
        handle.next = phandle.next;

        phandle.next.unwrap_unchecked().as_mut().prev = Some(ptr);
        phandle.next = Some(ptr);
    }

    #[inline(always)]
    unsafe fn remove_ptr_before_head(&mut self) -> Option<NonNull<FifoHandle<K, V>>> {
        if self.is_empty() {
            return None;
        }

        let ptr = self.head.prev.unwrap_unchecked();
        debug_assert_ne!(ptr, self.head_nonnull_ptr());

        self.remove_ptr(ptr);

        Some(ptr)
    }

    #[inline(always)]
    unsafe fn remove_ptr(&mut self, mut ptr: NonNull<FifoHandle<K, V>>) {
        let handle = ptr.as_mut();

        debug_assert!(handle.prev.is_some());
        debug_assert!(handle.next.is_some());

        handle.prev.unwrap_unchecked().as_mut().next = handle.next;
        handle.next.unwrap_unchecked().as_mut().prev = handle.prev;

        handle.next = None;
        handle.prev = None;
    }

    #[inline(always)]
    unsafe fn head_nonnull_ptr(&mut self) -> NonNull<FifoHandle<K, V>> {
        NonNull::new_unchecked(self.head.as_mut() as *mut _)
    }

    #[inline(always)]
    unsafe fn is_head_ptr(&self, ptr: NonNull<FifoHandle<K, V>>) -> bool {
        std::ptr::eq(self.head.as_ref(), ptr.as_ptr())
    }
}

impl<K, V> Eviction for Fifo<K, V>
where
    K: Key,
    V: Value,
{
    type Handle = FifoHandle<K, V>;
    type Config = FifoConfig;

    unsafe fn new<S: BuildHasher + Send + Sync + 'static>(_: &CacheConfig<Self, S>) -> Self
    where
        Self: Sized,
    {
        let mut head = Box::new(FifoHandle::new());
        let ptr = NonNull::new_unchecked(head.as_mut() as *mut _);
        head.prev = Some(ptr);
        head.next = Some(ptr);
        Self { head, len: 0 }
    }

    unsafe fn push(&mut self, mut ptr: NonNull<Self::Handle>) {
        self.insert_ptr_after_head(ptr);
        self.len += 1;
        ptr.as_mut().base_mut().set_in_eviction(true);
    }

    unsafe fn pop(&mut self) -> Option<NonNull<Self::Handle>> {
        let mut res = self.remove_ptr_before_head();
        if let Some(ptr) = res.as_mut() {
            self.len -= 1;
            ptr.as_mut().base_mut().set_in_eviction(false);
        }
        res
    }

    unsafe fn reinsert(&mut self, _: NonNull<Self::Handle>) -> bool {
        false
    }

    unsafe fn access(&mut self, _: NonNull<Self::Handle>) {}

    unsafe fn remove(&mut self, mut ptr: NonNull<Self::Handle>) {
        self.remove_ptr(ptr);
        self.len -= 1;
        ptr.as_mut().base_mut().set_in_eviction(false);
    }

    unsafe fn clear(&mut self) -> Vec<NonNull<Self::Handle>> {
        let mut res = Vec::with_capacity(self.len());
        while !self.is_empty() {
            let mut ptr = self.remove_ptr_before_head().unwrap_unchecked();
            self.len -= 1;
            ptr.as_mut().base_mut().set_in_eviction(false);
            res.push(ptr);
        }
        res
    }

    unsafe fn len(&self) -> usize {
        self.len
    }

    unsafe fn is_empty(&self) -> bool {
        debug_assert_eq!(
            self.len == 0,
            self.is_head_ptr(self.head.next.unwrap_unchecked())
        );
        self.len == 0
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
                eviction_config: FifoConfig {},
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
