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

#[derive(Debug)]
pub struct LruConfig {
    /// The ratio of the high priority pool occupied.
    ///
    /// [`Lru`] guarantees that the high priority charges are always as larger as
    /// but no larger that the capacity * high priority pool ratio.
    ///
    /// # Panic
    ///
    /// Panics if the value is not in [0, 1.0].
    pub high_priority_pool_ratio: f64,
}

#[derive(Debug)]
pub enum LruContext {
    HighPriority,
    LowPriority,
}

impl Default for LruContext {
    fn default() -> Self {
        Self::HighPriority
    }
}

pub struct LruHandle<K, V>
where
    K: Key,
    V: Value,
{
    prev: Option<NonNull<LruHandle<K, V>>>,
    next: Option<NonNull<LruHandle<K, V>>>,

    base: BaseHandle<K, V, LruContext>,

    in_high_priority_pool: bool,
}

impl<K, V> Handle for LruHandle<K, V>
where
    K: Key,
    V: Value,
{
    type Key = K;
    type Value = V;
    type Context = LruContext;

    fn new() -> Self {
        Self {
            prev: None,
            next: None,
            base: BaseHandle::new(),
            in_high_priority_pool: false,
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
        self.base.init(hash, key, value, charge, context)
    }

    fn base(&self) -> &BaseHandle<Self::Key, Self::Value, Self::Context> {
        &self.base
    }

    fn base_mut(&mut self) -> &mut BaseHandle<Self::Key, Self::Value, Self::Context> {
        &mut self.base
    }
}

unsafe impl<K, V> Send for LruHandle<K, V>
where
    K: Key,
    V: Value,
{
}
unsafe impl<K, V> Sync for LruHandle<K, V>
where
    K: Key,
    V: Value,
{
}

pub struct Lru<K, V>
where
    K: Key,
    V: Value,
{
    /// Dummy head node of a ring linked list.
    head: Box<LruHandle<K, V>>,
    /// The pointer to the last element in the high priority pool.
    /// The low priority element will be inserted after the pointer initially.
    low_priority_head: NonNull<LruHandle<K, V>>,

    charges: usize,
    high_priority_charges: usize,

    high_priority_charges_capacity: usize,

    len: usize,
}

impl<K, V> Lru<K, V>
where
    K: Key,
    V: Value,
{
    unsafe fn may_overflow_high_priority_pool(&mut self) {
        while self.high_priority_charges > self.high_priority_charges_capacity {
            // overflow last entry in high priority pool to low priority pool
            self.low_priority_head.as_mut().in_high_priority_pool = false;
            self.high_priority_charges -= self.low_priority_head.as_ref().base().charge();
            self.low_priority_head = self.low_priority_head.as_ref().prev.unwrap_unchecked();
        }
    }

    #[inline(always)]
    unsafe fn is_head_ptr(&self, ptr: NonNull<LruHandle<K, V>>) -> bool {
        std::ptr::eq(self.head.as_ref(), ptr.as_ptr())
    }

    #[inline(always)]
    unsafe fn head_nonnull_ptr(&mut self) -> NonNull<LruHandle<K, V>> {
        NonNull::new_unchecked(self.head.as_mut() as *mut _)
    }

    #[inline(always)]
    unsafe fn insert_ptr_after_head(&mut self, ptr: NonNull<LruHandle<K, V>>) {
        let pos = self.head_nonnull_ptr();
        self.insert_ptr_after(ptr, pos);
    }

    #[inline(always)]
    unsafe fn insert_ptr_after_low_priority_head(&mut self, ptr: NonNull<LruHandle<K, V>>) {
        let pos = self.low_priority_head;
        self.insert_ptr_after(ptr, pos);
    }

    #[inline(always)]
    unsafe fn remove_ptr_before_head(&mut self) -> Option<NonNull<LruHandle<K, V>>> {
        if self.is_empty() {
            return None;
        }

        let ptr = self.head.prev.unwrap_unchecked();
        debug_assert_ne!(ptr, self.head_nonnull_ptr());

        self.remove_ptr(ptr);

        Some(ptr)
    }

    #[inline(always)]
    unsafe fn insert_ptr_after(
        &mut self,
        mut ptr: NonNull<LruHandle<K, V>>,
        mut pos: NonNull<LruHandle<K, V>>,
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
    unsafe fn remove_ptr(&mut self, mut ptr: NonNull<LruHandle<K, V>>) {
        let handle = ptr.as_mut();

        debug_assert!(handle.prev.is_some());
        debug_assert!(handle.next.is_some());

        handle.prev.unwrap_unchecked().as_mut().next = handle.next;
        handle.next.unwrap_unchecked().as_mut().prev = handle.prev;

        handle.next = None;
        handle.prev = None;
    }
}

impl<K, V> Eviction for Lru<K, V>
where
    K: Key,
    V: Value,
{
    type Handle = LruHandle<K, V>;
    type Config = LruConfig;

    unsafe fn new<S: BuildHasher + Send + Sync + 'static>(config: &CacheConfig<Self, S>) -> Self
    where
        Self: Sized,
    {
        assert!(
            config.eviction_config.high_priority_pool_ratio >= 0.0
                && config.eviction_config.high_priority_pool_ratio <= 1.0,
            "high_priority_pool_ratio_percentage must be in [0, 100], given: {}",
            config.eviction_config.high_priority_pool_ratio
        );

        let high_priority_charges_capacity = config.capacity as f64
            * config.eviction_config.high_priority_pool_ratio
            / config.shards as f64;
        let high_priority_charges_capacity = high_priority_charges_capacity as usize;

        let mut head = Box::new(LruHandle::new());
        let ptr = NonNull::new_unchecked(head.as_mut() as *mut _);

        head.prev = Some(ptr);
        head.next = Some(ptr);
        head.in_high_priority_pool = true;

        let low_priority_head = ptr;

        Self {
            head,
            low_priority_head,
            charges: 0,
            high_priority_charges: 0,
            high_priority_charges_capacity,
            len: 0,
        }
    }

    unsafe fn push(&mut self, mut ptr: NonNull<Self::Handle>) {
        let handle = ptr.as_mut();

        debug_assert!(handle.next.is_none());
        debug_assert!(handle.prev.is_none());

        self.charges += handle.base().charge();
        self.len += 1;

        match handle.base().context() {
            LruContext::HighPriority => {
                handle.in_high_priority_pool = true;
                self.insert_ptr_after_head(ptr);

                self.high_priority_charges += handle.base().charge();

                if self.is_head_ptr(self.low_priority_head) {
                    self.low_priority_head = ptr;
                }

                self.may_overflow_high_priority_pool();
            }
            LruContext::LowPriority => {
                handle.in_high_priority_pool = false;
                self.insert_ptr_after_low_priority_head(ptr);
            }
        }
        handle.base_mut().set_in_eviction(true);
    }

    unsafe fn pop(&mut self) -> Option<NonNull<Self::Handle>> {
        let mut ptr = self.remove_ptr_before_head()?;
        let handle = ptr.as_mut();

        debug_assert!(handle.next.is_none());
        debug_assert!(handle.prev.is_none());

        if handle.in_high_priority_pool {
            self.high_priority_charges -= handle.base.charge();
        }
        self.charges -= handle.base.charge();
        self.len -= 1;
        handle.base_mut().set_in_eviction(false);

        Some(ptr)
    }

    unsafe fn access(&mut self, _: NonNull<Self::Handle>) {}

    unsafe fn reinsert(&mut self, mut ptr: NonNull<Self::Handle>) -> bool {
        let handle = ptr.as_mut();

        if handle.base_mut().is_in_eviction() {
            self.remove(ptr);
            self.push(ptr);
            false
        } else {
            self.push(ptr);
            true
        }
    }

    unsafe fn remove(&mut self, mut ptr: NonNull<Self::Handle>) {
        self.remove_ptr(ptr);
        let handle = ptr.as_mut();

        debug_assert!(handle.next.is_none());
        debug_assert!(handle.prev.is_none());

        if handle.in_high_priority_pool {
            self.high_priority_charges -= handle.base.charge();
        }
        self.charges -= handle.base.charge();
        self.len -= 1;
        handle.base_mut().set_in_eviction(false);
    }

    unsafe fn clear(&mut self) -> Vec<NonNull<Self::Handle>> {
        let mut res = Vec::with_capacity(self.len);

        while !self.is_empty() {
            let ptr = self.head.prev.unwrap_unchecked();
            debug_assert_ne!(ptr, self.head_nonnull_ptr());
            self.remove(ptr);
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

unsafe impl<K, V> Send for Lru<K, V>
where
    K: Key,
    V: Value,
{
}
unsafe impl<K, V> Sync for Lru<K, V>
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

    type TestLruHandle = LruHandle<u64, u64>;
    type TestLru = Lru<u64, u64>;

    unsafe fn new_test_lru_handle_ptr(
        key: u64,
        value: u64,
        context: LruContext,
    ) -> NonNull<TestLruHandle> {
        let mut handle = Box::new(TestLruHandle::new());
        handle.init(0, key, value, 1, context);
        NonNull::new_unchecked(Box::into_raw(handle))
    }

    unsafe fn del_test_lru_handle_ptr(ptr: NonNull<TestLruHandle>) {
        let _ = Box::from_raw(ptr.as_ptr());
    }

    #[test]
    fn test_lru() {
        unsafe {
            let ptrs = (0..20)
                .map(|i| {
                    new_test_lru_handle_ptr(
                        i,
                        i,
                        if i < 10 {
                            LruContext::HighPriority
                        } else {
                            LruContext::LowPriority
                        },
                    )
                })
                .collect_vec();

            let config = CacheConfig {
                capacity: 8,
                shards: 1,
                eviction_config: LruConfig {
                    high_priority_pool_ratio: 0.5,
                },
                object_pool_capacity: 0,
                hash_builder: RandomState::default(),
            };
            let mut lru = TestLru::new(&config);

            assert_eq!(lru.high_priority_charges_capacity, 4);

            // [0, 1, 2, 3]
            lru.push(ptrs[0]);
            lru.push(ptrs[1]);
            lru.push(ptrs[2]);
            lru.push(ptrs[3]);
            assert_eq!(lru.len, 4);
            assert_eq!(lru.charges, 4);
            assert_eq!(lru.high_priority_charges, 4);
            assert_eq!(lru.low_priority_head, ptrs[0]);

            // 0, [1, 2, 3, 4]
            lru.push(ptrs[4]);
            assert_eq!(lru.len, 5);
            assert_eq!(lru.charges, 5);
            assert_eq!(lru.high_priority_charges, 4);
            assert_eq!(lru.low_priority_head, ptrs[1]);

            // 0, 10, [1, 2, 3, 4]
            lru.push(ptrs[10]);
            assert_eq!(lru.len, 6);
            assert_eq!(lru.charges, 6);
            assert_eq!(lru.high_priority_charges, 4);
            assert_eq!(lru.low_priority_head, ptrs[1]);

            // 10, [1, 2, 3, 4]
            let p0 = lru.pop().unwrap();
            assert_eq!(ptrs[0], p0);
            assert_eq!(lru.len, 5);
            assert_eq!(lru.charges, 5);
            assert_eq!(lru.high_priority_charges, 4);
            assert_eq!(lru.low_priority_head, ptrs[1]);

            // 10, [1, 3, 4]
            lru.remove(ptrs[2]);
            assert_eq!(lru.len, 4);
            assert_eq!(lru.charges, 4);
            assert_eq!(lru.high_priority_charges, 3);
            assert_eq!(lru.low_priority_head, ptrs[1]);

            // 10, 11, [1, 3, 4]
            lru.push(ptrs[11]);
            assert_eq!(lru.len, 5);
            assert_eq!(lru.charges, 5);
            assert_eq!(lru.high_priority_charges, 3);
            assert_eq!(lru.low_priority_head, ptrs[1]);

            // 10, 11, 1, [3, 4, 5, 6]
            lru.push(ptrs[5]);
            lru.push(ptrs[6]);
            assert_eq!(lru.len, 7);
            assert_eq!(lru.charges, 7);
            assert_eq!(lru.high_priority_charges, 4);
            assert_eq!(lru.low_priority_head, ptrs[3]);

            // 10, 11, 1, 3, [4, 5, 6, 0]
            lru.push(ptrs[0]);
            assert_eq!(lru.len, 8);
            assert_eq!(lru.charges, 8);
            assert_eq!(lru.high_priority_charges, 4);
            assert_eq!(lru.low_priority_head, ptrs[4]);

            let ps = lru.clear();
            assert_eq!(ps, [10, 11, 1, 3, 4, 5, 6, 0].map(|i| ptrs[i]));

            for ptr in ptrs {
                del_test_lru_handle_ptr(ptr);
            }
        }
    }
}
