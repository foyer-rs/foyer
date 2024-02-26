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

use std::ptr::NonNull;

use crate::{
    eviction::Eviction,
    handle::{BaseHandle, Handle},
    Key, Value,
};

#[derive(Debug, Clone)]
pub struct LruConfig {
    /// The ratio (percentage) of the high priority pool occupied.
    ///
    /// [`Lru`] guarantees that the high priority charges are always as larger as
    /// but no larger that the total charges * high priority pool ratio.
    ///
    /// # Panic
    ///
    /// Panics if the value is not in [0, 100].
    pub high_priority_pool_ratio_percentage: usize,
}

#[derive(Debug)]
pub enum LruContext {
    HighPriority,
    LowPriority,
}

enum NeedUpdatePointer {
    NoNeed,
    Forward,
    Backward,
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
    /// Dummy header node of a ring linked list.
    head: Box<LruHandle<K, V>>,
    /// The pointer to the last element in the high priority pool.
    /// The low priority element will be inserted after the pointer initially.
    low_priority_head: NonNull<LruHandle<K, V>>,

    charges: usize,
    high_priority_charges: usize,

    high_priority_pool_ratio_percentage: usize,

    len: usize,
}

impl<K, V> Lru<K, V>
where
    K: Key,
    V: Value,
{
    unsafe fn update_low_priority_head(&mut self) {
        loop {
            match self.need_update_low_priority_head() {
                NeedUpdatePointer::NoNeed => return,
                NeedUpdatePointer::Forward => {
                    debug_assert!(
                        !self.is_head_ptr(self.low_priority_head.as_ref().next.unwrap_unchecked())
                    );

                    self.low_priority_head =
                        self.low_priority_head.as_mut().next.unwrap_unchecked();
                    self.high_priority_charges += self.low_priority_head.as_ref().base().charge();
                    self.low_priority_head.as_mut().in_high_priority_pool = true;
                }
                NeedUpdatePointer::Backward => {
                    debug_assert!(!self.is_head_ptr(self.low_priority_head));

                    self.low_priority_head.as_mut().in_high_priority_pool = false;
                    self.high_priority_charges -= self.low_priority_head.as_ref().base().charge();
                    self.low_priority_head =
                        self.low_priority_head.as_ref().prev.unwrap_unchecked();
                }
            }
        }
    }

    #[inline(always)]
    unsafe fn need_update_low_priority_head(&self) -> NeedUpdatePointer {
        if self.is_empty() {
            return NeedUpdatePointer::NoNeed;
        };

        // if the following conditions are met, move the insert point forward:
        //
        // 1. high priority pool size is lower than threshold
        // 2. there is node after the insert point (the next pointer does not point to head)
        // 3. high priority pool size will not exceed the threshold with the next node
        if self.high_priority_charges * 100
            < self.charges * self.high_priority_pool_ratio_percentage
            && let next = self.low_priority_head.as_ref().next.unwrap_unchecked()
            && !self.is_head_ptr(next)
            && (self.high_priority_charges + next.as_ref().base().charge())
                < self.charges * self.high_priority_pool_ratio_percentage
        {
            return NeedUpdatePointer::Forward;
        }

        // if the following conditions are met, move the insert point backward:
        //
        // 1. high priority pool size is higher than threshold
        // 2. there is node before the insert point (the pointer does not point to head)
        if self.high_priority_charges * 100
            > self.charges * self.high_priority_pool_ratio_percentage
            && !self.is_head_ptr(self.low_priority_head)
        {
            return NeedUpdatePointer::Backward;
        }

        NeedUpdatePointer::NoNeed
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

    unsafe fn new(config: Self::Config) -> Self {
        assert!(
            config.high_priority_pool_ratio_percentage <= 100,
            "high_priority_pool_ratio_percentage must be in [0, 100], given: {}",
            config.high_priority_pool_ratio_percentage
        );

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
            high_priority_pool_ratio_percentage: config.high_priority_pool_ratio_percentage,
            len: 0,
        }
    }

    unsafe fn push(&mut self, mut ptr: NonNull<Self::Handle>) {
        let handle = ptr.as_mut();

        debug_assert!(handle.next.is_none());
        debug_assert!(handle.prev.is_none());

        match handle.base().context() {
            LruContext::HighPriority => {
                handle.in_high_priority_pool = true;
                self.insert_ptr_after_head(ptr);
                self.high_priority_charges += handle.base().charge();
            }
            LruContext::LowPriority => {
                self.insert_ptr_after_low_priority_head(ptr);
            }
        }

        self.charges += handle.base().charge();

        self.update_low_priority_head();

        self.len += 1;
    }

    unsafe fn pop(&mut self) -> Option<NonNull<Self::Handle>> {
        let ptr = self.remove_ptr_before_head()?;
        let handle = ptr.as_ref();

        debug_assert!(handle.next.is_none());
        debug_assert!(handle.prev.is_none());

        if handle.in_high_priority_pool {
            self.high_priority_charges -= handle.base.charge();
        }
        self.charges -= handle.base.charge();

        self.update_low_priority_head();

        self.len -= 1;

        Some(ptr)
    }

    unsafe fn access(&mut self, _: NonNull<Self::Handle>) {}

    unsafe fn remove(&mut self, ptr: NonNull<Self::Handle>) {
        self.remove_ptr(ptr);

        let handle = ptr.as_ref();

        debug_assert!(handle.next.is_none());
        debug_assert!(handle.prev.is_none());

        if handle.in_high_priority_pool {
            self.high_priority_charges -= handle.base.charge();
        }
        self.charges -= handle.base.charge();

        self.update_low_priority_head();

        self.len -= 1;
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
        let res = self.is_head_ptr(self.head.next.unwrap_unchecked());
        debug_assert_eq!(self.len == 0, res);
        res
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
mod tests {}
