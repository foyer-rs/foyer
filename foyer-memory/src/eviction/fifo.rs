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

pub struct FifoHandle<K, V>
where
    K: Key,
    V: Value,
{
    base: BaseHandle<K, V>,
    pos: usize,
}

impl<K, V> Handle for FifoHandle<K, V>
where
    K: Key,
    V: Value,
{
    type K = K;
    type V = V;

    fn new() -> Self {
        Self {
            base: BaseHandle::new(),
            pos: usize::MAX,
        }
    }

    fn init(&mut self, hash: u64, key: Self::K, value: Self::V, charge: usize) {
        self.base.init(hash, key, value, charge);
    }

    fn base(&self) -> &BaseHandle<Self::K, Self::V> {
        &self.base
    }

    fn base_mut(&mut self) -> &mut BaseHandle<Self::K, Self::V> {
        &mut self.base
    }
}

#[derive(Debug, Clone)]
pub struct FifoConfig {
    pub default_capacity: usize,
}

pub struct Fifo<K, V>
where
    K: Key,
    V: Value,
{
    queue: Box<[Option<NonNull<FifoHandle<K, V>>>]>,
    head: usize,
    tail: usize,
    len: usize,
    capacity: usize,
}

impl<K, V> Fifo<K, V>
where
    K: Key,
    V: Value,
{
    fn new(config: FifoConfig) -> Self {
        Self {
            queue: vec![None; config.default_capacity].into_boxed_slice(),
            head: 0,
            tail: 0,
            len: 0,
            capacity: config.default_capacity,
        }
    }

    #[inline(always)]
    fn usage(&self) -> usize {
        self.tail - self.head
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.len
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline(always)]
    fn capacity(&self) -> usize {
        self.capacity
    }

    unsafe fn push(&mut self, mut ptr: NonNull<FifoHandle<K, V>>) {
        if self.usage() == self.capacity() {
            self.grow();
        }

        debug_assert_ne!(self.usage(), self.capacity());

        let pos = self.tail % self.capacity;
        ptr.as_mut().pos = pos;
        self.queue[pos] = Some(ptr);

        self.tail += 1;
        self.len += 1;
    }

    unsafe fn pop(&mut self) -> Option<NonNull<FifoHandle<K, V>>> {
        let mut res = None;

        while res.is_none() && self.usage() > 0 {
            let pos = self.head % self.capacity;
            res = self.queue[pos].take();
            self.head += 1;
        }

        while self.usage() > 0
            && let pos = self.head % self.capacity
            && self.queue[pos].is_none()
        {
            self.head += 1;
        }

        if let Some(mut ptr) = res {
            ptr.as_mut().pos = usize::MAX;
            self.len -= 1;
        }

        res
    }

    /// Take item at `pos`. The slot will be lazily released.
    ///
    /// # Safety
    ///
    /// Item at `pos` must be some.
    unsafe fn remove(&mut self, pos: usize) -> NonNull<FifoHandle<K, V>> {
        debug_assert!(self.queue[pos].is_some());
        self.len -= 1;
        self.queue[pos].take().unwrap_unchecked()
    }

    unsafe fn grow(&mut self) {
        let capacity = self.capacity * 2;
        let mut queue = vec![None; capacity].into_boxed_slice();
        let usage = self.usage();

        if usage == 0 {
            debug_assert_eq!(self.len, 0);
        } else {
            let phead = self.head % self.capacity;
            let ptail = self.tail % self.capacity;

            if phead < ptail {
                //          ↓ phead            ↓ ptail
                // * [ ][ ][x][x][x][x][x][x][ ][ ][ ][ ]
                queue[0..ptail - phead].copy_from_slice(&self.queue[phead..ptail]);
            } else {
                //          ↓ ptail            ↓ phead
                // * [x][x][ ][ ][ ][ ][ ][ ][x][x][x][x]
                //
                //          ↓ ptail & phead
                // * [x][x][x][x][x][x][x][x][x][x][x][x]
                queue[0..self.capacity - phead].copy_from_slice(&self.queue[phead..self.capacity]);
                queue[self.capacity - phead..self.capacity - phead + ptail]
                    .copy_from_slice(&self.queue[0..ptail]);
            }
        }

        self.queue = queue;
        self.capacity = capacity;
        self.head = 0;
        self.tail = usage;

        for (pos, item) in self.queue.iter_mut().enumerate() {
            if let Some(item) = item {
                item.as_mut().pos = pos;
            }
        }
    }

    unsafe fn clear(&mut self) -> Vec<NonNull<FifoHandle<K, V>>> {
        let mut res = vec![];
        for pos in self.head..self.tail {
            let pos = pos % self.capacity;
            if let Some(item) = self.queue[pos].take() {
                res.push(item);
            }
        }

        self.len = 0;
        self.head = 0;
        self.tail = 0;

        res
    }
}

impl<K, V> Eviction for Fifo<K, V>
where
    K: Key,
    V: Value,
{
    type H = FifoHandle<K, V>;
    type C = FifoConfig;

    fn new(config: Self::C) -> Self {
        Self::new(config)
    }

    unsafe fn push(&mut self, ptr: NonNull<Self::H>) {
        self.push(ptr);
    }

    unsafe fn pop(&mut self) -> Option<NonNull<Self::H>> {
        self.pop()
    }

    unsafe fn access(&mut self, _: NonNull<Self::H>) {}

    unsafe fn remove(&mut self, ptr: NonNull<Self::H>) {
        let mut p = self.remove(ptr.as_ref().pos);
        debug_assert_eq!(ptr, p);
        p.as_mut().pos = usize::MAX;
    }

    unsafe fn clear(&mut self) -> Vec<NonNull<Self::H>> {
        self.clear()
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
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
    use itertools::Itertools;

    use super::*;

    type TestFifoHandle = FifoHandle<u64, u64>;
    type TestFifo = Fifo<u64, u64>;

    unsafe fn new_test_fifo_handle_ptr(key: u64, value: u64) -> NonNull<TestFifoHandle> {
        let mut handle = Box::new(TestFifoHandle::new());
        handle.init(0, key, value, 0);
        NonNull::new_unchecked(Box::into_raw(handle))
    }

    unsafe fn del_test_fifo_handle_ptr(ptr: NonNull<TestFifoHandle>) {
        let _ = Box::from_raw(ptr.as_ptr());
    }

    #[test]
    fn test_fifo() {
        unsafe {
            let ptrs = (0..16)
                .map(|i| new_test_fifo_handle_ptr(i, i))
                .collect_vec();

            let config = FifoConfig {
                default_capacity: 4,
            };

            let mut fifo = TestFifo::new(config);
            assert_eq!(fifo.capacity(), 4);
            assert_eq!(fifo.len(), 0);
            assert_eq!(fifo.usage(), 0);

            fifo.push(ptrs[0]);
            fifo.push(ptrs[1]);
            fifo.push(ptrs[2]);
            fifo.push(ptrs[3]);
            assert_eq!(fifo.capacity(), 4);
            assert_eq!(fifo.len(), 4);
            assert_eq!(fifo.usage(), 4);

            let p0 = fifo.pop().unwrap();
            let p1 = fifo.pop().unwrap();
            assert_eq!(ptrs[0], p0);
            assert_eq!(ptrs[1], p1);
            assert_eq!(fifo.capacity(), 4);
            assert_eq!(fifo.len(), 2);
            assert_eq!(fifo.usage(), 2);

            fifo.push(ptrs[4]);
            fifo.push(ptrs[5]);
            fifo.push(ptrs[6]);
            assert_eq!(fifo.capacity(), 8);
            assert_eq!(fifo.len(), 5);
            assert_eq!(fifo.usage(), 5);

            let p3 = fifo.remove(ptrs[3].as_ref().pos);
            let p4 = fifo.remove(ptrs[4].as_ref().pos);
            let p5 = fifo.remove(ptrs[5].as_ref().pos);
            assert_eq!(ptrs[3], p3);
            assert_eq!(ptrs[4], p4);
            assert_eq!(ptrs[5], p5);
            assert_eq!(fifo.capacity(), 8);
            assert_eq!(fifo.len(), 2);
            assert_eq!(fifo.usage(), 5);

            let p2 = fifo.pop().unwrap();
            assert_eq!(ptrs[2], p2);
            assert_eq!(fifo.capacity(), 8);
            assert_eq!(fifo.len(), 1);
            assert_eq!(fifo.usage(), 1);

            for ptr in ptrs {
                del_test_fifo_handle_ptr(ptr);
            }
        }
    }
}
