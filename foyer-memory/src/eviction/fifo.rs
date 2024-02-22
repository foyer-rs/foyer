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

use std::{collections::VecDeque, ptr::NonNull};

use itertools::Itertools;

use crate::{handle::BaseHandle, Eviction, Handle, Key, Value};

pub struct FifoHandle<K, V>
where
    K: Key,
    V: Value,
{
    base: BaseHandle<K, V>,
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

pub struct Fifo<K, V>
where
    K: Key,
    V: Value,
{
    queue: VecDeque<NonNull<FifoHandle<K, V>>>,
}

impl<K, V> Eviction for Fifo<K, V>
where
    K: Key,
    V: Value,
{
    type H = FifoHandle<K, V>;

    fn push(&mut self, ptr: NonNull<Self::H>) {
        self.queue.push_back(ptr);
    }

    fn pop(&mut self) -> Option<NonNull<Self::H>> {
        self.queue.pop_front()
    }

    fn peek(&self) -> Option<NonNull<Self::H>> {
        self.queue.front().copied()
    }

    fn access(&mut self, _: NonNull<Self::H>) {}

    fn remove(&mut self, _: NonNull<Self::H>) {}

    fn clear(&mut self) -> Vec<NonNull<Self::H>> {
        self.queue.drain(..).collect_vec()
    }

    fn is_empty(&self) -> bool {
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
