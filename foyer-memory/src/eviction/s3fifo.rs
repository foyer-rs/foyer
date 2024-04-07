//  Copyright 2024 Foyer Project Authors
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

use std::{fmt::Debug, ptr::NonNull};

use foyer_intrusive::{
    collections::dlist::{Dlist, DlistLink},
    intrusive_adapter,
};

use crate::{
    eviction::Eviction,
    handle::{BaseHandle, Handle},
    CacheContext, Key, Value,
};

#[derive(Debug, Clone)]
pub struct S3FifoContext;

impl From<CacheContext> for S3FifoContext {
    fn from(_: CacheContext) -> Self {
        Self
    }
}

impl From<S3FifoContext> for CacheContext {
    fn from(_: S3FifoContext) -> Self {
        CacheContext::Default
    }
}

enum Queue {
    None,
    Main,
    Small,
}

pub struct S3FifoHandle<K, V>
where
    K: Key,
    V: Value,
{
    link: DlistLink,
    base: BaseHandle<K, V, S3FifoContext>,
    count: u8,
    queue: Queue,
}

impl<K, V> Debug for S3FifoHandle<K, V>
where
    K: Key,
    V: Value,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3FifoHandle").finish()
    }
}

intrusive_adapter! { S3FifoHandleDlistAdapter<K, V> = NonNull<S3FifoHandle<K, V>>: S3FifoHandle<K, V> { link: DlistLink } where K: Key, V: Value }

impl<K, V> S3FifoHandle<K, V>
where
    K: Key,
    V: Value,
{
    pub fn inc(&mut self) {
        self.count = std::cmp::min(self.count + 1, 3);
    }

    pub fn dec(&mut self) {
        self.count = self.count.saturating_sub(1);
    }
}

impl<K, V> Handle for S3FifoHandle<K, V>
where
    K: Key,
    V: Value,
{
    type Key = K;
    type Value = V;
    type Context = S3FifoContext;

    fn new() -> Self {
        Self {
            link: DlistLink::default(),
            count: 0,
            base: BaseHandle::new(),
            queue: Queue::None,
        }
    }

    fn init(&mut self, hash: u64, key: Self::Key, value: Self::Value, charge: usize, context: Self::Context) {
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
pub struct S3FifoConfig {
    pub small_queue_capacity_ratio: f64,
}

use std::{collections::HashMap, hash::Hash};

#[derive(Clone, Debug)]
struct GhostEntry<T> {
    fingerprint: u32,
    insertion_time: usize,
    key: T,
}

struct GhostQueue<T> {
    hashtable: HashMap<u32, GhostEntry<T>>,
    current_time: usize,
    max_size: usize,
}

impl<T: Hash + Eq + Clone> GhostQueue<T> {
    fn new(max_size: usize) -> GhostQueue<T> {
        GhostQueue {
            hashtable: HashMap::new(),
            current_time: 0,
            max_size,
        }
    }

    fn hash_key(&self, key: &T) -> u32 {
        use std::{collections::hash_map::DefaultHasher, hash::Hasher};

        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish() as u32
    }

    fn insert(&mut self, key: T) {
        let fingerprint = self.hash_key(&key);
        let entry = GhostEntry {
            fingerprint,
            insertion_time: self.current_time,
            key: key.clone(),
        };

        self.hashtable.insert(fingerprint, entry);
        self.current_time += 1;
        self.evict_old_entries();
    }

    fn evict_old_entries(&mut self) {
        let min_time = if self.current_time > self.max_size {
            self.current_time - self.max_size
        } else {
            0
        };

        self.hashtable.retain(|_, entry| entry.insertion_time >= min_time);
    }

    fn retrieve(&mut self, key: &T) -> Option<&GhostEntry<T>> {
        let fingerprint = self.hash_key(key);
        self.hashtable.get(&fingerprint)
    }
}

pub struct S3Fifo<K, V>
where
    K: Key,
    V: Value,
{
    small_queue: Dlist<S3FifoHandleDlistAdapter<K, V>>,
    main_queue: Dlist<S3FifoHandleDlistAdapter<K, V>>,

    _capacity: usize,
    small_capacity: usize,

    small_used: usize,
    main_used: usize,

    ghost_queue: GhostQueue<K>,
}

impl<K, V> S3Fifo<K, V>
where
    K: Key,
    V: Value,
{
    unsafe fn evict(&mut self) -> Option<NonNull<<S3Fifo<K, V> as Eviction>::Handle>> {
        if self.small_used > self.small_capacity
            && let Some(ptr) = self.evict_small()
        {
            Some(ptr)
        } else {
            self.evict_main()
        }
    }

    unsafe fn evict_small(&mut self) -> Option<NonNull<<S3Fifo<K, V> as Eviction>::Handle>> {
        while let Some(mut ptr) = self.small_queue.pop_front() {
            let handle = ptr.as_mut();
            if handle.count > 1 {
                self.main_queue.push_back(ptr);
                handle.queue = Queue::Main;
                self.small_used -= 1;
                self.main_used += 1;
            } else {
                handle.queue = Queue::None;
                self.small_used -= 1;
                let key = handle.base().key().clone();
                self.ghost_queue.insert(key);
                return Some(ptr);
            }
        }
        None
    }

    unsafe fn evict_main(&mut self) -> Option<NonNull<<S3Fifo<K, V> as Eviction>::Handle>> {
        while let Some(mut ptr) = self.main_queue.pop_front() {
            let handle = ptr.as_mut();
            if handle.count > 0 {
                self.main_queue.push_back(ptr);
                handle.dec();
            } else {
                handle.queue = Queue::None;
                self.main_used -= 1;
                return Some(ptr);
            }
        }
        None
    }
}

impl<K, V> Eviction for S3Fifo<K, V>
where
    K: Key,
    V: Value,
{
    type Handle = S3FifoHandle<K, V>;
    type Config = S3FifoConfig;

    unsafe fn new(capacity: usize, config: &Self::Config) -> Self
    where
        Self: Sized,
    {
        let small_capacity = (capacity as f64 * config.small_queue_capacity_ratio) as usize;
        Self {
            small_queue: Dlist::new(),
            main_queue: Dlist::new(),
            _capacity: capacity,
            small_capacity,
            small_used: 0,
            main_used: 0,
            ghost_queue: GhostQueue::new(small_capacity * 1),
        }
    }

    unsafe fn push(&mut self, mut ptr: NonNull<Self::Handle>) {
        let handle = ptr.as_mut();

        let key = handle.base().key().clone();

        if self.ghost_queue.retrieve(&key).is_some() {
            handle.queue = Queue::Main;
            self.main_queue.push_back(ptr);
            self.main_used += 1;
        } else {
            self.small_queue.push_back(ptr);
            handle.queue = Queue::Small;
            self.small_used += 1;
        }

        handle.base_mut().set_in_eviction(true);
    }

    unsafe fn pop(&mut self) -> Option<NonNull<Self::Handle>> {
        if let Some(mut ptr) = self.evict() {
            let handle = ptr.as_mut();
            // `handle.queue` has already been set with `evict()`
            handle.base_mut().set_in_eviction(false);
            Some(ptr)
        } else {
            None
        }
    }

    unsafe fn reinsert(&mut self, _: NonNull<Self::Handle>) {}

    unsafe fn access(&mut self, ptr: NonNull<Self::Handle>) {
        let mut ptr = ptr;
        ptr.as_mut().inc();
    }

    unsafe fn remove(&mut self, mut ptr: NonNull<Self::Handle>) {
        let handle = ptr.as_mut();

        match handle.queue {
            Queue::None => unreachable!(),
            Queue::Main => {
                let p = self
                    .main_queue
                    .iter_mut_from_raw(ptr.as_mut().link.raw())
                    .remove()
                    .unwrap_unchecked();
                debug_assert_eq!(p, ptr);

                handle.queue = Queue::None;
                handle.base_mut().set_in_eviction(false);

                self.main_used -= 1;
            }
            Queue::Small => {
                let p = self
                    .small_queue
                    .iter_mut_from_raw(ptr.as_mut().link.raw())
                    .remove()
                    .unwrap_unchecked();
                debug_assert_eq!(p, ptr);

                handle.queue = Queue::None;
                handle.base_mut().set_in_eviction(false);

                self.small_used -= 1;
            }
        }
    }

    unsafe fn clear(&mut self) -> Vec<NonNull<Self::Handle>> {
        let mut res = Vec::with_capacity(self.len());
        while let Some(mut ptr) = self.small_queue.pop_front() {
            let handle = ptr.as_mut();
            handle.base_mut().set_in_eviction(false);
            handle.queue = Queue::None;
            res.push(ptr);
        }
        while let Some(mut ptr) = self.main_queue.pop_front() {
            let handle = ptr.as_mut();
            handle.base_mut().set_in_eviction(false);
            handle.queue = Queue::None;
            res.push(ptr);
        }
        res
    }

    unsafe fn len(&self) -> usize {
        // if self.small_used >= self.small_capacity {
        //     return self._capacity;
        // }

        // if self.main_used >= self._capacity - self.small_capacity {
        //     return self._capacity;
        // }
        self.small_queue.len() + self.main_queue.len()
    }

    unsafe fn is_empty(&self) -> bool {
        self.small_queue.is_empty() && self.main_queue.is_empty()
    }
}

unsafe impl<K, V> Send for S3Fifo<K, V>
where
    K: Key,
    V: Value,
{
}
unsafe impl<K, V> Sync for S3Fifo<K, V>
where
    K: Key,
    V: Value,
{
}

#[cfg(test)]
mod tests {}
