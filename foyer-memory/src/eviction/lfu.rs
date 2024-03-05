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

use std::{fmt::Debug, hash::BuildHasher, ptr::NonNull};

use cmsketch::CMSketchU16;
use foyer_intrusive::{
    collections::dlist::{Dlist, DlistLink},
    core::adapter::Link,
    intrusive_adapter,
};

use crate::{
    cache::CacheConfig,
    eviction::Eviction,
    handle::{BaseHandle, Handle},
    Key, Value,
};

#[derive(Debug, Clone)]
pub struct LfuConfig {
    /// `window` capacity ratio of the total cache capacity.
    ///
    /// Must be in (0, 1).
    ///
    /// Must guarantee `window_capacity_ratio + protected_capacity_ratio < 1`.
    pub window_capacity_ratio: f64,
    /// `protected` capacity ratio of the total cache capacity.
    ///
    /// Must be in (0, 1).
    ///
    /// Must guarantee `window_capacity_ratio + protected_capacity_ratio < 1`.
    pub protected_capacity_ratio: f64,

    pub cmsketch_width: usize,
    pub cmsketch_depth: usize,
    pub cmsketch_decay: usize,
}

pub type LfuContext = ();

#[derive(Debug, PartialEq, Eq)]
enum Queue {
    None,
    Window,
    Probation,
    Protected,
}

pub struct LfuHandle<K, V>
where
    K: Key,
    V: Value,
{
    link: DlistLink,
    base: BaseHandle<K, V, LfuContext>,
    queue: Queue,
}

impl<K, V> Debug for LfuHandle<K, V>
where
    K: Key,
    V: Value,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LfuHandle").finish()
    }
}

intrusive_adapter! { LfuHandleDlistAdapter<K, V> = NonNull<LfuHandle<K, V>>: LfuHandle<K, V> { link: DlistLink } where K: Key, V: Value }

impl<K, V> Handle for LfuHandle<K, V>
where
    K: Key,
    V: Value,
{
    type Key = K;
    type Value = V;
    type Context = LfuContext;

    fn new() -> Self {
        Self {
            link: DlistLink::default(),
            base: BaseHandle::new(),
            queue: Queue::None,
        }
    }

    fn init(&mut self, hash: u64, key: Self::Key, value: Self::Value, charge: usize, context: Self::Context) {
        self.base.init(hash, key, value, charge, context)
    }

    fn base(&self) -> &BaseHandle<Self::Key, Self::Value, Self::Context> {
        &self.base
    }

    fn base_mut(&mut self) -> &mut BaseHandle<Self::Key, Self::Value, Self::Context> {
        &mut self.base
    }
}

unsafe impl<K, V> Send for LfuHandle<K, V>
where
    K: Key,
    V: Value,
{
}
unsafe impl<K, V> Sync for LfuHandle<K, V>
where
    K: Key,
    V: Value,
{
}

/// This implementation is inspired by [Caffeine](https://github.com/ben-manes/caffeine) under Apache License 2.0
///
/// A newcoming and hot entry is kept in `window`.
///
/// When `window` is full, entries from it will overflow to `probation`.
///
/// When a entry in `probation` is accessed, it will be promoted to `protected`.
///
/// When `protected` is full, entries from it will overflow to `probation`.
///
/// When evicting, the entry with a lower frequency from `window` or `probtion` will be evicted first, then from
/// `protected`.
pub struct Lfu<K, V>
where
    K: Key,
    V: Value,
{
    window: Dlist<LfuHandleDlistAdapter<K, V>>,
    probation: Dlist<LfuHandleDlistAdapter<K, V>>,
    protected: Dlist<LfuHandleDlistAdapter<K, V>>,

    window_charges: usize,
    probation_charges: usize,
    protected_charges: usize,

    window_charges_capacity: usize,
    protected_charges_capacity: usize,

    frequencies: CMSketchU16,

    step: usize,
    decay: usize,
}

impl<K, V> Lfu<K, V>
where
    K: Key,
    V: Value,
{
    fn increase_queue_charges(&mut self, handle: &LfuHandle<K, V>) {
        let charges = handle.base().charge();
        match handle.queue {
            Queue::None => unreachable!(),
            Queue::Window => self.window_charges += charges,
            Queue::Probation => self.probation_charges += charges,
            Queue::Protected => self.protected_charges += charges,
        }
    }

    fn decrease_queue_charges(&mut self, handle: &LfuHandle<K, V>) {
        let charges = handle.base().charge();
        match handle.queue {
            Queue::None => unreachable!(),
            Queue::Window => self.window_charges -= charges,
            Queue::Probation => self.probation_charges -= charges,
            Queue::Protected => self.protected_charges -= charges,
        }
    }

    fn update_frequencies(&mut self, hash: u64) {
        self.frequencies.record(hash);
        self.step += 1;
        if self.step >= self.decay {
            self.step = 0;
            self.frequencies.decay(0.5);
        }
    }
}

impl<K, V> Eviction for Lfu<K, V>
where
    K: Key,
    V: Value,
{
    type Handle = LfuHandle<K, V>;
    type Config = LfuConfig;

    unsafe fn new<S: BuildHasher + Send + Sync + 'static>(config: &CacheConfig<Self, S>) -> Self
    where
        Self: Sized,
    {
        assert!(
            config.eviction_config.window_capacity_ratio > 0.0 && config.eviction_config.window_capacity_ratio < 1.0,
            "window_capacity_ratio must be in (0, 1), given: {}",
            config.eviction_config.window_capacity_ratio
        );

        assert!(
            config.eviction_config.protected_capacity_ratio > 0.0
                && config.eviction_config.protected_capacity_ratio < 1.0,
            "protected_capacity_ratio must be in (0, 1), given: {}",
            config.eviction_config.protected_capacity_ratio
        );

        assert!(
            config.eviction_config.window_capacity_ratio + config.eviction_config.protected_capacity_ratio < 1.0,
            "must guarantee: window_capacity_ratio + protected_capacity_ratio < 1, given: {}",
            config.eviction_config.window_capacity_ratio + config.eviction_config.protected_capacity_ratio
        );

        let window_charges_capacity =
            config.capacity as f64 * config.eviction_config.window_capacity_ratio / config.shards as f64;
        let window_charges_capacity = window_charges_capacity as usize;

        let protected_charges_capacity =
            config.capacity as f64 * config.eviction_config.protected_capacity_ratio / config.shards as f64;
        let protected_charges_capacity = protected_charges_capacity as usize;

        let frequencies = CMSketchU16::new_with_size(
            config.eviction_config.cmsketch_width,
            config.eviction_config.cmsketch_width,
        );
        let decay = config.eviction_config.cmsketch_decay;

        Self {
            window: Dlist::new(),
            probation: Dlist::new(),
            protected: Dlist::new(),
            window_charges: 0,
            probation_charges: 0,
            protected_charges: 0,
            window_charges_capacity,
            protected_charges_capacity,
            frequencies,
            step: 0,
            decay,
        }
    }

    unsafe fn push(&mut self, mut ptr: NonNull<Self::Handle>) {
        let handle = ptr.as_mut();

        debug_assert!(!handle.link.is_linked());
        debug_assert!(!handle.base().is_in_eviction());
        debug_assert_eq!(handle.queue, Queue::None);

        self.window.push_back(ptr);
        handle.base_mut().set_in_eviction(true);
        handle.queue = Queue::Window;

        self.increase_queue_charges(handle);
        self.update_frequencies(handle.base().hash());

        // If `window`` charges exceeds the capacity, overflow entry from `window` to `probation`.
        while self.window_charges > self.window_charges_capacity {
            debug_assert!(!self.window.is_empty());
            let mut ptr = self.window.pop_front().unwrap_unchecked();
            let handle = ptr.as_mut();
            self.increase_queue_charges(handle);
            handle.queue = Queue::Probation;
            self.decrease_queue_charges(handle);
            self.probation.push_back(ptr);
        }
    }

    unsafe fn pop(&mut self) -> Option<NonNull<Self::Handle>> {
        // Compare the frequency of the front element of `window` and `probation` queue, and evict the lower one.
        // If both `window` and `probation` are empty, try evict from `protected`.
        let mut ptr = match (self.window.front(), self.probation.front()) {
            (None, None) => None,
            (None, Some(_)) => self.probation.pop_front(),
            (Some(_), None) => self.window.pop_front(),
            (Some(window), Some(probation)) => {
                if self.frequencies.count(window.base().hash()) < self.frequencies.count(probation.base().hash()) {
                    self.window.pop_front()
                } else {
                    self.probation.pop_front()
                }
            }
        }
        .or_else(|| self.protected.pop_front())?;

        let handle = ptr.as_mut();

        debug_assert!(!handle.link.is_linked());
        debug_assert!(handle.base().is_in_eviction());
        debug_assert_ne!(handle.queue, Queue::None);

        self.decrease_queue_charges(handle);
        handle.queue = Queue::None;
        handle.base_mut().set_in_eviction(false);

        Some(ptr)
    }

    unsafe fn reinsert(&mut self, mut ptr: NonNull<Self::Handle>) {
        let handle = ptr.as_mut();

        match handle.queue {
            Queue::None => {
                debug_assert!(!handle.link.is_linked());
                debug_assert!(!handle.base().is_in_eviction());
                self.push(ptr);
                debug_assert!(handle.link.is_linked());
                debug_assert!(handle.base().is_in_eviction());
            }
            Queue::Window => {
                // Move to MRU position of `window`.
                debug_assert!(handle.link.is_linked());
                debug_assert!(handle.base().is_in_eviction());
                self.window.remove_raw(handle.link.raw());
                self.window.push_back(ptr);
            }
            Queue::Probation => {
                // Promote to MRU position of `protected`.
                debug_assert!(handle.link.is_linked());
                debug_assert!(handle.base().is_in_eviction());
                self.probation.remove_raw(handle.link.raw());
                self.decrease_queue_charges(handle);
                handle.queue = Queue::Protected;
                self.increase_queue_charges(handle);
                self.protected.push_back(ptr);

                // If `protected` charges exceeds the capacity, overflow entry from `protected` to `probation`.
                while self.protected_charges > self.protected_charges_capacity {
                    debug_assert!(!self.protected.is_empty());
                    let mut ptr = self.protected.pop_front().unwrap_unchecked();
                    let handle = ptr.as_mut();
                    self.increase_queue_charges(handle);
                    handle.queue = Queue::Probation;
                    self.decrease_queue_charges(handle);
                    self.probation.push_back(ptr);
                }
            }
            Queue::Protected => {
                // Move to MRU position of `protected`.
                debug_assert!(handle.link.is_linked());
                debug_assert!(handle.base().is_in_eviction());
                self.protected.remove_raw(handle.link.raw());
                self.protected.push_back(ptr);
            }
        }
    }

    unsafe fn access(&mut self, ptr: NonNull<Self::Handle>) {
        self.update_frequencies(ptr.as_ref().base().hash());
    }

    unsafe fn remove(&mut self, mut ptr: NonNull<Self::Handle>) {
        let handle = ptr.as_mut();

        debug_assert!(handle.link.is_linked());
        debug_assert!(handle.base().is_in_eviction());
        debug_assert_ne!(handle.queue, Queue::None);

        match handle.queue {
            Queue::None => unreachable!(),
            Queue::Window => self.window.remove_raw(handle.link.raw()),
            Queue::Probation => self.probation.remove_raw(handle.link.raw()),
            Queue::Protected => self.protected.remove_raw(handle.link.raw()),
        };

        debug_assert!(!handle.link.is_linked());

        self.decrease_queue_charges(handle);
        handle.queue = Queue::None;
        handle.base_mut().set_in_eviction(false);
    }

    unsafe fn clear(&mut self) -> Vec<NonNull<Self::Handle>> {
        let mut res = Vec::with_capacity(self.len());

        while !self.is_empty() {
            let ptr = self.pop().unwrap_unchecked();
            debug_assert!(!ptr.as_ref().base().is_in_eviction());
            debug_assert!(!ptr.as_ref().link.is_linked());
            debug_assert_eq!(ptr.as_ref().queue, Queue::None);
            res.push(ptr);
        }

        res
    }

    unsafe fn len(&self) -> usize {
        self.window.len() + self.probation.len() + self.protected.len()
    }

    unsafe fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

unsafe impl<K, V> Send for Lfu<K, V>
where
    K: Key,
    V: Value,
{
}
unsafe impl<K, V> Sync for Lfu<K, V>
where
    K: Key,
    V: Value,
{
}

#[cfg(test)]
mod tests {}
