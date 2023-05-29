//  Copyright 2023 MrCroxx
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

//  Copyright (c) Meta Platforms, Inc. and affiliates.
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

use std::hash::Hasher;
use std::ptr::NonNull;
use std::time::SystemTime;

use cmsketch::CMSketchUsize;
use twox_hash::XxHash64;

use foyer_utils::dlist::{DList, Entry, Iter};
use foyer_utils::intrusive_dlist;

use super::Index;

const MIN_CAPACITY: usize = 100;
const ERROR_THRESHOLD: f64 = 5.0;
const HASH_COUNT: usize = 4;
const DECAY_FACTOR: f64 = 0.5;

#[derive(Clone, Debug)]
pub struct Config {
    /// The multiplier for window len given the cache size.
    pub window_to_cache_size_ratio: usize,

    /// The ratio of tiny lru capacity to overall capacity.
    pub tiny_lru_capacity_ratio: f64,
}

#[derive(PartialEq, Eq, Debug)]
enum LruType {
    Tiny,
    Main,
}

pub struct Handle<I: Index> {
    entry_tiny: Entry,
    entry_main: Entry,

    is_in_cache: bool,
    is_accessed: bool,
    update_time: SystemTime,

    lru_type: LruType,

    index: I,
}

impl<I: Index> Handle<I> {
    fn new(index: I) -> Self {
        Self {
            entry_tiny: Entry::default(),
            entry_main: Entry::default(),

            is_in_cache: false,
            is_accessed: false,
            update_time: SystemTime::now(),

            lru_type: LruType::Tiny,

            index,
        }
    }

    fn index(&self) -> &I {
        &self.index
    }
}

intrusive_dlist! { Handle<I: Index>, entry_tiny, HandleDListTinyAdapter}
intrusive_dlist! { Handle<I: Index>, entry_main, HandleDListMainAdapter}

/// Implements the W-TinyLFU cache eviction policy as described in -
///
/// https://arxiv.org/pdf/1512.00727.pdf
///
/// The cache is split into 2 parts, the main cache and the tiny cache.
/// The tiny cache is typically sized to be 1% of the total cache with
/// the main cache being the rest 99%. Both caches are implemented using
/// LRUs. New items land in tiny cache. During eviction, the tail item
/// from the tiny cache is promoted to main cache if its frequency is
/// higher than the tail item of of main cache, and the tail of main
/// cache is evicted. This gives the frequency based admission into main
/// cache. Hits in each cache simply move the item to the head of each
/// LRU cache.
/// The frequency counts are maintained in count-min-sketch approximate
/// counters -
///
/// Counter Overhead:
/// The window_to_cache_size_ratio determines the size of counters.
/// The default value is 32 which means the counting window size is
/// 32 times the cache size. After every 32 X cache capacity number
/// of items, the counts are halved to weigh frequency by recency.
/// The function counter_size() returns the size of the counters
/// in bytes. See maybe_grow_access_counters() implementation for
/// how the size is computed.
///
/// Tiny cache size:
/// This default to 1%. There's no need to tune this parameter.
pub struct TinyLfu<I: Index> {
    /// tiny lru list
    lru_tiny: DList<Handle<I>, HandleDListTinyAdapter>,

    /// main lru list
    lru_main: DList<Handle<I>, HandleDListMainAdapter>,

    /// the window length counter
    window_size: usize,

    /// maxumum value of window length which when hit the counters are halved
    max_window_size: usize,

    /// the capacity for which the counters are sized
    capacity: usize,

    /// approximate streaming frequency counters
    ///
    /// the counts are halved every time the max_window_len is hit
    frequencies: CMSketchUsize,

    config: Config,
}

impl<I: Index> TinyLfu<I> {
    pub fn new(config: Config) -> Self {
        let mut res = Self {
            lru_tiny: DList::new(),
            lru_main: DList::new(),

            window_size: 0,
            max_window_size: 0,
            capacity: 0,

            // A dummy size, will be updated later.
            frequencies: CMSketchUsize::new_with_size(1, 1),

            config,
        };
        res.maybe_grow_access_counters();
        res
    }

    /// Returns `true` if the information is recorded and bumped the handle to the head of the lru,
    /// returns `false` otherwise.
    fn access(&mut self, mut handle: NonNull<Handle<I>>) -> bool {
        unsafe {
            handle.as_mut().is_accessed = true;

            if !handle.as_ref().is_in_cache {
                return false;
            }

            match handle.as_ref().lru_type {
                LruType::Tiny => self.lru_tiny.move_to_head(handle),
                LruType::Main => self.lru_main.move_to_head(handle),
            }
            handle.as_mut().update_time = SystemTime::now();
            self.update_frequencies(handle);

            true
        }
    }

    /// Returns `true` if handle is successfully added into the lru,
    /// returns `false` if the handle is already in the lru.
    fn insert(&mut self, mut handle: NonNull<Handle<I>>) -> bool {
        unsafe {
            if handle.as_ref().is_in_cache {
                return false;
            }

            self.lru_tiny.link_at_head(handle);
            handle.as_mut().lru_type = LruType::Tiny;

            // Initialize the frequency count for this handle.
            self.update_frequencies(handle);

            // If tiny cache is full, unconditionally promote tail to main cache.
            let expected_tiny_len = (self.config.tiny_lru_capacity_ratio
                * (self.lru_tiny.len() + self.lru_main.len()) as f64)
                as usize;
            if self.lru_tiny.len() > expected_tiny_len {
                let mut handle_tail = self.lru_tiny.tail().unwrap();
                self.lru_tiny.remove(handle_tail);

                self.lru_main.link_at_head(handle_tail);
                handle_tail.as_mut().lru_type = LruType::Main;
            } else {
                self.maybe_promote_tail();
            }

            // If the number of counters are too small for the cache size, double them.
            self.maybe_grow_access_counters();

            handle.as_mut().is_in_cache = true;
            handle.as_mut().is_accessed = false;
            handle.as_mut().update_time = SystemTime::now();

            true
        }
    }

    /// Returns `true` if handle is successfully removed from the lru,
    /// returns `false` if the handle is unchanged.
    fn remove(&mut self, mut handle: NonNull<Handle<I>>) -> bool {
        unsafe {
            if !handle.as_ref().is_in_cache {
                return false;
            }

            match handle.as_ref().lru_type {
                LruType::Main => self.lru_main.remove(handle),
                LruType::Tiny => self.lru_tiny.remove(handle),
            }

            handle.as_mut().is_accessed = false;
            handle.as_mut().is_in_cache = false;

            true
        }
    }

    fn eviction_iter<'a>(&'a self) -> EvictionIter<'a, I> {
        unsafe {
            let mut iter_main: Iter<'a, _, _> = self.lru_main.iter();
            iter_main.tail();
            let mut iter_tiny: Iter<'a, _, _> = self.lru_tiny.iter();
            iter_tiny.tail();
            EvictionIter {
                tinylfu: self,
                iter_main,
                iter_tiny,
            }
        }
    }

    fn maybe_grow_access_counters(&mut self) {
        let capacity = self.lru_tiny.len() + self.lru_main.len();

        // If the new capacity ask is more than double the current size,
        // recreate the approximate frequency counters.
        if 2 * self.capacity > capacity {
            return;
        }

        self.capacity = std::cmp::max(capacity, MIN_CAPACITY);

        // The window counter that's incremented on every fetch.
        self.window_size = 0;

        // The frequency counters are halved every `max_window_size` fetches to decay the frequency counts.
        self.max_window_size = self.capacity * self.config.window_to_cache_size_ratio;

        // Number of frequency counters - roughly equal to the window size divided by error tolerance.
        let num_counters = (1f64.exp() * self.max_window_size as f64 / ERROR_THRESHOLD) as usize;
        let num_counters = num_counters.next_power_of_two();

        self.frequencies = CMSketchUsize::new_with_size(num_counters, HASH_COUNT);
    }

    fn update_frequencies(&mut self, handle: NonNull<Handle<I>>) {
        self.frequencies.record(Self::hash_handle(handle));
        self.window_size += 1;

        // Decay counts every `max_window_size`. This avoids having items that were
        // accessed frequently (were hot) but aren't being accessed anymore (are cold)
        // from staying in cache forever.
        if self.window_size == self.max_window_size {
            self.window_size >>= 1;
            self.frequencies.decay(DECAY_FACTOR);
        }
    }

    fn maybe_promote_tail(&mut self) {
        unsafe {
            let mut handle_main = match self.lru_main.tail() {
                Some(handle) => handle,
                None => return,
            };
            let mut handle_tiny = match self.lru_tiny.tail() {
                Some(handle) => handle,
                None => return,
            };

            if self.admit_to_main(handle_main, handle_tiny) {
                self.lru_tiny.remove(handle_tiny);
                self.lru_main.link_at_head(handle_tiny);
                handle_tiny.as_mut().lru_type = LruType::Main;

                self.lru_main.remove(handle_main);
                self.lru_tiny.link_at_tail(handle_main);
                handle_main.as_mut().lru_type = LruType::Tiny;
                return;
            }

            // A node with high frequency at the tail of main cache might prevent
            // promotions from tiny cache from happening for a long time. Relocate
            // the tail of main cache to prevent this.
            self.lru_main.move_to_head(handle_main);
        }
    }

    fn admit_to_main(
        &self,
        handle_main: NonNull<Handle<I>>,
        handle_tiny: NonNull<Handle<I>>,
    ) -> bool {
        unsafe {
            assert_eq!(handle_main.as_ref().lru_type, LruType::Main);
            assert_eq!(handle_tiny.as_ref().lru_type, LruType::Tiny);

            let frequent_main = self.frequencies.count(Self::hash_handle(handle_main));
            let frequent_tiny = self.frequencies.count(Self::hash_handle(handle_tiny));

            frequent_main <= frequent_tiny
        }
    }

    fn hash_handle(handle: NonNull<Handle<I>>) -> u64 {
        let mut hasher = XxHash64::default();
        unsafe { handle.as_ref().index.hash(&mut hasher) };
        hasher.finish()
    }
}

pub struct EvictionIter<'a, I: Index> {
    tinylfu: &'a TinyLfu<I>,
    iter_main: Iter<'a, Handle<I>, HandleDListMainAdapter>,
    iter_tiny: Iter<'a, Handle<I>, HandleDListTinyAdapter>,
}

impl<'a, I: Index> Iterator for EvictionIter<'a, I> {
    type Item = &'a I;

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            let handle_main = self.iter_main.element();
            let handle_tiny = self.iter_tiny.element();

            match (handle_main, handle_tiny) {
                (None, None) => None,
                (Some(handle_main), None) => {
                    self.iter_main.prev();
                    Some(&handle_main.as_ref().index)
                }
                (None, Some(handle_tiny)) => {
                    self.iter_tiny.prev();
                    Some(&handle_tiny.as_ref().index)
                }
                (Some(handle_main), Some(handle_tiny)) => {
                    // Eviction from tiny or main depending on whether the tiny handle woould be
                    // admitted to main cachce. If it would be, evict from main cache, otherwise
                    // from tiny cache.
                    if self.tinylfu.admit_to_main(handle_main, handle_tiny) {
                        self.iter_main.prev();
                        Some(&handle_main.as_ref().index)
                    } else {
                        self.iter_tiny.prev();
                        Some(&handle_tiny.as_ref().index)
                    }
                }
            }
        }
    }
}

// unsafe impl `Send + Sync` for structs with `NonNull` usage

unsafe impl<I: Index> Send for TinyLfu<I> {}
unsafe impl<I: Index> Sync for TinyLfu<I> {}

unsafe impl<I: Index> Send for Handle<I> {}
unsafe impl<I: Index> Sync for Handle<I> {}

unsafe impl<'a, I: Index> Send for EvictionIter<'a, I> {}
unsafe impl<'a, I: Index> Sync for EvictionIter<'a, I> {}

impl super::Config for Config {}

impl<I: Index> super::Handle for Handle<I> {
    type I = I;

    fn new(index: Self::I) -> Self {
        Self::new(index)
    }

    fn index(&self) -> &Self::I {
        self.index()
    }
}

impl<I: Index> super::Policy for TinyLfu<I> {
    type I = I;
    type C = Config;
    type H = Handle<I>;
    type E<'e> = EvictionIter<'e, I>;

    fn new(config: Self::C) -> Self {
        TinyLfu::new(config)
    }

    fn insert(&mut self, handle: NonNull<Self::H>) -> bool {
        self.insert(handle)
    }

    fn remove(&mut self, handle: NonNull<Self::H>) -> bool {
        self.remove(handle)
    }

    fn access(&mut self, handle: NonNull<Self::H>) -> bool {
        self.access(handle)
    }

    fn eviction_iter(&self) -> Self::E<'_> {
        self.eviction_iter()
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::*;

    fn ptr<I: Index>(handle: &mut Handle<I>) -> NonNull<Handle<I>> {
        unsafe { NonNull::new_unchecked(handle as *mut _) }
    }

    #[test]
    fn test_lru_simple() {
        let config = Config {
            window_to_cache_size_ratio: 10,
            tiny_lru_capacity_ratio: 0.01,
        };
        let mut lfu: TinyLfu<u64> = TinyLfu::new(config);

        let mut handles = (0..101).map(Handle::new).collect_vec();
        for handle in handles.iter_mut().take(100) {
            lfu.insert(ptr(handle));
        }

        assert_eq!(99, lfu.lru_main.len());
        assert_eq!(1, lfu.lru_tiny.len());
        assert_eq!(handles[0].lru_type, LruType::Tiny);

        // 0 will be evicted at last because it is on tiny lru but its frequency equals to others
        assert_eq!(
            (1..100).chain([0].into_iter()).collect_vec(),
            lfu.eviction_iter().copied().collect_vec()
        );

        for handle in handles.iter_mut().take(100) {
            lfu.access(ptr(handle));
        }
        lfu.access(ptr(&mut handles[0]));
        lfu.insert(ptr(&mut handles[100]));

        assert_eq!(handles[0].lru_type, LruType::Main);
        assert_eq!(handles[100].lru_type, LruType::Tiny);

        assert_eq!(
            [100]
                .into_iter()
                .chain(1..100)
                .chain([0].into_iter())
                .collect_vec(),
            lfu.eviction_iter().copied().collect_vec()
        );

        drop(handles);
    }
}
