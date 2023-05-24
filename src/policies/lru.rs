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

use std::ptr::NonNull;
use std::time::SystemTime;

use crate::collections::dlist::{intrusive_dlist, DList, Entry, Iter};

use super::Index;

#[derive(Clone, Debug)]
pub struct Config {
    pub update_on_write: bool,

    pub update_on_read: bool,

    /// Insertion point of the new entry, between 0 and 1.
    pub lru_insertion_point_fraction: f64,
}

pub struct Handle<I: Index> {
    entry: Entry,

    is_in_cache: bool,
    is_accessed: bool,
    update_time: SystemTime,

    is_in_tail: bool,

    index: I,
}

impl<I: Index> Handle<I> {
    fn new(index: I) -> Self {
        Self {
            entry: Entry::default(),

            is_in_cache: false,
            is_accessed: false,
            update_time: SystemTime::now(),

            is_in_tail: false,

            index,
        }
    }

    fn index(&self) -> &I {
        &self.index
    }
}

intrusive_dlist! { Handle<I: Index>, entry, HandleDListAdapter}

pub struct Lru<I: Index> {
    /// lru list
    lru: DList<Handle<I>, HandleDListAdapter>,

    /// insertion point
    insertion_point: Option<NonNull<Handle<I>>>,

    /// length of tail after insertion point
    tail_len: usize,

    config: Config,
}

impl<I: Index> Lru<I> {
    fn new(config: Config) -> Self {
        Self {
            lru: DList::new(),

            insertion_point: None,

            tail_len: 0,

            config,
        }
    }

    /// Returns `true` if the information is recorded and bumped the handle to the head of the lru,
    /// returns `false` otherwise.
    fn access(&mut self, mut handle: NonNull<Handle<I>>) -> bool {
        unsafe {
            handle.as_mut().is_accessed = true;

            // TODO(MrCroxx): try trigger reconfigure

            self.ensuer_not_insertion_point(handle);

            if handle.as_ref().is_in_cache {
                self.lru.move_to_head(handle);
                handle.as_mut().update_time = SystemTime::now();
            }

            if handle.as_ref().is_in_tail {
                handle.as_mut().is_in_tail = false;
                self.tail_len -= 1;
                self.update_lru_insertion_point();
            }

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

            match self.insertion_point {
                Some(insertion_point) => self.lru.link_before(insertion_point, handle),
                None => self.lru.link_at_head(handle),
            }
            handle.as_mut().is_in_cache = true;
            handle.as_mut().update_time = SystemTime::now();
            handle.as_mut().is_accessed = false;
            self.update_lru_insertion_point();

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

            self.ensuer_not_insertion_point(handle);
            self.lru.remove(handle);
            handle.as_mut().is_accessed = false;
            if handle.as_ref().is_in_tail {
                handle.as_mut().is_in_tail = false;
                self.tail_len -= 1;
            }

            true
        }
    }

    fn eviction_iter(&self) -> EvictionIter<'_, I> {
        unsafe {
            let mut iter = self.lru.iter();
            iter.tail();
            EvictionIter { iter }
        }
    }

    fn update_lru_insertion_point(&mut self) {
        unsafe {
            if self.config.lru_insertion_point_fraction == 0.0 {
                return;
            }

            if self.insertion_point.is_none() {
                self.insertion_point = self.lru.tail();
                self.tail_len = 0;
                if let Some(insertion_point) = &mut self.insertion_point {
                    insertion_point.as_mut().is_in_tail = true;
                    self.tail_len += 1;
                }
            }

            if self.lru.len() <= 1 {
                return;
            }

            assert!(self.insertion_point.is_some());

            let expected_tail_len =
                (self.lru.len() as f64 * (1.0 - self.config.lru_insertion_point_fraction)) as usize;

            let mut curr = self.insertion_point.unwrap();
            while self.tail_len < expected_tail_len && Some(curr) != self.lru.head() {
                curr = self.lru.prev(curr).unwrap();
                curr.as_mut().is_in_tail = true;
                self.tail_len += 1;
            }
            while self.tail_len > expected_tail_len && Some(curr) != self.lru.tail() {
                curr.as_mut().is_in_tail = false;
                self.tail_len -= 1;
                curr = self.lru.next(curr).unwrap();
            }

            self.insertion_point = Some(curr);
        }
    }

    fn ensuer_not_insertion_point(&mut self, handle: NonNull<Handle<I>>) {
        unsafe {
            if Some(handle) == self.insertion_point {
                self.insertion_point = self.lru.prev(handle);
                match &mut self.insertion_point {
                    Some(insertion_point) => {
                        self.tail_len += 1;
                        insertion_point.as_mut().is_in_tail = true;
                    }
                    // TODO(MrCroxx): think ?
                    None => assert_eq!(self.lru.len(), 1),
                }
            }
        }
    }
}

pub struct EvictionIter<'a, I: Index> {
    iter: Iter<'a, Handle<I>, HandleDListAdapter>,
}

impl<'a, I: Index> Iterator for EvictionIter<'a, I> {
    type Item = &'a I;

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            match self.iter.element() {
                Some(element) => {
                    self.iter.prev();
                    Some(&element.as_ref().index)
                }
                None => None,
            }
        }
    }
}

// unsafe impl `Send + Sync` for structs with `NonNull` usage

unsafe impl<I: Index> Send for Lru<I> {}
unsafe impl<I: Index> Sync for Lru<I> {}

unsafe impl<I: Index> Send for Handle<I> {}
unsafe impl<I: Index> Sync for Handle<I> {}

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

impl<I: Index> super::Policy for Lru<I> {
    type I = I;
    type C = Config;
    type H = Handle<I>;
    type E<'e> = EvictionIter<'e, I>;

    fn new(config: Self::C) -> Self {
        Lru::new(config)
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
            update_on_write: true,
            update_on_read: true,
            lru_insertion_point_fraction: 0.0,
        };
        let mut lru: Lru<u64> = Lru::new(config);

        let mut handles = vec![Handle::new(0), Handle::new(1), Handle::new(2)];

        lru.insert(ptr(&mut handles[0]));
        lru.insert(ptr(&mut handles[1]));
        lru.insert(ptr(&mut handles[2]));

        assert_eq!(vec![0, 1, 2], lru.eviction_iter().copied().collect_vec());

        lru.access(ptr(&mut handles[1]));

        assert_eq!(vec![0, 2, 1], lru.eviction_iter().copied().collect_vec());

        lru.remove(ptr(&mut handles[2]));

        assert_eq!(vec![0, 1], lru.eviction_iter().copied().collect_vec());

        drop(handles);
    }
}
