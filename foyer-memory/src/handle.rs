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

use bitflags::bitflags;

use crate::{Key, Value};

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    struct BaseHandleFlags: u8 {
        const IN_CACHE = 0b00000001;
        const IN_EVICTION = 0b00000010;
    }
}

#[derive(Debug)]
pub struct BaseHandle<K, V>
where
    K: Key,
    V: Value,
{
    kv: Option<(K, V)>,
    hash: u64,
    charge: usize,
    refs: usize,
    flags: BaseHandleFlags,
}

impl<K, V> Default for BaseHandle<K, V>
where
    K: Key,
    V: Value,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> BaseHandle<K, V>
where
    K: Key,
    V: Value,
{
    /// Create a uninited handle.
    #[inline(always)]
    pub fn new() -> Self {
        Self {
            kv: None,
            hash: 0,
            charge: 0,
            refs: 0,
            flags: BaseHandleFlags::empty(),
        }
    }

    /// Init handle with args.
    #[inline(always)]
    pub fn init(&mut self, hash: u64, key: K, value: V, charge: usize) {
        debug_assert!(self.kv.is_none());
        self.hash = hash;
        self.kv = Some((key, value));
        self.charge = charge;
        self.refs = 0;
        self.flags = BaseHandleFlags::empty();
    }

    /// Take key and value from the handle and reset it to the uninited state.
    #[inline(always)]
    pub fn take(&mut self) -> (K, V) {
        debug_assert!(self.kv.is_some());
        unsafe { self.kv.take().unwrap_unchecked() }
    }

    /// Return `true` if the handle is inited.
    #[inline(always)]
    pub fn is_inited(&self) -> bool {
        self.kv.is_some()
    }

    /// Get key hash.
    ///
    /// # Panics
    ///
    /// Panics if the handle is uninited.
    #[inline(always)]
    pub fn hash(&self) -> u64 {
        self.hash
    }

    /// Get key reference.
    ///  
    /// # Panics
    ///
    /// Panics if the handle is uninited.
    #[inline(always)]
    pub fn key(&self) -> &K {
        debug_assert!(self.kv.is_some());
        unsafe { self.kv.as_ref().map(|kv| &kv.0).unwrap_unchecked() }
    }

    /// Get value reference.
    ///  
    /// # Panics
    ///
    /// Panics if the handle is uninited.
    #[inline(always)]
    pub fn value(&self) -> &V {
        debug_assert!(self.kv.is_some());
        unsafe { self.kv.as_ref().map(|kv| &kv.1).unwrap_unchecked() }
    }

    /// Get the charge of the handle.
    #[inline(always)]
    pub fn charge(&self) -> usize {
        self.charge
    }

    /// Increase the external reference count of the handle, returns the new reference count.
    #[inline(always)]
    pub fn inc_ref(&mut self) -> usize {
        self.refs += 1;
        self.refs
    }

    /// Decrease the external reference count of the handle, returns the new reference count.
    #[inline(always)]
    pub fn dec_ref(&mut self) -> usize {
        self.refs -= 1;
        self.refs
    }

    /// Get the external reference count of the handle.
    #[inline(always)]
    pub fn refs(&self) -> usize {
        self.refs
    }

    #[inline(always)]
    pub fn set_in_cache(&mut self, in_cache: bool) {
        if in_cache {
            self.flags |= BaseHandleFlags::IN_CACHE;
        } else {
            self.flags -= BaseHandleFlags::IN_CACHE;
        }
    }

    #[inline(always)]
    pub fn is_in_cache(&self) -> bool {
        !(self.flags & BaseHandleFlags::IN_CACHE).is_empty()
    }

    #[inline(always)]
    pub fn set_in_eviction(&mut self, in_eviction: bool) {
        if in_eviction {
            self.flags |= BaseHandleFlags::IN_EVICTION;
        } else {
            self.flags -= BaseHandleFlags::IN_EVICTION;
        }
    }

    #[inline(always)]
    pub fn is_in_eviction(&self) -> bool {
        !(self.flags & BaseHandleFlags::IN_EVICTION).is_empty()
    }
}
