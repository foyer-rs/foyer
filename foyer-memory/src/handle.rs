//  Copyright 2024 foyer Project Authors
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

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use bitflags::bitflags;
use foyer_common::{
    assert::OptionExt,
    code::{Key, Value},
    strict_assert,
};

use crate::context::Context;

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    struct BaseHandleFlags: u64 {
        const IN_INDEXER = 0b00000001;
        const IN_EVICTION = 0b00000010;
        const DEPOSIT= 0b00000100;
    }
}

pub trait Handle: Send + Sync + 'static + Default {
    type Data;
    type Context: Context;

    fn base(&self) -> &BaseHandle<Self::Data, Self::Context>;
    fn base_mut(&mut self) -> &mut BaseHandle<Self::Data, Self::Context>;
}

pub trait HandleExt: Handle {
    fn init(&mut self, hash: u64, data: Self::Data, weight: usize, context: Self::Context) {
        self.base_mut().init(hash, data, weight, context);
    }
}
impl<H: Handle> HandleExt for H {}

pub trait KeyedHandle: Handle {
    type Key;

    fn key(&self) -> &Self::Key;
}

impl<K, V, T> KeyedHandle for T
where
    K: Key,
    V: Value,
    T: Handle<Data = (K, V)>,
{
    type Key = K;

    fn key(&self) -> &Self::Key {
        &self.base().data_unwrap_unchecked().0
    }
}

#[derive(Debug)]
pub struct BaseHandle<T, C> {
    /// key, value, context
    entry: Option<(T, C)>,
    /// key hash
    hash: u64,
    /// entry weight
    weight: usize,
    /// external reference count
    refs: AtomicUsize,
    /// flags that used by the general cache abstraction
    flags: AtomicU64,
}

impl<T, C> Default for BaseHandle<T, C> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T, C> BaseHandle<T, C> {
    /// Create a uninitialized handle.
    #[inline(always)]
    pub fn new() -> Self {
        Self {
            entry: None,
            hash: 0,
            weight: 0,
            refs: AtomicUsize::new(0),
            flags: AtomicU64::new(0),
        }
    }

    /// Init handle with args.
    #[inline(always)]
    pub fn init(&mut self, hash: u64, data: T, weight: usize, context: C) {
        strict_assert!(self.entry.is_none());
        assert_ne!(weight, 0);
        self.hash = hash;
        self.entry = Some((data, context));
        self.weight = weight;
        self.refs = AtomicUsize::new(0);
        self.flags = AtomicU64::new(0);
    }

    /// Take key and value from the handle and reset it to the uninitialized state.
    #[inline(always)]
    pub fn take(&mut self) -> (T, C, usize) {
        strict_assert!(self.entry.is_some());
        unsafe {
            self.entry
                .take()
                .map(|(data, context)| (data, context, self.weight))
                .strict_unwrap_unchecked()
        }
    }

    /// Return `true` if the handle is initialized.
    #[inline(always)]
    pub fn is_initialized(&self) -> bool {
        self.entry.is_some()
    }

    /// Get key hash.
    ///
    /// # Panics
    ///
    /// Panics if the handle is uninitialized.
    #[inline(always)]
    pub fn hash(&self) -> u64 {
        self.hash
    }

    /// Get data reference.
    ///
    /// # Panics
    ///
    /// Panics if the handle is uninitialized.
    #[inline(always)]
    pub fn data_unwrap_unchecked(&self) -> &T {
        strict_assert!(self.entry.is_some());
        unsafe { self.entry.as_ref().map(|entry| &entry.0).strict_unwrap_unchecked() }
    }

    /// Get context reference.
    ///  
    /// # Panics
    ///
    /// Panics if the handle is uninitialized.
    #[inline(always)]
    pub fn context(&self) -> &C {
        strict_assert!(self.entry.is_some());
        unsafe { self.entry.as_ref().map(|entry| &entry.1).strict_unwrap_unchecked() }
    }

    /// Get the weight of the handle.
    #[inline(always)]
    pub fn weight(&self) -> usize {
        self.weight
    }

    /// Get the atomic refs of the handle.
    #[inline(always)]
    pub fn refs(&self) -> &AtomicUsize {
        &self.refs
    }

    #[inline(always)]
    pub fn set_in_indexer(&mut self, val: bool) {
        self.set_flags(BaseHandleFlags::IN_INDEXER, val, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn is_in_indexer(&self) -> bool {
        self.get_flags(BaseHandleFlags::IN_INDEXER, Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn set_in_eviction(&mut self, val: bool) {
        self.set_flags(BaseHandleFlags::IN_EVICTION, val, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn is_in_eviction(&self) -> bool {
        self.get_flags(BaseHandleFlags::IN_EVICTION, Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn set_deposit(&mut self, val: bool) {
        self.set_flags(BaseHandleFlags::DEPOSIT, val, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn is_deposit(&self) -> bool {
        self.get_flags(BaseHandleFlags::DEPOSIT, Ordering::Relaxed)
    }

    #[inline(always)]
    fn set_flags(&self, flags: BaseHandleFlags, val: bool, order: Ordering) {
        match val {
            true => self.flags.fetch_or(flags.bits(), order),
            false => self.flags.fetch_add(!flags.bits(), order),
        };
    }

    #[inline(always)]
    fn get_flags(&self, flags: BaseHandleFlags, order: Ordering) -> bool {
        flags.bits() & self.flags.load(order) == flags.bits()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base_handle_basic() {
        let mut h = BaseHandle::<(), ()>::new();
        assert!(!h.is_in_indexer());
        assert!(!h.is_in_eviction());

        h.set_in_indexer(true);
        h.set_in_eviction(true);
        assert!(h.is_in_indexer());
        assert!(h.is_in_eviction());

        h.set_in_indexer(false);
        h.set_in_eviction(false);
        assert!(!h.is_in_indexer());
        assert!(!h.is_in_eviction());
    }
}
