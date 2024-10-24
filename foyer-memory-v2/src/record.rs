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

use crate::{slab::Token, Eviction};

/// Hint for the cache eviction algorithm to decide the priority of the specific entry if needed.
///
/// The meaning of the hint differs in each cache eviction algorithm, and some of them can be ignore by specific
/// algorithm.
///
/// If the given cache hint does not suitable for the cache eviction algorithm that is active, the algorithm may modify
/// it to a proper one.
///
/// For more details, please refer to the document of each enum options.
pub enum CacheHint {
    /// The default hint shared by all cache eviction algorithms.
    Normal,
    /// Suggest the priority of the entry is low.
    ///
    /// Used by [`crate::eviction::lru::Lru`].
    Low,
}

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct Flags: u64 {
        const IN_INDEXER = 0b00000001;
        const IN_EVICTION = 0b00000010;
        const EPHEMERAL= 0b00000100;
    }
}

pub struct Data<E>
where
    E: Eviction,
{
    pub key: E::Key,
    pub value: E::Value,
    pub hint: E::Hint,
    pub state: E::State,
    pub hash: u64,
    pub weight: usize,
}

/// [`Record`] holds the information of the cached entry.
pub struct Record<E>
where
    E: Eviction,
{
    key: E::Key,
    value: E::Value,
    hint: E::Hint,
    /// Make `state` visible to make intrusive data structure macro works.
    pub(crate) state: E::State,
    hash: u64,
    weight: usize,
    refs: AtomicUsize,
    flags: AtomicU64,
    token: Option<Token>,
}

impl<E> Record<E>
where
    E: Eviction,
{
    /// Create a new heap allocated record with data.
    pub fn new(data: Data<E>) -> Self {
        Record {
            key: data.key,
            value: data.value,
            hint: data.hint,
            state: data.state,
            hash: data.hash,
            weight: data.weight,
            refs: AtomicUsize::new(0),
            flags: AtomicU64::new(0),
            // Temporarily set to None, update after inserted into slab.
            token: None,
        }
    }

    /// Set the token of the record.
    ///
    /// # Safety
    ///
    /// Panics if the token is already set.
    pub fn init(&mut self, token: Token) {
        let old = self.token.replace(token);
        assert!(old.is_none());
    }

    /// Get the token of the record.
    ///
    /// # Safety
    ///
    /// Panics if the token is not set.
    pub fn token(&self) -> Token {
        self.token.unwrap()
    }

    pub fn into_data(self) -> Data<E> {
        Data {
            key: self.key,
            value: self.value,
            hint: self.hint,
            state: self.state,
            hash: self.hash,
            weight: self.weight,
        }
    }

    /// Get the immutable reference of the record key.
    pub fn key(&self) -> &E::Key {
        &self.key
    }

    /// Get the immutable reference of the record value.
    pub fn value(&self) -> &E::Value {
        &self.value
    }

    /// Get the immutable reference of the record hint.
    pub fn hint(&self) -> &E::Hint {
        &self.hint
    }

    /// Get the immutable reference of the record state.
    pub fn state(&self) -> &E::State {
        &self.state
    }

    /// Get the mutable reference of the record state.
    pub fn state_mut(&mut self) -> &mut E::State {
        &mut self.state
    }

    /// Get the record hash.
    pub fn hash(&self) -> u64 {
        self.hash
    }

    /// Get the record weight.
    pub fn weight(&self) -> usize {
        self.weight
    }

    /// Get the record atomic refs.
    pub fn refs(&self) -> &AtomicUsize {
        &self.refs
    }

    /// Set in eviction flag with relaxed memory order.
    pub fn set_in_eviction(&self, val: bool) {
        self.set_flags(Flags::IN_EVICTION, val, Ordering::Release);
    }

    /// Get in eviction flag with relaxed memory order.
    pub fn is_in_eviction(&self) -> bool {
        self.get_flags(Flags::IN_EVICTION, Ordering::Acquire)
    }

    /// Set in indexer flag with relaxed memory order.
    pub fn set_in_indexer(&self, val: bool) {
        self.set_flags(Flags::IN_INDEXER, val, Ordering::Release);
    }

    /// Get in indexer flag with relaxed memory order.
    pub fn is_in_indexer(&self) -> bool {
        self.get_flags(Flags::IN_INDEXER, Ordering::Acquire)
    }

    /// Set ephemeral flag with relaxed memory order.
    pub fn set_ephemeral(&self, val: bool) {
        self.set_flags(Flags::EPHEMERAL, val, Ordering::Release);
    }

    /// Get ephemeral flag with relaxed memory order.
    pub fn is_ephemeral(&self) -> bool {
        self.get_flags(Flags::EPHEMERAL, Ordering::Acquire)
    }

    /// Set the record atomic flags.
    pub fn set_flags(&self, flags: Flags, val: bool, order: Ordering) {
        match val {
            true => self.flags.fetch_or(flags.bits(), order),
            false => self.flags.fetch_and(!flags.bits(), order),
        };
    }

    /// Get the record atomic flags.
    pub fn get_flags(&self, flags: Flags, order: Ordering) -> bool {
        self.flags.load(order) & flags.bits() == flags.bits()
    }
}
