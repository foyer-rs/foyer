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

use std::sync::atomic::{AtomicIsize, AtomicU64, Ordering};

use bitflags::bitflags;
use serde::{Deserialize, Serialize};

use crate::eviction::Eviction;

/// Hint for the cache eviction algorithm to decide the priority of the specific entry if needed.
///
/// The meaning of the hint differs in each cache eviction algorithm, and some of them can be ignore by specific
/// algorithm.
///
/// If the given cache hint does not suitable for the cache eviction algorithm that is active, the algorithm may modify
/// it to a proper one.
///
/// For more details, please refer to the document of each enum options.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
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
    refs: AtomicIsize,
    flags: AtomicU64,
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
            refs: AtomicIsize::new(0),
            flags: AtomicU64::new(0),
        }
    }

    /// Consume the record and unwrap the data only.
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

    /// Get the atomic reference count.
    ///
    /// Return a non-negative value when the record is alive,
    /// otherwise, return -1 that implies the record is in the reclamation phase.
    pub fn refs(&self) -> isize {
        self.refs.load(Ordering::Acquire)
    }

    /// Reset the atomic reference count to 0.
    ///
    /// Only valid when the reclaimer gives up reclamation.
    pub fn reset(&self) {
        let old = self
            .refs
            .compare_exchange(-1, 0, Ordering::SeqCst, Ordering::Acquire)
            .unwrap();
        tracing::trace!(
            "[record]: reset record (hash: {}) refs from {} (must equals to -1)",
            self.hash,
            old,
        );
        assert_eq!(old, -1);
    }

    /// Increase the atomic reference count.
    ///
    /// This function returns the new reference count after the op.
    pub fn inc_refs(&self, val: isize) -> isize {
        let old = self.refs.fetch_add(val, Ordering::SeqCst);
        tracing::trace!(
            "[record]: inc record (hash: {}) refs: {} => {}",
            self.hash,
            old,
            old + val
        );
        old + val
    }

    // /// Decrease the atomic reference count.
    // ///
    // /// This function returns the new reference count after the op.
    // pub fn dec_refs(&self, val: isize) -> isize {
    //     let old = self.refs.fetch_sub(val, Ordering::SeqCst);
    //     tracing::trace!(
    //         "[record]: dec record (hash: {}) refs: {} => {}",
    //         self.hash,
    //         old,
    //         old - val
    //     );
    //     old - val
    // }

    /// Increase the atomic reference count with a cas operation,
    /// to prevent from increasing the record in the reclamation phase.
    ///
    /// This function returns the new reference count after the op if the record is not in the reclamation phase.
    pub fn inc_refs_cas(&self, val: isize) -> Option<isize> {
        let mut current = self.refs.load(Ordering::Relaxed);
        loop {
            if current == -1 {
                tracing::trace!(
                    "[record]: inc record (hash: {}) refs (cas) skipped for it is in reclamation phase",
                    self.hash
                );
                return None;
            }
            match self
                .refs
                .compare_exchange(current, current + val, Ordering::SeqCst, Ordering::Acquire)
            {
                Err(cur) => current = cur,
                Ok(_) => {
                    tracing::trace!(
                        "[record]: inc record (hash: {}) refs (cas): {} => {}",
                        self.hash,
                        current,
                        current + val
                    );
                    return Some(current + val);
                }
            }
        }
    }

    /// Decrease the atomic reference count by 1 with a cas operation.
    ///
    /// If the refs hits 0 after decreasing, get the permission to reclaim the record.
    ///
    /// This function returns the new reference count after the op if the record is not in the reclamation phase.
    pub fn dec_ref_cas(&self) -> isize {
        let mut current = self.refs.load(Ordering::Relaxed);
        loop {
            match current {
                1 => match self.refs.compare_exchange(1, -1, Ordering::SeqCst, Ordering::Acquire) {
                    Ok(_) => {
                        tracing::trace!(
                            "[record]: dec record (hash: {}) refs from 1 and got reclamation permission",
                            self.hash
                        );
                        return -1;
                    }
                    Err(cur) => current = cur,
                },
                c => match self
                    .refs
                    .compare_exchange(c, c - 1, Ordering::SeqCst, Ordering::Acquire)
                {
                    Ok(_) => {
                        tracing::trace!("[record]: dec record (hash: {}) refs: {} => {}", self.hash, c, c - 1);
                        return c - 1;
                    }
                    Err(cur) => current = cur,
                },
            }
        }
    }

    /// Try to acquire the permission to reclaim the record.
    ///
    /// If `true` is returned, the caller MUST reclaim the record.
    pub fn need_reclaim(&self) -> bool {
        let current = self.refs.load(Ordering::Acquire);
        if current != 0 {
            tracing::trace!(
                "[record]: check if record (hash: {}) needs reclamation: {} with refs {}",
                self.hash,
                false,
                current
            );
            return false;
        }
        self.refs
            .compare_exchange(0, -1, Ordering::SeqCst, Ordering::Acquire)
            .inspect(|refs| {
                tracing::trace!(
                    "[record]: check if record (hash: {}) needs reclamation: {} with refs {}",
                    self.hash,
                    true,
                    refs
                )
            })
            .inspect_err(|refs| {
                tracing::trace!(
                    "[record]: check if record (hash: {}) needs reclamation: {} with refs {}",
                    self.hash,
                    false,
                    refs
                )
            })
            .is_ok()
    }
}
