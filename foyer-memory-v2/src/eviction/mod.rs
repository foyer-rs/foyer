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

use std::ptr::NonNull;

use foyer_common::code::{Key, Value};
use serde::{de::DeserializeOwned, Serialize};

use crate::Record;

pub trait Hint: Send + Sync + 'static + Clone + Default {}
impl<T> Hint for T where T: Send + Sync + 'static + Clone + Default {}

pub trait State: Send + Sync + 'static + Default {}
impl<T> State for T where T: Send + Sync + 'static + Default {}

pub trait Config: Send + Sync + 'static + Clone + Serialize + DeserializeOwned + Default {}
impl<T> Config for T where T: Send + Sync + 'static + Clone + Serialize + DeserializeOwned + Default {}

pub enum Operator {
    Immutable,
    Mutable,
}

/// Cache eviction algorithm abstraction.
///
/// [`Eviction`] provides essential APIs for the plug-and-play algorithm abstraction.
///
/// [`Eviction`] is needs to be implemented to support a new cache eviction algorithm.
///
/// For performance considerations, most APIs pass parameters via [`NonNull`] pointers to implement intrusive data
/// structures. It is not required to implement the cache eviction algorithm using the [`NonNull`] pointers. They can
/// also be used as a token for the target entry.
///
/// # Safety
///
/// The pointer can be dereferenced as a mutable reference ***iff*** the `self` reference is also mutable.
/// Dereferencing a pointer as a mutable reference when `self` is immutable will cause UB.
pub trait Eviction: Send + Sync + 'static + Sized {
    /// Cache eviction algorithm configurations.
    type Config: Config;
    /// Cache key. Generally, it is supposed to be a generic type of the implementation.
    type Key: Key;
    /// Cache value. Generally, it is supposed to be a generic type of the implementation.
    type Value: Value;
    /// Hint for a cache entry. Can be used to support priority at the entry granularity.
    type Hint: Hint;
    /// State for a cache entry. Mutable state for maintaining the cache eviction algorithm implementation.
    type State: State;

    /// Create a new cache eviction algorithm instance with the given arguments.
    fn new(capacity: usize, config: &Self::Config) -> Self;

    /// Update the arguments of the ache eviction algorithm instance.
    fn update(&mut self, capacity: usize, config: &Self::Config);

    /// Push a record into the cache eviction algorithm instance.
    ///
    /// The caller guarantees that the record is NOT in the cache eviction algorithm instance.
    ///
    /// The cache eviction algorithm instance MUST hold the record and set its `IN_EVICTION` flag to true.
    fn push(&mut self, ptr: NonNull<Record<Self>>);

    /// Push a record from the cache eviction algorithm instance.
    ///
    /// The cache eviction algorithm instance MUST remove the record and set its `IN_EVICTION` flag to false.
    fn pop(&mut self) -> Option<NonNull<Record<Self>>>;

    /// Remove a record from the cache eviction algorithm instance.
    ///
    /// The caller guarantees that the record is in the cache eviction algorithm instance.
    ///
    /// The cache eviction algorithm instance MUST remove the record and set its `IN_EVICTION` flag to false.
    fn remove(&mut self, ptr: NonNull<Record<Self>>);

    /// Remove all records from the cache eviction algorithm instance.
    ///
    /// The cache eviction algorithm instance MUST remove the records and set its `IN_EVICTION` flag to false.
    fn clear(&mut self);

    /// Return the count of the records that in the cache eviction algorithm instance.
    fn len(&self) -> usize;

    /// Return if the cache eviction algorithm instance is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Determine if the immutable version or the mutable version to use for the `acquire` operation.
    ///
    /// Only the chosen version needs to be implemented. The other version is recommend to be left as `unreachable!()`.
    fn acquire_operator() -> Operator;

    /// Immutable version of the `acquire` operation.
    ///
    /// `acquire` is called when an external caller acquire a cache entry from the cache.
    ///
    /// The entry can be EITHER in the cache eviction algorithm instance or not.
    fn acquire_immutable(&self, ptr: NonNull<Record<Self>>);

    /// Mutable version of the `acquire` operation.
    ///
    /// `acquire` is called when an external caller acquire a cache entry from the cache.
    ///
    /// The entry can be EITHER in the cache eviction algorithm instance or not.
    fn acquire_mutable(&mut self, ptr: NonNull<Record<Self>>);

    /// `release` is called when the last external caller drops the cache entry.
    ///
    /// The entry can be EITHER in the cache eviction algorithm instance or not.
    ///
    /// `release` operation can either make the cache eviction algorithm instance hold or not hold the record, but the
    /// `IN_EVICTION` flags must be set properly according to it.
    fn release(&mut self, ptr: NonNull<Record<Self>>);
}

pub mod fifo;
pub mod lru;
