// Copyright 2026 foyer Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use foyer_common::{
    code::{Key, Value},
    error::Result,
    properties::Properties,
};
use serde::{de::DeserializeOwned, Serialize};

use crate::record::Record;

pub trait State: Send + Sync + 'static + Default {}
impl<T> State for T where T: Send + Sync + 'static + Default {}

pub trait Config: Send + Sync + 'static + Clone + Serialize + DeserializeOwned + Default {}
impl<T> Config for T where T: Send + Sync + 'static + Clone + Serialize + DeserializeOwned + Default {}

/// Wrapper for one of the three kind of operations for the eviction container:
///
/// 1. no operation
/// 2. immutable operation
/// 3. mutable operation
#[expect(clippy::type_complexity)]
pub enum Op<E>
where
    E: Eviction,
{
    /// no operation
    Noop,
    /// immutable operation
    Immutable(Box<dyn Fn(&E, &Arc<Record<E>>) + Send + Sync + 'static>),
    /// mutable operation
    Mutable(Box<dyn FnMut(&mut E, &Arc<Record<E>>) + Send + Sync + 'static>),
}

impl<E> Op<E>
where
    E: Eviction,
{
    /// no operation
    pub fn noop() -> Self {
        Self::Noop
    }

    /// immutable operation
    pub fn immutable<F>(f: F) -> Self
    where
        F: Fn(&E, &Arc<Record<E>>) + Send + Sync + 'static,
    {
        Self::Immutable(Box::new(f))
    }

    /// mutable operation
    pub fn mutable<F>(f: F) -> Self
    where
        F: FnMut(&mut E, &Arc<Record<E>>) + Send + Sync + 'static,
    {
        Self::Mutable(Box::new(f))
    }
}

/// Cache eviction algorithm abstraction.
///
/// [`Eviction`] provides essential APIs for the plug-and-play algorithm abstraction.
///
/// [`Eviction`] is needs to be implemented to support a new cache eviction algorithm.
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
    /// Properties for a cache entry, it is supposed to be a generic type of the implementation.
    ///
    /// Can be used to support priority at the entry granularity.
    type Properties: Properties;
    /// State for a cache entry. Mutable state for maintaining the cache eviction algorithm implementation.
    type State: State;

    /// Create a new cache eviction algorithm instance with the given arguments.
    fn new(capacity: usize, config: &Self::Config) -> Self;

    /// Update the arguments of the ache eviction algorithm instance.
    fn update(&mut self, capacity: usize, config: Option<&Self::Config>) -> Result<()>;

    /// Push a record into the cache eviction algorithm instance.
    ///
    /// The caller guarantees that the record is NOT in the cache eviction algorithm instance.
    ///
    /// The cache eviction algorithm instance MUST hold the record and set its `IN_EVICTION` flag to true.
    fn push(&mut self, record: Arc<Record<Self>>);

    /// Push a record from the cache eviction algorithm instance.
    ///
    /// The cache eviction algorithm instance MUST remove the record and set its `IN_EVICTION` flag to false.
    fn pop(&mut self) -> Option<Arc<Record<Self>>>;

    /// Remove a record from the cache eviction algorithm instance.
    ///
    /// The caller guarantees that the record is in the cache eviction algorithm instance.
    ///
    /// The cache eviction algorithm instance MUST remove the record and set its `IN_EVICTION` flag to false.
    fn remove(&mut self, record: &Arc<Record<Self>>);

    /// Remove all records from the cache eviction algorithm instance.
    ///
    /// The cache eviction algorithm instance MUST remove the records and set its `IN_EVICTION` flag to false.
    fn clear(&mut self) {
        while self.pop().is_some() {}
    }

    /// `acquire` is called when an external caller acquire a cache entry from the cache.
    ///
    /// The entry can be EITHER in the cache eviction algorithm instance or not.
    fn acquire() -> Op<Self>;

    /// `release` is called when the last external caller drops the cache entry.
    ///
    /// The entry can be EITHER in the cache eviction algorithm instance or not.
    fn release() -> Op<Self>;
}

pub mod fifo;
pub mod lfu;
pub mod lru;
pub mod s3fifo;
pub mod sieve;

#[cfg(any(test, feature = "test_utils"))]
pub mod test_utils;
