// Copyright 2025 foyer Project Authors
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

use foyer_common::code::{Key, Value};
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    error::Result,
    record::{CacheHint, Record},
};

pub trait Hint: Send + Sync + 'static + Clone + Default + From<CacheHint> + Into<CacheHint> {}
impl<T> Hint for T where T: Send + Sync + 'static + Clone + Default + From<CacheHint> + Into<CacheHint> {}

pub trait State: Send + Sync + 'static + Default {}
impl<T> State for T where T: Send + Sync + 'static + Default {}

pub trait Config: Send + Sync + 'static + Clone + Serialize + DeserializeOwned + Default {}
impl<T> Config for T where T: Send + Sync + 'static + Clone + Serialize + DeserializeOwned + Default {}

pub trait Context: Send + Sync + 'static + Clone {
    type Config: Config;
    fn init(config: &Self::Config) -> Self;
}

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
    /// perform immutable operation on cache hit
    Immutable(Box<dyn Fn(&E, &Arc<Record<E>>) + Send + Sync + 'static>),
    /// perform mutable operation on cache hit
    Mutable(Box<dyn FnMut(&mut E, &Arc<Record<E>>) + Send + Sync + 'static>),
    /// perform immutable operation on both hit and miss
    ImmutableCtx(Box<dyn Fn(&E, OpCtx<'_, E>) + Send + Sync + 'static>),
    /// perform mutable operation on both hit and miss
    MutableCtx(Box<dyn FnMut(&mut E, OpCtx<'_, E>) + Send + Sync + 'static>),
}

pub enum OpCtx<'a, E>
where
    E: Eviction,
{
    Hit { record: &'a Arc<Record<E>> },
    Miss { hash: u64 },
}

impl<E> Op<E>
where
    E: Eviction,
{
    /// no operation
    pub fn noop() -> Self {
        Self::Noop
    }

    /// perform immutable operation on cache hit
    pub fn immutable<F>(f: F) -> Self
    where
        F: Fn(&E, &Arc<Record<E>>) + Send + Sync + 'static,
    {
        Self::Immutable(Box::new(f))
    }

    /// perform mutable operation on cache hit
    pub fn mutable<F>(f: F) -> Self
    where
        F: FnMut(&mut E, &Arc<Record<E>>) + Send + Sync + 'static,
    {
        Self::Mutable(Box::new(f))
    }

    /// perform immutable operation on both hit and miss
    pub fn immutable_ctx<F>(f: F) -> Self
    where
        F: Fn(&E, OpCtx<'_, E>) + Send + Sync + 'static,
    {
        Self::ImmutableCtx(Box::new(f))
    }

    /// perform mutable operation on both hit and miss
    pub fn mutable_ctx<F>(f: F) -> Self
    where
        F: FnMut(&mut E, OpCtx<'_, E>) + Send + Sync + 'static,
    {
        Self::MutableCtx(Box::new(f))
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
    /// Hint for a cache entry. Can be used to support priority at the entry granularity.
    type Hint: Hint;
    /// State for a cache entry. Mutable state for maintaining the cache eviction algorithm implementation.
    type State: State;
    /// Shared context for all evction shards.
    type Context: Context<Config = Self::Config>;

    /// Create a new cache eviction algorithm instance with the given arguments.
    fn new(capacity: usize, config: &Self::Config, context: &Self::Context) -> Self;

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

/// Provides helper functions for developers.
pub trait EvictionExt: Eviction {
    fn acquire_immutable(&self, record: &Arc<Record<Self>>) {
        match Self::acquire() {
            Op::Immutable(f) => f(self, record),
            _ => unreachable!(),
        }
    }

    fn acquire_mutable(&mut self, record: &Arc<Record<Self>>) {
        match Self::acquire() {
            Op::Mutable(mut f) => f(self, record),
            _ => unreachable!(),
        }
    }

    fn acquire_immutable_ctx(&self, ctx: OpCtx<'_, Self>) {
        match Self::acquire() {
            Op::ImmutableCtx(f) => f(self, ctx),
            _ => unreachable!(),
        }
    }

    fn acquire_mutable_ctx(&mut self, ctx: OpCtx<'_, Self>) {
        match Self::acquire() {
            Op::MutableCtx(mut f) => f(self, ctx),
            _ => unreachable!(),
        }
    }

    fn release_immutable(&self, record: &Arc<Record<Self>>) {
        match Self::release() {
            Op::Immutable(f) => f(self, record),
            _ => unreachable!(),
        }
    }

    fn release_mutable(&mut self, record: &Arc<Record<Self>>) {
        match Self::release() {
            Op::Mutable(mut f) => f(self, record),
            _ => unreachable!(),
        }
    }
}

impl<E> EvictionExt for E where E: Eviction {}

pub mod fifo;
pub mod lfu;
pub mod lru;
pub mod s3fifo;

#[cfg(test)]
pub mod test_utils;
