//  Copyright 2024 Foyer Project Authors
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

use serde::{de::DeserializeOwned, Serialize};

use crate::handle::Handle;

pub trait EvictionConfig: Send + Sync + 'static + Clone + Serialize + DeserializeOwned + Default {}
impl<T> EvictionConfig for T where T: Send + Sync + 'static + Clone + Serialize + DeserializeOwned + Default {}

/// The lifetime of `handle: Self::H` is managed by [`Indexer`].
///
/// Each `handle`'s lifetime in [`Indexer`] must outlive the raw pointer in [`Eviction`].
pub trait Eviction: Send + Sync + 'static {
    type Handle: Handle;
    type Config: EvictionConfig;

    /// Create a new empty eviction container.
    ///
    /// # Safety
    unsafe fn new(capacity: usize, config: &Self::Config) -> Self
    where
        Self: Sized;

    /// Push a handle `ptr` into the eviction container.
    ///
    /// The caller guarantees that the `ptr` is NOT in the eviction container.
    ///
    /// # Safety
    ///
    /// The `ptr` must be kept holding until `pop` or `remove`.
    ///
    /// The base handle associated to the `ptr` must be set in cache.
    unsafe fn push(&mut self, ptr: NonNull<Self::Handle>);

    /// Pop a handle `ptr` from the eviction container.
    ///
    /// # Safety
    ///
    /// The `ptr` must be taken from the eviction container.
    /// Or it may become dangling and cause UB.
    ///
    /// The base handle associated to the `ptr` must be set NOT in cache.
    unsafe fn pop(&mut self) -> Option<NonNull<Self::Handle>>;

    /// Notify the eviction container that the `ptr` is acquired by **AN** external user.
    ///
    /// # Safety
    ///
    /// The given `ptr` can be EITHER in the eviction container OR not in the eviction container.
    unsafe fn acquire(&mut self, ptr: NonNull<Self::Handle>);

    /// Notify the eviction container that the `ptr` is released by **ALL** external users.
    ///
    /// # Safety
    ///
    /// The given `ptr` can be EITHER in the eviction container OR not in the eviction container.
    unsafe fn release(&mut self, ptr: NonNull<Self::Handle>);

    /// Remove the given `ptr` from the eviction container.
    ///
    /// /// The caller guarantees that the `ptr` is NOT in the eviction container.
    ///
    /// # Safety
    ///
    /// The `ptr` must be taken from the eviction container, otherwise it may become dangling and cause UB.
    ///
    /// The base handle associated to the `ptr` must be set NOT in cache.
    unsafe fn remove(&mut self, ptr: NonNull<Self::Handle>);

    /// Remove all `ptr`s from the eviction container and reset.
    ///
    /// # Safety
    ///
    /// All `ptr` must be taken from the eviction container, otherwise it may become dangling and cause UB.
    ///
    /// All base handles associated to the `ptr`s must be set NOT in cache.
    unsafe fn clear(&mut self) -> Vec<NonNull<Self::Handle>>;

    /// Return the count of the `ptr`s that in the eviction container.
    fn len(&self) -> usize;

    /// Return `true` if the eviction container is empty.
    fn is_empty(&self) -> bool;
}

pub mod fifo;
pub mod lfu;
pub mod lru;
pub mod s3fifo;

pub mod sanity;

#[cfg(test)]
pub mod test_utils;
