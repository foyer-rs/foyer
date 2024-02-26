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

use std::{hash::BuildHasher, ptr::NonNull};

use crate::{cache::CacheConfig, handle::Handle};

/// The lifetime of `handle: Self::H` is managed by [`Indexer`].
///
/// Each `handle`'s lifetime in [`Indexer`] must outlive the raw pointer in [`Eviction`].
pub trait Eviction: Send + Sync + 'static {
    type Handle: Handle;
    type Config;

    /// Create a new empty eviction container.
    ///
    /// # Safety
    unsafe fn new<S: BuildHasher>(config: &CacheConfig<Self, S>) -> Self
    where
        Self: Sized;

    /// Push a handle `ptr` into the eviction container.
    ///
    /// # Safety
    ///
    /// The `ptr` must be kept holding until `pop` or `remove`.
    unsafe fn push(&mut self, ptr: NonNull<Self::Handle>);

    /// Pop a handle `ptr` from the eviction container.
    ///
    /// # Safety
    ///
    /// The `ptr` must be taken from the eviction container.
    /// Or it may become dangling and cause UB.
    unsafe fn pop(&mut self) -> Option<NonNull<Self::Handle>>;

    /// Notify the eviciton container that the `ptr` is accessed.
    /// The eviction container can update its statistics.
    ///
    /// # Safety
    ///
    /// The given `ptr` can be in the eviction container or not in the eviction container.
    unsafe fn access(&mut self, ptr: NonNull<Self::Handle>);

    /// Remove the given `ptr` from the eviction container.
    ///
    /// # Safety
    ///
    /// The `ptr` must be taken from the eviction container.
    /// Or it may become dangling and cause UB.
    unsafe fn remove(&mut self, ptr: NonNull<Self::Handle>);

    /// Remove all `ptr`s from the eviction container and reset.
    ///
    /// # Safety
    ///
    /// All `ptr` must be taken from the eviction container.
    /// Or it may become dangling and cause UB.
    unsafe fn clear(&mut self) -> Vec<NonNull<Self::Handle>>;

    /// Return the count of the `ptr`s that in the eviction container.
    ///
    /// # Safety
    unsafe fn len(&self) -> usize;

    /// Return `true` if the eviction container is empty.
    ///
    /// # Safety
    unsafe fn is_empty(&self) -> bool;
}

pub mod fifo;
pub mod lru;
