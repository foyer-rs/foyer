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

//! This crate provides a concurrent in-memory cache component that supports replaceable eviction algorithm.
//!
//! # Motivation
//!
//! There are a few goals to achieve with the crate:
//!
//! 1. Plug-and-Play eviction algorithm with the same abstraction.
//! 2. Tracking the real memory usage by the cache. Including both holding by the cache and by the external users.
//! 3. Reduce the concurrent read-through requests into one.
//!
//! To achieve them, the crate needs to combine the advantages of the implementations of RocksDB and CacheLib.
//!
//! # Components
//!
//! The cache is mainly composed of the following components:
//! 1. record             : Carries the cached entry, reference count, pointer links in the eviction container, etc.
//! 2. indexer            : Indexes cached keys to the records.
//! 3. eviction container : Defines the order of eviction. Usually implemented with intrusive data structures.
//!
//! Because a record needs to be referenced and mutated by both the indexer and the eviction container in the same
//! thread, it is hard to implement in 100% safe Rust without overhead. So, accessing the algorithm managed per-entry
//! state requires operation on the `UnsafeCell`.

mod cache;
mod error;
mod eviction;
mod indexer;
mod raw;
mod record;

mod prelude;
pub use prelude::*;
