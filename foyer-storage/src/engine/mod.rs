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

use std::{any::Any, fmt::Debug, sync::Arc};

use foyer_common::{
    code::{StorageKey, StorageValue},
    error::Result,
    metrics::Metrics,
    properties::{Age, Properties},
};
use foyer_memory::Piece;
use futures_core::future::BoxFuture;

use crate::{filter::StorageFilterResult, io::engine::IoEngine, keeper::PieceRef, Device, Runtime};

/// Source context for populated entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Populated {
    /// The age of the entry.
    pub age: Age,
}

/// Load result.
pub enum Load<K, V, P> {
    /// Load entry success.
    Entry {
        /// The key of the entry.
        key: K,
        /// The value of the entry.
        value: V,
        /// The populated context of the entry.
        populated: Populated,
    },
    /// Load entry success from disk cache write queue.
    Piece {
        /// The piece of the entry.
        piece: Piece<K, V, P>,
        /// The populated context of the entry.
        populated: Populated,
    },
    /// The entry may be in the disk cache, the read io is throttled.
    Throttled,
    /// Disk cache miss.
    Miss,
}

impl<K, V, P> Debug for Load<K, V, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Load::Entry { populated, .. } => f.debug_struct("Load::Entry").field("populated", populated).finish(),
            Load::Piece { piece, populated } => f
                .debug_struct("Load::Piece")
                .field("piece", piece)
                .field("populated", populated)
                .finish(),
            Load::Throttled => f.debug_struct("Load::Throttled").finish(),
            Load::Miss => f.debug_struct("Load::Miss").finish(),
        }
    }
}

impl<K, V, P> Load<K, V, P> {
    /// Return `Some` with the entry if load success, otherwise return `None`.
    pub fn entry(self) -> Option<(K, V, Populated)> {
        match self {
            Load::Entry { key, value, populated } => Some((key, value, populated)),
            _ => None,
        }
    }

    /// Return `Some` with the entry if load success, otherwise return `None`.
    ///
    /// Only key and value will be returned.
    pub fn kv(self) -> Option<(K, V)> {
        match self {
            Load::Entry { key, value, .. } => Some((key, value)),
            _ => None,
        }
    }

    /// Check if the load result is a cache miss.
    pub fn is_miss(&self) -> bool {
        matches!(self, Load::Miss)
    }

    /// Check if the load result is miss caused by io throttled.
    pub fn is_throttled(&self) -> bool {
        matches!(self, Load::Throttled)
    }
}

/// The recover mode of the disk cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
pub enum RecoverMode {
    /// Do not recover disk cache.
    ///
    /// For updatable cache, either [`RecoverMode::None`] or the tombstone log must be used to prevent from phantom
    /// entry when reopen.
    None,
    /// Recover disk cache and skip errors.
    #[default]
    Quiet,
    /// Recover disk cache and panic on errors.
    Strict,
}

/// Context for building the disk cache engine.
pub struct EngineBuildContext {
    /// IO engine for the disk cache engine.
    pub io_engine: Arc<dyn IoEngine>,
    /// Shared metrics for all components.
    pub metrics: Arc<Metrics>,
    /// The runtime for the disk cache engine.
    pub runtime: Runtime,
    /// The recover mode of the disk cache engine.
    pub recover_mode: RecoverMode,
}

/// Disk cache engine builder trait.
#[expect(clippy::type_complexity)]
pub trait EngineConfig<K, V, P>: Send + Sync + 'static + Debug
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    /// Build the engine with the given configurations.
    fn build(self: Box<Self>, ctx: EngineBuildContext) -> BoxFuture<'static, Result<Arc<dyn Engine<K, V, P>>>>;

    /// Box the builder.
    fn boxed(self) -> Box<Self>
    where
        Self: Sized,
    {
        Box::new(self)
    }
}

/// Disk cache engine trait.
pub trait Engine<K, V, P>: Send + Sync + 'static + Debug + Any
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    /// Get the device used by this disk cache engine.
    fn device(&self) -> &Arc<dyn Device>;

    /// Return if the given key can be picked by the disk cache engine.
    fn filter(&self, hash: u64, estimated_size: usize) -> StorageFilterResult;

    /// Push a in-memory cache piece to the disk cache write queue.
    fn enqueue(&self, piece: PieceRef<K, V, P>, estimated_size: usize);

    /// Load a cache entry from the disk cache.
    ///
    /// `load` may return a false-positive result on entry key hash collision. It's the caller's responsibility to
    /// check if the returned key matches the given key.
    fn load(&self, hash: u64) -> BoxFuture<'static, Result<Load<K, V, P>>>;

    /// Delete the cache entry with the given key from the disk cache.
    fn delete(&self, hash: u64);

    /// Check if the disk cache contains a cached entry with the given key.
    ///
    /// `contains` may return a false-positive result if there is a hash collision with the given key.
    fn may_contains(&self, hash: u64) -> bool;

    /// Delete all cached entries of the disk cache.
    fn destroy(&self) -> BoxFuture<'static, Result<()>>;

    /// Wait for the ongoing flush and reclaim tasks to finish.
    fn wait(&self) -> BoxFuture<'static, ()>;

    /// Close the disk cache gracefully.
    ///
    /// `close` will wait for all ongoing flush and reclaim tasks to finish.
    fn close(&self) -> BoxFuture<'static, Result<()>>;
}

pub mod block;
pub mod noop;
