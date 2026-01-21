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

use std::{
    any::{Any, TypeId},
    borrow::Cow,
    fmt::Debug,
    hash::Hash,
    sync::Arc,
    time::Instant,
};

use equivalent::Equivalent;
use foyer_common::{
    code::{HashBuilder, StorageKey, StorageValue},
    error::Result,
    metrics::Metrics,
    properties::{Age, Properties},
    spawn::Spawner,
};
use foyer_memory::{Cache, Piece};

#[cfg(any(test, feature = "test_utils"))]
use crate::test_utils::*;
use crate::{
    compress::Compression,
    engine::{
        noop::{NoopEngine, NoopEngineConfig},
        Engine, EngineBuildContext, EngineConfig, Load, Populated, RecoverMode,
    },
    io::{
        device::{statistics::Statistics, throttle::Throttle, Device},
        engine::{monitor::MonitoredIoEngine, psync::PsyncIoEngineConfig, IoEngineBuildContext, IoEngineConfig},
    },
    keeper::Keeper,
    serde::EntrySerializer,
    StorageFilterResult,
};

/// The disk cache engine that serves as the storage backend of `foyer`.
pub struct Store<K, V, S, P>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
    P: Properties,
{
    inner: Arc<StoreInner<K, V, S, P>>,
}

struct StoreInner<K, V, S, P>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
    P: Properties,
{
    hasher: Arc<S>,

    keeper: Keeper<K, V, P>,
    engine: Arc<dyn Engine<K, V, P>>,

    compression: Compression,

    spawner: Spawner,

    metrics: Arc<Metrics>,

    #[cfg(any(test, feature = "test_utils"))]
    load_throttle_switch: LoadThrottleSwitch,
}

impl<K, V, S, P> Debug for Store<K, V, S, P>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
    P: Properties,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Store")
            .field("keeper", &self.inner.keeper)
            .field("engine", &self.inner.engine)
            .field("compression", &self.inner.compression)
            .field("runtimes", &self.inner.spawner)
            .finish()
    }
}

impl<K, V, S, P> Clone for Store<K, V, S, P>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
    P: Properties,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<K, V, S, P> Store<K, V, S, P>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
    P: Properties,
{
    /// Close the disk cache gracefully.
    ///
    /// `close` will wait for all ongoing flush and reclaim tasks to finish.
    pub async fn close(&self) -> Result<()> {
        self.inner.engine.close().await
    }

    /// Return if the given key can be picked by the admission filter.
    pub fn filter(&self, hash: u64, estimated_size: usize) -> StorageFilterResult {
        self.inner.engine.filter(hash, estimated_size)
    }

    /// Push a in-memory cache piece to the disk cache write queue.
    pub fn enqueue(&self, piece: Piece<K, V, P>, force: bool) {
        tracing::trace!(hash = piece.hash(), "[store]: enqueue piece");
        let now = Instant::now();

        if force
            || self
                .filter(
                    piece.hash(),
                    piece.key().estimated_size() + piece.value().estimated_size(),
                )
                .is_admitted()
        {
            let estimated_size = EntrySerializer::estimated_size(piece.key(), piece.value());
            let rpiece = self.inner.keeper.insert(piece);
            self.inner.engine.enqueue(rpiece, estimated_size);
        }

        self.inner.metrics.storage_enqueue.increase(1);
        self.inner
            .metrics
            .storage_enqueue_duration
            .record(now.elapsed().as_secs_f64());
    }

    /// Load a cache entry from the disk cache.
    pub async fn load<Q>(&self, key: &Q) -> Result<Load<K, V, P>>
    where
        Q: Hash + Equivalent<K> + ?Sized,
    {
        let now = Instant::now();

        let hash = self.inner.hasher.hash_one(key);

        if let Some(piece) = self.inner.keeper.get(hash, key) {
            tracing::trace!(hash, "[store]: load from keeper");
            return Ok(Load::Piece {
                piece,
                populated: Populated { age: Age::Young },
            });
        }

        #[cfg(feature = "test_utils")]
        if self.inner.load_throttle_switch.is_throttled() {
            self.inner.metrics.storage_throttled.increase(1);
            self.inner
                .metrics
                .storage_throttled_duration
                .record(now.elapsed().as_secs_f64());
            return Ok(Load::Throttled);
        }

        match self.inner.engine.load(hash).await {
            Ok(Load::Entry {
                key: k,
                value: v,
                populated: p,
            }) if key.equivalent(&k) => {
                self.inner.metrics.storage_hit.increase(1);
                self.inner
                    .metrics
                    .storage_hit_duration
                    .record(now.elapsed().as_secs_f64());
                Ok(Load::Entry {
                    key: k,
                    value: v,
                    populated: p,
                })
            }
            Ok(Load::Piece { piece, populated }) if key.equivalent(piece.key()) => {
                self.inner.metrics.storage_hit.increase(1);
                self.inner
                    .metrics
                    .storage_hit_duration
                    .record(now.elapsed().as_secs_f64());
                Ok(Load::Piece { piece, populated })
            }
            Ok(Load::Entry { .. }) | Ok(Load::Piece { .. }) | Ok(Load::Miss) => {
                self.inner.metrics.storage_miss.increase(1);
                self.inner
                    .metrics
                    .storage_miss_duration
                    .record(now.elapsed().as_secs_f64());
                Ok(Load::Miss)
            }
            Ok(Load::Throttled) => {
                self.inner.metrics.storage_throttled.increase(1);
                self.inner
                    .metrics
                    .storage_throttled_duration
                    .record(now.elapsed().as_secs_f64());
                Ok(Load::Throttled)
            }
            Err(e) => {
                self.inner.metrics.storage_error.increase(1);
                Err(e)
            }
        }
    }

    /// Delete the cache entry with the given key from the disk cache.
    pub fn delete<'a, Q>(&'a self, key: &'a Q)
    where
        Q: Hash + Equivalent<K> + ?Sized,
    {
        let now = Instant::now();

        let hash = self.inner.hasher.hash_one(key);
        self.inner.engine.delete(hash);

        self.inner.metrics.storage_delete.increase(1);
        self.inner
            .metrics
            .storage_delete_duration
            .record(now.elapsed().as_secs_f64());
    }

    /// Check if the disk cache contains a cached entry with the given key.
    ///
    /// `contains` may return a false-positive result if there is a hash collision with the given key.
    pub fn may_contains<Q>(&self, key: &Q) -> bool
    where
        Q: Hash + Equivalent<K> + ?Sized,
    {
        let hash = self.inner.hasher.hash_one(key);
        self.inner.engine.may_contains(hash)
    }

    /// Delete all cached entries of the disk cache.
    pub async fn destroy(&self) -> Result<()> {
        self.inner.engine.destroy().await
    }

    /// Get the device of the disk cache.
    pub fn device(&self) -> &Arc<dyn Device> {
        self.inner.engine.device()
    }

    /// Get the statistics information of the disk cache.
    pub fn statistics(&self) -> &Arc<Statistics> {
        self.inner.engine.device().statistics()
    }

    /// Get the io throttle of the disk cache.
    pub fn throttle(&self) -> &Throttle {
        self.inner.engine.device().statistics().throttle()
    }

    /// Get the spawner.
    pub fn spawner(&self) -> &Spawner {
        &self.inner.spawner
    }

    /// Wait for the ongoing flush and reclaim tasks to finish.
    pub async fn wait(&self) {
        self.inner.engine.wait().await
    }

    /// Return the estimated serialized size of the entry.
    pub fn entry_estimated_size(&self, key: &K, value: &V) -> usize {
        EntrySerializer::estimated_size(key, value)
    }

    /// Get the load throttle switch for the disk cache.
    #[cfg(feature = "test_utils")]
    pub fn load_throttle_switch(&self) -> &LoadThrottleSwitch {
        &self.inner.load_throttle_switch
    }

    /// If the disk cache is enabled.
    pub fn is_enabled(&self) -> bool {
        self.inner.engine.type_id() != TypeId::of::<Arc<NoopEngine<K, V, P>>>()
    }
}

/// The builder of the disk cache.
pub struct StoreBuilder<K, V, S, P>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
    P: Properties,
{
    name: Cow<'static, str>,
    memory: Cache<K, V, S, P>,
    metrics: Arc<Metrics>,

    io_engine_config: Option<Box<dyn IoEngineConfig>>,
    engine_config: Option<Box<dyn EngineConfig<K, V, P>>>,

    spawner: Option<Spawner>,

    compression: Compression,
    recover_mode: RecoverMode,

    #[cfg(any(test, feature = "test_utils"))]
    load_throttle_switch: LoadThrottleSwitch,
}

impl<K, V, S, P> Debug for StoreBuilder<K, V, S, P>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
    P: Properties,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreBuilder")
            .field("name", &self.name)
            .field("memory", &self.memory)
            .field("metrics", &self.metrics)
            .field("io_engine_builder", &self.io_engine_config)
            .field("engine_builder", &self.engine_config)
            .field("spawner", &self.spawner)
            .field("compression", &self.compression)
            .field("recover_mode", &self.recover_mode)
            .finish()
    }
}

impl<K, V, S, P> StoreBuilder<K, V, S, P>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
    P: Properties,
{
    /// Setup disk cache store for the given in-memory cache.
    pub fn new(name: impl Into<Cow<'static, str>>, memory: Cache<K, V, S, P>, metrics: Arc<Metrics>) -> Self {
        Self {
            name: name.into(),
            memory,
            metrics,

            io_engine_config: None,
            engine_config: None,

            spawner: None,

            compression: Compression::default(),
            recover_mode: RecoverMode::default(),
            #[cfg(any(test, feature = "test_utils"))]
            load_throttle_switch: LoadThrottleSwitch::default(),
        }
    }

    /// Set io engine config for the disk cache store.
    ///
    /// Default: [`crate::io::engine::psync::PsyncIoEngineConfig`].
    pub fn with_io_engine_config(mut self, io_engine_builder: impl Into<Box<dyn IoEngineConfig>>) -> Self {
        self.io_engine_config = Some(io_engine_builder.into());
        self
    }

    /// Set engine config for the disk cache store.
    pub fn with_engine_config(mut self, config: impl Into<Box<dyn EngineConfig<K, V, P>>>) -> Self {
        self.engine_config = Some(config.into());
        self
    }

    /// Set the compression algorithm of the disk cache store.
    ///
    /// Default: [`Compression::None`].
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Set the recover mode for the disk cache store.
    ///
    /// See more in [`RecoverMode`].
    ///
    /// Default: [`RecoverMode::Quiet`].
    pub fn with_recover_mode(mut self, recover_mode: RecoverMode) -> Self {
        self.recover_mode = recover_mode;
        self
    }

    /// Configure the task spawner for the disk cache store.
    ///
    /// By default, it will use the current spawner that built foyer.
    ///
    /// For example, with tokio, it will be `tokio::runtime::Handle::current()`.
    ///
    /// FYI: [`Spawner`] and [`Spawner::current()`]
    pub fn with_spawner(mut self, spawner: Spawner) -> Self {
        self.spawner = Some(spawner);
        self
    }

    /// Set the load throttle switch for the disk cache store.
    #[cfg(any(test, feature = "test_utils"))]
    pub fn with_load_throttle_switch(mut self, switch: LoadThrottleSwitch) -> Self {
        self.load_throttle_switch = switch;
        self
    }

    #[doc(hidden)]
    pub fn is_noop(&self) -> bool {
        self.engine_config.is_none()
    }

    /// Build the disk cache store with the given configuration.
    pub async fn build(self) -> Result<Store<K, V, S, P>> {
        let memory = self.memory;
        let metrics = self.metrics;

        let compression = self.compression;

        let spawner = self.spawner.unwrap_or_else(Spawner::current);

        let io_engine_builder = match self.io_engine_config {
            Some(builder) => builder,
            None => {
                tracing::info!("[store builder]: No I/O engine builder is provided, use `PsyncIoEngineConfig` with default parameters as default.");
                PsyncIoEngineConfig::new().boxed()
            }
        };
        let io_engine = io_engine_builder
            .build(IoEngineBuildContext {
                spawner: spawner.clone(),
            })
            .await?;
        let io_engine = MonitoredIoEngine::new(io_engine, metrics.clone());

        let engine_builder = match self.engine_config {
            Some(eb) => eb,
            None => {
                tracing::info!(
                    "[store builder]: No engine builder is provided, run disk cache in mock mode that do nothing."
                );

                Box::<NoopEngineConfig<K, V, P>>::default()
            }
        };

        let engine = engine_builder
            .build(EngineBuildContext {
                io_engine,
                metrics: metrics.clone(),
                spawner: spawner.clone(),
                recover_mode: self.recover_mode,
            })
            .await?;

        let keeper = Keeper::new(memory.shards());
        let hasher = memory.hash_builder().clone();
        #[cfg(any(test, feature = "test_utils"))]
        let load_throttle_switch = self.load_throttle_switch;
        let inner = StoreInner {
            hasher,
            keeper,
            engine,
            compression,
            spawner,
            metrics,
            #[cfg(any(test, feature = "test_utils"))]
            load_throttle_switch,
        };
        let inner = Arc::new(inner);
        let store = Store { inner };

        Ok(store)
    }
}

#[cfg(test)]
mod tests {
    use foyer_common::hasher::ModHasher;
    use foyer_memory::CacheBuilder;

    use super::*;
    use crate::{
        engine::block::engine::BlockEngineConfig,
        io::{device::fs::FsDeviceBuilder, engine::psync::PsyncIoEngineConfig},
        DeviceBuilder,
    };

    #[tokio::test]
    async fn test_build_with_unaligned_buffer_pool_size() {
        let dir = tempfile::tempdir().unwrap();
        let metrics = Arc::new(Metrics::noop());
        let memory: Cache<u64, u64> = CacheBuilder::new(10).build();
        let _ = StoreBuilder::new("test", memory, metrics)
            .with_io_engine_config(PsyncIoEngineConfig::new())
            .with_engine_config(
                BlockEngineConfig::new(
                    FsDeviceBuilder::new(dir.path())
                        .with_capacity(64 * 1024)
                        .build()
                        .unwrap(),
                )
                .with_flushers(3)
                .with_block_size(16 * 1024)
                .with_buffer_pool_size(128 * 1024 * 1024),
            )
            .build()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_entry_hash_collision() {
        let dir = tempfile::tempdir().unwrap();
        let metrics = Arc::new(Metrics::noop());
        let memory: Cache<u128, String, ModHasher> =
            CacheBuilder::new(10).with_hash_builder(ModHasher::default()).build();

        let e1 = memory.insert(1, "foo".to_string());
        let e2 = memory.insert(1 + 1 + u64::MAX as u128, "bar".to_string());

        assert_eq!(memory.hash(e1.key()), memory.hash(e2.key()));

        let store = StoreBuilder::new("test", memory, metrics)
            .with_io_engine_config(PsyncIoEngineConfig::new())
            .with_engine_config(
                BlockEngineConfig::new(
                    FsDeviceBuilder::new(dir.path())
                        .with_capacity(4 * 1024 * 1024)
                        .build()
                        .unwrap(),
                )
                .with_block_size(16 * 1024),
            )
            .build()
            .await
            .unwrap();

        store.enqueue(e1.piece(), true);
        store.enqueue(e2.piece(), true);
        store.wait().await;

        let l1 = store.load(e1.key()).await.unwrap();
        let l2 = store.load(e2.key()).await.unwrap();

        assert!(matches!(l1, Load::Miss));
        assert!(matches!(l2, Load::Entry { .. }));
        assert_eq!(l2.entry().unwrap().1, "bar");
    }
}
