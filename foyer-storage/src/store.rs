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

use std::{borrow::Cow, fmt::Debug, hash::Hash, marker::PhantomData, sync::Arc, time::Instant};

use equivalent::Equivalent;
use foyer_common::{
    bits,
    code::{HashBuilder, StorageKey, StorageValue},
    metrics::Metrics,
    properties::{Populated, Properties},
    runtime::BackgroundShutdownRuntime,
};
use foyer_memory::{Cache, Piece};
use tokio::runtime::Handle;

#[cfg(feature = "test_utils")]
use crate::test_utils::*;
use crate::{
    ChainedAdmissionPickerBuilder, Dev, DevExt, DirectFileDeviceOptions, DirectFsDeviceOptions, IoThrottlerPicker,
    Pick, Throttle,
    compress::Compression,
    device::{
        DeviceConfig, RegionId,
        monitor::{Monitored, MonitoredConfig},
    },
    engine::{EngineConfig, EngineEnum, SizeSelector},
    error::{Error, Result},
    io::PAGE,
    large::{generic::GenericLargeStorageConfig, recover::RecoverMode, tombstone::TombstoneLogConfig},
    picker::{
        AdmissionPicker, EvictionPicker, ReinsertionPicker,
        utils::{AdmitAllPicker, FifoPicker, InvalidRatioPicker, IoThrottlerTarget, RejectAllPicker},
    },
    runtime::Runtime,
    serde::EntrySerializer,
    small::generic::GenericSmallStorageConfig,
    statistics::Statistics,
    storage::{
        Storage,
        either::{EitherConfig, Order},
    },
};

/// Load result.
#[derive(Debug)]
pub enum Load<K, V> {
    /// Load entry success.
    Entry {
        /// The key of the entry.
        key: K,
        /// The value of the entry.
        value: V,
        /// The populated source context of the entry.
        populated: Populated,
    },
    /// The entry may be in the disk cache, the read io is throttled.
    Throttled,
    /// Disk cache miss.
    Miss,
}

impl<K, V> Load<K, V> {
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

    /// Check if the load result is a cache entry.
    pub fn is_entry(&self) -> bool {
        matches!(self, Load::Entry { .. })
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

    engine: EngineEnum<K, V, P>,

    admission_picker: Arc<dyn AdmissionPicker>,
    load_throttler: Option<IoThrottlerPicker>,

    compression: Compression,

    runtime: Runtime,

    statistics: Arc<Statistics>,
    metrics: Arc<Metrics>,

    #[cfg(feature = "test_utils")]
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
            .field("engine", &self.inner.engine)
            .field("admission_picker", &self.inner.admission_picker)
            .field("load_throttler", &self.inner.load_throttler)
            .field("compression", &self.inner.compression)
            .field("runtimes", &self.inner.runtime)
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

    /// Return if the given key can be picked by the admission picker.
    pub fn pick(&self, hash: u64) -> Pick {
        self.inner.admission_picker.pick(&self.inner.statistics, hash)
    }

    /// Push a in-memory cache piece to the disk cache write queue.
    pub fn enqueue(&self, piece: Piece<K, V, P>, force: bool) {
        tracing::trace!(hash = piece.hash(), "[store]: enqueue piece");
        let now = Instant::now();

        if force || self.pick(piece.hash()).admitted() {
            let estimated_size = EntrySerializer::estimated_size(piece.key(), piece.value());
            self.inner.engine.enqueue(piece, estimated_size);
        }

        self.inner.metrics.storage_enqueue.increase(1);
        self.inner
            .metrics
            .storage_enqueue_duration
            .record(now.elapsed().as_secs_f64());
    }

    /// Load a cache entry from the disk cache.
    pub async fn load<Q>(&self, key: &Q) -> Result<Load<K, V>>
    where
        Q: Hash + Equivalent<K> + ?Sized + Send + Sync + 'static,
    {
        let hash = self.inner.hasher.hash_one(key);

        #[cfg(feature = "test_utils")]
        if self.inner.load_throttle_switch.is_throttled() {
            return Ok(Load::Throttled);
        }

        if let Some(throttler) = self.inner.load_throttler.as_ref() {
            match throttler.pick(&self.inner.statistics, hash) {
                Pick::Admit => {}
                Pick::Reject => unreachable!(),
                Pick::Throttled(_) => {
                    if self.inner.engine.may_contains(hash) {
                        return Ok(Load::Throttled);
                    } else {
                        return Ok(Load::Miss);
                    }
                }
            }
        }

        let future = self.inner.engine.load(hash);
        match self.inner.runtime.read().spawn(future).await.unwrap() {
            Ok(Load::Entry {
                key: k,
                value: v,
                populated: p,
            }) if key.equivalent(&k) => Ok(Load::Entry {
                key: k,
                value: v,
                populated: p,
            }),
            Ok(Load::Entry { .. }) | Ok(Load::Miss) => Ok(Load::Miss),
            Ok(Load::Throttled) => Ok(Load::Throttled),
            Err(e) => Err(e),
        }
    }

    /// Delete the cache entry with the given key from the disk cache.
    pub fn delete<'a, Q>(&'a self, key: &'a Q)
    where
        Q: Hash + Equivalent<K> + ?Sized,
    {
        let hash = self.inner.hasher.hash_one(key);
        self.inner.engine.delete(hash)
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

    /// Get the statistics information of the disk cache.
    pub fn statistics(&self) -> &Arc<Statistics> {
        self.inner.engine.statistics()
    }

    /// Get the io throttle of the disk cache.
    pub fn throttle(&self) -> &Throttle {
        self.inner.engine.throttle()
    }

    /// Get the runtime.
    pub fn runtime(&self) -> &Runtime {
        &self.inner.runtime
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
        !matches! { self.inner.engine, EngineEnum::Noop(_)}
    }
}

/// The configurations for the device.
#[derive(Debug, Clone)]
pub enum DeviceOptions {
    /// No device.
    None,
    /// With device options.
    DeviceConfig(DeviceConfig),
}

impl From<DirectFileDeviceOptions> for DeviceOptions {
    fn from(options: DirectFileDeviceOptions) -> Self {
        Self::DeviceConfig(options.into())
    }
}

impl From<DirectFsDeviceOptions> for DeviceOptions {
    fn from(options: DirectFsDeviceOptions) -> Self {
        Self::DeviceConfig(options.into())
    }
}

/// [`Engine`] controls the ratio of the large object disk cache and the small object disk cache.
///
/// If [`Engine::Mixed`] is used, it will use the `Either` engine
/// with the small object disk cache as the left engine,
/// and the large object disk cache as the right engine.
#[derive(Debug)]
pub enum Engine {
    /// Large object disk cache storage engine.
    Large(LargeEngineOptions),
    /// Small object disk cache storage engine.
    Small(SmallEngineOptions),
    /// Mixed large object disk cache and small object disk cache.
    Mixed {
        /// The ratio of the small object disk cache.
        ///
        /// Range: [0 ~ 1]
        ratio: f64,
        /// Large object disk cache storage engine options.
        large: LargeEngineOptions,
        /// Small object disk cache storage engine options.
        small: SmallEngineOptions,
    },
}

impl Engine {
    /// Threshold for distinguishing small and large objects.
    pub const OBJECT_SIZE_THRESHOLD: usize = 2048;
    /// Check the large object disk cache first, for checking it does NOT involve disk ops.
    pub const MIXED_LOAD_ORDER: Order = Order::RightFirst;

    /// Large object disk cache storage engine with default configurations.
    pub fn large() -> Self {
        Self::Large(LargeEngineOptions::default())
    }

    /// Small object disk cache storage engine with default configurations.
    pub fn small() -> Self {
        Self::Small(SmallEngineOptions::default())
    }

    /// Mixed large and small object disk cache storage engine with default configurations.
    ///
    /// The ratio of the small object disk cache is set to `0.1`.
    pub fn mixed() -> Self {
        Self::Mixed {
            ratio: 0.1,
            large: LargeEngineOptions::default(),
            small: SmallEngineOptions::default(),
        }
    }
}

/// Tokio runtime configuration.
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TokioRuntimeOptions {
    /// Dedicated runtime worker threads.
    ///
    /// If the value is set to `0`, the dedicated will use the default worker threads of tokio.
    /// Which is 1 worker per core.
    ///
    /// See [`tokio::runtime::Builder::worker_threads`].
    pub worker_threads: usize,

    /// Max threads to run blocking io.
    ///
    /// If the value is set to `0`, use the tokio default value (which is 512).
    ///
    /// See [`tokio::runtime::Builder::max_blocking_threads`].
    pub max_blocking_threads: usize,
}

/// Options for the dedicated runtime.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum RuntimeOptions {
    /// Disable dedicated runtime. The runtime which foyer is built on will be used.
    Disabled,
    /// Use unified dedicated runtime for both reads and writes.
    Unified(TokioRuntimeOptions),
    /// Use separated dedicated runtime for reads or writes.
    Separated {
        /// Dedicated runtime for reads.
        read_runtime_options: TokioRuntimeOptions,
        /// Dedicated runtime for both foreground and background writes
        write_runtime_options: TokioRuntimeOptions,
    },
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

    device_options: DeviceOptions,
    engine: Engine,
    runtime_config: RuntimeOptions,

    admission_picker: Arc<dyn AdmissionPicker>,
    compression: Compression,
    recover_mode: RecoverMode,
    flush: bool,
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
            .field("device_options", &self.device_options)
            .field("engine", &self.engine)
            .field("runtime_config", &self.runtime_config)
            .field("admission_picker", &self.admission_picker)
            .field("compression", &self.compression)
            .field("recover_mode", &self.recover_mode)
            .field("flush", &self.flush)
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
    pub fn new(
        name: impl Into<Cow<'static, str>>,
        memory: Cache<K, V, S, P>,
        metrics: Arc<Metrics>,
        engine: Engine,
    ) -> Self {
        if matches!(engine, Engine::Mixed{ ratio, .. } if !(0.0..=1.0).contains(&ratio)) {
            panic!("mixed engine small object disk cache ratio must be a f64 in range [0.0, 1.0]");
        }

        Self {
            name: name.into(),
            memory,
            metrics,

            device_options: DeviceOptions::None,
            engine,
            runtime_config: RuntimeOptions::Disabled,

            admission_picker: Arc::<AdmitAllPicker>::default(),
            compression: Compression::default(),
            recover_mode: RecoverMode::default(),
            flush: false,
        }
    }

    /// Set device options for the disk cache store.
    pub fn with_device_options(mut self, device_options: impl Into<DeviceOptions>) -> Self {
        self.device_options = device_options.into();
        self
    }

    /// Enable/disable `sync` after writes.
    ///
    /// Default: `false`.
    pub fn with_flush(mut self, flush: bool) -> Self {
        self.flush = flush;
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

    /// Set the admission pickers for th disk cache store.
    ///
    /// The admission picker is used to pick the entries that can be inserted into the disk cache store.
    ///
    /// Default: [`AdmitAllPicker`].
    pub fn with_admission_picker(mut self, admission_picker: Arc<dyn AdmissionPicker>) -> Self {
        self.admission_picker = admission_picker;
        self
    }

    /// Configure the dedicated runtime for the disk cache store.
    pub fn with_runtime_options(mut self, runtime_options: RuntimeOptions) -> Self {
        self.runtime_config = runtime_options;
        self
    }

    /// Build the disk cache store with the given configuration.
    pub async fn build(self) -> Result<Store<K, V, S, P>> {
        let memory = self.memory.clone();
        let metrics = self.metrics.clone();
        let mut admission_picker = self.admission_picker.clone();

        let compression = self.compression;

        let build_runtime = |config: &TokioRuntimeOptions, suffix: &str| {
            let mut builder = tokio::runtime::Builder::new_multi_thread();
            #[cfg(madsim)]
            let _ = config;
            #[cfg(not(madsim))]
            if config.worker_threads != 0 {
                builder.worker_threads(config.worker_threads);
            }
            #[cfg(not(madsim))]
            if config.max_blocking_threads != 0 {
                builder.max_blocking_threads(config.max_blocking_threads);
            }
            builder.thread_name(format!("{}-{}", &self.name, suffix));
            let runtime = builder.enable_all().build().map_err(anyhow::Error::from)?;
            let runtime = BackgroundShutdownRuntime::from(runtime);
            Ok::<_, Error>(Arc::new(runtime))
        };

        let user_runtime_handle = Handle::current();
        let (read_runtime, write_runtime) = match self.runtime_config {
            RuntimeOptions::Disabled => {
                tracing::warn!("[store]: Dedicated runtime is disabled");
                (None, None)
            }
            RuntimeOptions::Unified(runtime_config) => {
                let runtime = build_runtime(&runtime_config, "unified")?;
                (Some(runtime.clone()), Some(runtime.clone()))
            }
            RuntimeOptions::Separated {
                read_runtime_options: read_runtime_config,
                write_runtime_options: write_runtime_config,
            } => {
                let read_runtime = build_runtime(&read_runtime_config, "read")?;
                let write_runtime = build_runtime(&write_runtime_config, "write")?;
                (Some(read_runtime), Some(write_runtime))
            }
        };
        let runtime = Runtime::new(read_runtime, write_runtime, user_runtime_handle);

        let engine = {
            let metrics = metrics.clone();
            let runtime = runtime.clone();
            // Use the user runtime to open engine.
            tokio::spawn(async move {
                match self.device_options {
                    DeviceOptions::None => {
                        tracing::warn!(
                            "[store builder]: No device config set. Use `NoneStore` which always returns `None` for queries."
                        );
                        EngineEnum::open(EngineConfig::Noop).await
                    }
                    DeviceOptions::DeviceConfig(options) => {
                        let device = match Monitored::open(MonitoredConfig {
                            config: options,
                            metrics: metrics.clone(),
                        }, runtime.clone())
                        .await {
                            Ok(device) => device,
                            Err(e) =>return Err(e),
                        };
                        match self.engine {
                            Engine::Large(large) => {
                                let regions = 0..device.regions() as RegionId;
                                EngineEnum::open(EngineConfig::Large(GenericLargeStorageConfig {
                                    device,
                                    regions,
                                    compression: self.compression,
                                    flush: self.flush,
                                    indexer_shards: large.indexer_shards,
                                    recover_mode: self.recover_mode,
                                    recover_concurrency: large.recover_concurrency,
                                    flushers: large.flushers,
                                    reclaimers: large.reclaimers,
                                    clean_region_threshold: large.clean_region_threshold.unwrap_or(large.reclaimers),
                                    eviction_pickers: large.eviction_pickers,
                                    reinsertion_picker: large.reinsertion_picker,
                                    tombstone_log_config: large.tombstone_log_config,
                                    buffer_pool_size: large.buffer_pool_size,
                                    blob_index_size: large.blob_index_size,
                                    submit_queue_size_threshold: large.submit_queue_size_threshold.unwrap_or(large.buffer_pool_size * 2),
                                    runtime,
                                    marker: PhantomData,
                                }))
                                .await
                            }
                            Engine::Small(small) => {
                                let regions = 0..device.regions() as RegionId;
                                EngineEnum::open(EngineConfig::Small(GenericSmallStorageConfig {
                                    set_size: small.set_size,
                                    set_cache_capacity: small.set_cache_capacity,
                                    set_cache_shards: small.set_cache_shards,
                                    device,
                                    regions,
                                    flush: self.flush,
                                    flushers: small.flushers,
                                    buffer_pool_size: small.buffer_pool_size,
                                    runtime,
                                    marker: PhantomData,
                                }))
                                .await
                            }
                            Engine::Mixed{ratio, large, small} => {
                                let small_region_count = std::cmp::max((device.regions() as f64 * ratio) as usize,1);
                                let small_regions = 0..small_region_count as RegionId;
                                let large_regions = small_region_count as RegionId..device.regions() as RegionId;
                                EngineEnum::open(EngineConfig::Mixed(EitherConfig {
                                    selector: SizeSelector::new(Engine::OBJECT_SIZE_THRESHOLD),
                                    left: GenericSmallStorageConfig {
                                        set_size: small.set_size,
                                        set_cache_capacity: small.set_cache_capacity,
                                        set_cache_shards: small.set_cache_shards,
                                        device: device.clone(),
                                        regions: small_regions,
                                        flush: self.flush,
                                        flushers: small.flushers,
                                        buffer_pool_size: small.buffer_pool_size,
                                        runtime: runtime.clone(),
                                        marker: PhantomData,
                                    },
                                    right: GenericLargeStorageConfig {
                                        device,
                                        regions: large_regions,
                                        compression: self.compression,
                                        flush: self.flush,
                                        indexer_shards: large.indexer_shards,
                                        recover_mode: self.recover_mode,
                                        recover_concurrency: large.recover_concurrency,
                                        flushers: large.flushers,
                                        reclaimers: large.reclaimers,
                                        clean_region_threshold: large.clean_region_threshold.unwrap_or(large.reclaimers),
                                        eviction_pickers: large.eviction_pickers,
                                        reinsertion_picker: large.reinsertion_picker,
                                        tombstone_log_config: large.tombstone_log_config,
                                        buffer_pool_size: large.buffer_pool_size,
                                        blob_index_size: large.blob_index_size,
                                        submit_queue_size_threshold: large.submit_queue_size_threshold.unwrap_or(large.buffer_pool_size * 2),
                                        runtime,
                                        marker: PhantomData,
                                    },
                                    load_order: Engine::MIXED_LOAD_ORDER,
                                }))
                                .await
                            }
                        }
                    }
                }
            }).await.unwrap()?
        };

        let statistics = engine.statistics().clone();

        let throttle = engine.throttle();
        if throttle.write_throughput.is_some() || throttle.write_iops.is_some() {
            tracing::debug!(?throttle, "[store builder]: Device is throttled.");
            admission_picker = Arc::new(
                ChainedAdmissionPickerBuilder::default()
                    .chain(Arc::new(IoThrottlerPicker::new(
                        IoThrottlerTarget::Write,
                        throttle.write_throughput,
                        throttle.write_iops,
                    )))
                    .chain(admission_picker)
                    .build(),
            );
        }
        let load_throttler = (throttle.read_throughput.is_some() || throttle.read_iops.is_some()).then_some(
            IoThrottlerPicker::new(IoThrottlerTarget::Read, throttle.read_throughput, throttle.read_iops),
        );

        let hasher = memory.hash_builder().clone();
        let inner = StoreInner {
            hasher,
            engine,
            admission_picker,
            load_throttler,
            compression,
            runtime,
            statistics,
            metrics,

            #[cfg(feature = "test_utils")]
            load_throttle_switch: LoadThrottleSwitch::default(),
        };
        let inner = Arc::new(inner);
        let store = Store { inner };

        Ok(store)
    }

    /// Return true the builder is based on a noop device.
    pub fn is_noop(&self) -> bool {
        matches! {self.device_options, DeviceOptions::None}
    }
}

/// Large object disk cache engine default options.
#[derive(Debug)]
pub struct LargeEngineOptions {
    indexer_shards: usize,
    recover_concurrency: usize,
    flushers: usize,
    reclaimers: usize,
    buffer_pool_size: usize,
    blob_index_size: usize,
    submit_queue_size_threshold: Option<usize>,
    clean_region_threshold: Option<usize>,
    eviction_pickers: Vec<Box<dyn EvictionPicker>>,
    reinsertion_picker: Arc<dyn ReinsertionPicker>,
    tombstone_log_config: Option<TombstoneLogConfig>,
}

impl Default for LargeEngineOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl LargeEngineOptions {
    /// Create large object disk cache engine default options.
    pub fn new() -> Self {
        Self {
            indexer_shards: 64,
            recover_concurrency: 8,
            flushers: 1,
            reclaimers: 1,
            buffer_pool_size: 16 * 1024 * 1024, // 16 MiB
            blob_index_size: 4 * 1024,          // 4 KiB
            submit_queue_size_threshold: None,
            clean_region_threshold: None,
            eviction_pickers: vec![Box::new(InvalidRatioPicker::new(0.8)), Box::<FifoPicker>::default()],
            reinsertion_picker: Arc::<RejectAllPicker>::default(),
            tombstone_log_config: None,
        }
    }

    /// Set the shard num of the indexer. Each shard has its own lock.
    ///
    /// Default: `64`.
    pub fn with_indexer_shards(mut self, indexer_shards: usize) -> Self {
        self.indexer_shards = indexer_shards;
        self
    }

    /// Set the recover concurrency for the disk cache store.
    ///
    /// Default: `8`.
    pub fn with_recover_concurrency(mut self, recover_concurrency: usize) -> Self {
        self.recover_concurrency = recover_concurrency;
        self
    }

    /// Set the flusher count for the disk cache store.
    ///
    /// The flusher count limits how many regions can be concurrently written.
    ///
    /// Default: `1`.
    pub fn with_flushers(mut self, flushers: usize) -> Self {
        self.flushers = flushers;
        self
    }

    /// Set the reclaimer count for the disk cache store.
    ///
    /// The reclaimer count limits how many regions can be concurrently reclaimed.
    ///
    /// Default: `1`.
    pub fn with_reclaimers(mut self, reclaimers: usize) -> Self {
        self.reclaimers = reclaimers;
        self
    }

    /// Set the total flush buffer pool size.
    ///
    /// Each flusher shares a volume at `threshold / flushers`.
    ///
    /// If the buffer of the flush queue exceeds the threshold, the further entries will be ignored.
    ///
    /// Default: 16 MiB.
    pub fn with_buffer_pool_size(mut self, buffer_pool_size: usize) -> Self {
        self.buffer_pool_size = buffer_pool_size;
        self
    }

    /// Set the blob index size for each blob.
    ///
    /// A larger blob index size can hold more blob entries, but it will also increase the io size of each blob part
    /// write.
    ///
    /// NOTE: The size will be aligned up to a multiplier of 4K.
    ///
    /// Default: 4 KiB
    pub fn with_blob_index_size(mut self, blob_index_size: usize) -> Self {
        let blob_index_size = bits::align_up(PAGE, blob_index_size);
        self.blob_index_size = blob_index_size;
        self
    }

    /// Set the submit queue size threshold.
    ///
    /// If the total entry estimated size in the submit queue exceeds the threshold, the further entries will be
    /// ignored.
    ///
    /// Default: `buffer_pool_size` * 2.
    pub fn with_submit_queue_size_threshold(mut self, submit_queue_size_threshold: usize) -> Self {
        self.submit_queue_size_threshold = Some(submit_queue_size_threshold);
        self
    }

    /// Set the clean region threshold for the disk cache store.
    ///
    /// The reclaimers only work when the clean region count is equal to or lower than the clean region threshold.
    ///
    /// Default: the same value as the `reclaimers`.
    pub fn with_clean_region_threshold(mut self, clean_region_threshold: usize) -> Self {
        self.clean_region_threshold = Some(clean_region_threshold);
        self
    }

    /// Set the eviction pickers for th disk cache store.
    ///
    /// The eviction picker is used to pick the region to reclaim.
    ///
    /// The eviction pickers are applied in order. If the previous eviction picker doesn't pick any region, the next one
    /// will be applied.
    ///
    /// If no eviction picker picks a region, a region will be picked randomly.
    ///
    /// Default: [ invalid ratio picker { threshold = 0.8 }, fifo picker ]
    pub fn with_eviction_pickers(mut self, eviction_pickers: Vec<Box<dyn EvictionPicker>>) -> Self {
        self.eviction_pickers = eviction_pickers;
        self
    }

    /// Set the reinsertion pickers for th disk cache store.
    ///
    /// The reinsertion picker is used to pick the entries that can be reinsertion into the disk cache store while
    /// reclaiming.
    ///
    /// Note: Only extremely important entries should be picked. If too many entries are picked, both insertion and
    /// reinsertion will be stuck.
    ///
    /// Default: [`RejectAllPicker`].
    pub fn with_reinsertion_picker(mut self, reinsertion_picker: Arc<dyn ReinsertionPicker>) -> Self {
        self.reinsertion_picker = reinsertion_picker;
        self
    }

    /// Enable the tombstone log with the given config.
    ///
    /// For updatable cache, either the tombstone log or [`RecoverMode::None`] must be enabled to prevent from the
    /// phantom entries after reopen.
    pub fn with_tombstone_log_config(mut self, tombstone_log_config: TombstoneLogConfig) -> Self {
        self.tombstone_log_config = Some(tombstone_log_config);
        self
    }
}

/// Small object disk cache engine default options.
#[derive(Debug)]
pub struct SmallEngineOptions {
    set_size: usize,
    set_cache_capacity: usize,
    set_cache_shards: usize,
    buffer_pool_size: usize,
    flushers: usize,
}

impl Default for SmallEngineOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// Create small object disk cache engine default options.
impl SmallEngineOptions {
    /// Create small object disk cache engine default options.
    pub fn new() -> Self {
        Self {
            set_size: 16 * 1024,    // 16 KiB
            set_cache_capacity: 64, // 64 sets
            set_cache_shards: 4,
            flushers: 1,
            buffer_pool_size: 4 * 1024 * 1024, // 4 MiB
        }
    }

    /// Set the set size of the set-associated cache.
    ///
    /// The set size will be 4K aligned.
    ///
    /// Default: 16 KiB
    pub fn with_set_size(mut self, set_size: usize) -> Self {
        bits::assert_aligned(PAGE, set_size);
        self.set_size = set_size;
        self
    }

    /// Set the capacity of the set cache.
    ///
    /// Count by set amount.
    ///
    /// Default: 64
    pub fn with_set_cache_capacity(mut self, set_cache_capacity: usize) -> Self {
        self.set_cache_capacity = set_cache_capacity;
        self
    }

    /// Set the shards of the set cache.
    ///
    /// Default: 4
    pub fn with_set_cache_shards(mut self, set_cache_shards: usize) -> Self {
        self.set_cache_shards = set_cache_shards;
        self
    }

    /// Set the total flush buffer pool size.
    ///
    /// Each flusher shares a volume at `threshold / flushers`.
    ///
    /// If the buffer of the flush queue exceeds the threshold, the further entries will be ignored.
    ///
    /// Default: 4 MiB.
    pub fn with_buffer_pool_size(mut self, buffer_pool_size: usize) -> Self {
        self.buffer_pool_size = buffer_pool_size;
        self
    }

    /// Set the flusher count for the disk cache store.
    ///
    /// The flusher count limits how many regions can be concurrently written.
    ///
    /// Default: `1`.
    pub fn with_flushers(mut self, flushers: usize) -> Self {
        self.flushers = flushers;
        self
    }
}

#[cfg(test)]
mod tests {
    use foyer_common::hasher::ModHasher;
    use foyer_memory::CacheBuilder;

    use super::*;

    #[tokio::test]
    async fn test_build_with_unaligned_buffer_pool_size() {
        let dir = tempfile::tempdir().unwrap();
        let metrics = Arc::new(Metrics::noop());
        let memory: Cache<u64, u64> = CacheBuilder::new(10).build();
        let _ = StoreBuilder::new(
            "test",
            memory,
            metrics,
            Engine::Large(
                LargeEngineOptions::new()
                    .with_flushers(3)
                    .with_buffer_pool_size(128 * 1024 * 1024),
            ),
        )
        .with_device_options(DirectFsDeviceOptions::new(dir.path()))
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

        let store = StoreBuilder::new("test", memory, metrics, Engine::Large(LargeEngineOptions::default()))
            .with_device_options(
                DirectFsDeviceOptions::new(dir.path())
                    .with_capacity(4 * 1024 * 1024)
                    .with_file_size(1024 * 1024),
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
