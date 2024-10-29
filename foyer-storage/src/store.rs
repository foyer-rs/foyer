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

use std::{
    fmt::{Debug, Display},
    hash::Hash,
    marker::PhantomData,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::Instant,
};

use ahash::RandomState;
use equivalent::Equivalent;
use foyer_common::{
    bits,
    code::{HashBuilder, StorageKey, StorageValue},
    metrics::Metrics,
    runtime::BackgroundShutdownRuntime,
};
use foyer_memory::{Cache, CacheEntry};
use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;

use crate::{
    compress::Compression,
    device::{
        monitor::{DeviceStats, Monitored, MonitoredConfig},
        DeviceConfig, RegionId, ALIGN,
    },
    engine::{EngineConfig, EngineEnum, SizeSelector},
    error::{Error, Result},
    large::{generic::GenericLargeStorageConfig, recover::RecoverMode, tombstone::TombstoneLogConfig},
    manifest::Manifest,
    picker::{
        utils::{AdmitAllPicker, FifoPicker, InvalidRatioPicker, RejectAllPicker},
        AdmissionPicker, EvictionPicker, ReinsertionPicker,
    },
    runtime::Runtime,
    serde::EntrySerializer,
    small::generic::GenericSmallStorageConfig,
    statistics::Statistics,
    storage::{
        either::{EitherConfig, Order},
        Storage,
    },
    Dev, DevExt, DirectFileDeviceOptions, DirectFsDeviceOptions,
};

/// The disk cache engine that serves as the storage backend of `foyer`.
pub struct Store<K, V, S = RandomState>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    inner: Arc<StoreInner<K, V, S>>,
}

struct StoreInner<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    memory: Cache<K, V, S>,

    engine: EngineEnum<K, V, S>,

    admission_picker: Arc<dyn AdmissionPicker<Key = K>>,

    compression: Compression,

    runtime: Runtime,

    statistics: Arc<Statistics>,
    metrics: Arc<Metrics>,
}

impl<K, V, S> Debug for Store<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Store")
            .field("memory", &self.inner.memory)
            .field("engine", &self.inner.engine)
            .field("admission_picker", &self.inner.admission_picker)
            .field("compression", &self.inner.compression)
            .field("runtimes", &self.inner.runtime)
            .finish()
    }
}

impl<K, V, S> Clone for Store<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<K, V, S> Store<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    /// Close the disk cache gracefully.
    ///
    /// `close` will wait for all ongoing flush and reclaim tasks to finish.
    pub async fn close(&self) -> Result<()> {
        self.inner.engine.close().await
    }

    /// Return if the given key can be picked by the admission picker.
    pub fn pick(&self, key: &K) -> bool {
        self.inner.admission_picker.pick(&self.inner.statistics, key)
    }

    /// Push a in-memory cache entry to the disk cache write queue.
    pub fn enqueue(&self, entry: CacheEntry<K, V, S>, force: bool) {
        let now = Instant::now();

        if force || self.pick(entry.key()) {
            let estimated_size = EntrySerializer::estimated_size(entry.key(), entry.value());
            self.inner.engine.enqueue(entry, estimated_size);
        }

        self.inner.metrics.storage_enqueue.increment(1);
        self.inner.metrics.storage_enqueue_duration.record(now.elapsed());
    }

    /// Load a cache entry from the disk cache.
    pub async fn load<Q>(&self, key: &Q) -> Result<Option<(K, V)>>
    where
        Q: Hash + Equivalent<K> + ?Sized + Send + Sync + 'static,
    {
        let hash = self.inner.memory.hash(key);
        let future = self.inner.engine.load(hash);
        match self.inner.runtime.read().spawn(future).await.unwrap() {
            Ok(Some((k, v))) if key.equivalent(&k) => Ok(Some((k, v))),
            Ok(_) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Delete the cache entry with the given key from the disk cache.
    pub fn delete<'a, Q>(&'a self, key: &'a Q)
    where
        Q: Hash + Equivalent<K> + ?Sized,
    {
        let hash = self.inner.memory.hash(key);
        self.inner.engine.delete(hash)
    }

    /// Check if the disk cache contains a cached entry with the given key.
    ///
    /// `contains` may return a false-positive result if there is a hash collision with the given key.
    pub fn may_contains<Q>(&self, key: &Q) -> bool
    where
        Q: Hash + Equivalent<K> + ?Sized,
    {
        let hash = self.inner.memory.hash(key);
        self.inner.engine.may_contains(hash)
    }

    /// Delete all cached entries of the disk cache.
    pub async fn destroy(&self) -> Result<()> {
        self.inner.engine.destroy().await
    }

    /// Get the statistics information of the disk cache.
    pub fn stats(&self) -> Arc<DeviceStats> {
        self.inner.engine.stats()
    }

    /// Get the runtime.
    pub fn runtime(&self) -> &Runtime {
        &self.inner.runtime
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
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Engine {
    /// All space are used as the large object disk cache.
    Large,
    /// All space are used as the small object disk cache.
    Small,
    /// Mixed the large object disk cache and the small object disk cache.
    ///
    /// The argument controls the ratio of the small object disk cache.
    ///
    /// Range: [0 ~ 1]
    Mixed(f64),
}

impl Default for Engine {
    fn default() -> Self {
        // TODO(MrCroxx): Use Mixed cache after small object disk cache is ready.
        Self::Large
    }
}

impl Engine {
    /// Threshold for distinguishing small and large objects.
    pub const OBJECT_SIZE_THRESHOLD: usize = 2048;
    /// Check the large object disk cache first, for checking it does NOT involve disk ops.
    pub const MIXED_LOAD_ORDER: Order = Order::RightFirst;

    /// Default large object disk cache only config.
    pub fn large() -> Self {
        Self::Large
    }

    /// Default small object disk cache only config.
    pub fn small() -> Self {
        Self::Small
    }

    /// Default mixed large object disk cache and small object disk cache config.
    pub fn mixed() -> Self {
        Self::Mixed(0.1)
    }
}

impl Display for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Engine::Large => write!(f, "large"),
            Engine::Small => write!(f, "small"),
            Engine::Mixed(ratio) => write!(f, "mixed({ratio})"),
        }
    }
}

impl FromStr for Engine {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        const MIXED_PREFIX: &str = "mixed=";

        match s {
            "large" => return Ok(Engine::Large),
            "small" => return Ok(Engine::Small),
            _ => {}
        }

        if s.starts_with(MIXED_PREFIX) {
            if let Ok(ratio) = s[MIXED_PREFIX.len()..s.len()].parse::<f64>() {
                return Ok(Engine::Mixed(ratio));
            }
        }

        Err(format!("invalid input: {s}"))
    }
}

/// Tokio runtime configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
pub struct StoreBuilder<K, V, S = RandomState>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    name: String,
    memory: Cache<K, V, S>,

    device_options: DeviceOptions,
    engine: Engine,
    runtime_config: RuntimeOptions,

    manifest_file_path: Option<PathBuf>,

    admission_picker: Arc<dyn AdmissionPicker<Key = K>>,
    compression: Compression,
    recover_mode: RecoverMode,
    flush: bool,

    large: LargeEngineOptions<K, V, S>,
    small: SmallEngineOptions<K, V, S>,
}

impl<K, V, S> StoreBuilder<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    /// Setup disk cache store for the given in-memory cache.
    pub fn new(memory: Cache<K, V, S>, engine: Engine) -> Self {
        if matches!(engine, Engine::Mixed(ratio) if !(0.0..=1.0).contains(&ratio)) {
            panic!("mixed engine small object disk cache ratio must be a f64 in range [0.0, 1.0]");
        }

        Self {
            name: "foyer".to_string(),
            memory,

            device_options: DeviceOptions::None,
            engine,
            runtime_config: RuntimeOptions::Disabled,

            manifest_file_path: None,

            admission_picker: Arc::<AdmitAllPicker<K>>::default(),
            compression: Compression::None,
            recover_mode: RecoverMode::Quiet,
            flush: false,

            large: LargeEngineOptions::new(),
            small: SmallEngineOptions::new(),
        }
    }

    /// Set the name of the foyer disk cache instance.
    ///
    /// foyer will use the name as the prefix of the metric names.
    ///
    /// Default: `foyer`.
    pub fn with_name(mut self, name: &str) -> Self {
        self.name = name.to_string();
        self
    }

    /// Set device options for the disk cache store.
    pub fn with_device_options(mut self, device_options: impl Into<DeviceOptions>) -> Self {
        self.device_options = device_options.into();
        self
    }

    /// Set the path for the manifest file.
    ///
    /// The manifest file is used to tracking the watermark for fast disk cache clearing.
    ///
    /// When creating a disk cache on a fs, it is not required to set the manifest file path.
    ///
    /// When creating a disk cache on a raw device, it is required to set the manifest file path.
    pub fn with_manifest_file_path(mut self, path: impl AsRef<Path>) -> Self {
        self.manifest_file_path = Some(path.as_ref().into());
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
    pub fn with_admission_picker(mut self, admission_picker: Arc<dyn AdmissionPicker<Key = K>>) -> Self {
        self.admission_picker = admission_picker;
        self
    }

    /// Configure the dedicated runtime for the disk cache store.
    pub fn with_runtime_options(mut self, runtime_options: RuntimeOptions) -> Self {
        self.runtime_config = runtime_options;
        self
    }

    /// Setup the large object disk cache engine with the given options.
    ///
    /// Otherwise, the default options will be used. See [`LargeEngineOptions`].
    pub fn with_large_object_disk_cache_options(mut self, options: LargeEngineOptions<K, V, S>) -> Self {
        if matches!(self.engine, Engine::Small { .. }) {
            tracing::warn!("[store builder]: Setting up large object disk cache options, but only small object disk cache is enabled.");
        }
        self.large = options;
        self
    }

    /// Setup the small object disk cache engine with the given options.
    ///
    /// Otherwise, the default options will be used. See [`SmallEngineOptions`].
    pub fn with_small_object_disk_cache_options(mut self, options: SmallEngineOptions<K, V, S>) -> Self {
        if matches!(self.engine, Engine::Large { .. }) {
            tracing::warn!("[store builder]: Setting up small object disk cache options, but only large object disk cache is enabled.");
        }
        self.small = options;
        self
    }

    /// Build the disk cache store with the given configuration.
    pub async fn build(self) -> Result<Store<K, V, S>> {
        let memory = self.memory.clone();
        let admission_picker = self.admission_picker.clone();

        let metrics = Arc::new(Metrics::new(&self.name));
        let statistics = Arc::<Statistics>::default();

        let compression = self.compression;

        let build_runtime = |config: &TokioRuntimeOptions, suffix: &str| {
            let mut builder = tokio::runtime::Builder::new_multi_thread();
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

        let manifest_file_path = match &self.device_options {
            DeviceOptions::None => "phantom".into(),
            DeviceOptions::DeviceConfig(DeviceConfig::DirectFile(_)) => self
                .manifest_file_path
                .expect("manifest file path must be set when using a direct file device for disk cache"),
            DeviceOptions::DeviceConfig(DeviceConfig::DirectFs(config)) => self
                .manifest_file_path
                .unwrap_or_else(|| config.dir().join(Manifest::DEFAULT_FILENAME)),
        };
        let manifest = Manifest::open(manifest_file_path, self.flush, runtime.clone()).await?;

        let engine = {
            let statistics = statistics.clone();
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
                        Engine::Large => {
                            let regions = 0..device.regions() as RegionId;
                            EngineEnum::open(EngineConfig::Large(GenericLargeStorageConfig {
                                name: self.name,
                                device,
                                manifest,
                                regions,
                                compression: self.compression,
                                flush: self.flush,
                                indexer_shards: self.large.indexer_shards,
                                recover_mode: self.recover_mode,
                                recover_concurrency: self.large.recover_concurrency,
                                flushers: self.large.flushers,
                                reclaimers: self.large.reclaimers,
                                clean_region_threshold: self.large.clean_region_threshold.unwrap_or(self.large.reclaimers),
                                eviction_pickers: self.large.eviction_pickers,
                                reinsertion_picker: self.large.reinsertion_picker,
                                tombstone_log_config: self.large.tombstone_log_config,
                                buffer_pool_size: self.large.buffer_pool_size,
                                submit_queue_size_threshold: self.large.submit_queue_size_threshold.unwrap_or(self.large.buffer_pool_size * 2),
                                statistics: statistics.clone(),
                                runtime,
                                marker: PhantomData,
                            }))
                            .await
                        }
                        Engine::Small => {
                            let regions = 0..device.regions() as RegionId;
                            EngineEnum::open(EngineConfig::Small(GenericSmallStorageConfig {
                                set_size: self.small.set_size,
                                set_cache_capacity: self.small.set_cache_capacity,
                                set_cache_shards: self.small.set_cache_shards,
                                device,
                                manifest,
                                regions,
                                flush: self.flush,
                                flushers: self.small.flushers,
                                buffer_pool_size: self.small.buffer_pool_size,
                                statistics: statistics.clone(),
                                runtime,
                                marker: PhantomData,
                            }))
                            .await
                        }
                        Engine::Mixed(ratio) => {
                            let small_region_count = std::cmp::max((device.regions() as f64 * ratio) as usize,1);
                            let small_regions = 0..small_region_count as RegionId;
                            let large_regions = small_region_count as RegionId..device.regions() as RegionId;
                            EngineEnum::open(EngineConfig::Mixed(EitherConfig {
                                selector: SizeSelector::new(Engine::OBJECT_SIZE_THRESHOLD),
                                left: GenericSmallStorageConfig {
                                    set_size: self.small.set_size,
                                    set_cache_capacity: self.small.set_cache_capacity,
                                    set_cache_shards: self.small.set_cache_shards,
                                    device: device.clone(),
                                    manifest: manifest.clone(),
                                    regions: small_regions,
                                    flush: self.flush,
                                    flushers: self.small.flushers,
                                    buffer_pool_size: self.small.buffer_pool_size,
                                    statistics: statistics.clone(),
                                    runtime: runtime.clone(),
                                    marker: PhantomData,
                                },
                                right: GenericLargeStorageConfig {
                                    name: self.name,
                                    device,
                                    manifest,
                                    regions: large_regions,
                                    compression: self.compression,
                                    flush: self.flush,
                                    indexer_shards: self.large.indexer_shards,
                                    recover_mode: self.recover_mode,
                                    recover_concurrency: self.large.recover_concurrency,
                                    flushers: self.large.flushers,
                                    reclaimers: self.large.reclaimers,
                                    clean_region_threshold: self.large.clean_region_threshold.unwrap_or(self.large.reclaimers),
                                    eviction_pickers: self.large.eviction_pickers,
                                    reinsertion_picker: self.large.reinsertion_picker,
                                    tombstone_log_config: self.large.tombstone_log_config,
                                    buffer_pool_size: self.large.buffer_pool_size,
                                    submit_queue_size_threshold: self.large.submit_queue_size_threshold.unwrap_or(self.large.buffer_pool_size * 2),
                                    statistics: statistics.clone(),
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

        let inner = StoreInner {
            memory,
            engine,
            admission_picker,
            compression,
            runtime,
            statistics,
            metrics,
        };
        let inner = Arc::new(inner);
        let store = Store { inner };

        Ok(store)
    }
}

/// Large object disk cache engine default options.
pub struct LargeEngineOptions<K, V, S = RandomState>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    indexer_shards: usize,
    recover_concurrency: usize,
    flushers: usize,
    reclaimers: usize,
    buffer_pool_size: usize,
    submit_queue_size_threshold: Option<usize>,
    clean_region_threshold: Option<usize>,
    eviction_pickers: Vec<Box<dyn EvictionPicker>>,
    reinsertion_picker: Arc<dyn ReinsertionPicker<Key = K>>,
    tombstone_log_config: Option<TombstoneLogConfig>,

    _marker: PhantomData<(K, V, S)>,
}

impl<K, V, S> Default for LargeEngineOptions<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V, S> LargeEngineOptions<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    /// Create large object disk cache engine default options.
    pub fn new() -> Self {
        Self {
            indexer_shards: 64,
            recover_concurrency: 8,
            flushers: 1,
            reclaimers: 1,
            buffer_pool_size: 16 * 1024 * 1024, // 16 MiB
            submit_queue_size_threshold: None,
            clean_region_threshold: None,
            eviction_pickers: vec![Box::new(InvalidRatioPicker::new(0.8)), Box::<FifoPicker>::default()],
            reinsertion_picker: Arc::<RejectAllPicker<K>>::default(),
            tombstone_log_config: None,
            _marker: PhantomData,
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

    /// Set the submit queue size threshold.
    ///
    /// If the total entry estimated size in the submit queue exceeds the threshold, the further entries will be
    /// ignored.
    ///
    /// Default: `buffer_pool_size`` * 2.
    pub fn with_submit_queue_size_threshold(mut self, buffer_pool_size: usize) -> Self {
        self.buffer_pool_size = buffer_pool_size;
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
    pub fn with_reinsertion_picker(mut self, reinsertion_picker: Arc<dyn ReinsertionPicker<Key = K>>) -> Self {
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
pub struct SmallEngineOptions<K, V, S = RandomState>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    set_size: usize,
    set_cache_capacity: usize,
    set_cache_shards: usize,
    buffer_pool_size: usize,
    flushers: usize,

    _marker: PhantomData<(K, V, S)>,
}

impl<K, V, S> Default for SmallEngineOptions<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn default() -> Self {
        Self::new()
    }
}

/// Create small object disk cache engine default options.
impl<K, V, S> SmallEngineOptions<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    /// Create small object disk cache engine default options.
    pub fn new() -> Self {
        Self {
            set_size: 16 * 1024,    // 16 KiB
            set_cache_capacity: 64, // 64 sets
            set_cache_shards: 4,
            flushers: 1,
            buffer_pool_size: 4 * 1024 * 1024, // 4 MiB
            _marker: PhantomData,
        }
    }

    /// Set the set size of the set-associated cache.
    ///
    /// The set size will be 4K aligned.
    ///
    /// Default: 16 KiB
    pub fn with_set_size(mut self, set_size: usize) -> Self {
        bits::assert_aligned(ALIGN, set_size);
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
