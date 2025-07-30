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
    metrics::Metrics,
    properties::{Age, Populated, Properties},
    runtime::BackgroundShutdownRuntime,
};
use foyer_memory::{Cache, Piece};
use tokio::runtime::Handle;

#[cfg(feature = "test_utils")]
use crate::test_utils::*;
use crate::{
    compress::Compression,
    engine::{
        large::engine::LargeObjectEngineBuilder,
        noop::{NoopEngine, NoopEngineBuilder},
        Engine, EngineBuildContext, EngineBuilder, Load, RecoverMode,
    },
    error::{Error, Result},
    io::{
        device::{
            noop::{NoopDevice, NoopDeviceBuilder},
            DeviceBuilder,
        },
        engine::{monitor::MonitoredIoEngine, noop::NoopIoEngineBuilder, psync::PsyncIoEngineBuilder, IoEngineBuilder},
        throttle::Throttle,
    },
    keeper::Keeper,
    runtime::Runtime,
    serde::EntrySerializer,
    statistics::Statistics,
    AdmissionPicker, AdmitAllPicker, ChainedAdmissionPickerBuilder, IoThrottlerPicker, IoThrottlerTarget, Pick,
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

    admission_picker: Arc<dyn AdmissionPicker>,
    load_throttler: Option<IoThrottlerPicker>,

    compression: Compression,

    runtime: Runtime,

    throttle: Throttle,
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
            .field("keeper", &self.inner.keeper)
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
        Q: Hash + Equivalent<K> + ?Sized + Send + Sync + 'static,
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

        if let Some(throttler) = self.inner.load_throttler.as_ref() {
            match throttler.pick(&self.inner.statistics, hash) {
                Pick::Admit => {}
                Pick::Reject => unreachable!(),
                Pick::Throttled(_) => {
                    if self.inner.engine.may_contains(hash) {
                        self.inner.metrics.storage_throttled.increase(1);
                        self.inner
                            .metrics
                            .storage_throttled_duration
                            .record(now.elapsed().as_secs_f64());
                        return Ok(Load::Throttled);
                    } else {
                        self.inner.metrics.storage_miss.increase(1);
                        self.inner
                            .metrics
                            .storage_miss_duration
                            .record(now.elapsed().as_secs_f64());
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

    /// Get the statistics information of the disk cache.
    pub fn statistics(&self) -> &Arc<Statistics> {
        &self.inner.statistics
    }

    /// Get the io throttle of the disk cache.
    pub fn throttle(&self) -> &Throttle {
        &self.inner.throttle
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
        self.inner.engine.type_id() != TypeId::of::<Arc<NoopEngine<K, V, P>>>()
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

    device_builder: Box<dyn DeviceBuilder>,
    io_engine_builder: Box<dyn IoEngineBuilder>,
    engine_builder: Box<dyn EngineBuilder<K, V, P>>,

    runtime_config: RuntimeOptions,

    admission_picker: Arc<dyn AdmissionPicker>,
    compression: Compression,
    recover_mode: RecoverMode,
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
            .field("device_builder", &self.device_builder)
            .field("io_engine_builder", &self.io_engine_builder)
            .field("engine_builder", &self.engine_builder)
            .field("runtime_config", &self.runtime_config)
            .field("admission_picker", &self.admission_picker)
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

            device_builder: Box::<NoopDeviceBuilder>::default(),
            io_engine_builder: PsyncIoEngineBuilder::new().boxed(),
            engine_builder: LargeObjectEngineBuilder::new().boxed(),

            runtime_config: RuntimeOptions::Disabled,

            admission_picker: Arc::<AdmitAllPicker>::default(),
            compression: Compression::default(),
            recover_mode: RecoverMode::default(),
        }
    }

    /// Set device builder for the disk cache store.
    pub fn with_device_builder(mut self, builder: impl Into<Box<dyn DeviceBuilder>>) -> Self {
        self.device_builder = builder.into();
        self
    }

    /// Set io engine builder for the disk cache store.
    pub fn with_io_engine_builder(mut self, builder: impl Into<Box<dyn IoEngineBuilder>>) -> Self {
        self.io_engine_builder = builder.into();
        self
    }

    /// Set engine builder for the disk cache store.
    pub fn with_engine_builder(mut self, builder: impl Into<Box<dyn EngineBuilder<K, V, P>>>) -> Self {
        self.engine_builder = builder.into();
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

    #[doc(hidden)]
    pub fn is_noop(&self) -> bool {
        (*self.device_builder).type_id() == TypeId::of::<Arc<NoopDeviceBuilder>>()
    }

    /// Build the disk cache store with the given configuration.
    pub async fn build(mut self) -> Result<Store<K, V, S, P>> {
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
                tracing::info!(
                    "[store]: Dedicated runtime is disabled. This may lead to spikes in latency under high load. Hint: Consider configuring a dedicated runtime."
                );
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

        let device = self.device_builder.build()?;
        let throttle = device.throttle().clone();
        let statistics = Arc::new(Statistics::new(throttle.iops_counter.clone()));
        if device.type_id() == TypeId::of::<Arc<NoopDevice>>() {
            tracing::info!("[store builder]: No device is provided, run disk cache in mock mode that do nothing.");
            self.io_engine_builder = Box::<NoopIoEngineBuilder>::default();
            self.engine_builder = Box::<NoopEngineBuilder<K, V, P>>::default();
        }

        let io_engine = self.io_engine_builder.build(runtime.clone())?;
        let io_engine = MonitoredIoEngine::new(io_engine, statistics.clone(), metrics.clone());
        let engine = self
            .engine_builder
            .build(EngineBuildContext {
                device,
                io_engine,
                metrics: metrics.clone(),
                statistics: statistics.clone(),
                runtime: runtime.clone(),
                recover_mode: self.recover_mode,
            })
            .await?;

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

        let keeper = Keeper::new(memory.shards());
        let hasher = memory.hash_builder().clone();
        let inner = StoreInner {
            hasher,
            keeper,
            engine,
            admission_picker,
            load_throttler,
            compression,
            runtime,
            throttle,
            metrics,
            statistics,

            #[cfg(feature = "test_utils")]
            load_throttle_switch: LoadThrottleSwitch::default(),
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
        engine::large::engine::LargeObjectEngineBuilder,
        io::{device::fs::FsDeviceBuilder, engine::psync::PsyncIoEngineBuilder},
    };

    #[tokio::test]
    async fn test_build_with_unaligned_buffer_pool_size() {
        let dir = tempfile::tempdir().unwrap();
        let metrics = Arc::new(Metrics::noop());
        let memory: Cache<u64, u64> = CacheBuilder::new(10).build();
        let _ = StoreBuilder::new("test", memory, metrics)
            .with_device_builder(FsDeviceBuilder::new(dir.path()).with_capacity(64 * 1024))
            .with_io_engine_builder(PsyncIoEngineBuilder::new())
            .with_engine_builder(
                LargeObjectEngineBuilder::new()
                    .with_flushers(3)
                    .with_region_size(16 * 1024)
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
            .with_device_builder(FsDeviceBuilder::new(dir.path()).with_capacity(4 * 1024 * 1024))
            .with_io_engine_builder(PsyncIoEngineBuilder::new())
            .with_engine_builder(LargeObjectEngineBuilder::new().with_region_size(16 * 1024))
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
