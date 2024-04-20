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

use foyer_common::code::{StorageKey, StorageValue};
use foyer_memory::{EvictionConfig, LfuConfig};
use std::{borrow::Borrow, fmt::Debug, hash::Hash, sync::Arc};

use crate::{
    compress::Compression,
    device::fs::FsDevice,
    error::Result,
    generic::{GenericStore, GenericStoreConfig, GenericStoreWriter},
    lazy::{Lazy, LazyStoreWriter},
    none::{NoneStore, NoneStoreWriter},
    runtime::{Runtime, RuntimeStoreConfig, RuntimeStoreWriter},
    storage::{CachedEntry, Storage, StorageWriter},
    AdmissionPolicy, FsDeviceConfig, ReinsertionPolicy, RuntimeConfig,
};

pub type FsStore<K, V> = GenericStore<K, V, FsDevice>;
pub type FsStoreConfig<K, V> = GenericStoreConfig<K, V, FsDevice>;
pub type FsStoreWriter<K, V> = GenericStoreWriter<K, V, FsDevice>;

#[derive(Debug, Clone)]
pub enum DeviceConfig {
    None,
    Fs(FsDeviceConfig),
}

impl From<FsDeviceConfig> for DeviceConfig {
    fn from(value: FsDeviceConfig) -> Self {
        Self::Fs(value)
    }
}

pub struct StoreBuilder<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    name: String,
    eviction_config: EvictionConfig,
    device_config: DeviceConfig,
    catalog_shards: usize,
    admissions: Vec<Arc<dyn AdmissionPolicy<Key = K, Value = V>>>,
    reinsertions: Vec<Arc<dyn ReinsertionPolicy<Key = K, Value = V>>>,
    flushers: usize,
    reclaimers: usize,
    clean_region_threshold: Option<usize>,
    recover_concurrency: usize,
    compression: Compression,
    lazy: bool,
    runtime_config: Option<RuntimeConfig>,
}

impl<K, V> Default for StoreBuilder<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> StoreBuilder<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    pub fn new() -> Self {
        Self {
            name: "foyer".to_string(),
            eviction_config: LfuConfig {
                window_capacity_ratio: 0.1,
                protected_capacity_ratio: 0.8,
                cmsketch_eps: 0.001,
                cmsketch_confidence: 0.9,
            }
            .into(),
            device_config: DeviceConfig::None,
            catalog_shards: 64,
            admissions: vec![],
            reinsertions: vec![],
            flushers: 4,
            reclaimers: 4,
            clean_region_threshold: None,
            recover_concurrency: 8,
            compression: Compression::None,
            runtime_config: None,
            lazy: false,
        }
    }
}

impl<K, V> StoreBuilder<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    /// For distinguish different foyer metrics.
    ///
    /// Metrics of this foyer instance has label `foyer = {{ name }}`.
    pub fn with_name(mut self, name: &str) -> Self {
        self.name = name.to_string();
        self
    }

    /// Eviction policy configurations.
    ///
    /// The default eviction policy is a general-used LFU configuration.
    pub fn with_eviction_config(mut self, eviction_config: impl Into<EvictionConfig>) -> Self {
        self.eviction_config = eviction_config.into();
        self
    }

    /// Device configurations.
    ///
    /// If not give, an always-return-none store will be used.
    pub fn with_device_config(mut self, device_config: impl Into<DeviceConfig>) -> Self {
        self.device_config = device_config.into();
        self
    }

    /// Catalog indices sharding count.
    pub fn with_catalog_shards(mut self, catalog_shards: usize) -> Self {
        self.catalog_shards = catalog_shards;
        self
    }

    /// Push a new admission policy in order.
    pub fn with_admission_policy(mut self, admission: Arc<dyn AdmissionPolicy<Key = K, Value = V>>) -> Self {
        self.admissions.push(admission);
        self
    }

    /// Push a new reinsertion policy in order.
    pub fn with_reinsertion_policy(mut self, reinsertion: Arc<dyn ReinsertionPolicy<Key = K, Value = V>>) -> Self {
        self.reinsertions.push(reinsertion);
        self
    }

    /// Count of flushers.
    pub fn with_flushers(mut self, flushers: usize) -> Self {
        self.flushers = flushers;
        self
    }

    /// Count of reclaimers.
    pub fn with_reclaimers(mut self, reclaimers: usize) -> Self {
        self.reclaimers = reclaimers;
        self
    }

    /// Clean region count threshold to trigger reclamation.
    ///
    /// `clean_region_threshold` is recommended to be equal or larger than `reclaimers`.
    ///
    /// The default clean region thrshold is the reclaimer count.
    pub fn with_clean_region_threshold(mut self, clean_region_threshold: usize) -> Self {
        self.clean_region_threshold = Some(clean_region_threshold);
        self
    }

    /// Concurrency of recovery.
    pub fn with_recover_concurrency(mut self, recover_concurrency: usize) -> Self {
        self.recover_concurrency = recover_concurrency;
        self
    }

    /// Compression algorithm.
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Enable a dedicated tokio runtime for the store with a runtime config.
    ///
    /// If not given, the store will use the user's runtime.
    pub fn with_runtime_config(mut self, runtime_config: RuntimeConfig) -> Self {
        self.runtime_config = Some(runtime_config);
        self
    }

    /// Decide if lazy reocvery feature is enabled.
    ///
    /// If enabled, the opening for the store will return immediately, but always return `None` before recovery is finished.
    pub fn with_lazy(mut self, lazy: bool) -> Self {
        self.lazy = lazy;
        self
    }

    /// Build and return the [`StoreConfig`] only.
    pub fn build_config(self) -> StoreConfig<K, V> {
        let clean_region_threshold = self.clean_region_threshold.unwrap_or(self.reclaimers);

        match (self.device_config, self.runtime_config, self.lazy) {
            (DeviceConfig::None, _, _) => StoreConfig::None,
            (DeviceConfig::Fs(device_config), None, false) => StoreConfig::Fs(FsStoreConfig {
                name: self.name,
                eviction_config: self.eviction_config,
                device_config,
                catalog_shards: self.catalog_shards,
                admissions: self.admissions,
                reinsertions: self.reinsertions,
                flushers: self.flushers,
                reclaimers: self.reclaimers,
                clean_region_threshold,
                recover_concurrency: self.recover_concurrency,
                compression: self.compression,
            }),
            (DeviceConfig::Fs(device_config), None, true) => StoreConfig::LazyFs(FsStoreConfig {
                name: self.name,
                eviction_config: self.eviction_config,
                device_config,
                catalog_shards: self.catalog_shards,
                admissions: self.admissions,
                reinsertions: self.reinsertions,
                flushers: self.flushers,
                reclaimers: self.reclaimers,
                clean_region_threshold,
                recover_concurrency: self.recover_concurrency,
                compression: self.compression,
            }),
            (DeviceConfig::Fs(device_config), Some(runtime_config), true) => {
                StoreConfig::RuntimeFs(RuntimeStoreConfig {
                    store_config: FsStoreConfig {
                        name: self.name,
                        eviction_config: self.eviction_config,
                        device_config,
                        catalog_shards: self.catalog_shards,
                        admissions: self.admissions,
                        reinsertions: self.reinsertions,
                        flushers: self.flushers,
                        reclaimers: self.reclaimers,
                        clean_region_threshold,
                        recover_concurrency: self.recover_concurrency,
                        compression: self.compression,
                    },
                    runtime_config,
                })
            }
            (DeviceConfig::Fs(device_config), Some(runtime_config), false) => {
                StoreConfig::RuntimeLazyFs(RuntimeStoreConfig {
                    store_config: FsStoreConfig {
                        name: self.name,
                        eviction_config: self.eviction_config,
                        device_config,
                        catalog_shards: self.catalog_shards,
                        admissions: self.admissions,
                        reinsertions: self.reinsertions,
                        flushers: self.flushers,
                        reclaimers: self.reclaimers,
                        clean_region_threshold,
                        recover_concurrency: self.recover_concurrency,
                        compression: self.compression,
                    },
                    runtime_config,
                })
            }
        }
    }

    /// Open a store with the given configuration.
    pub async fn build(self) -> Result<Store<K, V>> {
        let config = self.build_config();
        tracing::info!("Open foyer store with config:\n{config:#?}");
        Store::open(config).await
    }
}

pub enum StoreConfig<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    None,

    Fs(FsStoreConfig<K, V>),
    LazyFs(FsStoreConfig<K, V>),
    RuntimeFs(RuntimeStoreConfig<K, V, FsStore<K, V>>),
    RuntimeLazyFs(RuntimeStoreConfig<K, V, Lazy<K, V, FsStore<K, V>>>),
}

impl<K, V> Debug for StoreConfig<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "None"),
            Self::Fs(config) => f.debug_tuple("Fs").field(config).finish(),
            Self::LazyFs(config) => f.debug_tuple("LazyFs").field(config).finish(),
            Self::RuntimeFs(config) => f.debug_tuple("RuntimeFs").field(config).finish(),
            Self::RuntimeLazyFs(config) => f.debug_tuple("RuntimeLazyFs").field(config).finish(),
        }
    }
}

impl<K, V> Clone for StoreConfig<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn clone(&self) -> Self {
        match self {
            StoreConfig::None => StoreConfig::None,
            StoreConfig::Fs(config) => StoreConfig::Fs(config.clone()),
            StoreConfig::LazyFs(config) => StoreConfig::LazyFs(config.clone()),
            StoreConfig::RuntimeFs(config) => StoreConfig::RuntimeFs(config.clone()),
            StoreConfig::RuntimeLazyFs(config) => StoreConfig::RuntimeLazyFs(config.clone()),
        }
    }
}

pub enum StoreWriter<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    None(NoneStoreWriter<K, V>),

    Fs(FsStoreWriter<K, V>),
    LazyFs(LazyStoreWriter<K, V, FsStore<K, V>>),
    RuntimeFs(RuntimeStoreWriter<K, V, FsStore<K, V>>),
    RuntimeLazyFs(RuntimeStoreWriter<K, V, Lazy<K, V, FsStore<K, V>>>),
}

impl<K, V> Debug for StoreWriter<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None(_) => f.debug_tuple("None").finish(),
            Self::Fs(_) => f.debug_tuple("Fs").finish(),
            Self::LazyFs(_) => f.debug_tuple("LazyFs").finish(),
            Self::RuntimeFs(_) => f.debug_tuple("RuntimeFs").finish(),
            Self::RuntimeLazyFs(_) => f.debug_tuple("RuntimeLazyFs").finish(),
        }
    }
}

pub enum Store<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    None(NoneStore<K, V>),

    Fs(FsStore<K, V>),
    LazyFs(Lazy<K, V, FsStore<K, V>>),
    RuntimeFs(Runtime<K, V, FsStore<K, V>>),
    RuntimeLazyFs(Runtime<K, V, Lazy<K, V, FsStore<K, V>>>),
}

impl<K, V> Debug for Store<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None(_) => f.debug_tuple("None").finish(),
            Self::Fs(_) => f.debug_tuple("Fs").finish(),
            Self::LazyFs(_) => f.debug_tuple("LazyFs").finish(),
            Self::RuntimeFs(_) => f.debug_tuple("RuntimeFs").finish(),
            Self::RuntimeLazyFs(_) => f.debug_tuple("RuntimeLazyFs").finish(),
        }
    }
}

impl<K, V> Clone for Store<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn clone(&self) -> Self {
        match self {
            Self::None(store) => Self::None(store.clone()),
            Self::Fs(store) => Self::Fs(store.clone()),
            Self::LazyFs(store) => Self::LazyFs(store.clone()),
            Self::RuntimeFs(store) => Self::RuntimeFs(store.clone()),
            Self::RuntimeLazyFs(store) => Self::RuntimeLazyFs(store.clone()),
        }
    }
}

impl<K, V> StorageWriter<K, V> for StoreWriter<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn key(&self) -> &K {
        match self {
            StoreWriter::None(writer) => writer.key(),
            StoreWriter::Fs(writer) => writer.key(),
            StoreWriter::LazyFs(writer) => writer.key(),
            StoreWriter::RuntimeFs(writer) => writer.key(),
            StoreWriter::RuntimeLazyFs(writer) => writer.key(),
        }
    }

    fn judge(&mut self) -> bool {
        match self {
            StoreWriter::None(writer) => writer.judge(),
            StoreWriter::Fs(writer) => writer.judge(),
            StoreWriter::LazyFs(writer) => writer.judge(),
            StoreWriter::RuntimeFs(writer) => writer.judge(),
            StoreWriter::RuntimeLazyFs(writer) => writer.judge(),
        }
    }

    fn force(&mut self) {
        match self {
            StoreWriter::None(writer) => writer.force(),
            StoreWriter::Fs(writer) => writer.force(),
            StoreWriter::LazyFs(writer) => writer.force(),
            StoreWriter::RuntimeFs(writer) => writer.force(),
            StoreWriter::RuntimeLazyFs(writer) => writer.force(),
        }
    }

    fn compression(&self) -> Compression {
        match self {
            StoreWriter::None(writer) => writer.compression(),
            StoreWriter::Fs(writer) => writer.compression(),
            StoreWriter::LazyFs(writer) => writer.compression(),
            StoreWriter::RuntimeFs(writer) => writer.compression(),
            StoreWriter::RuntimeLazyFs(writer) => writer.compression(),
        }
    }

    fn set_compression(&mut self, compression: Compression) {
        match self {
            StoreWriter::None(writer) => writer.set_compression(compression),
            StoreWriter::Fs(writer) => writer.set_compression(compression),
            StoreWriter::LazyFs(writer) => writer.set_compression(compression),
            StoreWriter::RuntimeFs(writer) => writer.set_compression(compression),
            StoreWriter::RuntimeLazyFs(writer) => writer.set_compression(compression),
        }
    }

    async fn finish(self, value: V) -> Result<Option<CachedEntry<K, V>>>
    where
        K: StorageKey,
        V: StorageValue,
    {
        match self {
            StoreWriter::None(writer) => writer.finish(value).await,
            StoreWriter::Fs(writer) => writer.finish(value).await,
            StoreWriter::LazyFs(writer) => writer.finish(value).await,
            StoreWriter::RuntimeFs(writer) => writer.finish(value).await,
            StoreWriter::RuntimeLazyFs(writer) => writer.finish(value).await,
        }
    }
}

impl<K, V> Storage<K, V> for Store<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    type Config = StoreConfig<K, V>;
    type Writer = StoreWriter<K, V>;

    async fn open(config: Self::Config) -> Result<Self> {
        let store = match config {
            StoreConfig::None => Self::None(NoneStore::open(()).await?),
            StoreConfig::Fs(config) => Self::Fs(FsStore::open(config).await?),
            StoreConfig::LazyFs(config) => Self::LazyFs(Lazy::open(config).await?),
            StoreConfig::RuntimeFs(config) => Self::RuntimeFs(Runtime::open(config).await?),
            StoreConfig::RuntimeLazyFs(config) => Self::RuntimeLazyFs(Runtime::open(config).await?),
        };
        Ok(store)
    }

    fn is_ready(&self) -> bool {
        match self {
            Store::None(store) => store.is_ready(),
            Store::Fs(store) => store.is_ready(),
            Store::LazyFs(store) => store.is_ready(),
            Store::RuntimeFs(store) => store.is_ready(),
            Store::RuntimeLazyFs(store) => store.is_ready(),
        }
    }

    async fn close(&self) -> Result<()> {
        match self {
            Store::None(store) => store.close().await,
            Store::Fs(store) => store.close().await,
            Store::LazyFs(store) => store.close().await,
            Store::RuntimeFs(store) => store.close().await,
            Store::RuntimeLazyFs(store) => store.close().await,
        }
    }

    fn writer(&self, key: K) -> Self::Writer {
        match self {
            Store::None(store) => StoreWriter::None(store.writer(key)),
            Store::Fs(store) => StoreWriter::Fs(store.writer(key)),
            Store::LazyFs(store) => StoreWriter::LazyFs(store.writer(key)),
            Store::RuntimeFs(store) => StoreWriter::RuntimeFs(store.writer(key)),
            Store::RuntimeLazyFs(store) => StoreWriter::RuntimeLazyFs(store.writer(key)),
        }
    }

    fn exists<Q>(&self, key: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        match self {
            Store::None(store) => store.exists(key),
            Store::Fs(store) => store.exists(key),
            Store::LazyFs(store) => store.exists(key),
            Store::RuntimeFs(store) => store.exists(key),
            Store::RuntimeLazyFs(store) => store.exists(key),
        }
    }

    async fn lookup<Q>(&self, key: &Q) -> Result<Option<CachedEntry<K, V>>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Send + Sync + Clone + 'static,
    {
        match self {
            Store::None(store) => store.lookup(key).await,
            Store::Fs(store) => store.lookup(key).await,
            Store::LazyFs(store) => store.lookup(key).await,
            Store::RuntimeFs(store) => store.lookup(key).await,
            Store::RuntimeLazyFs(store) => store.lookup(key).await,
        }
    }

    fn remove<Q>(&self, key: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        match self {
            Store::None(store) => store.remove(key),
            Store::Fs(store) => store.remove(key),
            Store::LazyFs(store) => store.remove(key),
            Store::RuntimeFs(store) => store.remove(key),
            Store::RuntimeLazyFs(store) => store.remove(key),
        }
    }

    fn clear(&self) -> Result<()> {
        match self {
            Store::None(store) => store.clear(),
            Store::Fs(store) => store.clear(),
            Store::LazyFs(store) => store.clear(),
            Store::RuntimeFs(store) => store.clear(),
            Store::RuntimeLazyFs(store) => store.clear(),
        }
    }
}
