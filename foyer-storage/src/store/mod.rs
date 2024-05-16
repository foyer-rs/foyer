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

pub mod noop;
pub mod runtime;

use std::{
    borrow::Borrow,
    fmt::Debug,
    future::Future,
    hash::Hash,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use ahash::RandomState;
use foyer_common::code::{HashBuilder, StorageKey, StorageValue};
use foyer_memory::{Cache, CacheEntry};

use super::{
    device::direct_fs::{DirectFsDevice, DirectFsDeviceOptions},
    storage::{EnqueueFuture, Storage},
    tombstone::TombstoneLogConfig,
};

use crate::{
    compress::Compression,
    large::{
        generic::{GenericStore, GenericStoreConfig},
        recover::RecoverMode,
    },
    picker::{
        utils::{AdmitAllPicker, RejectAllPicker},
        AdmissionPicker, EvictionPicker, ReinsertionPicker,
    },
    FifoPicker, InvalidRatioPicker,
};
use noop::NoopStore;
use runtime::{Runtime, RuntimeConfig, RuntimeStoreConfig};

use crate::error::Result;

pub enum StoreConfig<K, V, S = RandomState>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    Noop,
    DirectFs(GenericStoreConfig<K, V, S, DirectFsDevice>),
    RuntimeDirectFs(RuntimeStoreConfig<GenericStore<K, V, S, DirectFsDevice>>),
}

impl<K, V, S> Debug for StoreConfig<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Noop => f.debug_tuple("None").finish(),
            Self::DirectFs(config) => f.debug_tuple("DirectFs").field(config).finish(),
            Self::RuntimeDirectFs(config) => f.debug_tuple("RuntimeDirectFs").field(config).finish(),
        }
    }
}

pub enum Store<K, V, S = RandomState>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    Noop(NoopStore<K, V, S>),
    DirectFs(GenericStore<K, V, S, DirectFsDevice>),
    RuntimeDirectFs(Runtime<GenericStore<K, V, S, DirectFsDevice>>),
}

impl<K, V, S> Debug for Store<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Noop(store) => f.debug_tuple("None").field(store).finish(),
            Self::DirectFs(store) => f.debug_tuple("DirectFs").field(store).finish(),
            Self::RuntimeDirectFs(store) => f.debug_tuple("RuntimeDirectFs").field(store).finish(),
        }
    }
}

impl<K, V, S> Clone for Store<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn clone(&self) -> Self {
        match self {
            Self::Noop(store) => Self::Noop(store.clone()),
            Self::DirectFs(store) => Self::DirectFs(store.clone()),
            Self::RuntimeDirectFs(store) => Self::RuntimeDirectFs(store.clone()),
        }
    }
}

impl<K, V, S> Storage for Store<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    type Key = K;
    type Value = V;
    type BuildHasher = S;
    type Config = StoreConfig<K, V, S>;

    async fn open(config: Self::Config) -> Result<Self> {
        match config {
            StoreConfig::Noop => Ok(Self::Noop(NoopStore::open(()).await?)),
            StoreConfig::DirectFs(config) => Ok(Self::DirectFs(GenericStore::open(config).await?)),
            StoreConfig::RuntimeDirectFs(config) => Ok(Self::RuntimeDirectFs(Runtime::open(config).await?)),
        }
    }

    async fn close(&self) -> Result<()> {
        match self {
            Store::Noop(store) => store.close().await,
            Store::DirectFs(store) => store.close().await,
            Store::RuntimeDirectFs(store) => store.close().await,
        }
    }

    fn enqueue(&self, entry: CacheEntry<Self::Key, Self::Value, Self::BuildHasher>) -> super::storage::EnqueueFuture {
        match self {
            Store::Noop(store) => store.enqueue(entry),
            Store::DirectFs(store) => store.enqueue(entry),
            Store::RuntimeDirectFs(store) => store.enqueue(entry),
        }
    }

    fn load<Q>(&self, key: &Q) -> impl Future<Output = Result<Option<(Self::Key, Self::Value)>>> + Send + 'static
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Send + Sync + 'static,
    {
        match self {
            Store::Noop(store) => LoadFuture::Noop(store.load(key)),
            Store::DirectFs(store) => LoadFuture::DirectFs(store.load(key)),
            Store::RuntimeDirectFs(store) => LoadFuture::RuntimeDirectFs(store.load(key)),
        }
    }

    fn delete<Q>(&self, key: &Q) -> EnqueueFuture
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        match self {
            Store::Noop(store) => store.delete(key),
            Store::DirectFs(store) => store.delete(key),
            Store::RuntimeDirectFs(store) => store.delete(key),
        }
    }

    fn may_contains<Q>(&self, key: &Q) -> bool
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        match self {
            Store::Noop(store) => store.may_contains(key),
            Store::DirectFs(store) => store.may_contains(key),
            Store::RuntimeDirectFs(store) => store.may_contains(key),
        }
    }

    async fn destroy(&self) -> Result<()> {
        match self {
            Store::Noop(store) => store.destroy().await,
            Store::DirectFs(store) => store.destroy().await,
            Store::RuntimeDirectFs(store) => store.destroy().await,
        }
    }

    fn stats(&self) -> Arc<crate::device::monitor::DeviceStats> {
        match self {
            Store::Noop(store) => store.stats(),
            Store::DirectFs(store) => store.stats(),
            Store::RuntimeDirectFs(store) => store.stats(),
        }
    }
}

enum LoadFuture<F1, F2, F3> {
    Noop(F1),
    DirectFs(F2),
    RuntimeDirectFs(F3),
}
impl<F1, F2, F3> LoadFuture<F1, F2, F3> {
    // TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
    #[allow(clippy::type_complexity)]
    pub fn as_pin_mut(self: Pin<&mut Self>) -> LoadFuture<Pin<&mut F1>, Pin<&mut F2>, Pin<&mut F3>> {
        unsafe {
            match *Pin::get_unchecked_mut(self) {
                LoadFuture::Noop(ref mut inner) => LoadFuture::Noop(Pin::new_unchecked(inner)),
                LoadFuture::DirectFs(ref mut inner) => LoadFuture::DirectFs(Pin::new_unchecked(inner)),
                LoadFuture::RuntimeDirectFs(ref mut inner) => LoadFuture::RuntimeDirectFs(Pin::new_unchecked(inner)),
            }
        }
    }
}

impl<F1, F2, F3> Future for LoadFuture<F1, F2, F3>
where
    F1: Future,
    F2: Future<Output = F1::Output>,
    F3: Future<Output = F1::Output>,
{
    type Output = F1::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.as_pin_mut() {
            LoadFuture::Noop(future) => future.poll(cx),
            LoadFuture::DirectFs(future) => future.poll(cx),
            LoadFuture::RuntimeDirectFs(future) => future.poll(cx),
        }
    }
}

#[derive(Debug, Clone)]
pub enum DeviceConfig {
    None,
    DirectFs(DirectFsDeviceOptions),
}

impl From<DirectFsDeviceOptions> for DeviceConfig {
    fn from(value: DirectFsDeviceOptions) -> Self {
        Self::DirectFs(value)
    }
}

pub struct StoreBuilder<K, V, S = RandomState>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    memory: Cache<K, V, S>,
    device_config: DeviceConfig,
    flush: bool,
    indexer_shards: usize,
    recover_mode: RecoverMode,
    recover_concurrency: usize,
    flushers: usize,
    reclaimers: usize,
    clean_region_threshold: Option<usize>,
    eviction_pickers: Vec<Box<dyn EvictionPicker>>,
    admision_picker: Arc<dyn AdmissionPicker<Key = K>>,
    reinsertion_picker: Arc<dyn ReinsertionPicker<Key = K>>,
    compression: Compression,
    tombstone_log_config: Option<TombstoneLogConfig>,

    runtime_config: Option<RuntimeConfig>,
}

impl<K, V, S> StoreBuilder<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    /// Setup disk cache store for the given in-memory cache.
    pub fn new(memory: Cache<K, V, S>) -> Self {
        Self {
            memory,
            device_config: DeviceConfig::None,
            flush: false,
            indexer_shards: 64,
            recover_mode: RecoverMode::Quiet,
            recover_concurrency: 8,
            flushers: 1,
            reclaimers: 1,
            clean_region_threshold: None,
            eviction_pickers: vec![Box::new(InvalidRatioPicker::new(0.8)), Box::<FifoPicker>::default()],
            admision_picker: Arc::<AdmitAllPicker<K>>::default(),
            reinsertion_picker: Arc::<RejectAllPicker<K>>::default(),
            compression: Compression::None,
            tombstone_log_config: None,
            runtime_config: None,
        }
    }

    /// Set device config for the disk cache store.
    pub fn with_device_config(mut self, device_config: impl Into<DeviceConfig>) -> Self {
        self.device_config = device_config.into();
        self
    }

    /// Enable/disable `sync` after writes.
    ///
    /// Default: `false`.
    pub fn with_flush(mut self, flush: bool) -> Self {
        self.flush = flush;
        self
    }

    /// Set the shard num of the indexer. Each shard has its own lock.
    ///
    /// Default: `64`.
    pub fn with_indexer_shards(mut self, indexer_shards: usize) -> Self {
        self.indexer_shards = indexer_shards;
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
    /// If no eviction picker pickes a region, a region will be picked randomly.
    ///
    /// Default: [ invalid ratio picker { threshold = 0.8 }, fifo picker ]
    pub fn with_eviction_pickers(mut self, eviction_pickers: Vec<Box<dyn EvictionPicker>>) -> Self {
        self.eviction_pickers = eviction_pickers;
        self
    }

    /// Set the admission pickers for th disk cache store.
    ///
    /// The admission picker is used to pick the entries that can be inserted into the disk cache store.
    ///
    /// Default: [`AdmitAllPicker`].
    pub fn with_admission_picker(mut self, admission_picker: Arc<dyn AdmissionPicker<Key = K>>) -> Self {
        self.admision_picker = admission_picker;
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

    /// Set the compression algorithm of the disk cache store.
    ///
    /// Default: [`Compression::None`].
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
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

    /// Enable the dedicated runtime for the disk cache store.
    pub fn with_runtime_config(mut self, runtime_config: RuntimeConfig) -> Self {
        self.runtime_config = Some(runtime_config);
        self
    }

    /// Build the disk cache store with the given configuration.
    pub async fn build(self) -> Result<Store<K, V, S>> {
        let clean_region_threshold = self.clean_region_threshold.unwrap_or(self.reclaimers);

        if matches!(self.device_config, DeviceConfig::None) {
            tracing::warn!(
                "[store builder]: No device config set. Use `NoneStore` which always returns `None` for queries."
            );
            return Store::open(StoreConfig::Noop).await;
        }

        match (self.device_config, self.runtime_config) {
            (DeviceConfig::None, _) => unreachable!(),
            (DeviceConfig::DirectFs(device_config), None) => {
                Store::open(StoreConfig::DirectFs(GenericStoreConfig {
                    memory: self.memory,
                    device_config,
                    compression: self.compression,
                    flush: self.flush,
                    indexer_shards: self.indexer_shards,
                    recover_mode: self.recover_mode,
                    recover_concurrency: self.recover_concurrency,
                    flushers: self.flushers,
                    reclaimers: self.reclaimers,
                    clean_region_threshold,
                    eviction_pickers: self.eviction_pickers,
                    admission_picker: self.admision_picker,
                    reinsertion_picker: self.reinsertion_picker,
                    tombstone_log_config: self.tombstone_log_config,
                }))
                .await
            }
            (DeviceConfig::DirectFs(device_config), Some(runtime_config)) => {
                Store::open(StoreConfig::RuntimeDirectFs(RuntimeStoreConfig {
                    store_config: GenericStoreConfig {
                        memory: self.memory,
                        device_config,
                        compression: self.compression,
                        flush: self.flush,
                        indexer_shards: self.indexer_shards,
                        recover_mode: self.recover_mode,
                        recover_concurrency: self.recover_concurrency,
                        flushers: self.flushers,
                        reclaimers: self.reclaimers,
                        clean_region_threshold,
                        eviction_pickers: self.eviction_pickers,
                        admission_picker: self.admision_picker,
                        reinsertion_picker: self.reinsertion_picker,
                        tombstone_log_config: self.tombstone_log_config,
                    },
                    runtime_config,
                }))
                .await
            }
        }
    }
}
