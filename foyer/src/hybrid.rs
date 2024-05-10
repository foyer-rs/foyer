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

use std::{
    borrow::Borrow,
    fmt::Debug,
    future::Future,
    hash::{BuildHasher, Hash},
    sync::Arc,
};

use ahash::RandomState;
use foyer_common::code::{StorageKey, StorageValue};
use foyer_memory::{Cache, CacheBuilder, CacheContext, CacheEntry, Entry, EvictionConfig, Weighter};
use foyer_storage::{
    AdmissionPolicy, AsyncStorageExt, Compression, DeviceConfig, RecoverMode, ReinsertionPolicy, RuntimeConfig,
    Storage, Store, StoreBuilder,
};

pub struct HybridCacheBuilder;

impl Default for HybridCacheBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl HybridCacheBuilder {
    pub fn new() -> Self {
        Self
    }

    pub fn memory<K, V>(self, capacity: usize) -> HybridCacheBuilderPhaseMemory<K, V, RandomState>
    where
        K: StorageKey,
        V: StorageValue,
    {
        HybridCacheBuilderPhaseMemory {
            builder: CacheBuilder::new(capacity),
        }
    }
}

pub struct HybridCacheBuilderPhaseMemory<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: BuildHasher + Send + Sync + 'static,
{
    builder: CacheBuilder<K, V, S>,
}

impl<K, V, S> HybridCacheBuilderPhaseMemory<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: BuildHasher + Send + Sync + 'static,
{
    /// Set in-memory cache sharding count. Entries will be distributed to different shards based on their hash.
    /// Operations on different shard can be parallelized.
    pub fn with_shards(self, shards: usize) -> Self {
        let builder = self.builder.with_shards(shards);
        HybridCacheBuilderPhaseMemory { builder }
    }

    /// Set in-memory cache eviction algorithm.
    ///
    /// The default value is a general-used w-TinyLFU algorithm.
    pub fn with_eviction_config(self, eviction_config: impl Into<EvictionConfig>) -> Self {
        let builder = self.builder.with_eviction_config(eviction_config.into());
        HybridCacheBuilderPhaseMemory { builder }
    }

    /// Set object pool for handles. The object pool is used to reduce handle allocation.
    ///
    /// The optimized value is supposed to be equal to the max cache entry count.
    ///
    /// The default value is 1024.
    pub fn with_object_pool_capacity(self, object_pool_capacity: usize) -> Self {
        let builder = self.builder.with_object_pool_capacity(object_pool_capacity);
        HybridCacheBuilderPhaseMemory { builder }
    }

    /// Set in-memory cache hash builder.
    pub fn with_hash_builder<OS>(self, hash_builder: OS) -> HybridCacheBuilderPhaseMemory<K, V, OS>
    where
        OS: BuildHasher + Send + Sync + 'static,
    {
        let builder = self.builder.with_hash_builder(hash_builder);
        HybridCacheBuilderPhaseMemory { builder }
    }

    /// Set in-memory cache weighter.
    pub fn with_weighter(self, weighter: impl Weighter<K, V>) -> Self {
        let builder = self.builder.with_weighter(weighter);
        HybridCacheBuilderPhaseMemory { builder }
    }

    pub fn storage(self) -> HybridCacheBuilderPhaseStorage<K, V, S> {
        HybridCacheBuilderPhaseStorage {
            cache: self.builder.build(),
            builder: StoreBuilder::new(),
        }
    }
}

pub struct HybridCacheBuilderPhaseStorage<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: BuildHasher + Send + Sync + 'static,
{
    cache: Cache<K, V, S>,
    builder: StoreBuilder<K, V>,
}

impl<K, V, S> HybridCacheBuilderPhaseStorage<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: BuildHasher + Send + Sync + 'static,
{
    /// For distinguish different foyer metrics.
    ///
    /// Metrics of this foyer instance has label `foyer = {{ name }}`.
    pub fn with_name(self, name: &str) -> Self {
        let builder = self.builder.with_name(name);
        Self {
            cache: self.cache,
            builder,
        }
    }

    /// Eviction policy configurations.
    ///
    /// The default eviction policy is a general-used LFU configuration.
    pub fn with_eviction_config(self, eviction_config: impl Into<EvictionConfig>) -> Self {
        let builder = self.builder.with_eviction_config(eviction_config);
        Self {
            cache: self.cache,
            builder,
        }
    }

    /// Device configurations.
    ///
    /// If not give, an always-return-none store will be used.
    pub fn with_device_config(self, device_config: impl Into<DeviceConfig>) -> Self {
        let builder = self.builder.with_device_config(device_config);
        Self {
            cache: self.cache,
            builder,
        }
    }

    /// Catalog indices sharding count.
    pub fn with_catalog_shards(self, catalog_shards: usize) -> Self {
        let builder = self.builder.with_catalog_shards(catalog_shards);
        Self {
            cache: self.cache,
            builder,
        }
    }

    /// Push a new admission policy in order.
    pub fn with_admission_policy(self, admission: Arc<dyn AdmissionPolicy<Key = K, Value = V>>) -> Self {
        let builder = self.builder.with_admission_policy(admission);
        Self {
            cache: self.cache,
            builder,
        }
    }

    /// Push a new reinsertion policy in order.
    pub fn with_reinsertion_policy(self, reinsertion: Arc<dyn ReinsertionPolicy<Key = K, Value = V>>) -> Self {
        let builder = self.builder.with_reinsertion_policy(reinsertion);
        Self {
            cache: self.cache,
            builder,
        }
    }

    /// Count of flushers.
    pub fn with_flushers(self, flushers: usize) -> Self {
        let builder = self.builder.with_flushers(flushers);
        Self {
            cache: self.cache,
            builder,
        }
    }

    /// Count of reclaimers.
    pub fn with_reclaimers(self, reclaimers: usize) -> Self {
        let builder = self.builder.with_reclaimers(reclaimers);
        Self {
            cache: self.cache,
            builder,
        }
    }

    /// Clean region count threshold to trigger reclamation.
    ///
    /// `clean_region_threshold` is recommended to be equal or larger than `reclaimers`.
    ///
    /// The default clean region thrshold is the reclaimer count.
    pub fn with_clean_region_threshold(self, clean_region_threshold: usize) -> Self {
        let builder = self.builder.with_clean_region_threshold(clean_region_threshold);
        Self {
            cache: self.cache,
            builder,
        }
    }

    /// Set recover mode.
    ///
    /// See [`RecoverMode`].
    pub fn with_recover_mode(self, recover_mode: RecoverMode) -> Self {
        let builder = self.builder.with_recover_mode(recover_mode);
        Self {
            cache: self.cache,
            builder,
        }
    }

    /// Concurrency of recovery.
    pub fn with_recover_concurrency(self, recover_concurrency: usize) -> Self {
        let builder = self.builder.with_recover_concurrency(recover_concurrency);
        Self {
            cache: self.cache,
            builder,
        }
    }

    /// Compression algorithm.
    pub fn with_compression(self, compression: Compression) -> Self {
        let builder = self.builder.with_compression(compression);
        Self {
            cache: self.cache,
            builder,
        }
    }

    /// Flush device for each io.
    pub fn with_flush(self, flush: bool) -> Self {
        let builder = self.builder.with_flush(flush);
        Self {
            cache: self.cache,
            builder,
        }
    }

    /// Enable a dedicated tokio runtime for the store with a runtime config.
    ///
    /// If not given, the store will use the user's runtime.
    pub fn with_runtime_config(self, runtime_config: RuntimeConfig) -> Self {
        let builder = self.builder.with_runtime_config(runtime_config);
        Self {
            cache: self.cache,
            builder,
        }
    }

    /// Decide if lazy reocvery feature is enabled.
    ///
    /// If enabled, the opening for the store will return immediately, but always return `None` before recovery is finished.
    pub fn with_lazy(self, lazy: bool) -> Self {
        let builder = self.builder.with_lazy(lazy);
        Self {
            cache: self.cache,
            builder,
        }
    }

    pub async fn build(self) -> anyhow::Result<HybridCache<K, V, S>> {
        let store = self.builder.build().await?;
        Ok(HybridCache {
            cache: self.cache,
            store,
        })
    }
}

pub type HybridCacheEntry<K, V, S = RandomState> = CacheEntry<K, V, S>;

pub struct HybridCache<K, V, S = RandomState>
where
    K: StorageKey,
    V: StorageValue,
    S: BuildHasher + Send + Sync + 'static,
{
    cache: Cache<K, V, S>,
    store: Store<K, V>,
}

impl<K, V, S> Debug for HybridCache<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: BuildHasher + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HybridCache")
            .field("cache", &self.cache)
            .field("store", &self.store)
            .finish()
    }
}

impl<K, V, S> Clone for HybridCache<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: BuildHasher + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
            store: self.store.clone(),
        }
    }
}

impl<K, V, S> HybridCache<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: BuildHasher + Send + Sync + 'static,
{
    pub fn cache(&self) -> &Cache<K, V, S> {
        &self.cache
    }

    pub fn store(&self) -> &Store<K, V> {
        &self.store
    }

    pub fn insert<AK, AV>(&self, key: AK, value: AV) -> HybridCacheEntry<K, V, S>
    where
        AK: Into<Arc<K>> + Send + 'static,
        AV: Into<Arc<V>> + Send + 'static,
    {
        let key = key.into();
        let value = value.into();
        let entry = self.cache.insert(key.clone(), value.clone());
        self.store.insert_async(key, value);
        entry
    }

    pub fn insert_with_context<AK, AV>(&self, key: AK, value: AV, context: CacheContext) -> HybridCacheEntry<K, V, S>
    where
        AK: Into<Arc<K>> + Send + 'static,
        AV: Into<Arc<V>> + Send + 'static,
    {
        let key = key.into();
        let value = value.into();
        let entry = self.cache.insert_with_context(key.clone(), value.clone(), context);
        self.store.insert_async(key, value);
        entry
    }

    pub async fn get<Q>(&self, key: &Q) -> anyhow::Result<Option<HybridCacheEntry<K, V, S>>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Send + Sync + 'static + Clone,
    {
        if let Some(entry) = self.cache.get(key) {
            return Ok(Some(entry));
        }
        if let Some(entry) = self.store.get(key).await? {
            let (key, value) = entry.to_arc();
            return Ok(Some(self.cache.insert(key, value)));
        }
        Ok(None)
    }

    pub fn remove<Q>(&self, key: &Q) -> anyhow::Result<bool>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let cache = self.cache.remove(key).is_some();
        let store = self.store.remove(key)?;
        Ok(cache || store)
    }

    pub fn contains<Q>(&self, key: &Q) -> anyhow::Result<bool>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        Ok(self.cache.contains(key) || self.store.exists(key)?)
    }

    pub fn clear(&self) -> anyhow::Result<()> {
        self.cache.clear();
        self.store.clear()?;
        Ok(())
    }
}

pub type HybridEntry<K, V, S = RandomState> = Entry<K, V, anyhow::Error, S>;

impl<K, V, S> HybridCache<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: BuildHasher + Send + Sync + 'static,
{
    pub fn entry<AK, AV, F, FU>(&self, key: AK, f: F) -> HybridEntry<K, V, S>
    where
        AK: Into<Arc<K>>,
        AV: Into<Arc<V>>,
        F: FnOnce() -> FU,
        FU: Future<Output = anyhow::Result<(AV, CacheContext)>> + Send + 'static,
    {
        let key: Arc<K> = key.into();
        let store = self.store.clone();
        self.cache.entry(key.clone(), || {
            let future = f();
            async move {
                if let Some(entry) = store.get(&key).await.map_err(anyhow::Error::from)? {
                    return Ok((entry.to_arc().1, CacheContext::default()));
                }
                future
                    .await
                    .map(|(value, context)| (value.into(), context))
                    .map_err(anyhow::Error::from)
            }
        })
    }
}
