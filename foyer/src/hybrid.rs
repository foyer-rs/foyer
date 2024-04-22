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
    future::Future,
    hash::{BuildHasher, Hash},
    sync::{Arc, OnceLock},
};

use ahash::RandomState;
use foyer_common::{
    arcable::Arcable,
    code::{StorageKey, StorageValue},
};
use foyer_memory::{
    Cache, CacheBuilder, CacheContext, CacheEntry, CacheEventListener, Entry, EvictionConfig, Weighter,
};
use foyer_storage::{
    AdmissionPolicy, AsyncStorageExt, Compression, DeviceConfig, ReinsertionPolicy, RuntimeConfig, Storage, Store,
    StoreBuilder,
};

struct HybridCacheEventListenerInner<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    store: OnceLock<Store<K, V>>,
}

pub struct HybridCacheEventListener<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    inner: Arc<HybridCacheEventListenerInner<K, V>>,
}

impl<K, V> Default for HybridCacheEventListener<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn default() -> Self {
        Self {
            inner: Arc::new(HybridCacheEventListenerInner { store: OnceLock::new() }),
        }
    }
}

impl<K, V> Clone for HybridCacheEventListener<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<K, V> CacheEventListener<K, V> for HybridCacheEventListener<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn on_release(&self, key: Arc<K>, value: Arc<V>, _context: CacheContext, _weight: usize) {
        // TODO(MrCroxx): Return read handle to block following request of the key and clear with callback?
        unsafe { self.inner.store.get().unwrap_unchecked() }.insert_if_not_exists_async(key, value)
    }
}

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
        let listener = HybridCacheEventListener::default();
        HybridCacheBuilderPhaseMemory {
            builder: CacheBuilder::new(capacity).with_event_listener(listener.clone()),
            listener,
        }
    }
}

pub struct HybridCacheBuilderPhaseMemory<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: BuildHasher + Send + Sync + 'static,
{
    builder: CacheBuilder<K, V, HybridCacheEventListener<K, V>, S>,
    listener: HybridCacheEventListener<K, V>,
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
        HybridCacheBuilderPhaseMemory {
            builder,
            listener: self.listener,
        }
    }

    /// Set in-memory cache eviction algorithm.
    ///
    /// The default value is a general-used w-TinyLFU algorithm.
    pub fn with_eviction_config(self, eviction_config: impl Into<EvictionConfig>) -> Self {
        let builder = self.builder.with_eviction_config(eviction_config.into());
        HybridCacheBuilderPhaseMemory {
            builder,
            listener: self.listener,
        }
    }

    /// Set object pool for handles. The object pool is used to reduce handle allocation.
    ///
    /// The optimized value is supposed to be equal to the max cache entry count.
    ///
    /// The default value is 1024.
    pub fn with_object_pool_capacity(self, object_pool_capacity: usize) -> Self {
        let builder = self.builder.with_object_pool_capacity(object_pool_capacity);
        HybridCacheBuilderPhaseMemory {
            builder,
            listener: self.listener,
        }
    }

    /// Set in-memory cache hash builder.
    pub fn with_hash_builder<OS>(self, hash_builder: OS) -> HybridCacheBuilderPhaseMemory<K, V, OS>
    where
        OS: BuildHasher + Send + Sync + 'static,
    {
        let builder = self.builder.with_hash_builder(hash_builder);
        HybridCacheBuilderPhaseMemory {
            builder,
            listener: self.listener,
        }
    }

    /// Set in-memory cache weighter.
    pub fn with_weighter(self, weighter: impl Weighter<K, V>) -> Self {
        let builder = self.builder.with_weighter(weighter);
        HybridCacheBuilderPhaseMemory {
            builder,
            listener: self.listener,
        }
    }

    pub fn storage(self) -> HybridCacheBuilderPhaseStorage<K, V, S> {
        HybridCacheBuilderPhaseStorage {
            listener: self.listener,
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
    listener: HybridCacheEventListener<K, V>,
    cache: Cache<K, V, HybridCacheEventListener<K, V>, S>,
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
            listener: self.listener,
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
            listener: self.listener,
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
            listener: self.listener,
            cache: self.cache,
            builder,
        }
    }

    /// Catalog indices sharding count.
    pub fn with_catalog_shards(self, catalog_shards: usize) -> Self {
        let builder = self.builder.with_catalog_shards(catalog_shards);
        Self {
            listener: self.listener,
            cache: self.cache,
            builder,
        }
    }

    /// Push a new admission policy in order.
    pub fn with_admission_policy(self, admission: Arc<dyn AdmissionPolicy<Key = K, Value = V>>) -> Self {
        let builder = self.builder.with_admission_policy(admission);
        Self {
            listener: self.listener,
            cache: self.cache,
            builder,
        }
    }

    /// Push a new reinsertion policy in order.
    pub fn with_reinsertion_policy(self, reinsertion: Arc<dyn ReinsertionPolicy<Key = K, Value = V>>) -> Self {
        let builder = self.builder.with_reinsertion_policy(reinsertion);
        Self {
            listener: self.listener,
            cache: self.cache,
            builder,
        }
    }

    /// Count of flushers.
    pub fn with_flushers(self, flushers: usize) -> Self {
        let builder = self.builder.with_flushers(flushers);
        Self {
            listener: self.listener,
            cache: self.cache,
            builder,
        }
    }

    /// Count of reclaimers.
    pub fn with_reclaimers(self, reclaimers: usize) -> Self {
        let builder = self.builder.with_reclaimers(reclaimers);
        Self {
            listener: self.listener,
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
            listener: self.listener,
            cache: self.cache,
            builder,
        }
    }

    /// Concurrency of recovery.
    pub fn with_recover_concurrency(self, recover_concurrency: usize) -> Self {
        let builder = self.builder.with_recover_concurrency(recover_concurrency);
        Self {
            listener: self.listener,
            cache: self.cache,
            builder,
        }
    }

    /// Compression algorithm.
    pub fn with_compression(self, compression: Compression) -> Self {
        let builder = self.builder.with_compression(compression);
        Self {
            listener: self.listener,
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
            listener: self.listener,
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
            listener: self.listener,
            cache: self.cache,
            builder,
        }
    }

    pub async fn build(self) -> anyhow::Result<HybridCache<K, V, S>> {
        let store = self.builder.build().await?;
        self.listener.inner.store.set(store.clone()).unwrap();
        Ok(HybridCache {
            cache: self.cache,
            store,
        })
    }
}

pub type HybridCacheEntry<K, V, S> = CacheEntry<K, V, HybridCacheEventListener<K, V>, S>;

pub struct HybridCache<K, V, S = RandomState>
where
    K: StorageKey,
    V: StorageValue,
    S: BuildHasher + Send + Sync + 'static,
{
    cache: Cache<K, V, HybridCacheEventListener<K, V>, S>,
    store: Store<K, V>,
}

impl<K, V, S> HybridCache<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: BuildHasher + Send + Sync + 'static,
{
    pub fn cache(&self) -> &Cache<K, V, HybridCacheEventListener<K, V>, S> {
        &self.cache
    }

    pub fn store(&self) -> &Store<K, V> {
        &self.store
    }

    pub fn insert<AK, AV>(&self, key: AK, value: AV) -> HybridCacheEntry<K, V, S>
    where
        AK: Into<Arcable<K>> + Send + 'static,
        AV: Into<Arcable<V>> + Send + 'static,
    {
        self.cache.insert(key, value)
    }

    pub fn insert_with_context<AK, AV>(&self, key: AK, value: AV, context: CacheContext) -> HybridCacheEntry<K, V, S>
    where
        AK: Into<Arcable<K>> + Send + 'static,
        AV: Into<Arcable<V>> + Send + 'static,
    {
        self.cache.insert_with_context(key, value, context)
    }

    pub async fn get<Q>(&self, key: &Q) -> anyhow::Result<Option<HybridCacheEntry<K, V, S>>>
    where
        K: Borrow<Q> + Clone,
        V: Clone,
        Q: Hash + Eq + ?Sized + Send + Sync + 'static + Clone,
    {
        if let Some(entry) = self.cache.get(key) {
            return Ok(Some(entry));
        }
        if let Some(entry) = self.store.get(key).await? {
            return Ok(Some(self.cache.insert(entry.key().clone(), entry.value().clone())));
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

pub type HybridEntry<K, V, S> = Entry<K, V, anyhow::Error, HybridCacheEventListener<K, V>, S>;

impl<K, V, S> HybridCache<K, V, S>
where
    K: StorageKey + Clone,
    V: StorageValue + Clone,
    S: BuildHasher + Send + Sync + 'static,
{
    pub fn entry<F, FU>(&self, key: K, f: F) -> HybridEntry<K, V, S>
    where
        F: FnOnce() -> FU + Send + 'static,
        FU: Future<Output = anyhow::Result<(V, CacheContext)>> + Send + 'static,
    {
        let store = self.store.clone();
        self.cache.entry(key.clone(), || async move {
            let key = key;
            if let Some(value) = store.get(&key).await.map_err(anyhow::Error::from)? {
                return Ok((value.to_owned().1, CacheContext::default()));
            }
            f().await.map_err(anyhow::Error::from)
        })
    }
}
