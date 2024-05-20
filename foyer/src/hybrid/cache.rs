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

use std::{borrow::Borrow, fmt::Debug, future::Future, hash::Hash, sync::Arc, time::Instant};

use ahash::RandomState;
use foyer_common::{
    code::{HashBuilder, StorageKey, StorageValue},
    metrics::Metrics,
};
use foyer_memory::{Cache, CacheContext, CacheEntry, Fetch};
use foyer_storage::{DeviceStats, Storage, Store};

/// A cached entry holder of the hybrid cache.
pub type HybridCacheEntry<K, V, S = RandomState> = CacheEntry<K, V, S>;

/// Hybrid cache that integrates in-memory cache and disk cache.
pub struct HybridCache<K, V, S = RandomState>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    memory: Cache<K, V, S>,
    storage: Store<K, V, S>,
    metrics: Arc<Metrics>,
}

impl<K, V, S> Debug for HybridCache<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HybridCache")
            .field("memory", &self.memory)
            .field("storage", &self.storage)
            .finish()
    }
}

impl<K, V, S> Clone for HybridCache<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn clone(&self) -> Self {
        Self {
            memory: self.memory.clone(),
            storage: self.storage.clone(),
            metrics: self.metrics.clone(),
        }
    }
}

impl<K, V, S> HybridCache<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    pub(crate) fn new(name: String, memory: Cache<K, V, S>, storage: Store<K, V, S>) -> Self {
        let metrics = Arc::new(Metrics::new(&name));
        Self {
            memory,
            storage,
            metrics,
        }
    }

    /// Access the in-memory cache.
    pub fn memory(&self) -> &Cache<K, V, S> {
        &self.memory
    }

    /// Insert cache entry to the hybrid cache.
    pub fn insert(&self, key: K, value: V) -> HybridCacheEntry<K, V, S> {
        let now = Instant::now();

        let entry = self.memory.insert(key, value);
        self.storage.enqueue(entry.clone(), false);

        self.metrics.hybrid_insert.increment(1);
        self.metrics.hybrid_insert_duration.record(now.elapsed());

        entry
    }

    /// Insert cache entry with cache context to the hybrid cache.
    pub fn insert_with_context(&self, key: K, value: V, context: CacheContext) -> HybridCacheEntry<K, V, S> {
        let now = Instant::now();

        let entry = self.memory.insert_with_context(key, value, context);
        self.storage.enqueue(entry.clone(), false);

        self.metrics.hybrid_insert.increment(1);
        self.metrics.hybrid_insert_duration.record(now.elapsed());

        entry
    }

    /// Insert disk cache only.
    pub fn insert_storage(&self, key: K, value: V) -> HybridCacheEntry<K, V, S> {
        let now = Instant::now();

        let entry = self.memory.deposit(key, value);
        self.storage.enqueue(entry.clone(), false);

        self.metrics.hybrid_insert.increment(1);
        self.metrics.hybrid_insert_duration.record(now.elapsed());

        entry
    }

    /// Insert disk cache only with cache context.
    pub async fn insert_storage_with_fetch<F, FU>(&self, key: K, fetch: F) -> Option<HybridCacheEntry<K, V, S>>
    where
        F: FnOnce() -> FU,
        FU: Future<Output = anyhow::Result<V>> + Send + 'static,
    {
        let now = Instant::now();

        if !self.storage.pick(&key) {
            return None;
        }

        let value = match fetch().await {
            Ok(value) => value,
            Err(e) => {
                tracing::warn!("[hybrid cache]: error raised when fetching value during `insert_storage_with_fetch`, skip entry, error: {e}");
                return None;
            }
        };

        let entry = self.memory.deposit(key, value);
        self.storage.enqueue(entry.clone(), true);

        self.metrics.hybrid_insert.increment(1);
        self.metrics.hybrid_insert_duration.record(now.elapsed());

        Some(entry)
    }

    /// Get cached entry with the given key from the hybrid cache..
    pub async fn get<Q>(&self, key: &Q) -> anyhow::Result<Option<HybridCacheEntry<K, V, S>>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Send + Sync + 'static + Clone,
    {
        let now = Instant::now();

        let record_hit = || {
            self.metrics.hybrid_hit.increment(1);
            self.metrics.hybrid_hit_duration.record(now.elapsed());
        };
        let record_miss = || {
            self.metrics.hybrid_miss.increment(1);
            self.metrics.hybrid_miss_duration.record(now.elapsed());
        };

        if let Some(entry) = self.memory.get(key) {
            record_hit();
            return Ok(Some(entry));
        }

        if let Some((k, v)) = self.storage.load(key).await? {
            if k.borrow() != key {
                record_miss();
                return Ok(None);
            }
            record_hit();
            return Ok(Some(self.memory.insert(k, v)));
        }
        record_miss();
        Ok(None)
    }

    /// Get cached entry with the given key from the hybrid cache.
    ///
    /// Different from `get`, `obtain` dedeplicates the disk cache queries.
    ///
    /// `obtain` is always supposed to be used instead of `get` if the overhead of getting the ownership of the given
    /// key is acceptable.
    pub async fn obtain(&self, key: K) -> anyhow::Result<Option<HybridCacheEntry<K, V, S>>>
    where
        K: Clone,
    {
        let now = Instant::now();

        let res = self
            .memory
            .fetch(key.clone(), || {
                let store = self.storage.clone();
                async move {
                    match store.load(&key).await.map_err(anyhow::Error::from) {
                        Err(e) => Err(e),
                        Ok(None) => Ok(None),
                        Ok(Some((k, _))) if key != k => Ok(None),
                        Ok(Some((_, v))) => Ok(Some((v, CacheContext::default()))),
                    }
                }
            })
            .await;

        match res {
            Ok(Some(_)) => {
                self.metrics.hybrid_hit.increment(1);
                self.metrics.hybrid_hit_duration.record(now.elapsed());
            }
            Ok(None) => {
                self.metrics.hybrid_miss.increment(1);
                self.metrics.hybrid_miss_duration.record(now.elapsed());
            }
            _ => {}
        }

        res
    }

    /// Remove a cached entry with the given key from the hybrid cache.
    pub fn remove<Q>(&self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Send + Sync + 'static,
    {
        let now = Instant::now();

        self.memory.remove(key);
        self.storage.delete(key);

        self.metrics.hybrid_remove.increment(1);
        self.metrics.hybrid_remove_duration.record(now.elapsed());
    }

    /// Check if the hybrid cache contains a cached entry with the given key.
    ///
    /// `contains` may return a false-positive result if there is a hash collision with the given key.
    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.memory.contains(key) || self.storage.may_contains(key)
    }

    /// Clear the hybrid cache.
    pub async fn clear(&self) -> anyhow::Result<()> {
        self.memory.clear();
        self.storage.destroy().await?;
        Ok(())
    }

    /// Gracefully close the hybrid cache.
    ///
    /// `close` will wait for the ongoing flush and reclaim tasks to finish.
    pub async fn close(&self) -> anyhow::Result<()> {
        self.storage.close().await?;
        Ok(())
    }

    /// Return the statistics information of the hybrid cache.
    pub fn stats(&self) -> Arc<DeviceStats> {
        self.storage.stats()
    }
}

/// The future generated by [`HybridCache::fetch`].
pub type HybridFetch<K, V, S = RandomState> = Fetch<K, V, anyhow::Error, S>;

impl<K, V, S> HybridCache<K, V, S>
where
    K: StorageKey + Clone,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    /// Fetch and insert a cache entry with the given method if there is a cache miss.
    pub fn fetch<F, FU>(&self, key: K, fetch: F) -> HybridFetch<K, V, S>
    where
        F: FnOnce() -> FU,
        FU: Future<Output = anyhow::Result<Option<(V, CacheContext)>>> + Send + 'static,
    {
        let store = self.storage.clone();
        self.memory.fetch(key.clone(), || {
            let future = fetch();
            async move {
                match store.load(&key).await.map_err(anyhow::Error::from)? {
                    None => {}
                    Some((k, _)) if key != k => {}
                    Some((_, v)) => return Ok(Some((v, CacheContext::default()))),
                }
                future.await.map_err(anyhow::Error::from)
            }
        })
    }
}

#[cfg(test)]
mod tests {

    use std::path::Path;

    use crate::*;

    const KB: usize = 1024;
    const MB: usize = 1024 * 1024;

    async fn open(dir: impl AsRef<Path>) -> HybridCache<u64, Vec<u8>> {
        HybridCacheBuilder::new()
            .with_name("test")
            .memory(4 * MB)
            .storage()
            .with_device_config(
                DirectFsDeviceOptionsBuilder::new(dir)
                    .with_capacity(16 * MB)
                    .with_file_size(MB)
                    .build(),
            )
            .build()
            .await
            .unwrap()
    }

    #[test_log::test(tokio::test)]
    async fn test_hybrid_cache() {
        let dir = tempfile::tempdir().unwrap();

        let hybrid = open(dir.path()).await;

        let e1 = hybrid.insert(1, vec![1; 7 * KB]);
        let e2 = hybrid.insert_with_context(2, vec![2; 7 * KB], CacheContext::default());
        assert_eq!(e1.value(), &vec![1; 7 * KB]);
        assert_eq!(e2.value(), &vec![2; 7 * KB]);

        let e3 = hybrid.insert_storage(3, vec![3; 7 * KB]);
        let e4 = hybrid
            .insert_storage_with_fetch(4, || async move { Ok(vec![4; 7 * KB]) })
            .await
            .unwrap();
        assert_eq!(e3.value(), &vec![3; 7 * KB]);
        assert_eq!(e4.value(), &vec![4; 7 * KB]);

        let e5 = hybrid
            .fetch(
                5,
                || async move { Ok(Some((vec![5; 7 * KB], CacheContext::default()))) },
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(e5.value(), &vec![5; 7 * KB]);

        let e1g = hybrid.get(&1).await.unwrap().unwrap();
        assert_eq!(e1g.value(), &vec![1; 7 * KB]);
        let e2g = hybrid.obtain(2).await.unwrap().unwrap();
        assert_eq!(e2g.value(), &vec![2; 7 * KB]);

        hybrid.storage.wait().await.unwrap();

        assert!(hybrid.contains(&3));
        hybrid.remove(&3);
        assert!(!hybrid.contains(&3));

        assert!(hybrid.contains(&4));
        hybrid.clear().await.unwrap();
        assert!(!hybrid.contains(&4));
    }
}
