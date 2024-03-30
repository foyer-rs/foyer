//  Copyright 2024 MrCroxx
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

use std::{ops::Deref, sync::Arc};

use futures::{Future, FutureExt};
use tokio::sync::oneshot;

use crate::{
    context::CacheContext,
    eviction::{
        fifo::{Fifo, FifoHandle},
        lfu::{Lfu, LfuHandle},
        lru::{Lru, LruHandle},
    },
    generic::{CacheConfig, GenericCache, GenericCacheEntry, GenericEntry},
    indexer::HashTableIndexer,
    listener::{CacheEventListener, DefaultCacheEventListener},
    metrics::Metrics,
    Key, Value,
};

pub type FifoCache<K, V, L = DefaultCacheEventListener<K, V>> =
    GenericCache<K, V, FifoHandle<K, V>, Fifo<K, V>, HashTableIndexer<K, FifoHandle<K, V>>, L>;
pub type FifoCacheConfig<K, V, L = DefaultCacheEventListener<K, V>> = CacheConfig<Fifo<K, V>, L>;
pub type FifoCacheEntry<K, V, L = DefaultCacheEventListener<K, V>> =
    GenericCacheEntry<K, V, FifoHandle<K, V>, Fifo<K, V>, HashTableIndexer<K, FifoHandle<K, V>>, L>;
pub type FifoEntry<K, V, ER, L = DefaultCacheEventListener<K, V>> =
    GenericEntry<K, V, FifoHandle<K, V>, Fifo<K, V>, HashTableIndexer<K, FifoHandle<K, V>>, L, ER>;

pub type LruCache<K, V, L = DefaultCacheEventListener<K, V>> =
    GenericCache<K, V, LruHandle<K, V>, Lru<K, V>, HashTableIndexer<K, LruHandle<K, V>>, L>;
pub type LruCacheConfig<K, V, L = DefaultCacheEventListener<K, V>> = CacheConfig<Lru<K, V>, L>;
pub type LruCacheEntry<K, V, L = DefaultCacheEventListener<K, V>> =
    GenericCacheEntry<K, V, LruHandle<K, V>, Lru<K, V>, HashTableIndexer<K, LruHandle<K, V>>, L>;
pub type LruEntry<K, V, ER, L = DefaultCacheEventListener<K, V>> =
    GenericEntry<K, V, LruHandle<K, V>, Lru<K, V>, HashTableIndexer<K, LruHandle<K, V>>, L, ER>;

pub type LfuCache<K, V, L = DefaultCacheEventListener<K, V>> =
    GenericCache<K, V, LfuHandle<K, V>, Lfu<K, V>, HashTableIndexer<K, LfuHandle<K, V>>, L>;
pub type LfuCacheConfig<K, V, L = DefaultCacheEventListener<K, V>> = CacheConfig<Lfu<K, V>, L>;
pub type LfuCacheEntry<K, V, L = DefaultCacheEventListener<K, V>> =
    GenericCacheEntry<K, V, LfuHandle<K, V>, Lfu<K, V>, HashTableIndexer<K, LfuHandle<K, V>>, L>;
pub type LfuEntry<K, V, ER, L = DefaultCacheEventListener<K, V>> =
    GenericEntry<K, V, LfuHandle<K, V>, Lfu<K, V>, HashTableIndexer<K, LfuHandle<K, V>>, L, ER>;

pub enum CacheEntry<K, V, L>
where
    K: Key,
    V: Value,
    L: CacheEventListener<K, V>,
{
    Fifo(FifoCacheEntry<K, V, L>),
    Lru(LruCacheEntry<K, V, L>),
    Lfu(LfuCacheEntry<K, V, L>),
}

impl<K, V, L> Clone for CacheEntry<K, V, L>
where
    K: Key,
    V: Value,
    L: CacheEventListener<K, V>,
{
    fn clone(&self) -> Self {
        match self {
            Self::Fifo(entry) => Self::Fifo(entry.clone()),
            Self::Lru(entry) => Self::Lru(entry.clone()),
            Self::Lfu(entry) => Self::Lfu(entry.clone()),
        }
    }
}

impl<K, V, L> Deref for CacheEntry<K, V, L>
where
    K: Key,
    V: Value,
    L: CacheEventListener<K, V>,
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        match self {
            CacheEntry::Fifo(entry) => entry.deref(),
            CacheEntry::Lru(entry) => entry.deref(),
            CacheEntry::Lfu(entry) => entry.deref(),
        }
    }
}

impl<K, V, L> From<FifoCacheEntry<K, V, L>> for CacheEntry<K, V, L>
where
    K: Key,
    V: Value,
    L: CacheEventListener<K, V>,
{
    fn from(entry: FifoCacheEntry<K, V, L>) -> Self {
        Self::Fifo(entry)
    }
}

impl<K, V, L> From<LruCacheEntry<K, V, L>> for CacheEntry<K, V, L>
where
    K: Key,
    V: Value,
    L: CacheEventListener<K, V>,
{
    fn from(entry: LruCacheEntry<K, V, L>) -> Self {
        Self::Lru(entry)
    }
}

impl<K, V, L> From<LfuCacheEntry<K, V, L>> for CacheEntry<K, V, L>
where
    K: Key,
    V: Value,
    L: CacheEventListener<K, V>,
{
    fn from(entry: LfuCacheEntry<K, V, L>) -> Self {
        Self::Lfu(entry)
    }
}

impl<K, V, L> CacheEntry<K, V, L>
where
    K: Key,
    V: Value,
    L: CacheEventListener<K, V>,
{
    pub fn key(&self) -> &K {
        match self {
            CacheEntry::Fifo(entry) => entry.key(),
            CacheEntry::Lru(entry) => entry.key(),
            CacheEntry::Lfu(entry) => entry.key(),
        }
    }

    pub fn value(&self) -> &V {
        match self {
            CacheEntry::Fifo(entry) => entry.value(),
            CacheEntry::Lru(entry) => entry.value(),
            CacheEntry::Lfu(entry) => entry.value(),
        }
    }

    pub fn context(&self) -> CacheContext {
        match self {
            CacheEntry::Fifo(entry) => entry.context().clone().into(),
            CacheEntry::Lru(entry) => entry.context().clone().into(),
            CacheEntry::Lfu(entry) => entry.context().clone().into(),
        }
    }

    pub fn charge(&self) -> usize {
        match self {
            CacheEntry::Fifo(entry) => entry.charge(),
            CacheEntry::Lru(entry) => entry.charge(),
            CacheEntry::Lfu(entry) => entry.charge(),
        }
    }

    pub fn refs(&self) -> usize {
        match self {
            CacheEntry::Fifo(entry) => entry.refs(),
            CacheEntry::Lru(entry) => entry.refs(),
            CacheEntry::Lfu(entry) => entry.refs(),
        }
    }
}

pub enum Cache<K, V, L = DefaultCacheEventListener<K, V>>
where
    K: Key,
    V: Value,
    L: CacheEventListener<K, V>,
{
    Fifo(Arc<FifoCache<K, V, L>>),
    Lru(Arc<LruCache<K, V, L>>),
    Lfu(Arc<LfuCache<K, V, L>>),
}

impl<K, V, L> Clone for Cache<K, V, L>
where
    K: Key,
    V: Value,
    L: CacheEventListener<K, V>,
{
    fn clone(&self) -> Self {
        match self {
            Self::Fifo(cache) => Self::Fifo(cache.clone()),
            Self::Lru(cache) => Self::Lru(cache.clone()),
            Self::Lfu(cache) => Self::Lfu(cache.clone()),
        }
    }
}

impl<K, V, L> Cache<K, V, L>
where
    K: Key,
    V: Value,
    L: CacheEventListener<K, V>,
{
    pub fn fifo(config: FifoCacheConfig<K, V, L>) -> Self {
        Self::Fifo(Arc::new(GenericCache::new(config)))
    }

    pub fn lru(config: LruCacheConfig<K, V, L>) -> Self {
        Self::Lru(Arc::new(GenericCache::new(config)))
    }

    pub fn lfu(config: LfuCacheConfig<K, V, L>) -> Self {
        Self::Lfu(Arc::new(GenericCache::new(config)))
    }

    pub fn insert(&self, key: K, value: V, charge: usize) -> CacheEntry<K, V, L> {
        match self {
            Cache::Fifo(cache) => cache.insert(key, value, charge).into(),
            Cache::Lru(cache) => cache.insert(key, value, charge).into(),
            Cache::Lfu(cache) => cache.insert(key, value, charge).into(),
        }
    }

    pub fn insert_with_context(&self, key: K, value: V, charge: usize, context: CacheContext) -> CacheEntry<K, V, L> {
        match self {
            Cache::Fifo(cache) => cache.insert_with_context(key, value, charge, context).into(),
            Cache::Lru(cache) => cache.insert_with_context(key, value, charge, context).into(),
            Cache::Lfu(cache) => cache.insert_with_context(key, value, charge, context).into(),
        }
    }

    pub fn remove(&self, key: &K) {
        match self {
            Cache::Fifo(cache) => cache.remove(key),
            Cache::Lru(cache) => cache.remove(key),
            Cache::Lfu(cache) => cache.remove(key),
        }
    }

    pub fn get(&self, key: &K) -> Option<CacheEntry<K, V, L>> {
        match self {
            Cache::Fifo(cache) => cache.get(key).map(CacheEntry::from),
            Cache::Lru(cache) => cache.get(key).map(CacheEntry::from),
            Cache::Lfu(cache) => cache.get(key).map(CacheEntry::from),
        }
    }

    pub fn clear(&self) {
        match self {
            Cache::Fifo(cache) => cache.clear(),
            Cache::Lru(cache) => cache.clear(),
            Cache::Lfu(cache) => cache.clear(),
        }
    }

    pub fn capacity(&self) -> usize {
        match self {
            Cache::Fifo(cache) => cache.capacity(),
            Cache::Lru(cache) => cache.capacity(),
            Cache::Lfu(cache) => cache.capacity(),
        }
    }

    pub fn usage(&self) -> usize {
        match self {
            Cache::Fifo(cache) => cache.usage(),
            Cache::Lru(cache) => cache.usage(),
            Cache::Lfu(cache) => cache.usage(),
        }
    }

    pub fn metrics(&self) -> &Metrics {
        match self {
            Cache::Fifo(cache) => cache.metrics(),
            Cache::Lru(cache) => cache.metrics(),
            Cache::Lfu(cache) => cache.metrics(),
        }
    }
}

pub enum Entry<K, V, ER, L = DefaultCacheEventListener<K, V>>
where
    K: Key + Clone,
    V: Value,
    ER: std::error::Error,
    L: CacheEventListener<K, V>,
{
    Fifo(FifoEntry<K, V, ER, L>),
    Lru(LruEntry<K, V, ER, L>),
    Lfu(LfuEntry<K, V, ER, L>),
}

impl<K, V, ER, L> From<FifoEntry<K, V, ER, L>> for Entry<K, V, ER, L>
where
    K: Key + Clone,
    V: Value,
    ER: std::error::Error,
    L: CacheEventListener<K, V>,
{
    fn from(entry: FifoEntry<K, V, ER, L>) -> Self {
        Self::Fifo(entry)
    }
}

impl<K, V, ER, L> From<LruEntry<K, V, ER, L>> for Entry<K, V, ER, L>
where
    K: Key + Clone,
    V: Value,
    ER: std::error::Error,
    L: CacheEventListener<K, V>,
{
    fn from(entry: LruEntry<K, V, ER, L>) -> Self {
        Self::Lru(entry)
    }
}

impl<K, V, ER, L> From<LfuEntry<K, V, ER, L>> for Entry<K, V, ER, L>
where
    K: Key + Clone,
    V: Value,
    ER: std::error::Error,
    L: CacheEventListener<K, V>,
{
    fn from(entry: LfuEntry<K, V, ER, L>) -> Self {
        Self::Lfu(entry)
    }
}

impl<K, V, ER, L> Future for Entry<K, V, ER, L>
where
    K: Key + Clone,
    V: Value,
    ER: std::error::Error + From<oneshot::error::RecvError>,
    L: CacheEventListener<K, V>,
{
    type Output = std::result::Result<CacheEntry<K, V, L>, ER>;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        match &mut *self {
            Entry::Fifo(entry) => entry.poll_unpin(cx).map(|res| res.map(CacheEntry::from)),
            Entry::Lru(entry) => entry.poll_unpin(cx).map(|res| res.map(CacheEntry::from)),
            Entry::Lfu(entry) => entry.poll_unpin(cx).map(|res| res.map(CacheEntry::from)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryState {
    Hit,
    Wait,
    Miss,
}

impl<K, V, ER, L> Entry<K, V, ER, L>
where
    K: Key + Clone,
    V: Value,
    ER: std::error::Error,
    L: CacheEventListener<K, V>,
{
    pub fn state(&self) -> EntryState {
        match self {
            Entry::Fifo(FifoEntry::Hit(_)) | Entry::Lru(LruEntry::Hit(_)) | Entry::Lfu(LfuEntry::Hit(_)) => {
                EntryState::Hit
            }
            Entry::Fifo(FifoEntry::Wait(_)) | Entry::Lru(LruEntry::Wait(_)) | Entry::Lfu(LfuEntry::Wait(_)) => {
                EntryState::Wait
            }
            Entry::Fifo(FifoEntry::Miss(_)) | Entry::Lru(LruEntry::Miss(_)) | Entry::Lfu(LfuEntry::Miss(_)) => {
                EntryState::Miss
            }
            _ => unreachable!(),
        }
    }
}

impl<K, V, L> Cache<K, V, L>
where
    K: Key + Clone,
    V: Value,
    L: CacheEventListener<K, V>,
{
    pub fn entry<F, FU, ER>(&self, key: K, f: F) -> Entry<K, V, ER, L>
    where
        F: FnOnce() -> FU,
        FU: Future<Output = std::result::Result<(V, usize, CacheContext), ER>> + Send + 'static,
        ER: std::error::Error + Send + 'static,
    {
        match self {
            Cache::Fifo(cache) => Entry::from(cache.entry(key, f)),
            Cache::Lru(cache) => Entry::from(cache.entry(key, f)),
            Cache::Lfu(cache) => Entry::from(cache.entry(key, f)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{ops::Range, time::Duration};

    use futures::future::join_all;
    use hashbrown::hash_map::DefaultHashBuilder;
    use itertools::Itertools;
    use rand::{rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};

    use super::*;
    use crate::{FifoConfig, LfuConfig, LruConfig};

    const CAPACITY: usize = 100;
    const SHARDS: usize = 4;
    const OBJECT_POOL_CAPACITY: usize = 64;
    const RANGE: Range<u64> = 0..1000;
    const OPS: usize = 10000;
    const CONCURRENCY: usize = 8;

    fn fifo() -> Cache<u64, u64> {
        Cache::fifo(FifoCacheConfig {
            capacity: CAPACITY,
            shards: SHARDS,
            eviction_config: FifoConfig {},
            object_pool_capacity: OBJECT_POOL_CAPACITY,
            hash_builder: DefaultHashBuilder::default(),
            event_listener: DefaultCacheEventListener::default(),
        })
    }

    fn lru() -> Cache<u64, u64> {
        Cache::lru(LruCacheConfig {
            capacity: CAPACITY,
            shards: SHARDS,
            eviction_config: LruConfig {
                high_priority_pool_ratio: 0.1,
            },
            object_pool_capacity: OBJECT_POOL_CAPACITY,
            hash_builder: DefaultHashBuilder::default(),
            event_listener: DefaultCacheEventListener::default(),
        })
    }

    fn lfu() -> Cache<u64, u64> {
        Cache::lfu(LfuCacheConfig {
            capacity: CAPACITY,
            shards: SHARDS,
            eviction_config: LfuConfig {
                window_capacity_ratio: 0.1,
                protected_capacity_ratio: 0.8,
                cmsketch_eps: 0.001,
                cmsketch_confidence: 0.9,
            },
            object_pool_capacity: OBJECT_POOL_CAPACITY,
            hash_builder: DefaultHashBuilder::default(),
            event_listener: DefaultCacheEventListener::default(),
        })
    }

    fn init_cache(cache: &Cache<u64, u64>, rng: &mut StdRng) {
        let mut v = RANGE.collect_vec();
        v.shuffle(rng);
        for i in v {
            cache.insert(i, i, 1);
        }
    }

    async fn operate(cache: &Cache<u64, u64>, rng: &mut StdRng) {
        let i = rng.gen_range(RANGE);
        match rng.gen_range(0..=3) {
            0 => {
                let entry = cache.insert(i, i, 1);
                assert_eq!(*entry.key(), i);
                assert_eq!(entry.key(), entry.value());
            }
            1 => {
                if let Some(entry) = cache.get(&i) {
                    assert_eq!(*entry.key(), i);
                    assert_eq!(entry.key(), entry.value());
                }
            }
            2 => {
                cache.remove(&i);
            }
            3 => {
                let entry = cache
                    .entry(i, || async move {
                        tokio::time::sleep(Duration::from_micros(10)).await;
                        Ok::<_, tokio::sync::oneshot::error::RecvError>((i, 1, CacheContext::Default))
                    })
                    .await
                    .unwrap();
                assert_eq!(*entry.key(), i);
                assert_eq!(entry.key(), entry.value());
            }
            _ => unreachable!(),
        }
    }

    async fn case(cache: Cache<u64, u64>) {
        let mut rng = StdRng::seed_from_u64(42);

        init_cache(&cache, &mut rng);

        let handles = (0..CONCURRENCY)
            .map(|_| {
                let cache = cache.clone();
                let mut rng = rng.clone();
                tokio::spawn(async move {
                    for _ in 0..OPS {
                        operate(&cache, &mut rng).await;
                    }
                })
            })
            .collect_vec();

        join_all(handles).await;
    }

    #[tokio::test]
    async fn test_fifo_cache() {
        case(fifo()).await
    }

    #[tokio::test]
    async fn test_lru_cache() {
        case(lru()).await
    }

    #[tokio::test]
    async fn test_lfu_cache() {
        case(lfu()).await
    }
}
