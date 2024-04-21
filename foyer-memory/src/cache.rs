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
    hash::{BuildHasher, Hash},
    ops::Deref,
    sync::Arc,
};

use ahash::RandomState;
use futures::{Future, FutureExt};
use tokio::sync::oneshot;

use foyer_common::code::{Key, Value};

use crate::{
    context::CacheContext,
    eviction::{
        fifo::{Fifo, FifoHandle},
        lfu::{Lfu, LfuHandle},
        lru::{Lru, LruHandle},
        s3fifo::{S3Fifo, S3FifoHandle},
    },
    generic::{GenericCache, GenericCacheConfig, GenericCacheEntry, GenericEntry, Weighter},
    indexer::HashTableIndexer,
    listener::{CacheEventListener, DefaultCacheEventListener},
    metrics::Metrics,
    FifoConfig, LfuConfig, LruConfig, S3FifoConfig,
};

pub type FifoCache<K, V, L = DefaultCacheEventListener<K, V>, S = RandomState> =
    GenericCache<K, V, Fifo<(K, V)>, HashTableIndexer<K, FifoHandle<(K, V)>>, L, S>;
pub type FifoCacheEntry<K, V, L = DefaultCacheEventListener<K, V>, S = RandomState> =
    GenericCacheEntry<K, V, Fifo<(K, V)>, HashTableIndexer<K, FifoHandle<(K, V)>>, L, S>;
pub type FifoEntry<K, V, ER, L = DefaultCacheEventListener<K, V>, S = RandomState> =
    GenericEntry<K, V, Fifo<(K, V)>, HashTableIndexer<K, FifoHandle<(K, V)>>, L, S, ER>;

pub type LruCache<K, V, L = DefaultCacheEventListener<K, V>, S = RandomState> =
    GenericCache<K, V, Lru<(K, V)>, HashTableIndexer<K, LruHandle<(K, V)>>, L, S>;
pub type LruCacheEntry<K, V, L = DefaultCacheEventListener<K, V>, S = RandomState> =
    GenericCacheEntry<K, V, Lru<(K, V)>, HashTableIndexer<K, LruHandle<(K, V)>>, L, S>;
pub type LruEntry<K, V, ER, L = DefaultCacheEventListener<K, V>, S = RandomState> =
    GenericEntry<K, V, Lru<(K, V)>, HashTableIndexer<K, LruHandle<(K, V)>>, L, S, ER>;

pub type LfuCache<K, V, L = DefaultCacheEventListener<K, V>, S = RandomState> =
    GenericCache<K, V, Lfu<(K, V)>, HashTableIndexer<K, LfuHandle<(K, V)>>, L, S>;
pub type LfuCacheEntry<K, V, L = DefaultCacheEventListener<K, V>, S = RandomState> =
    GenericCacheEntry<K, V, Lfu<(K, V)>, HashTableIndexer<K, LfuHandle<(K, V)>>, L, S>;
pub type LfuEntry<K, V, ER, L = DefaultCacheEventListener<K, V>, S = RandomState> =
    GenericEntry<K, V, Lfu<(K, V)>, HashTableIndexer<K, LfuHandle<(K, V)>>, L, S, ER>;

pub type S3FifoCache<K, V, L = DefaultCacheEventListener<K, V>, S = RandomState> =
    GenericCache<K, V, S3Fifo<(K, V)>, HashTableIndexer<K, S3FifoHandle<(K, V)>>, L, S>;
pub type S3FifoCacheEntry<K, V, L = DefaultCacheEventListener<K, V>, S = RandomState> =
    GenericCacheEntry<K, V, S3Fifo<(K, V)>, HashTableIndexer<K, S3FifoHandle<(K, V)>>, L, S>;
pub type S3FifoEntry<K, V, ER, L = DefaultCacheEventListener<K, V>, S = RandomState> =
    GenericEntry<K, V, S3Fifo<(K, V)>, HashTableIndexer<K, S3FifoHandle<(K, V)>>, L, S, ER>;

pub enum CacheEntry<K, V, L, S = RandomState>
where
    K: Key,
    V: Value,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
{
    Fifo(FifoCacheEntry<K, V, L, S>),
    Lru(LruCacheEntry<K, V, L, S>),
    Lfu(LfuCacheEntry<K, V, L, S>),
    S3Fifo(S3FifoCacheEntry<K, V, L, S>),
}

impl<K, V, L, S> Clone for CacheEntry<K, V, L, S>
where
    K: Key,
    V: Value,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        match self {
            Self::Fifo(entry) => Self::Fifo(entry.clone()),
            Self::Lru(entry) => Self::Lru(entry.clone()),
            Self::Lfu(entry) => Self::Lfu(entry.clone()),
            Self::S3Fifo(entry) => Self::S3Fifo(entry.clone()),
        }
    }
}

impl<K, V, L, S> Deref for CacheEntry<K, V, L, S>
where
    K: Key,
    V: Value,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        match self {
            CacheEntry::Fifo(entry) => entry.deref(),
            CacheEntry::Lru(entry) => entry.deref(),
            CacheEntry::Lfu(entry) => entry.deref(),
            CacheEntry::S3Fifo(entry) => entry.deref(),
        }
    }
}

impl<K, V, L, S> From<FifoCacheEntry<K, V, L, S>> for CacheEntry<K, V, L, S>
where
    K: Key,
    V: Value,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
{
    fn from(entry: FifoCacheEntry<K, V, L, S>) -> Self {
        Self::Fifo(entry)
    }
}

impl<K, V, L, S> From<LruCacheEntry<K, V, L, S>> for CacheEntry<K, V, L, S>
where
    K: Key,
    V: Value,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
{
    fn from(entry: LruCacheEntry<K, V, L, S>) -> Self {
        Self::Lru(entry)
    }
}

impl<K, V, L, S> From<LfuCacheEntry<K, V, L, S>> for CacheEntry<K, V, L, S>
where
    K: Key,
    V: Value,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
{
    fn from(entry: LfuCacheEntry<K, V, L, S>) -> Self {
        Self::Lfu(entry)
    }
}

impl<K, V, L, S> From<S3FifoCacheEntry<K, V, L, S>> for CacheEntry<K, V, L, S>
where
    K: Key,
    V: Value,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
{
    fn from(entry: S3FifoCacheEntry<K, V, L, S>) -> Self {
        Self::S3Fifo(entry)
    }
}

impl<K, V, L, S> CacheEntry<K, V, L, S>
where
    K: Key,
    V: Value,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
{
    pub fn key(&self) -> &K {
        match self {
            CacheEntry::Fifo(entry) => entry.key(),
            CacheEntry::Lru(entry) => entry.key(),
            CacheEntry::Lfu(entry) => entry.key(),
            CacheEntry::S3Fifo(entry) => entry.key(),
        }
    }

    pub fn value(&self) -> &V {
        match self {
            CacheEntry::Fifo(entry) => entry.value(),
            CacheEntry::Lru(entry) => entry.value(),
            CacheEntry::Lfu(entry) => entry.value(),
            CacheEntry::S3Fifo(entry) => entry.value(),
        }
    }

    pub fn context(&self) -> CacheContext {
        match self {
            CacheEntry::Fifo(entry) => entry.context().clone().into(),
            CacheEntry::Lru(entry) => entry.context().clone().into(),
            CacheEntry::Lfu(entry) => entry.context().clone().into(),
            CacheEntry::S3Fifo(entry) => entry.context().clone().into(),
        }
    }

    pub fn weight(&self) -> usize {
        match self {
            CacheEntry::Fifo(entry) => entry.weight(),
            CacheEntry::Lru(entry) => entry.weight(),
            CacheEntry::Lfu(entry) => entry.weight(),
            CacheEntry::S3Fifo(entry) => entry.weight(),
        }
    }

    pub fn refs(&self) -> usize {
        match self {
            CacheEntry::Fifo(entry) => entry.refs(),
            CacheEntry::Lru(entry) => entry.refs(),
            CacheEntry::Lfu(entry) => entry.refs(),
            CacheEntry::S3Fifo(entry) => entry.refs(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum EvictionConfig {
    Fifo(FifoConfig),
    Lru(LruConfig),
    Lfu(LfuConfig),
    S3Fifo(S3FifoConfig),
}

impl From<FifoConfig> for EvictionConfig {
    fn from(value: FifoConfig) -> EvictionConfig {
        EvictionConfig::Fifo(value)
    }
}

impl From<LruConfig> for EvictionConfig {
    fn from(value: LruConfig) -> EvictionConfig {
        EvictionConfig::Lru(value)
    }
}

impl From<LfuConfig> for EvictionConfig {
    fn from(value: LfuConfig) -> EvictionConfig {
        EvictionConfig::Lfu(value)
    }
}

impl From<S3FifoConfig> for EvictionConfig {
    fn from(value: S3FifoConfig) -> EvictionConfig {
        EvictionConfig::S3Fifo(value)
    }
}

pub struct CacheBuilder<K, V, L, S>
where
    K: Key,
    V: Value,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
{
    capacity: usize,
    shards: usize,
    eviction_config: EvictionConfig,
    object_pool_capacity: usize,
    event_listener: L,
    hash_builder: S,
    weighter: Arc<dyn Weighter<K, V>>,
}

impl<K, V> CacheBuilder<K, V, DefaultCacheEventListener<K, V>, RandomState>
where
    K: Key,
    V: Value,
{
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            shards: 8,
            eviction_config: LfuConfig {
                window_capacity_ratio: 0.1,
                protected_capacity_ratio: 0.8,
                cmsketch_eps: 0.001,
                cmsketch_confidence: 0.9,
            }
            .into(),
            object_pool_capacity: 1024,
            event_listener: DefaultCacheEventListener::default(),
            hash_builder: RandomState::default(),
            weighter: Arc::new(|_, _| 1),
        }
    }
}

impl<K, V, L, S> CacheBuilder<K, V, L, S>
where
    K: Key,
    V: Value,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
{
    /// Set in-memory cache sharding count. Entries will be distributed to different shards based on their hash.
    /// Operations on different shard can be parallelized.
    pub fn with_shards(mut self, shards: usize) -> Self {
        self.shards = shards;
        self
    }

    /// Set in-memory cache eviction algorithm.
    ///
    /// The default value is a general-used w-TinyLFU algorithm.
    pub fn with_eviction_config(mut self, eviction_config: impl Into<EvictionConfig>) -> Self {
        self.eviction_config = eviction_config.into();
        self
    }

    /// Set object pool for handles. The object pool is used to reduce handle allocation.
    ///
    /// The optimized value is supposed to be equal to the max cache entry count.
    ///
    /// The default value is 1024.
    pub fn with_object_pool_capacity(mut self, object_pool_capacity: usize) -> Self {
        self.object_pool_capacity = object_pool_capacity;
        self
    }

    /// Set in-memory cache event listener.
    pub fn with_event_listener<OL>(self, event_listener: OL) -> CacheBuilder<K, V, OL, S>
    where
        OL: CacheEventListener<K, V>,
    {
        CacheBuilder {
            capacity: self.capacity,
            shards: self.shards,
            eviction_config: self.eviction_config,
            object_pool_capacity: self.object_pool_capacity,
            event_listener,
            hash_builder: self.hash_builder,
            weighter: self.weighter,
        }
    }

    /// Set in-memory cache hash builder.
    pub fn with_hash_builder<OS>(self, hash_builder: OS) -> CacheBuilder<K, V, L, OS>
    where
        OS: BuildHasher + Send + Sync + 'static,
    {
        CacheBuilder {
            capacity: self.capacity,
            shards: self.shards,
            eviction_config: self.eviction_config,
            object_pool_capacity: self.object_pool_capacity,
            event_listener: self.event_listener,
            hash_builder,
            weighter: self.weighter,
        }
    }

    /// Set in-memory cache weighter.
    pub fn with_weighter(mut self, weighter: impl Weighter<K, V>) -> Self {
        self.weighter = Arc::new(weighter);
        self
    }

    /// Build in-memory cache with the given configuration.
    pub fn build(self) -> Cache<K, V, L, S> {
        match self.eviction_config {
            EvictionConfig::Fifo(eviction_config) => Cache::Fifo(Arc::new(GenericCache::new(GenericCacheConfig {
                capacity: self.capacity,
                shards: self.shards,
                eviction_config,
                object_pool_capacity: self.object_pool_capacity,
                hash_builder: self.hash_builder,
                event_listener: self.event_listener,
                weighter: self.weighter,
            }))),
            EvictionConfig::Lru(eviction_config) => Cache::Lru(Arc::new(GenericCache::new(GenericCacheConfig {
                capacity: self.capacity,
                shards: self.shards,
                eviction_config,
                object_pool_capacity: self.object_pool_capacity,
                hash_builder: self.hash_builder,
                event_listener: self.event_listener,
                weighter: self.weighter,
            }))),
            EvictionConfig::Lfu(eviction_config) => Cache::Lfu(Arc::new(GenericCache::new(GenericCacheConfig {
                capacity: self.capacity,
                shards: self.shards,
                eviction_config,
                object_pool_capacity: self.object_pool_capacity,
                hash_builder: self.hash_builder,
                event_listener: self.event_listener,
                weighter: self.weighter,
            }))),
            EvictionConfig::S3Fifo(eviction_config) => Cache::S3Fifo(Arc::new(GenericCache::new(GenericCacheConfig {
                capacity: self.capacity,
                shards: self.shards,
                eviction_config,
                object_pool_capacity: self.object_pool_capacity,
                hash_builder: self.hash_builder,
                event_listener: self.event_listener,
                weighter: self.weighter,
            }))),
        }
    }
}

pub enum Cache<K, V, L = DefaultCacheEventListener<K, V>, S = RandomState>
where
    K: Key,
    V: Value,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
{
    Fifo(Arc<FifoCache<K, V, L, S>>),
    Lru(Arc<LruCache<K, V, L, S>>),
    Lfu(Arc<LfuCache<K, V, L, S>>),
    S3Fifo(Arc<S3FifoCache<K, V, L, S>>),
}

impl<K, V, L, S> Debug for Cache<K, V, L, S>
where
    K: Key,
    V: Value,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fifo(_) => f.debug_tuple("Cache::FifoCache").finish(),
            Self::Lru(_) => f.debug_tuple("Cache::LruCache").finish(),
            Self::Lfu(_) => f.debug_tuple("Cache::LfuCache").finish(),
            Self::S3Fifo(_) => f.debug_tuple("Cache::S3FifoCache").finish(),
        }
    }
}

impl<K, V, L, S> Clone for Cache<K, V, L, S>
where
    K: Key,
    V: Value,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        match self {
            Self::Fifo(cache) => Self::Fifo(cache.clone()),
            Self::Lru(cache) => Self::Lru(cache.clone()),
            Self::Lfu(cache) => Self::Lfu(cache.clone()),
            Self::S3Fifo(cache) => Self::S3Fifo(cache.clone()),
        }
    }
}

impl<K, V, L, S> Cache<K, V, L, S>
where
    K: Key,
    V: Value,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
{
    pub fn insert(&self, key: K, value: V) -> CacheEntry<K, V, L, S> {
        match self {
            Cache::Fifo(cache) => cache.insert(key, value).into(),
            Cache::Lru(cache) => cache.insert(key, value).into(),
            Cache::Lfu(cache) => cache.insert(key, value).into(),
            Cache::S3Fifo(cache) => cache.insert(key, value).into(),
        }
    }

    pub fn insert_with_context(&self, key: K, value: V, context: CacheContext) -> CacheEntry<K, V, L, S> {
        match self {
            Cache::Fifo(cache) => cache.insert_with_context(key, value, context).into(),
            Cache::Lru(cache) => cache.insert_with_context(key, value, context).into(),
            Cache::Lfu(cache) => cache.insert_with_context(key, value, context).into(),
            Cache::S3Fifo(cache) => cache.insert_with_context(key, value, context).into(),
        }
    }

    pub fn remove<Q>(&self, key: &Q) -> Option<CacheEntry<K, V, L, S>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        match self {
            Cache::Fifo(cache) => cache.remove(key).map(CacheEntry::from),
            Cache::Lru(cache) => cache.remove(key).map(CacheEntry::from),
            Cache::Lfu(cache) => cache.remove(key).map(CacheEntry::from),
            Cache::S3Fifo(cache) => cache.remove(key).map(CacheEntry::from),
        }
    }

    pub fn pop(&self) -> Option<CacheEntry<K, V, L, S>> {
        match self {
            Cache::Fifo(cache) => cache.pop().map(CacheEntry::from),
            Cache::Lru(cache) => cache.pop().map(CacheEntry::from),
            Cache::Lfu(cache) => cache.pop().map(CacheEntry::from),
            Cache::S3Fifo(cache) => cache.pop().map(CacheEntry::from),
        }
    }

    pub fn pop_corase(&self) -> Option<CacheEntry<K, V, L, S>> {
        match self {
            Cache::Fifo(cache) => cache.pop_corase().map(CacheEntry::from),
            Cache::Lru(cache) => cache.pop_corase().map(CacheEntry::from),
            Cache::Lfu(cache) => cache.pop_corase().map(CacheEntry::from),
            Cache::S3Fifo(cache) => cache.pop_corase().map(CacheEntry::from),
        }
    }

    pub fn get<Q>(&self, key: &Q) -> Option<CacheEntry<K, V, L, S>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        match self {
            Cache::Fifo(cache) => cache.get(key).map(CacheEntry::from),
            Cache::Lru(cache) => cache.get(key).map(CacheEntry::from),
            Cache::Lfu(cache) => cache.get(key).map(CacheEntry::from),
            Cache::S3Fifo(cache) => cache.get(key).map(CacheEntry::from),
        }
    }

    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        match self {
            Cache::Fifo(cache) => cache.contains(key),
            Cache::Lru(cache) => cache.contains(key),
            Cache::Lfu(cache) => cache.contains(key),
            Cache::S3Fifo(cache) => cache.contains(key),
        }
    }

    pub fn touch<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        match self {
            Cache::Fifo(cache) => cache.touch(key),
            Cache::Lru(cache) => cache.touch(key),
            Cache::Lfu(cache) => cache.touch(key),
            Cache::S3Fifo(cache) => cache.touch(key),
        }
    }

    pub fn clear(&self) {
        match self {
            Cache::Fifo(cache) => cache.clear(),
            Cache::Lru(cache) => cache.clear(),
            Cache::Lfu(cache) => cache.clear(),
            Cache::S3Fifo(cache) => cache.clear(),
        }
    }

    pub fn capacity(&self) -> usize {
        match self {
            Cache::Fifo(cache) => cache.capacity(),
            Cache::Lru(cache) => cache.capacity(),
            Cache::Lfu(cache) => cache.capacity(),
            Cache::S3Fifo(cache) => cache.capacity(),
        }
    }

    pub fn usage(&self) -> usize {
        match self {
            Cache::Fifo(cache) => cache.usage(),
            Cache::Lru(cache) => cache.usage(),
            Cache::Lfu(cache) => cache.usage(),
            Cache::S3Fifo(cache) => cache.usage(),
        }
    }

    pub fn metrics(&self) -> &Metrics {
        match self {
            Cache::Fifo(cache) => cache.metrics(),
            Cache::Lru(cache) => cache.metrics(),
            Cache::Lfu(cache) => cache.metrics(),
            Cache::S3Fifo(cache) => cache.metrics(),
        }
    }
}

pub enum Entry<K, V, ER, L = DefaultCacheEventListener<K, V>, S = RandomState>
where
    K: Key + Clone,
    V: Value,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
{
    Fifo(FifoEntry<K, V, ER, L, S>),
    Lru(LruEntry<K, V, ER, L, S>),
    Lfu(LfuEntry<K, V, ER, L, S>),
    S3Fifo(S3FifoEntry<K, V, ER, L, S>),
}

impl<K, V, ER, L, S> From<FifoEntry<K, V, ER, L, S>> for Entry<K, V, ER, L, S>
where
    K: Key + Clone,
    V: Value,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
{
    fn from(entry: FifoEntry<K, V, ER, L, S>) -> Self {
        Self::Fifo(entry)
    }
}

impl<K, V, ER, L, S> From<LruEntry<K, V, ER, L, S>> for Entry<K, V, ER, L, S>
where
    K: Key + Clone,
    V: Value,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
{
    fn from(entry: LruEntry<K, V, ER, L, S>) -> Self {
        Self::Lru(entry)
    }
}

impl<K, V, ER, L, S> From<LfuEntry<K, V, ER, L, S>> for Entry<K, V, ER, L, S>
where
    K: Key + Clone,
    V: Value,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
{
    fn from(entry: LfuEntry<K, V, ER, L, S>) -> Self {
        Self::Lfu(entry)
    }
}

impl<K, V, ER, L, S> From<S3FifoEntry<K, V, ER, L, S>> for Entry<K, V, ER, L, S>
where
    K: Key + Clone,
    V: Value,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
{
    fn from(entry: S3FifoEntry<K, V, ER, L, S>) -> Self {
        Self::S3Fifo(entry)
    }
}

impl<K, V, ER, L, S> Future for Entry<K, V, ER, L, S>
where
    K: Key + Clone,
    V: Value,
    ER: From<oneshot::error::RecvError>,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
{
    type Output = std::result::Result<CacheEntry<K, V, L, S>, ER>;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        match &mut *self {
            Entry::Fifo(entry) => entry.poll_unpin(cx).map(|res| res.map(CacheEntry::from)),
            Entry::Lru(entry) => entry.poll_unpin(cx).map(|res| res.map(CacheEntry::from)),
            Entry::Lfu(entry) => entry.poll_unpin(cx).map(|res| res.map(CacheEntry::from)),
            Entry::S3Fifo(entry) => entry.poll_unpin(cx).map(|res| res.map(CacheEntry::from)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryState {
    Hit,
    Wait,
    Miss,
}

impl<K, V, ER, L, S> Entry<K, V, ER, L, S>
where
    K: Key + Clone,
    V: Value,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
{
    pub fn state(&self) -> EntryState {
        match self {
            Entry::Fifo(FifoEntry::Hit(_))
            | Entry::Lru(LruEntry::Hit(_))
            | Entry::Lfu(LfuEntry::Hit(_))
            | Entry::S3Fifo(S3FifoEntry::Hit(_)) => EntryState::Hit,
            Entry::Fifo(FifoEntry::Wait(_))
            | Entry::Lru(LruEntry::Wait(_))
            | Entry::Lfu(LfuEntry::Wait(_))
            | Entry::S3Fifo(S3FifoEntry::Wait(_)) => EntryState::Wait,
            Entry::Fifo(FifoEntry::Miss(_))
            | Entry::Lru(LruEntry::Miss(_))
            | Entry::Lfu(LfuEntry::Miss(_))
            | Entry::S3Fifo(S3FifoEntry::Miss(_)) => EntryState::Miss,
            Entry::Fifo(FifoEntry::Invalid)
            | Entry::Lru(LruEntry::Invalid)
            | Entry::Lfu(LfuEntry::Invalid)
            | Entry::S3Fifo(S3FifoEntry::Invalid) => unreachable!(),
        }
    }
}

impl<K, V, L, S> Cache<K, V, L, S>
where
    K: Key + Clone,
    V: Value,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
{
    pub fn entry<F, FU, ER>(&self, key: K, f: F) -> Entry<K, V, ER, L, S>
    where
        F: FnOnce() -> FU,
        FU: Future<Output = std::result::Result<(V, CacheContext), ER>> + Send + 'static,
        ER: Send + 'static,
    {
        match self {
            Cache::Fifo(cache) => Entry::from(cache.entry(key, f)),
            Cache::Lru(cache) => Entry::from(cache.entry(key, f)),
            Cache::Lfu(cache) => Entry::from(cache.entry(key, f)),
            Cache::S3Fifo(cache) => Entry::from(cache.entry(key, f)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{ops::Range, time::Duration};

    use futures::future::join_all;
    use itertools::Itertools;
    use rand::{rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};

    use super::*;
    use crate::{eviction::s3fifo::S3FifoConfig, FifoConfig, LfuConfig, LruConfig};

    const CAPACITY: usize = 100;
    const SHARDS: usize = 4;
    const OBJECT_POOL_CAPACITY: usize = 64;
    const RANGE: Range<u64> = 0..1000;
    const OPS: usize = 10000;
    const CONCURRENCY: usize = 8;

    fn fifo() -> Cache<u64, u64> {
        CacheBuilder::new(CAPACITY)
            .with_shards(SHARDS)
            .with_eviction_config(FifoConfig {})
            .with_object_pool_capacity(OBJECT_POOL_CAPACITY)
            .build()
    }

    fn lru() -> Cache<u64, u64> {
        CacheBuilder::new(CAPACITY)
            .with_shards(SHARDS)
            .with_eviction_config(LruConfig {
                high_priority_pool_ratio: 0.1,
            })
            .with_object_pool_capacity(OBJECT_POOL_CAPACITY)
            .build()
    }

    fn lfu() -> Cache<u64, u64> {
        CacheBuilder::new(CAPACITY)
            .with_shards(SHARDS)
            .with_eviction_config(LfuConfig {
                window_capacity_ratio: 0.1,
                protected_capacity_ratio: 0.8,
                cmsketch_eps: 0.001,
                cmsketch_confidence: 0.9,
            })
            .with_object_pool_capacity(OBJECT_POOL_CAPACITY)
            .build()
    }

    fn s3fifo() -> Cache<u64, u64> {
        CacheBuilder::new(CAPACITY)
            .with_shards(SHARDS)
            .with_eviction_config(S3FifoConfig {
                small_queue_capacity_ratio: 0.1,
            })
            .with_object_pool_capacity(OBJECT_POOL_CAPACITY)
            .build()
    }

    fn init_cache(cache: &Cache<u64, u64>, rng: &mut StdRng) {
        let mut v = RANGE.collect_vec();
        v.shuffle(rng);
        for i in v {
            cache.insert(i, i);
        }
    }

    async fn operate(cache: &Cache<u64, u64>, rng: &mut StdRng) {
        let i = rng.gen_range(RANGE);
        match rng.gen_range(0..=3) {
            0 => {
                let entry = cache.insert(i, i);
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
                        Ok::<_, tokio::sync::oneshot::error::RecvError>((i, CacheContext::Default))
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

    #[tokio::test]
    async fn test_s3fifo_cache() {
        case(s3fifo()).await
    }

    #[tokio::test]
    async fn test_cache_with_zero_object_pool() {
        case(CacheBuilder::new(8).with_object_pool_capacity(0).build()).await
    }
}
