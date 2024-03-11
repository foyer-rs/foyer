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

use std::{hash::BuildHasher, ops::Deref, sync::Arc};

use ahash::RandomState;
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

pub type FifoCache<K, V, L = DefaultCacheEventListener<K, V>, S = RandomState> =
    GenericCache<K, V, FifoHandle<K, V>, Fifo<K, V>, HashTableIndexer<K, FifoHandle<K, V>>, L, S>;
pub type FifoCacheConfig<K, V, L = DefaultCacheEventListener<K, V>, S = RandomState> = CacheConfig<Fifo<K, V>, L, S>;
pub type FifoCacheEntry<K, V, L = DefaultCacheEventListener<K, V>, S = RandomState> =
    GenericCacheEntry<K, V, FifoHandle<K, V>, Fifo<K, V>, HashTableIndexer<K, FifoHandle<K, V>>, L, S>;
pub type FifoEntry<K, V, ER, L = DefaultCacheEventListener<K, V>, S = RandomState> =
    GenericEntry<K, V, FifoHandle<K, V>, Fifo<K, V>, HashTableIndexer<K, FifoHandle<K, V>>, L, S, ER>;

pub type LruCache<K, V, L = DefaultCacheEventListener<K, V>, S = RandomState> =
    GenericCache<K, V, LruHandle<K, V>, Lru<K, V>, HashTableIndexer<K, LruHandle<K, V>>, L, S>;
pub type LruCacheConfig<K, V, L = DefaultCacheEventListener<K, V>, S = RandomState> = CacheConfig<Lru<K, V>, L, S>;
pub type LruCacheEntry<K, V, L = DefaultCacheEventListener<K, V>, S = RandomState> =
    GenericCacheEntry<K, V, LruHandle<K, V>, Lru<K, V>, HashTableIndexer<K, LruHandle<K, V>>, L, S>;
pub type LruEntry<K, V, ER, L = DefaultCacheEventListener<K, V>, S = RandomState> =
    GenericEntry<K, V, LruHandle<K, V>, Lru<K, V>, HashTableIndexer<K, LruHandle<K, V>>, L, S, ER>;

pub type LfuCache<K, V, L = DefaultCacheEventListener<K, V>, S = RandomState> =
    GenericCache<K, V, LfuHandle<K, V>, Lfu<K, V>, HashTableIndexer<K, LfuHandle<K, V>>, L, S>;
pub type LfuCacheConfig<K, V, L = DefaultCacheEventListener<K, V>, S = RandomState> = CacheConfig<Lfu<K, V>, L, S>;
pub type LfuCacheEntry<K, V, L = DefaultCacheEventListener<K, V>, S = RandomState> =
    GenericCacheEntry<K, V, LfuHandle<K, V>, Lfu<K, V>, HashTableIndexer<K, LfuHandle<K, V>>, L, S>;
pub type LfuEntry<K, V, ER, L = DefaultCacheEventListener<K, V>, S = RandomState> =
    GenericEntry<K, V, LfuHandle<K, V>, Lfu<K, V>, HashTableIndexer<K, LfuHandle<K, V>>, L, S, ER>;

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

pub enum Cache<K, V, L, S = RandomState>
where
    K: Key,
    V: Value,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
{
    Fifo(Arc<FifoCache<K, V, L, S>>),
    Lru(Arc<LruCache<K, V, L, S>>),
    Lfu(Arc<LfuCache<K, V, L, S>>),
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
    pub fn fifo(config: FifoCacheConfig<K, V, L, S>) -> Self {
        Self::Fifo(Arc::new(GenericCache::new(config)))
    }

    pub fn lru(config: LruCacheConfig<K, V, L, S>) -> Self {
        Self::Lru(Arc::new(GenericCache::new(config)))
    }

    pub fn lfu(config: LfuCacheConfig<K, V, L, S>) -> Self {
        Self::Lfu(Arc::new(GenericCache::new(config)))
    }

    pub fn insert(&self, key: K, value: V, charge: usize) -> CacheEntry<K, V, L, S> {
        match self {
            Cache::Fifo(cache) => cache.insert(key, value, charge).into(),
            Cache::Lru(cache) => cache.insert(key, value, charge).into(),
            Cache::Lfu(cache) => cache.insert(key, value, charge).into(),
        }
    }

    pub fn insert_with_context(
        &self,
        key: K,
        value: V,
        charge: usize,
        context: CacheContext,
    ) -> CacheEntry<K, V, L, S> {
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

    pub fn get(&self, key: &K) -> Option<CacheEntry<K, V, L, S>> {
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

pub enum Entry<K, V, ER, L, S>
where
    K: Key + Clone,
    V: Value,
    ER: std::error::Error,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
{
    Fifo(FifoEntry<K, V, ER, L, S>),
    Lru(LruEntry<K, V, ER, L, S>),
    Lfu(LfuEntry<K, V, ER, L, S>),
}

impl<K, V, ER, L, S> From<FifoEntry<K, V, ER, L, S>> for Entry<K, V, ER, L, S>
where
    K: Key + Clone,
    V: Value,
    ER: std::error::Error,
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
    ER: std::error::Error,
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
    ER: std::error::Error,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
{
    fn from(entry: LfuEntry<K, V, ER, L, S>) -> Self {
        Self::Lfu(entry)
    }
}

impl<K, V, ER, L, S> Future for Entry<K, V, ER, L, S>
where
    K: Key + Clone,
    V: Value,
    ER: std::error::Error + From<oneshot::error::RecvError>,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
{
    type Output = std::result::Result<CacheEntry<K, V, L, S>, ER>;

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

impl<K, V, ER, L, S> Entry<K, V, ER, L, S>
where
    K: Key + Clone,
    V: Value,
    ER: std::error::Error + From<oneshot::error::RecvError>,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
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
        FU: Future<Output = std::result::Result<(V, usize, CacheContext), ER>> + Send + 'static,
        ER: std::error::Error + Send + 'static,
    {
        match self {
            Cache::Fifo(cache) => Entry::from(cache.entry(key, f)),
            Cache::Lru(_) => todo!(),
            Cache::Lfu(_) => todo!(),
        }
    }
}
