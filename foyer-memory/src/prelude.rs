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

use ahash::RandomState;

use crate::{
    cache::{Cache, CacheConfig, CacheEntry},
    eviction::{
        fifo::{Fifo, FifoHandle},
        lfu::{Lfu, LfuHandle},
        lru::{Lru, LruHandle},
    },
    indexer::HashTableIndexer,
    listener::DefaultCacheEventListener,
};

pub type FifoCache<K, V, L = DefaultCacheEventListener<K, V, FifoContext>, S = RandomState> =
    Cache<K, V, FifoHandle<K, V>, Fifo<K, V>, HashTableIndexer<K, FifoHandle<K, V>>, L, S>;
pub type FifoCacheConfig<K, V, L = DefaultCacheEventListener<K, V, FifoContext>, S = RandomState> =
    CacheConfig<Fifo<K, V>, L, S>;
pub type FifoCacheEntry<K, V, L = DefaultCacheEventListener<K, V, FifoContext>, S = RandomState> =
    CacheEntry<K, V, FifoHandle<K, V>, Fifo<K, V>, HashTableIndexer<K, FifoHandle<K, V>>, L, S>;

pub type LruCache<K, V, L = DefaultCacheEventListener<K, V, LruContext>, S = RandomState> =
    Cache<K, V, LruHandle<K, V>, Lru<K, V>, HashTableIndexer<K, LruHandle<K, V>>, L, S>;
pub type LruCacheConfig<K, V, L = DefaultCacheEventListener<K, V, LruContext>, S = RandomState> =
    CacheConfig<Lru<K, V>, L, S>;
pub type LruCacheEntry<K, V, L = DefaultCacheEventListener<K, V, LruContext>, S = RandomState> =
    CacheEntry<K, V, LruHandle<K, V>, Lru<K, V>, HashTableIndexer<K, LruHandle<K, V>>, L, S>;

pub type LfuCache<K, V, L = DefaultCacheEventListener<K, V, LfuContext>, S = RandomState> =
    Cache<K, V, LfuHandle<K, V>, Lfu<K, V>, HashTableIndexer<K, LfuHandle<K, V>>, L, S>;
pub type LfuCacheConfig<K, V, L = DefaultCacheEventListener<K, V, LfuContext>, S = RandomState> =
    CacheConfig<Lfu<K, V>, L, S>;
pub type LfuCacheEntry<K, V, L = DefaultCacheEventListener<K, V, LfuContext>, S = RandomState> =
    CacheEntry<K, V, LfuHandle<K, V>, Lfu<K, V>, HashTableIndexer<K, LfuHandle<K, V>>, L, S>;

pub use crate::{
    cache::Entry,
    eviction::{
        fifo::{FifoConfig, FifoContext},
        lfu::{LfuConfig, LfuContext},
        lru::{LruConfig, LruContext},
    },
    listener::CacheEventListener,
};
