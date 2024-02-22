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

pub mod v2;

use std::{hash::Hasher, sync::Arc};

use foyer_common::code::{Key, Value};
use foyer_intrusive::{
    collections::hashmap::{HashMap, HashMapLink},
    core::adapter::Link,
    eviction::EvictionPolicy,
    intrusive_adapter, key_adapter, priority_adapter,
};
use parking_lot::Mutex;
use twox_hash::XxHash64;

pub struct CacheConfig<E>
where
    E: EvictionPolicy,
{
    capacity: usize,
    shard_bits: usize,
    hashmap_bits: usize,
    eviction_config: E::Config,
}

pub struct Cache<K, V, E, EL>
where
    K: Key,
    V: Value,
    E: EvictionPolicy<Adapter = CacheItemEpAdapter<K, V, EL>>,
    EL: Link,
{
    shards: Vec<Mutex<CacheShard<K, V, E, EL>>>,
}

struct CacheShard<K, V, E, EL>
where
    K: Key,
    V: Value,
    E: EvictionPolicy<Adapter = CacheItemEpAdapter<K, V, EL>>,
    EL: Link,
{
    container: HashMap<K, V, CacheItemHmAdapter<K, V, EL>>,
    eviction: E,

    capacity: usize,
    size: usize,
}

#[derive(Debug)]
pub struct CacheItem<K, V, EL>
where
    K: Key,
    V: Value,
    EL: Link,
{
    elink: EL,
    clink: HashMapLink,

    key: K,
    value: V,

    priority: usize,
}

impl<K, V, EL> CacheItem<K, V, EL>
where
    K: Key,
    V: Value,
    EL: Link,
{
    pub fn new(key: K, value: V) -> Self {
        Self {
            elink: EL::default(),
            clink: HashMapLink::default(),
            key,
            value,

            priority: 0,
        }
    }

    pub fn key(&self) -> &K {
        &self.key
    }

    pub fn value(&self) -> &V {
        &self.value
    }
}

intrusive_adapter! { pub CacheItemEpAdapter<K, V, EL> = Arc<CacheItem<K, V, EL>>: CacheItem<K, V, EL> { elink: EL } where K: Key, V: Value, EL: Link }
key_adapter! { CacheItemEpAdapter<K, V, EL> = CacheItem<K, V, EL> { key: K } where K: Key, V: Value, EL: Link }
priority_adapter! { CacheItemEpAdapter<K, V, EL> = CacheItem<K, V, EL> { priority: usize } where K: Key, V: Value, EL: Link }

intrusive_adapter! { pub CacheItemHmAdapter<K, V, EL> = Arc<CacheItem<K, V, EL>>: CacheItem<K, V, EL> { clink: HashMapLink } where K: Key, V: Value, EL: Link }
key_adapter! { CacheItemHmAdapter<K, V, EL> = CacheItem<K, V, EL> { key: K } where K: Key, V: Value, EL: Link }

impl<K, V, E, EL> Cache<K, V, E, EL>
where
    K: Key,
    V: Value,
    E: EvictionPolicy<Adapter = CacheItemEpAdapter<K, V, EL>>,
    EL: Link,
{
    pub fn new(config: CacheConfig<E>) -> Self {
        let mut shards = Vec::with_capacity(1 << config.shard_bits);

        let shard_capacity = config.capacity / (1 << config.shard_bits);

        for _ in 0..(1 << config.shard_bits) {
            let shard = CacheShard {
                container: HashMap::new(config.hashmap_bits),
                eviction: E::new(config.eviction_config.clone()),
                capacity: shard_capacity,
                size: 0,
            };
            shards.push(Mutex::new(shard));
        }

        Self { shards }
    }

    pub fn insert(&self, key: K, value: V) -> Vec<Arc<CacheItem<K, V, EL>>> {
        let hash = self.hash_key(&key);
        let slot = (self.shards.len() - 1) & hash as usize;
        let weight = key.weight() + value.weight();

        let item = Arc::new(CacheItem::new(key, value));

        let mut shard = self.shards[slot].lock();

        let to_evict = {
            let mut to_evict = vec![];
            let mut to_evict_size = 0;
            for item in shard.eviction.iter() {
                if shard.size + weight - to_evict_size <= shard.capacity {
                    break;
                }
                to_evict.push(item.clone());
                to_evict_size += item.key.weight() + item.value.weight();
            }
            to_evict
        };
        for item in to_evict.iter() {
            shard.eviction.remove(item);
            unsafe { shard.container.remove_in_place(item.clink.raw()) };
        }

        shard.container.insert(item.clone());
        shard.eviction.insert(item);
        shard.size += weight;

        to_evict
    }

    pub fn remove(&self, key: &K) -> Option<Arc<CacheItem<K, V, EL>>> {
        let hash = self.hash_key(key);
        let slot = (self.shards.len() - 1) & hash as usize;

        let mut shard = self.shards[slot].lock();

        let item = shard.container.remove(key);
        if let Some(item) = &item {
            shard.eviction.remove(item);
        }
        item
    }

    pub fn lookup(&self, key: &K) -> Option<Arc<CacheItem<K, V, EL>>> {
        let hash = self.hash_key(key);
        let slot = (self.shards.len() - 1) & hash as usize;

        let mut shard = self.shards[slot].lock();
        match shard.container.lookup(key) {
            Some(item) => {
                let item = unsafe {
                    Arc::increment_strong_count(item as *const _);
                    Arc::from_raw(item as *const _)
                };
                shard.eviction.access(&item);
                Some(item)
            }
            None => None,
        }
    }

    fn hash_key(&self, key: &K) -> u64 {
        let mut hasher = XxHash64::default();
        key.hash(&mut hasher);
        hasher.finish()
    }
}

#[cfg(test)]
mod tests {
    use foyer_intrusive::eviction::sfifo::{SegmentedFifo, SegmentedFifoConfig, SegmentedFifoLink};

    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
    struct K(usize);

    impl Key for K {
        fn weight(&self) -> usize {
            0
        }
    }

    #[derive(Debug, PartialEq, Eq, Clone)]
    struct V(usize);

    impl Value for V {
        fn weight(&self) -> usize {
            self.0
        }
    }

    type FifoCacheConfig = CacheConfig<SegmentedFifo<CacheItemEpAdapter<K, V, SegmentedFifoLink>>>;
    type FifoCache =
        Cache<K, V, SegmentedFifo<CacheItemEpAdapter<K, V, SegmentedFifoLink>>, SegmentedFifoLink>;

    #[test]
    fn test_fifo_cache_simple() {
        let config = FifoCacheConfig {
            capacity: 10,
            shard_bits: 0, // 1 shard
            hashmap_bits: 6,
            eviction_config: SegmentedFifoConfig {
                segment_ratios: vec![1],
            },
        };
        let cache = FifoCache::new(config);

        // cache: 1, 2, 3, 4
        for i in 1..=4 {
            let evicted = cache.insert(K(i), V(i));
            assert!(evicted.is_empty());
        }
        for i in 1..=4 {
            let item = cache.lookup(&K(i)).unwrap();
            assert_eq!(item.key(), &K(i));
            assert_eq!(item.value(), &V(i));
        }

        // cache: 4, 5
        // evicted: 1, 2, 3
        let evicted = cache.insert(K(5), V(5));
        assert_eq!(evicted.len(), 3);
        for (i, item) in evicted.into_iter().enumerate() {
            assert_eq!(Arc::strong_count(&item), 1);
            assert_eq!(item.key().0, i + 1);
            assert_eq!(item.value().0, i + 1);
        }

        // lookup: 5
        // cache: 4
        // removed: 5
        let res = cache.lookup(&K(5)).unwrap();
        let item = cache.remove(&K(5)).unwrap();
        assert_eq!(item.key(), &K(5));
        assert_eq!(item.value(), &V(5));
        assert_eq!(Arc::strong_count(&item), 2);
        drop(res);
        assert_eq!(Arc::strong_count(&item), 1);

        // cache: 10
        // evicted: 4
        let evicted = cache.insert(K(10), V(10));
        assert_eq!(evicted.len(), 1);
        assert_eq!(evicted[0].key(), &K(4));
        assert_eq!(evicted[0].value(), &V(4));
        assert_eq!(Arc::strong_count(&evicted[0]), 1);
    }
}
