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

#![no_main]

use std::sync::Arc;

use ahash::RandomState;
use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;

use foyer_memory::{
    Cache, DefaultCacheEventListener, FifoCacheConfig, FifoConfig, LfuCacheConfig, LfuConfig, LruCacheConfig,
    LruConfig, S3FifoCacheConfig, S3FifoConfig,
};

type CacheKey = u8;
type CacheValue = u8;
const SHARDS: usize = 1;
const OBJECT_POOL_CAPACITY: usize = 16;

fn new_fifo_cache(capacity: usize) -> Arc<Cache<CacheKey, CacheValue>> {
    let config = FifoCacheConfig {
        capacity,
        shards: SHARDS,
        eviction_config: FifoConfig {},
        object_pool_capacity: OBJECT_POOL_CAPACITY,
        hash_builder: RandomState::default(),
        event_listener: DefaultCacheEventListener::default(),
    };

    Arc::new(Cache::fifo(config))
}

fn new_lru_cache(capacity: usize) -> Arc<Cache<CacheKey, CacheValue>> {
    let config = LruCacheConfig {
        capacity,
        shards: SHARDS,
        eviction_config: LruConfig {
            high_priority_pool_ratio: 0.1,
        },
        object_pool_capacity: OBJECT_POOL_CAPACITY,
        hash_builder: RandomState::default(),
        event_listener: DefaultCacheEventListener::default(),
    };

    Arc::new(Cache::lru(config))
}

fn new_lfu_cache(capacity: usize) -> Arc<Cache<CacheKey, CacheValue>> {
    let config = LfuCacheConfig {
        capacity,
        shards: SHARDS,
        eviction_config: LfuConfig {
            window_capacity_ratio: 0.1,
            protected_capacity_ratio: 0.8,
            cmsketch_eps: 0.001,
            cmsketch_confidence: 0.9,
        },
        object_pool_capacity: OBJECT_POOL_CAPACITY,
        hash_builder: RandomState::default(),
        event_listener: DefaultCacheEventListener::default(),
    };

    Arc::new(Cache::lfu(config))
}

fn new_s3fifo_cache(capacity: usize) -> Arc<Cache<CacheKey, CacheValue>> {
    let config = S3FifoCacheConfig {
        capacity,
        shards: SHARDS,
        eviction_config: S3FifoConfig {
            small_queue_capacity_ratio: 0.1,
        },
        object_pool_capacity: OBJECT_POOL_CAPACITY,
        hash_builder: RandomState::default(),
        event_listener: DefaultCacheEventListener::default(),
    };

    Arc::new(Cache::s3fifo(config))
}

#[derive(Debug, Arbitrary)]
enum Op {
    Insert(CacheKey, CacheValue, usize),
    Get(CacheKey),
    Remove(CacheKey),
    Clear,
}

#[derive(Debug, Arbitrary)]
enum CacheType {
    Fifo,
    Lru,
    Lfu,
    S3Fifo,
}

#[derive(Debug, Arbitrary)]
struct Input {
    capacity: usize,
    cache_type: CacheType,
    operations: Vec<Op>,
}

fn create_cache(cache_type: CacheType, capacity: usize) -> Arc<Cache<CacheKey, CacheValue>> {
    match cache_type {
        CacheType::Fifo => new_fifo_cache(capacity),
        CacheType::Lru => new_lru_cache(capacity),
        CacheType::Lfu => new_lfu_cache(capacity),
        CacheType::S3Fifo => new_s3fifo_cache(capacity),
    }
}

fuzz_target!(|data: Input| {
    let cache = create_cache(data.cache_type, data.capacity);

    for op in data.operations {
        match op {
            Op::Insert(k, v, size) => {
                cache.insert(k, v, size);
            }
            Op::Get(k) => {
                let _ = cache.get(&k);
            }
            Op::Remove(k) => {
                cache.remove(&k);
            }
            Op::Clear => {
                cache.clear();
            }
        }
    }
});
