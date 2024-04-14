#![no_main]

use std::sync::Arc;

use ahash::RandomState;
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

fn create_cache(op: u8) -> Arc<Cache<CacheKey, CacheValue>> {
    match op % 4 {
        0 => new_fifo_cache(20),
        1 => new_lru_cache(20),
        2 => new_lfu_cache(20),
        3 => new_s3fifo_cache(20),
        _ => unreachable!(),
    }
}

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }
    let cache = create_cache(data[0]);
    let mut it = data.iter();
    for &op in it.by_ref() {
        match op % 4 {
            0 => {
                cache.insert(op, op, 1);
            }
            1 => match cache.get(&op) {
                Some(v) => assert_eq!(*v, op),
                None => {}
            },
            2 => {
                cache.remove(&op);
            }
            3 => {
                // Low probability to clear the cache
                if op % 10 == 1 {
                    cache.clear();
                }
            }
            _ => unreachable!(),
        }
    }
});
