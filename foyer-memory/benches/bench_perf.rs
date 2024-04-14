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
    sync::Arc,
    time::{Duration, Instant},
};

use ahash::RandomState;
use foyer_memory::{
    Cache, DefaultCacheEventListener, FifoCacheConfig, FifoConfig, LfuCacheConfig, LfuConfig, LruCacheConfig,
    LruConfig, S3FifoCacheConfig, S3FifoConfig,
};
use rand::{distributions::Distribution, thread_rng};

type CacheKey = String;
type CacheValue = ();
const SHARDS: usize = 1;
const OBJECT_POOL_CAPACITY: usize = 16;

const ITEM_COUNT: usize = 10_000;
const ITERATIONS: usize = 1_000_000;

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

fn read_perf(cache: Arc<Cache<CacheKey, CacheValue>>, keys: Arc<Vec<CacheKey>>, thread_num: usize) -> Duration {
    let start = Instant::now();
    for _ in 0..thread_num {
        let cache = cache.clone();
        let keys = keys.clone();
        let thread = std::thread::spawn(move || {
            for key in keys.iter() {
                cache.get(key);
            }
        });
        thread.join().unwrap();
    }
    start.elapsed() / (keys.len() * thread_num) as u32
}

fn read_write_perf(cache: Arc<Cache<CacheKey, CacheValue>>, keys: Arc<Vec<CacheKey>>, thread_num: usize) -> Duration {
    let start = Instant::now();
    // Start a thread to read and write the cache
    let write_handle = {
        let cache = cache.clone();
        let keys = keys.clone();
        std::thread::spawn(move || {
            for key in keys.iter() {
                cache.insert(key.clone(), (), 0);
            }
        })
    };
    for _ in 0..thread_num {
        let cache = cache.clone();
        let keys = keys.clone();
        let thread = std::thread::spawn(move || {
            for key in keys.iter() {
                cache.get(key);
            }
        });
        thread.join().unwrap();
    }
    write_handle.join().unwrap();
    start.elapsed() / (keys.len() * (thread_num + 1)) as u32
}

/*
1 threads read, FIFO: 36ns, LRU: 39ns, LFU: 42ns, S3FIFO: 32ns
2 threads read, FIFO: 39ns, LRU: 38ns, LFU: 35ns, S3FIFO: 35ns
4 threads read, FIFO: 35ns, LRU: 36ns, LFU: 38ns, S3FIFO: 33ns
8 threads read, FIFO: 35ns, LRU: 36ns, LFU: 35ns, S3FIFO: 33ns
16 threads read, FIFO: 37ns, LRU: 36ns, LFU: 37ns, S3FIFO: 34ns
1 threads read, 1 thread write, FIFO: 228ns, LRU: 269ns, LFU: 348ns, S3FIFO: 242ns
2 threads read, 1 thread write, FIFO: 313ns, LRU: 419ns, LFU: 487ns, S3FIFO: 286ns
4 threads read, 1 thread write, FIFO: 265ns, LRU: 254ns, LFU: 312ns, S3FIFO: 217ns
8 threads read, 1 thread write, FIFO: 184ns, LRU: 193ns, LFU: 261ns, S3FIFO: 166ns
16 threads read, 1 thread write, FIFO: 133ns, LRU: 142ns, LFU: 196ns, S3FIFO: 140ns
*/
fn main() {
    // Single thread read
    let keys = Arc::new((0..ITEM_COUNT).map(|i| format!("key-{}", i)).collect::<Vec<_>>());

    let fifo = new_fifo_cache(ITEM_COUNT);
    let lru = new_lru_cache(ITEM_COUNT);
    let lfu = new_lfu_cache(ITEM_COUNT);
    let s3fifo = new_s3fifo_cache(ITEM_COUNT);

    // Insert keys into each cache
    for key in keys.iter() {
        fifo.insert(key.clone(), (), 0);
        lru.insert(key.clone(), (), 0);
        lfu.insert(key.clone(), (), 0);
    }

    let mut rng = thread_rng();
    let zipf = zipf::ZipfDistribution::new(ITEM_COUNT, 0.9).unwrap();

    let iter_keys = Arc::new(
        (0..ITERATIONS)
            .map(|_| zipf.sample(&mut rng).to_string())
            .collect::<Vec<_>>(),
    );

    for read_thread_num in [1, 2, 4, 8, 16].iter() {
        let fifo_dur = read_perf(fifo.clone(), iter_keys.clone(), *read_thread_num);
        let lru_dur = read_perf(lru.clone(), iter_keys.clone(), *read_thread_num);
        let lfu_dur = read_perf(lfu.clone(), iter_keys.clone(), *read_thread_num);
        let s3fifo_dur = read_perf(s3fifo.clone(), iter_keys.clone(), *read_thread_num);

        println!(
            "{} threads read, FIFO: {:?}, LRU: {:?}, LFU: {:?}, S3FIFO: {:?}",
            read_thread_num, fifo_dur, lru_dur, lfu_dur, s3fifo_dur
        );
    }

    for read_thread_num in [1, 2, 4, 8, 16].iter() {
        let fifo_dur = read_write_perf(fifo.clone(), iter_keys.clone(), *read_thread_num);
        let lru_dur = read_write_perf(lru.clone(), iter_keys.clone(), *read_thread_num);
        let lfu_dur = read_write_perf(lfu.clone(), iter_keys.clone(), *read_thread_num);
        let s3fifo_dur = read_write_perf(s3fifo.clone(), iter_keys.clone(), *read_thread_num);

        println!(
            "{} threads read, 1 thread write, FIFO: {:?}, LRU: {:?}, LFU: {:?}, S3FIFO: {:?}",
            read_thread_num, fifo_dur, lru_dur, lfu_dur, s3fifo_dur
        );
    }

    // 8 threads read
}
