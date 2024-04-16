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

use std::sync::Arc;

use foyer_memory::{Cache, CacheBuilder, FifoConfig, LfuConfig, LruConfig, S3FifoConfig};
use rand::{distributions::Distribution, thread_rng};

type CacheKey = String;
type CacheValue = ();

const ITEMS: usize = 10_000;
const ITERATIONS: usize = 5_000_000;

const SHARDS: usize = 1;
const OBJECT_POOL_CAPACITY: usize = 16;
/*
inspired by pingora/tinyufo/benches/bench_hit_ratio.rs
cargo bench --bench bench_hit_ratio

zif_exp, cache_size             fifo            lru             lfu             s3fifo          moka
0.90, 0.005                     16.24%          19.22%          32.37%          32.39%          33.50%
0.90, 0.01                      22.55%          26.20%          38.54%          39.20%          37.92%
0.90, 0.05                      41.05%          45.56%          55.37%          56.63%          55.25%
0.90,  0.1                      51.06%          55.68%          63.82%          65.06%          64.20%
0.90, 0.25                      66.81%          71.17%          76.21%          77.26%          77.12%
1.00, 0.005                     26.62%          31.10%          44.16%          44.15%          45.62%
1.00, 0.01                      34.38%          39.17%          50.63%          51.29%          50.72%
1.00, 0.05                      54.04%          58.76%          66.79%          67.85%          66.89%
1.00,  0.1                      63.15%          67.60%          73.93%          74.92%          74.38%
1.00, 0.25                      76.18%          79.95%          83.63%          84.39%          84.38%
1.05, 0.005                     32.67%          37.71%          50.26%          50.21%          51.85%
1.05, 0.01                      40.97%          46.10%          56.74%          57.40%          57.09%
1.05, 0.05                      60.44%          65.03%          72.04%          73.02%          72.28%
1.05,  0.1                      68.93%          73.12%          78.49%          79.37%          79.00%
1.05, 0.25                      80.38%          83.73%          86.78%          87.42%          87.41%
1.10, 0.005                     39.02%          44.50%          56.26%          56.20%          57.90%
1.10, 0.01                      47.60%          52.93%          62.61%          63.24%          63.05%
1.10, 0.05                      66.59%          70.95%          76.92%          77.76%          77.27%
1.10,  0.1                      74.24%          78.07%          82.54%          83.28%          83.00%
1.10, 0.25                      84.18%          87.10%          89.57%          90.06%          90.08%
1.50, 0.005                     81.17%          85.27%          88.90%          89.10%          89.89%
1.50, 0.01                      86.91%          89.87%          92.25%          92.56%          92.79%
1.50, 0.05                      94.77%          96.04%          96.96%          97.10%          97.07%
1.50,  0.1                      96.65%          97.50%          98.06%          98.14%          98.15%
1.50, 0.25                      98.36%          98.81%          99.04%          99.06%          99.09%
*/
fn cache_hit(cache: Cache<CacheKey, CacheValue>, keys: Arc<Vec<CacheKey>>) -> f64 {
    let mut hit = 0;
    for key in keys.iter() {
        let value = cache.get(key);
        if value.is_some() {
            hit += 1;
        } else {
            cache.insert(key.clone(), (), 1);
        }
    }
    hit as f64 / ITERATIONS as f64
}

fn moka_cache_hit(cache: &moka::sync::Cache<CacheKey, CacheValue>, keys: &[String]) -> f64 {
    let mut hit = 0;
    for key in keys.iter() {
        let value = cache.get(key);
        if value.is_some() {
            hit += 1;
        } else {
            cache.insert(key.clone(), ());
        }
    }
    hit as f64 / ITERATIONS as f64
}

fn new_fifo_cache(capacity: usize) -> Cache<CacheKey, CacheValue> {
    CacheBuilder::new(capacity)
        .with_shards(SHARDS)
        .with_eviction_config(FifoConfig {}.into())
        .with_object_pool_capacity(OBJECT_POOL_CAPACITY)
        .build()
}

fn new_lru_cache(capacity: usize) -> Cache<CacheKey, CacheValue> {
    CacheBuilder::new(capacity)
        .with_shards(SHARDS)
        .with_eviction_config(
            LruConfig {
                high_priority_pool_ratio: 0.1,
            }
            .into(),
        )
        .with_object_pool_capacity(OBJECT_POOL_CAPACITY)
        .build()
}

fn new_lfu_cache(capacity: usize) -> Cache<CacheKey, CacheValue> {
    CacheBuilder::new(capacity)
        .with_shards(SHARDS)
        .with_eviction_config(
            LfuConfig {
                window_capacity_ratio: 0.1,
                protected_capacity_ratio: 0.8,
                cmsketch_eps: 0.001,
                cmsketch_confidence: 0.9,
            }
            .into(),
        )
        .with_object_pool_capacity(OBJECT_POOL_CAPACITY)
        .build()
}

fn new_s3fifo_cache(capacity: usize) -> Cache<CacheKey, CacheValue> {
    CacheBuilder::new(capacity)
        .with_shards(SHARDS)
        .with_eviction_config(
            S3FifoConfig {
                small_queue_capacity_ratio: 0.1,
            }
            .into(),
        )
        .with_object_pool_capacity(OBJECT_POOL_CAPACITY)
        .build()
}

fn bench_one(zif_exp: f64, cache_size_percent: f64) {
    print!("{zif_exp:.2}, {cache_size_percent:4}\t\t\t");
    let mut rng = thread_rng();
    let zipf = zipf::ZipfDistribution::new(ITEMS, zif_exp).unwrap();

    let cache_size = (ITEMS as f64 * cache_size_percent) as usize;

    let fifo_cache = new_fifo_cache(cache_size);
    let lru_cache = new_lru_cache(cache_size);
    let lfu_cache = new_lfu_cache(cache_size);
    let s3fifo_cache = new_s3fifo_cache(cache_size);
    let moka_cache = moka::sync::Cache::new(cache_size as u64);

    let mut keys = Vec::with_capacity(ITERATIONS);
    for _ in 0..ITERATIONS {
        let key = zipf.sample(&mut rng).to_string();
        keys.push(key.clone());
    }

    let keys = Arc::new(keys);

    // Use multiple threads to simulate concurrent read-through requests.
    let fifo_cache_hit_handle = std::thread::spawn({
        let cache = fifo_cache.clone();
        let keys = keys.clone();
        move || cache_hit(cache, keys)
    });

    let lru_cache_hit_handle = std::thread::spawn({
        let cache = lru_cache.clone();
        let keys = keys.clone();
        move || cache_hit(cache, keys)
    });

    let lfu_cache_hit_handle = std::thread::spawn({
        let cache = lfu_cache.clone();
        let keys = keys.clone();
        move || cache_hit(cache, keys)
    });

    let s3fifo_cache_hit_handle = std::thread::spawn({
        let cache = s3fifo_cache.clone();
        let keys = keys.clone();
        move || cache_hit(cache, keys)
    });

    let moka_cache_hit_handle = std::thread::spawn({
        let cache = moka_cache.clone();
        let keys = keys.clone();
        move || moka_cache_hit(&cache, &keys)
    });

    let fifo_hit_ratio = fifo_cache_hit_handle.join().unwrap();
    let lru_hit_ratio = lru_cache_hit_handle.join().unwrap();
    let lfu_hit_ratio = lfu_cache_hit_handle.join().unwrap();
    let s3fifo_hit_ratio = s3fifo_cache_hit_handle.join().unwrap();
    let moka_hit_ratio = moka_cache_hit_handle.join().unwrap();

    print!("{:.2}%\t\t", fifo_hit_ratio * 100.0);
    print!("{:.2}%\t\t", lru_hit_ratio * 100.0);
    print!("{:.2}%\t\t", lfu_hit_ratio * 100.0);
    print!("{:.2}%\t\t", s3fifo_hit_ratio * 100.0);
    println!("{:.2}%", moka_hit_ratio * 100.0);
}

fn bench_zipf_hit() {
    println!("zif_exp, cache_size\t\tfifo\t\tlru\t\tlfu\t\ts3fifo\t\tmoka");
    for zif_exp in [0.9, 1.0, 1.05, 1.1, 1.5] {
        for cache_capacity in [0.005, 0.01, 0.05, 0.1, 0.25] {
            bench_one(zif_exp, cache_capacity);
        }
    }
}

fn main() {
    bench_zipf_hit();
}
