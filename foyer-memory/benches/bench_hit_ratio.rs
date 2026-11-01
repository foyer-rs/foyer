// Copyright 2026 foyer Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! micro benchmark for foyer in-memory cache hit ratio

use std::sync::Arc;

use csv::Reader;
use foyer_memory::{Cache, CacheBuilder, FifoConfig, LfuConfig, LruConfig, S3FifoConfig};
use rand::{distr::Distribution, rng};

type CacheKey = String;
type CacheValue = ();

const ITEMS: usize = 10_000;
const ITERATIONS: usize = 5_000_000;

const SHARDS: usize = 1;

/*
inspired by pingora/tinyufo/benches/bench_hit_ratio.rs
cargo bench --bench bench_hit_ratio

zif_exp, cache_size           fifo            lru             lfu             s3fifo (0g)     s3fifo (1g)     moka
  0.90,  0.005                16.18%          18.76%          32.58%          32.54%          31.85%          33.35%
  0.90,   0.01                22.47%          25.74%          38.64%          39.30%          38.39%          37.86%
  0.90,   0.05                41.03%          45.10%          55.45%          56.54%          55.32%          55.18%
  0.90,    0.1                50.97%          55.18%          63.75%          65.20%          63.53%          64.16%
  0.90,   0.25                66.83%          70.82%          76.24%          77.64%          75.73%          77.12%
  1.00,  0.005                26.46%          30.45%          44.30%          44.44%          43.45%          45.50%
  1.00,   0.01                34.28%          38.60%          50.73%          51.48%          50.50%          50.60%
  1.00,   0.05                53.96%          58.26%          66.77%          67.87%          66.76%          66.86%
  1.00,    0.1                63.05%          67.14%          73.87%          75.00%          73.74%          74.34%
  1.00,   0.25                76.15%          79.63%          83.63%          84.50%          83.28%          84.37%
  1.05,  0.005                32.56%          37.10%          50.46%          50.46%          49.55%          51.79%
  1.05,   0.01                40.82%          45.50%          56.82%          57.48%          56.59%          56.89%
  1.05,   0.05                60.35%          64.60%          72.03%          73.08%          72.07%          72.29%
  1.05,    0.1                68.85%          72.70%          78.45%          79.36%          78.35%          78.91%
  1.05,   0.25                80.33%          83.44%          86.77%          87.45%          86.48%          87.40%
  1.10,  0.005                38.87%          43.84%          56.42%          56.34%          55.55%          57.82%
  1.10,   0.01                47.51%          52.38%          62.70%          63.36%          62.50%          63.03%
  1.10,   0.05                66.50%          70.51%          76.88%          77.80%          76.92%          77.19%
  1.10,    0.1                74.22%          77.74%          82.54%          83.30%          82.47%          82.93%
  1.10,   0.25                84.14%          86.86%          89.54%          90.04%          89.30%          90.06%
  1.50,  0.005                80.98%          84.81%          88.91%          89.18%          88.66%          89.79%
  1.50,   0.01                86.80%          89.58%          92.20%          92.58%          92.23%          92.72%
  1.50,   0.05                94.72%          95.93%          96.92%          97.07%          96.93%          97.06%
  1.50,    0.1                96.64%          97.45%          98.05%          98.09%          98.04%          98.13%
  1.50,   0.25                98.34%          98.78%          99.03%          99.01%          99.01%          99.09%
*/
fn cache_hit(cache: Cache<CacheKey, CacheValue>, keys: Arc<Vec<CacheKey>>) -> f64 {
    let mut hit = 0;
    for key in keys.iter() {
        let value = cache.get(key);
        if value.is_some() {
            hit += 1;
        } else {
            cache.insert(key.clone(), ());
        }
    }
    (hit as f64) / (keys.len() as f64)
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
    hit as f64 / (keys.len() as f64)
}

fn new_fifo_cache(capacity: usize) -> Cache<CacheKey, CacheValue> {
    CacheBuilder::new(capacity)
        .with_shards(SHARDS)
        .with_eviction_config(FifoConfig {})
        .build()
}

fn new_lru_cache(capacity: usize) -> Cache<CacheKey, CacheValue> {
    CacheBuilder::new(capacity)
        .with_shards(SHARDS)
        .with_eviction_config(LruConfig {
            high_priority_pool_ratio: 0.1,
        })
        .build()
}

fn new_lfu_cache(capacity: usize) -> Cache<CacheKey, CacheValue> {
    CacheBuilder::new(capacity)
        .with_shards(SHARDS)
        .with_eviction_config(LfuConfig {
            window_capacity_ratio: 0.1,
            protected_capacity_ratio: 0.8,
            cmsketch_eps: 0.001,
            cmsketch_confidence: 0.9,
        })
        .build()
}

fn new_s3fifo_cache_wo_ghost(capacity: usize) -> Cache<CacheKey, CacheValue> {
    CacheBuilder::new(capacity)
        .with_shards(SHARDS)
        .with_eviction_config(S3FifoConfig {
            small_queue_capacity_ratio: 0.1,
            ghost_queue_capacity_ratio: 0.0,
            small_to_main_freq_threshold: 2,
        })
        .build()
}

fn new_s3fifo_cache_w_ghost(capacity: usize) -> Cache<CacheKey, CacheValue> {
    CacheBuilder::new(capacity)
        .with_shards(SHARDS)
        .with_eviction_config(S3FifoConfig {
            small_queue_capacity_ratio: 0.1,
            ghost_queue_capacity_ratio: 1.0,
            small_to_main_freq_threshold: 2,
        })
        .build()
}

fn bench_workload(keys: Vec<String>, cache_size: usize) {
    let fifo_cache = new_fifo_cache(cache_size);
    let lru_cache = new_lru_cache(cache_size);
    let lfu_cache = new_lfu_cache(cache_size);
    let s3fifo_cache_wo_ghost = new_s3fifo_cache_wo_ghost(cache_size);
    let s3fifo_cache_w_ghost = new_s3fifo_cache_w_ghost(cache_size);
    let moka_cache = moka::sync::Cache::new(cache_size as u64);

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

    let s3fifo_cache_wo_ghost_hit_handle = std::thread::spawn({
        let cache = s3fifo_cache_wo_ghost.clone();
        let keys = keys.clone();
        move || cache_hit(cache, keys)
    });

    let s3fifo_cache_w_ghost_hit_handle = std::thread::spawn({
        let cache = s3fifo_cache_w_ghost.clone();
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
    let s3fifo_wo_ghost_hit_ratio = s3fifo_cache_wo_ghost_hit_handle.join().unwrap();
    let s3fifo_w_ghost_hit_ratio = s3fifo_cache_w_ghost_hit_handle.join().unwrap();
    let moka_hit_ratio = moka_cache_hit_handle.join().unwrap();

    print!("{:15.2}%", fifo_hit_ratio * 100.0);
    print!("{:15.2}%", lru_hit_ratio * 100.0);
    print!("{:15.2}%", lfu_hit_ratio * 100.0);
    print!("{:15.2}%", s3fifo_wo_ghost_hit_ratio * 100.0);
    print!("{:15.2}%", s3fifo_w_ghost_hit_ratio * 100.0);
    print!("{:15.2}%", moka_hit_ratio * 100.0);
    println!();
}

fn bench_one(zif_exp: f64, cache_size_percent: f64) {
    print!("{zif_exp:6.2}, {cache_size_percent:6}{:6}", "");

    let items = ITEMS as f64;

    let mut rng = rng();
    let zipf = rand_distr::Zipf::new(items, zif_exp).unwrap();

    let cache_size = (items * cache_size_percent) as usize;
    let mut keys = Vec::with_capacity(ITERATIONS);
    for _ in 0..ITERATIONS {
        let key = zipf.sample(&mut rng).round() as usize;
        let key = key.to_string();
        keys.push(key.clone());
    }
    bench_workload(keys, cache_size);
}

fn bench_zipf_hit() {
    println!(
        "{:30}{:16}{:16}{:16}{:16}{:16}{:16}",
        "zif_exp, cache_size", "fifo", "lru", "lfu", "s3fifo (0g)", "s3fifo (1g)", "moka"
    );
    for zif_exp in [0.9, 1.0, 1.05, 1.1, 1.5] {
        for cache_capacity in [0.005, 0.01, 0.05, 0.1, 0.25] {
            bench_one(zif_exp, cache_capacity);
        }
    }
}

fn read_twitter_trace(path: &str, limit: usize) -> Vec<String> {
    let file = std::fs::File::open(path).unwrap();
    let mut reader = Reader::from_reader(file);
    let mut keys = Vec::new();
    for result in reader.records() {
        let record = result.unwrap();
        let key = record.get(1).unwrap().to_string();
        keys.push(key);
        if keys.len() >= limit {
            break;
        }
    }
    keys
}

/*
cache_size                  fifo            lru             lfu             s3fifo (0g)     s3fifo (1g)     moka
50000                     67.50%          70.51%          74.99%          70.88%          72.33%          64.70%
zif_exp, cache_size           fifo            lru             lfu             s3fifo (0g)     s3fifo (1g)     moka
  0.90,  0.005                16.24%          19.20%          32.38%          32.06%          31.94%          33.44%
  0.90,   0.01                22.55%          26.21%          38.56%          39.27%          38.46%          37.86%
  0.90,   0.05                41.10%          45.61%          55.41%          56.64%          55.37%          55.19%
  0.90,    0.1                51.05%          55.69%          63.81%          65.27%          63.61%          64.16%
  0.90,   0.25                66.76%          71.15%          76.17%          77.53%          75.68%          77.11%
  1.00,  0.005                26.59%          31.05%          44.11%          44.37%          43.54%          45.54%
  1.00,   0.01                34.36%          39.13%          50.64%          51.40%          50.59%          50.69%
  1.00,   0.05                54.03%          58.75%          66.80%          67.94%          66.81%          66.91%
  1.00,    0.1                63.16%          67.62%          73.93%          75.01%          73.83%          74.38%
  1.00,   0.25                76.17%          79.93%          83.61%          84.53%          83.27%          84.36%
  1.05,  0.005                32.64%          37.67%          50.23%          50.31%          49.63%          51.83%
  1.05,   0.01                40.90%          46.02%          56.71%          57.59%          56.67%          56.99%
  1.05,   0.05                60.44%          65.03%          72.05%          73.02%          72.09%          72.31%
  1.05,    0.1                68.90%          73.10%          78.50%          79.36%          78.39%          78.98%
  1.05,   0.25                80.34%          83.71%          86.77%          87.47%          86.49%          87.40%
  1.10,  0.005                38.98%          44.45%          56.25%          56.62%          55.62%          57.89%
  1.10,   0.01                47.66%          52.98%          62.65%          63.55%          62.62%          63.22%
  1.10,   0.05                66.55%          70.91%          76.90%          77.77%          76.96%          77.22%
  1.10,    0.1                74.27%          78.07%          82.56%          83.38%          82.51%          82.97%
  1.10,   0.25                84.19%          87.10%          89.56%          90.08%          89.34%          90.08%
  1.50,  0.005                81.20%          85.30%          88.90%          89.32%          88.79%          89.92%
  1.50,   0.01                86.91%          89.87%          92.25%          92.66%          92.29%          92.76%
  1.50,   0.05                94.77%          96.05%          96.96%          97.08%          96.96%          97.10%
  1.50,    0.1                96.64%          97.50%          98.05%          98.10%          98.04%          98.12%
  1.50,   0.25                98.37%          98.82%          99.05%          99.03%          99.03%          99.10%
*/
fn main() {
    // Try to read the csv file path by environment variable.
    let path = std::env::var("TWITTER_TRACE_PATH").ok();
    if let Some(path) = path {
        // Limit the number of keys to read.
        // MAX means read all keys, which may take a really long time.
        let limit = usize::MAX;
        let capacity = 50_000;
        let keys = read_twitter_trace(&path, limit);
        println!(
            "{:30}{:16}{:16}{:16}{:16}{:16}{:16}",
            "cache_size", "fifo", "lru", "lfu", "s3fifo (0g)", "s3fifo (1g)", "moka"
        );
        print!("{capacity:10}");
        print!("{:9}", " ");
        bench_workload(keys, capacity);
    }
    bench_zipf_hit();
}
