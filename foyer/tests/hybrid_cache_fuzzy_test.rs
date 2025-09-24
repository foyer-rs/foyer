// Copyright 2025 foyer Project Authors
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

//! Fuzzy test for foyer hybrid cache.

use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
    time::Duration,
};

use foyer::{
    BlockEngineBuilder, DeviceBuilder, Event, EventListener, FsDeviceBuilder, HybridCache, HybridCacheBuilder,
    HybridCachePolicy, HybridCacheProperties, IoEngineBuilder, Location, PsyncIoEngineBuilder,
};
use rand::{rng, Rng};

const KB: usize = 1024;
const MB: usize = 1024 * KB;

const WRITERS: usize = 8;
const FETCHERS: usize = 16;
const READERS: usize = 8;

const WRITES: usize = 1000;
const FETCHES: usize = 1000;
const READS: usize = 1000;

const DUPLICATES: usize = 10;

const MISS_WAIT: Duration = Duration::from_millis(10);

const WRITE_WAIT: Duration = Duration::from_millis(1);
const FETCH_WAIT: Duration = Duration::from_millis(1);
const READ_WAIT: Duration = Duration::from_millis(1);

const INTERVAL: usize = 100;

#[derive(Debug)]
struct RecentEvictionQueue {
    queue: RwLock<VecDeque<u64>>,
    capacity: usize,
}

impl EventListener for RecentEvictionQueue {
    type Key = u64;
    type Value = Vec<u8>;

    fn on_leave(&self, _: Event, key: &Self::Key, _: &Self::Value) {
        let mut queue = self.queue.write().unwrap();
        if queue.len() < self.capacity {
            queue.push_back(*key)
        } else {
            queue.pop_front();
            queue.push_back(*key);
        }
    }
}

impl RecentEvictionQueue {
    fn new(capacity: usize) -> Self {
        Self {
            queue: RwLock::new(VecDeque::with_capacity(capacity)),
            capacity,
        }
    }

    fn pick(&self) -> Option<u64> {
        let queue = self.queue.read().unwrap();
        if queue.is_empty() {
            return None;
        }
        let index = rng().random_range(0..queue.len());
        Some(queue[index])
    }
}

#[test_log::test(tokio::test)]
async fn test_concurrent_insert_disk_cache_and_fetch() {
    let dir = tempfile::tempdir().unwrap();

    let recent = Arc::new(RecentEvictionQueue::new(10));

    let hybrid: HybridCache<u64, Vec<u8>> = HybridCacheBuilder::new()
        .with_name("test")
        .with_policy(HybridCachePolicy::WriteOnEviction)
        .with_event_listener(recent.clone())
        .memory(MB)
        .with_weighter(|_, v| 8 + v.len())
        .storage()
        .with_io_engine(PsyncIoEngineBuilder::new().build().await.unwrap())
        .with_engine_config(
            BlockEngineBuilder::new(FsDeviceBuilder::new(dir).with_capacity(64 * MB).build().unwrap())
                .with_block_size(4 * MB),
        )
        .build()
        .await
        .unwrap();

    let idx = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];
    for _ in 0..WRITERS {
        let h = hybrid.clone();
        let r = recent.clone();
        let i = idx.clone();
        handles.push(tokio::spawn(async move { write(h, r, i).await }));
    }
    for _ in 0..FETCHERS {
        let h = hybrid.clone();
        let r = recent.clone();
        handles.push(tokio::spawn(async move { fetch(h, r).await }));
    }
    for _ in 0..READERS {
        let h = hybrid.clone();
        let r = recent.clone();
        handles.push(tokio::spawn(async move { read(h, r).await }));
    }
    for h in handles {
        h.await.unwrap();
    }
}

fn value(key: u64) -> Vec<u8> {
    vec![key as u8; 4 * KB]
}

async fn write(hybrid: HybridCache<u64, Vec<u8>>, _: Arc<RecentEvictionQueue>, idx: Arc<AtomicU64>) {
    loop {
        let key = idx.fetch_add(1, Ordering::Relaxed);
        if key > WRITES as u64 {
            break;
        }
        if key % INTERVAL as u64 == 0 {
            tracing::info!("Inserted {key} items");
        }
        for k in key.saturating_sub(DUPLICATES as u64)..=key {
            hybrid.insert_with_properties(
                k,
                value(k),
                HybridCacheProperties::default().with_location(Location::OnDisk),
            );
            tokio::time::sleep(WRITE_WAIT).await;
        }
    }
}

async fn fetch(hybrid: HybridCache<u64, Vec<u8>>, recent: Arc<RecentEvictionQueue>) {
    let mut cnt = 0;
    loop {
        tokio::time::sleep(FETCH_WAIT).await;
        let key = match recent.pick() {
            Some(v) => v,
            None => continue,
        };
        let e = hybrid
            .fetch(key, || async move {
                tokio::time::sleep(MISS_WAIT).await;
                Ok(value(key))
            })
            .await
            .unwrap();

        assert_eq!(e.value(), &value(key));
        cnt += 1;
        if cnt % INTERVAL as u64 == 0 {
            tracing::info!("Fetch {cnt} items");
        }
        if cnt >= FETCHES as u64 {
            break;
        }
    }
}

async fn read(hybrid: HybridCache<u64, Vec<u8>>, recent: Arc<RecentEvictionQueue>) {
    let mut cnt = 0;
    loop {
        tokio::time::sleep(READ_WAIT).await;
        let key = match recent.pick() {
            Some(v) => v,
            None => continue,
        };
        let e = hybrid.get(&key).await.unwrap();

        if let Some(e) = e {
            assert_eq!(e.value(), &value(key));
        }
        cnt += 1;
        if cnt % INTERVAL as u64 == 0 {
            tracing::info!("Read {cnt} items");
        }
        if cnt >= READS as u64 {
            break;
        }
    }
}
