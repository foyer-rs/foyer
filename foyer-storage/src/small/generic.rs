//  Copyright 2024 foyer Project Authors
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
    fmt::Debug,
    marker::PhantomData,
    ops::Range,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use foyer_common::code::{HashBuilder, StorageKey, StorageValue};
use foyer_memory::CacheEntry;
use futures::{future::join_all, Future};
use itertools::Itertools;

use super::flusher::Submission;
use crate::{
    device::{MonitoredDevice, RegionId},
    error::Result,
    small::{flusher::Flusher, set::SetId, set_manager::SetManager},
    storage::Storage,
    DeviceStats, Runtime, Statistics,
};

pub struct GenericSmallStorageConfig<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    pub set_size: usize,
    pub set_cache_capacity: usize,
    pub device: MonitoredDevice,
    pub regions: Range<RegionId>,
    pub flush: bool,
    pub flushers: usize,
    pub buffer_pool_size: usize,
    pub statistics: Arc<Statistics>,
    pub runtime: Runtime,
    pub marker: PhantomData<(K, V, S)>,
}

impl<K, V, S> Debug for GenericSmallStorageConfig<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenericSmallStorageConfig")
            .field("set_size", &self.set_size)
            .field("set_cache_capacity", &self.set_cache_capacity)
            .field("device", &self.device)
            .field("regions", &self.regions)
            .field("flush", &self.flush)
            .field("flushers", &self.flushers)
            .field("buffer_pool_size", &self.buffer_pool_size)
            .field("statistics", &self.statistics)
            .field("runtime", &self.runtime)
            .field("marker", &self.marker)
            .finish()
    }
}

struct GenericSmallStorageInner<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    flushers: Vec<Flusher<K, V, S>>,

    device: MonitoredDevice,
    set_manager: SetManager,

    active: AtomicBool,

    stats: Arc<Statistics>,
    runtime: Runtime,
}

pub struct GenericSmallStorage<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    inner: Arc<GenericSmallStorageInner<K, V, S>>,
}

impl<K, V, S> Debug for GenericSmallStorage<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenericSmallStorage").finish()
    }
}

impl<K, V, S> Clone for GenericSmallStorage<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<K, V, S> GenericSmallStorage<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    async fn open(config: GenericSmallStorageConfig<K, V, S>) -> Result<Self> {
        let stats = config.statistics.clone();
        let metrics = config.device.metrics().clone();

        let set_manager = SetManager::new(
            config.set_size,
            config.set_cache_capacity,
            config.device.clone(),
            config.regions.clone(),
            config.flush,
        );

        let flushers = (0..config.flushers)
            .map(|_| Flusher::open(&config, set_manager.clone(), stats.clone(), metrics.clone()))
            .collect_vec();

        let inner = GenericSmallStorageInner {
            flushers,
            device: config.device,
            set_manager,
            active: AtomicBool::new(true),
            stats,
            runtime: config.runtime,
        };
        let inner = Arc::new(inner);

        Ok(Self { inner })
    }

    fn wait(&self) -> impl Future<Output = ()> + Send + 'static {
        let wait_flushers = join_all(self.inner.flushers.iter().map(|flusher| flusher.wait()));
        async move {
            wait_flushers.await;
        }
    }

    async fn close(&self) -> Result<()> {
        self.inner.active.store(false, Ordering::Relaxed);
        self.wait().await;
        Ok(())
    }

    fn enqueue(&self, entry: CacheEntry<K, V, S>, estimated_size: usize) {
        if !self.inner.active.load(Ordering::Relaxed) {
            tracing::warn!("cannot enqueue new entry after closed");
            return;
        }

        // Entries with the same hash must be grouped in the batch.
        let id = entry.hash() as usize % self.inner.flushers.len();
        self.inner.flushers[id].submit(Submission::Insertion { entry, estimated_size });
    }

    fn load(&self, hash: u64) -> impl Future<Output = Result<Option<(K, V)>>> + Send + 'static {
        let set_manager = self.inner.set_manager.clone();
        let sid = hash % set_manager.sets() as SetId;
        let stats = self.inner.stats.clone();

        async move {
            stats
                .cache_read_bytes
                .fetch_add(set_manager.set_size(), Ordering::Relaxed);

            match set_manager.read(sid, hash).await? {
                Some(set) => {
                    let kv = set.get(hash)?;
                    Ok(kv)
                }
                None => Ok(None),
            }
        }
    }

    fn delete(&self, hash: u64) {
        if !self.inner.active.load(Ordering::Relaxed) {
            tracing::warn!("cannot enqueue new entry after closed");
            return;
        }

        // Entries with the same hash MUST be grouped in the same batch.
        let id = hash as usize % self.inner.flushers.len();
        self.inner.flushers[id].submit(Submission::Deletion { hash });
    }

    fn may_contains(&self, hash: u64) -> bool {
        let set_manager = self.inner.set_manager.clone();
        let sid = hash % set_manager.sets() as SetId;
        // FIXME: Anyway without blocking? Use atomic?
        self.inner
            .runtime
            .read()
            .block_on(async move { set_manager.contains(sid, hash).await })
    }

    fn stats(&self) -> Arc<DeviceStats> {
        self.inner.device.stat().clone()
    }
}

impl<K, V, S> Storage for GenericSmallStorage<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    type Key = K;
    type Value = V;
    type BuildHasher = S;
    type Config = GenericSmallStorageConfig<K, V, S>;

    async fn open(config: Self::Config) -> Result<Self> {
        Self::open(config).await
    }

    async fn close(&self) -> Result<()> {
        self.close().await?;
        Ok(())
    }

    fn enqueue(&self, entry: CacheEntry<Self::Key, Self::Value, Self::BuildHasher>, estimated_size: usize) {
        self.enqueue(entry, estimated_size);
    }

    fn load(&self, hash: u64) -> impl Future<Output = Result<Option<(Self::Key, Self::Value)>>> + Send + 'static {
        self.load(hash)
    }

    fn delete(&self, hash: u64) {
        self.delete(hash)
    }

    fn may_contains(&self, hash: u64) -> bool {
        self.may_contains(hash)
    }

    async fn destroy(&self) -> Result<()> {
        todo!()
    }

    fn stats(&self) -> Arc<DeviceStats> {
        self.stats()
    }

    fn wait(&self) -> impl Future<Output = ()> + Send + 'static {
        self.wait()
    }
}
