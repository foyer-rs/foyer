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

use std::{
    fmt::Debug,
    future::Future,
    marker::PhantomData,
    ops::Range,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use foyer_common::{
    code::{StorageKey, StorageValue},
    metrics::Metrics,
    properties::Properties,
};
use foyer_memory::Piece;
use futures_util::future::join_all;
use itertools::Itertools;

use crate::{
    device::{MonitoredDevice, RegionId},
    error::Result,
    small::{
        flusher::{Flusher, Submission},
        set_manager::SetManager,
    },
    storage::Storage,
    Dev, Runtime, Statistics, Throttle,
};

pub struct GenericSmallStorageConfig<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    pub set_size: usize,
    pub set_cache_capacity: usize,
    pub set_cache_shards: usize,
    pub device: MonitoredDevice,
    pub regions: Range<RegionId>,
    pub flush: bool,
    pub flushers: usize,
    pub buffer_pool_size: usize,
    pub runtime: Runtime,
    pub marker: PhantomData<(K, V)>,
}

impl<K, V> Debug for GenericSmallStorageConfig<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenericSmallStorageConfig")
            .field("set_size", &self.set_size)
            .field("set_cache_capacity", &self.set_cache_capacity)
            .field("set_cache_shards", &self.set_cache_shards)
            .field("device", &self.device)
            .field("regions", &self.regions)
            .field("flush", &self.flush)
            .field("flushers", &self.flushers)
            .field("buffer_pool_size", &self.buffer_pool_size)
            .field("runtime", &self.runtime)
            .field("marker", &self.marker)
            .finish()
    }
}

struct GenericSmallStorageInner<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    flushers: Vec<Flusher<K, V, P>>,

    device: MonitoredDevice,
    set_manager: SetManager,

    active: AtomicBool,

    metrics: Arc<Metrics>,
    _runtime: Runtime,
}

pub struct GenericSmallStorage<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    inner: Arc<GenericSmallStorageInner<K, V, P>>,
}

impl<K, V, P> Debug for GenericSmallStorage<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenericSmallStorage").finish()
    }
}

impl<K, V, P> Clone for GenericSmallStorage<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<K, V, P> GenericSmallStorage<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    async fn open(config: GenericSmallStorageConfig<K, V>) -> Result<Self> {
        let metrics = config.device.metrics().clone();

        assert_eq!(
            config.regions.start, 0,
            "small object disk cache must start with region 0, current: {:?}",
            config.regions
        );

        let set_manager = SetManager::open(&config).await?;

        let flushers = (0..config.flushers)
            .map(|_| Flusher::open(&config, set_manager.clone(), metrics.clone()))
            .collect_vec();

        let inner = GenericSmallStorageInner {
            flushers,
            device: config.device,
            set_manager,
            active: AtomicBool::new(true),
            metrics,
            _runtime: config.runtime,
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

    fn enqueue(&self, piece: Piece<K, V, P>, estimated_size: usize) {
        if !self.inner.active.load(Ordering::Relaxed) {
            tracing::warn!("cannot enqueue new entry after closed");
            return;
        }

        // Entries with the same hash must be grouped in the batch.
        let id = piece.hash() as usize % self.inner.flushers.len();
        self.inner.flushers[id].submit(Submission::Insertion { piece, estimated_size });
    }

    fn load(&self, hash: u64) -> impl Future<Output = Result<Option<(K, V)>>> + Send + 'static {
        let set_manager = self.inner.set_manager.clone();
        let metrics = self.inner.metrics.clone();

        async move {
            set_manager.load(hash).await.inspect_err(|e| {
                tracing::error!(hash, ?e, "[sodc load]: fail to load");
                metrics.storage_error.increase(1);
            })
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

    async fn destroy(&self) -> Result<()> {
        // TODO(MrCroxx): reset bloom filters
        self.inner.set_manager.destroy().await
    }

    fn may_contains(&self, hash: u64) -> bool {
        self.inner.set_manager.may_contains(hash)
    }

    fn throttle(&self) -> &Throttle {
        self.inner.device.throttle()
    }

    fn statistics(&self) -> &Arc<Statistics> {
        self.inner.device.statistics()
    }
}

impl<K, V, P> Storage for GenericSmallStorage<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    type Key = K;
    type Value = V;
    type Properties = P;
    type Config = GenericSmallStorageConfig<K, V>;

    async fn open(config: Self::Config) -> Result<Self> {
        Self::open(config).await
    }

    async fn close(&self) -> Result<()> {
        self.close().await?;
        Ok(())
    }

    fn enqueue(&self, piece: Piece<Self::Key, Self::Value, Self::Properties>, estimated_size: usize) {
        self.enqueue(piece, estimated_size);
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
        self.destroy().await
    }

    fn throttle(&self) -> &Throttle {
        self.throttle()
    }

    fn statistics(&self) -> &Arc<Statistics> {
        self.statistics()
    }

    fn wait(&self) -> impl Future<Output = ()> + Send + 'static {
        self.wait()
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use bytesize::ByteSize;
    use foyer_common::{hasher::ModRandomState, metrics::Metrics};
    use foyer_memory::{Cache, CacheBuilder, CacheEntry, FifoConfig, TestProperties};
    use tokio::runtime::Handle;

    use super::*;
    use crate::{
        device::{
            monitor::{Monitored, MonitoredConfig},
            Dev,
        },
        serde::EntrySerializer,
        DevExt, DirectFsDeviceOptions,
    };

    fn cache_for_test() -> Cache<u64, Vec<u8>, ModRandomState, TestProperties> {
        CacheBuilder::new(10)
            .with_shards(1)
            .with_hash_builder(ModRandomState::default())
            .with_eviction_config(FifoConfig::default())
            .build()
    }

    async fn device_for_test(dir: impl AsRef<Path>) -> MonitoredDevice {
        let runtime = Runtime::current();
        Monitored::open(
            MonitoredConfig {
                config: DirectFsDeviceOptions::new(dir)
                    .with_capacity(ByteSize::kib(64).as_u64() as _)
                    .with_file_size(ByteSize::kib(16).as_u64() as _)
                    .into(),
                metrics: Arc::new(Metrics::noop()),
            },
            runtime,
        )
        .await
        .unwrap()
    }

    async fn store_for_test(dir: impl AsRef<Path>) -> GenericSmallStorage<u64, Vec<u8>, TestProperties> {
        let device = device_for_test(dir).await;
        let regions = 0..device.regions() as RegionId;
        let config = GenericSmallStorageConfig {
            set_size: ByteSize::kib(4).as_u64() as _,
            set_cache_capacity: 4,
            set_cache_shards: 1,
            device,
            regions,
            flush: false,
            flushers: 1,
            buffer_pool_size: ByteSize::kib(64).as_u64() as _,
            runtime: Runtime::new(None, None, Handle::current()),
            marker: PhantomData,
        };
        GenericSmallStorage::open(config).await.unwrap()
    }

    fn enqueue(store: &GenericSmallStorage<u64, Vec<u8>, TestProperties>, piece: Piece<u64, Vec<u8>, TestProperties>) {
        let estimated_size = EntrySerializer::estimated_size(piece.key(), piece.value());
        store.enqueue(piece, estimated_size);
    }

    async fn assert_some(
        store: &GenericSmallStorage<u64, Vec<u8>, TestProperties>,
        entry: &CacheEntry<u64, Vec<u8>, ModRandomState, TestProperties>,
    ) {
        assert_eq!(
            store.load(entry.hash()).await.unwrap().unwrap(),
            (*entry.key(), entry.value().clone())
        );
    }

    async fn assert_none(
        store: &GenericSmallStorage<u64, Vec<u8>, TestProperties>,
        entry: &CacheEntry<u64, Vec<u8>, ModRandomState, TestProperties>,
    ) {
        assert!(store.load(entry.hash()).await.unwrap().is_none());
    }

    #[test_log::test(tokio::test)]
    async fn test_store_enqueue_lookup_destroy_recovery() {
        let dir = tempfile::tempdir().unwrap();

        let memory = cache_for_test();
        let store = store_for_test(dir.path()).await;

        let e1 = memory.insert(1, vec![1; 42]);
        enqueue(&store, e1.piece());
        store.wait().await;

        assert_some(&store, &e1).await;

        store.delete(e1.hash());
        store.wait().await;

        assert_none(&store, &e1).await;

        let e2 = memory.insert(2, vec![2; 192]);
        let e3 = memory.insert(3, vec![3; 168]);

        enqueue(&store, e1.piece());
        enqueue(&store, e2.piece());
        enqueue(&store, e3.piece());
        store.wait().await;

        assert_some(&store, &e1).await;
        assert_some(&store, &e2).await;
        assert_some(&store, &e3).await;

        store.destroy().await.unwrap();

        assert_none(&store, &e1).await;
        assert_none(&store, &e2).await;
        assert_none(&store, &e3).await;
    }
}
