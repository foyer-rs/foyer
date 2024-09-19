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
    fmt::Debug,
    future::Future,
    marker::PhantomData,
    ops::Range,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Instant,
};

use fastrace::prelude::*;
use foyer_common::{
    bits,
    code::{HashBuilder, StorageKey, StorageValue},
    metrics::Metrics,
};
use foyer_memory::CacheEntry;
use futures::future::{join_all, try_join_all};
use tokio::{runtime::Handle, sync::Semaphore};

use super::{
    batch::InvalidStats,
    flusher::{Flusher, Submission},
    indexer::Indexer,
    reclaimer::Reclaimer,
    recover::{RecoverMode, RecoverRunner},
};
use crate::{
    compress::Compression,
    device::{monitor::DeviceStats, Dev, DevExt, MonitoredDevice, RegionId},
    error::{Error, Result},
    large::{
        reclaimer::RegionCleaner,
        serde::{AtomicSequence, EntryHeader},
        tombstone::{Tombstone, TombstoneLog, TombstoneLogConfig},
    },
    picker::{EvictionPicker, ReinsertionPicker},
    region::RegionManager,
    serde::EntryDeserializer,
    statistics::Statistics,
    storage::Storage,
};

pub struct GenericLargeStorageConfig<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    pub name: String,
    pub device: MonitoredDevice,
    pub regions: Range<RegionId>,
    pub compression: Compression,
    pub flush: bool,
    pub indexer_shards: usize,
    pub recover_mode: RecoverMode,
    pub recover_concurrency: usize,
    pub flushers: usize,
    pub reclaimers: usize,
    pub buffer_threshold: usize,
    pub clean_region_threshold: usize,
    pub eviction_pickers: Vec<Box<dyn EvictionPicker>>,
    pub reinsertion_picker: Arc<dyn ReinsertionPicker<Key = K>>,
    pub tombstone_log_config: Option<TombstoneLogConfig>,
    pub statistics: Arc<Statistics>,
    pub read_runtime_handle: Handle,
    pub write_runtime_handle: Handle,
    pub user_runtime_handle: Handle,
    pub marker: PhantomData<(V, S)>,
}

impl<K, V, S> Debug for GenericLargeStorageConfig<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenericStoreConfig")
            .field("name", &self.name)
            .field("device", &self.device)
            .field("compression", &self.compression)
            .field("flush", &self.flush)
            .field("indexer_shards", &self.indexer_shards)
            .field("recover_mode", &self.recover_mode)
            .field("recover_concurrency", &self.recover_concurrency)
            .field("flushers", &self.flushers)
            .field("reclaimers", &self.reclaimers)
            .field("buffer_threshold", &self.buffer_threshold)
            .field("clean_region_threshold", &self.clean_region_threshold)
            .field("eviction_pickers", &self.eviction_pickers)
            .field("reinsertion_pickers", &self.reinsertion_picker)
            .field("tombstone_log_config", &self.tombstone_log_config)
            .field("statistics", &self.statistics)
            .field("read_runtime_handle", &self.read_runtime_handle)
            .field("write_runtime_handle", &self.write_runtime_handle)
            .finish()
    }
}

pub struct GenericLargeStorage<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    inner: Arc<GenericStoreInner<K, V, S>>,
}

impl<K, V, S> Debug for GenericLargeStorage<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenericStore").finish()
    }
}

struct GenericStoreInner<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    indexer: Indexer,
    device: MonitoredDevice,
    region_manager: RegionManager,

    flushers: Vec<Flusher<K, V, S>>,
    reclaimers: Vec<Reclaimer>,

    statistics: Arc<Statistics>,

    flush: bool,

    sequence: AtomicSequence,

    _read_runtime_handle: Handle,
    write_runtime_handle: Handle,
    _user_runtime_handle: Handle,

    active: AtomicBool,

    metrics: Arc<Metrics>,
}

impl<K, V, S> Clone for GenericLargeStorage<K, V, S>
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

impl<K, V, S> GenericLargeStorage<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    async fn open(mut config: GenericLargeStorageConfig<K, V, S>) -> Result<Self> {
        let stats = config.statistics.clone();

        let device = config.device.clone();
        let metrics = device.metrics().clone();

        let mut tombstones = vec![];
        let tombstone_log = match &config.tombstone_log_config {
            None => None,
            Some(config) => {
                let log = TombstoneLog::open(
                    &config.path,
                    device.clone(),
                    config.flush,
                    &mut tombstones,
                    metrics.clone(),
                )
                .await?;
                Some(log)
            }
        };

        let indexer = Indexer::new(config.indexer_shards);
        let mut eviction_pickers = std::mem::take(&mut config.eviction_pickers);
        for picker in eviction_pickers.iter_mut() {
            picker.init(device.regions(), device.region_size());
        }
        let reclaim_semaphore = Arc::new(Semaphore::new(0));
        let region_manager = RegionManager::new(
            device.clone(),
            eviction_pickers,
            reclaim_semaphore.clone(),
            metrics.clone(),
        );
        let sequence = AtomicSequence::default();

        RecoverRunner::run(
            &config,
            config.regions.clone(),
            &sequence,
            &indexer,
            &region_manager,
            &tombstones,
            metrics.clone(),
            config.user_runtime_handle.clone(),
        )
        .await?;

        let flushers = try_join_all((0..config.flushers).map(|_| async {
            Flusher::open(
                &config,
                indexer.clone(),
                region_manager.clone(),
                device.clone(),
                tombstone_log.clone(),
                stats.clone(),
                metrics.clone(),
                config.write_runtime_handle.clone(),
            )
            .await
        }))
        .await?;

        let reclaimers = join_all((0..config.reclaimers).map(|_| async {
            Reclaimer::open(
                region_manager.clone(),
                reclaim_semaphore.clone(),
                config.reinsertion_picker.clone(),
                indexer.clone(),
                flushers.clone(),
                stats.clone(),
                config.flush,
                metrics.clone(),
                config.write_runtime_handle.clone(),
            )
            .await
        }))
        .await;

        Ok(Self {
            inner: Arc::new(GenericStoreInner {
                indexer,
                device,
                region_manager,
                flushers,
                reclaimers,
                statistics: stats,
                flush: config.flush,
                sequence,
                _read_runtime_handle: config.read_runtime_handle,
                write_runtime_handle: config.write_runtime_handle,
                _user_runtime_handle: config.user_runtime_handle,
                active: AtomicBool::new(true),
                metrics,
            }),
        })
    }

    fn wait(&self) -> impl Future<Output = ()> + Send + 'static {
        let wait_flushers = join_all(self.inner.flushers.iter().map(|flusher| flusher.wait()));
        let wait_reclaimers = join_all(self.inner.reclaimers.iter().map(|reclaimer| reclaimer.wait()));
        async move {
            wait_flushers.await;
            wait_reclaimers.await;
        }
    }

    async fn close(&self) -> Result<()> {
        self.inner.active.store(false, Ordering::Relaxed);
        self.wait().await;
        Ok(())
    }

    #[fastrace::trace(name = "foyer::storage::large::generic::enqueue")]
    fn enqueue(&self, entry: CacheEntry<K, V, S>, estimated_size: usize) {
        if !self.inner.active.load(Ordering::Relaxed) {
            tracing::warn!("cannot enqueue new entry after closed");
            return;
        }

        let sequence = self.inner.sequence.fetch_add(1, Ordering::Relaxed);
        self.inner.flushers[sequence as usize % self.inner.flushers.len()].submit(Submission::CacheEntry {
            entry,
            estimated_size,
            sequence,
        });
    }

    fn load(&self, hash: u64) -> impl Future<Output = Result<Option<(K, V)>>> + Send + 'static {
        let now = Instant::now();

        let device = self.inner.device.clone();
        let indexer = self.inner.indexer.clone();
        let stats = self.inner.statistics.clone();
        let metrics = self.inner.metrics.clone();

        async move {
            let addr = match indexer.get(hash) {
                Some(addr) => addr,
                None => {
                    metrics.storage_miss.increment(1);
                    metrics.storage_miss_duration.record(now.elapsed());
                    return Ok(None);
                }
            };

            tracing::trace!("{addr:#?}");

            let buffer = device.read(addr.region, addr.offset as _, addr.len as _).await?;

            stats
                .cache_read_bytes
                .fetch_add(bits::align_up(device.align(), buffer.len()), Ordering::Relaxed);

            let header = match EntryHeader::read(&buffer[..EntryHeader::serialized_len()]) {
                Ok(header) => header,
                Err(e @ Error::MagicMismatch { .. })
                | Err(e @ Error::ChecksumMismatch { .. })
                | Err(e @ Error::CompressionAlgorithmNotSupported(_)) => {
                    tracing::trace!("deserialize entry header error: {e}, remove this entry and skip");
                    indexer.remove(hash);
                    metrics.storage_miss.increment(1);
                    metrics.storage_miss_duration.record(now.elapsed());
                    return Ok(None);
                }
                Err(e) => return Err(e),
            };

            let (k, v) = match EntryDeserializer::deserialize::<K, V>(
                &buffer[EntryHeader::serialized_len()..],
                header.key_len as _,
                header.value_len as _,
                header.compression,
                Some(header.checksum),
                &metrics,
            ) {
                Ok(res) => res,
                Err(e @ Error::MagicMismatch { .. }) | Err(e @ Error::ChecksumMismatch { .. }) => {
                    tracing::trace!("deserialize read buffer raise error: {e}, remove this entry and skip");
                    indexer.remove(hash);
                    metrics.storage_miss.increment(1);
                    metrics.storage_miss_duration.record(now.elapsed());
                    return Ok(None);
                }
                Err(e) => return Err(e),
            };

            metrics.storage_hit.increment(1);
            metrics.storage_hit_duration.record(now.elapsed());

            Ok(Some((k, v)))
        }
        .in_span(Span::enter_with_local_parent("foyer::storage::large::generic::load"))
    }

    fn delete(&self, hash: u64) {
        let now = Instant::now();

        if !self.inner.active.load(Ordering::Relaxed) {
            tracing::warn!("cannot delete entry after closed");
            return;
        }

        let stats = self.inner.indexer.remove(hash).map(|addr| InvalidStats {
            region: addr.region,
            size: bits::align_up(self.inner.device.align(), addr.len as usize),
        });

        let this = self.clone();
        self.inner.write_runtime_handle.spawn(async move {
            let sequence = this.inner.sequence.fetch_add(1, Ordering::Relaxed);
            this.inner.flushers[sequence as usize % this.inner.flushers.len()].submit(Submission::Tombstone {
                tombstone: Tombstone { hash, sequence },
                stats,
            });
        });

        self.inner.metrics.storage_delete.increment(1);
        self.inner.metrics.storage_miss_duration.record(now.elapsed());
    }

    fn may_contains(&self, hash: u64) -> bool {
        self.inner.indexer.get(hash).is_some()
    }

    async fn destroy(&self) -> Result<()> {
        if !self.inner.active.load(Ordering::Relaxed) {
            return Err(anyhow::anyhow!("cannot delete entry after closed").into());
        }

        // Write an tombstone to clear tombstone log by increase the max sequence.
        let sequence = self.inner.sequence.fetch_add(1, Ordering::Relaxed);

        self.inner.flushers[sequence as usize % self.inner.flushers.len()].submit(Submission::Tombstone {
            tombstone: Tombstone { hash: 0, sequence },
            stats: None,
        });
        self.wait().await;

        // Clear indices.
        //
        // This step must perform after the latest writer finished,
        // otherwise the indices of the latest batch cannot be cleared.
        self.inner.indexer.clear();

        // Clean regions.
        try_join_all((0..self.inner.region_manager.regions() as RegionId).map(|id| {
            let region = self.inner.region_manager.region(id).clone();
            async move {
                let res = RegionCleaner::clean(&region, self.inner.flush).await;
                region.stats().reset();
                res
            }
        }))
        .await?;

        Ok(())
    }
}

impl<K, V, S> Storage for GenericLargeStorage<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    type Key = K;
    type Value = V;
    type BuildHasher = S;
    type Config = GenericLargeStorageConfig<K, V, S>;

    async fn open(config: Self::Config) -> Result<Self> {
        Self::open(config).await
    }

    async fn close(&self) -> Result<()> {
        self.close().await
    }

    fn enqueue(&self, entry: CacheEntry<Self::Key, Self::Value, Self::BuildHasher>, estimated_size: usize) {
        self.enqueue(entry, estimated_size)
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

    fn stats(&self) -> Arc<DeviceStats> {
        self.inner.device.stat().clone()
    }

    fn wait(&self) -> impl Future<Output = ()> + Send + 'static {
        self.wait()
    }
}

#[cfg(test)]
mod tests {

    use std::{fs::File, path::Path};

    use ahash::RandomState;
    use foyer_memory::{Cache, CacheBuilder, FifoConfig};
    use itertools::Itertools;

    use super::*;
    use crate::{
        device::{
            direct_fs::DirectFsDeviceOptions,
            monitor::{Monitored, MonitoredOptions},
        },
        picker::utils::{FifoPicker, RejectAllPicker},
        serde::EntrySerializer,
        test_utils::BiasedPicker,
        TombstoneLogConfigBuilder,
    };

    const KB: usize = 1024;

    fn cache_for_test() -> Cache<u64, Vec<u8>> {
        CacheBuilder::new(10)
            .with_eviction_config(FifoConfig::default())
            .build()
    }

    async fn device_for_test(dir: impl AsRef<Path>) -> MonitoredDevice {
        Monitored::open(MonitoredOptions {
            options: DirectFsDeviceOptions {
                dir: dir.as_ref().into(),
                capacity: 64 * KB,
                file_size: 16 * KB,
            }
            .into(),
            metrics: Arc::new(Metrics::new("test")),
        })
        .await
        .unwrap()
    }

    /// 4 files, fifo eviction, 16 KiB region, 64 KiB capacity.
    async fn store_for_test(dir: impl AsRef<Path>) -> GenericLargeStorage<u64, Vec<u8>, RandomState> {
        store_for_test_with_reinsertion_picker(dir, Arc::<RejectAllPicker<u64>>::default()).await
    }

    async fn store_for_test_with_reinsertion_picker(
        dir: impl AsRef<Path>,
        reinsertion_picker: Arc<dyn ReinsertionPicker<Key = u64>>,
    ) -> GenericLargeStorage<u64, Vec<u8>, RandomState> {
        let device = device_for_test(dir).await;
        let regions = 0..device.regions() as RegionId;
        let config = GenericLargeStorageConfig {
            name: "test".to_string(),
            device,
            regions,
            compression: Compression::None,
            flush: true,
            indexer_shards: 4,
            recover_mode: RecoverMode::Strict,
            recover_concurrency: 2,
            flushers: 1,
            reclaimers: 1,
            clean_region_threshold: 1,
            eviction_pickers: vec![Box::<FifoPicker>::default()],
            reinsertion_picker,
            tombstone_log_config: None,
            buffer_threshold: 16 * 1024 * 1024,
            statistics: Arc::<Statistics>::default(),
            read_runtime_handle: Handle::current(),
            write_runtime_handle: Handle::current(),
            user_runtime_handle: Handle::current(),
            marker: PhantomData,
        };
        GenericLargeStorage::open(config).await.unwrap()
    }

    async fn store_for_test_with_tombstone_log(
        dir: impl AsRef<Path>,
        path: impl AsRef<Path>,
    ) -> GenericLargeStorage<u64, Vec<u8>, RandomState> {
        let device = device_for_test(dir).await;
        let regions = 0..device.regions() as RegionId;
        let config = GenericLargeStorageConfig {
            name: "test".to_string(),
            device,
            regions,
            compression: Compression::None,
            flush: true,
            indexer_shards: 4,
            recover_mode: RecoverMode::Strict,
            recover_concurrency: 2,
            flushers: 1,
            reclaimers: 1,
            clean_region_threshold: 1,
            eviction_pickers: vec![Box::<FifoPicker>::default()],
            reinsertion_picker: Arc::<RejectAllPicker<u64>>::default(),
            tombstone_log_config: Some(TombstoneLogConfigBuilder::new(path).with_flush(true).build()),
            buffer_threshold: 16 * 1024 * 1024,
            statistics: Arc::<Statistics>::default(),
            read_runtime_handle: Handle::current(),
            write_runtime_handle: Handle::current(),
            user_runtime_handle: Handle::current(),
            marker: PhantomData,
        };
        GenericLargeStorage::open(config).await.unwrap()
    }

    fn enqueue(store: &GenericLargeStorage<u64, Vec<u8>, RandomState>, entry: CacheEntry<u64, Vec<u8>, RandomState>) {
        let estimated_size = EntrySerializer::estimated_size(entry.key(), entry.value());
        store.enqueue(entry, estimated_size);
    }

    #[test_log::test(tokio::test)]
    async fn test_store_enqueue_lookup_recovery() {
        let dir = tempfile::tempdir().unwrap();

        let memory = cache_for_test();
        let store = store_for_test(dir.path()).await;

        // [ [e1, e2], [], [], [] ]
        let e1 = memory.insert(1, vec![1; 7 * KB]);
        let e2 = memory.insert(2, vec![2; 7 * KB]);

        enqueue(&store, e1.clone());
        enqueue(&store, e2);
        store.wait().await;

        let r1 = store.load(memory.hash(&1)).await.unwrap().unwrap();
        assert_eq!(r1, (1, vec![1; 7 * KB]));
        let r2 = store.load(memory.hash(&2)).await.unwrap().unwrap();
        assert_eq!(r2, (2, vec![2; 7 * KB]));

        // [ [e1, e2], [e3, e4], [], [] ]
        let e3 = memory.insert(3, vec![3; 7 * KB]);
        let e4 = memory.insert(4, vec![4; 6 * KB]);

        enqueue(&store, e3);
        enqueue(&store, e4);
        store.wait().await;

        let r1 = store.load(memory.hash(&1)).await.unwrap().unwrap();
        assert_eq!(r1, (1, vec![1; 7 * KB]));
        let r2 = store.load(memory.hash(&2)).await.unwrap().unwrap();
        assert_eq!(r2, (2, vec![2; 7 * KB]));
        let r3 = store.load(memory.hash(&3)).await.unwrap().unwrap();
        assert_eq!(r3, (3, vec![3; 7 * KB]));
        let r4 = store.load(memory.hash(&4)).await.unwrap().unwrap();
        assert_eq!(r4, (4, vec![4; 6 * KB]));

        // [ [e1, e2], [e3, e4], [e5], [] ]
        let e5 = memory.insert(5, vec![5; 13 * KB]);
        enqueue(&store, e5);
        store.wait().await;

        let r1 = store.load(memory.hash(&1)).await.unwrap().unwrap();
        assert_eq!(r1, (1, vec![1; 7 * KB]));
        let r2 = store.load(memory.hash(&2)).await.unwrap().unwrap();
        assert_eq!(r2, (2, vec![2; 7 * KB]));
        let r3 = store.load(memory.hash(&3)).await.unwrap().unwrap();
        assert_eq!(r3, (3, vec![3; 7 * KB]));
        let r4 = store.load(memory.hash(&4)).await.unwrap().unwrap();
        assert_eq!(r4, (4, vec![4; 6 * KB]));
        let r5 = store.load(memory.hash(&5)).await.unwrap().unwrap();
        assert_eq!(r5, (5, vec![5; 13 * KB]));

        // [ [], [e3, e4], [e5], [e6, e4*] ]
        let e6 = memory.insert(6, vec![6; 7 * KB]);
        let e4v2 = memory.insert(4, vec![!4; 7 * KB]);
        enqueue(&store, e6);
        enqueue(&store, e4v2);
        store.wait().await;

        assert!(store.load(memory.hash(&1)).await.unwrap().is_none());
        assert!(store.load(memory.hash(&2)).await.unwrap().is_none());
        let r3 = store.load(memory.hash(&3)).await.unwrap().unwrap();
        assert_eq!(r3, (3, vec![3; 7 * KB]));
        let r4v2 = store.load(memory.hash(&4)).await.unwrap().unwrap();
        assert_eq!(r4v2, (4, vec![!4; 7 * KB]));
        let r5 = store.load(memory.hash(&5)).await.unwrap().unwrap();
        assert_eq!(r5, (5, vec![5; 13 * KB]));
        let r6 = store.load(memory.hash(&6)).await.unwrap().unwrap();
        assert_eq!(r6, (6, vec![6; 7 * KB]));

        store.close().await.unwrap();
        enqueue(&store, e1);
        store.wait().await;

        drop(store);

        let store = store_for_test(dir.path()).await;

        assert!(store.load(memory.hash(&1)).await.unwrap().is_none());
        assert!(store.load(memory.hash(&2)).await.unwrap().is_none());
        let r3 = store.load(memory.hash(&3)).await.unwrap().unwrap();
        assert_eq!(r3, (3, vec![3; 7 * KB]));
        let r4v2 = store.load(memory.hash(&4)).await.unwrap().unwrap();
        assert_eq!(r4v2, (4, vec![!4; 7 * KB]));
        let r5 = store.load(memory.hash(&5)).await.unwrap().unwrap();
        assert_eq!(r5, (5, vec![5; 13 * KB]));
        let r6 = store.load(memory.hash(&6)).await.unwrap().unwrap();
        assert_eq!(r6, (6, vec![6; 7 * KB]));
    }

    #[test_log::test(tokio::test)]
    async fn test_store_delete_recovery() {
        let dir = tempfile::tempdir().unwrap();

        let memory = cache_for_test();
        let store = store_for_test_with_tombstone_log(dir.path(), dir.path().join("test-tombstone-log")).await;

        let es = (0..10).map(|i| memory.insert(i, vec![i as u8; 7 * KB])).collect_vec();

        for e in es.iter().take(6) {
            enqueue(&store, e.clone());
        }
        store.wait().await;

        for i in 0..6 {
            assert_eq!(
                store.load(memory.hash(&i)).await.unwrap(),
                Some((i, vec![i as u8; 7 * KB]))
            );
        }

        store.delete(memory.hash(&3));
        store.wait().await;
        assert_eq!(store.load(memory.hash(&3)).await.unwrap(), None);

        store.close().await.unwrap();
        drop(store);

        let store = store_for_test_with_tombstone_log(dir.path(), dir.path().join("test-tombstone-log")).await;
        for i in 0..6 {
            if i != 3 {
                assert_eq!(
                    store.load(memory.hash(&i)).await.unwrap(),
                    Some((i, vec![i as u8; 7 * KB]))
                );
            } else {
                assert_eq!(store.load(memory.hash(&3)).await.unwrap(), None);
            }
        }

        enqueue(&store, es[3].clone());
        store.wait().await;
        assert_eq!(store.load(memory.hash(&3)).await.unwrap(), Some((3, vec![3; 7 * KB])));

        store.close().await.unwrap();
        drop(store);

        let store = store_for_test_with_tombstone_log(dir.path(), dir.path().join("test-tombstone-log")).await;

        assert_eq!(store.load(memory.hash(&3)).await.unwrap(), Some((3, vec![3; 7 * KB])));
    }

    #[test_log::test(tokio::test)]
    async fn test_store_destroy_recovery() {
        let dir = tempfile::tempdir().unwrap();

        let memory = cache_for_test();
        let store = store_for_test_with_tombstone_log(dir.path(), dir.path().join("test-tombstone-log")).await;

        let es = (0..10).map(|i| memory.insert(i, vec![i as u8; 7 * KB])).collect_vec();

        for e in es.iter().take(6) {
            enqueue(&store, e.clone());
        }
        store.wait().await;

        for i in 0..6 {
            assert_eq!(
                store.load(memory.hash(&i)).await.unwrap(),
                Some((i, vec![i as u8; 7 * KB]))
            );
        }

        store.delete(memory.hash(&3));
        store.wait().await;
        assert_eq!(store.load(memory.hash(&3)).await.unwrap(), None);

        store.destroy().await.unwrap();

        store.close().await.unwrap();
        drop(store);

        let store = store_for_test_with_tombstone_log(dir.path(), dir.path().join("test-tombstone-log")).await;
        for i in 0..6 {
            assert_eq!(store.load(memory.hash(&i)).await.unwrap(), None);
        }

        enqueue(&store, es[3].clone());
        store.wait().await;
        assert_eq!(store.load(memory.hash(&3)).await.unwrap(), Some((3, vec![3; 7 * KB])));

        store.close().await.unwrap();
        drop(store);

        let store = store_for_test_with_tombstone_log(dir.path(), dir.path().join("test-tombstone-log")).await;

        assert_eq!(store.load(memory.hash(&3)).await.unwrap(), Some((3, vec![3; 7 * KB])));
    }

    // FIXME(MrCroxx): Move the admission test to store level.
    // #[test_log::test(tokio::test)]
    // async fn test_store_admission() {
    //     let dir = tempfile::tempdir().unwrap();

    //     let memory = cache_for_test();
    //     let store = store_for_test_with_admission_picker(&memory, dir.path(),
    // Arc::new(BiasedPicker::new([1]))).await;

    //     let e1 = memory.insert(1, vec![1; 7 * KB]);
    //     let e2 = memory.insert(2, vec![2; 7 * KB]);

    //     assert!(enqueue(&store, e1.clone(),).await.unwrap());
    //     assert!(!enqueue(&store, e2,).await.unwrap());

    //     let r1 = store.load(&1).await.unwrap().unwrap();
    //     assert_eq!(r1, (1, vec![1; 7 * KB]));
    //     assert!(store.load(&2).await.unwrap().is_none());
    // }

    #[test_log::test(tokio::test)]
    async fn test_store_reinsertion() {
        let dir = tempfile::tempdir().unwrap();

        let memory = cache_for_test();
        let store =
            store_for_test_with_reinsertion_picker(dir.path(), Arc::new(BiasedPicker::new(vec![1, 3, 5, 7, 9]))).await;

        let es = (0..10).map(|i| memory.insert(i, vec![i as u8; 7 * KB])).collect_vec();

        // [ [e0, e1], [e2, e3], [e4, e5], [] ]
        for e in es.iter().take(6).cloned() {
            enqueue(&store, e);
            store.wait().await;
        }

        for i in 0..6 {
            let r = store.load(memory.hash(&i)).await.unwrap().unwrap();
            assert_eq!(r, (i, vec![i as u8; 7 * KB]));
        }

        // [ [], [e2, e3], [e4, e5], [e6, e1] ]
        enqueue(&store, es[6].clone());
        store.wait().await;
        for reclaimer in store.inner.reclaimers.iter() {
            reclaimer.wait().await;
        }
        let mut res = vec![];
        for i in 0..7 {
            res.push(store.load(memory.hash(&i)).await.unwrap());
        }
        assert_eq!(
            res,
            vec![
                None,
                Some((1, vec![1; 7 * KB])),
                Some((2, vec![2; 7 * KB])),
                Some((3, vec![3; 7 * KB])),
                Some((4, vec![4; 7 * KB])),
                Some((5, vec![5; 7 * KB])),
                Some((6, vec![6; 7 * KB])),
            ]
        );

        // [ [e7, e3], [], [e4, e5], [e6, e1] ]
        enqueue(&store, es[7].clone());
        store.wait().await;
        for reclaimer in store.inner.reclaimers.iter() {
            reclaimer.wait().await;
        }
        let mut res = vec![];
        for i in 0..8 {
            tracing::trace!("==========> {i}");
            res.push(store.load(memory.hash(&i)).await.unwrap());
        }
        assert_eq!(
            res,
            vec![
                None,
                Some((1, vec![1; 7 * KB])),
                None,
                Some((3, vec![3; 7 * KB])),
                Some((4, vec![4; 7 * KB])),
                Some((5, vec![5; 7 * KB])),
                Some((6, vec![6; 7 * KB])),
                Some((7, vec![7; 7 * KB])),
            ]
        );

        // [ [e7, e3], [e8, e9], [], [e6, e1] ]
        store.delete(memory.hash(&5));
        enqueue(&store, es[8].clone());
        enqueue(&store, es[9].clone());
        store.wait().await;
        for reclaimer in store.inner.reclaimers.iter() {
            reclaimer.wait().await;
        }
        let mut res = vec![];
        for i in 0..10 {
            res.push(store.load(memory.hash(&i)).await.unwrap());
        }
        assert_eq!(
            res,
            vec![
                None,
                Some((1, vec![1; 7 * KB])),
                None,
                Some((3, vec![3; 7 * KB])),
                None,
                None,
                Some((6, vec![6; 7 * KB])),
                Some((7, vec![7; 7 * KB])),
                Some((8, vec![8; 7 * KB])),
                Some((9, vec![9; 7 * KB])),
            ]
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_store_magic_checksum_mismatch() {
        let dir = tempfile::tempdir().unwrap();

        let memory = cache_for_test();
        let store = store_for_test(dir.path()).await;

        // write entry 1
        let e1 = memory.insert(1, vec![1; 7 * KB]);
        enqueue(&store, e1);
        store.wait().await;

        // check entry 1
        let r1 = store.load(memory.hash(&1)).await.unwrap().unwrap();
        assert_eq!(r1, (1, vec![1; 7 * KB]));

        // corrupt entry and header
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let entry = entry.unwrap();
            if !entry.metadata().unwrap().is_file() {
                continue;
            }

            let file = File::options().write(true).open(entry.path()).unwrap();
            #[cfg(target_family = "unix")]
            {
                use std::os::unix::fs::FileExt;
                file.write_all_at(&[b'x'; 4 * 1024], 0).unwrap();
            }
            #[cfg(target_family = "windows")]
            {
                use std::os::windows::fs::FileExt;
                file.seek_write(&[b'x'; 4 * 1024], 0).unwrap();
            }
        }

        assert!(store.load(memory.hash(&1)).await.unwrap().is_none());
    }
}
