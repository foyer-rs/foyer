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
//  limitations under the License.use std::marker::PhantomData;

use std::{
    borrow::Borrow,
    fmt::Debug,
    future::Future,
    hash::Hash,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use foyer_common::{
    bits,
    code::{HashBuilder, StorageKey, StorageValue},
};
use foyer_memory::{Cache, CacheEntry};
use futures::future::{join_all, try_join_all};

use tokio::{
    runtime::Handle,
    sync::{oneshot, Semaphore},
};

use crate::{
    compress::Compression,
    device::{
        monitor::{DeviceStats, Monitored},
        Device, DeviceExt, RegionId,
    },
    error::{Error, Result},
    large::reclaimer::RegionCleaner,
    picker::{AdmissionPicker, EvictionPicker, ReinsertionPicker},
    region::RegionManager,
    serde::EntryDeserializer,
    statistics::Statistics,
    storage::{EnqueueFuture, Storage},
    tombstone::{Tombstone, TombstoneLog, TombstoneLogConfig},
    AtomicSequence,
};

use super::{
    flusher::{Flusher, InvalidStats, Submission},
    indexer::Indexer,
    reclaimer::Reclaimer,
    recover::{RecoverMode, RecoverRunner},
};

pub struct GenericStoreConfig<K, V, S, D>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
    D: Device,
{
    pub memory: Cache<K, V, S>,

    pub device_config: D::Options,
    pub compression: Compression,
    pub flush: bool,
    pub indexer_shards: usize,
    pub recover_mode: RecoverMode,
    pub recover_concurrency: usize,
    pub flushers: usize,
    pub reclaimers: usize,
    pub clean_region_threshold: usize,
    pub eviction_pickers: Vec<Box<dyn EvictionPicker>>,
    pub admission_picker: Arc<dyn AdmissionPicker<Key = K>>,
    pub reinsertion_picker: Arc<dyn ReinsertionPicker<Key = K>>,
    pub tombstone_log_config: Option<TombstoneLogConfig>,
}

impl<K, V, S, D> Debug for GenericStoreConfig<K, V, S, D>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
    D: Device,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenericStoreConfig")
            .field("memory", &self.memory)
            .field("device_config", &self.device_config)
            .field("compression", &self.compression)
            .field("flush", &self.flush)
            .field("indexer_shards", &self.indexer_shards)
            .field("recover_mode", &self.recover_mode)
            .field("recover_concurrency", &self.recover_concurrency)
            .field("flushers", &self.flushers)
            .field("reclaimers", &self.reclaimers)
            .field("clean_region_threshold", &self.clean_region_threshold)
            .field("eviction_pickers", &self.eviction_pickers)
            .field("admission_pickers", &self.admission_picker)
            .field("reinsertion_pickers", &self.reinsertion_picker)
            .field("tombstone_log_config", &self.tombstone_log_config)
            .finish()
    }
}

pub struct GenericStore<K, V, S, D>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
    D: Device,
{
    inner: Arc<GenericStoreInner<K, V, S, D>>,
}

impl<K, V, S, D> Debug for GenericStore<K, V, S, D>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
    D: Device,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenericStore").finish()
    }
}

struct GenericStoreInner<K, V, S, D>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
    D: Device,
{
    /// Holds the memory cache for reinsertion.
    memory: Cache<K, V, S>,

    indexer: Indexer,
    device: Monitored<D>,
    region_manager: RegionManager<D>,

    flushers: Vec<Flusher<K, V, S, D>>,
    reclaimers: Vec<Reclaimer>,

    admission_picker: Arc<dyn AdmissionPicker<Key = K>>,

    stats: Arc<Statistics>,

    flush: bool,

    sequence: AtomicSequence,

    runtime: Handle,

    active: AtomicBool,
}

impl<K, V, S, D> Clone for GenericStore<K, V, S, D>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
    D: Device,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<K, V, S, D> GenericStore<K, V, S, Monitored<D>>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
    D: Device,
{
    pub fn device_stats(&self) -> &Arc<DeviceStats> {
        self.inner.device.stat()
    }
}

impl<K, V, S, D> GenericStore<K, V, S, D>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
    D: Device,
{
    async fn open(mut config: GenericStoreConfig<K, V, S, D>) -> Result<Self> {
        let runtime = Handle::current();

        let device = Monitored::open(config.device_config.clone()).await?;

        let stats = Arc::<Statistics>::default();

        let mut tombstones = vec![];
        let tombstone_log = match &config.tombstone_log_config {
            None => None,
            Some(config) => {
                let log = TombstoneLog::open(&config.path, device.clone(), config.flush, &mut tombstones).await?;
                Some(log)
            }
        };

        let indexer = Indexer::new(config.indexer_shards);
        let mut eviction_pickers = std::mem::take(&mut config.eviction_pickers);
        for picker in eviction_pickers.iter_mut() {
            picker.init(device.regions(), device.region_size());
        }
        let reclaim_semaphore = Arc::new(Semaphore::new(0));
        let region_manager = RegionManager::new(device.clone(), eviction_pickers, reclaim_semaphore.clone());
        let sequence = AtomicSequence::default();

        RecoverRunner::run(
            &config,
            device.clone(),
            &sequence,
            &indexer,
            &region_manager,
            &tombstones,
            runtime.clone(),
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
                runtime.clone(),
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
                runtime.clone(),
            )
            .await
        }))
        .await;

        Ok(Self {
            inner: Arc::new(GenericStoreInner {
                memory: config.memory,
                indexer,
                device,
                region_manager,
                flushers,
                reclaimers,
                admission_picker: config.admission_picker,
                stats,
                flush: config.flush,
                sequence,
                runtime,
                active: AtomicBool::new(true),
            }),
        })
    }

    async fn close(&self) -> Result<()> {
        self.inner.active.store(false, Ordering::Relaxed);
        try_join_all(self.inner.flushers.iter().map(|flusher| flusher.wait())).await?;
        join_all(self.inner.reclaimers.iter().map(|reclaimer| reclaimer.wait())).await;
        Ok(())
    }

    fn enqueue(&self, entry: CacheEntry<K, V, S>) -> EnqueueFuture {
        let (tx, rx) = oneshot::channel();
        let future = EnqueueFuture::new(rx);

        let this = self.clone();

        self.inner.runtime.spawn(async move {
            if !this.inner.active.load(Ordering::Relaxed) {
                tx.send(Err(anyhow::anyhow!("cannot enqueue new entry after closed").into()))
                    .unwrap();
                return;
            }

            if this.inner.admission_picker.pick(&this.inner.stats, entry.key()) {
                let sequence = this.inner.sequence.fetch_add(1, Ordering::Relaxed);
                this.inner.flushers[sequence as usize % this.inner.flushers.len()]
                    .submit(Submission::CacheEntry { entry, tx }, sequence);
            } else {
                let _ = tx.send(Ok(false));
            }
        });

        future
    }

    fn load<Q>(&self, key: &Q) -> impl Future<Output = Result<Option<(K, V)>>> + Send + 'static
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Send + Sync + 'static,
    {
        let hash = self.inner.memory.hash_builder().hash_one(key);

        let device = self.inner.device.clone();
        let indexer = self.inner.indexer.clone();
        let stats = self.inner.stats.clone();

        async move {
            let addr = match indexer.get(hash) {
                Some(addr) => addr,
                None => return Ok(None),
            };

            tracing::trace!("{addr:#?}");

            let buffer = device.read(addr.region, addr.offset as _, addr.len as _).await?;

            stats
                .cache_read_bytes
                .fetch_add(bits::align_up(device.align(), buffer.len()), Ordering::Relaxed);

            let (_, k, v) = match EntryDeserializer::deserialize::<K, V>(&buffer) {
                Ok(res) => res,
                Err(e) => match e {
                    Error::MagicMismatch { .. } | Error::ChecksumMismatch { .. } => {
                        tracing::trace!("deserialize read buffer raise error: {e}, remove this entry and skip");
                        indexer.remove(hash);
                        return Ok(None);
                    }
                    e => return Err(e),
                },
            };

            Ok(Some((k, v)))
        }
    }

    fn delete<Q>(&self, key: &Q) -> EnqueueFuture
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let (tx, rx) = oneshot::channel();
        let future = EnqueueFuture::new(rx);

        if !self.inner.active.load(Ordering::Relaxed) {
            tx.send(Err(anyhow::anyhow!("cannot delete entry after closed").into()))
                .unwrap();
            return future;
        }

        let hash = self.inner.memory.hash_builder().hash_one(key);

        let stats = self.inner.indexer.remove(hash).map(|addr| InvalidStats {
            region: addr.region,
            size: bits::align_up(self.inner.device.align(), addr.len as usize),
        });

        let this = self.clone();
        self.inner.runtime.spawn(async move {
            let sequence = this.inner.sequence.fetch_add(1, Ordering::Relaxed);
            this.inner.flushers[sequence as usize % this.inner.flushers.len()].submit(
                Submission::Tombstone {
                    tombstone: Tombstone { hash, sequence },
                    stats,
                    tx,
                },
                sequence,
            );
        });

        future
    }

    fn may_contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.inner.memory.hash_builder().hash_one(key);
        self.inner.indexer.get(hash).is_some()
    }

    async fn destroy(&self) -> Result<()> {
        if !self.inner.active.load(Ordering::Relaxed) {
            return Err(anyhow::anyhow!("cannot delete entry after closed").into());
        }

        // Clear indices.
        self.inner.indexer.clear();

        // Write an tombstone to clear tombstone log by increase the max sequence.
        let sequence = self.inner.sequence.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        let future = EnqueueFuture::new(rx);
        self.inner.flushers[sequence as usize % self.inner.flushers.len()].submit(
            Submission::Tombstone {
                tombstone: Tombstone { hash: 0, sequence },
                stats: None,
                tx,
            },
            sequence,
        );
        future.await?;

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

impl<K, V, S, D> Storage for GenericStore<K, V, S, D>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
    D: Device,
{
    type Key = K;
    type Value = V;
    type BuildHasher = S;
    type Config = GenericStoreConfig<K, V, S, D>;

    async fn open(config: Self::Config) -> Result<Self> {
        Self::open(config).await
    }

    async fn close(&self) -> Result<()> {
        self.close().await
    }

    fn enqueue(&self, entry: CacheEntry<Self::Key, Self::Value, Self::BuildHasher>) -> EnqueueFuture {
        self.enqueue(entry)
    }

    fn load<Q>(&self, key: &Q) -> impl Future<Output = Result<Option<(Self::Key, Self::Value)>>> + Send + 'static
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Send + Sync + 'static,
    {
        self.load(key)
    }

    fn delete<Q>(&self, key: &Q) -> EnqueueFuture
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.delete(key)
    }

    fn may_contains<Q>(&self, key: &Q) -> bool
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.may_contains(key)
    }

    async fn destroy(&self) -> Result<()> {
        self.destroy().await
    }

    fn stats(&self) -> Arc<DeviceStats> {
        self.inner.device.stat().clone()
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use ahash::RandomState;
    use foyer_memory::{Cache, CacheBuilder, FifoConfig};
    use itertools::Itertools;

    use crate::{
        device::direct_fs::{DirectFsDevice, DirectFsDeviceOptions},
        picker::utils::{AdmitAllPicker, FifoPicker, RejectAllPicker},
        test_utils::BiasedPicker,
        tombstone::TombstoneLogConfigBuilder,
    };

    use super::*;

    const KB: usize = 1024;

    fn cache_for_test() -> Cache<u64, Vec<u8>> {
        CacheBuilder::new(10)
            .with_eviction_config(FifoConfig::default())
            .build()
    }

    /// 4 files, fifo eviction, 16 KiB region, 64 KiB capacity.
    async fn store_for_test(
        memory: &Cache<u64, Vec<u8>>,
        dir: impl AsRef<Path>,
    ) -> GenericStore<u64, Vec<u8>, RandomState, DirectFsDevice> {
        store_for_test_with_admission_picker(memory, dir, Arc::<AdmitAllPicker<u64>>::default()).await
    }

    async fn store_for_test_with_admission_picker(
        memory: &Cache<u64, Vec<u8>>,
        dir: impl AsRef<Path>,
        admission_picker: Arc<dyn AdmissionPicker<Key = u64>>,
    ) -> GenericStore<u64, Vec<u8>, RandomState, DirectFsDevice> {
        let config = GenericStoreConfig {
            memory: memory.clone(),
            device_config: DirectFsDeviceOptions {
                dir: dir.as_ref().into(),
                capacity: 64 * KB,
                file_size: 16 * KB,
            },
            compression: Compression::None,
            flush: true,
            indexer_shards: 4,
            recover_mode: RecoverMode::Strict,
            recover_concurrency: 2,
            flushers: 1,
            reclaimers: 1,
            clean_region_threshold: 1,
            eviction_pickers: vec![Box::<FifoPicker>::default()],
            admission_picker,
            reinsertion_picker: Arc::<RejectAllPicker<u64>>::default(),
            tombstone_log_config: None,
        };
        GenericStore::open(config).await.unwrap()
    }

    async fn store_for_test_with_reinsertion_picker(
        memory: &Cache<u64, Vec<u8>>,
        dir: impl AsRef<Path>,
        reinsertion_picker: Arc<dyn ReinsertionPicker<Key = u64>>,
    ) -> GenericStore<u64, Vec<u8>, RandomState, DirectFsDevice> {
        let config = GenericStoreConfig {
            memory: memory.clone(),
            device_config: DirectFsDeviceOptions {
                dir: dir.as_ref().into(),
                capacity: 64 * KB,
                file_size: 16 * KB,
            },
            compression: Compression::None,
            flush: true,
            indexer_shards: 4,
            recover_mode: RecoverMode::Strict,
            recover_concurrency: 2,
            flushers: 1,
            reclaimers: 1,
            clean_region_threshold: 1,
            eviction_pickers: vec![Box::<FifoPicker>::default()],
            admission_picker: Arc::<AdmitAllPicker<u64>>::default(),
            reinsertion_picker,
            tombstone_log_config: None,
        };
        GenericStore::open(config).await.unwrap()
    }

    async fn store_for_test_with_tombstone_log(
        memory: &Cache<u64, Vec<u8>>,
        dir: impl AsRef<Path>,
        path: impl AsRef<Path>,
    ) -> GenericStore<u64, Vec<u8>, RandomState, DirectFsDevice> {
        let config = GenericStoreConfig {
            memory: memory.clone(),
            device_config: DirectFsDeviceOptions {
                dir: dir.as_ref().into(),
                capacity: 64 * KB,
                file_size: 16 * KB,
            },
            compression: Compression::None,
            flush: true,
            indexer_shards: 4,
            recover_mode: RecoverMode::Strict,
            recover_concurrency: 2,
            flushers: 1,
            reclaimers: 1,
            clean_region_threshold: 1,
            eviction_pickers: vec![Box::<FifoPicker>::default()],
            admission_picker: Arc::<AdmitAllPicker<u64>>::default(),
            reinsertion_picker: Arc::<RejectAllPicker<u64>>::default(),
            tombstone_log_config: Some(TombstoneLogConfigBuilder::new(path).with_flush(true).build()),
        };
        GenericStore::open(config).await.unwrap()
    }

    #[test_log::test(tokio::test)]
    async fn test_store_enqueue_lookup_recovery() {
        let dir = tempfile::tempdir().unwrap();

        let memory = cache_for_test();
        let store = store_for_test(&memory, dir.path()).await;

        // [ [e1, e2], [], [], [] ]
        let e1 = memory.insert(1, vec![1; 7 * KB]);
        let e2 = memory.insert(2, vec![2; 7 * KB]);

        assert!(store.enqueue(e1.clone()).await.unwrap());
        assert!(store.enqueue(e2).await.unwrap());

        let r1 = store.load(&1).await.unwrap().unwrap();
        assert_eq!(r1, (1, vec![1; 7 * KB]));
        let r2 = store.load(&2).await.unwrap().unwrap();
        assert_eq!(r2, (2, vec![2; 7 * KB]));

        // [ [e1, e2], [e3, e4], [], [] ]
        let e3 = memory.insert(3, vec![3; 7 * KB]);
        let e4 = memory.insert(4, vec![4; 6 * KB]);

        assert!(store.enqueue(e3).await.unwrap());
        assert!(store.enqueue(e4).await.unwrap());

        let r1 = store.load(&1).await.unwrap().unwrap();
        assert_eq!(r1, (1, vec![1; 7 * KB]));
        let r2 = store.load(&2).await.unwrap().unwrap();
        assert_eq!(r2, (2, vec![2; 7 * KB]));
        let r3 = store.load(&3).await.unwrap().unwrap();
        assert_eq!(r3, (3, vec![3; 7 * KB]));
        let r4 = store.load(&4).await.unwrap().unwrap();
        assert_eq!(r4, (4, vec![4; 6 * KB]));

        // [ [e1, e2], [e3, e4], [e5], [] ]
        let e5 = memory.insert(5, vec![5; 13 * KB]);
        assert!(store.enqueue(e5).await.unwrap());

        let r1 = store.load(&1).await.unwrap().unwrap();
        assert_eq!(r1, (1, vec![1; 7 * KB]));
        let r2 = store.load(&2).await.unwrap().unwrap();
        assert_eq!(r2, (2, vec![2; 7 * KB]));
        let r3 = store.load(&3).await.unwrap().unwrap();
        assert_eq!(r3, (3, vec![3; 7 * KB]));
        let r4 = store.load(&4).await.unwrap().unwrap();
        assert_eq!(r4, (4, vec![4; 6 * KB]));
        let r5 = store.load(&5).await.unwrap().unwrap();
        assert_eq!(r5, (5, vec![5; 13 * KB]));

        // [ [], [e3, e4], [e5], [e6, e4*] ]
        let e6 = memory.insert(6, vec![6; 7 * KB]);
        let e4v2 = memory.insert(4, vec![!4; 7 * KB]);
        assert!(store.enqueue(e6).await.unwrap());
        assert!(store.enqueue(e4v2).await.unwrap());

        assert!(store.load(&1).await.unwrap().is_none());
        assert!(store.load(&2).await.unwrap().is_none());
        let r3 = store.load(&3).await.unwrap().unwrap();
        assert_eq!(r3, (3, vec![3; 7 * KB]));
        let r4v2 = store.load(&4).await.unwrap().unwrap();
        assert_eq!(r4v2, (4, vec![!4; 7 * KB]));
        let r5 = store.load(&5).await.unwrap().unwrap();
        assert_eq!(r5, (5, vec![5; 13 * KB]));
        let r6 = store.load(&6).await.unwrap().unwrap();
        assert_eq!(r6, (6, vec![6; 7 * KB]));

        store.close().await.unwrap();
        assert!(store.enqueue(e1).await.is_err());
        drop(store);

        let store = store_for_test(&memory, dir.path()).await;

        assert!(store.load(&1).await.unwrap().is_none());
        assert!(store.load(&2).await.unwrap().is_none());
        let r3 = store.load(&3).await.unwrap().unwrap();
        assert_eq!(r3, (3, vec![3; 7 * KB]));
        let r4v2 = store.load(&4).await.unwrap().unwrap();
        assert_eq!(r4v2, (4, vec![!4; 7 * KB]));
        let r5 = store.load(&5).await.unwrap().unwrap();
        assert_eq!(r5, (5, vec![5; 13 * KB]));
        let r6 = store.load(&6).await.unwrap().unwrap();
        assert_eq!(r6, (6, vec![6; 7 * KB]));
    }

    #[test_log::test(tokio::test)]
    async fn test_store_delete_recovery() {
        let dir = tempfile::tempdir().unwrap();

        let memory = cache_for_test();
        let store = store_for_test_with_tombstone_log(&memory, dir.path(), dir.path().join("test-tombstone-log")).await;

        let es = (0..10).map(|i| memory.insert(i, vec![i as u8; 7 * KB])).collect_vec();

        assert!(try_join_all(es.iter().take(6).map(|e| store.enqueue(e.clone())))
            .await
            .unwrap()
            .into_iter()
            .all(|inserted| inserted));

        for i in 0..6 {
            assert_eq!(store.load(&i).await.unwrap(), Some((i, vec![i as u8; 7 * KB])));
        }

        assert!(store.delete(&3).await.unwrap());
        assert_eq!(store.load(&3).await.unwrap(), None);

        store.close().await.unwrap();
        drop(store);

        let store = store_for_test_with_tombstone_log(&memory, dir.path(), dir.path().join("test-tombstone-log")).await;
        for i in 0..6 {
            if i != 3 {
                assert_eq!(store.load(&i).await.unwrap(), Some((i, vec![i as u8; 7 * KB])));
            } else {
                assert_eq!(store.load(&3).await.unwrap(), None);
            }
        }

        assert!(store.enqueue(es[3].clone()).await.unwrap());
        assert_eq!(store.load(&3).await.unwrap(), Some((3, vec![3; 7 * KB])));

        store.close().await.unwrap();
        drop(store);

        let store = store_for_test_with_tombstone_log(&memory, dir.path(), dir.path().join("test-tombstone-log")).await;

        assert_eq!(store.load(&3).await.unwrap(), Some((3, vec![3; 7 * KB])));
    }

    #[test_log::test(tokio::test)]
    async fn test_store_destroy_recovery() {
        let dir = tempfile::tempdir().unwrap();

        let memory = cache_for_test();
        let store = store_for_test_with_tombstone_log(&memory, dir.path(), dir.path().join("test-tombstone-log")).await;

        let es = (0..10).map(|i| memory.insert(i, vec![i as u8; 7 * KB])).collect_vec();

        assert!(try_join_all(es.iter().take(6).map(|e| store.enqueue(e.clone())))
            .await
            .unwrap()
            .into_iter()
            .all(|inserted| inserted));

        for i in 0..6 {
            assert_eq!(store.load(&i).await.unwrap(), Some((i, vec![i as u8; 7 * KB])));
        }

        assert!(store.delete(&3).await.unwrap());
        assert_eq!(store.load(&3).await.unwrap(), None);

        store.destroy().await.unwrap();

        store.close().await.unwrap();
        drop(store);

        let store = store_for_test_with_tombstone_log(&memory, dir.path(), dir.path().join("test-tombstone-log")).await;
        for i in 0..6 {
            assert_eq!(store.load(&i).await.unwrap(), None);
        }

        assert!(store.enqueue(es[3].clone()).await.unwrap());
        assert_eq!(store.load(&3).await.unwrap(), Some((3, vec![3; 7 * KB])));

        store.close().await.unwrap();
        drop(store);

        let store = store_for_test_with_tombstone_log(&memory, dir.path(), dir.path().join("test-tombstone-log")).await;

        assert_eq!(store.load(&3).await.unwrap(), Some((3, vec![3; 7 * KB])));
    }

    #[test_log::test(tokio::test)]
    async fn test_store_admission() {
        let dir = tempfile::tempdir().unwrap();

        let memory = cache_for_test();
        let store = store_for_test_with_admission_picker(&memory, dir.path(), Arc::new(BiasedPicker::new([1]))).await;

        let e1 = memory.insert(1, vec![1; 7 * KB]);
        let e2 = memory.insert(2, vec![2; 7 * KB]);

        assert!(store.enqueue(e1.clone()).await.unwrap());
        assert!(!store.enqueue(e2).await.unwrap());

        let r1 = store.load(&1).await.unwrap().unwrap();
        assert_eq!(r1, (1, vec![1; 7 * KB]));
        assert!(store.load(&2).await.unwrap().is_none());
    }

    #[test_log::test(tokio::test)]
    async fn test_store_reinsertion() {
        let dir = tempfile::tempdir().unwrap();

        let memory = cache_for_test();
        let store = store_for_test_with_reinsertion_picker(
            &memory,
            dir.path(),
            Arc::new(BiasedPicker::new(vec![1, 3, 5, 7, 9])),
        )
        .await;

        let es = (0..10).map(|i| memory.insert(i, vec![i as u8; 7 * KB])).collect_vec();

        // [ [e0, e1], [e2, e3], [e4, e5], [] ]
        for e in es.iter().take(6).cloned() {
            assert!(store.enqueue(e).await.unwrap());
        }
        for i in 0..6 {
            let r = store.load(&i).await.unwrap().unwrap();
            assert_eq!(r, (i, vec![i as u8; 7 * KB]));
        }

        // [ [], [e2, e3], [e4, e5], [e6, e1] ]
        assert!(store.enqueue(es[6].clone()).await.unwrap());
        for reclaimer in store.inner.reclaimers.iter() {
            reclaimer.wait().await;
        }
        let mut res = vec![];
        for i in 0..7 {
            res.push(store.load(&i).await.unwrap());
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
        assert!(store.enqueue(es[7].clone()).await.unwrap());
        for reclaimer in store.inner.reclaimers.iter() {
            reclaimer.wait().await;
        }
        let mut res = vec![];
        for i in 0..8 {
            res.push(store.load(&i).await.unwrap());
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
        store.delete(&5).await.unwrap();
        assert!(store.enqueue(es[8].clone()).await.unwrap());
        assert!(store.enqueue(es[9].clone()).await.unwrap());
        for reclaimer in store.inner.reclaimers.iter() {
            reclaimer.wait().await;
        }
        let mut res = vec![];
        for i in 0..10 {
            res.push(store.load(&i).await.unwrap());
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
}
