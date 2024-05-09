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
    hash::{BuildHasher, Hash},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use foyer_common::code::{StorageKey, StorageValue};
use foyer_memory::{CacheEntry, DefaultCacheEventListener};
use futures::future::{join_all, try_join_all};

use tokio::sync::{oneshot, Semaphore};

use crate::{
    error::{Error, Result},
    large::{device::RegionId, reclaimer::RegionCleaner},
    serde::EntryDeserializer,
    Compression,
};

use crate::catalog::AtomicSequence;

use super::{
    admission::AdmissionPicker,
    device::{Device, DeviceExt},
    eviction::EvictionPicker,
    flusher::Flusher,
    indexer::Indexer,
    reclaimer::Reclaimer,
    recover::{RecoverMode, RecoverRunner},
    region::RegionManager,
    storage::{EnqueueFuture, Storage},
    tombstone::{Tombstone, TombstoneLog, TombstoneLogConfig},
};

pub struct GenericStoreConfig<K, V, S, D>
where
    K: StorageKey,
    V: StorageValue,
    S: BuildHasher + Send + Sync + 'static + Debug,
    D: Device,
{
    pub device_config: D::Config,
    pub compression: Compression,
    pub flush: bool,
    pub indexer_shards: usize,
    pub recover_mode: RecoverMode,
    pub recover_concurrency: usize,
    pub hash_builder: S,
    pub flushers: usize,
    pub reclaimers: usize,
    pub clean_region_threshold: usize,
    pub eviction_pickers: Vec<Box<dyn EvictionPicker>>,
    pub admission_picker: Box<dyn AdmissionPicker<Key = K, Value = V>>,
    pub tombstone_log_config: Option<TombstoneLogConfig>,
}

impl<K, V, S, D> Debug for GenericStoreConfig<K, V, S, D>
where
    K: StorageKey,
    V: StorageValue,
    S: BuildHasher + Send + Sync + 'static + Debug,
    D: Device,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenericStoreConfig")
            .field("device_config", &self.device_config)
            .field("compression", &self.compression)
            .field("flush", &self.flush)
            .field("indexer_shards", &self.indexer_shards)
            .field("recover_mode", &self.recover_mode)
            .field("recover_concurrency", &self.recover_concurrency)
            .field("hash_builder", &self.hash_builder)
            .field("flushers", &self.flushers)
            .field("reclaimers", &self.reclaimers)
            .field("clean_region_threshold", &self.clean_region_threshold)
            .field("eviction_pickers", &self.eviction_pickers)
            .finish()
    }
}

#[derive(Debug)]
pub struct GenericStore<K, V, S, D>
where
    K: StorageKey,
    V: StorageValue,
    S: BuildHasher + Send + Sync + 'static + Debug,
    D: Device,
{
    inner: Arc<GenericStoreInner<K, V, S, D>>,
}

#[derive(Debug)]
struct GenericStoreInner<K, V, S, D>
where
    K: StorageKey,
    V: StorageValue,
    S: BuildHasher + Send + Sync + 'static + Debug,
    D: Device,
{
    indexer: Indexer,
    device: D,
    region_manager: RegionManager<D>,

    flushers: Vec<Flusher<K, V, S, D>>,
    reclaimers: Vec<Reclaimer>,

    admission_picker: Box<dyn AdmissionPicker<Key = K, Value = V>>,

    sequence: AtomicSequence,

    hash_builder: S,

    active: AtomicBool,
}

impl<K, V, S, D> Clone for GenericStore<K, V, S, D>
where
    K: StorageKey,
    V: StorageValue,
    S: BuildHasher + Send + Sync + 'static + Debug,
    D: Device,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<K, V, S, D> GenericStore<K, V, S, D>
where
    K: StorageKey,
    V: StorageValue,
    S: BuildHasher + Send + Sync + 'static + Debug,
    D: Device,
{
    async fn open(mut config: GenericStoreConfig<K, V, S, D>) -> Result<Self> {
        let device = D::open(config.device_config.clone()).await?;

        let mut tombstones = vec![];
        let tombstone_log = match &config.tombstone_log_config {
            None => None,
            Some(config) => {
                let log = TombstoneLog::open(&config.path, device.clone(), config.flush, &mut tombstones).await?;
                Some(log)
            }
        };

        let indexer = Indexer::new(device.regions(), config.indexer_shards);
        let eviction_pickers = std::mem::take(&mut config.eviction_pickers);
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
        )
        .await?;

        let flushers = try_join_all((0..config.flushers).map(|_| async {
            Flusher::open(
                &config,
                indexer.clone(),
                region_manager.clone(),
                device.clone(),
                tombstone_log.clone(),
            )
            .await
        }))
        .await?;

        let reclaimers = join_all((0..config.reclaimers).map(|_| async {
            Reclaimer::open(region_manager.clone(), reclaim_semaphore.clone(), indexer.clone()).await
        }))
        .await;

        Ok(Self {
            inner: Arc::new(GenericStoreInner {
                indexer,
                device,
                region_manager,
                flushers,
                reclaimers,
                admission_picker: config.admission_picker,
                sequence,
                hash_builder: config.hash_builder,
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

    fn enqueue(&self, entry: CacheEntry<K, V, DefaultCacheEventListener<K, V>, S>) -> EnqueueFuture {
        if !self.inner.active.load(Ordering::Relaxed) {
            let (tx, rx) = oneshot::channel();
            tx.send(Err(anyhow::anyhow!("cannot enqueue new entry after closed").into()))
                .unwrap();
            return EnqueueFuture::new(rx);
        }
        if self.inner.admission_picker.pick(entry.key(), entry.value()) {
            let sequence = self.inner.sequence.fetch_add(1, Ordering::Relaxed);
            self.inner.flushers[sequence as usize % self.inner.flushers.len()].submit(entry, sequence)
        } else {
            let (tx, rx) = oneshot::channel();
            let _ = tx.send(Ok(false));
            EnqueueFuture::new(rx)
        }
    }

    async fn lookup<Q>(&self, key: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Send + Sync + 'static,
    {
        let hash = self.inner.hash_builder.hash_one(key);
        let addr = match self.inner.indexer.get(hash) {
            Some(addr) => addr,
            None => return Ok(None),
        };

        tracing::trace!("{addr:#?}");

        let buffer = self
            .inner
            .device
            .read(addr.region, addr.offset as _, addr.len as _)
            .await?;

        let (_, k, v) = match EntryDeserializer::deserialize::<K, V>(&buffer) {
            Ok(res) => res,
            Err(e) => match e {
                Error::MagicMismatch { .. } | Error::ChecksumMismatch { .. } => {
                    tracing::trace!("deserialize read buffer raise error: {e}, remove this entry and skip");
                    self.inner.indexer.remove(hash);
                    return Ok(None);
                }
                e => return Err(e),
            },
        };
        if k.borrow() != key {
            return Ok(None);
        }

        Ok(Some(v))
    }

    fn delete<Q>(&self, key: &Q) -> EnqueueFuture
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Send + Sync + 'static,
    {
        if !self.inner.active.load(Ordering::Relaxed) {
            let (tx, rx) = oneshot::channel();
            tx.send(Err(anyhow::anyhow!("cannot delete entry after closed").into()))
                .unwrap();
            return EnqueueFuture::new(rx);
        }

        let hash = self.inner.hash_builder.hash_one(key);

        self.inner.indexer.remove(hash);

        let sequence = self.inner.sequence.fetch_add(1, Ordering::Relaxed);
        self.inner.flushers[sequence as usize % self.inner.flushers.len()]
            .submit(Tombstone { hash, sequence }, sequence)
    }

    async fn destroy(&self) -> Result<()> {
        if !self.inner.active.load(Ordering::Relaxed) {
            return Err(anyhow::anyhow!("cannot delete entry after closed").into());
        }

        // Clear indices.
        self.inner.indexer.clear();

        // Write an tombstone to clear tombstone log by increase the max sequence.
        let sequence = self.inner.sequence.fetch_add(1, Ordering::Relaxed);
        self.inner.flushers[sequence as usize % self.inner.flushers.len()]
            .submit(Tombstone { hash: 0, sequence }, sequence)
            .await?;

        // Clean regions.
        try_join_all((0..self.inner.region_manager.regions() as RegionId).map(|id| {
            let region = self.inner.region_manager.region(id).clone();
            async move { RegionCleaner::clean(&region).await }
        }))
        .await?;

        Ok(())
    }
}

impl<K, V, S, D> Storage for GenericStore<K, V, S, D>
where
    K: StorageKey,
    V: StorageValue,
    S: BuildHasher + Send + Sync + 'static + Debug,
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

    fn enqueue(
        &self,
        entry: CacheEntry<Self::Key, Self::Value, DefaultCacheEventListener<Self::Key, Self::Value>, Self::BuildHasher>,
    ) -> EnqueueFuture {
        self.enqueue(entry)
    }

    async fn lookup<Q>(&self, key: &Q) -> Result<Option<Self::Value>>
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Send + Sync + 'static,
    {
        self.lookup(key).await
    }

    fn delete<Q>(&self, key: &Q) -> EnqueueFuture
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Send + Sync + 'static,
    {
        self.delete(key)
    }

    async fn destroy(&self) -> Result<()> {
        self.destroy().await
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use ahash::RandomState;
    use foyer_memory::{Cache, CacheBuilder, FifoConfig};
    use itertools::Itertools;

    use crate::large::{
        admission::AdmitAllPicker,
        device::direct_fs::{DirectFsDevice, DirectFsDeviceConfig},
        eviction::FifoPicker,
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
        store_for_test_with_admission_picker(memory, dir, Box::<AdmitAllPicker<u64, Vec<u8>>>::default()).await
    }

    async fn store_for_test_with_admission_picker(
        memory: &Cache<u64, Vec<u8>>,
        dir: impl AsRef<Path>,
        admission_picker: Box<dyn AdmissionPicker<Key = u64, Value = Vec<u8>>>,
    ) -> GenericStore<u64, Vec<u8>, RandomState, DirectFsDevice> {
        let config = GenericStoreConfig {
            device_config: DirectFsDeviceConfig {
                dir: dir.as_ref().into(),
                capacity: 64 * KB,
                file_size: 16 * KB,
            },
            compression: Compression::None,
            flush: true,
            indexer_shards: 4,
            recover_mode: RecoverMode::StrictRecovery,
            recover_concurrency: 2,
            hash_builder: memory.hash_builder().clone(),
            flushers: 1,
            reclaimers: 1,
            clean_region_threshold: 1,
            eviction_pickers: vec![Box::<FifoPicker>::default()],
            admission_picker,
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
            device_config: DirectFsDeviceConfig {
                dir: dir.as_ref().into(),
                capacity: 64 * KB,
                file_size: 16 * KB,
            },
            compression: Compression::None,
            flush: true,
            indexer_shards: 4,
            recover_mode: RecoverMode::StrictRecovery,
            recover_concurrency: 2,
            hash_builder: memory.hash_builder().clone(),
            flushers: 1,
            reclaimers: 1,
            clean_region_threshold: 1,
            eviction_pickers: vec![Box::<FifoPicker>::default()],
            admission_picker: Box::<AdmitAllPicker<u64, Vec<u8>>>::default(),
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

        let v1 = store.lookup(&1).await.unwrap().unwrap();
        assert_eq!(v1, vec![1; 7 * KB]);
        let v2 = store.lookup(&2).await.unwrap().unwrap();
        assert_eq!(v2, vec![2; 7 * KB]);

        // [ [e1, e2], [e3, e4], [], [] ]
        let e3 = memory.insert(3, vec![3; 7 * KB]);
        let e4 = memory.insert(4, vec![4; 6 * KB]);

        assert!(store.enqueue(e3).await.unwrap());
        assert!(store.enqueue(e4).await.unwrap());

        let v1 = store.lookup(&1).await.unwrap().unwrap();
        assert_eq!(v1, vec![1; 7 * KB]);
        let v2 = store.lookup(&2).await.unwrap().unwrap();
        assert_eq!(v2, vec![2; 7 * KB]);
        let v3 = store.lookup(&3).await.unwrap().unwrap();
        assert_eq!(v3, vec![3; 7 * KB]);
        let v4 = store.lookup(&4).await.unwrap().unwrap();
        assert_eq!(v4, vec![4; 6 * KB]);

        // [ [e1, e2], [e3, e4], [e5], [] ]
        let e5 = memory.insert(5, vec![5; 13 * KB]);
        assert!(store.enqueue(e5).await.unwrap());

        let v1 = store.lookup(&1).await.unwrap().unwrap();
        assert_eq!(v1, vec![1; 7 * KB]);
        let v2 = store.lookup(&2).await.unwrap().unwrap();
        assert_eq!(v2, vec![2; 7 * KB]);
        let v3 = store.lookup(&3).await.unwrap().unwrap();
        assert_eq!(v3, vec![3; 7 * KB]);
        let v4 = store.lookup(&4).await.unwrap().unwrap();
        assert_eq!(v4, vec![4; 6 * KB]);
        let v5 = store.lookup(&5).await.unwrap().unwrap();
        assert_eq!(v5, vec![5; 13 * KB]);

        // [ [], [e3, e4], [e5], [e6, e4*] ]
        let e6 = memory.insert(6, vec![6; 7 * KB]);
        let e4v2 = memory.insert(4, vec![!4; 7 * KB]);
        assert!(store.enqueue(e6).await.unwrap());
        assert!(store.enqueue(e4v2).await.unwrap());

        assert!(store.lookup(&1).await.unwrap().is_none());
        assert!(store.lookup(&2).await.unwrap().is_none());
        let v3 = store.lookup(&3).await.unwrap().unwrap();
        assert_eq!(v3, vec![3; 7 * KB]);
        let v4v2 = store.lookup(&4).await.unwrap().unwrap();
        assert_eq!(v4v2, vec![!4; 7 * KB]);
        let v5 = store.lookup(&5).await.unwrap().unwrap();
        assert_eq!(v5, vec![5; 13 * KB]);
        let v6 = store.lookup(&6).await.unwrap().unwrap();
        assert_eq!(v6, vec![6; 7 * KB]);

        store.close().await.unwrap();
        assert!(store.enqueue(e1).await.is_err());
        drop(store);

        let store = store_for_test(&memory, dir.path()).await;

        assert!(store.lookup(&1).await.unwrap().is_none());
        assert!(store.lookup(&2).await.unwrap().is_none());
        let v3 = store.lookup(&3).await.unwrap().unwrap();
        assert_eq!(v3, vec![3; 7 * KB]);
        let v4v2 = store.lookup(&4).await.unwrap().unwrap();
        assert_eq!(v4v2, vec![!4; 7 * KB]);
        let v5 = store.lookup(&5).await.unwrap().unwrap();
        assert_eq!(v5, vec![5; 13 * KB]);
        let v6 = store.lookup(&6).await.unwrap().unwrap();
        assert_eq!(v6, vec![6; 7 * KB]);
    }

    #[test_log::test(tokio::test)]
    async fn test_store_reject() {
        let dir = tempfile::tempdir().unwrap();

        let memory = cache_for_test();
        let store = store_for_test_with_admission_picker(&memory, dir.path(), Box::new(BiasedPicker::new([1]))).await;

        let e1 = memory.insert(1, vec![1; 7 * KB]);
        let e2 = memory.insert(2, vec![2; 7 * KB]);

        assert!(store.enqueue(e1.clone()).await.unwrap());
        assert!(!store.enqueue(e2).await.unwrap());

        let v1 = store.lookup(&1).await.unwrap().unwrap();
        assert_eq!(v1, vec![1; 7 * KB]);
        assert!(store.lookup(&2).await.unwrap().is_none());
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
            assert_eq!(store.lookup(&i).await.unwrap(), Some(vec![i as u8; 7 * KB]));
        }

        assert!(store.delete(&3).await.unwrap());
        assert_eq!(store.lookup(&3).await.unwrap(), None);

        store.close().await.unwrap();
        drop(store);

        let store = store_for_test_with_tombstone_log(&memory, dir.path(), dir.path().join("test-tombstone-log")).await;
        for i in 0..6 {
            if i != 3 {
                assert_eq!(store.lookup(&i).await.unwrap(), Some(vec![i as u8; 7 * KB]));
            } else {
                assert_eq!(store.lookup(&3).await.unwrap(), None);
            }
        }

        assert!(store.enqueue(es[3].clone()).await.unwrap());
        assert_eq!(store.lookup(&3).await.unwrap(), Some(vec![3; 7 * KB]));

        store.close().await.unwrap();
        drop(store);

        let store = store_for_test_with_tombstone_log(&memory, dir.path(), dir.path().join("test-tombstone-log")).await;

        assert_eq!(store.lookup(&3).await.unwrap(), Some(vec![3; 7 * KB]));
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
            assert_eq!(store.lookup(&i).await.unwrap(), Some(vec![i as u8; 7 * KB]));
        }

        assert!(store.delete(&3).await.unwrap());
        assert_eq!(store.lookup(&3).await.unwrap(), None);

        store.destroy().await.unwrap();

        store.close().await.unwrap();
        drop(store);

        let store = store_for_test_with_tombstone_log(&memory, dir.path(), dir.path().join("test-tombstone-log")).await;
        for i in 0..6 {
            assert_eq!(store.lookup(&i).await.unwrap(), None);
        }

        assert!(store.enqueue(es[3].clone()).await.unwrap());
        assert_eq!(store.lookup(&3).await.unwrap(), Some(vec![3; 7 * KB]));

        store.close().await.unwrap();
        drop(store);

        let store = store_for_test_with_tombstone_log(&memory, dir.path(), dir.path().join("test-tombstone-log")).await;

        assert_eq!(store.lookup(&3).await.unwrap(), Some(vec![3; 7 * KB]));
    }
}
