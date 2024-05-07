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

use crate::{error::Result, serde::EntryDeserializer, Compression};

use crate::catalog::AtomicSequence;

use super::{
    device::{Device, DeviceExt, IoBuffer, IO_BUFFER_ALLOCATOR},
    flusher::Flusher,
    indexer::Indexer,
    picker::EvictionPicker,
    reclaimer::Reclaimer,
    recover::{RecoverMode, RecoverRunner},
    region::RegionManager,
    storage::{EnqueueHandle, Storage},
};

pub struct GenericStoreConfig<S, D>
where
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
}

impl<S, D> Debug for GenericStoreConfig<S, D>
where
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
    _region_manager: RegionManager<D>,
    flushers: Vec<Flusher<K, V, S, D>>,
    reclaimers: Vec<Reclaimer>,
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
    async fn open(mut config: GenericStoreConfig<S, D>) -> Result<Self> {
        let indexer = Indexer::new(config.indexer_shards);
        let device = D::open(&config.device_config).await?;
        let pickers = std::mem::take(&mut config.eviction_pickers);
        let reclaim_semaphore = Arc::new(Semaphore::new(0));
        let region_manager = RegionManager::new(device.clone(), pickers, reclaim_semaphore.clone());
        let sequence = AtomicSequence::default();

        RecoverRunner::run(&config, device.clone(), &sequence, &indexer, &region_manager).await?;

        let flushers =
            try_join_all((0..config.flushers).map(|_| async {
                Flusher::open(&config, device.clone(), indexer.clone(), region_manager.clone()).await
            }))
            .await?;

        let reclaimers = join_all(
            (0..config.reclaimers)
                .map(|_| async { Reclaimer::open(region_manager.clone(), reclaim_semaphore.clone()).await }),
        )
        .await;

        Ok(Self {
            inner: Arc::new(GenericStoreInner {
                indexer,
                device,
                _region_manager: region_manager,
                flushers,
                reclaimers,
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

    fn enqueue(&self, entry: CacheEntry<K, V, DefaultCacheEventListener<K, V>, S>) -> EnqueueHandle {
        if !self.inner.active.load(Ordering::Relaxed) {
            let (tx, rx) = oneshot::channel();
            tx.send(Err(anyhow::anyhow!("cannot enqueue new entry after closed").into()))
                .unwrap();
            return EnqueueHandle::new(rx);
        }
        let sequence = self.inner.sequence.fetch_add(1, Ordering::Relaxed);
        let rx = self.inner.flushers[sequence as usize % self.inner.flushers.len()].submit(entry, sequence);
        EnqueueHandle::new(rx)
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

        let (_, k, v) = EntryDeserializer::deserialize::<K, V>(&buffer)?;
        if k.borrow() != key {
            return Ok(None);
        }

        Ok(Some(v))
    }

    fn remove<Q>(&self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Send + Sync + 'static,
    {
        let hash = self.inner.hash_builder.hash_one(key);
        self.inner.indexer.remove(hash);
    }

    async fn delete<Q>(&self, key: &Q) -> Result<()>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Send + Sync + 'static,
    {
        let hash = self.inner.hash_builder.hash_one(key);
        if let Some(addr) = self.inner.indexer.remove(hash) {
            let align = self.inner.device.align();
            let mut buf = IoBuffer::with_capacity_in(align, &IO_BUFFER_ALLOCATOR);
            buf.resize(align, 0);
            self.inner.device.write(buf, addr.region, addr.offset as _).await?;
        }
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
    type Config = GenericStoreConfig<S, D>;

    async fn open(config: Self::Config) -> Result<Self> {
        Self::open(config).await
    }

    async fn close(&self) -> Result<()> {
        self.close().await
    }

    fn enqueue(
        &self,
        entry: CacheEntry<Self::Key, Self::Value, DefaultCacheEventListener<Self::Key, Self::Value>, Self::BuildHasher>,
    ) -> EnqueueHandle {
        self.enqueue(entry)
    }

    async fn lookup<Q>(&self, key: &Q) -> Result<Option<Self::Value>>
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Send + Sync + 'static,
    {
        self.lookup(key).await
    }

    fn remove<Q>(&self, key: &Q)
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Send + Sync + 'static,
    {
        self.remove(key)
    }

    async fn delete<Q>(&self, key: &Q) -> Result<()>
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Send + Sync + 'static,
    {
        self.delete(key).await
    }
}

#[cfg(test)]
mod tests {
    use foyer_memory::{Cache, CacheBuilder, FifoConfig};

    use crate::large::{
        device::direct_fs::{DirectFsDevice, DirectFsDeviceConfig},
        picker::FifoPicker,
    };

    use super::*;

    const KB: usize = 1024;
    const CAPACITY: usize = 64 * KB;
    const FILE_SIZE: usize = 16 * KB;

    #[test_log::test(tokio::test)]
    async fn test_store() {
        let dir = tempfile::tempdir().unwrap();

        let memory: Cache<u64, Vec<u8>> = CacheBuilder::new(10)
            .with_eviction_config(FifoConfig::default())
            .build();

        let store: GenericStore<u64, Vec<u8>, _, DirectFsDevice> = GenericStore::open(GenericStoreConfig {
            device_config: DirectFsDeviceConfig {
                dir: dir.path().into(),
                capacity: CAPACITY,
                file_size: FILE_SIZE,
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
        })
        .await
        .unwrap();

        let e1 = memory.insert(1, vec![1; 7 * KB]);
        let e2 = memory.insert(2, vec![2; 7 * KB]);

        store.enqueue(e1).await.unwrap();
        store.enqueue(e2).await.unwrap();

        let v1 = store.lookup(&1).await.unwrap().unwrap();
        assert_eq!(v1, vec![1; 7 * KB]);
        let v2 = store.lookup(&2).await.unwrap().unwrap();
        assert_eq!(v2, vec![2; 7 * KB]);

        let e3 = memory.insert(3, vec![3; 7 * KB]);
        let e4 = memory.insert(4, vec![4; 7 * KB]);

        store.enqueue(e3).await.unwrap();
        store.enqueue(e4).await.unwrap();

        let v1 = store.lookup(&1).await.unwrap().unwrap();
        assert_eq!(v1, vec![1; 7 * KB]);
        let v2 = store.lookup(&2).await.unwrap().unwrap();
        assert_eq!(v2, vec![2; 7 * KB]);
        let v3 = store.lookup(&3).await.unwrap().unwrap();
        assert_eq!(v3, vec![3; 7 * KB]);
        let v4 = store.lookup(&4).await.unwrap().unwrap();
        assert_eq!(v4, vec![4; 7 * KB]);
    }
}
