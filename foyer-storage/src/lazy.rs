//  Copyright 2023 MrCroxx
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

use std::sync::{Arc, OnceLock};

use crate::{
    error::Result,
    storage::Storage,
    store::{Store, StoreConfig, StoreWriter},
};
use foyer_common::code::{Key, Value};
use futures::Future;

#[derive(Debug)]
pub struct LazyStore<K, V>
where
    K: Key,
    V: Value,
{
    once: Arc<OnceLock<Store<K, V>>>,
    none: Store<K, V>,
}

impl<K, V> Clone for LazyStore<K, V>
where
    K: Key,
    V: Value,
{
    fn clone(&self) -> Self {
        Self {
            once: Arc::clone(&self.once),
            none: Store::None,
        }
    }
}

impl<K, V> LazyStore<K, V>
where
    K: Key,
    V: Value,
{
    pub fn lazy(config: StoreConfig<K, V>) -> Self {
        let (res, task) = Self::lazy_with_task(config);
        tokio::spawn(task);
        res
    }

    pub fn lazy_with_task(
        config: StoreConfig<K, V>,
    ) -> (Self, impl Future<Output = Result<Store<K, V>>> + Send) {
        let once = Arc::new(OnceLock::new());

        let task = {
            let once = once.clone();
            async move {
                let store = match Store::open(config).await {
                    Ok(store) => store,
                    Err(e) => {
                        tracing::error!("Lazy open store fail: {}", e);
                        return Err(e);
                    }
                };
                once.set(store.clone()).unwrap();
                Ok(store)
            }
        };

        let res = Self {
            once,
            none: Store::None,
        };

        (res, task)
    }
}

impl<K, V> Storage for LazyStore<K, V>
where
    K: Key,
    V: Value,
{
    type Key = K;
    type Value = V;
    type Config = StoreConfig<K, V>;
    type Owned = Self;
    type Writer<'a> = StoreWriter<'a, K, V>;

    async fn open(config: Self::Config) -> Result<Self::Owned> {
        let once = Arc::new(OnceLock::new());
        let store = Store::open(config).await?;
        once.set(store).unwrap();
        Ok(Self {
            once,
            none: Store::None,
        })
    }

    async fn close(&self) -> Result<()> {
        match self.once.get() {
            Some(store) => store.close().await,
            None => self.none.close().await,
        }
    }

    fn writer(&self, key: Self::Key, weight: usize) -> Self::Writer<'_> {
        match self.once.get() {
            Some(store) => store.writer(key, weight),
            None => self.none.writer(key, weight),
        }
    }

    fn exists(&self, key: &Self::Key) -> Result<bool> {
        match self.once.get() {
            Some(store) => store.exists(key),
            None => self.none.exists(key),
        }
    }

    async fn lookup(&self, key: &Self::Key) -> Result<Option<Self::Value>> {
        match self.once.get() {
            Some(store) => store.lookup(key).await,
            None => self.none.lookup(key).await,
        }
    }

    fn remove(&self, key: &Self::Key) -> Result<bool> {
        match self.once.get() {
            Some(store) => store.remove(key),
            None => self.none.remove(key),
        }
    }

    fn clear(&self) -> Result<()> {
        match self.once.get() {
            Some(store) => store.clear(),
            None => self.none.clear(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, time::Duration};

    use foyer_intrusive::eviction::fifo::FifoConfig;

    use crate::{device::fs::FsDeviceConfig, storage::StorageExt, store::FifoFsStoreConfig};

    use super::*;

    const KB: usize = 1024;
    const MB: usize = 1024 * 1024;

    #[tokio::test]
    async fn test_lazy_store() {
        let tempdir = tempfile::tempdir().unwrap();

        let config = FifoFsStoreConfig {
            name: "".to_string(),
            eviction_config: FifoConfig,
            device_config: FsDeviceConfig {
                dir: PathBuf::from(tempdir.path()),
                capacity: 16 * MB,
                file_capacity: 4 * MB,
                align: 4096,
                io_size: 4096 * KB,
            },
            allocator_bits: 1,
            admissions: vec![],
            reinsertions: vec![],
            buffer_pool_size: 8 * MB,
            flushers: 1,
            flush_rate_limit: 0,
            reclaimers: 1,
            reclaim_rate_limit: 0,
            recover_concurrency: 2,
            allocation_timeout: Duration::from_millis(10),
            clean_region_threshold: 1,
        };

        let (store, task) = LazyStore::lazy_with_task(config.into());

        assert!(!store.insert(100, 100).await.unwrap());

        tokio::spawn(task).await.unwrap().unwrap();

        assert!(store.insert(100, 100).await.unwrap());
        assert_eq!(store.lookup(&100).await.unwrap(), Some(100));

        store.close().await.unwrap();
        drop(store);

        let config = FifoFsStoreConfig {
            name: "".to_string(),
            eviction_config: FifoConfig,
            device_config: FsDeviceConfig {
                dir: PathBuf::from(tempdir.path()),
                capacity: 16 * MB,
                file_capacity: 4 * MB,
                align: 4096,
                io_size: 4096 * KB,
            },
            allocator_bits: 1,
            admissions: vec![],
            reinsertions: vec![],
            buffer_pool_size: 8 * MB,
            flushers: 1,
            flush_rate_limit: 0,
            reclaimers: 1,
            reclaim_rate_limit: 0,
            recover_concurrency: 2,
            allocation_timeout: Duration::from_millis(10),
            clean_region_threshold: 1,
        };

        let (store, task) = LazyStore::lazy_with_task(config.into());

        assert!(store.lookup(&100).await.unwrap().is_none());

        tokio::spawn(task).await.unwrap().unwrap();

        assert_eq!(store.lookup(&100).await.unwrap(), Some(100));
    }
}
