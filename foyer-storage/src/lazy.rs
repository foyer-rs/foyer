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
    borrow::Borrow,
    hash::Hash,
    sync::{Arc, OnceLock},
};

use foyer_common::code::{StorageKey, StorageValue};
use tokio::task::JoinHandle;

use crate::{
    compress::Compression,
    error::Result,
    none::{NoneStore, NoneStoreWriter},
    storage::{CachedEntry, Storage, StorageWriter},
};

#[derive(Debug)]
pub enum LazyStoreWriter<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: Storage<K, V>,
{
    Store { writer: S::Writer },
    None { writer: NoneStoreWriter<K, V> },
}

impl<K, V, S> StorageWriter<K, V> for LazyStoreWriter<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: Storage<K, V>,
{
    fn key(&self) -> &K {
        match self {
            LazyStoreWriter::Store { writer } => writer.key(),
            LazyStoreWriter::None { writer } => writer.key(),
        }
    }

    fn judge(&mut self) -> bool {
        match self {
            LazyStoreWriter::Store { writer } => writer.judge(),
            LazyStoreWriter::None { writer } => writer.judge(),
        }
    }

    fn force(&mut self) {
        match self {
            LazyStoreWriter::Store { writer } => writer.force(),
            LazyStoreWriter::None { writer } => writer.force(),
        }
    }

    async fn finish(self, value: V) -> Result<Option<CachedEntry<K, V>>> {
        match self {
            LazyStoreWriter::Store { writer } => writer.finish(value).await,
            LazyStoreWriter::None { writer } => writer.finish(value).await,
        }
    }

    fn compression(&self) -> Compression {
        match self {
            LazyStoreWriter::Store { writer } => writer.compression(),
            LazyStoreWriter::None { writer } => writer.compression(),
        }
    }

    fn set_compression(&mut self, compression: Compression) {
        match self {
            LazyStoreWriter::Store { writer } => writer.set_compression(compression),
            LazyStoreWriter::None { writer } => writer.set_compression(compression),
        }
    }
}

#[derive(Debug)]
pub struct Lazy<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: Storage<K, V>,
{
    once: Arc<OnceLock<S>>,
    none: NoneStore<K, V>,
}

impl<K, V, S> Clone for Lazy<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: Storage<K, V>,
{
    fn clone(&self) -> Self {
        Self {
            once: Arc::clone(&self.once),
            none: NoneStore::default(),
        }
    }
}

impl<K, V, S> Lazy<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: Storage<K, V>,
{
    fn with_handle(config: S::Config) -> (Self, JoinHandle<Result<S>>) {
        let once = Arc::new(OnceLock::new());

        let handle = tokio::spawn({
            let once = once.clone();
            async move {
                let store = match S::open(config).await {
                    Ok(store) => store,
                    Err(e) => {
                        tracing::error!("Lazy open store fail: {}", e);
                        return Err(e);
                    }
                };
                if once.set(store.clone()).is_err() {
                    panic!("Lazy store has been initialized before.");
                }
                Ok(store)
            }
        });

        let res = Self {
            once,
            none: NoneStore::default(),
        };

        (res, handle)
    }
}

impl<K, V, S> Storage<K, V> for Lazy<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: Storage<K, V>,
{
    type Config = S::Config;
    type Writer = LazyStoreWriter<K, V, S>;

    async fn open(config: S::Config) -> Result<Self> {
        let (store, task) = Self::with_handle(config);
        tokio::spawn(task);
        Ok(store)
    }

    fn is_ready(&self) -> bool {
        self.once.get().is_some()
    }

    async fn close(&self) -> Result<()> {
        match self.once.get() {
            Some(store) => store.close().await,
            None => self.none.close().await,
        }
    }

    fn writer(&self, key: K) -> Self::Writer {
        match self.once.get() {
            Some(store) => LazyStoreWriter::Store {
                writer: store.writer(key),
            },
            None => LazyStoreWriter::None {
                writer: NoneStoreWriter::new(key),
            },
        }
    }

    fn exists<Q>(&self, key: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        match self.once.get() {
            Some(store) => store.exists(key),
            None => self.none.exists(key),
        }
    }

    async fn lookup<Q>(&self, key: &Q) -> Result<Option<CachedEntry<K, V>>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Send + Sync + Clone + 'static,
    {
        match self.once.get() {
            Some(store) => store.lookup(key).await,
            None => self.none.lookup(key).await,
        }
    }

    fn remove<Q>(&self, key: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
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
    use std::path::PathBuf;

    use foyer_memory::FifoConfig;

    use super::*;
    use crate::{
        device::fs::FsDeviceConfig,
        storage::StorageExt,
        store::{FsStore, FsStoreConfig},
    };

    const KB: usize = 1024;
    const MB: usize = 1024 * 1024;

    #[tokio::test]
    async fn test_lazy_store() {
        let tempdir = tempfile::tempdir().unwrap();

        let config = FsStoreConfig {
            name: "".to_string(),
            eviction_config: FifoConfig {}.into(),
            device_config: FsDeviceConfig {
                dir: PathBuf::from(tempdir.path()),
                capacity: 16 * MB,
                file_size: 4 * MB,
                align: 4096,
                io_size: 4096 * KB,
            },
            catalog_shards: 1,
            admissions: vec![],
            reinsertions: vec![],
            flushers: 1,
            reclaimers: 1,
            recover_concurrency: 2,
            clean_region_threshold: 1,
            compression: crate::compress::Compression::None,
        };

        let (store, handle) = Lazy::<u64, u64, FsStore<_, _>>::with_handle(config);

        assert!(store.insert(100, 100).await.unwrap().is_none());

        handle.await.unwrap().unwrap();

        assert!(store.insert(100, 100).await.unwrap().is_some());
        assert_eq!(store.lookup(&100).await.unwrap().unwrap().value(), &100);

        store.close().await.unwrap();
        drop(store);

        let config = FsStoreConfig {
            name: "".to_string(),
            eviction_config: FifoConfig {}.into(),
            device_config: FsDeviceConfig {
                dir: PathBuf::from(tempdir.path()),
                capacity: 16 * MB,
                file_size: 4 * MB,
                align: 4096,
                io_size: 4096 * KB,
            },
            catalog_shards: 1,
            admissions: vec![],
            reinsertions: vec![],
            flushers: 1,
            reclaimers: 1,
            recover_concurrency: 2,
            clean_region_threshold: 1,
            compression: crate::compress::Compression::None,
        };

        let (store, handle) = Lazy::<u64, u64, FsStore<_, _>>::with_handle(config);

        assert!(store.lookup(&100).await.unwrap().is_none());

        handle.await.unwrap().unwrap();

        assert_eq!(store.lookup(&100).await.unwrap().unwrap().value(), &100);
        store.close().await.unwrap();
    }
}
