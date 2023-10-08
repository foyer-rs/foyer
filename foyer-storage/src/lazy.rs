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

use std::{
    marker::PhantomData,
    sync::{Arc, OnceLock},
};

use crate::{
    error::Result,
    storage::{Storage, StorageWriter},
};
use foyer_common::code::{Key, Value};
use futures::Future;

#[derive(Debug)]
pub struct NoneStoreWriter<K: Key, V: Value>(PhantomData<(K, V)>);

impl<K: Key, V: Value> StorageWriter for NoneStoreWriter<K, V> {
    type Key = K;
    type Value = V;

    fn judge(&mut self) -> bool {
        false
    }

    async fn finish(self, _: Self::Value) -> Result<bool> {
        Ok(false)
    }
}

#[derive(Debug)]
pub struct NoneStore<K: Key, V: Value>(PhantomData<(K, V)>);

impl<K: Key, V: Value> Clone for NoneStore<K, V> {
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

impl<K: Key, V: Value> Storage for NoneStore<K, V> {
    type Key = K;
    type Value = V;
    type Config = ();
    type Writer = NoneStoreWriter<K, V>;

    #[expect(clippy::let_unit_value)]
    async fn open(_: Self::Config) -> Result<Self> {
        Ok(NoneStore(PhantomData))
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }

    fn writer(&self, _: Self::Key, _: usize) -> Self::Writer {
        NoneStoreWriter(PhantomData)
    }

    fn exists(&self, _: &Self::Key) -> Result<bool> {
        Ok(false)
    }

    async fn lookup(&self, _: &Self::Key) -> Result<Option<Self::Value>> {
        Ok(None)
    }

    fn remove(&self, _: &Self::Key) -> Result<bool> {
        Ok(false)
    }

    fn clear(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
pub enum LazyStoreWriter<K, V, S>
where
    K: Key,
    V: Value,
    S: Storage<Key = K, Value = V>,
{
    Store { writer: S::Writer },
    None { writer: NoneStoreWriter<K, V> },
}

impl<K, V, S> StorageWriter for LazyStoreWriter<K, V, S>
where
    K: Key,
    V: Value,
    S: Storage<Key = K, Value = V>,
{
    type Key = K;
    type Value = V;

    fn judge(&mut self) -> bool {
        match self {
            LazyStoreWriter::Store { writer } => writer.judge(),
            LazyStoreWriter::None { writer } => writer.judge(),
        }
    }

    async fn finish(self, value: Self::Value) -> Result<bool> {
        match self {
            LazyStoreWriter::Store { writer } => writer.finish(value).await,
            LazyStoreWriter::None { writer } => writer.finish(value).await,
        }
    }
}

#[derive(Debug)]
pub struct LazyStore<K, V, S>
where
    K: Key,
    V: Value,
    S: Storage<Key = K, Value = V>,
{
    once: Arc<OnceLock<S>>,
    none: NoneStore<K, V>,
}

impl<K, V, S> Clone for LazyStore<K, V, S>
where
    K: Key,
    V: Value,
    S: Storage<Key = K, Value = V>,
{
    fn clone(&self) -> Self {
        Self {
            once: Arc::clone(&self.once),
            none: NoneStore(PhantomData),
        }
    }
}

impl<K, V, S> LazyStore<K, V, S>
where
    K: Key,
    V: Value,
    S: Storage<Key = K, Value = V>,
{
    fn with_task(config: S::Config) -> (Self, impl Future<Output = Result<S>> + Send) {
        let once = Arc::new(OnceLock::new());

        let task = {
            let once = once.clone();
            async move {
                let store = match S::open(config).await {
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
            none: NoneStore(PhantomData),
        };

        (res, task)
    }
}

impl<K, V, S> Storage for LazyStore<K, V, S>
where
    K: Key,
    V: Value,
    S: Storage<Key = K, Value = V>,
{
    type Key = K;
    type Value = V;
    type Config = S::Config;
    type Writer = LazyStoreWriter<K, V, S>;

    async fn open(config: S::Config) -> Result<Self> {
        let (store, task) = Self::with_task(config);
        tokio::spawn(task);
        Ok(store)
    }

    async fn close(&self) -> Result<()> {
        match self.once.get() {
            Some(store) => store.close().await,
            None => self.none.close().await,
        }
    }

    fn writer(&self, key: Self::Key, weight: usize) -> Self::Writer {
        match self.once.get() {
            Some(store) => LazyStoreWriter::Store {
                writer: store.writer(key, weight),
            },
            None => LazyStoreWriter::None {
                writer: NoneStoreWriter(PhantomData),
            },
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

    use crate::{
        device::fs::FsDeviceConfig,
        storage::StorageExt,
        store::{FifoFsStoreConfig, Store},
    };

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

        let (store, task) = LazyStore::<_, _, Store<_, _>>::with_task(config.into());

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

        let (store, task) = LazyStore::<_, _, Store<_, _>>::with_task(config.into());

        assert!(store.lookup(&100).await.unwrap().is_none());

        tokio::spawn(task).await.unwrap().unwrap();

        assert_eq!(store.lookup(&100).await.unwrap(), Some(100));
    }
}
