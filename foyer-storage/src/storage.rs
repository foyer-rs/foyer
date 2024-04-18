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

use std::{borrow::Borrow, fmt::Debug, hash::Hash};

use foyer_common::code::{StorageKey, StorageValue};
use futures::Future;

use crate::{compress::Compression, error::Result};

// TODO(MrCroxx): Use `trait_alias` after stable.
// pub trait FetchValueFuture<V> = Future<Output = anyhow::Result<V>> + Send + 'static;

pub trait FetchValueFuture<V>: Future<Output = anyhow::Result<V>> + Send + 'static {}
impl<V, T: Future<Output = anyhow::Result<V>> + Send + 'static> FetchValueFuture<V> for T {}

pub trait StorageWriter<K, V>: Send + Sync + Debug
where
    K: StorageKey,
    V: StorageValue,
{
    fn key(&self) -> &K;

    fn judge(&mut self) -> bool;

    fn force(&mut self);

    fn compression(&self) -> Compression;

    fn set_compression(&mut self, compression: Compression);

    fn finish(self, value: V) -> impl Future<Output = Result<bool>> + Send;
}

pub trait Storage<K, V>: Send + Sync + Debug + Clone + 'static
where
    K: StorageKey,
    V: StorageValue,
{
    type Config: Send + Clone + Debug;
    type Writer: StorageWriter<K, V>;

    #[must_use]
    fn open(config: Self::Config) -> impl Future<Output = Result<Self>> + Send;

    fn is_ready(&self) -> bool;

    #[must_use]
    fn close(&self) -> impl Future<Output = Result<()>> + Send;

    fn writer(&self, key: K) -> Self::Writer;

    fn exists<Q>(&self, key: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized;

    #[must_use]
    fn lookup<Q>(&self, key: &Q) -> impl Future<Output = Result<Option<V>>> + Send
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Send + Sync + Clone + 'static;

    fn remove<Q>(&self, key: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized;

    fn clear(&self) -> Result<()>;
}

pub trait StorageExt<K, V>: Storage<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    #[must_use]
    #[tracing::instrument(skip(self, value))]
    fn insert(&self, key: K, value: V) -> impl Future<Output = Result<bool>> + Send {
        self.writer(key).finish(value)
    }

    #[must_use]
    #[tracing::instrument(skip(self, value))]
    fn insert_if_not_exists(&self, key: K, value: V) -> impl Future<Output = Result<bool>> + Send {
        async move {
            if self.exists(&key)? {
                return Ok(false);
            }
            self.insert(key, value).await
        }
    }

    /// First judge if the entry will be admitted with `key` and `weight` by admission policies.
    /// Then `f` will be called and entry will be inserted.
    ///
    /// # Safety
    ///
    /// `weight` MUST be equal to `key.serialized_len() + value.serialized_len()`
    #[must_use]
    #[tracing::instrument(skip(self, f))]
    fn insert_with<F>(&self, key: K, f: F) -> impl Future<Output = Result<bool>> + Send
    where
        F: FnOnce() -> anyhow::Result<V> + Send,
    {
        async move {
            let mut writer = self.writer(key);
            if !writer.judge() {
                return Ok(false);
            }
            let value = match f() {
                Ok(value) => value,
                Err(e) => {
                    tracing::warn!("fetch value error: {:?}", e);
                    return Ok(false);
                }
            };
            writer.finish(value).await
        }
    }

    /// First judge if the entry will be admitted with `key` and `weight` by admission policies.
    /// Then `f` will be called to fetch value, and entry will be inserted.
    ///
    /// # Safety
    ///
    /// `weight` MUST be equal to `key.serialized_len() + value.serialized_len()`
    #[tracing::instrument(skip(self, f))]
    fn insert_with_future<F, FU>(&self, key: K, f: F, weight: usize) -> impl Future<Output = Result<bool>> + Send
    where
        F: FnOnce() -> FU + Send,
        FU: FetchValueFuture<V>,
    {
        async move {
            let mut writer = self.writer(key);
            if !writer.judge() {
                return Ok(false);
            }
            let value = match f().await {
                Ok(value) => value,
                Err(e) => {
                    tracing::warn!("fetch value error: {:?}", e);
                    return Ok(false);
                }
            };
            writer.finish(value).await
        }
    }

    #[tracing::instrument(skip(self, f))]
    fn insert_if_not_exists_with<F>(&self, key: K, f: F, weight: usize) -> impl Future<Output = Result<bool>> + Send
    where
        F: FnOnce() -> anyhow::Result<V> + Send,
    {
        async move {
            if self.exists(&key)? {
                return Ok(false);
            }
            self.insert_with(key, f).await
        }
    }

    #[tracing::instrument(skip(self, f))]
    fn insert_if_not_exists_with_future<F, FU>(
        &self,
        key: K,
        f: F,
        weight: usize,
    ) -> impl Future<Output = Result<bool>> + Send
    where
        F: FnOnce() -> FU + Send,
        FU: FetchValueFuture<V>,
    {
        async move {
            if self.exists(&key)? {
                return Ok(false);
            }
            self.insert_with_future(key, f, weight).await
        }
    }
}

impl<K, V, S> StorageExt<K, V> for S
where
    K: StorageKey,
    V: StorageValue,
    S: Storage<K, V>,
{
}

pub trait AsyncStorageExt<K, V>: Storage<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    #[tracing::instrument(skip(self, value))]
    fn insert_async(&self, key: K, value: V) {
        let store = self.clone();
        tokio::spawn(async move {
            if let Err(e) = store.insert(key, value).await {
                tracing::warn!("async storage insert error: {}", e);
            }
        });
    }

    #[tracing::instrument(skip(self, value))]
    fn insert_if_not_exists_async(&self, key: K, value: V) {
        let store = self.clone();
        tokio::spawn(async move {
            if let Err(e) = store.insert_if_not_exists(key, value).await {
                tracing::warn!("async storage insert error: {}", e);
            }
        });
    }

    fn insert_async_with_callback<F, FU>(&self, key: K, value: V, f: F)
    where
        F: FnOnce(Result<bool>) -> FU + Send + 'static,
        FU: Future<Output = ()> + Send + 'static,
    {
        let store = self.clone();
        tokio::spawn(async move {
            let res = store.insert(key, value).await;
            let future = f(res);
            future.await;
        });
    }

    fn insert_if_not_exists_async_with_callback<F, FU>(&self, key: K, value: V, f: F)
    where
        F: FnOnce(Result<bool>) -> FU + Send + 'static,
        FU: Future<Output = ()> + Send + 'static,
    {
        let store = self.clone();
        tokio::spawn(async move {
            let res = store.insert_if_not_exists(key, value).await;
            let future = f(res);
            future.await;
        });
    }
}

impl<K, V, S> AsyncStorageExt<K, V> for S
where
    K: StorageKey,
    V: StorageValue,
    S: Storage<K, V>,
{
}

pub trait ForceStorageExt<K, V>: Storage<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    #[tracing::instrument(skip(self, value))]
    fn insert_force(&self, key: K, value: V) -> impl Future<Output = Result<bool>> + Send {
        let mut writer = self.writer(key);
        writer.force();
        writer.finish(value)
    }

    /// First judge if the entry will be admitted with `key` and `weight` by admission policies.
    /// Then `f` will be called and entry will be inserted.
    ///
    /// # Safety
    ///
    /// `weight` MUST be equal to `key.serialized_len() + value.serialized_len()`
    #[tracing::instrument(skip(self, f))]
    fn insert_force_with<F>(&self, key: K, f: F) -> impl Future<Output = Result<bool>> + Send
    where
        F: FnOnce() -> anyhow::Result<V> + Send,
    {
        async move {
            let mut writer = self.writer(key);
            writer.force();
            if !writer.judge() {
                return Ok(false);
            }
            let value = match f() {
                Ok(value) => value,
                Err(e) => {
                    tracing::warn!("fetch value error: {:?}", e);
                    return Ok(false);
                }
            };
            let inserted = writer.finish(value).await?;
            Ok(inserted)
        }
    }

    /// First judge if the entry will be admitted with `key` and `weight` by admission policies.
    /// Then `f` will be called to fetch value, and entry will be inserted.
    ///
    /// # Safety
    ///
    /// `weight` MUST be equal to `key.serialized_len() + value.serialized_len()`
    #[tracing::instrument(skip(self, f))]
    fn insert_force_with_future<F, FU>(&self, key: K, f: F) -> impl Future<Output = Result<bool>> + Send
    where
        F: FnOnce() -> FU + Send,
        FU: FetchValueFuture<V>,
    {
        async move {
            let mut writer = self.writer(key);
            writer.force();
            if !writer.judge() {
                return Ok(false);
            }
            let value = match f().await {
                Ok(value) => value,
                Err(e) => {
                    tracing::warn!("fetch value error: {:?}", e);
                    return Ok(false);
                }
            };
            let inserted = writer.finish(value).await?;
            Ok(inserted)
        }
    }
}

impl<K, V, S> ForceStorageExt<K, V> for S
where
    K: StorageKey,
    V: StorageValue,
    S: Storage<K, V>,
{
}

#[cfg(test)]
mod tests {
    //! storage interface test

    use std::{path::Path, sync::Arc, time::Duration};

    use foyer_memory::{EvictionConfig, FifoConfig};
    use tokio::sync::Barrier;

    use super::*;
    use crate::{
        device::fs::FsDeviceConfig,
        store::{FsStore, FsStoreConfig},
    };

    const KB: usize = 1024;
    const MB: usize = 1024 * 1024;

    fn config_for_test(dir: impl AsRef<Path>) -> FsStoreConfig<u64, Vec<u8>> {
        FsStoreConfig {
            name: "".to_string(),
            eviction_config: EvictionConfig::Fifo(FifoConfig {}),
            device_config: FsDeviceConfig {
                dir: dir.as_ref().into(),
                capacity: 4 * MB,
                file_size: MB,
                align: 4 * KB,
                io_size: 4 * KB,
            },
            catalog_bits: 1,
            admissions: vec![],
            reinsertions: vec![],
            flushers: 1,
            reclaimers: 1,
            clean_region_threshold: 1,
            recover_concurrency: 2,
            compression: Compression::None,
        }
    }

    #[tokio::test]
    async fn test_storage() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = config_for_test(tempdir.path());

        let storage = FsStore::open(config).await.unwrap();
        assert!(storage.is_ready());

        assert!(!storage.exists(&1).unwrap());

        let mut writer = storage.writer(1);
        assert_eq!(writer.key(), &1);
        assert!(writer.judge());
        assert_eq!(writer.compression(), Compression::None);
        writer.set_compression(Compression::Lz4);
        assert_eq!(writer.compression(), Compression::Lz4);
        writer.force();
        assert!(writer.finish(vec![b'x'; KB]).await.unwrap());

        assert!(storage.exists(&1).unwrap());
        assert_eq!(storage.lookup(&1).await.unwrap().unwrap(), vec![b'x'; KB]);

        assert!(storage.remove(&1).unwrap());
        assert!(!storage.exists(&1).unwrap());
        assert!(!storage.remove(&1).unwrap());

        storage.clear().unwrap();
        storage.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_storage_ext() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = config_for_test(tempdir.path());

        let storage = FsStore::open(config).await.unwrap();

        assert!(storage.insert(1, vec![b'x'; KB]).await.unwrap());
        assert!(storage.exists(&1).unwrap());

        assert!(!storage.insert_if_not_exists(1, vec![b'x'; KB]).await.unwrap());
        assert!(storage.insert_if_not_exists(2, vec![b'x'; KB]).await.unwrap());
        assert!(storage.exists(&2).unwrap());

        assert!(storage.insert_with(3, || { Ok(vec![b'x'; KB]) },).await.unwrap());
        assert!(storage.exists(&3).unwrap());

        assert!(storage
            .insert_with_future(4, || { async move { Ok(vec![b'x'; KB]) } }, KB)
            .await
            .unwrap());
        assert!(storage.exists(&4).unwrap());

        assert!(!storage
            .insert_if_not_exists_with(4, || { Ok(vec![b'x'; KB]) }, KB)
            .await
            .unwrap());
        assert!(storage
            .insert_if_not_exists_with(5, || { Ok(vec![b'x'; KB]) }, KB)
            .await
            .unwrap());
        assert!(storage.exists(&5).unwrap());

        assert!(!storage
            .insert_if_not_exists_with_future(5, || { async move { Ok(vec![b'x'; KB]) } }, KB)
            .await
            .unwrap());
        assert!(storage
            .insert_if_not_exists_with_future(6, || { async move { Ok(vec![b'x'; KB]) } }, KB)
            .await
            .unwrap());
        assert!(storage.exists(&6).unwrap());
    }

    async fn exists_with_retry(storage: &impl Storage<u64, Vec<u8>>, key: &u64) -> bool {
        tokio::time::sleep(Duration::from_millis(1)).await;
        for _ in 0..10 {
            if storage.exists(key).unwrap() {
                return true;
            };
            tokio::time::sleep(Duration::from_millis(10)).await
        }
        false
    }

    #[tokio::test]
    async fn test_async_storage_ext() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = config_for_test(tempdir.path());

        let storage = FsStore::open(config).await.unwrap();

        storage.insert_async(1, vec![b'x'; KB]);
        assert!(exists_with_retry(&storage, &1).await);

        storage.insert_if_not_exists_async(2, vec![b'x'; KB]);
        assert!(exists_with_retry(&storage, &2).await);

        let barrier = Arc::new(Barrier::new(2));
        let b = barrier.clone();
        storage.insert_async_with_callback(3, vec![b'x'; KB], |res| async move {
            assert!(res.unwrap());
            b.wait().await;
        });
        barrier.wait().await;

        storage.insert_if_not_exists_async_with_callback(3, vec![b'x'; KB], |res| async move {
            assert!(!res.unwrap());
        });
        storage.insert_if_not_exists_async_with_callback(4, vec![b'x'; KB], |res| async move {
            assert!(res.unwrap());
        });
    }
}
