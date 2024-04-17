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

use std::{marker::PhantomData, sync::Arc};

use foyer_common::{
    code::{StorageKey, StorageValue},
    runtime::BackgroundShutdownRuntime,
};

use crate::{
    compress::Compression,
    error::Result,
    lazy::LazyStore,
    storage::{Storage, StorageWriter},
    store::Store,
};

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub worker_threads: Option<usize>,
    pub thread_name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RuntimeStorageConfig<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: Storage<K, V>,
{
    pub store: S::Config,
    pub runtime: RuntimeConfig,
}

#[derive(Debug)]
pub struct RuntimeStorageWriter<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: Storage<K, V>,
{
    runtime: Arc<BackgroundShutdownRuntime>,
    writer: S::Writer,
}

impl<K, V, S> StorageWriter<K, V> for RuntimeStorageWriter<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: Storage<K, V>,
{
    fn key(&self) -> &K {
        self.writer.key()
    }

    fn weight(&self) -> usize {
        self.writer.weight()
    }

    fn judge(&mut self) -> bool {
        self.writer.judge()
    }

    fn force(&mut self) {
        self.writer.force()
    }

    async fn finish(self, value: V) -> Result<bool> {
        self.runtime
            .spawn(async move { self.writer.finish(value).await })
            .await
            .unwrap()
    }

    fn compression(&self) -> Compression {
        self.writer.compression()
    }

    fn set_compression(&mut self, compression: Compression) {
        self.writer.set_compression(compression)
    }
}

#[derive(Debug)]
pub struct RuntimeStorage<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: Storage<K, V>,
{
    runtime: Arc<BackgroundShutdownRuntime>,
    store: S,
    _marker: PhantomData<(K, V)>,
}

impl<K, V, S> Clone for RuntimeStorage<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: Storage<K, V>,
{
    fn clone(&self) -> Self {
        Self {
            runtime: Arc::clone(&self.runtime),
            store: self.store.clone(),
            _marker: PhantomData,
        }
    }
}

impl<K, V, S> Storage<K, V> for RuntimeStorage<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: Storage<K, V>,
{
    type Config = RuntimeStorageConfig<K, V, S>;
    type Writer = RuntimeStorageWriter<K, V, S>;

    async fn open(config: Self::Config) -> Result<Self> {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        if let Some(worker_threads) = config.runtime.worker_threads {
            builder.worker_threads(worker_threads);
        }
        if let Some(thread_name) = config.runtime.thread_name {
            builder.thread_name(thread_name);
        }
        let runtime = builder.enable_all().build().map_err(anyhow::Error::from)?;
        let runtime = BackgroundShutdownRuntime::from(runtime);
        let runtime = Arc::new(runtime);
        let store = runtime
            .spawn(async move { S::open(config.store).await })
            .await
            .unwrap()?;
        Ok(Self {
            runtime,
            store,
            _marker: PhantomData,
        })
    }

    fn is_ready(&self) -> bool {
        self.store.is_ready()
    }

    async fn close(&self) -> Result<()> {
        let store = self.store.clone();
        self.runtime.spawn(async move { store.close().await }).await.unwrap()
    }

    fn writer(&self, key: K, weight: usize) -> Self::Writer {
        let writer = self.store.writer(key, weight);
        RuntimeStorageWriter {
            runtime: self.runtime.clone(),
            writer,
        }
    }

    fn exists(&self, key: &K) -> crate::error::Result<bool> {
        self.store.exists(key)
    }

    async fn lookup(&self, key: &K) -> Result<Option<V>> {
        let store = self.store.clone();
        let key = key.clone();
        self.runtime
            .spawn(async move { store.lookup(&key).await })
            .await
            .unwrap()
    }

    fn remove(&self, key: &K) -> crate::error::Result<bool> {
        self.store.remove(key)
    }

    fn clear(&self) -> crate::error::Result<()> {
        self.store.clear()
    }
}

pub type RuntimeStore<K, V> = RuntimeStorage<K, V, Store<K, V>>;
pub type RuntimeStoreWriter<K, V> = RuntimeStorageWriter<K, V, Store<K, V>>;
pub type RuntimeStoreConfig<K, V> = RuntimeStorageConfig<K, V, Store<K, V>>;

pub type RuntimeLazyStore<K, V> = RuntimeStorage<K, V, LazyStore<K, V>>;
pub type RuntimeLazyStoreWriter<K, V> = RuntimeStorageWriter<K, V, LazyStore<K, V>>;
pub type RuntimeLazyStoreConfig<K, V> = RuntimeStorageConfig<K, V, LazyStore<K, V>>;
