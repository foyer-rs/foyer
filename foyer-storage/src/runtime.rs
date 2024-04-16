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

use std::sync::Arc;

use foyer_common::runtime::BackgroundShutdownRuntime;

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
pub struct RuntimeStorageConfig<S: Storage> {
    pub store: S::Config,
    pub runtime: RuntimeConfig,
}

#[derive(Debug)]
pub struct RuntimeStorageWriter<S: Storage> {
    runtime: Arc<BackgroundShutdownRuntime>,
    writer: S::Writer,
}

impl<S: Storage> StorageWriter for RuntimeStorageWriter<S> {
    type Key = S::Key;
    type Value = S::Value;

    fn key(&self) -> &Self::Key {
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

    async fn finish(self, value: Self::Value) -> Result<bool> {
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
pub struct RuntimeStorage<S: Storage> {
    runtime: Arc<BackgroundShutdownRuntime>,
    store: S,
}

impl<S: Storage> Clone for RuntimeStorage<S> {
    fn clone(&self) -> Self {
        Self {
            runtime: Arc::clone(&self.runtime),
            store: self.store.clone(),
        }
    }
}

impl<S: Storage> Storage for RuntimeStorage<S> {
    type Key = S::Key;
    type Value = S::Value;
    type Config = RuntimeStorageConfig<S>;
    type Writer = RuntimeStorageWriter<S>;

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
        Ok(Self { runtime, store })
    }

    fn is_ready(&self) -> bool {
        self.store.is_ready()
    }

    async fn close(&self) -> Result<()> {
        let store = self.store.clone();
        self.runtime.spawn(async move { store.close().await }).await.unwrap()
    }

    fn writer(&self, key: Self::Key, weight: usize) -> Self::Writer {
        let writer = self.store.writer(key, weight);
        RuntimeStorageWriter {
            runtime: self.runtime.clone(),
            writer,
        }
    }

    fn exists(&self, key: &Self::Key) -> crate::error::Result<bool> {
        self.store.exists(key)
    }

    async fn lookup(&self, key: &Self::Key) -> Result<Option<Self::Value>> {
        let store = self.store.clone();
        let key = key.clone();
        self.runtime
            .spawn(async move { store.lookup(&key).await })
            .await
            .unwrap()
    }

    fn remove(&self, key: &Self::Key) -> crate::error::Result<bool> {
        self.store.remove(key)
    }

    fn clear(&self) -> crate::error::Result<()> {
        self.store.clear()
    }
}

pub type RuntimeStore<K, V> = RuntimeStorage<Store<K, V>>;
pub type RuntimeStoreWriter<K, V> = RuntimeStorageWriter<Store<K, V>>;
pub type RuntimeStoreConfig<K, V> = RuntimeStorageConfig<Store<K, V>>;

pub type RuntimeLazyStore<K, V> = RuntimeStorage<LazyStore<K, V>>;
pub type RuntimeLazyStoreWriter<K, V> = RuntimeStorageWriter<LazyStore<K, V>>;
pub type RuntimeLazyStoreConfig<K, V> = RuntimeStorageConfig<LazyStore<K, V>>;
