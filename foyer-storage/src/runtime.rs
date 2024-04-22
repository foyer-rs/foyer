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

use std::{borrow::Borrow, fmt::Debug, hash::Hash, marker::PhantomData, sync::Arc};

use foyer_common::{
    code::{StorageKey, StorageValue},
    runtime::BackgroundShutdownRuntime,
};

use crate::{
    compress::Compression,
    error::Result,
    storage::{CachedEntry, Storage, StorageWriter},
};

pub struct RuntimeConfigBuilder {
    worker_threads: Option<usize>,
    thread_name: String,
}

impl Default for RuntimeConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl RuntimeConfigBuilder {
    pub fn new() -> Self {
        Self {
            worker_threads: None,
            thread_name: "foyer".to_string(),
        }
    }

    pub fn with_worker_threads(mut self, worker_threads: usize) -> Self {
        self.worker_threads = Some(worker_threads);
        self
    }

    pub fn with_thread_name(mut self, thread_name: &str) -> Self {
        self.thread_name = thread_name.to_string();
        self
    }

    pub fn build(self) -> RuntimeConfig {
        RuntimeConfig {
            worker_threads: self.worker_threads,
            thread_name: self.thread_name,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    worker_threads: Option<usize>,
    thread_name: String,
}

pub struct RuntimeStoreConfig<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: Storage<K, V>,
{
    pub store_config: S::Config,
    pub runtime_config: RuntimeConfig,
}

impl<K, V, S> Debug for RuntimeStoreConfig<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: Storage<K, V>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeStoreConfig")
            .field("store_config", &self.store_config)
            .field("runtime_config", &self.runtime_config)
            .finish()
    }
}

impl<K, V, S> Clone for RuntimeStoreConfig<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: Storage<K, V>,
{
    fn clone(&self) -> Self {
        Self {
            store_config: self.store_config.clone(),
            runtime_config: self.runtime_config.clone(),
        }
    }
}

#[derive(Debug)]
pub struct RuntimeStoreWriter<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: Storage<K, V>,
{
    runtime: Arc<BackgroundShutdownRuntime>,
    writer: S::Writer,
}

impl<K, V, S> StorageWriter<K, V> for RuntimeStoreWriter<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: Storage<K, V>,
{
    fn key(&self) -> &K {
        self.writer.key()
    }

    fn judge(&mut self) -> bool {
        self.writer.judge()
    }

    fn force(&mut self) {
        self.writer.force()
    }

    async fn finish<AV>(self, value: AV) -> Result<Option<CachedEntry<K, V>>>
    where
        AV: Into<Arc<V>> + Send + 'static,
    {
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
pub struct Runtime<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: Storage<K, V>,
{
    runtime: Arc<BackgroundShutdownRuntime>,
    store: S,
    _marker: PhantomData<(K, V)>,
}

impl<K, V, S> Clone for Runtime<K, V, S>
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

impl<K, V, S> Storage<K, V> for Runtime<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: Storage<K, V>,
{
    type Config = RuntimeStoreConfig<K, V, S>;
    type Writer = RuntimeStoreWriter<K, V, S>;

    async fn open(config: Self::Config) -> Result<Self> {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        if let Some(worker_threads) = config.runtime_config.worker_threads {
            builder.worker_threads(worker_threads);
        }
        builder.thread_name(config.runtime_config.thread_name);

        let runtime = builder.enable_all().build().map_err(anyhow::Error::from)?;
        let runtime = BackgroundShutdownRuntime::from(runtime);
        let runtime = Arc::new(runtime);
        let store = runtime
            .spawn(async move { S::open(config.store_config).await })
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

    fn writer<AK>(&self, key: AK) -> Self::Writer
    where
        AK: Into<Arc<K>> + Send + 'static,
    {
        let writer = self.store.writer(key);
        RuntimeStoreWriter {
            runtime: self.runtime.clone(),
            writer,
        }
    }

    fn exists<Q>(&self, key: &Q) -> crate::error::Result<bool>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.store.exists(key)
    }

    async fn get<Q>(&self, key: &Q) -> Result<Option<CachedEntry<K, V>>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Send + Sync + 'static + Clone,
    {
        let store = self.store.clone();
        let key = key.clone();
        self.runtime.spawn(async move { store.get(&key).await }).await.unwrap()
    }

    fn remove<Q>(&self, key: &Q) -> crate::error::Result<bool>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.store.remove(key)
    }

    fn clear(&self) -> crate::error::Result<()> {
        self.store.clear()
    }
}
