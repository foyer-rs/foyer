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

use std::sync::Arc;

use foyer_common::{
    code::{Key, Value},
    runtime::BackgroundShutdownRuntime,
};

use crate::{
    error::Result,
    storage::{Storage, StorageWriter},
};

#[derive(Debug)]
pub struct RuntimeConfig {
    pub worker_threads: Option<usize>,
    pub thread_name: Option<String>,
}

#[derive(Debug)]
pub struct RuntimeStoreConfig<K, V, S>
where
    K: Key,
    V: Value,
    S: Storage<Key = K, Value = V>,
{
    pub store: S::Config,
    pub runtime: RuntimeConfig,
}

#[derive(Debug)]
pub struct RuntimeStoreWriter<K, V, S>
where
    K: Key,
    V: Value,
    S: Storage<Key = K, Value = V>,
{
    runtime: Arc<BackgroundShutdownRuntime>,
    writer: S::Writer,
}

impl<K, V, S> StorageWriter for RuntimeStoreWriter<K, V, S>
where
    K: Key,
    V: Value,
    S: Storage<Key = K, Value = V>,
{
    type Key = K;
    type Value = V;

    fn judge(&mut self) -> bool {
        self.writer.judge()
    }

    async fn finish(self, value: Self::Value) -> Result<bool> {
        self.runtime
            .spawn(async move { self.writer.finish(value).await })
            .await
            .unwrap()
    }
}

#[derive(Debug)]
pub struct RuntimeStore<K, V, S>
where
    K: Key,
    V: Value,
    S: Storage<Key = K, Value = V>,
{
    runtime: Arc<BackgroundShutdownRuntime>,
    store: S,
}

impl<K, V, S> Clone for RuntimeStore<K, V, S>
where
    K: Key,
    V: Value,
    S: Storage<Key = K, Value = V>,
{
    fn clone(&self) -> Self {
        Self {
            runtime: Arc::clone(&self.runtime),
            store: self.store.clone(),
        }
    }
}

impl<K, V, S> Storage for RuntimeStore<K, V, S>
where
    K: Key,
    V: Value,
    S: Storage<Key = K, Value = V>,
{
    type Key = K;
    type Value = V;
    type Config = RuntimeStoreConfig<K, V, S>;
    type Writer = RuntimeStoreWriter<K, V, S>;

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

    async fn close(&self) -> Result<()> {
        let store = self.store.clone();
        self.runtime
            .spawn(async move { store.close().await })
            .await
            .unwrap()
    }

    fn writer(&self, key: Self::Key, weight: usize) -> Self::Writer {
        let writer = self.store.writer(key, weight);
        RuntimeStoreWriter {
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
