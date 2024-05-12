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

use std::{borrow::Borrow, fmt::Debug, hash::Hash, sync::Arc};

use foyer_common::runtime::BackgroundShutdownRuntime;
use foyer_memory::CacheEntry;
use futures::{Future, FutureExt};

use crate::error::Result;

use super::storage::{EnqueueFuture, Storage};

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

pub struct RuntimeStoreConfig<SS>
where
    SS: Storage,
{
    pub store_config: SS::Config,
    pub runtime_config: RuntimeConfig,
}

impl<SS> Debug for RuntimeStoreConfig<SS>
where
    SS: Storage,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeStoreConfig")
            .field("store_config", &self.store_config)
            .field("runtime_config", &self.runtime_config)
            .finish()
    }
}

#[derive(Debug)]
pub struct Runtime<SS>
where
    SS: Storage,
{
    runtime: Arc<BackgroundShutdownRuntime>,
    store: SS,
}

impl<SS> Clone for Runtime<SS>
where
    SS: Storage,
{
    fn clone(&self) -> Self {
        Self {
            runtime: self.runtime.clone(),
            store: self.store.clone(),
        }
    }
}

impl<SS> Storage for Runtime<SS>
where
    SS: Storage,
{
    type Key = SS::Key;
    type Value = SS::Value;
    type BuildHasher = SS::BuildHasher;

    type Config = RuntimeStoreConfig<SS>;

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
            .spawn(async move { SS::open(config.store_config).await })
            .await
            .unwrap()?;
        Ok(Self { runtime, store })
    }

    async fn close(&self) -> Result<()> {
        let store = self.store.clone();
        self.runtime.spawn(async move { store.close().await }).await.unwrap()
    }

    fn enqueue(&self, entry: CacheEntry<Self::Key, Self::Value, Self::BuildHasher>) -> EnqueueFuture {
        self.store.enqueue(entry)
    }

    fn load<Q>(&self, key: &Q) -> impl Future<Output = Result<Option<(Self::Key, Self::Value)>>> + Send + 'static
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Send + Sync + 'static,
    {
        let future = self.store.load(key);
        self.runtime.spawn(future).map(|join_result| join_result.unwrap())
    }

    fn delete<Q>(&self, key: &Q) -> EnqueueFuture
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Send + Sync + 'static,
    {
        self.store.delete(key)
    }

    async fn destroy(&self) -> Result<()> {
        self.store.destroy().await
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use ahash::RandomState;
    use foyer_memory::{Cache, CacheBuilder, FifoConfig};
    use futures::future::try_join_all;
    use itertools::Itertools;

    use crate::{
        large::{
            admission::AdmitAllPicker,
            device::direct_fs::{DirectFsDevice, DirectFsDeviceConfig},
            eviction::FifoPicker,
            generic::{GenericStore, GenericStoreConfig},
            recover::RecoverMode,
            reinsertion::RejectAllPicker,
            storage::Storage,
        },
        Compression,
    };

    use super::*;

    const KB: usize = 1024;

    fn cache_for_test() -> Cache<u64, Vec<u8>> {
        CacheBuilder::new(10)
            .with_eviction_config(FifoConfig::default())
            .build()
    }
    fn config_for_test(
        memory: &Cache<u64, Vec<u8>>,
        dir: impl AsRef<Path>,
    ) -> GenericStoreConfig<u64, Vec<u8>, RandomState, DirectFsDevice> {
        GenericStoreConfig {
            memory: memory.clone(),
            device_config: DirectFsDeviceConfig {
                dir: dir.as_ref().into(),
                capacity: 64 * KB,
                file_size: 16 * KB,
            },
            compression: Compression::None,
            flush: true,
            indexer_shards: 4,
            recover_mode: RecoverMode::Strict,
            recover_concurrency: 2,
            flushers: 1,
            reclaimers: 1,
            clean_region_threshold: 1,
            eviction_pickers: vec![Box::<FifoPicker>::default()],
            admission_picker: Box::<AdmitAllPicker<u64>>::default(),
            reinsertion_picker: Arc::<RejectAllPicker<u64>>::default(),
            tombstone_log_config: None,
        }
    }

    #[test_log::test]
    fn test_runtime_store() {
        let dir = tempfile::tempdir().unwrap();
        let background = BackgroundShutdownRuntime::from(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(anyhow::Error::from)
                .unwrap(),
        );
        let memory = cache_for_test();

        let es = (0..100).map(|i| memory.insert(i, vec![i as u8; 7 * KB])).collect_vec();

        let config = config_for_test(&memory, dir);
        let store = background.block_on(async move { GenericStore::open(config).await.unwrap() });

        let fs = es.iter().cloned().map(|e| store.enqueue(e)).collect_vec();
        background.block_on(async { try_join_all(fs).await.unwrap() });

        let mut fs = vec![];
        for i in 0..100 {
            fs.push(store.load(&i));
        }
        background.block_on(async { try_join_all(fs).await.unwrap() });

        let mut fs = vec![];
        for i in 0..100 {
            fs.push(store.delete(&i));
        }
        background.block_on(async { try_join_all(fs).await.unwrap() });

        background.block_on(async { store.close().await.unwrap() });

        drop(store);
    }
}
