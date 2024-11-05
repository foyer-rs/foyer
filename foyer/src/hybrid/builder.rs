//  Copyright 2024 foyer Project Authors
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

use std::{fmt::Debug, sync::Arc};

use ahash::RandomState;
use foyer_common::{
    code::{HashBuilder, StorageKey, StorageValue},
    event::EventListener,
    tracing::TracingOptions,
};
use foyer_memory::{Cache, CacheBuilder, EvictionConfig, Weighter};
use foyer_storage::{
    AdmissionPicker, Compression, DeviceOptions, Engine, LargeEngineOptions, RecoverMode, RuntimeOptions,
    SmallEngineOptions, StoreBuilder,
};

use crate::HybridCache;

/// Hybrid cache builder.
pub struct HybridCacheBuilder<K, V> {
    name: String,
    event_listener: Option<Arc<dyn EventListener<Key = K, Value = V>>>,
    tracing_options: TracingOptions,
}

impl<K, V> Default for HybridCacheBuilder<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> HybridCacheBuilder<K, V> {
    /// Create a new hybrid cache builder.
    pub fn new() -> Self {
        Self {
            name: "foyer".to_string(),
            event_listener: None,
            tracing_options: TracingOptions::default(),
        }
    }

    /// Set the name of the foyer hybrid cache instance.
    ///
    /// foyer will use the name as the prefix of the metric names.
    ///
    /// Default: `foyer`.
    pub fn with_name(mut self, name: &str) -> Self {
        self.name = name.to_string();
        self
    }

    /// Set event listener.
    ///
    /// Default: No event listener installed.
    pub fn with_event_listener(mut self, event_listener: Arc<dyn EventListener<Key = K, Value = V>>) -> Self {
        self.event_listener = Some(event_listener);
        self
    }

    /// Set tracing options.
    ///
    /// Default: Only operations over 1s will be recorded.
    pub fn with_tracing_options(mut self, tracing_options: TracingOptions) -> Self {
        self.tracing_options = tracing_options;
        self
    }

    /// Continue to modify the in-memory cache configurations.
    pub fn memory(self, capacity: usize) -> HybridCacheBuilderPhaseMemory<K, V, RandomState>
    where
        K: StorageKey,
        V: StorageValue,
    {
        let mut builder = CacheBuilder::new(capacity).with_name(&self.name);
        if let Some(event_listener) = self.event_listener {
            builder = builder.with_event_listener(event_listener);
        }
        HybridCacheBuilderPhaseMemory {
            builder,
            name: self.name,
            tracing_options: self.tracing_options,
        }
    }
}

/// Hybrid cache builder to modify the in-memory cache configurations.
pub struct HybridCacheBuilderPhaseMemory<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    name: String,
    tracing_options: TracingOptions,
    builder: CacheBuilder<K, V, S>,
}

impl<K, V, S> HybridCacheBuilderPhaseMemory<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    /// Set in-memory cache sharding count. Entries will be distributed to different shards based on their hash.
    /// Operations on different shard can be parallelized.
    pub fn with_shards(self, shards: usize) -> Self {
        let builder = self.builder.with_shards(shards);
        HybridCacheBuilderPhaseMemory {
            name: self.name,
            tracing_options: self.tracing_options,
            builder,
        }
    }

    /// Set in-memory cache eviction algorithm.
    ///
    /// The default value is a general-used w-TinyLFU algorithm.
    pub fn with_eviction_config(self, eviction_config: impl Into<EvictionConfig>) -> Self {
        let builder = self.builder.with_eviction_config(eviction_config.into());
        HybridCacheBuilderPhaseMemory {
            name: self.name,
            tracing_options: self.tracing_options,
            builder,
        }
    }

    /// Set in-memory cache hash builder.
    pub fn with_hash_builder<OS>(self, hash_builder: OS) -> HybridCacheBuilderPhaseMemory<K, V, OS>
    where
        OS: HashBuilder + Debug,
    {
        let builder = self.builder.with_hash_builder(hash_builder);
        HybridCacheBuilderPhaseMemory {
            name: self.name,
            tracing_options: self.tracing_options,
            builder,
        }
    }

    /// Set in-memory cache weighter.
    pub fn with_weighter(self, weighter: impl Weighter<K, V>) -> Self {
        let builder = self.builder.with_weighter(weighter);
        HybridCacheBuilderPhaseMemory {
            name: self.name,
            tracing_options: self.tracing_options,
            builder,
        }
    }

    /// Set object pool capacity.
    ///
    /// The object pool is used for avoiding frequent memory allocation.
    ///
    /// Each shard gets `object_pool_capacity / shards` slots.
    ///
    /// Default: 4096
    pub fn with_object_pool_capacity(self, object_pool_capacity: usize) -> Self {
        let builder = self.builder.with_object_pool_capacity(object_pool_capacity);
        HybridCacheBuilderPhaseMemory {
            name: self.name,
            tracing_options: self.tracing_options,
            builder,
        }
    }

    /// Continue to modify the disk cache configurations.
    pub fn storage(self, engine: Engine) -> HybridCacheBuilderPhaseStorage<K, V, S> {
        let memory = self.builder.build();
        HybridCacheBuilderPhaseStorage {
            builder: StoreBuilder::new(memory.clone(), engine).with_name(&self.name),
            name: self.name,
            tracing_options: self.tracing_options,
            memory,
        }
    }
}

/// Hybrid cache builder modify the disk cache configurations.
pub struct HybridCacheBuilderPhaseStorage<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    name: String,
    tracing_options: TracingOptions,
    memory: Cache<K, V, S>,
    builder: StoreBuilder<K, V, S>,
}

impl<K, V, S> HybridCacheBuilderPhaseStorage<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    /// Set device options for the disk cache store.
    pub fn with_device_options(self, device_options: impl Into<DeviceOptions>) -> Self {
        let builder = self.builder.with_device_options(device_options);
        Self {
            name: self.name,
            tracing_options: self.tracing_options,
            memory: self.memory,
            builder,
        }
    }

    /// Enable/disable `sync` after writes.
    ///
    /// Default: `false`.
    pub fn with_flush(self, flush: bool) -> Self {
        let builder = self.builder.with_flush(flush);
        Self {
            name: self.name,
            tracing_options: self.tracing_options,
            memory: self.memory,
            builder,
        }
    }

    /// Set the recover mode for the disk cache store.
    ///
    /// See more in [`RecoverMode`].
    ///
    /// Default: [`RecoverMode::Quiet`].
    pub fn with_recover_mode(self, recover_mode: RecoverMode) -> Self {
        let builder = self.builder.with_recover_mode(recover_mode);
        Self {
            name: self.name,
            tracing_options: self.tracing_options,
            memory: self.memory,
            builder,
        }
    }

    /// Set the admission pickers for th disk cache store.
    ///
    /// The admission picker is used to pick the entries that can be inserted into the disk cache store.
    ///
    /// Default: [`AdmitAllPicker`].
    pub fn with_admission_picker(self, admission_picker: Arc<dyn AdmissionPicker<Key = K>>) -> Self {
        let builder = self.builder.with_admission_picker(admission_picker);
        Self {
            name: self.name,
            tracing_options: self.tracing_options,
            memory: self.memory,
            builder,
        }
    }

    /// Set the compression algorithm of the disk cache store.
    ///
    /// Default: [`Compression::None`].
    pub fn with_compression(self, compression: Compression) -> Self {
        let builder = self.builder.with_compression(compression);
        Self {
            name: self.name,
            tracing_options: self.tracing_options,
            memory: self.memory,
            builder,
        }
    }

    /// Configure the dedicated runtime for the disk cache store.
    pub fn with_runtime_options(self, runtime_options: RuntimeOptions) -> Self {
        let builder = self.builder.with_runtime_options(runtime_options);
        Self {
            name: self.name,
            tracing_options: self.tracing_options,
            memory: self.memory,
            builder,
        }
    }

    /// Setup the large object disk cache engine with the given options.
    ///
    /// Otherwise, the default options will be used. See [`LargeEngineOptions`].
    pub fn with_large_object_disk_cache_options(self, options: LargeEngineOptions<K, V, S>) -> Self {
        let builder = self.builder.with_large_object_disk_cache_options(options);
        Self {
            name: self.name,
            tracing_options: self.tracing_options,
            memory: self.memory,
            builder,
        }
    }

    /// Setup the small object disk cache engine with the given options.
    ///
    /// Otherwise, the default options will be used. See [`SmallEngineOptions`].
    pub fn with_small_object_disk_cache_options(self, options: SmallEngineOptions<K, V, S>) -> Self {
        let builder = self.builder.with_small_object_disk_cache_options(options);
        Self {
            name: self.name,
            tracing_options: self.tracing_options,
            memory: self.memory,
            builder,
        }
    }

    /// Build and open the hybrid cache with the given configurations.
    pub async fn build(self) -> anyhow::Result<HybridCache<K, V, S>> {
        let storage = self.builder.build().await?;
        Ok(HybridCache::new(self.name, self.memory, storage, self.tracing_options))
    }
}
