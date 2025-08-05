// Copyright 2025 foyer Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{borrow::Cow, fmt::Debug, sync::Arc};

#[cfg(feature = "tracing")]
use foyer_common::tracing::TracingOptions;
use foyer_common::{
    code::{DefaultHasher, HashBuilder, StorageKey, StorageValue},
    event::EventListener,
    metrics::Metrics,
};
use foyer_memory::{Cache, CacheBuilder, EvictionConfig, Weighter};
use foyer_storage::{Compression, EngineConfig, IoEngine, RecoverMode, RuntimeOptions, StoreBuilder};
use mixtrics::{metrics::BoxedRegistry, registry::noop::NoopMetricsRegistry};

use crate::hybrid::{
    cache::{HybridCache, HybridCacheOptions, HybridCachePipe, HybridCachePolicy, HybridCacheProperties},
    error::Result,
};

/// Hybrid cache builder.
pub struct HybridCacheBuilder<K, V> {
    name: Cow<'static, str>,
    options: HybridCacheOptions,
    event_listener: Option<Arc<dyn EventListener<Key = K, Value = V>>>,
    registry: BoxedRegistry,
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
            name: "foyer".into(),
            options: HybridCacheOptions::default(),
            event_listener: None,
            registry: Box::new(NoopMetricsRegistry),
        }
    }
}

impl<K, V> HybridCacheBuilder<K, V> {
    /// Set the name of the foyer hybrid cache instance.
    ///
    /// foyer will use the name as the prefix of the metric names.
    ///
    /// Default: `foyer`.
    pub fn with_name(mut self, name: impl Into<Cow<'static, str>>) -> Self {
        self.name = name.into();
        self
    }

    /// Set the policy of the hybrid cache.
    ///
    /// Default: [`HybridCachePolicy::default()`].
    pub fn with_policy(mut self, policy: HybridCachePolicy) -> Self {
        self.options.policy = policy;
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
    #[cfg(feature = "tracing")]
    pub fn with_tracing_options(mut self, tracing_options: TracingOptions) -> Self {
        self.options.tracing_options = tracing_options;
        self
    }

    /// Set whether flush all in-memory cached entries to disk cache on close.
    ///
    /// Based on the disk cache performance and the io throttle configuration, this may take some extra time on close.
    ///
    /// Default: `true`.
    pub fn with_flush_on_close(mut self, flush_on_close: bool) -> Self {
        self.options.flush_on_close = flush_on_close;
        self
    }

    /// Set metrics registry.
    ///
    /// Default: [`NoopMetricsRegistry`].
    pub fn with_metrics_registry(mut self, registry: BoxedRegistry) -> HybridCacheBuilder<K, V> {
        self.registry = registry;
        self
    }

    /// Continue to modify the in-memory cache configurations.
    pub fn memory(self, capacity: usize) -> HybridCacheBuilderPhaseMemory<K, V, DefaultHasher>
    where
        K: StorageKey,
        V: StorageValue,
    {
        let metrics = Arc::new(Metrics::new(self.name.clone(), &self.registry));
        let mut builder = CacheBuilder::new(capacity)
            .with_name(self.name.clone())
            .with_metrics(metrics.clone());
        if let Some(event_listener) = self.event_listener {
            builder = builder.with_event_listener(event_listener);
        }
        HybridCacheBuilderPhaseMemory {
            name: self.name,
            options: self.options,
            metrics,
            builder,
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
    name: Cow<'static, str>,
    options: HybridCacheOptions,
    metrics: Arc<Metrics>,
    // `NoopMetricsRegistry` here will be ignored, for its metrics is already set.
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
            options: self.options,
            metrics: self.metrics,
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
            options: self.options,
            metrics: self.metrics,
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
            options: self.options,
            metrics: self.metrics,
            builder,
        }
    }

    /// Set in-memory cache weighter.
    pub fn with_weighter(self, weighter: impl Weighter<K, V>) -> Self {
        let builder = self.builder.with_weighter(weighter);
        HybridCacheBuilderPhaseMemory {
            name: self.name,
            options: self.options,
            metrics: self.metrics,
            builder,
        }
    }

    /// Continue to modify the disk cache configurations.
    pub fn storage(self) -> HybridCacheBuilderPhaseStorage<K, V, S> {
        let memory = self.builder.build();
        HybridCacheBuilderPhaseStorage {
            name: self.name.clone(),
            options: self.options,
            metrics: self.metrics.clone(),
            memory: memory.clone(),
            builder: StoreBuilder::new(self.name, memory, self.metrics),
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
    name: Cow<'static, str>,
    options: HybridCacheOptions,
    metrics: Arc<Metrics>,
    memory: Cache<K, V, S, HybridCacheProperties>,
    builder: StoreBuilder<K, V, S, HybridCacheProperties>,
}

impl<K, V, S> HybridCacheBuilderPhaseStorage<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    /// Set io engine for the disk cache store.
    pub fn with_io_engine(self, io_engine: Arc<dyn IoEngine>) -> Self {
        let builder = self.builder.with_io_engine(io_engine);
        Self {
            name: self.name,
            options: self.options,
            metrics: self.metrics,
            memory: self.memory,
            builder,
        }
    }

    /// Set engine config for the disk cache store.
    pub fn with_engine_config(self, config: impl Into<Box<dyn EngineConfig<K, V, HybridCacheProperties>>>) -> Self {
        let builder = self.builder.with_engine_config(config);
        Self {
            name: self.name,
            options: self.options,
            metrics: self.metrics,
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
            options: self.options,
            metrics: self.metrics,
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
            options: self.options,
            metrics: self.metrics,
            memory: self.memory,
            builder,
        }
    }

    /// Configure the dedicated runtime for the disk cache store.
    pub fn with_runtime_options(self, runtime_options: RuntimeOptions) -> Self {
        let builder = self.builder.with_runtime_options(runtime_options);
        Self {
            name: self.name,
            options: self.options,
            metrics: self.metrics,
            memory: self.memory,
            builder,
        }
    }

    /// Build and open the hybrid cache with the given configurations.
    pub async fn build(self) -> Result<HybridCache<K, V, S>> {
        let builder = self.builder;

        let piped = !builder.is_noop() && self.options.policy == HybridCachePolicy::WriteOnEviction;

        let memory = self.memory;
        let storage = builder.build().await?;

        if piped {
            let pipe = HybridCachePipe::new(storage.clone());
            memory.set_pipe(Box::new(pipe));
        }

        Ok(HybridCache::new(self.name, self.options, memory, storage, self.metrics))
    }
}
