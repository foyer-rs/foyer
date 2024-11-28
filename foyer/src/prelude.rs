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

#[cfg(feature = "opentelemetry")]
pub use crate::common::metrics::registry::opentelemetry;
#[cfg(feature = "opentelemetry_0_26")]
pub use crate::common::metrics::registry::opentelemetry_0_26;
#[cfg(feature = "opentelemetry_0_27")]
pub use crate::common::metrics::registry::opentelemetry_0_27;
#[cfg(feature = "prometheus")]
pub use crate::common::metrics::registry::prometheus;
#[cfg(feature = "prometheus-client")]
pub use crate::common::metrics::registry::prometheus_client;
#[cfg(feature = "prometheus-client_0_22")]
pub use crate::common::metrics::registry::prometheus_client_0_22;
pub use crate::{
    common::{
        buf::{BufExt, BufMutExt},
        code::{Key, StorageKey, StorageValue, Value},
        event::{Event, EventListener},
        metrics::{
            registry::noop::NoopMetricsRegistry, CounterOps, CounterVecOps, GaugeOps, GaugeVecOps, HistogramOps,
            HistogramVecOps, RegistryOps,
        },
        range::RangeBoundsExt,
        tracing::TracingOptions,
    },
    hybrid::{
        builder::{HybridCacheBuilder, HybridCacheBuilderPhaseMemory, HybridCacheBuilderPhaseStorage},
        cache::{HybridCache, HybridCacheEntry, HybridFetch, HybridFetchInner},
        writer::{HybridCacheStorageWriter, HybridCacheWriter},
    },
    memory::{
        Cache, CacheBuilder, CacheEntry, CacheHint, EvictionConfig, FetchState, FifoConfig, LfuConfig, LruConfig,
        S3FifoConfig, Weighter,
    },
    storage::{
        AdmissionPicker, AdmitAllPicker, Compression, Dev, DevConfig, DevExt, DeviceStats, DirectFileDevice,
        DirectFileDeviceOptions, DirectFsDevice, DirectFsDeviceOptions, Engine, EvictionPicker, FifoPicker,
        InvalidRatioPicker, LargeEngineOptions, RateLimitPicker, RecoverMode, ReinsertionPicker, RejectAllPicker,
        Runtime, RuntimeOptions, SmallEngineOptions, Storage, Store, StoreBuilder, TokioRuntimeOptions,
        TombstoneLogConfigBuilder,
    },
};
