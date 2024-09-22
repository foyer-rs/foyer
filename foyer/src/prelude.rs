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

pub use common::{
    buf::{BufExt, BufMutExt},
    code::{Key, StorageKey, StorageValue, Value},
    event::EventListener,
    range::RangeBoundsExt,
    tracing::TracingConfig,
};
pub use memory::{
    Cache, CacheBuilder, CacheContext, CacheEntry, EvictionConfig, FetchState, FifoConfig, LfuConfig, LruConfig,
    S3FifoConfig, Weighter,
};
pub use storage::{
    AdmissionPicker, AdmitAllPicker, Compression, Dev, DevExt, DevOptions, DeviceStats, DirectFileDevice,
    DirectFileDeviceOptions, DirectFileDeviceOptionsBuilder, DirectFsDevice, DirectFsDeviceOptions,
    DirectFsDeviceOptionsBuilder, Engine, EvictionPicker, FifoPicker, InvalidRatioPicker, LargeEngineOptions,
    RateLimitPicker, RecoverMode, ReinsertionPicker, RejectAllPicker, RuntimeConfig, RuntimeHandles,
    SmallEngineOptions, Storage, Store, StoreBuilder, TokioRuntimeConfig, TombstoneLogConfigBuilder,
};

pub use crate::hybrid::{
    builder::{HybridCacheBuilder, HybridCacheBuilderPhaseMemory, HybridCacheBuilderPhaseStorage},
    cache::{HybridCache, HybridCacheEntry, HybridFetch, HybridFetchInner},
    writer::{HybridCacheStorageWriter, HybridCacheWriter},
};
use crate::{common, memory, storage};
