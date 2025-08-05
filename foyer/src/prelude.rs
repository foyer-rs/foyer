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

#[cfg(feature = "tracing")]
pub use crate::common::tracing::TracingOptions;
#[cfg(target_os = "linux")]
pub use crate::storage::{UringIoEngine, UringIoEngineBuilder};
pub use crate::{
    common::{
        buf::{BufExt, BufMutExt},
        code::{Code, CodeError, CodeResult, DefaultHasher, Key, StorageKey, StorageValue, Value},
        event::{Event, EventListener},
        properties::{Age, Hint, Location, Source},
        utils::{option::OptionExt, range::RangeBoundsExt, scope::Scope},
    },
    hybrid::{
        builder::{HybridCacheBuilder, HybridCacheBuilderPhaseMemory, HybridCacheBuilderPhaseStorage},
        cache::{HybridCache, HybridCacheEntry, HybridCachePolicy, HybridCacheProperties, HybridFetch},
        error::{Error, Result},
        writer::{HybridCacheStorageWriter, HybridCacheWriter},
    },
    memory::{
        Cache, CacheBuilder, CacheEntry, CacheProperties, EvictionConfig, FetchState, FifoConfig, LfuConfig, LruConfig,
        S3FifoConfig, Weighter,
    },
    storage::{
        AdmissionPicker, AdmitAllPicker, ChainedAdmissionPicker, ChainedAdmissionPickerBuilder, CombinedDeviceBuilder,
        Compression, Device, DeviceBuilder, Engine, EngineBuildContext, EngineConfig, EvictionInfo, EvictionPicker,
        FifoPicker, FileDeviceBuilder, FsDeviceBuilder, InvalidRatioPicker, IoEngine, IoEngineBuilder, IoError,
        IoHandle, IoResult, IopsCounter, LargeObjectEngineBuilder, Load, NoopDeviceBuilder, NoopIoEngine,
        NoopIoEngineBuilder, PartialDeviceBuilder, Pick, PsyncIoEngine, PsyncIoEngineBuilder, RawFile, RecoverMode,
        Region, RegionStatistics, ReinsertionPicker, RejectAllPicker, Runtime, RuntimeOptions, Statistics, Store,
        StoreBuilder, Throttle, TokioRuntimeOptions,
    },
};
