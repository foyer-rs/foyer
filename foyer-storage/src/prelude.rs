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

#[cfg(target_os = "linux")]
pub use crate::io::engine::uring::UringIoEngineBuilder;
pub use crate::{
    compress::Compression,
    engine::{
        large::{
            engine::LargeObjectEngineBuilder,
            eviction::{EvictionInfo, EvictionPicker, FifoPicker, InvalidRatioPicker},
            region::{Region, RegionStatistics},
        },
        Engine, EngineBuildContext, EngineBuilder, Load, RecoverMode,
    },
    error::{Error, Result},
    io::{
        device::{
            combined::CombinedDeviceBuilder, file::FileDeviceBuilder, fs::FsDeviceBuilder, noop::NoopDeviceBuilder,
            partial::PartialDeviceBuilder, Device, DeviceBuilder, RawFile,
        },
        engine::{noop::NoopIoEngineBuilder, psync::PsyncIoEngineBuilder, IoEngine, IoEngineBuilder, IoHandle},
        error::{IoError, IoResult},
        throttle::{IoThrottler, IopsCounter, Throttle},
    },
    picker::{
        utils::{
            AdmitAllPicker, ChainedAdmissionPicker, ChainedAdmissionPickerBuilder, IoThrottlerPicker,
            IoThrottlerTarget, RejectAllPicker,
        },
        AdmissionPicker, Pick, ReinsertionPicker,
    },
    statistics::Statistics,
    store::{Store, StoreBuilder},
};
