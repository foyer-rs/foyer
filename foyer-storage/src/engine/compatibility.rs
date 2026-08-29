// Copyright 2026 foyer Project Authors
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

use foyer_common::{
    code::{StorageKey, StorageValue},
    properties::Properties,
};

use crate::{
    engine::block::engine::BlockEngineConfig,
    io::device::{
        DeviceFor, combined::CombinedDevice, file::FileDevice, fs::FsDevice, noop::NoopDevice, partial::PartialDevice,
    },
};

impl<K, V, P> DeviceFor<BlockEngineConfig<K, V, P>> for FsDevice
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
}

impl<K, V, P> DeviceFor<BlockEngineConfig<K, V, P>> for FileDevice
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
}

impl<K, V, P> DeviceFor<BlockEngineConfig<K, V, P>> for CombinedDevice
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
}

impl<K, V, P> DeviceFor<BlockEngineConfig<K, V, P>> for PartialDevice
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
}

impl<K, V, P> DeviceFor<BlockEngineConfig<K, V, P>> for NoopDevice
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
}
