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

use foyer_memory::CacheProperties;
use foyer_storage::{
    BlockEngineConfig, CombinedDevice, DeviceBuilder, DeviceFor, FileDevice, FsDevice, FsDeviceBuilder, NoopDevice,
    PartialDevice,
};

type Config = BlockEngineConfig<u64, u64, CacheProperties>;

fn assert_compatible<D>()
where
    D: DeviceFor<Config>,
{
}

fn main() -> foyer_storage::Result<()> {
    assert_compatible::<FsDevice>();
    assert_compatible::<FileDevice>();
    assert_compatible::<CombinedDevice>();
    assert_compatible::<PartialDevice>();
    assert_compatible::<NoopDevice>();

    let device = FsDeviceBuilder::new("cache").build()?;
    let _config: Config = BlockEngineConfig::new(device);
    Ok(())
}
