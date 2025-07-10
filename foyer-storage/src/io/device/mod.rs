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

use std::{fmt::Debug, os::fd::RawFd, sync::Arc};

use crate::{io::error::IoResult, Throttle};

pub type RegionId = u64;

pub trait DeviceBuilder: Send + Sync + 'static + Debug {
    /// Build a device from the given configuration.
    fn build(self) -> IoResult<Arc<dyn Device>>;
}

pub trait Device: Send + Sync + 'static + Debug {
    /// The capacity of the device, must be 4K aligned.
    fn capacity(&self) -> usize;
    /// The region size of the device, must be 4K aligned.
    fn region_size(&self) -> usize;
    /// The throttle config for the device.
    fn throttle(&self) -> &Throttle;
    /// Translate a region and offset to a raw file descriptor and offset.
    fn translate(&self, region: RegionId, offset: u64) -> (RawFd, u64);
}

pub mod file;
pub mod fs;
