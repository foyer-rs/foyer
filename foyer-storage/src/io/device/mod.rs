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

pub mod statistics;
pub mod throttle;

use std::{any::Any, fmt::Debug, sync::Arc};

use foyer_common::error::Result;

use crate::io::device::statistics::Statistics;

pub type PartitionId = u32;

/// Raw os file resource.
///
/// Use `fd` with unix and wasm, use `handle` with windows.
#[cfg(any(target_family = "unix", target_family = "wasm"))]
pub struct RawFile(pub std::os::fd::RawFd);

/// Raw os file resource.
///
/// Use `fd` with unix and wasm, use `handle` with windows.
#[cfg(target_family = "windows")]
pub struct RawFile(pub std::os::windows::io::RawHandle);

unsafe impl Send for RawFile {}
unsafe impl Sync for RawFile {}

/// Device builder trait.
pub trait DeviceBuilder: Send + Sync + 'static + Debug {
    /// Build a device from the given configuration.
    fn build(self) -> Result<Arc<dyn Device>>;
}

/// Partition is a logical segment of a device.
pub trait Partition: Send + Sync + 'static + Debug + Any {
    /// Get the id of the partition.
    fn id(&self) -> PartitionId;

    /// Get the capacity of the partition.
    ///
    /// NOTE: `size` must be 4K aligned.
    fn size(&self) -> usize;

    /// Translate an address to a raw file descriptor and address.
    fn translate(&self, address: u64) -> (RawFile, u64);

    /// Get the statistics of the device this partition belongs to.
    fn statistics(&self) -> &Arc<Statistics>;
}

/// Device trait.
pub trait Device: Send + Sync + 'static + Debug + Any {
    /// Get the capacity of the device.
    ///
    /// NOTE: `capacity` must be 4K aligned.
    fn capacity(&self) -> usize;

    /// Get the allocated space in the device.
    fn allocated(&self) -> usize;

    /// Get the free space in the device.
    fn free(&self) -> usize {
        self.capacity() - self.allocated()
    }

    /// Create a new partition with the given size.
    ///
    /// NOTE:
    ///
    /// - Allocating partition may consume more space than requested.
    /// - `size` must be 4K aligned.
    fn create_partition(&self, size: usize) -> Result<Arc<dyn Partition>>;

    /// Get the number of partitions in the device.
    fn partitions(&self) -> usize;

    /// Get the partition with given id in the device.
    fn partition(&self, id: PartitionId) -> Arc<dyn Partition>;

    /// Get the statistics of the device this partition belongs to.
    fn statistics(&self) -> &Arc<Statistics>;
}

pub mod file;
pub mod fs;
pub mod noop;

pub mod combined;
pub mod partial;
