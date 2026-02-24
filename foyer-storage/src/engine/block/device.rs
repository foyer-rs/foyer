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

use std::{any::Any, fmt::Debug, sync::Arc};

use crate::io::device::{Device, RawFile, statistics::Statistics};

use foyer_common::error::Result;

pub type PartitionId = u32;

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
pub trait PartitionableDevice: Send + Sync + 'static + Debug + Any + Device {
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
}
