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

use std::sync::{Arc, RwLock};

use crate::{
    io::{
        device::{
            statistics::Statistics, throttle::Throttle, BlockCaps, Device, DeviceBuilder, DeviceCaps, Partition,
            PartitionId,
        },
        error::IoResult,
    },
    RawFile,
};

/// Builder for a no-operation mock device.
#[derive(Debug)]
pub struct NoopDeviceBuilder {
    capacity: usize,
}

impl NoopDeviceBuilder {
    /// Create a new no-operation mock device builder with the specified capacity.
    pub fn new(capacity: usize) -> Self {
        Self { capacity }
    }
}

impl Default for NoopDeviceBuilder {
    fn default() -> Self {
        Self::new(0)
    }
}

impl DeviceBuilder for NoopDeviceBuilder {
    fn build(self) -> IoResult<Arc<dyn Device>> {
        let statistics = Arc::new(Statistics::new(Throttle::default()));
        Ok(Arc::new(NoopDevice {
            partitions: RwLock::new(vec![]),
            capacity: self.capacity,
            statistics,
            caps: DeviceCaps::Block(BlockCaps::default()),
        }))
    }
}

#[derive(Debug)]
pub struct NoopDevice {
    partitions: RwLock<Vec<Arc<NoopPartition>>>,

    capacity: usize,

    statistics: Arc<Statistics>,
    caps: DeviceCaps,
}

impl Device for NoopDevice {
    fn caps(&self) -> &DeviceCaps {
        &self.caps
    }

    fn capacity(&self) -> usize {
        self.capacity
    }

    fn allocated(&self) -> usize {
        self.partitions.read().unwrap().iter().map(|p| p.size).sum()
    }

    fn create_partition(&self, size: usize) -> IoResult<Arc<dyn Partition>> {
        let mut partitions = self.partitions.write().unwrap();
        let id = partitions.len() as PartitionId;
        let partition = Arc::new(NoopPartition {
            id,
            size,
            statistics: self.statistics.clone(),
        });
        partitions.push(partition.clone());
        Ok(partition)
    }

    fn partitions(&self) -> usize {
        self.partitions.read().unwrap().len()
    }

    fn partition(&self, id: super::PartitionId) -> Arc<dyn Partition> {
        self.partitions.read().unwrap()[id as usize].clone()
    }

    fn statistics(&self) -> &Arc<Statistics> {
        &self.statistics
    }
}

#[derive(Debug)]
pub struct NoopPartition {
    id: PartitionId,
    size: usize,
    statistics: Arc<Statistics>,
}

impl Default for NoopPartition {
    fn default() -> Self {
        Self {
            id: 0,
            size: 0,
            statistics: Arc::new(Statistics::new(Throttle::default())),
        }
    }
}

impl Partition for NoopPartition {
    fn id(&self) -> PartitionId {
        self.id
    }

    fn size(&self) -> usize {
        self.size
    }

    fn translate(&self, _: u64) -> (RawFile, u64) {
        (RawFile(0 as _), 0)
    }

    fn statistics(&self) -> &Arc<Statistics> {
        &self.statistics
    }
}
