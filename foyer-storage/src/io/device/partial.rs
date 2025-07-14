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
        device::{Device, DeviceBuilder, Partition, PartitionId},
        error::IoResult,
        throttle::Throttle,
    },
    IoError,
};

/// Builder for a partial device that wraps another device and allows access to only a subset of capacity.
#[derive(Debug)]
pub struct PartialDeviceBuilder {
    device: Arc<dyn Device>,
    capacity: usize,
    throttle: Option<Throttle>,
}

impl PartialDeviceBuilder {
    /// Create a new partial device builder with the specified device.
    pub fn new(device: Arc<dyn Device>) -> Self {
        let capacity = device.capacity();
        Self {
            device,
            capacity,
            throttle: None,
        }
    }

    /// Set the capacity of the partial device.
    ///
    /// NOTE:
    /// - The capacity must be less than or equal to the inner device's capacity.
    /// - The sum of all capacities of the inner device's partial devices must be less than or equal to the inner
    ///   device's capacity.
    pub fn with_capacity(mut self, capacity: usize) -> Self {
        assert!(capacity <= self.device.capacity());
        self.capacity = capacity;
        self
    }

    /// Set the throttle for the partial device to override the inner device's throttle.
    pub fn with_throttle(mut self, throttle: Throttle) -> Self {
        self.throttle = Some(throttle);
        self
    }
}

impl DeviceBuilder for PartialDeviceBuilder {
    fn build(self: Box<Self>) -> IoResult<Arc<dyn Device>> {
        Ok(Arc::new(PartialDevice {
            inner: self.device,
            capacity: self.capacity,
            throttle: self.throttle,
            partitions: RwLock::new(vec![]),
        }))
    }
}

/// [`PartialDevice`] is a wrapper for other device to use only a part of it.
#[derive(Debug)]
pub struct PartialDevice {
    inner: Arc<dyn Device>,
    capacity: usize,
    throttle: Option<Throttle>,
    partitions: RwLock<Vec<Arc<PartialPartition>>>,
}

impl Device for PartialDevice {
    fn capacity(&self) -> usize {
        self.capacity
    }

    fn allocated(&self) -> usize {
        self.partitions.read().unwrap().iter().map(|p| p.size()).sum()
    }

    fn create_partition(&self, size: usize) -> IoResult<Arc<dyn Partition>> {
        let mut partitions = self.partitions.write().unwrap();
        let allocated = partitions.iter().map(|p| p.size()).sum::<usize>();
        if allocated + size > self.capacity {
            return Err(IoError::NoSpace {
                capacity: self.capacity,
                allocated,
                required: size,
            });
        }
        self.inner.create_partition(size).map(|inner| {
            let partition = PartialPartition {
                inner,
                id: partitions.len() as PartitionId,
            };
            let partition = Arc::new(partition);
            partitions.push(partition.clone());
            partition as Arc<dyn Partition>
        })
    }

    fn partitions(&self) -> usize {
        self.partitions.read().unwrap().len()
    }

    fn partition(&self, id: PartitionId) -> Arc<dyn Partition> {
        self.partitions.read().unwrap()[id as usize].clone()
    }

    fn throttle(&self) -> &Throttle {
        self.throttle.as_ref().unwrap_or_else(|| self.inner.throttle())
    }
}

#[derive(Debug)]
pub struct PartialPartition {
    inner: Arc<dyn Partition>,
    id: PartitionId,
}

impl Partition for PartialPartition {
    fn id(&self) -> PartitionId {
        self.id
    }

    fn size(&self) -> usize {
        self.inner.size()
    }

    fn translate(&self, address: u64) -> (super::RawFile, u64) {
        self.inner.translate(address)
    }
}
