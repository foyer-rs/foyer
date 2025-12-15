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

use foyer_common::error::{Error, ErrorKind, Result};

use crate::{
    io::device::{Device, DeviceBuilder, Partition, PartitionId},
    RawFile, Statistics, Throttle,
};

/// Builder for a combined device that wraps multiple devices and allows access to their blocks.
///
/// The throttle and statistics of the combined device will override the inner devices' throttles and statistics.
///
/// NOTE: The kind of device is a preview, the throttle and statistics strategy is likely to be modified later.
#[derive(Debug)]
pub struct CombinedDeviceBuilder {
    devices: Vec<Arc<dyn Device>>,
    throttle: Throttle,
}

impl Default for CombinedDeviceBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl CombinedDeviceBuilder {
    /// Create a new combined device builder with empty devices.
    pub fn new() -> Self {
        Self {
            devices: vec![],
            throttle: Throttle::default(),
        }
    }

    /// Add a device to the combined device builder.
    pub fn with_device(mut self, device: Arc<dyn Device>) -> Self {
        self.devices.push(device);
        self
    }

    /// Set the throttle for the combined device to override the inner devices' throttles.
    ///
    /// The throttles of the combined devices are disabled by default.
    pub fn with_throttle(mut self, throttle: Throttle) -> Self {
        self.throttle = throttle;
        self
    }
}

impl DeviceBuilder for CombinedDeviceBuilder {
    fn build(self) -> Result<Arc<dyn Device>> {
        let device = CombinedDevice {
            devices: self.devices,
            statistics: Arc::new(Statistics::new(self.throttle)),
            inner: RwLock::new(Inner {
                partitions: vec![],
                next: 0,
            }),
        };
        let device = Arc::new(device);
        Ok(device)
    }
}

#[derive(Debug)]
struct Inner {
    partitions: Vec<Arc<CombinedPartition>>,
    next: usize,
}

/// [`CombinedDevice`] is a wrapper for other device to use only a part of it.
#[derive(Debug)]
pub struct CombinedDevice {
    devices: Vec<Arc<dyn Device>>,
    inner: RwLock<Inner>,
    statistics: Arc<Statistics>,
}

impl Device for CombinedDevice {
    fn capacity(&self) -> usize {
        self.devices.iter().map(|d| d.capacity()).sum()
    }

    fn allocated(&self) -> usize {
        let inner = self.inner.read().unwrap();
        let allocated = inner.partitions.iter().take(inner.next).map(|p| p.size()).sum();
        if inner.next < inner.partitions.len() {
            allocated + self.devices[inner.next].allocated()
        } else {
            allocated
        }
    }

    fn create_partition(&self, size: usize) -> Result<Arc<dyn Partition>> {
        let mut inner = self.inner.write().unwrap();
        loop {
            if inner.next >= self.devices.len() {
                let capacity = self.devices.iter().map(|d| d.capacity()).sum::<usize>();
                return Err(Error::no_space(capacity, capacity, size));
            }
            let device = &self.devices[inner.next];
            match device.create_partition(size) {
                Ok(p) => {
                    let partition = CombinedPartition {
                        inner: p,
                        id: inner.partitions.len() as PartitionId,
                        statistics: self.statistics.clone(),
                    };
                    let partition = Arc::new(partition);
                    inner.partitions.push(partition.clone());
                    return Ok(partition);
                }
                Err(e) => {
                    if e.kind() == ErrorKind::NoSpace {
                        inner.next += 1;
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }

    fn partitions(&self) -> usize {
        self.inner.read().unwrap().partitions.len()
    }

    fn partition(&self, id: PartitionId) -> Arc<dyn Partition> {
        self.inner.read().unwrap().partitions[id as usize].clone()
    }

    fn statistics(&self) -> &Arc<Statistics> {
        &self.statistics
    }
}

#[derive(Debug)]
pub struct CombinedPartition {
    inner: Arc<dyn Partition>,
    id: PartitionId,
    statistics: Arc<Statistics>,
}

impl Partition for CombinedPartition {
    fn id(&self) -> PartitionId {
        self.id
    }

    fn size(&self) -> usize {
        self.inner.size()
    }

    fn translate(&self, address: u64) -> (RawFile, u64) {
        self.inner.translate(address)
    }

    fn statistics(&self) -> &Arc<Statistics> {
        &self.statistics
    }
}
