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

use std::{os::fd::RawFd, sync::Arc};

use crate::io::{
    device::{Device, DeviceBuilder, RegionId},
    error::{IoError, IoResult},
    throttle::Throttle,
};

/// Builder for a combined device that wraps multiple devices and allows access to their regions.
#[derive(Debug)]
pub struct CombinedDeviceBuilder {
    devices: Vec<Arc<dyn Device>>,
    throttle: Option<Throttle>,
}

impl Default for CombinedDeviceBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl CombinedDeviceBuilder {
    /// Create a new combined device builder with empty devices..
    pub fn new() -> Self {
        Self {
            devices: vec![],
            throttle: None,
        }
    }

    /// Add a device to the combined device builder.
    pub fn with_device(mut self, device: Arc<dyn Device>) -> Self {
        self.devices.push(device);
        self
    }

    /// Set the throttle for the combined device to override the inner devices' throttles.
    pub fn with_throttle(mut self, throttle: Throttle) -> Self {
        self.throttle = Some(throttle);
        self
    }
}

impl DeviceBuilder for CombinedDeviceBuilder {
    fn build(self: Box<Self>) -> IoResult<Arc<dyn Device>> {
        if self.devices.is_empty() {
            return Err(IoError::other("No devices provided for CombinedDevice"));
        }

        let region_size = self.devices.iter().map(|d| d.region_size()).min().unwrap();
        let mut mapping = vec![];
        let mut regions = 0;
        for device in self.devices.iter() {
            regions += device.regions();
            mapping.push(regions);
        }
        let capacity = regions * region_size;
        Ok(Arc::new(CombinedDevice {
            devices: self.devices,
            throttle: self.throttle,
            mapping: vec![],
            capacity,
            region_size,
            regions,
        }))
    }
}

/// [`CombinedDevice`] is a wrapper for other device to use only a part of it.
#[derive(Debug)]
pub struct CombinedDevice {
    devices: Vec<Arc<dyn Device>>,
    throttle: Option<Throttle>,
    mapping: Vec<usize>,
    capacity: usize,
    region_size: usize,
    regions: usize,
}

impl Device for CombinedDevice {
    fn capacity(&self) -> usize {
        self.capacity
    }

    fn region_size(&self) -> usize {
        self.region_size
    }

    fn throttle(&self) -> &Throttle {
        self.throttle.as_ref().unwrap_or_else(|| self.throttle())
    }

    fn translate(&self, region: RegionId, offset: u64) -> (RawFd, u64) {
        // let region = self.region_mapping[region as usize];
        // self.inner.translate(region, offset)

        let i = self.mapping.partition_point(|x| *x <= region as _);
        let region = if i == 0 {
            region
        } else {
            region - self.mapping[i - 1] as RegionId
        };
        self.devices[i].translate(region, offset)
    }

    fn regions(&self) -> usize {
        self.regions
    }
}
