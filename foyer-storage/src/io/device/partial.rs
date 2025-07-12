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

use std::sync::Arc;

use crate::{
    io::{
        device::{Device, DeviceBuilder, RegionId},
        error::IoResult,
        throttle::Throttle,
    },
    RawFile,
};

/// Builder for a partial device that wraps another device and allows access to only a subset of its regions.
#[derive(Debug)]
pub struct PartialDeviceBuilder {
    device: Arc<dyn Device>,
    region_mapping: Vec<RegionId>,
    throttle: Option<Throttle>,
}

impl PartialDeviceBuilder {
    /// Create a new partial device builder with the specified device.
    ///
    /// It uses none of the regions by default.
    pub fn new(device: Arc<dyn Device>) -> Self {
        Self {
            device,
            region_mapping: vec![],
            throttle: None,
        }
    }

    /// Set the region mapping for the partial device.
    pub fn with_region_mapping(mut self, region_mapping: Vec<RegionId>) -> Self {
        self.region_mapping = region_mapping;
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
            region_mapping: self.region_mapping,
            throttle: self.throttle,
        }))
    }
}

/// [`PartialDevice`] is a wrapper for other device to use only a part of it.
#[derive(Debug)]
pub struct PartialDevice {
    inner: Arc<dyn Device>,
    /// Mapping region id as index to the inner device's region.
    region_mapping: Vec<RegionId>,
    throttle: Option<Throttle>,
}

impl Device for PartialDevice {
    fn capacity(&self) -> usize {
        self.inner.region_size() * self.region_mapping.len()
    }

    fn region_size(&self) -> usize {
        self.inner.region_size()
    }

    fn throttle(&self) -> &Throttle {
        self.throttle.as_ref().unwrap_or_else(|| self.inner.throttle())
    }

    fn translate(&self, region: RegionId, offset: u64) -> (RawFile, u64) {
        let region = self.region_mapping[region as usize];
        self.inner.translate(region, offset)
    }
}
