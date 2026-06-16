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

use std::{
    fs::{File, OpenOptions, create_dir_all},
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

use foyer_common::error::{Error, Result};
use fs4::free_space;

use crate::{
    RawFile, Statistics, Throttle,
    iov2::{
        PAGE,
        device::{Device, DeviceBuilder},
    },
};

/// Builder for a filesystem-based device that manages files in a directory.
#[derive(Debug)]
pub struct FsDeviceBuilder {
    dir: PathBuf,
    capacity: Option<usize>,
    throttle: Throttle,
    #[cfg(target_os = "linux")]
    direct: bool,
}

impl FsDeviceBuilder {
    /// Use the given file path as the file device path.
    pub fn new(dir: impl AsRef<Path>) -> Self {
        Self {
            dir: dir.as_ref().into(),
            capacity: None,
            throttle: Throttle::default(),
            #[cfg(target_os = "linux")]
            direct: false,
        }
    }

    /// Set the capacity of the file device.
    ///
    /// The given capacity may be modified on build for alignment.
    ///
    /// The file device uses 80% of the current free disk space by default.
    pub fn with_capacity(mut self, capacity: usize) -> Self {
        self.capacity = Some(capacity);
        self
    }

    /// Set the throttle of the file device.
    pub fn with_throttle(mut self, throttle: Throttle) -> Self {
        self.throttle = throttle;
        self
    }

    /// Set whether the file device should use direct I/O.
    #[cfg(target_os = "linux")]
    pub fn with_direct(mut self, direct: bool) -> Self {
        self.direct = direct;
        self
    }
}

impl DeviceBuilder for FsDeviceBuilder {
    fn build(self) -> Result<Arc<dyn Device>> {
        // Normalize configurations.

        let align_v = |value: usize, align: usize| value - value % align;

        let capacity = self.capacity.unwrap_or({
            // Create an empty directory before to get free space.
            create_dir_all(&self.dir).unwrap();
            free_space(&self.dir).unwrap() as usize / 10 * 8
        });
        let capacity = align_v(capacity, PAGE);

        let statistics = Arc::new(Statistics::new(self.throttle));

        // Build device.

        if !self.dir.exists() {
            create_dir_all(&self.dir).map_err(Error::io_error)?;
        }

        let device = FsDevice {
            capacity,
            statistics,
            dir: self.dir,
            #[cfg(target_os = "linux")]
            direct: self.direct,
        };
        let device: Arc<dyn Device> = Arc::new(device);
        Ok(device)
    }
}

#[derive(Debug)]
pub struct FsDevice {
    capacity: usize,
    statistics: Arc<Statistics>,
    dir: PathBuf,
    #[cfg(target_os = "linux")]
    direct: bool,
}

impl FsDevice {
    // pub fn 
}

impl Device for FsDevice {}
