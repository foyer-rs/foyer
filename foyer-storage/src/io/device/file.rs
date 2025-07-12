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

use std::{
    fs::{create_dir_all, File, OpenOptions},
    path::{Path, PathBuf},
    sync::Arc,
};

use fs4::free_space;

use crate::{
    io::{
        device::{Device, DeviceBuilder, RegionId},
        error::{IoError, IoResult},
        throttle::Throttle,
        PAGE,
    },
    RawFile,
};

/// Builder for a file-based device that manages a single file or a raw block device.
#[derive(Debug)]
pub struct FileDeviceBuilder {
    path: PathBuf,
    capacity: Option<usize>,
    region_size: Option<usize>,
    throttle: Throttle,
    direct: bool,
}

impl FileDeviceBuilder {
    const DEFAULT_REGION_SIZE: usize = 64 * 1024 * 1024;

    /// Use the given file path as the file device path.
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().into(),
            capacity: None,
            region_size: None,
            throttle: Throttle::default(),
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

    /// Set the file size of the file device.
    ///
    /// The given file size may be modified on build for alignment.
    ///
    /// The serialized entry size (with extra metadata) must be equal to or smaller than the file size.
    pub fn with_region_size(mut self, region_size: usize) -> Self {
        self.region_size = Some(region_size);
        self
    }

    /// Set the throttle of the file device.
    pub fn with_throttle(mut self, throttle: Throttle) -> Self {
        self.throttle = throttle;
        self
    }

    /// Set whether the file device should use direct I/O.
    pub fn with_direct(mut self, direct: bool) -> Self {
        self.direct = direct;
        self
    }
}

impl DeviceBuilder for FileDeviceBuilder {
    fn build(self: Box<Self>) -> IoResult<Arc<dyn Device>> {
        // Normalize configurations.

        let align_v = |value: usize, align: usize| value - (value % align);

        let capacity = self.capacity.unwrap_or({
            // Create an empty directory before to get free space.
            let dir = self.path.parent().expect("path must point to a file").to_path_buf();
            create_dir_all(&dir).unwrap();
            free_space(&dir).unwrap() as usize / 10 * 8
        });
        let capacity = align_v(capacity, PAGE);

        let region_size = self
            .region_size
            .unwrap_or(Self::DEFAULT_REGION_SIZE)
            .min(capacity / 16)
            .max(PAGE);
        let region_size = align_v(region_size, PAGE);

        let capacity = align_v(capacity, region_size);

        // Verify configurations.

        if region_size == 0 || region_size % PAGE != 0 {
            return Err(IoError::other(anyhow::anyhow!(
                "region size ({region_size}) must be a multiplier of PAGE ({PAGE})",
            )));
        }

        if capacity == 0 || capacity % region_size != 0 {
            return Err(IoError::other(anyhow::anyhow!(
                "capacity ({capacity}) must be a multiplier of region size ({region_size})",
            )));
        }

        // Build device.

        let mut opts = OpenOptions::new();
        opts.create(true).write(true).read(true);
        #[cfg(target_os = "linux")]
        if self.direct {
            use std::os::unix::fs::OpenOptionsExt;
            opts.custom_flags(libc::O_DIRECT | libc::O_NOATIME);
        }

        let file = opts.open(&self.path)?;
        file.set_len(capacity as _)?;
        let throttle = self.throttle;

        let inner = Inner {
            file,
            capacity,
            region_size,
            throttle,
        };
        let inner = Arc::new(inner);
        let device = FileDevice { inner };
        let device: Arc<dyn Device> = Arc::new(device);
        Ok(device)
    }
}

#[derive(Debug)]
struct Inner {
    file: File,
    capacity: usize,
    region_size: usize,
    throttle: Throttle,
}

/// A device upon a single file or a raw block device.
#[derive(Debug, Clone)]
pub struct FileDevice {
    inner: Arc<Inner>,
}

impl Device for FileDevice {
    fn capacity(&self) -> usize {
        self.inner.capacity
    }

    fn region_size(&self) -> usize {
        self.inner.region_size
    }

    fn throttle(&self) -> &Throttle {
        &self.inner.throttle
    }

    fn translate(&self, region: RegionId, offset: u64) -> (RawFile, u64) {
        #[cfg(any(target_family = "unix", target_family = "wasm"))]
        let raw = {
            use std::os::fd::AsRawFd;
            self.inner.file.as_raw_fd()
        };

        #[cfg(target_family = "windows")]
        let raw = {
            use std::os::windows::io::AsRawHandle;
            self.inner.file.as_raw_handle()
        };

        let offset = region as u64 * self.inner.region_size as u64 + offset;
        (raw, offset)
    }
}
