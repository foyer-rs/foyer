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

/// Builder for a filesystem-based device that manages files in a directory.
#[derive(Debug)]
pub struct FsDeviceBuilder {
    dir: PathBuf,
    capacity: Option<usize>,
    file_size: Option<usize>,
    throttle: Throttle,
    direct: bool,
}

impl FsDeviceBuilder {
    const DEFAULT_FILE_SIZE: usize = 64 * 1024 * 1024;

    /// Use the given file path as the file device path.
    pub fn new(dir: impl AsRef<Path>) -> Self {
        Self {
            dir: dir.as_ref().into(),
            capacity: None,
            file_size: None,
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

    /// Set the file size of each file of the fs device.
    ///
    /// The given file size may be modified on build for alignment.
    ///
    /// The serialized entry size (with extra metadata) must be equal to or smaller than the file size.
    pub fn with_file_size(mut self, file_size: usize) -> Self {
        self.file_size = Some(file_size);
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

impl DeviceBuilder for FsDeviceBuilder {
    fn build(self: Box<Self>) -> IoResult<Arc<dyn Device>> {
        // Normalize configurations.

        let align_v = |value: usize, align: usize| value - value % align;

        let capacity = self.capacity.unwrap_or({
            // Create an empty directory before to get free space.
            create_dir_all(&self.dir).unwrap();
            free_space(&self.dir).unwrap() as usize / 10 * 8
        });
        let capacity = align_v(capacity, PAGE);

        let file_size = self.file_size.unwrap_or(Self::DEFAULT_FILE_SIZE).min(capacity);
        let file_size = align_v(file_size, PAGE);

        let capacity = align_v(capacity, file_size);

        let throttle = self.throttle;

        // Verify configurations.

        if file_size == 0 || file_size % PAGE != 0 {
            return Err(IoError::other(anyhow::anyhow!(
                "file size ({file_size}) must be a multiplier of PAGE ({PAGE})",
            )));
        }

        if capacity == 0 || capacity % file_size != 0 {
            return Err(IoError::other(anyhow::anyhow!(
                "capacity ({capacity}) must be a multiplier of file size ({file_size})",
            )));
        }

        // Build device.

        let regions = capacity / file_size;

        if !self.dir.exists() {
            create_dir_all(&self.dir)?;
        }

        const PREFIX: &str = "foyer-storage-direct-fs-";
        let filename = |region: RegionId| format!("{PREFIX}{region:08}");

        let mut files = Vec::with_capacity(regions);

        for region in 0..regions as RegionId {
            let path = self.dir.join(filename(region as RegionId));
            let mut opts = OpenOptions::new();
            opts.create(true).write(true).read(true);
            #[cfg(target_os = "linux")]
            if self.direct {
                use std::os::unix::fs::OpenOptionsExt;
                opts.custom_flags(libc::O_DIRECT | libc::O_NOATIME);
            }
            let file = opts.open(path)?;
            file.set_len(file_size as _)?;
            let file = Arc::new(file);
            files.push(file);
        }

        let inner = Inner {
            files,
            capacity,
            file_size,
            throttle,
        };
        let inner = Arc::new(inner);
        let device = FsDevice { inner };
        let device: Arc<dyn Device> = Arc::new(device);
        Ok(device)
    }
}

#[derive(Debug)]
struct Inner {
    files: Vec<Arc<File>>,
    capacity: usize,
    file_size: usize,
    throttle: Throttle,
}

/// A device upon a directory in a filesystem.
#[derive(Debug, Clone)]
pub struct FsDevice {
    inner: Arc<Inner>,
}

impl Device for FsDevice {
    fn capacity(&self) -> usize {
        self.inner.capacity
    }

    fn region_size(&self) -> usize {
        self.inner.file_size
    }

    fn throttle(&self) -> &Throttle {
        &self.inner.throttle
    }

    fn translate(&self, region: RegionId, offset: u64) -> (RawFile, u64) {
        #[cfg(any(target_family = "unix", target_family = "wasm"))]
        let raw = {
            use std::os::fd::AsRawFd;
            self.inner.files[region as usize].as_raw_fd()
        };

        #[cfg(target_family = "windows")]
        let raw = {
            use std::os::windows::io::AsRawHandle;
            self.inner.files[region as usize].as_raw_handle()
        };

        (raw, offset)
    }
}
