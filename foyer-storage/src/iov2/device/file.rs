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

/// Builder for a file-based device that manages a single file or a raw block device.
#[derive(Debug)]
pub struct FileDeviceBuilder {
    path: PathBuf,
    capacity: Option<usize>,
    throttle: Throttle,
    #[cfg(target_os = "linux")]
    direct: bool,
}

impl FileDeviceBuilder {
    /// Use the given file path as the file device path.
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().into(),
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

impl DeviceBuilder for FileDeviceBuilder {
    fn build(self) -> Result<Arc<dyn Device>> {
        // Normalize configurations.

        let align_v = |value: usize, align: usize| value - (value % align);

        let capacity = self.capacity.unwrap_or_else(|| {
            // Try to get the capacity if `path` refer to a raw block device.
            #[cfg(unix)]
            if let Ok(metadata) = std::fs::metadata(&self.path) {
                let file_type = metadata.file_type();

                use std::os::unix::fs::FileTypeExt;
                if file_type.is_block_device() {
                    return super::utils::get_dev_capacity(&self.path).unwrap();
                }
            }

            // Create an empty directory if needed before to get free space.
            let dir = self.path.parent().expect("path must point to a file").to_path_buf();
            create_dir_all(&dir).unwrap();
            free_space(&dir).unwrap() as usize / 10 * 8
        });
        let size = align_v(capacity, PAGE);

        println!("==========> {size}");

        // Build device.

        let mut opts = OpenOptions::new();
        opts.create(true).write(true).read(true);
        #[cfg(target_os = "linux")]
        if self.direct {
            use std::os::unix::fs::OpenOptionsExt;
            opts.custom_flags(libc::O_DIRECT | libc::O_NOATIME);
        }

        let file = opts.open(&self.path).map_err(Error::io_error)?;

        if file.metadata().unwrap().is_file() {
            tracing::warn!(
                "{} {} {}",
                "It seems a `DirectFileDevice` is used within a normal file system, which is inefficient.",
                "Please use `DirectFileDevice` directly on a raw block device.",
                "Or use `DirectFsDevice` within a normal file system.",
            );
            file.set_len(size as _).map_err(Error::io_error)?;
        }
        let file = Arc::new(file);

        let statistics = Arc::new(Statistics::new(self.throttle));

        let device = FileDevice {
            file,
            offset: 0,
            size,
            statistics,
        };
        let device: Arc<dyn Device> = Arc::new(device);
        Ok(device)
    }
}

/// A device upon a single file or a raw block device.
#[derive(Debug)]
pub struct FileDevice {
    file: Arc<File>,
    offset: u64,
    size: usize,
    statistics: Arc<Statistics>,
}

impl FileDevice {
    /// Resolve the raw file handle and offset for the given position.
    pub fn resolve(&self, pos: u64) -> (RawFile, u64) {
        #[cfg(any(target_family = "unix", target_family = "wasm"))]
        let raw = {
            use std::os::fd::AsRawFd;
            RawFile(self.file.as_raw_fd())
        };

        #[cfg(target_family = "windows")]
        let raw = {
            use std::os::windows::io::AsRawHandle;
            RawFile(self.file.as_raw_handle())
        };

        let pos = self.offset + pos;

        (raw, pos)
    }

    /// Split the file device into two at the given position.
    pub fn split(self: &Arc<Self>, pos: u64) -> (Arc<Self>, Arc<Self>) {
        let left = Arc::new(Self {
            file: self.file.clone(),
            offset: self.offset,
            size: pos as usize,
            statistics: self.statistics.clone(),
        });

        let right = Arc::new(Self {
            file: self.file.clone(),
            offset: self.offset + pos,
            size: self.size - pos as usize,
            statistics: self.statistics.clone(),
        });

        (left, right)
    }

    /// Get the offset of the file device to the underlying file.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Get the size of the file device.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Get the statistics of the file device.
    pub fn statistics(&self) -> &Arc<Statistics> {
        &self.statistics
    }
}

impl Device for FileDevice {}
