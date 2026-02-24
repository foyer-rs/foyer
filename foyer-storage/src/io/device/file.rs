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
    RawFile,
    engine::block::device::{Partition, PartitionId, PartitionableDevice},
    io::{
        PAGE,
        device::{Device, DeviceBuilder, statistics::Statistics, throttle::Throttle},
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
        let capacity = align_v(capacity, PAGE);

        println!("==========> {capacity}");

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
            file.set_len(capacity as _).map_err(Error::io_error)?;
        }
        let file = Arc::new(file);

        let statistics = Arc::new(Statistics::new(self.throttle));

        let device = FileDevice {
            file,
            capacity,
            statistics,
            partitions: RwLock::new(vec![]),
        };
        let device: Arc<dyn Device> = Arc::new(device);
        Ok(device)
    }
}

/// A device upon a single file or a raw block device.
#[derive(Debug)]
pub struct FileDevice {
    file: Arc<File>,
    capacity: usize,
    partitions: RwLock<Vec<Arc<FilePartition>>>,
    statistics: Arc<Statistics>,
}

impl Device for FileDevice {
    fn capacity(&self) -> usize {
        self.capacity
    }

    fn allocated(&self) -> usize {
        self.partitions.read().unwrap().iter().map(|p| p.size).sum()
    }

    fn statistics(&self) -> &Arc<Statistics> {
        &self.statistics
    }
}

impl PartitionableDevice for FileDevice {
    fn create_partition(&self, size: usize) -> Result<Arc<dyn Partition>> {
        let mut partitions = self.partitions.write().unwrap();
        let allocated = partitions.iter().map(|p| p.size).sum::<usize>();
        if allocated + size > self.capacity {
            return Err(Error::no_space(self.capacity, allocated, allocated + size));
        }
        let offset = partitions.last().map(|p| p.offset + p.size as u64).unwrap_or_default();
        let id = partitions.len() as PartitionId;
        let partition = Arc::new(FilePartition {
            file: self.file.clone(),
            id,
            size,
            offset,
            statistics: self.statistics.clone(),
        });
        partitions.push(partition.clone());
        Ok(partition)
    }

    fn partitions(&self) -> usize {
        self.partitions.read().unwrap().len()
    }

    fn partition(&self, id: PartitionId) -> Arc<dyn Partition> {
        self.partitions.read().unwrap()[id as usize].clone()
    }
}

#[derive(Debug)]
pub struct FilePartition {
    file: Arc<File>,
    id: PartitionId,
    size: usize,
    offset: u64,
    statistics: Arc<Statistics>,
}

impl Partition for FilePartition {
    fn id(&self) -> PartitionId {
        self.id
    }

    fn size(&self) -> usize {
        self.size
    }

    fn translate(&self, address: u64) -> (RawFile, u64) {
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

        let address = self.offset + address;
        (raw, address)
    }

    fn statistics(&self) -> &Arc<Statistics> {
        &self.statistics
    }
}
