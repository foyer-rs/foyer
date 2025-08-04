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
    sync::{Arc, RwLock},
};

use fs4::free_space;

use crate::{
    io::{
        device::{statistics::Statistics, throttle::Throttle, Device, DeviceBuilder, Partition, PartitionId},
        error::IoResult,
        PAGE,
    },
    IoError, RawFile,
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
    fn build(self) -> IoResult<Arc<dyn Device>> {
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
            create_dir_all(&self.dir)?;
        }

        let device = FsDevice {
            capacity,
            statistics,
            dir: self.dir,
            #[cfg(target_os = "linux")]
            direct: self.direct,
            partitions: RwLock::new(vec![]),
        };
        let device: Arc<dyn Device> = Arc::new(device);
        Ok(device)
    }
}

/// A device upon a directory in a filesystem.
#[derive(Debug)]
pub struct FsDevice {
    capacity: usize,
    statistics: Arc<Statistics>,
    dir: PathBuf,
    #[cfg(target_os = "linux")]
    direct: bool,
    partitions: RwLock<Vec<Arc<FsPartition>>>,
}

impl FsDevice {
    const PREFIX: &str = "foyer-storage-direct-fs-";
    fn filename(partition: PartitionId) -> String {
        format!("{prefix}{partition:08}", prefix = Self::PREFIX,)
    }
}

impl Device for FsDevice {
    fn capacity(&self) -> usize {
        self.capacity
    }

    fn allocated(&self) -> usize {
        self.partitions.read().unwrap().iter().map(|p| p.size).sum()
    }

    fn create_partition(&self, size: usize) -> IoResult<Arc<dyn Partition>> {
        let mut partitions = self.partitions.write().unwrap();
        let allocated = partitions.iter().map(|p| p.size).sum::<usize>();
        if allocated + size > self.capacity {
            return Err(IoError::NoSpace {
                capacity: self.capacity,
                allocated,
                required: allocated + size,
            });
        }
        let id = partitions.len() as PartitionId;
        let path = self.dir.join(Self::filename(id));
        let mut opts = OpenOptions::new();
        opts.create(true).write(true).read(true);
        #[cfg(target_os = "linux")]
        if self.direct {
            use std::os::unix::fs::OpenOptionsExt;
            opts.custom_flags(libc::O_DIRECT | libc::O_NOATIME);
        }
        let file = opts.open(path)?;
        file.set_len(size as _)?;

        let partition = Arc::new(FsPartition {
            id,
            size,
            file,
            staticistics: self.statistics.clone(),
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

    fn statistics(&self) -> &Arc<Statistics> {
        &self.statistics
    }
}

#[derive(Debug)]
pub struct FsPartition {
    id: PartitionId,
    size: usize,
    staticistics: Arc<Statistics>,
    file: File,
}

impl Partition for FsPartition {
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

        (raw, address)
    }

    fn statistics(&self) -> &Arc<Statistics> {
        &self.staticistics
    }
}
