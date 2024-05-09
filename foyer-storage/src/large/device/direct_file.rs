//  Copyright 2024 Foyer Project Authors
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use foyer_common::{asyncify::asyncify, bits, fs::freespace};

use super::{Device, DeviceConfig, DeviceExt, RegionId};
use crate::{
    error::{Error, Result},
    large::device::{IoBuffer, ALIGN, IO_BUFFER_ALLOCATOR},
};
use std::{
    fs::{create_dir_all, File, OpenOptions},
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Debug, Clone)]
pub struct DirectFileDeviceConfig {
    pub path: PathBuf,
    pub capacity: usize,
    pub region_size: usize,
}

#[derive(Debug, Clone)]
pub struct DirectFileDevice {
    file: Arc<File>,

    capacity: usize,
    region_size: usize,
}

impl DeviceConfig for DirectFileDeviceConfig {
    fn verify(&self) -> Result<()> {
        if self.region_size == 0 || self.region_size % ALIGN != 0 {
            return Err(anyhow::anyhow!(
                "region size ({region_size}) must be a multiplier of ALIGN ({ALIGN})",
                region_size = self.region_size,
            )
            .into());
        }

        if self.capacity == 0 || self.capacity % self.region_size != 0 {
            return Err(anyhow::anyhow!(
                "capacity ({capacity}) must be a multiplier of region size ({region_size})",
                capacity = self.capacity,
                region_size = self.region_size,
            )
            .into());
        }

        Ok(())
    }
}

impl DirectFileDevice {
    #[cfg(target_os = "linux")]
    pub const O_DIRECT: i32 = 0x4000;
}

impl DirectFileDevice {
    pub async fn pwrite(&self, mut buf: IoBuffer, offset: u64) -> Result<()> {
        bits::assert_aligned(self.align() as u64, offset);

        let aligned = bits::align_up(self.align(), buf.len());
        buf.reserve(aligned - buf.len());
        unsafe { buf.set_len(aligned) };

        assert!(
            offset as usize + aligned <= self.capacity(),
            "offset ({offset}) + aligned ({aligned}) = total ({total}) <= capacity ({capacity})",
            total = offset as usize + aligned,
            capacity = self.capacity,
        );

        let file = self.file.clone();
        asyncify(move || {
            #[cfg(target_family = "unix")]
            use std::os::unix::fs::FileExt;

            #[cfg(target_family = "windows")]
            use std::os::windows::fs::FileExt;

            let written = file.write_at(buf.as_ref(), offset)?;
            if written != aligned {
                return Err(anyhow::anyhow!("written {written}, expected: {aligned}").into());
            }

            Ok(())
        })
        .await
    }

    pub async fn pread(&self, offset: u64, len: usize) -> Result<IoBuffer> {
        bits::assert_aligned(self.align() as u64, offset);

        let aligned = bits::align_up(self.align(), len);

        assert!(
            offset as usize + aligned <= self.region_size(),
            "offset ({offset}) + aligned ({aligned}) = total ({total}) <= capacity ({capacity})",
            total = offset as usize + aligned,
            capacity = self.capacity,
        );

        let mut buf = IoBuffer::with_capacity_in(aligned, &IO_BUFFER_ALLOCATOR);
        unsafe {
            buf.set_len(aligned);
        }

        let file = self.file.clone();
        let mut buffer = asyncify(move || {
            #[cfg(target_family = "unix")]
            use std::os::unix::fs::FileExt;

            #[cfg(target_family = "windows")]
            use std::os::windows::fs::FileExt;

            let read = file.read_at(buf.as_mut(), offset)?;
            if read != aligned {
                return Err(anyhow::anyhow!("read {read}, expected: {aligned}").into());
            }

            Ok::<_, Error>(buf)
        })
        .await?;

        buffer.truncate(len);

        Ok(buffer)
    }
}

impl Device for DirectFileDevice {
    type Config = DirectFileDeviceConfig;

    fn capacity(&self) -> usize {
        self.capacity
    }

    fn region_size(&self) -> usize {
        self.region_size
    }

    async fn open(config: Self::Config) -> Result<Self> {
        config.verify()?;

        let dir = config.path.parent().expect("path must point to a file").to_path_buf();
        asyncify(move || create_dir_all(dir)).await?;

        let mut opts = OpenOptions::new();

        opts.create(true).write(true).read(true);

        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.custom_flags(Self::O_DIRECT);
        }

        let file = opts.open(&config.path)?;
        file.set_len(config.capacity as _)?;
        let file = Arc::new(file);

        Ok(Self {
            file,
            capacity: config.capacity,
            region_size: config.region_size,
        })
    }

    async fn write(&self, mut buf: IoBuffer, region: RegionId, offset: u64) -> Result<()> {
        bits::assert_aligned(self.align() as u64, offset);

        let aligned = bits::align_up(self.align(), buf.len());
        buf.reserve(aligned - buf.len());
        unsafe { buf.set_len(aligned) };

        assert!(
            offset as usize + aligned <= self.region_size(),
            "offset ({offset}) + aligned ({aligned}) = total ({total}) <= region size ({region_size})",
            total = offset as usize + aligned,
            region_size = self.region_size(),
        );

        let poffset = offset + region as u64 * self.region_size as u64;
        self.pwrite(buf, poffset).await
    }

    async fn read(&self, region: RegionId, offset: u64, len: usize) -> Result<IoBuffer> {
        bits::assert_aligned(self.align() as u64, offset);

        let aligned = bits::align_up(self.align(), len);

        assert!(
            offset as usize + aligned <= self.region_size(),
            "offset ({offset}) + aligned ({aligned}) = total ({total}) <= region size ({region_size})",
            total = offset as usize + aligned,
            region_size = self.region_size(),
        );

        let poffset = offset + region as u64 * self.region_size as u64;
        self.pread(poffset, len).await
    }

    async fn flush(&self, _: Option<RegionId>) -> Result<()> {
        let file = self.file.clone();
        asyncify(move || file.sync_all().map_err(Error::from)).await
    }
}

/// [`DirectFsDeviceConfigBuilder`] is used to build the config for the direct fs device.
///
/// The direct fs device uses a directory in a file system to store the data of disk cache.
///
/// It uses direct I/O to reduce buffer copy and page cache pollution if supported.
#[derive(Debug)]
pub struct DirectFileDeviceConfigBuilder {
    path: PathBuf,
    capacity: Option<usize>,
    region_size: Option<usize>,
}

impl DirectFileDeviceConfigBuilder {
    const DEFAULT_FILE_SIZE: usize = 64 * 1024 * 1024;

    /// Use the given file path as the direct file device path.
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().into(),
            capacity: None,
            region_size: None,
        }
    }

    /// Set the capacity of the direct file device.
    ///
    /// The given capacity may be modified on build for alignment.
    ///
    /// The direct file device uses 80% of the current free disk space by default.
    pub fn with_capacity(mut self, capacity: usize) -> Self {
        self.capacity = Some(capacity);
        self
    }

    /// Set the file size of the direct file device.
    ///
    /// The given file size may be modified on build for alignment.
    ///
    /// The serialized entry size (with extra metadata) must be equal to or smaller than the file size.
    pub fn with_region_size(mut self, region_size: usize) -> Self {
        self.region_size = Some(region_size);
        self
    }

    /// Build the config of the direct file device with the given arguments.
    pub fn build(self) -> DirectFileDeviceConfig {
        let path = self.path;

        let align_v = |value: usize, align: usize| value - value % align;

        let capacity = self.capacity.unwrap_or({
            // Create an empty directory before to get freespace.
            let dir = path.parent().expect("path must point to a file").to_path_buf();
            create_dir_all(&dir).unwrap();
            freespace(&dir).unwrap() / 10 * 8
        });
        let capacity = align_v(capacity, ALIGN);

        let region_size = self.region_size.unwrap_or(Self::DEFAULT_FILE_SIZE).min(capacity);
        let region_size = align_v(region_size, ALIGN);

        let capacity = align_v(capacity, region_size);

        DirectFileDeviceConfig {
            path,
            capacity,
            region_size,
        }
    }
}

#[cfg(test)]
mod tests {
    use itertools::repeat_n;

    use super::*;

    #[test_log::test]
    fn test_config_builder() {
        let dir = tempfile::tempdir().unwrap();

        let config = DirectFileDeviceConfigBuilder::new(dir.path().join("test-direct-file")).build();

        tracing::debug!("{config:?}");

        config.verify().unwrap();
    }

    #[test_log::test]

    fn test_config_builder_noent() {
        let dir = tempfile::tempdir().unwrap();

        let config = DirectFileDeviceConfigBuilder::new(dir.path().join("noent").join("test-direct-file")).build();

        tracing::debug!("{config:?}");

        config.verify().unwrap();
    }

    #[test_log::test(tokio::test)]
    async fn test_direct_file_device_io() {
        let dir = tempfile::tempdir().unwrap();

        let config = DirectFileDeviceConfigBuilder::new(dir.path().join("test-direct-file"))
            .with_capacity(4 * 1024 * 1024)
            .with_region_size(1024 * 1024)
            .build();

        tracing::debug!("{config:?}");

        let device = DirectFileDevice::open(config.clone()).await.unwrap();

        let mut buf = IoBuffer::with_capacity_in(64 * 1024, &IO_BUFFER_ALLOCATOR);
        buf.extend(repeat_n(b'x', 64 * 1024 - 100));

        device.write(buf.clone(), 0, 4096).await.unwrap();

        let b = device.read(0, 4096, 64 * 1024 - 100).await.unwrap();
        assert_eq!(buf, b);

        device.flush(None).await.unwrap();

        drop(device);

        let device = DirectFileDevice::open(config).await.unwrap();

        let b = device.read(0, 4096, 64 * 1024 - 100).await.unwrap();
        assert_eq!(buf, b);
    }
}
