//  Copyright 2024 foyer Project Authors
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

use std::{
    fs::{create_dir_all, File, OpenOptions},
    path::{Path, PathBuf},
    sync::Arc,
};

use foyer_common::{asyncify::asyncify_with_runtime, bits};
use fs4::free_space;
use serde::{Deserialize, Serialize};

use super::{Dev, DevExt, RegionId};
use crate::{
    device::ALIGN,
    error::{Error, Result},
    IoBytes, IoBytesMut, Runtime,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectFileDeviceConfig {
    path: PathBuf,
    capacity: usize,
    region_size: usize,
}

impl DirectFileDeviceConfig {
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

/// A device that uses a single direct i/o file.
#[derive(Debug, Clone)]
pub struct DirectFileDevice {
    file: Arc<File>,

    capacity: usize,
    region_size: usize,

    runtime: Runtime,
}

impl DirectFileDevice {
    /// Positioned write API for the direct file device.
    #[fastrace::trace(name = "foyer::storage::device::direct_file::pwrite")]
    pub async fn pwrite(&self, buf: IoBytes, offset: u64) -> Result<()> {
        let aligned = buf.as_aligned().len();

        assert!(
            offset as usize + aligned <= self.capacity(),
            "offset ({offset}) + aligned ({aligned}) = total ({total}) <= capacity ({capacity})",
            total = offset as usize + aligned,
            capacity = self.capacity,
        );

        let file = self.file.clone();

        asyncify_with_runtime(self.runtime.write(), move || {
            #[cfg(target_family = "windows")]
            let written = {
                use std::os::windows::fs::FileExt;
                file.seek_write(buf.as_aligned(), offset)?
            };

            #[cfg(target_family = "unix")]
            let written = {
                use std::os::unix::fs::FileExt;
                file.write_at(buf.as_aligned(), offset)?
            };

            if written != aligned {
                return Err(anyhow::anyhow!("written {written}, expected: {aligned}").into());
            }

            Ok(())
        })
        .await
    }

    /// Positioned read API for the direct file device.
    #[fastrace::trace(name = "foyer::storage::device::direct_file::pread")]
    pub async fn pread(&self, offset: u64, len: usize) -> Result<IoBytesMut> {
        bits::assert_aligned(self.align() as u64, offset);

        let aligned = bits::align_up(self.align(), len);

        assert!(
            offset as usize + aligned <= self.capacity(),
            "offset ({offset}) + aligned ({aligned}) = total ({total}) <= capacity ({capacity})",
            total = offset as usize + aligned,
            capacity = self.capacity,
        );

        let mut buf = IoBytesMut::with_capacity(aligned);
        unsafe {
            buf.set_len(aligned);
        }

        let file = self.file.clone();

        let mut buffer = asyncify_with_runtime(self.runtime.read(), move || {
            #[cfg(target_family = "windows")]
            let read = {
                use std::os::windows::fs::FileExt;
                file.seek_read(buf.as_mut(), offset)?
            };

            #[cfg(target_family = "unix")]
            let read = {
                use std::os::unix::fs::FileExt;
                file.read_at(buf.as_mut(), offset)?
            };

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

impl Dev for DirectFileDevice {
    type Config = DirectFileDeviceConfig;

    fn capacity(&self) -> usize {
        self.capacity
    }

    fn region_size(&self) -> usize {
        self.region_size
    }

    #[fastrace::trace(name = "foyer::storage::device::direct_file::open")]
    async fn open(options: Self::Config, runtime: Runtime) -> Result<Self> {
        options.verify()?;

        let dir = options
            .path
            .parent()
            .expect("path must not be the root directory")
            .to_path_buf();
        if !dir.exists() {
            create_dir_all(dir)?;
        }

        let mut opts = OpenOptions::new();

        opts.create(true).write(true).read(true);

        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.custom_flags(libc::O_DIRECT | libc::O_NOATIME);
        }

        let file = opts.open(&options.path)?;

        if file.metadata().unwrap().is_file() {
            tracing::warn!(
                "{} {} {}",
                "It seems a `DirectFileDevice` is used within a normal file system, which is inefficient.",
                "Please use `DirectFileDevice` directly on a raw block device.",
                "Or use `DirectFsDevice` within a normal file system.",
            );
            file.set_len(options.capacity as _)?;
        }

        let file = Arc::new(file);

        Ok(Self {
            file,
            capacity: options.capacity,
            region_size: options.region_size,
            runtime,
        })
    }

    #[fastrace::trace(name = "foyer::storage::device::direct_file::write")]
    async fn write(&self, buf: IoBytes, region: RegionId, offset: u64) -> Result<()> {
        let aligned = buf.as_aligned().len();

        assert!(
            offset as usize + aligned <= self.region_size(),
            "offset ({offset}) + aligned ({aligned}) = total ({total}) <= region size ({region_size})",
            total = offset as usize + aligned,
            region_size = self.region_size(),
        );

        let poffset = offset + region as u64 * self.region_size as u64;
        self.pwrite(buf, poffset).await
    }

    #[fastrace::trace(name = "foyer::storage::device::direct_file::read")]
    async fn read(&self, region: RegionId, offset: u64, len: usize) -> Result<IoBytesMut> {
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

    #[fastrace::trace(name = "foyer::storage::device::direct_file::flush")]
    async fn flush(&self, _: Option<RegionId>) -> Result<()> {
        let file = self.file.clone();
        asyncify_with_runtime(self.runtime.write(), move || file.sync_all().map_err(Error::from)).await
    }
}

/// [`DirectFileDeviceOptions`] is used to build the options for the direct fs device.
///
/// The direct fs device uses a directory in a file system to store the data of disk cache.
///
/// It uses direct I/O to reduce buffer copy and page cache pollution if supported.
#[derive(Debug)]
pub struct DirectFileDeviceOptions {
    path: PathBuf,
    capacity: Option<usize>,
    region_size: Option<usize>,
}

impl DirectFileDeviceOptions {
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
}

impl From<DirectFileDeviceOptions> for DirectFileDeviceConfig {
    fn from(options: DirectFileDeviceOptions) -> Self {
        let path = options.path;

        let align_v = |value: usize, align: usize| value - value % align;

        let capacity = options.capacity.unwrap_or({
            // Create an empty directory before to get free space.
            let dir = path.parent().expect("path must point to a file").to_path_buf();
            create_dir_all(&dir).unwrap();
            free_space(&dir).unwrap() as usize / 10 * 8
        });
        let capacity = align_v(capacity, ALIGN);

        let region_size = options
            .region_size
            .unwrap_or(DirectFileDeviceOptions::DEFAULT_FILE_SIZE)
            .min(capacity);
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
    fn test_options_builder() {
        let dir = tempfile::tempdir().unwrap();

        let config: DirectFileDeviceConfig = DirectFileDeviceOptions::new(dir.path().join("test-direct-file")).into();

        tracing::debug!("{config:?}");

        config.verify().unwrap();
    }

    #[test_log::test]

    fn test_options_builder_noent() {
        let dir = tempfile::tempdir().unwrap();

        let config: DirectFileDeviceConfig =
            DirectFileDeviceOptions::new(dir.path().join("noent").join("test-direct-file")).into();

        tracing::debug!("{config:?}");

        config.verify().unwrap();
    }

    #[test_log::test(tokio::test)]
    async fn test_direct_file_device_io() {
        let dir = tempfile::tempdir().unwrap();
        let runtime = Runtime::current();

        let config: DirectFileDeviceConfig = DirectFileDeviceOptions::new(dir.path().join("test-direct-file"))
            .with_capacity(4 * 1024 * 1024)
            .with_region_size(1024 * 1024)
            .into();

        tracing::debug!("{config:?}");

        let device = DirectFileDevice::open(config.clone(), runtime.clone()).await.unwrap();

        let mut buf = IoBytesMut::with_capacity(64 * 1024);
        buf.extend(repeat_n(b'x', 64 * 1024 - 100));
        let buf = buf.freeze();

        device.write(buf.clone(), 0, 4096).await.unwrap();

        let b = device.read(0, 4096, 64 * 1024 - 100).await.unwrap().freeze();
        assert_eq!(buf, b);

        device.flush(None).await.unwrap();

        drop(device);

        let device = DirectFileDevice::open(config, runtime).await.unwrap();

        let b = device.read(0, 4096, 64 * 1024 - 100).await.unwrap().freeze();
        assert_eq!(buf, b);
    }
}
