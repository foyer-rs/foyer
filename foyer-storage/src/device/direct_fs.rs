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

use std::{
    fs::{create_dir_all, File, OpenOptions},
    io::{Read, Seek, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use foyer_common::{asyncify::asyncify_with_runtime, bits, fs::freespace};
use futures::future::try_join_all;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;

use super::{Dev, DevExt, DevOptions, RegionId};
use crate::{
    device::ALIGN,
    error::{Error, Result},
    IoBytes, IoBytesMut,
};

/// Options for the direct fs device.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectFsDeviceOptions {
    /// Directory of the direct fs device.
    pub dir: PathBuf,
    /// Capacity of the direct fs device.
    pub capacity: usize,
    /// Direct i/o file size of the direct fs device.
    pub file_size: usize,
}

/// A device that uses direct i/o files in a directory of a file system.
#[derive(Debug, Clone)]
pub struct DirectFsDevice {
    inner: Arc<DirectFsDeviceInner>,
}

#[derive(Debug)]
struct DirectFsDeviceInner {
    files: Vec<Arc<File>>,

    capacity: usize,
    file_size: usize,

    runtime: Handle,
}

impl DevOptions for DirectFsDeviceOptions {
    fn verify(&self) -> Result<()> {
        if self.file_size == 0 || self.file_size % ALIGN != 0 {
            return Err(anyhow::anyhow!(
                "file size ({file_size}) must be a multiplier of ALIGN ({ALIGN})",
                file_size = self.file_size
            )
            .into());
        }

        if self.capacity == 0 || self.capacity % self.file_size != 0 {
            return Err(anyhow::anyhow!(
                "capacity ({capacity}) must be a multiplier of file size ({file_size})",
                capacity = self.capacity,
                file_size = self.file_size
            )
            .into());
        }

        Ok(())
    }
}

impl DirectFsDevice {
    const PREFIX: &'static str = "foyer-storage-direct-fs-";

    fn filename(region: RegionId) -> String {
        format!("{}{:08}", Self::PREFIX, region)
    }

    fn file(&self, region: RegionId) -> &Arc<File> {
        &self.inner.files[region as usize]
    }
}

impl Dev for DirectFsDevice {
    type Options = DirectFsDeviceOptions;

    fn capacity(&self) -> usize {
        self.inner.capacity
    }

    fn region_size(&self) -> usize {
        self.inner.file_size
    }

    #[fastrace::trace(name = "foyer::storage::device::direct_fs::open")]
    async fn open(options: Self::Options) -> Result<Self> {
        let runtime = Handle::current();

        options.verify()?;

        // TODO(MrCroxx): write and read options to a manifest file for pinning

        let regions = options.capacity / options.file_size;

        let path = options.dir.clone();
        asyncify_with_runtime(&runtime, move || create_dir_all(path)).await?;

        let futures = (0..regions)
            .map(|i| {
                let path = options.dir.clone().join(Self::filename(i as RegionId));
                async {
                    let mut opts = OpenOptions::new();

                    opts.create(true).write(true).read(true);

                    #[cfg(target_os = "linux")]
                    {
                        use std::os::unix::fs::OpenOptionsExt;
                        opts.custom_flags(libc::O_DIRECT | libc::O_NOATIME);
                    }

                    let file = opts.open(path)?;
                    file.set_len(options.file_size as _)?;
                    let file = Arc::new(file);

                    Ok::<_, Error>(file)
                }
            })
            .collect_vec();
        let files = try_join_all(futures).await?;

        Ok(Self {
            inner: Arc::new(DirectFsDeviceInner {
                files,
                capacity: options.capacity,
                file_size: options.file_size,
                runtime,
            }),
        })
    }

    #[fastrace::trace(name = "foyer::storage::device::direct_fs::write")]
    async fn write(&self, buf: IoBytes, region: RegionId, offset: u64) -> Result<()> {
        let aligned = buf.as_aligned().len();

        assert!(
            offset as usize + aligned <= self.region_size(),
            "offset ({offset}) + aligned ({aligned}) = total ({total}) <= region size ({region_size})",
            total = offset as usize + aligned,
            region_size = self.region_size(),
        );

        let mut file = self.file(region).clone();
        asyncify_with_runtime(&self.inner.runtime, move || {
            #[cfg(target_family = "unix")]
            use std::os::unix::fs::FileExt;
            #[cfg(target_family = "windows")]
            let written = {
                let original = file.stream_position()?;
                file.seek(std::io::SeekFrom::Start(offset))?;
                let written = file.write(buf.as_aligned())?;
                file.seek(std::io::SeekFrom::Start(original))?;
                written
            };

            #[cfg(target_family = "unix")]
            let written = file.write_at(buf.as_aligned(), offset)?;

            if written != aligned {
                return Err(anyhow::anyhow!("written {written}, expected: {aligned}").into());
            }

            Ok(())
        })
        .await
    }

    #[fastrace::trace(name = "foyer::storage::device::direct_fs::read")]
    async fn read(&self, region: RegionId, offset: u64, len: usize) -> Result<IoBytesMut> {
        bits::assert_aligned(self.align() as u64, offset);

        let aligned = bits::align_up(self.align(), len);

        assert!(
            offset as usize + aligned <= self.region_size(),
            "offset ({offset}) + aligned ({aligned}) = total ({total}) <= region size ({region_size})",
            total = offset as usize + aligned,
            region_size = self.region_size(),
        );

        let mut buf = IoBytesMut::with_capacity(aligned);
        unsafe {
            buf.set_len(aligned);
        }

        let mut file = self.file(region).clone();
        let mut buffer = asyncify_with_runtime(&self.inner.runtime, move || {
            #[cfg(target_family = "unix")]
            use std::os::unix::fs::FileExt;
            #[cfg(target_family = "unix")]
            let read = file.read_at(buf.as_mut(), offset)?;

            #[cfg(target_family = "windows")]
            let read = {
                let original = file.stream_position()?;
                file.seek(std::io::SeekFrom::Start(offset))?;
                let read = file.read(buf.as_mut())?;
                file.seek(std::io::SeekFrom::Start(original))?;
                read
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

    #[fastrace::trace(name = "foyer::storage::device::direct_fs::flush")]
    async fn flush(&self, region: Option<super::RegionId>) -> Result<()> {
        let flush = |region: RegionId| {
            let file = self.file(region).clone();
            asyncify_with_runtime(&self.inner.runtime, move || file.sync_all().map_err(Error::from))
        };

        if let Some(region) = region {
            flush(region).await
        } else {
            try_join_all((0..self.regions() as RegionId).map(flush))
                .await
                .map(|_| ())
        }
    }
}

/// [`DirectFsDeviceOptionsBuilder`] is used to build the options for the direct fs device.
///
/// The direct fs device uses a directory in a file system to store the data of disk cache.
///
/// It uses direct I/O to reduce buffer copy and page cache pollution if supported.
#[derive(Debug)]
pub struct DirectFsDeviceOptionsBuilder {
    dir: PathBuf,
    capacity: Option<usize>,
    file_size: Option<usize>,
}

impl DirectFsDeviceOptionsBuilder {
    const DEFAULT_FILE_SIZE: usize = 64 * 1024 * 1024;

    /// Use the given `dir` as the direct fs device.
    pub fn new(dir: impl AsRef<Path>) -> Self {
        Self {
            dir: dir.as_ref().into(),
            capacity: None,
            file_size: None,
        }
    }

    /// Set the capacity of the direct fs device.
    ///
    /// The given capacity may be modified on build for alignment.
    ///
    /// The direct fs device uses 80% of the current free disk space by default.
    pub fn with_capacity(mut self, capacity: usize) -> Self {
        self.capacity = Some(capacity);
        self
    }

    /// Set the file size of the direct fs device.
    ///
    /// The given file size may be modified on build for alignment.
    ///
    /// The serialized entry size (with extra metadata) must be equal to or smaller than the file size.
    pub fn with_file_size(mut self, file_size: usize) -> Self {
        self.file_size = Some(file_size);
        self
    }

    /// Build the options of the direct fs device with the given arguments.
    pub fn build(self) -> DirectFsDeviceOptions {
        let dir = self.dir;

        let align_v = |value: usize, align: usize| value - value % align;

        let capacity = self.capacity.unwrap_or({
            // Create an empty directory before to get freespace.
            create_dir_all(&dir).unwrap();
            freespace(&dir).unwrap() as usize / 10 * 8
        });
        let capacity = align_v(capacity, ALIGN);

        let file_size = self.file_size.unwrap_or(Self::DEFAULT_FILE_SIZE).min(capacity);
        let file_size = align_v(file_size, ALIGN);

        let capacity = align_v(capacity, file_size);

        DirectFsDeviceOptions {
            dir,
            capacity,
            file_size,
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

        let options = DirectFsDeviceOptionsBuilder::new(dir.path()).build();

        tracing::debug!("{options:?}");

        options.verify().unwrap();
    }

    #[test_log::test]

    fn test_options_builder_noent() {
        let dir = tempfile::tempdir().unwrap();

        let options = DirectFsDeviceOptionsBuilder::new(dir.path().join("noent")).build();

        tracing::debug!("{options:?}");

        options.verify().unwrap();
    }

    #[test_log::test(tokio::test)]
    async fn test_direct_fd_device_io() {
        let dir = tempfile::tempdir().unwrap();

        let options = DirectFsDeviceOptionsBuilder::new(dir.path())
            .with_capacity(4 * 1024 * 1024)
            .with_file_size(1024 * 1024)
            .build();

        tracing::debug!("{options:?}");

        let device = DirectFsDevice::open(options.clone()).await.unwrap();

        let mut buf = IoBytesMut::with_capacity(64 * 1024);
        buf.extend(repeat_n(b'x', 64 * 1024 - 100));
        let buf = buf.freeze();

        device.write(buf.clone(), 0, 4096).await.unwrap();

        let b = device.read(0, 4096, 64 * 1024 - 100).await.unwrap().freeze();
        assert_eq!(buf, b);

        device.flush(None).await.unwrap();

        drop(device);

        let device = DirectFsDevice::open(options).await.unwrap();

        let b = device.read(0, 4096, 64 * 1024 - 100).await.unwrap().freeze();
        assert_eq!(buf, b);
    }
}
