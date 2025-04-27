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

use foyer_common::{asyncify::asyncify_with_runtime, bits};
use fs4::free_space;
use futures_util::future::try_join_all;
use itertools::Itertools;

use super::{Dev, DevExt, RegionId, Throttle};
use crate::{
    error::{Error, Result},
    io::{
        buffer::{IoBuf, IoBufMut},
        PAGE,
    },
    Runtime,
};

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DirectFsDeviceConfig {
    dir: PathBuf,
    capacity: usize,
    file_size: usize,
    throttle: Throttle,
}

impl DirectFsDeviceConfig {
    fn verify(&self) -> Result<()> {
        if self.file_size == 0 || self.file_size % PAGE != 0 {
            return Err(anyhow::anyhow!(
                "file size ({file_size}) must be a multiplier of PAGE ({PAGE})",
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

    throttle: Throttle,

    runtime: Runtime,
}

impl DirectFsDevice {
    const PREFIX: &'static str = "foyer-storage-direct-fs-";

    fn filename(region: RegionId) -> String {
        format!("{}{:08}", Self::PREFIX, region)
    }

    fn file(&self, region: RegionId) -> &Arc<File> {
        &self.inner.files[region as usize]
    }

    fn check_io_range(&self, region: RegionId, offset: u64, len: usize) -> Result<()> {
        // Assert alignment.
        bits::assert_aligned(PAGE, offset as _);
        bits::assert_aligned(PAGE, len);

        let e = Error::InvalidIoRange {
            range: offset as usize..offset as usize + len,
            region_size: self.region_size(),
            capacity: self.capacity(),
        };

        // Assert region capacity bound if region is given.
        if offset as usize + len > self.inner.file_size {
            tracing::error!(region, ?e, "[direct fs]: io range out of region capacity");
            return Err(e);
        }

        Ok(())
    }
}

impl Dev for DirectFsDevice {
    type Config = DirectFsDeviceConfig;

    fn capacity(&self) -> usize {
        self.inner.capacity
    }

    fn region_size(&self) -> usize {
        self.inner.file_size
    }

    fn throttle(&self) -> &Throttle {
        &self.inner.throttle
    }

    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::storage::device::direct_fs::open")
    )]
    async fn open(options: Self::Config, runtime: Runtime) -> Result<Self> {
        options.verify()?;

        let regions = options.capacity / options.file_size;

        if !options.dir.exists() {
            create_dir_all(&options.dir)?;
        }

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
                throttle: options.throttle,
                runtime,
            }),
        })
    }

    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::storage::device::direct_fs::write")
    )]
    async fn write<B>(&self, buf: B, region: RegionId, offset: u64) -> (B, Result<()>)
    where
        B: IoBuf,
    {
        let len = buf.len();
        if let Err(e) = self.check_io_range(region, offset, len) {
            return (buf, Err(e));
        }

        let file = self.file(region).clone();
        asyncify_with_runtime(self.inner.runtime.write(), move || {
            #[cfg(target_family = "windows")]
            let written = {
                use std::os::windows::fs::FileExt;
                match file.seek_write(&buf, offset) {
                    Ok(v) => v,
                    Err(e) => return (buf, Err(e.into())),
                }
            };

            #[cfg(target_family = "unix")]
            let written = {
                use std::os::unix::fs::FileExt;
                match file.write_at(&buf, offset) {
                    Ok(v) => v,
                    Err(e) => return (buf, Err(e.into())),
                }
            };

            if written != len {
                return (buf, Err(anyhow::anyhow!("written {written}, expected: {len}").into()));
            }

            (buf, Ok(()))
        })
        .await
    }

    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::storage::device::direct_fs::read")
    )]
    async fn read<B>(&self, mut buf: B, region: RegionId, offset: u64) -> (B, Result<()>)
    where
        B: IoBufMut,
    {
        let len = buf.len();
        if let Err(e) = self.check_io_range(region, offset, len) {
            return (buf, Err(e));
        }

        let file = self.file(region).clone();
        asyncify_with_runtime(self.inner.runtime.read(), move || {
            #[cfg(target_family = "unix")]
            let read = {
                use std::os::unix::fs::FileExt;
                match file.read_at(&mut buf, offset) {
                    Ok(v) => v,
                    Err(e) => return (buf, Err(e.into())),
                }
            };

            #[cfg(target_family = "windows")]
            let read = {
                use std::os::windows::fs::FileExt;
                match file.seek_read(&mut buf, offset) {
                    Ok(v) => v,
                    Err(e) => return (buf, Err(e.into())),
                }
            };

            if read != len {
                return (buf, Err(anyhow::anyhow!("read {read}, expected: {len}").into()));
            }

            (buf, Ok(()))
        })
        .await
    }

    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::storage::device::direct_fs::flush")
    )]
    async fn flush(&self, region: Option<super::RegionId>) -> Result<()> {
        let flush = |region: RegionId| {
            let file = self.file(region).clone();
            asyncify_with_runtime(self.inner.runtime.write(), move || file.sync_all().map_err(Error::from))
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

/// [`DirectFsDeviceOptions`] is used to build the options for the direct fs device.
///
/// The direct fs device uses a directory in a file system to store the data of disk cache.
///
/// It uses direct I/O to reduce buffer copy and page cache pollution if supported.
#[derive(Debug)]
pub struct DirectFsDeviceOptions {
    dir: PathBuf,
    capacity: Option<usize>,
    file_size: Option<usize>,
    throttle: Throttle,
}

impl DirectFsDeviceOptions {
    const DEFAULT_FILE_SIZE: usize = 64 * 1024 * 1024;

    /// Use the given `dir` as the direct fs device.
    pub fn new(dir: impl AsRef<Path>) -> Self {
        Self {
            dir: dir.as_ref().into(),
            capacity: None,
            file_size: None,
            throttle: Throttle::default(),
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

    /// Set the throttle of the direct file device.
    pub fn with_throttle(mut self, throttle: Throttle) -> Self {
        self.throttle = throttle;
        self
    }
}

impl From<DirectFsDeviceOptions> for DirectFsDeviceConfig {
    fn from(options: DirectFsDeviceOptions) -> Self {
        let dir = options.dir;

        let align_v = |value: usize, align: usize| value - value % align;

        let capacity = options.capacity.unwrap_or({
            // Create an empty directory before to get free space.
            create_dir_all(&dir).unwrap();
            free_space(&dir).unwrap() as usize / 10 * 8
        });
        let capacity = align_v(capacity, PAGE);

        let file_size = options
            .file_size
            .unwrap_or(DirectFsDeviceOptions::DEFAULT_FILE_SIZE)
            .min(capacity);
        let file_size = align_v(file_size, PAGE);

        let capacity = align_v(capacity, file_size);

        let throttle = options.throttle;

        DirectFsDeviceConfig {
            dir,
            capacity,
            file_size,
            throttle,
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::BufMut;

    use super::*;
    use crate::io::buffer::IoBuffer;

    #[test_log::test]
    fn test_options_builder() {
        let dir = tempfile::tempdir().unwrap();

        let config: DirectFsDeviceConfig = DirectFsDeviceOptions::new(dir.path()).into();

        tracing::debug!("{config:?}");

        config.verify().unwrap();
    }

    #[test_log::test]

    fn test_options_builder_noent() {
        let dir = tempfile::tempdir().unwrap();

        let config: DirectFsDeviceConfig = DirectFsDeviceOptions::new(dir.path().join("noent")).into();

        tracing::debug!("{config:?}");

        config.verify().unwrap();
    }

    #[test_log::test(tokio::test)]
    async fn test_direct_fd_device_io() {
        let dir = tempfile::tempdir().unwrap();
        let runtime = Runtime::current();

        let config: DirectFsDeviceConfig = DirectFsDeviceOptions::new(dir.path())
            .with_capacity(4 * 1024 * 1024)
            .with_file_size(1024 * 1024)
            .into();

        tracing::debug!("{config:?}");

        let device = DirectFsDevice::open(config.clone(), runtime.clone()).await.unwrap();

        let mut buf = IoBuffer::new(64 * 1024);
        (&mut buf[..]).put_bytes(b'x', 64 * 1024 - 100);
        let buf = buf.into_shared_io_slice();

        let (_, res) = device.write(buf.clone(), 0, 4096).await;
        res.unwrap();

        let b = IoBuffer::new(64 * 1024);
        let (b, res) = device.read(b, 0, 4096).await;
        res.unwrap();
        assert_eq!(&buf[..], &b[..]);

        device.flush(None).await.unwrap();

        drop(device);

        let device = DirectFsDevice::open(config, runtime).await.unwrap();

        let b = IoBuffer::new(64 * 1024);
        let (b, res) = device.read(b, 0, 4096).await;
        res.unwrap();
        assert_eq!(&buf[..], &b[..]);
    }
}
