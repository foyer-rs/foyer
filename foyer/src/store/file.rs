//  Copyright 2023 MrCroxx
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

use bytes::{Buf, BufMut};
use nix::sys::{
    stat::fstat,
    uio::{pread, pwrite},
};

use super::{asyncify, error::Result};

use std::{
    fs::{remove_file, File, OpenOptions},
    os::fd::{AsRawFd, RawFd},
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
};

#[derive(Clone, Copy, Debug)]
pub struct Location {
    pub offset: u32,
    pub len: u32,
}

impl Location {
    pub fn write(&self, mut buf: &mut [u8]) {
        buf.put_u32_le(self.offset);
        buf.put_u32_le(self.len);
    }

    pub fn read(mut buf: &[u8]) -> Self {
        let offset = buf.get_u32_le();
        let len = buf.get_u32_le();
        Self { offset, len }
    }

    pub fn size() -> usize {
        8
    }
}

// TODO(MrCroxx): Use buffer and direct I/O to better manage memory usage.
pub struct AppendableFile {
    path: PathBuf,

    fd: RawFd,

    size: AtomicUsize,

    file: File,
}

impl AppendableFile {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = PathBuf::from(path.as_ref());

        let opts = {
            let mut opts = OpenOptions::new();
            opts.create(true);
            opts.write(true);
            opts.read(true);
            opts
        };

        let (file, fd, size) = asyncify({
            let path = path.clone();
            move || {
                let file = opts.open(path)?;
                let fd = file.as_raw_fd();
                let stat = fstat(fd)?;
                let size = stat.st_size as usize;
                Ok((file, fd, size))
            }
        })
        .await?;

        Ok(Self {
            path,
            fd,
            size: AtomicUsize::new(size),
            file,
        })
    }

    pub async fn append(&self, buf: Vec<u8>) -> Result<Location> {
        let len = buf.len();
        let offset = self.size.fetch_add(len, Ordering::Relaxed);
        let fd = self.fd;
        asyncify(move || {
            pwrite(fd, &buf, offset as i64)?;
            Ok(())
        })
        .await?;

        Ok(Location {
            offset: offset as u32,
            len: len as u32,
        })
    }

    #[allow(clippy::uninit_vec)]
    pub async fn read(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        let fd = self.fd;
        let buf = asyncify(move || {
            let mut buf = Vec::with_capacity(len);
            unsafe { buf.set_len(len) };
            pread(fd, &mut buf, offset as i64)?;
            Ok(buf)
        })
        .await?;
        Ok(buf)
    }

    pub fn len(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub async fn size(&self) -> Result<usize> {
        let fd = self.fd;
        let size = asyncify(move || {
            let stat = fstat(fd)?;
            Ok(stat.st_size as usize)
        })
        .await?;
        Ok(size)
    }

    pub async fn reclaim(self) -> Result<()> {
        let file = self.file;
        drop(file);

        asyncify(move || {
            remove_file(self.path)?;
            Ok(())
        })
        .await
    }
}

// TODO(MrCroxx): Use buffer and direct I/O to better manage memory usage.
pub struct ReadableFile {
    path: PathBuf,

    fd: RawFd,

    size: usize,

    file: File,
}

impl ReadableFile {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = PathBuf::from(path.as_ref());

        let opts = {
            let mut opts = OpenOptions::new();
            opts.read(true);
            opts
        };

        let (file, fd, size) = asyncify({
            let path = path.clone();
            move || {
                let file = opts.open(path)?;
                let fd = file.as_raw_fd();
                let stat = fstat(fd)?;
                let size = stat.st_size as usize;
                Ok((file, fd, size))
            }
        })
        .await?;

        Ok(Self {
            path,
            fd,
            size,
            file,
        })
    }

    #[allow(clippy::uninit_vec)]
    pub async fn read(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        let fd = self.fd;
        let buf = asyncify(move || {
            let mut buf = Vec::with_capacity(len);
            unsafe { buf.set_len(len) };
            pread(fd, &mut buf, offset as i64)?;
            Ok(buf)
        })
        .await?;
        Ok(buf)
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub async fn size(&self) -> Result<usize> {
        let fd = self.fd;
        let size = asyncify(move || {
            let stat = fstat(fd)?;
            Ok(stat.st_size as usize)
        })
        .await?;
        Ok(size)
    }

    pub async fn reclaim(self) -> Result<()> {
        let file = self.file;
        drop(file);

        asyncify(move || {
            remove_file(self.path)?;
            Ok(())
        })
        .await
    }
}

pub struct WritableFile {
    path: PathBuf,

    fd: RawFd,

    size: AtomicUsize,

    file: File,
}

impl WritableFile {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = PathBuf::from(path.as_ref());

        let opts = {
            let mut opts = OpenOptions::new();
            opts.create(true);
            opts.write(true);
            opts.read(true);
            opts
        };

        let (file, fd, size) = asyncify({
            let path = path.clone();
            move || {
                let file = opts.open(path)?;
                let fd = file.as_raw_fd();
                let stat = fstat(fd)?;
                let size = stat.st_size as usize;
                Ok((file, fd, size))
            }
        })
        .await?;

        Ok(Self {
            path,
            fd,
            size: AtomicUsize::new(size),
            file,
        })
    }

    pub async fn append(&self, buf: Vec<u8>) -> Result<Location> {
        let len = buf.len();
        let offset = self.size.fetch_add(len, Ordering::Relaxed);
        let fd = self.fd;
        asyncify(move || {
            pwrite(fd, &buf, offset as i64)?;
            Ok(())
        })
        .await?;

        Ok(Location {
            offset: offset as u32,
            len: len as u32,
        })
    }

    /// # Safety
    ///
    /// Concurrent writes or appends on the same address are not guaranteed.
    pub async fn write(&self, offset: u64, buf: Vec<u8>) -> Result<Location> {
        let fd = self.fd;
        let len = buf.len();
        asyncify(move || {
            pwrite(fd, &buf, offset as i64)?;
            Ok(())
        })
        .await?;
        Ok(Location {
            offset: offset as u32,
            len: len as u32,
        })
    }

    #[allow(clippy::uninit_vec)]
    pub async fn read(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        let fd = self.fd;
        let buf = asyncify(move || {
            let mut buf = Vec::with_capacity(len);
            unsafe { buf.set_len(len) };
            pread(fd, &mut buf, offset as i64)?;
            Ok(buf)
        })
        .await?;
        Ok(buf)
    }

    pub async fn size(&self) -> Result<usize> {
        let fd = self.fd;
        let size = asyncify(move || {
            let stat = fstat(fd)?;
            Ok(stat.st_size as usize)
        })
        .await?;
        Ok(size)
    }

    pub async fn reclaim(self) -> Result<()> {
        let file = self.file;
        drop(file);

        asyncify(move || {
            remove_file(self.path)?;
            Ok(())
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use crate::tests::is_send_sync_static;

    use super::*;

    use futures::future::try_join_all;
    use itertools::Itertools;
    use tempfile::tempdir;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_rw() {
        is_send_sync_static::<AppendableFile>();
        is_send_sync_static::<ReadableFile>();

        let dir = tempdir().unwrap();
        let path = PathBuf::from(dir.path()).join("testfile");

        let afile = AppendableFile::open(&path).await.unwrap();

        let bufs = (0..4).map(|i| vec![i as u8; 1024]).collect_vec();
        let futures = bufs
            .iter()
            .map(|buf| afile.append(buf.clone()))
            .collect_vec();
        let locs = try_join_all(futures).await.unwrap();

        assert_eq!(afile.len(), 4 * 1024);
        assert_eq!(afile.size().await.unwrap(), 4 * 1024);
        for (buf, loc) in bufs.iter().zip_eq(locs.iter()) {
            assert_eq!(
                buf,
                &afile
                    .read(loc.offset as u64, loc.len as usize)
                    .await
                    .unwrap()
            );
        }

        drop(afile);

        let rfile = ReadableFile::open(&path).await.unwrap();
        for (buf, loc) in bufs.iter().zip_eq(locs.iter()) {
            assert_eq!(
                buf,
                &rfile
                    .read(loc.offset as u64, loc.len as usize)
                    .await
                    .unwrap()
            );
        }

        drop(rfile);
    }
}
