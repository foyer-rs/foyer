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
    path::{Path, PathBuf},
    sync::Arc,
};

use array_util::SliceExt;
use bytes::{Buf, BufMut};
use foyer_common::bits;
use futures::future::try_join_all;
use tokio::sync::Mutex;

use crate::device::{
    direct_file::{DirectFileDevice, DirectFileDeviceOptionsBuilder},
    Device, DeviceExt, IoBuffer, RegionId, IO_BUFFER_ALLOCATOR,
};

use crate::error::{Error, Result};

#[derive(Debug, Clone)]
pub struct TombstoneLogConfig {
    pub path: PathBuf,
    pub flush: bool,
}

#[derive(Debug)]
pub struct TombstoneLogConfigBuilder {
    path: PathBuf,
    flush: bool,
}

impl TombstoneLogConfigBuilder {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().into(),
            flush: true,
        }
    }

    pub fn with_flush(mut self, flush: bool) -> Self {
        self.flush = flush;
        self
    }

    pub fn build(self) -> TombstoneLogConfig {
        TombstoneLogConfig {
            path: self.path,
            flush: self.flush,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Tombstone {
    pub hash: u64,
    pub sequence: u64,
}

impl Tombstone {
    const fn serialized_len() -> usize {
        8 + 8
    }

    fn write(&self, mut buf: impl BufMut) {
        buf.put_u64(self.hash);
        buf.put_u64(self.sequence);
    }

    fn read(mut buf: impl Buf) -> Self {
        let hash = buf.get_u64();
        let sequence = buf.get_u64();
        Self { hash, sequence }
    }
}

#[derive(Debug, Clone)]
pub struct TombstoneLog {
    inner: Arc<Mutex<TombstoneLogInner>>,
}

#[derive(Debug)]
struct TombstoneLogInner {
    offset: u64,
    buffer: PageBuffer<DirectFileDevice>,
}

impl TombstoneLog {
    const RECOVER_IO_SIZE: usize = 128 * 1024;

    /// Open the tombstone log with given a dedicated device.
    ///
    /// The tombstone log will
    pub async fn open<D>(
        path: impl AsRef<Path>,
        cache_device: D,
        flush: bool,
        tombstones: &mut Vec<Tombstone>,
    ) -> Result<Self>
    where
        D: Device,
    {
        let align = cache_device.align();

        // For large entry disk cache, the minimum entry size is the alignment.
        //
        // So, the tombstone log needs at most `cache device capacity / align` slots.
        //
        // For the alignment is 4K and the slot size is 16B, tombstone log requires 1/256 of the cache device size.
        let capacity = bits::align_up(align, (cache_device.capacity() / align) * Tombstone::serialized_len());

        let device = DirectFileDevice::open(
            DirectFileDeviceOptionsBuilder::new(path)
                .with_region_size(align)
                .with_capacity(capacity)
                .build(),
        )
        .await?;

        let tasks = bits::align_up(Self::RECOVER_IO_SIZE, capacity) / Self::RECOVER_IO_SIZE;
        tracing::trace!("[tombstone log]: recover task count: {tasks}");
        let res = try_join_all((0..tasks).map(|i| {
            let offset = i * Self::RECOVER_IO_SIZE;
            let len = std::cmp::min(offset + Self::RECOVER_IO_SIZE, capacity) - offset;
            let device = device.clone();
            async move {
                let buffer = device.pread(offset as _, len).await?;

                let mut seq = 0;
                let mut addr = 0;
                let mut hash = 0;
                let mut tombstones = vec![];

                // TODO(MrCroxx): use `array_chunks` after `#![feature(array_chunks)]` is stable.
                for (slot, buf) in buffer
                    .as_slice()
                    .array_chunks_ext::<{ Tombstone::serialized_len() }>()
                    .enumerate()
                {
                    let tombstone = Tombstone::read(&buf[..]);
                    if tombstone.sequence > seq {
                        seq = tombstone.sequence;
                        addr = offset + slot * Tombstone::serialized_len();
                        hash = tombstone.hash
                    }
                    tombstones.push(tombstone);
                }

                Ok::<_, Error>((tombstones, seq, addr, hash))
            }
        }))
        .await?;
        let offset = res
            .iter()
            .reduce(|a, b| if a.1 > b.1 { a } else { b })
            .inspect(|(_, seq, addr, hash)| {
                tracing::trace!(
                    "[tombstone log]: found latest tombstone at {addr} with sequence = {seq}, hash = {hash}"
                )
            })
            .map(|(_, _, addr, _)| *addr)
            .unwrap() as u64;

        res.into_iter().for_each(|mut r| tombstones.append(&mut r.0));
        let offset = (offset + Tombstone::serialized_len() as u64) % capacity as u64;

        let region = bits::align_down(align as RegionId, offset as RegionId) / align as RegionId;
        let buffer = PageBuffer::open(device, region, 0, flush).await?;

        Ok(Self {
            inner: Arc::new(Mutex::new(TombstoneLogInner { offset, buffer })),
        })
    }

    pub async fn append(&self, tombstones: impl Iterator<Item = &Tombstone>) -> Result<()> {
        let mut inner = self.inner.lock().await;

        let align = inner.buffer.device.align();

        for tombstone in tombstones {
            if bits::is_aligned(align as u64, inner.offset) {
                inner.buffer.flush().await?;
                let region = bits::align_down(align as RegionId, inner.offset as RegionId) / align as RegionId;
                inner.buffer.load(region, 0).await?;
            }
            let start = inner.offset as usize % align;
            let end = start + Tombstone::serialized_len();
            tombstone.write(&mut inner.buffer.as_mut()[start..end]);

            inner.offset = (inner.offset + Tombstone::serialized_len() as u64) % inner.buffer.device.capacity() as u64;
        }

        inner.buffer.flush().await
    }
}

#[derive(Debug)]
pub struct PageBuffer<D> {
    region: RegionId,
    idx: u32,
    buffer: IoBuffer,

    device: D,

    sync: bool,
}

impl<D> AsRef<[u8]> for PageBuffer<D> {
    fn as_ref(&self) -> &[u8] {
        &self.buffer
    }
}

impl<D> AsMut<[u8]> for PageBuffer<D> {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.buffer
    }
}

impl<D> PageBuffer<D>
where
    D: Device,
{
    pub async fn open(device: D, region: RegionId, idx: u32, sync: bool) -> Result<Self> {
        let mut this = Self {
            region,
            idx,
            buffer: IoBuffer::new_in(&IO_BUFFER_ALLOCATOR),
            device,
            sync,
        };

        this.update().await?;

        Ok(this)
    }

    pub async fn update(&mut self) -> Result<()> {
        self.buffer = self
            .device
            .read(
                self.region,
                Self::offset(self.device.align(), self.idx),
                self.device.align(),
            )
            .await?;

        debug_assert_eq!(self.buffer.len(), self.device.align());
        debug_assert_eq!(self.buffer.capacity(), self.device.align());

        Ok(())
    }

    pub async fn load(&mut self, region: RegionId, idx: u32) -> Result<()> {
        self.region = region;
        self.idx = idx;
        self.update().await
    }

    pub async fn flush(&self) -> Result<()> {
        self.device
            .write(
                self.buffer.clone(),
                self.region,
                Self::offset(self.device.align(), self.idx),
            )
            .await?;
        if self.sync {
            self.device.flush(Some(self.region)).await?;
        }
        Ok(())
    }

    fn offset(align: usize, idx: u32) -> u64 {
        align as u64 * idx as u64
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use tempfile::tempdir;

    use crate::device::direct_fs::{DirectFsDevice, DirectFsDeviceOptionsBuilder};

    use super::*;

    #[test_log::test(tokio::test)]
    async fn test_tombstone_log() {
        let dir = tempdir().unwrap();

        // 4 MB cache device => 16 KB tombstone log => 1K tombstones
        let device = DirectFsDevice::open(
            DirectFsDeviceOptionsBuilder::new(dir.path())
                .with_capacity(4 * 1024 * 1024)
                .build(),
        )
        .await
        .unwrap();

        let log = TombstoneLog::open(dir.path().join("test-tombstone-log"), device.clone(), true, &mut vec![])
            .await
            .unwrap();

        log.append(
            (0..3 * 1024 + 42)
                .map(|i| Tombstone { hash: i, sequence: i })
                .collect_vec()
                .iter(),
        )
        .await
        .unwrap();

        {
            let inner = log.inner.lock().await;
            assert_eq!(
                inner.offset,
                (3 * 1024 + 42 + 1) * Tombstone::serialized_len() as u64 % (16 * 1024)
            )
        }

        drop(log);

        let log = TombstoneLog::open(dir.path().join("test-tombstone-log"), device, true, &mut vec![])
            .await
            .unwrap();

        {
            let inner = log.inner.lock().await;
            assert_eq!(
                inner.offset,
                (3 * 1024 + 42 + 1) * Tombstone::serialized_len() as u64 % (16 * 1024)
            )
        }
    }
}
