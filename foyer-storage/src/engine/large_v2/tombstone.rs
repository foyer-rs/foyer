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
    path::{Path, PathBuf},
    sync::Arc,
};

use bytes::{Buf, BufMut};
use foyer_common::{bits, metrics::Metrics};
use futures_util::future::try_join_all;
use tokio::sync::Mutex;

use crate::{
    device::{
        direct_file::DirectFileDevice,
        monitor::{Monitored, MonitoredConfig},
        Dev, DevExt, RegionId,
    },
    error::{Error, Result},
    io::{buffer::IoBuffer, device::Device, PAGE},
    DirectFileDeviceOptions, IopsCounter, Runtime,
};

/// The configurations for the tombstone log.
#[derive(Debug, Clone)]
pub struct TombstoneLogConfig {
    /// Path of the tombstone log.
    pub path: PathBuf,
    /// If enabled, `sync` will be called after writes to make sure the data is safely persisted on the device.
    pub flush: bool,
    /// iops_counter for the tombstone log device monitor.
    pub iops_counter: IopsCounter,
}

/// The builder for the tombstone log config.
#[derive(Debug)]
pub struct TombstoneLogConfigBuilder {
    path: PathBuf,
    flush: bool,
    iops_counter: IopsCounter,
}

impl TombstoneLogConfigBuilder {
    /// Create a new builder for the tombstone log config on the given path.
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().into(),
            flush: true,
            iops_counter: IopsCounter::per_io(),
        }
    }

    /// Set whether to enable flush.
    ///
    /// If enabled, `sync` will be called after writes to make sure the data is safely persisted on the device.
    pub fn with_flush(mut self, flush: bool) -> Self {
        self.flush = flush;
        self
    }

    /// Set iops counter for the tombstone log device monitor.
    pub fn with_iops_counter(mut self, iops_counter: IopsCounter) -> Self {
        self.iops_counter = iops_counter;
        self
    }

    /// Build the tombstone log config with the given args.
    pub fn build(self) -> TombstoneLogConfig {
        TombstoneLogConfig {
            path: self.path,
            flush: self.flush,
            iops_counter: self.iops_counter,
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
    buffer: PageBuffer<Monitored<DirectFileDevice>>,
}

impl TombstoneLog {
    const RECOVER_IO_SIZE: usize = 128 * 1024;

    /// Open the tombstone log with given a dedicated device.
    ///
    /// The tombstone log will
    pub async fn open(
        config: &TombstoneLogConfig,
        cache_device: Arc<dyn Device>,
        tombstones: &mut Vec<Tombstone>,
        metrics: Arc<Metrics>,
        runtime: Runtime,
    ) -> Result<Self> {
        // For large entry disk cache, the minimum entry size is the alignment.
        //
        // So, the tombstone log needs at most `cache device capacity / align` slots.
        //
        // For the alignment is 4K and the slot size is 16B, tombstone log requires 1/256 of the cache device size.
        let capacity = bits::align_up(PAGE, (cache_device.capacity() / PAGE) * Tombstone::serialized_len());

        let device = Monitored::open(
            MonitoredConfig {
                config: DirectFileDeviceOptions::new(&config.path)
                    .with_region_size(PAGE)
                    .with_capacity(capacity)
                    .into(),
                metrics: metrics.clone(),
            },
            runtime,
        )
        .await?;

        let tasks = bits::align_up(Self::RECOVER_IO_SIZE, capacity) / Self::RECOVER_IO_SIZE;
        tracing::trace!("[tombstone log]: recover task count: {tasks}");
        let res = try_join_all((0..tasks).map(|i| {
            let offset = i * Self::RECOVER_IO_SIZE;
            let len = std::cmp::min(offset + Self::RECOVER_IO_SIZE, capacity) - offset;
            let device = device.clone();
            async move {
                let buf = IoBuffer::new(len);
                let (buffer, res) = device.pread(buf, offset as _).await;
                res?;

                let mut seq = 0;
                let mut addr = 0;
                let mut hash = 0;
                let mut tombstones = vec![];

                for (slot, buf) in buffer.chunks_exact(Tombstone::serialized_len()).enumerate() {
                    let tombstone = Tombstone::read(buf);
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

        let region = bits::align_down(PAGE as RegionId, offset as RegionId) / PAGE as RegionId;
        let buffer = PageBuffer::open(device, region, 0, config.flush).await?;

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
    // NOTE: This is always `Some(..)`.
    buffer: Option<IoBuffer>,

    device: D,

    sync: bool,
}

impl<D> AsRef<[u8]> for PageBuffer<D> {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_ref().unwrap()
    }
}

impl<D> AsMut<[u8]> for PageBuffer<D> {
    fn as_mut(&mut self) -> &mut [u8] {
        self.buffer.as_mut().unwrap()
    }
}

impl<D> PageBuffer<D>
where
    D: Dev,
{
    pub async fn open(device: D, region: RegionId, idx: u32, sync: bool) -> Result<Self> {
        let mut this = Self {
            region,
            idx,
            buffer: Some(IoBuffer::new(PAGE)),
            device,
            sync,
        };

        this.update().await?;

        Ok(this)
    }

    pub async fn update(&mut self) -> Result<()> {
        let buf = self.buffer.take().unwrap();
        let (buf, res) = self
            .device
            .read(buf, self.region, Self::offset(self.device.align(), self.idx))
            .await;
        self.buffer = Some(buf);
        res?;
        Ok(())
    }

    pub async fn load(&mut self, region: RegionId, idx: u32) -> Result<()> {
        self.region = region;
        self.idx = idx;
        self.update().await
    }

    pub async fn flush(&mut self) -> Result<()> {
        let buf = self.buffer.take().unwrap();
        let (buf, res) = self
            .device
            .write(buf, self.region, Self::offset(self.device.align(), self.idx))
            .await;
        self.buffer = Some(buf);
        res?;
        if self.sync {
            self.device.sync(Some(self.region)).await?;
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

    use super::*;
    use crate::io::device::{fs::FsDeviceBuilder, DeviceBuilder};

    #[test_log::test(tokio::test)]
    async fn test_tombstone_log() {
        let runtime = Runtime::current();

        let dir = tempdir().unwrap();

        // 4 MB cache device => 16 KB tombstone log => 1K tombstones
        let device = FsDeviceBuilder::new(dir.path())
            .with_capacity(4 * 1024 * 1024)
            .boxed()
            .build()
            .unwrap();

        let log = TombstoneLog::open(
            &TombstoneLogConfig {
                path: dir.path().join("test-tombstone-log"),
                flush: true,
                iops_counter: IopsCounter::per_io(),
            },
            device.clone(),
            &mut vec![],
            Arc::new(Metrics::noop()),
            runtime.clone(),
        )
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

        let log = TombstoneLog::open(
            &TombstoneLogConfig {
                path: dir.path().join("test-tombstone-log"),
                flush: true,
                iops_counter: IopsCounter::per_io(),
            },
            device.clone(),
            &mut vec![],
            Arc::new(Metrics::noop()),
            runtime.clone(),
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
    }
}
