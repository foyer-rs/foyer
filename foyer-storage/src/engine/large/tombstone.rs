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
use foyer_common::bits;
use tokio::sync::Mutex;

use crate::{
    error::Result,
    io::{bytes::IoSliceMut, device::RegionId, throttle::IopsCounter, PAGE},
    Device, DeviceBuilder, FileDeviceBuilder, IoEngine, IoEngineBuilder, PsyncIoEngineBuilder, Runtime,
};

/// The configurations for the tombstone log.
#[derive(Debug, Clone)]
pub struct TombstoneLogConfig {
    /// Path of the tombstone log.
    pub path: PathBuf,
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
    buffer: PageBuffer,
}

impl TombstoneLog {
    /// Open the tombstone log with given a dedicated device.
    ///
    /// The tombstone log will
    pub async fn open(
        config: &TombstoneLogConfig,
        runtime: Runtime,
        cache_device: Arc<dyn Device>,
        tombstones: &mut Vec<Tombstone>,
    ) -> Result<Self> {
        // For large entry disk cache, the minimum entry size is the alignment.
        //
        // So, the tombstone log needs at most `cache device capacity / align` slots.
        //
        // For the alignment is 4K and the slot size is 16B, tombstone log requires 1/256 of the cache device size.
        let capacity = bits::align_up(PAGE, (cache_device.capacity() / PAGE) * Tombstone::serialized_len());

        let device = FileDeviceBuilder::new(&config.path)
            .with_region_size(PAGE)
            .with_capacity(capacity)
            .boxed()
            .build()?;
        let io_engine = PsyncIoEngineBuilder::new().boxed().build(device, runtime)?;

        let mut recovered = vec![];

        for region in 0..io_engine.device().regions() as RegionId {
            tracing::trace!(region, "[tombstone log]: recover region");
            let buf = IoSliceMut::new(PAGE);
            let (buffer, res) = io_engine.read(Box::new(buf), region, 0).await;
            res?;

            let mut seq = 0;
            let mut addr = 0;

            for (slot, buf) in buffer.chunks_exact(Tombstone::serialized_len()).enumerate() {
                let tombstone = Tombstone::read(buf);
                if tombstone.sequence > seq {
                    seq = tombstone.sequence;
                    addr = slot * Tombstone::serialized_len();
                }
                recovered.push((tombstone, addr));
            }
        }

        let offset = recovered
            .iter()
            .reduce(|a, b| if a.0.sequence > b.0.sequence { a } else { b })
            .map(|(tombstone, addr)| {
                tracing::trace!(?tombstone, "[tombstone log]: found latest tombstone");
                *addr
            })
            .unwrap() as u64;

        tombstones.extend(recovered.into_iter().map(|(tombstone, _)| tombstone));
        let offset = (offset + Tombstone::serialized_len() as u64) % capacity as u64;

        let region = bits::align_down(PAGE as RegionId, offset as RegionId) / PAGE as RegionId;
        let buffer = PageBuffer::open(io_engine, region, 0).await?;

        Ok(Self {
            inner: Arc::new(Mutex::new(TombstoneLogInner { offset, buffer })),
        })
    }

    pub async fn append(&self, tombstones: impl Iterator<Item = &Tombstone>) -> Result<()> {
        let mut inner = self.inner.lock().await;

        for tombstone in tombstones {
            if bits::is_aligned(PAGE as u64, inner.offset) {
                inner.buffer.flush().await?;
                let region = bits::align_down(PAGE as RegionId, inner.offset as RegionId) / PAGE as RegionId;
                inner.buffer.load(region, 0).await?;
            }
            let start = inner.offset as usize % PAGE;
            let end = start + Tombstone::serialized_len();
            tombstone.write(&mut inner.buffer.as_mut()[start..end]);

            inner.offset =
                (inner.offset + Tombstone::serialized_len() as u64) % inner.buffer.io_engine.device().capacity() as u64;
        }

        inner.buffer.flush().await
    }
}

#[derive(Debug)]
pub struct PageBuffer {
    region: RegionId,
    idx: u32,
    // NOTE: This is always `Some(..)`.
    buffer: Option<IoSliceMut>,

    io_engine: Arc<dyn IoEngine>,
}

impl AsRef<[u8]> for PageBuffer {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_ref().unwrap()
    }
}

impl AsMut<[u8]> for PageBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        self.buffer.as_mut().unwrap()
    }
}

impl PageBuffer {
    pub async fn open(io_engine: Arc<dyn IoEngine>, region: RegionId, idx: u32) -> Result<Self> {
        let mut this = Self {
            region,
            idx,
            buffer: Some(IoSliceMut::new(PAGE)),
            io_engine,
        };

        this.update().await?;

        Ok(this)
    }

    pub async fn update(&mut self) -> Result<()> {
        let buf = self.buffer.take().unwrap();
        let (buf, res) = self
            .io_engine
            .read(Box::new(buf), self.region, Self::offset(PAGE, self.idx))
            .await;
        let buf = *buf.try_into_io_slice_mut().unwrap();
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
            .io_engine
            .write(Box::new(buf), self.region, Self::offset(PAGE, self.idx))
            .await;
        let buf = *buf.try_into_io_slice_mut().unwrap();
        self.buffer = Some(buf);
        res?;
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
                iops_counter: IopsCounter::per_io(),
            },
            Runtime::current(),
            device.clone(),
            &mut vec![],
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
                iops_counter: IopsCounter::per_io(),
            },
            Runtime::current(),
            device.clone(),
            &mut vec![],
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
