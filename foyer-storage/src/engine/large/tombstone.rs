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

use std::sync::Arc;

use bytes::{Buf, BufMut};
use tokio::sync::Mutex;

use crate::{
    error::Result,
    io::{bytes::IoSliceMut, device::Partition, PAGE},
    IoEngine,
};

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
    pages: usize,
}

#[derive(Debug)]
struct TombstoneLogInner {
    buffer: PageBuffer,
    slot: usize,
}

impl TombstoneLog {
    pub const SLOTS_PER_PAGE: usize = PAGE / Tombstone::serialized_len();

    /// Open the tombstone log with given a dedicated device.
    ///
    /// The tombstone log will
    pub async fn open(
        partition: Arc<dyn Partition>,
        io_engine: Arc<dyn IoEngine>,
        tombstones: &mut Vec<Tombstone>,
    ) -> Result<Self> {
        let mut recovered = vec![];

        for offset in (0..partition.size()).step_by(PAGE) {
            tracing::trace!(offset, "[tombstone log]: recover at");
            let buf = IoSliceMut::new(PAGE);
            let (buffer, res) = io_engine.read(Box::new(buf), partition.as_ref(), offset as u64).await;
            res?;

            let mut seq = 0;
            let mut addr = 0;

            for (slot, buf) in buffer.chunks_exact(Tombstone::serialized_len()).enumerate() {
                let tombstone = Tombstone::read(buf);
                if tombstone.sequence > seq {
                    seq = tombstone.sequence;
                    addr = slot * Tombstone::serialized_len();
                }
                if tombstone.sequence == 0 {
                    continue;
                }
                recovered.push((tombstone, addr));
            }
        }

        tracing::trace!(?recovered, "[tombstone log]: recovered tombstones");

        let latest_tombstone_offset = recovered
            .iter()
            .reduce(|a, b| if a.0.sequence > b.0.sequence { a } else { b })
            .map(|(tombstone, addr)| {
                tracing::trace!(?tombstone, "[tombstone log]: found latest tombstone");
                *addr
            })
            .unwrap_or_default();

        tombstones.extend(recovered.into_iter().map(|(tombstone, _)| tombstone));

        let latest_tombstone_page = latest_tombstone_offset / PAGE;
        let latest_tombstone_slot = if latest_tombstone_page == 0 {
            latest_tombstone_offset / Tombstone::serialized_len()
        } else {
            let pages_before_latest_tombstone = latest_tombstone_page - 1;
            Self::SLOTS_PER_PAGE * pages_before_latest_tombstone
                + (latest_tombstone_offset - pages_before_latest_tombstone * PAGE) / Tombstone::serialized_len()
        };

        let pages = partition.size() / PAGE;
        let slot = latest_tombstone_slot + 1;
        let (page, _) = Self::calculate_slot_addr(pages, slot);
        let buffer = PageBuffer::open(io_engine, partition, page as _).await?;

        Ok(Self {
            inner: Arc::new(Mutex::new(TombstoneLogInner { buffer, slot })),
            pages,
        })
    }

    fn calculate_slot_addr(pages: usize, slot: usize) -> (u32, usize) {
        let page = slot / Self::SLOTS_PER_PAGE;
        let page = page % pages;
        let offset = (slot % Self::SLOTS_PER_PAGE) * Tombstone::serialized_len();
        (page as u32, offset)
    }

    fn slot_addr(&self, slot: usize) -> (u32, usize) {
        Self::calculate_slot_addr(self.pages, slot)
    }

    pub async fn append(&self, tombstones: impl Iterator<Item = &Tombstone>) -> Result<()> {
        let mut inner = self.inner.lock().await;

        for tombstone in tombstones {
            let slot = inner.slot;

            let (page, offset) = self.slot_addr(slot);
            if page != inner.buffer.page {
                inner.buffer.flush().await?;
                inner.buffer.load(page).await?;
            }
            let start = offset;
            let end = start + Tombstone::serialized_len();
            tombstone.write(&mut inner.buffer.as_mut()[start..end]);

            inner.slot += 1;
        }

        inner.buffer.flush().await
    }
}

#[derive(Debug)]
pub struct PageBuffer {
    // NOTE: This is always `Some(..)`.
    buffer: Option<IoSliceMut>,

    io_engine: Arc<dyn IoEngine>,
    partition: Arc<dyn Partition>,
    page: u32,
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
    pub async fn open(io_engine: Arc<dyn IoEngine>, partition: Arc<dyn Partition>, page: u32) -> Result<Self> {
        let mut this = Self {
            buffer: Some(IoSliceMut::new(PAGE)),
            io_engine,
            partition,
            page,
        };

        this.update().await?;

        Ok(this)
    }

    pub async fn update(&mut self) -> Result<()> {
        let buf = self.buffer.take().unwrap();
        let (buf, res) = self
            .io_engine
            .read(Box::new(buf), self.partition.as_ref(), Self::offset(self.page))
            .await;
        let buf = *buf.try_into_io_slice_mut().unwrap();
        self.buffer = Some(buf);
        res?;
        Ok(())
    }

    pub async fn load(&mut self, page: u32) -> Result<()> {
        tracing::trace!(page, "[tombstone log page buffer]: load page");
        self.page = page;
        self.update().await
    }

    pub async fn flush(&mut self) -> Result<()> {
        tracing::trace!(page = self.page, "[tombstone log page buffer]: flush page");
        let buf = self.buffer.take().unwrap();
        let (buf, res) = self
            .io_engine
            .write(Box::new(buf), self.partition.as_ref(), Self::offset(self.page))
            .await;
        let buf = *buf.try_into_io_slice_mut().unwrap();
        self.buffer = Some(buf);
        res?;
        Ok(())
    }

    fn offset(page: u32) -> u64 {
        PAGE as u64 * page as u64
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        io::device::{fs::FsDeviceBuilder, DeviceBuilder},
        IoEngineBuilder, PsyncIoEngineBuilder,
    };

    #[test_log::test(tokio::test)]
    async fn test_tombstone_log() {
        let dir = tempdir().unwrap();

        // 4 MB cache device => 16 KB tombstone log => 1K tombstones => 16K partition => 4 pages
        let device = FsDeviceBuilder::new(dir.path())
            .with_capacity(4 * 1024 * 1024 + 16 * 1024)
            .boxed()
            .build()
            .unwrap();
        let partition = device.create_partition(16 * 1024).unwrap();
        let io_engine = PsyncIoEngineBuilder::new().boxed().build(device).unwrap();

        let log = TombstoneLog::open(partition.clone(), io_engine.clone(), &mut vec![])
            .await
            .unwrap();

        log.append(
            (0..3 * 1024 + 42)
                .map(|i| Tombstone {
                    hash: i + 1,
                    sequence: i + 1,
                })
                .collect_vec()
                .iter(),
        )
        .await
        .unwrap();

        {
            let inner = log.inner.lock().await;
            assert_eq!(inner.slot, (3 * 1024 + 42 + 1));
            let (page, _) = log.slot_addr(inner.slot);
            assert_eq!(inner.buffer.page, page);
        }

        drop(log);

        let log = TombstoneLog::open(partition.clone(), io_engine.clone(), &mut vec![])
            .await
            .unwrap();

        {
            let inner = log.inner.lock().await;
            assert_eq!(inner.slot, (3 * 1024 + 42 + 1) % (TombstoneLog::SLOTS_PER_PAGE * 4));
            let (page, _) = log.slot_addr(inner.slot);
            assert_eq!(inner.buffer.page, page);
        }
    }
}
