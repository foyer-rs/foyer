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

use std::fmt::Debug;

use allocator_api2::vec::Vec as VecA;
use either::Either;
use foyer_common::{
    bits::{align_up, is_aligned},
    code::{StorageKey, StorageValue},
};

use crate::{
    device::{allocator::WritableVecA, Device},
    error::Result,
    flusher::Entry,
    region::{RegionHeader, RegionId, Version, REGION_MAGIC},
    serde::EntrySerializer,
};

#[derive(Debug)]
pub struct PositionedEntry<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    pub entry: Entry<K, V>,
    pub region: RegionId,
    pub offset: usize,
    pub len: usize,
}

pub struct FlushBuffer<K, V, D>
where
    K: StorageKey,
    V: StorageValue,
    D: Device,
{
    // TODO(MrCroxx): optimize buffer allocation
    /// io buffer
    buffer: VecA<u8, D::IoBufferAllocator>,

    /// current writing region
    region: Option<RegionId>,

    /// current buffer offset of current writing region
    offset: usize,

    /// entries in io buffer waiting for flush
    entries: Vec<PositionedEntry<K, V>>,

    // underlying device
    device: D,

    default_buffer_capacity: usize,

    flush: bool,
}

impl<K, V, D> Debug for FlushBuffer<K, V, D>
where
    K: StorageKey,
    V: StorageValue,
    D: Device,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlushBuffer")
            .field("region", &self.region)
            .field("offset", &self.offset)
            .field("default_buffer_capacity", &self.default_buffer_capacity)
            .field("flush", &self.flush)
            .finish()
    }
}

impl<K, V, D> FlushBuffer<K, V, D>
where
    K: StorageKey,
    V: StorageValue,
    D: Device,
{
    pub fn new(device: D, flush: bool) -> Self {
        let default_buffer_capacity = align_up(device.align(), device.io_size() + device.io_size() / 2);
        let buffer = device.io_buffer(0, default_buffer_capacity);
        Self {
            buffer,
            region: None,
            offset: 0,
            entries: vec![],
            device,
            default_buffer_capacity,
            flush,
        }
    }

    pub fn region(&self) -> Option<RegionId> {
        self.region
    }

    pub fn remaining(&self) -> usize {
        if self.region.is_none() {
            0
        } else {
            self.device
                .region_size()
                .saturating_sub(self.offset + self.buffer.len())
        }
    }

    /// Flush io buffer if necessary, and reset io buffer to a new region.
    ///
    /// Returns fully flushed entries.
    pub async fn rotate(&mut self, region: RegionId) -> Result<Vec<PositionedEntry<K, V>>> {
        let entries = self.flush().await?;
        debug_assert!(self.buffer.is_empty());
        self.region = Some(region);
        self.offset = 0;

        // write region header
        let len = std::cmp::max(self.device.align(), RegionHeader::serialized_len());
        unsafe { self.buffer.set_len(len) };
        let header = RegionHeader {
            magic: REGION_MAGIC,
            version: Version::latest(),
        };
        header.write(&mut self.buffer[..]);
        debug_assert_eq!(self.buffer.len(), len);

        Ok(entries)
    }

    /// Flush io buffer and move the io buffer to the next position.
    ///
    /// The io buffer will be cleared after flush.
    ///
    /// Returns fully flushed entries.
    pub async fn flush(&mut self) -> Result<Vec<PositionedEntry<K, V>>> {
        let Some(region) = self.region else {
            debug_assert!(self.entries.is_empty());
            return Ok(vec![]);
        };

        // align io buffer
        let len = align_up(self.device.align(), self.buffer.len());
        debug_assert!(len <= self.buffer.capacity());
        unsafe { self.buffer.set_len(len) };
        debug_assert!(self.offset + self.buffer.len() <= self.device.region_size());

        // flush and clear buffer
        let mut buf = self.device.io_buffer(0, self.default_buffer_capacity);
        std::mem::swap(&mut self.buffer, &mut buf);

        let (res, _buf) = self.device.write(buf, .., region, self.offset).await;
        res?;

        // advance io buffer
        self.offset += len;
        if self.offset >= self.device.region_size() {
            self.region = None;
        }

        let mut entries = vec![];
        std::mem::swap(&mut self.entries, &mut entries);

        if self.flush {
            self.device.flush_region(region).await?;
        }

        Ok(entries)
    }

    /// Write entry to io buffer.
    ///
    /// The io buffer may be flushed if buffer size equals or exceeds device io size.
    ///
    /// Returns fully flushed entries if there is enough space in the current region.
    /// Otherwise, returns `NotEnough` error with the given `entry`.
    ///
    /// # Format
    ///
    /// | header | value (compressed) | key | <padding> |
    // TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
    #[allow(clippy::uninit_vec)]
    pub async fn write(
        &mut self,
        Entry {
            key,
            value,
            sequence,
            compression,
        }: Entry<K, V>,
    ) -> Result<Either<Vec<PositionedEntry<K, V>>, Entry<K, V>>> {
        // Notify caller to rotate buffer if there is not enough space for the entry.
        //
        // NOTICE:
        //
        // Buffer remaining size is not compared here because the compressed entry size can be
        // either larger (rarely) or smaller than the uncompressed size and it can not be determined
        // before compression. So we first try to compress it and rollback if it exceeds region size.
        //
        // P.S. About rollback, see (*).
        if self.region.is_none() {
            return Ok(Either::Right(Entry {
                key,
                value,
                sequence,
                compression,
            }));
        }

        let old = self.buffer.len();
        debug_assert!(is_aligned(self.device.align(), old));

        EntrySerializer::serialize(&key, &value, &sequence, &compression, WritableVecA(&mut self.buffer))?;

        // (*) if size exceeds region limit, rollback write and return
        if self.offset + self.buffer.len() > self.device.region_size() {
            unsafe { self.buffer.set_len(old) };
            return Ok(Either::Right(Entry {
                key,
                value,
                sequence,
                compression,
            }));
        }

        // 3. align buffer size
        let target = align_up(self.device.align(), self.buffer.len());
        self.buffer.reserve(target - self.buffer.len());
        unsafe { self.buffer.set_len(target) }

        self.entries.push(PositionedEntry {
            entry: Entry {
                key,
                value,
                sequence,
                compression,
            },
            region: self.region.unwrap(),
            offset: self.offset + old,
            len: self.buffer.len() - old,
        });

        // flush if buffer equals or exceeds device io size
        let entries = if self.buffer.len() >= self.device.io_size() || self.remaining() == 0 {
            self.flush().await?
        } else {
            vec![]
        };

        Ok(Either::Left(entries))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::tempdir;

    use super::*;
    use crate::{
        device::fs::{FsDevice, FsDeviceConfig},
        serde::EntryHeader,
        Compression,
    };

    fn ent(size: usize) -> Entry<(), Vec<u8>> {
        Entry {
            key: Arc::new(()),
            value: Arc::new(vec![b'x'; size]),
            compression: Compression::None,
            sequence: 0,
        }
    }

    fn assert_buffer(positioneds: impl IntoIterator<Item = PositionedEntry<(), Vec<u8>>>, buf: &[u8]) {
        for positioned in positioneds {
            let b = &buf[positioned.offset..positioned.offset + positioned.len];
            let h = EntryHeader::read(b).unwrap();
            let v: &[u8] = bincode::deserialize(
                &b[EntryHeader::serialized_len()..EntryHeader::serialized_len() + h.value_len as usize],
            )
            .unwrap();
            assert_eq!(v, positioned.entry.value.as_ref());
        }
    }

    #[tokio::test]
    async fn test_flush_buffer() {
        let tempdir = tempdir().unwrap();

        let device = FsDevice::open(FsDeviceConfig {
            dir: tempdir.path().into(),
            capacity: 256 * 1024, // 256 KiB
            file_size: 64 * 1024, // 64 KiB
            align: 4 * 1024,      // 4 KiB
            io_size: 16 * 1024,   // 16 KiB
            direct: true,
        })
        .await
        .unwrap();

        let mut buffer = FlushBuffer::new(device.clone(), false);
        assert_eq!(buffer.region(), None);

        {
            let entry = ent(5 * 1024 - 128); // ~ 5 KiB
            let mut positioneds = vec![];

            let res = buffer.write(entry).await;
            let entry = match res {
                Ok(Either::Right(entry)) => entry,
                _ => panic!("got: {:?}", res),
            };

            let entries = buffer.rotate(0).await.unwrap();
            assert!(entries.is_empty());

            // 4 ~ 12 KiB
            let entries = buffer.write(entry.clone()).await.unwrap().unwrap_left();
            assert!(entries.is_empty());
            // 12 ~ 20 KiB
            let entries = buffer.write(entry.clone()).await.unwrap().unwrap_left();
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0].offset, 4 * 1024);
            assert_eq!(entries[1].offset, 12 * 1024);
            positioneds.extend(entries);

            // 20 ~ 28 KiB
            let entries = buffer.write(entry.clone()).await.unwrap().unwrap_left();
            assert!(entries.is_empty());
            let entries = buffer.flush().await.unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].offset, 20 * 1024);
            positioneds.extend(entries);

            let buf = device.io_buffer(64 * 1024, 64 * 1024);
            let (res, buf) = device.read(buf, .., 0, 0).await;
            res.unwrap();

            assert_buffer(positioneds, &buf);

            assert!(buffer.entries.is_empty());
        }

        {
            let entry = ent(54 * 1024 - 128); // ~ 54 KiB
            let mut positioneds = vec![];

            let res = buffer.write(entry).await;
            let entry = match res {
                Ok(Either::Right(entry)) => entry,
                _ => panic!("got: {:?}", res),
            };

            let entries = buffer.rotate(1).await.unwrap();
            assert!(entries.is_empty());

            // 4 ~ 60 KiB
            let entries = buffer.write(entry).await.unwrap().unwrap_left();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].offset, 4 * 1024);
            positioneds.extend(entries);

            let entry = ent(3 * 1024 - 128); // ~ 3 KiB

            // 60 ~ 64 KiB
            let entries = buffer.write(entry).await.unwrap().unwrap_left();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].offset, 60 * 1024);
            positioneds.extend(entries);

            let buf = device.io_buffer(64 * 1024, 64 * 1024);
            let (res, buf) = device.read(buf, .., 1, 0).await;
            res.unwrap();

            assert_buffer(positioneds, &buf);

            assert!(buffer.entries.is_empty());
        }
    }
}
