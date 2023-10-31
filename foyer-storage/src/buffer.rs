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

use foyer_common::bits::{align_up, is_aligned};

use crate::{
    device::{error::DeviceError, Device},
    flusher::Entry,
    region::{RegionHeader, RegionId, REGION_MAGIC},
};

#[derive(thiserror::Error, Debug)]
pub enum BufferError {
    #[error(transparent)]
    Device(#[from] DeviceError),
    #[error("")]
    NotEnough { entry: Entry },
}

pub type BufferResult<T> = std::result::Result<T, BufferError>;

#[derive(Debug)]
pub struct PositionedEntry {
    pub entry: Entry,
    pub region: RegionId,
    pub offset: usize,
}

#[derive(Debug)]
pub struct FlushBuffer<D>
where
    D: Device,
{
    // TODO(MrCroxx): optimize buffer allocation
    /// io buffer
    buffer: Vec<u8, D::IoBufferAllocator>,

    /// current writing region
    region: Option<RegionId>,

    /// current buffer offset of current writing region
    offset: usize,

    /// entries in io buffer waiting for flush
    entries: Vec<PositionedEntry>,

    // underlying device
    device: D,

    default_buffer_capacity: usize,
}

impl<D> FlushBuffer<D>
where
    D: Device,
{
    pub fn new(device: D, default_buffer_capacity: usize) -> Self {
        let default_buffer_capacity = std::cmp::max(default_buffer_capacity, device.io_size());
        let buffer = device.io_buffer(0, default_buffer_capacity);
        Self {
            buffer,
            region: None,
            offset: 0,
            entries: vec![],
            device,
            default_buffer_capacity,
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
    pub async fn rotate(&mut self, region: RegionId) -> BufferResult<Vec<PositionedEntry>> {
        let entries = self.flush().await?;
        debug_assert!(self.buffer.is_empty());
        self.region = Some(region);
        self.offset = 0;

        // write region header
        unsafe { self.buffer.set_len(self.device.align()) };
        let header = RegionHeader {
            magic: REGION_MAGIC,
        };
        header.write(&mut self.buffer[..]);
        debug_assert_eq!(self.buffer.len(), self.device.align());

        Ok(entries)
    }

    /// Flush io buffer and move the io buffer to the next position.
    ///
    /// The io buffer will be cleared after flush.
    ///
    /// Returns fully flushed entries.
    pub async fn flush(&mut self) -> BufferResult<Vec<PositionedEntry>> {
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
        if self.offset == self.device.region_size() {
            self.region = None;
        }

        let mut entries = vec![];
        std::mem::swap(&mut self.entries, &mut entries);
        Ok(entries)
    }

    /// Write entry to io buffer.
    ///
    /// The io buffer may be flushed if buffer size equals or exceeds device io size.
    ///
    /// Returns fully flushed entries if there is enough space in the current region.
    /// Otherwise, returns `NotEnough` error with the given `entry`.
    pub async fn write(&mut self, entry: Entry) -> BufferResult<Vec<PositionedEntry>> {
        // check region remaining size
        let len = align_up(self.device.align(), entry.view.len());
        if self.remaining() < len {
            return Err(BufferError::NotEnough { entry });
        }

        // write view and padding
        let region = self.region.unwrap();
        let offset = self.offset + self.buffer.len();
        let target_len = self.buffer.len() + len;
        debug_assert!(is_aligned(self.device.align(), offset));

        self.buffer.reserve_exact(len);
        std::io::copy(&mut &entry.view[..], &mut self.buffer).map_err(DeviceError::from)?;
        unsafe { self.buffer.set_len(target_len) };
        self.entries.push(PositionedEntry {
            entry,
            region,
            offset,
        });

        // flush if buffer equals or exceeds device io size
        let entries = if self.buffer.len() >= self.device.io_size() || self.remaining() == 0 {
            self.flush().await?
        } else {
            vec![]
        };

        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::BufMut;
    use tempfile::tempdir;

    use crate::{
        device::fs::{FsDevice, FsDeviceConfig},
        ring::{RingBuffer, RingBufferView},
    };

    use super::*;

    fn ent(view: RingBufferView) -> Entry {
        Entry {
            key: Arc::new(()),
            view,
            key_len: 0,
            value_len: 0,
            sequence: 0,
        }
    }

    #[tokio::test]
    async fn test_flush_buffer() {
        let tempdir = tempdir().unwrap();

        let ring = Arc::new(RingBuffer::new(4096, 16 * 1024 * 1024));
        let device = FsDevice::open(FsDeviceConfig {
            dir: tempdir.path().into(),
            capacity: 256 * 1024,     // 256 KiB
            file_capacity: 64 * 1024, // 64 KiB
            align: 4 * 1024,          // 4 KiB
            io_size: 16 * 1024,       // 16 KiB
        })
        .await
        .unwrap();
        const DEFAULT_BUFFER_CAPACITY: usize = 32 * 1024;

        let mut buffer = FlushBuffer::new(device.clone(), DEFAULT_BUFFER_CAPACITY);
        assert_eq!(buffer.region(), None);

        {
            let view = {
                let mut view = ring.allocate(5 * 1024 - 128, 0).await; // ~ 6 KiB
                (&mut view[..]).put_slice(&[b'x'; 5 * 1024 - 128]);
                view.shrink_to(5 * 1024 - 128); // ~ 5 KiB
                view.freeze()
            };
            let entry = ent(view);
            assert_eq!(ring.continuum(), 0);

            let res = buffer.write(entry).await;
            let entry = match res {
                Err(BufferError::NotEnough { entry }) => entry,
                _ => panic!("should be not enough error"),
            };

            let entries = buffer.rotate(0).await.unwrap();
            assert!(entries.is_empty());

            // 4 ~ 12 KiB
            let entries = buffer.write(entry.clone()).await.unwrap();
            assert!(entries.is_empty());
            // 12 ~ 20 KiB
            let entries = buffer.write(entry.clone()).await.unwrap();
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0].offset, 4 * 1024);
            assert_eq!(entries[1].offset, 12 * 1024);

            // 20 ~ 28 KiB
            let entries = buffer.write(entry.clone()).await.unwrap();
            assert!(entries.is_empty());
            let entries = buffer.flush().await.unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].offset, 20 * 1024);

            let buf = device.io_buffer(64 * 1024, 64 * 1024);
            let (res, buf) = device.read(buf, .., 0, 0).await;
            res.unwrap();
            assert_eq!(&buf[4 * 1024..9 * 1024 - 128], &[b'x'; 5 * 1024 - 128]);
            assert_eq!(&buf[12 * 1024..17 * 1024 - 128], &[b'x'; 5 * 1024 - 128]);
            assert_eq!(&buf[20 * 1024..25 * 1024 - 128], &[b'x'; 5 * 1024 - 128]);

            assert!(buffer.entries.is_empty());
        }

        ring.advance();
        assert_eq!(ring.continuum(), 8 * 1024);

        {
            let view = {
                let mut view = ring.allocate(55 * 1024 - 128, 1).await; // ~ 55 KiB
                (&mut view[..]).put_slice(&[b'x'; 54 * 1024 - 128]);
                view.shrink_to(54 * 1024 - 128); // ~ 54 KiB
                view.freeze()
            };
            let entry = ent(view);

            let res = buffer.write(entry).await;
            let entry = match res {
                Err(BufferError::NotEnough { entry }) => entry,
                _ => panic!("should be not enough error"),
            };

            let entries = buffer.rotate(1).await.unwrap();
            assert!(entries.is_empty());

            // 4 ~ 60 KiB
            let entries = buffer.write(entry).await.unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].offset, 4 * 1024);

            let view = {
                let mut view = ring.allocate(3 * 1024 - 128, 2).await; // ~ 3 KiB
                (&mut view[..]).put_slice(&[b'x'; 3 * 1024 - 128]);
                view.freeze()
            };
            let entry = ent(view);

            // 60 ~ 64 KiB
            let entries = buffer.write(entry).await.unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].offset, 60 * 1024);

            let buf = device.io_buffer(64 * 1024, 64 * 1024);
            let (res, buf) = device.read(buf, .., 1, 0).await;
            res.unwrap();
            assert_eq!(&buf[4 * 1024..58 * 1024 - 128], &[b'x'; 54 * 1024 - 128]);
            assert_eq!(&buf[60 * 1024..63 * 1024 - 128], &[b'x'; 3 * 1024 - 128]);

            assert!(buffer.entries.is_empty());
        }

        ring.advance();
        assert_eq!(ring.continuum(), 68 * 1024);
    }
}
