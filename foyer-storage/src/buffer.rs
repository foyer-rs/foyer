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
    flusher_v2::Entry,
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
    /// io buffer
    ///
    /// # Safety
    ///
    /// `buffer` should always be `Some`. The usage of `Option` is for temporarily taking ownership.
    buffer: Option<Vec<u8, D::IoBufferAllocator>>,

    /// current writing region
    region: Option<RegionId>,

    /// current buffer offset of current writing region
    offset: usize,

    /// entries in io buffer waiting for flush
    entries: Vec<PositionedEntry>,

    // underlying device
    device: D,
}

impl<D> FlushBuffer<D>
where
    D: Device,
{
    pub fn new(device: D) -> Self {
        let buffer = Some(device.io_buffer(0, device.io_size()));
        Self {
            buffer,
            region: None,
            offset: 0,
            entries: vec![],
            device,
        }
    }

    pub fn region(&self) -> Option<RegionId> {
        self.region
    }

    pub fn remaining(&self) -> usize {
        if self.region.is_none() {
            return 0;
        }
        self.device.region_size() - self.offset - self.buffer.as_ref().unwrap().len()
    }

    /// Flush io buffer if necessary, and reset io buffer to a new region.
    ///
    /// Returns fully flushed entries.
    pub async fn rotate(&mut self, region: RegionId) -> BufferResult<Vec<PositionedEntry>> {
        let entries = self.flush().await?;
        debug_assert!(self.buffer.as_ref().unwrap().is_empty());
        self.region = Some(region);
        self.offset = 0;

        // write region header
        let buffer = self.buffer.as_mut().unwrap();
        unsafe { buffer.set_len(self.device.align()) };
        let header = RegionHeader {
            magic: REGION_MAGIC,
        };
        header.write(buffer);

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
        let mut buffer = self.buffer.take().unwrap();
        let len = align_up(self.device.align(), buffer.len());
        debug_assert!(len <= buffer.capacity());
        unsafe { buffer.set_len(len) };
        debug_assert!(self.offset + buffer.len() <= self.device.region_size());

        // flush and clear buffer
        let (res, mut buffer) = self
            .device
            .write(buffer, .., region, self.offset as u64)
            .await;
        buffer.clear();
        self.buffer = Some(buffer);
        res?;

        // advance io buffer
        self.offset += self.device.io_size();
        if self.offset == self.device.region_size() {
            self.region = None;
        }

        let mut entries = vec![];
        std::mem::swap(&mut self.entries, &mut entries);
        Ok(entries)
    }

    /// Write entry to io buffer.
    ///
    /// The io buffer may be flushed if needed.
    ///
    /// Returns fully flushed entries if there is enough space in the current region.
    /// Otherwise, returns `NotEnough` error with the given `entry`.
    pub async fn write(&mut self, entry: Entry) -> BufferResult<Vec<PositionedEntry>> {
        // check region remaining size
        let padding = align_up(self.device.align(), entry.view.len()) - entry.view.len();
        if self.remaining() < entry.view.len() + padding {
            return Err(BufferError::NotEnough { entry });
        }

        let region = self.region.unwrap();
        let offset = self.offset + self.buffer.as_ref().unwrap().len();

        let mut entries = vec![];
        let mut written = 0;

        // write view
        let mut flushed = true;
        while written < entry.view.len() {
            flushed = false;
            let buffer = self.buffer.as_mut().unwrap();
            let bytes = std::cmp::min(
                entry.view.len() - written,
                self.device.io_size() - buffer.len(),
            );
            std::io::copy(&mut &entry.view[written..written + bytes], buffer)
                .map_err(DeviceError::from)?;
            written += bytes;

            if buffer.len() == self.device.io_size() {
                entries.append(&mut self.flush().await?);
                flushed = true;
            }
        }

        // write padding
        let buffer = self.buffer.as_mut().unwrap();
        debug_assert!(self.device.io_size() - buffer.len() >= padding);
        unsafe { buffer.set_len(buffer.len() + padding) };
        debug_assert!(is_aligned(self.device.align(), buffer.len()));
        if buffer.len() == self.device.io_size() {
            entries.append(&mut self.flush().await?);
            flushed = true;
        }

        let entry = PositionedEntry {
            entry,
            region,
            offset,
        };
        if flushed {
            entries.push(entry);
        } else {
            self.entries.push(entry);
        }

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
        ring::{RingBuffer, View},
    };

    use super::*;

    fn ent(view: View) -> Entry {
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

        let ring = Arc::new(RingBuffer::new(4 * 1024, 4 * 1024)); // 16 MiB
        let device = FsDevice::open(FsDeviceConfig {
            dir: tempdir.path().into(),
            capacity: 256 * 1024,     // 256 KiB
            file_capacity: 64 * 1024, // 64 KiB
            align: 4 * 1024,          // 4 KiB
            io_size: 16 * 1024,       // 16 KiB
        })
        .await
        .unwrap();

        let mut buffer = FlushBuffer::new(device.clone());
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

            let entries = buffer.write(entry.clone()).await.unwrap();
            assert!(entries.is_empty());
            let entries = buffer.write(entry.clone()).await.unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].offset, 4 * 1024);

            let entries = buffer.flush().await.unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].offset, 12 * 1024);

            let buf = device.io_buffer(64 * 1024, 64 * 1024);
            let (res, buf) = device.read(buf, .., 0, 0).await;
            res.unwrap();
            assert_eq!(&buf[4 * 1024..9 * 1024 - 128], &[b'x'; 5 * 1024 - 128]);
            assert_eq!(&buf[12 * 1024..17 * 1024 - 128], &[b'x'; 5 * 1024 - 128]);

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

            let entries = buffer.write(entry).await.unwrap();
            assert!(entries.is_empty());

            let view = {
                let mut view = ring.allocate(3 * 1024 - 128, 2).await; // ~ 3 KiB
                (&mut view[..]).put_slice(&[b'x'; 3 * 1024 - 128]);
                view.freeze()
            };
            let entry = ent(view);

            let entries = buffer.write(entry).await.unwrap();
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0].offset, 4 * 1024);
            assert_eq!(entries[1].offset, 60 * 1024);

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
