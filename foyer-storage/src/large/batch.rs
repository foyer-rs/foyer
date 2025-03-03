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

//！ # Blob Format
//！
//！ Disk cache is written in blobs:
//！
//！ ```plain
//！ | blob 1 | blob 2 | ... | blob N |
//！ ```
//！
//！ The format of the blob looks like:
//！
//！ ```plain
//！ | checksum (4B, ALIGN without self) | size (8B) | count(4B) |       <========== meta
//！ | index 1 | index 2 | ... | index N |                               <========== ... (4K in total)
//！ | entry 1 | entry 2 | ... | entry N |                               <========== data (4K aligned)
//！ ```
//！
//！ On recovery, only the indices needs to be read. The data part can be skipped.
//!
//! # Batch Format
//!
//! A batch may contains data in multiple regions and blobs. Data in the same region is combined into a **Window**.

use std::{
    fmt::Debug,
    ops::{Deref, Range},
    sync::Arc,
};

use bytes::{Buf, BufMut};
use foyer_common::{
    bits,
    code::{StorageKey, StorageValue},
    metrics::Metrics,
};
use itertools::Itertools;

use super::serde::Sequence;
use crate::{
    error::{Error, Result},
    io::{IoBuffer, OwnedIoSlice, PAGE},
    large::serde::EntryHeader,
    serde::{Checksummer, EntrySerializer},
    Compression,
};

/// The op that the caller needs to perform.
#[derive(Debug, PartialEq, Eq)]
pub enum Op {
    /// No op.
    Noop,
    /// Split the writer and retry write.
    SplitRetry,
    /// Skip this entry.
    Skip,
}

/// [`EntryWriter`] helps serialize entry into a dest.
pub trait EntryWriter {
    type Write;
    type Target;

    /// Serialize an kv entry into the dest.
    fn push<K, V>(&mut self, key: &K, value: &V, hash: u64, compression: Compression, sequence: Sequence) -> Op
    where
        K: StorageKey,
        V: StorageValue;

    /// Serialize an serialized kv entry slice into the dest.
    fn push_slice(&mut self, slice: &[u8], hash: u64, sequence: Sequence) -> Op;

    /// Return whether there is data needs to be flushed.
    fn is_empty(&self) -> bool;

    /// Finish serializing, return the dest (whole or part, based on the writer impl) and the target.
    fn finish(self) -> (Self::Write, Self::Target);
}

/// [`EntryIndex`] index entry in the blob, which can be used to speed up recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryIndex {
    /// Entry hash.
    pub hash: u64,
    /// Entry sequence.
    pub sequence: Sequence,
    /// Offset to the blob head.
    pub offset: u32,
    /// Length of the entry.
    pub len: u32,
}

impl EntryIndex {
    pub const fn serialized_len() -> usize {
        8 + 8 + 4 + 4
    }

    pub fn write(&self, buf: &mut [u8]) {
        (&mut buf[0..8]).put_u64(self.hash);
        (&mut buf[8..16]).put_u64(self.sequence);
        (&mut buf[16..20]).put_u32(self.offset);
        (&mut buf[20..24]).put_u32(self.len);
    }

    pub fn read(buf: &[u8]) -> Self {
        let hash = (&buf[0..8]).get_u64();
        let sequence = (&buf[8..16]).get_u64();
        let offset = (&buf[16..20]).get_u32();
        let len = (&buf[20..24]).get_u32();
        Self {
            hash,
            sequence,
            offset,
            len,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Blob {
    pub size: usize,
    pub entry_indices: Vec<EntryIndex>,
}

impl Blob {
    const META_CHECKSUM_BYTES: usize = 4;
    const SIZE_BYTES: usize = 8;
    const ENTRY_COUNT_BYTES: usize = 4;

    /// The maximum entry count in a single blob.
    const ENTRY_CAPACITY: usize = (PAGE - (Self::META_CHECKSUM_BYTES + Self::SIZE_BYTES + Self::ENTRY_COUNT_BYTES))
        / EntryIndex::serialized_len();

    const SIZE_OFFSET: usize = Self::META_CHECKSUM_BYTES;
    const ENTRY_COUNT_OFFSET: usize = Self::META_CHECKSUM_BYTES + Self::SIZE_BYTES;

    const ENTRY_INDEX_OFFSET: usize = Self::META_CHECKSUM_BYTES + Self::SIZE_BYTES + Self::ENTRY_COUNT_BYTES;
    const DATA_OFFSET: usize = PAGE;
}

#[derive(Debug, PartialEq, Eq)]
pub struct Window {
    pub absolute_window_range: Range<usize>,
    pub absolute_dirty_range: Range<usize>,
    pub blobs: Vec<Blob>,
}

impl Window {
    pub fn is_empty(&self) -> bool {
        self.blobs.is_empty()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Batch {
    pub windows: Vec<Window>,
}

#[derive(Debug)]
struct BlobWriter {
    io_slice: OwnedIoSlice,

    entry_indices: Vec<EntryIndex>,

    data_len: usize,

    metrics: Arc<Metrics>,
}

impl BlobWriter {
    /// The io slice must match the blob capacity.
    fn new(io_slice: OwnedIoSlice, metrics: Arc<Metrics>) -> Self {
        Self {
            io_slice,
            entry_indices: vec![],
            data_len: 0,
            metrics,
        }
    }

    fn next_entry_index_offset(&self) -> usize {
        Blob::ENTRY_INDEX_OFFSET + self.entry_indices.len() * EntryIndex::serialized_len()
    }
}

impl EntryWriter for BlobWriter {
    type Write = OwnedIoSlice;
    type Target = Option<Blob>;

    /// Push an entry into the blob.
    fn push<K, V>(&mut self, key: &K, value: &V, hash: u64, compression: Compression, sequence: Sequence) -> Op
    where
        K: StorageKey,
        V: StorageValue,
    {
        tracing::trace!("[blob writer]: push hash: {hash}");

        // Entry count reach capacity, split and retry.
        if self.entry_indices.len() >= Blob::ENTRY_CAPACITY {
            return Op::SplitRetry;
        }

        let entry_offset = Blob::DATA_OFFSET + self.data_len;

        // If there is no space even for entry header, skip
        if entry_offset + EntryHeader::serialized_len() >= self.io_slice.len() {
            return Op::SplitRetry;
        }

        tracing::trace!("[blob writer]: try write hash: {hash}");

        let info = match EntrySerializer::serialize(
            key,
            value,
            compression,
            &mut self.io_slice[entry_offset + EntryHeader::serialized_len()..],
            &self.metrics,
        ) {
            Ok(info) => info,
            Err(Error::SizeLimit) => return Op::SplitRetry,
            Err(e) => {
                tracing::warn!("[blob writer]: serialize entry kv error: {e}");
                return Op::Skip;
            }
        };
        let serialized_len = EntryHeader::serialized_len() + info.key_len as usize + info.value_len as usize;

        let checksum = Checksummer::checksum64(
            &self.io_slice[entry_offset + EntryHeader::serialized_len()
                ..entry_offset + EntryHeader::serialized_len() + info.key_len as usize + info.value_len as usize],
        );
        let header = EntryHeader {
            key_len: info.key_len as _,
            value_len: info.value_len as _,
            hash,
            sequence,
            checksum,
            compression,
        };
        header.write(&mut self.io_slice[entry_offset..entry_offset + EntryHeader::serialized_len()]);

        let index = EntryIndex {
            hash,
            sequence,
            offset: entry_offset as _,
            len: serialized_len as _,
        };
        let index_offset = self.next_entry_index_offset();
        index.write(&mut self.io_slice[index_offset..index_offset + EntryIndex::serialized_len()]);
        self.entry_indices.push(index);

        let aligned = bits::align_up(
            PAGE,
            EntryHeader::serialized_len() + info.key_len as usize + info.value_len as usize,
        );
        self.data_len += aligned;

        tracing::trace!("[blob writer]: finish write hash: {hash}");

        Op::Noop
    }

    /// Push an entry slice into the blob.
    fn push_slice(&mut self, slice: &[u8], hash: u64, sequence: Sequence) -> Op {
        tracing::trace!("[blob writer]: try push hash (slice): {hash}");

        // Entry count reach capacity, split and retry.
        if self.entry_indices.len() >= Blob::ENTRY_CAPACITY {
            return Op::SplitRetry;
        }

        let entry_offset = Blob::DATA_OFFSET + self.data_len;

        let aligned = bits::align_up(PAGE, slice.len());

        // If there is no space for the entry, skip
        if entry_offset + aligned > self.io_slice.len() {
            return Op::SplitRetry;
        }

        tracing::trace!("[blob writer]: try write hash (slice): {hash}");

        self.io_slice[entry_offset..entry_offset + slice.len()].copy_from_slice(slice);

        let index = EntryIndex {
            hash,
            sequence,
            offset: entry_offset as _,
            len: slice.len() as _,
        };
        let index_offset = self.next_entry_index_offset();
        index.write(&mut self.io_slice[index_offset..index_offset + EntryIndex::serialized_len()]);
        self.entry_indices.push(index);

        self.data_len += aligned;

        tracing::trace!("[blob writer]: finish write hash (slice): {hash}");

        Op::Noop
    }

    fn is_empty(&self) -> bool {
        self.entry_indices.is_empty()
    }

    /// Return the remaining slice and blob info (if any).
    fn finish(mut self) -> (OwnedIoSlice, Option<Blob>) {
        if self.entry_indices.is_empty() {
            tracing::trace!(
                len = self.io_slice.len(),
                "[blob writer]: finish (empty), return io slice",
            );
            return (self.io_slice, None);
        }

        let size = Blob::DATA_OFFSET + self.data_len;
        bits::assert_aligned(PAGE, size);

        (&mut self.io_slice[Blob::SIZE_OFFSET..Blob::SIZE_OFFSET + Blob::SIZE_BYTES]).put_u64(size as _);
        (&mut self.io_slice[Blob::ENTRY_COUNT_OFFSET..Blob::ENTRY_COUNT_OFFSET + Blob::ENTRY_COUNT_BYTES])
            .put_u32(self.entry_indices.len() as _);

        let checksum = Checksummer::checksum32(&self.io_slice[Blob::META_CHECKSUM_BYTES..PAGE]);
        (&mut self.io_slice[..Blob::META_CHECKSUM_BYTES]).put_u32(checksum as _);

        let io_slice = self.io_slice.slice(size..);
        tracing::trace!(len = io_slice.len(), "[blob writer]: finish, return io slice");

        (
            io_slice,
            Some(Blob {
                size,
                entry_indices: self.entry_indices,
            }),
        )
    }
}

#[derive(Debug)]
pub struct WindowWriter {
    absolute_window_range: Range<usize>,

    /// NOTE: This is always `Some(..)`.
    blob_writer: Option<BlobWriter>,
    blobs: Vec<Blob>,

    metrics: Arc<Metrics>,
}

impl WindowWriter {
    /// The io slice must match the window capacity.
    fn new(io_slice: OwnedIoSlice, metrics: Arc<Metrics>) -> Self {
        let absolute_range = io_slice.absolute();
        Self {
            absolute_window_range: absolute_range,
            blob_writer: Some(BlobWriter::new(io_slice, metrics.clone())),
            blobs: vec![],
            metrics,
        }
    }
}

impl EntryWriter for WindowWriter {
    type Write = OwnedIoSlice;
    type Target = Window;

    /// Push an entry into the blob.
    fn push<K, V>(&mut self, key: &K, value: &V, hash: u64, compression: Compression, sequence: Sequence) -> Op
    where
        K: StorageKey,
        V: StorageValue,
    {
        match self
            .blob_writer
            .as_mut()
            .unwrap()
            .push(key, value, hash, compression, sequence)
        {
            Op::Noop => return Op::Noop,
            Op::Skip => return Op::Skip,
            Op::SplitRetry => {}
        }

        let (io_slice, blob) = self.blob_writer.take().unwrap().finish();
        self.blob_writer = Some(BlobWriter::new(io_slice, self.metrics.clone()));

        if let Some(blob) = blob {
            self.blobs.push(blob);
        }

        // retry, proxy the op
        self.blob_writer
            .as_mut()
            .unwrap()
            .push(key, value, hash, compression, sequence)
    }

    /// Push an entry slice into the blob.
    fn push_slice(&mut self, slice: &[u8], hash: u64, sequence: Sequence) -> Op {
        match self.blob_writer.as_mut().unwrap().push_slice(slice, hash, sequence) {
            Op::Noop => return Op::Noop,
            Op::Skip => return Op::Skip,
            Op::SplitRetry => {}
        }

        let (io_slice, blob) = self.blob_writer.take().unwrap().finish();
        self.blob_writer = Some(BlobWriter::new(io_slice, self.metrics.clone()));

        if let Some(blob) = blob {
            self.blobs.push(blob);
        }

        // retry, proxy the op
        self.blob_writer.as_mut().unwrap().push_slice(slice, hash, sequence)
    }

    fn is_empty(&self) -> bool {
        self.blobs.is_empty() && self.blob_writer.as_ref().unwrap().is_empty()
    }

    /// Return the remaining buffer and window info.
    fn finish(mut self) -> (OwnedIoSlice, Window) {
        let (io_slice, blob) = self.blob_writer.take().unwrap().finish();

        if let Some(blob) = blob {
            self.blobs.push(blob);
        }

        let absolute_window_range = self.absolute_window_range;
        let absolute_dirty_range = absolute_window_range.start
            ..absolute_window_range.start + self.blobs.iter().map(|blob| blob.size).sum::<usize>();

        let window = Window {
            absolute_window_range,
            absolute_dirty_range,
            blobs: self.blobs,
        };

        (io_slice, window)
    }
}

#[derive(Debug)]
pub struct BatchWriter {
    window_size: usize,
    max_absolute_range: Range<usize>,
    current_window_absolute_range: Range<usize>,

    /// NOTE: This is always `Some(..)`.
    window_writer: Option<WindowWriter>,
    windows: Vec<Window>,

    metrics: Arc<Metrics>,
}

impl BatchWriter {
    pub fn new(io_buffer: IoBuffer, window_size: usize, first_window_size: usize, metrics: Arc<Metrics>) -> Self {
        let max_absolute_range = 0..io_buffer.len();
        let io_slice = io_buffer
            .into_owned_io_slice()
            .slice(..std::cmp::min(first_window_size, max_absolute_range.end));
        let current_window_absolute_range = io_slice.absolute();
        let window_writer = Some(WindowWriter::new(io_slice, metrics.clone()));
        Self {
            window_size,
            max_absolute_range,
            current_window_absolute_range,
            window_writer,
            windows: vec![],
            metrics,
        }
    }
}

impl EntryWriter for BatchWriter {
    type Write = IoBuffer;
    type Target = Batch;

    fn push<K, V>(&mut self, key: &K, value: &V, hash: u64, compression: Compression, sequence: Sequence) -> Op
    where
        K: StorageKey,
        V: StorageValue,
    {
        tracing::trace!(hash, sequence, "[batch writer] push");

        if self.current_window_absolute_range.start == self.max_absolute_range.end {
            // No space in the batch, skip.
            return Op::Skip;
        }

        match self
            .window_writer
            .as_mut()
            .unwrap()
            .push(key, value, hash, compression, sequence)
        {
            Op::Noop => return Op::Noop,
            Op::Skip => return Op::Skip,
            Op::SplitRetry => {}
        }

        let (io_slice, window) = self.window_writer.take().unwrap().finish();

        // Push a non-empty window, or push an empty window to notify sealing the current region.
        if !window.is_empty() || self.windows.is_empty() {
            tracing::trace!("[batch writer] rotate window");

            self.windows.push(window);

            let new_window_absolute_start = self
                .current_window_absolute_range
                .end
                .clamp(self.max_absolute_range.start, self.max_absolute_range.end);
            let new_window_absolute_end = (new_window_absolute_start + self.window_size)
                .clamp(self.max_absolute_range.start, self.max_absolute_range.end);
            let io_slice = io_slice.absolute_slice(new_window_absolute_start..new_window_absolute_end);
            self.current_window_absolute_range = io_slice.absolute();
            self.window_writer = Some(WindowWriter::new(io_slice, self.metrics.clone()));
        }

        if self.current_window_absolute_range.start == self.max_absolute_range.end {
            // No space in the batch, skip.
            return Op::Skip;
        }

        // retry, proxy the op
        match self
            .window_writer
            .as_mut()
            .unwrap()
            .push(key, value, hash, compression, sequence)
        {
            Op::Noop => Op::Noop,
            // Skip entry if it is already written in a newly split window.
            Op::SplitRetry | Op::Skip => Op::Skip,
        }
    }

    fn push_slice(&mut self, slice: &[u8], hash: u64, sequence: Sequence) -> Op {
        tracing::trace!(hash, sequence, "[batch writer] push (slice)");

        if self.current_window_absolute_range.start == self.max_absolute_range.end {
            // No space in the batch, skip.
            return Op::Skip;
        }

        match self.window_writer.as_mut().unwrap().push_slice(slice, hash, sequence) {
            Op::Noop => return Op::Noop,
            Op::Skip => return Op::Skip,
            Op::SplitRetry => {}
        }

        let (io_slice, window) = self.window_writer.take().unwrap().finish();

        // Push a non-empty window, or push an empty window to notify sealing the current region.
        if !window.is_empty() || self.windows.is_empty() {
            tracing::trace!("[batch writer] rotate window");

            self.windows.push(window);

            let new_window_absolute_start = self
                .current_window_absolute_range
                .end
                .clamp(self.max_absolute_range.start, self.max_absolute_range.end);
            let new_window_absolute_end = (new_window_absolute_start + self.window_size)
                .clamp(self.max_absolute_range.start, self.max_absolute_range.end);
            let io_slice = io_slice.absolute_slice(new_window_absolute_start..new_window_absolute_end);
            self.current_window_absolute_range = io_slice.absolute();
            self.window_writer = Some(WindowWriter::new(io_slice, self.metrics.clone()));
        }

        if self.current_window_absolute_range.start == self.max_absolute_range.end {
            // No space in the batch, skip.
            return Op::Skip;
        }

        // retry, proxy the op
        match self.window_writer.as_mut().unwrap().push_slice(slice, hash, sequence) {
            Op::Noop => Op::Noop,
            // Skip entry if it is already written in a newly split window.
            Op::SplitRetry | Op::Skip => Op::Skip,
        }
    }

    fn is_empty(&self) -> bool {
        self.windows.is_empty() && self.window_writer.as_ref().unwrap().is_empty()
    }

    /// Return the original io slice and the batch.
    fn finish(mut self) -> (IoBuffer, Batch) {
        let (io_slice, window) = self.window_writer.take().unwrap().finish();
        if !window.is_empty() {
            self.windows.push(window);
        }
        let io_buffer = io_slice.into_io_buffer();
        (io_buffer, Batch { windows: self.windows })
    }
}

/// Read the meta of the blob.
#[derive(Debug, Default)]
pub struct BlobReader;

impl BlobReader {
    /// Return all entry indices in the blob, and the blob size in bytes.
    pub fn read<R>(buffer: R) -> Result<Blob>
    where
        R: Deref<Target = [u8]>,
    {
        let buf = &buffer[..PAGE];

        // compare checksum
        let get = Checksummer::checksum32(&buf[Blob::META_CHECKSUM_BYTES..PAGE]);
        let expected = (&buf[..Blob::META_CHECKSUM_BYTES]).get_u32();
        if expected != get {
            return Err(Error::ChecksumMismatch {
                expected: expected as _,
                get: get as _,
            });
        }

        let size = (&buf[Blob::SIZE_OFFSET..Blob::SIZE_OFFSET + Blob::SIZE_BYTES]).get_u64() as usize;
        let entry_count =
            (&buf[Blob::ENTRY_COUNT_OFFSET..Blob::ENTRY_COUNT_OFFSET + Blob::ENTRY_COUNT_BYTES]).get_u32() as usize;

        let entry_indices = buf
            [Blob::ENTRY_INDEX_OFFSET..Blob::ENTRY_INDEX_OFFSET + entry_count * EntryIndex::serialized_len()]
            .chunks_exact(EntryIndex::serialized_len())
            .map(EntryIndex::read)
            .collect_vec();

        Ok(Blob { size, entry_indices })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KB: usize = 1024;

    #[test_log::test]
    fn test_blob_serde() {
        let buf = IoBuffer::new(16 * KB);
        let slice = buf.into_owned_io_slice();

        // 4K (meta)
        let mut writer = BlobWriter::new(slice, Arc::new(Metrics::noop()));

        // 4K
        let op = writer.push(&1, &vec![1u8; 3 * KB], 1, Compression::None, 1);
        assert_eq!(op, Op::Noop);

        // 12K (deny)
        let op = writer.push(&2, &vec![2u8; 11 * KB], 2, Compression::None, 2);
        assert_eq!(op, Op::SplitRetry);

        // 4K
        let op = writer.push(&3, &vec![3u8; 3 * KB], 3, Compression::None, 3);
        assert_eq!(op, Op::Noop);

        let (slice, blob) = writer.finish();
        assert_eq!(slice.len(), 4 * KB);
        let buf = slice.into_io_buffer();
        let w_blob = blob.unwrap();

        let r_blob = BlobReader::read(&buf[..]).unwrap();

        assert_eq!(w_blob, r_blob);
    }

    #[test_log::test]
    fn test_window_serde() {
        let buf = IoBuffer::new(64 * KB);
        let io_slice = buf.into_owned_io_slice().slice(..64 * KB);
        assert_eq!(io_slice.len(), 64 * KB);

        // 4K (meta)
        let mut writer = WindowWriter::new(io_slice, Arc::new(Metrics::noop()));

        // 48K
        let op = writer.push(&1, &vec![1u8; 47 * KB], 1, Compression::None, 1);
        assert_eq!(op, Op::Noop);

        // 48K (deny, blob split) | 4K (meta)
        let op = writer.push(&2, &vec![2u8; 47 * KB], 2, Compression::None, 2);
        assert_eq!(op, Op::SplitRetry);

        // 4K
        let op = writer.push(&3, &vec![3u8; 3 * KB], 3, Compression::None, 3);
        assert_eq!(op, Op::Noop);

        let (io_slice, w_window) = writer.finish();
        assert_eq!(io_slice.len(), 4 * KB);

        let buf = io_slice.into_io_buffer();

        let blob1 = BlobReader::read(&buf[..]).unwrap();
        let blob2 = BlobReader::read(&buf[blob1.size..]).unwrap();
        let r_window = Window {
            absolute_window_range: 0..64 * KB,
            absolute_dirty_range: 0..60 * KB,
            blobs: vec![blob1, blob2],
        };
        assert_eq!(w_window, r_window);

        // Test empty window.

        let io_slice = buf.into_owned_io_slice().slice(60 * KB..);
        assert_eq!(io_slice.len(), 4 * KB);

        // 4K (meta)
        let mut writer = WindowWriter::new(io_slice, Arc::new(Metrics::noop()));

        // 4K (deny)
        let op = writer.push(&4, &vec![4u8; 3 * KB], 4, Compression::None, 4);
        assert_eq!(op, Op::SplitRetry);

        // 4K (deny)
        let op = writer.push(&5, &vec![5u8; 3 * KB], 5, Compression::None, 5);
        assert_eq!(op, Op::SplitRetry);

        let (io_slice, window) = writer.finish();
        assert_eq!(io_slice.len(), 4 * KB);
        assert!(window.is_empty());
    }

    #[test_log::test]
    fn test_batch_serde() {
        let buf = IoBuffer::new(64 * KB);

        // windows:
        //
        // 8 KB | 16 KB | 16 KB | 16 KB | 8 KB

        // w0: 4K (meta) | ..
        let mut writer = BatchWriter::new(buf, 16 * KB, 8 * KB, Arc::new(Metrics::noop()));

        // w0: 4K (meta) | 4K |
        let op = writer.push(&1, &vec![1u8; 3 * KB], 1, Compression::None, 1);
        assert_eq!(op, Op::Noop);

        // w0: 4K (meta) | 4K |
        // w1: 4K (meta) | 4K | ..
        let op = writer.push(&2, &vec![2u8; 3 * KB], 2, Compression::None, 2);
        assert_eq!(op, Op::Noop);

        // w0: 4K (meta) | 4K |
        // w1: 4K (meta) | 4K | 4K | ..
        let op = writer.push(&3, &vec![3u8; 3 * KB], 3, Compression::None, 3);
        assert_eq!(op, Op::Noop);

        // w0: 4K (meta) | 4K |
        // w1: 4K (meta) | 4K | 4K | ..
        // w2: 4K (meta) | 8K | ..
        let op = writer.push(&4, &vec![4u8; 7 * KB], 4, Compression::None, 4);
        assert_eq!(op, Op::Noop);

        // w0: 4K (meta) | 4K |
        // w1: 4K (meta) | 4K | 4K | ..
        // w2: 4K (meta) | 8K | ..
        // w3: 4K (meta) | ..
        let op = writer.push(&5, &vec![5u8; 20 * KB], 5, Compression::None, 5);
        assert_eq!(op, Op::Skip);

        // w0: 4K (meta) | 4K |
        // w1: 4K (meta) | 4K | 4K | ..
        // w2: 4K (meta) | 8K | ..
        // w3: 4K (meta) | 12K |
        let op = writer.push(&6, &vec![6u8; 11 * KB], 6, Compression::None, 6);
        assert_eq!(op, Op::Noop);

        // w0: 4K (meta) | 4K |
        // w1: 4K (meta) | 4K | 4K | ..
        // w2: 4K (meta) | 8K | ..
        // w3: 4K (meta) | 12K |
        // w4: 4K (meta) | 4K |
        let op = writer.push(&7, &vec![7u8; 3 * KB], 7, Compression::None, 7);
        assert_eq!(op, Op::Noop);

        // w0: 4K (meta) | 4K |
        // w1: 4K (meta) | 4K | 4K | ..
        // w2: 4K (meta) | 8K | ..
        // w3: 4K (meta) | 12K |
        // w4: 4K (meta) | 4K |
        let op = writer.push(&7, &vec![8u8; 3 * KB], 8, Compression::None, 8);
        assert_eq!(op, Op::Skip);

        let (buf, w_batch) = writer.finish();

        let blob0 = BlobReader::read(&buf[..]).unwrap();
        let blob1 = BlobReader::read(&buf[8 * KB..]).unwrap();
        let blob2 = BlobReader::read(&buf[24 * KB..]).unwrap();
        let blob3 = BlobReader::read(&buf[40 * KB..]).unwrap();
        let blob4 = BlobReader::read(&buf[56 * KB..]).unwrap();

        let r_batch = Batch {
            windows: vec![
                Window {
                    absolute_window_range: 0..8 * KB,
                    absolute_dirty_range: 0..8 * KB,
                    blobs: vec![blob0],
                },
                Window {
                    absolute_window_range: 8 * KB..24 * KB,
                    absolute_dirty_range: 8 * KB..20 * KB,
                    blobs: vec![blob1],
                },
                Window {
                    absolute_window_range: 24 * KB..40 * KB,
                    absolute_dirty_range: 24 * KB..36 * KB,
                    blobs: vec![blob2],
                },
                Window {
                    absolute_window_range: 40 * KB..56 * KB,
                    absolute_dirty_range: 40 * KB..56 * KB,
                    blobs: vec![blob3],
                },
                Window {
                    absolute_window_range: 56 * KB..64 * KB,
                    absolute_dirty_range: 56 * KB..64 * KB,
                    blobs: vec![blob4],
                },
            ],
        };
        assert_eq!(w_batch, r_batch);

        // w0: 4K (meta) | ..
        let mut writer = BatchWriter::new(buf, 16 * KB, 8 * KB, Arc::new(Metrics::noop()));

        // w0: 4K (meta) | .. (empty)
        // w1: 4K (meta) | 12K | ..
        let op = writer.push(&6, &vec![6u8; 11 * KB], 6, Compression::None, 6);
        assert_eq!(op, Op::Noop);

        let (buf, w_batch) = writer.finish();

        let blob = BlobReader::read(&buf[8 * KB..]).unwrap();

        let r_batch = Batch {
            windows: vec![
                Window {
                    absolute_window_range: 0..8 * KB,
                    absolute_dirty_range: 0..0,
                    blobs: vec![],
                },
                Window {
                    absolute_window_range: 8 * KB..24 * KB,
                    absolute_dirty_range: 8 * KB..24 * KB,
                    blobs: vec![blob],
                },
            ],
        };
        assert_eq!(w_batch, r_batch);
    }

    #[test]
    fn test_buffer_smaller_than_window() {
        let buf = IoBuffer::new(8 * KB);

        // windows:
        //
        // 8 KB | 16 KB | 16 KB | 16 KB | 8 KB

        // w0: 4K (meta) | ..
        let mut writer = BatchWriter::new(buf, 64 * KB, 16 * KB, Arc::new(Metrics::noop()));

        // w0: 4K (meta) | 4K |
        let op = writer.push(&1, &vec![1u8; 3 * KB], 1, Compression::None, 1);
        assert_eq!(op, Op::Noop);

        // (deny)
        let op = writer.push(&2, &vec![2u8; 3 * KB], 2, Compression::None, 2);
        assert_eq!(op, Op::Skip);

        let (buf, w_batch) = writer.finish();

        let blob = BlobReader::read(&buf[..]).unwrap();

        let r_batch = Batch {
            windows: vec![Window {
                absolute_window_range: 0..8 * KB,
                absolute_dirty_range: 0..8 * KB,
                blobs: vec![blob],
            }],
        };
        assert_eq!(w_batch, r_batch);
    }
}
