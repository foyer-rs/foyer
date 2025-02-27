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

//！ # Format
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

use std::{fmt::Debug, ops::Range, sync::Arc};

use bytes::{Buf, BufMut};
use foyer_common::{
    bits,
    code::{StorageKey, StorageValue},
    metrics::Metrics,
};
use itertools::Itertools;

use crate::{
    device::ALIGN,
    large::serde::{EntryHeader, Sequence},
    serde::{Checksummer, EntrySerializer},
    Compression, Error, Result,
};

const BLOB_META_CHECKSUM_BYTES: usize = 4;
const BLOB_SIZE_BYTES: usize = 8;
const BLOB_ENTRY_COUNT_BYTES: usize = 4;

/// The maximum entry count in a single blob.
const BLOB_ENTRY_CAPACITY: usize =
    (ALIGN - (BLOB_META_CHECKSUM_BYTES + BLOB_SIZE_BYTES + BLOB_ENTRY_COUNT_BYTES)) / BlobEntryIndex::serialized_len();

const BLOB_SIZE_OFFSET: usize = BLOB_META_CHECKSUM_BYTES;
const BLOB_ENTRY_COUNT_OFFSET: usize = BLOB_META_CHECKSUM_BYTES + BLOB_SIZE_BYTES;

const BLOB_ENTRY_INDEX_OFFSET: usize = BLOB_META_CHECKSUM_BYTES + BLOB_SIZE_BYTES + BLOB_ENTRY_COUNT_BYTES;
const BLOB_DATA_OFFSET: usize = ALIGN;

#[derive(Debug, Clone)]
pub struct BlobEntryIndex {
    /// Offset to the blob head.
    pub offset: u32,
    /// Length of the entry.
    pub len: u32,
    /// Entry sequence.
    pub sequence: Sequence,
}

#[derive(Debug, Clone)]
pub struct BlobEntryInfo {
    pub hash: u64,
    pub index: BlobEntryIndex,
}

#[derive(Debug, Clone)]
pub struct BlobInfo {
    pub window: usize,
    pub range: Range<usize>,
    pub entries: Vec<BlobEntryInfo>,
}

impl BlobEntryIndex {
    pub const fn serialized_len() -> usize {
        4 + 4 + 8
    }

    pub fn write(&self, buf: &mut [u8]) {
        (&mut buf[0..4]).put_u32(self.offset);
        (&mut buf[4..8]).put_u32(self.len);
        (&mut buf[8..16]).put_u64(self.sequence);
    }

    pub fn read(buf: &[u8]) -> Self {
        let offset = (&buf[0..4]).get_u32();
        let len = (&buf[4..8]).get_u32();
        let sequence = (&buf[8..16]).get_u64();
        Self { offset, len, sequence }
    }
}

/// Judge if the blob writer needs to split based on window.
#[derive(Debug)]
pub struct WindowSpliter {
    /// window index
    id: usize,
    /// Windos size.
    window: usize,
    /// Remaining bytes of the current window,
    remain: usize,
}

impl WindowSpliter {
    pub fn new(window: usize, first: usize) -> Self {
        Self {
            id: 0,
            window,
            remain: first,
        }
    }

    /// Judge if the blob needs to be split.
    fn judge(&self, blob_size: usize, serialized_len: usize) -> Split {
        let aligned = bits::align_up(ALIGN, serialized_len);

        tracing::trace!(
            "[judge]: blob size: {blob_size}, aligned: {aligned}, remain: {}, window: {}",
            self.remain,
            self.window
        );

        if aligned > self.window {
            return Split::Deny;
        }

        if blob_size + aligned > self.remain {
            return Split::Need;
        }

        if self.remain == 0 {
            return Split::Exact;
        }

        Split::NoNeed
    }

    fn split(&mut self, blob_size: usize) {
        if self.remain >= blob_size {
            self.remain -= blob_size;
        } else {
            self.remain = self.window - blob_size;
            self.id += 1;
            tracing::trace!("rotate window {} => {}", self.id - 1, self.id);
        }
    }

    fn split_empty(&mut self) {
        self.remain = self.window;
        self.id += 1;
        tracing::trace!("rotate window {} => {}", self.id - 1, self.id);
    }
}

#[derive(Debug)]
enum Split {
    /// Need to split blob.
    Need,
    /// No need to split blob.
    NoNeed,
    /// Need to split blob exactly after writting this entry.
    Exact,
    /// Deny the entry for it is larger than the window size.
    Deny,
}

pub struct MultiBlobWriter<W> {
    writer: W,

    spliter: WindowSpliter,

    blob_offset: usize,

    blob_data_len: usize,

    blob_entries: Vec<BlobEntryInfo>,
    sealed_blobs: Vec<BlobInfo>,

    metrics: Arc<Metrics>,
}

impl<W> Debug for MultiBlobWriter<W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiBlobWriter")
            .field("spliter", &self.spliter)
            .field("blob_offset", &self.blob_offset)
            .field("blob_data_len", &self.blob_data_len)
            .field("blob_entries", &self.blob_entries)
            .field("sealed_blobs", &self.sealed_blobs)
            .field("metrics", &self.metrics)
            .finish()
    }
}

impl<W> MultiBlobWriter<W>
where
    W: AsMut<[u8]>,
{
    pub fn new(buffer: W, spliter: WindowSpliter, metrics: Arc<Metrics>) -> Self {
        Self {
            writer: buffer,
            spliter,
            blob_offset: 0,
            blob_data_len: 0,
            blob_entries: vec![],
            sealed_blobs: vec![],
            metrics,
        }
    }

    /// Push a entry into the blob.
    ///
    /// Return weather the entry is inserted.
    pub fn push<K, V>(&mut self, key: &K, value: &V, hash: u64, compression: Compression, sequence: Sequence) -> bool
    where
        K: StorageKey,
        V: StorageValue,
    {
        tracing::trace!("try push hash: {hash}");

        // Entry count overflow, split and retry.
        if self.blob_entries.len() >= BLOB_ENTRY_CAPACITY {
            self.split();
            return self.push(key, value, hash, compression, sequence);
        }

        let entry_offset = self.blob_data_offset() + self.blob_data_len;

        // If there is no space even for entry header, skip
        if entry_offset + EntryHeader::serialized_len() >= self.writer.as_mut().len() {
            return false;
        }

        tracing::trace!("try write hash: {hash}");

        // Serialize entry kv error (serde error or buffer overflow), log it and skip.
        let info = match EntrySerializer::serialize(
            key,
            value,
            compression,
            &mut self.writer.as_mut()[entry_offset + EntryHeader::serialized_len()..],
            &self.metrics,
        ) {
            Ok(info) => info,
            Err(e) => {
                tracing::warn!("[lodc batch]: serialize entry kv error: {e}");
                return false;
            }
        };
        let serialized_len = EntryHeader::serialized_len() + info.key_len as usize + info.value_len as usize;

        let split = self.spliter.judge(self.blob_size(), serialized_len);
        tracing::trace!("split: {split:?}");
        let split = match split {
            Split::Need => {
                self.split();
                return self.push(key, value, hash, compression, sequence);
            }
            Split::NoNeed => false,
            Split::Exact => true,
            Split::Deny => return false,
        };

        let checksum = Checksummer::checksum64(
            &self.writer.as_mut()[entry_offset + EntryHeader::serialized_len()
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
        header.write(&mut self.writer.as_mut()[entry_offset..entry_offset + EntryHeader::serialized_len()]);

        let entry_len = EntryHeader::serialized_len() + info.key_len + info.value_len;
        let blob_entry_offset = entry_offset - self.blob_offset;

        let index = BlobEntryIndex {
            offset: blob_entry_offset as _,
            len: entry_len as _,
            sequence,
        };
        let index_offset = self.blob_next_entry_index_offset();
        index.write(&mut self.writer.as_mut()[index_offset..index_offset + BlobEntryIndex::serialized_len()]);

        let aligned = bits::align_up(
            ALIGN,
            EntryHeader::serialized_len() + info.key_len as usize + info.value_len as usize,
        );
        self.blob_data_len += aligned;

        self.blob_entries.push(BlobEntryInfo { hash, index });

        tracing::trace!("finish write hash: {hash}");

        if split {
            self.split();
        }

        true
    }

    pub fn push_slice(&mut self, slice: &[u8], hash: u64, sequence: Sequence) -> bool {
        tracing::trace!("try push hash (slice): {hash}");

        // Entry count overflow, split and retry.
        if self.blob_entries.len() >= BLOB_ENTRY_CAPACITY {
            self.split();
            return self.push_slice(slice, hash, sequence);
        }

        let entry_offset = self.blob_data_offset() + self.blob_data_len;

        let aligned = bits::align_up(ALIGN, slice.len());

        // If there is no space for the entry, skip
        if entry_offset + aligned > self.writer.as_mut().len() {
            return false;
        }

        let split = self.spliter.judge(self.blob_size(), aligned);
        tracing::trace!("split: {split:?}");
        let split = match split {
            Split::Need => {
                self.split();
                return self.push_slice(slice, hash, sequence);
            }
            Split::NoNeed => false,
            Split::Exact => true,
            Split::Deny => return false,
        };

        tracing::trace!("try write hash (hash): {hash}");

        self.writer.as_mut()[entry_offset..entry_offset + slice.len()].copy_from_slice(slice);

        let blob_entry_offset = entry_offset - self.blob_offset;
        let index = BlobEntryIndex {
            offset: blob_entry_offset as _,
            len: slice.len() as _,
            sequence,
        };
        let index_offset = self.blob_next_entry_index_offset();
        index.write(&mut self.writer.as_mut()[index_offset..index_offset + BlobEntryIndex::serialized_len()]);

        self.blob_data_len += aligned;

        self.blob_entries.push(BlobEntryInfo { hash, index });

        tracing::trace!("finish write hash (hash): {hash}");

        if split {
            self.split();
        }

        true
    }

    /// Seal the current blob and perpare a new one.
    ///
    /// The perparing doesn't write any data. It is okay to be called as the last blob.
    pub fn split(&mut self) {
        tracing::trace!("split");

        if self.blob_entries.is_empty() {
            self.spliter.split_empty();
            return;
        }

        let window = self.spliter.id;

        let blob_size = self.blob_size();
        bits::assert_aligned(ALIGN, blob_size);

        self.spliter.split(blob_size);

        let range = self.blob_size_range();
        (&mut self.writer.as_mut()[range]).put_u64(blob_size as _);

        let range = self.blob_entry_count_range();
        let entry_count = self.blob_entries.len();
        (&mut self.writer.as_mut()[range]).put_u32(entry_count as _);

        let range = self.blob_checksum_data_range();
        let checksum = Checksummer::checksum32(&self.writer.as_mut()[range]);
        let range = self.blob_checksum_range();
        (&mut self.writer.as_mut()[range]).put_u32(checksum);

        let entries = std::mem::replace(&mut self.blob_entries, vec![]);
        self.sealed_blobs.push(BlobInfo {
            window,
            range: self.blob_offset..self.blob_offset + blob_size,
            entries,
        });

        self.blob_offset += blob_size;
        self.blob_data_len = 0;
    }

    /// Get the current blob size.
    #[inline]
    fn blob_size(&self) -> usize {
        let size = self.blob_data_offset() + self.blob_data_len - self.blob_offset;
        bits::assert_aligned(ALIGN, size);
        size
    }

    pub fn finish(mut self) -> (W, Vec<BlobInfo>) {
        self.split();
        (self.writer, self.sealed_blobs)
    }

    #[inline]
    fn blob_checksum_offset(&self) -> usize {
        self.blob_offset
    }

    #[inline]
    fn blob_checksum_range(&self) -> Range<usize> {
        self.blob_checksum_offset()..self.blob_checksum_offset() + BLOB_META_CHECKSUM_BYTES
    }

    #[inline]
    fn blob_checksum_data_range(&self) -> Range<usize> {
        self.blob_offset + BLOB_META_CHECKSUM_BYTES..self.blob_offset + ALIGN
    }

    #[inline]
    fn blob_size_offset(&self) -> usize {
        self.blob_offset + BLOB_SIZE_OFFSET
    }

    #[inline]
    fn blob_size_range(&self) -> Range<usize> {
        self.blob_size_offset()..self.blob_size_offset() + BLOB_SIZE_BYTES
    }

    #[inline]
    fn blob_entry_count_offset(&self) -> usize {
        self.blob_offset + BLOB_ENTRY_COUNT_OFFSET
    }

    #[inline]
    fn blob_entry_count_range(&self) -> Range<usize> {
        self.blob_entry_count_offset()..self.blob_entry_count_offset() + BLOB_ENTRY_COUNT_BYTES
    }

    #[inline]
    fn blob_entry_index_offset(&self) -> usize {
        self.blob_offset + (BLOB_META_CHECKSUM_BYTES + BLOB_SIZE_BYTES + BLOB_ENTRY_COUNT_BYTES)
    }

    #[inline]
    fn blob_entry_index_range(&self) -> Range<usize> {
        self.blob_entry_index_offset()..self.blob_entry_index_offset() + BlobEntryIndex::serialized_len()
    }

    #[inline]
    fn blob_data_offset(&self) -> usize {
        self.blob_offset + ALIGN
    }

    #[inline]
    fn blob_next_entry_index_offset(&self) -> usize {
        self.blob_entry_index_offset() + self.blob_entries.len() * BlobEntryIndex::serialized_len()
    }
}

/// Read the meta of the blob.
#[derive(Debug, Default)]
pub struct BlobReader;

impl BlobReader {
    /// Return all kv infos in the blob, and the blob size in bytes.
    pub fn read<R>(buffer: R) -> Result<(Vec<BlobEntryIndex>, usize)>
    where
        R: AsRef<[u8]>,
    {
        let buf = &buffer.as_ref()[..ALIGN];

        // compare checksum
        let get = Checksummer::checksum32(&buf[BLOB_META_CHECKSUM_BYTES..ALIGN]);
        let expected = (&buf[..BLOB_META_CHECKSUM_BYTES]).get_u32();
        if expected != get {
            return Err(Error::ChecksumMismatch {
                expected: expected as _,
                get: get as _,
            });
        }

        let blob_size = (&buf[BLOB_SIZE_OFFSET..BLOB_SIZE_OFFSET + BLOB_SIZE_BYTES]).get_u64() as usize;
        let blob_entry_count =
            (&buf[BLOB_ENTRY_COUNT_OFFSET..BLOB_ENTRY_COUNT_OFFSET + BLOB_ENTRY_COUNT_BYTES]).get_u32() as usize;

        let indices = buf
            [BLOB_ENTRY_INDEX_OFFSET..BLOB_ENTRY_INDEX_OFFSET + blob_entry_count * BlobEntryIndex::serialized_len()]
            .chunks_exact(BlobEntryIndex::serialized_len())
            .map(BlobEntryIndex::read)
            .collect_vec();

        Ok((indices, blob_size))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        ops::{Range, RangeInclusive},
        usize,
    };

    use super::*;
    use crate::serde::EntryDeserializer;

    #[test]
    fn test_blob_serde_wo_split() {
        let assert_indices = |blob: &[u8], indices: &[BlobEntryIndex], ids: Range<u64>, metrics: &Metrics| {
            for (index, id) in indices.iter().zip(ids) {
                let buf = &blob[index.offset as usize..index.offset as usize + index.len as usize];
                let header = EntryHeader::read(buf).unwrap();
                let (k, v) = EntryDeserializer::deserialize::<u64, Vec<u8>>(
                    &buf[EntryHeader::serialized_len()..],
                    header.key_len as _,
                    header.value_len as _,
                    header.compression,
                    Some(header.checksum),
                    metrics,
                )
                .unwrap();
                assert_eq!(k, id);
                assert_eq!(v, vec![id as u8; id as usize]);
            }
        };

        let metrics = Arc::new(Metrics::noop());

        let mut buffer = vec![0; 1024 * 1024];

        let mut w = MultiBlobWriter::new(&mut buffer, WindowSpliter::new(usize::MAX, usize::MAX), metrics.clone());

        for i in 0..10u64 {
            w.push(&i, &vec![i as u8; i as usize], i, Compression::None, i as _);
        }
        w.split();

        for i in 10..20u64 {
            w.push(&i, &vec![i as u8; i as usize], i, Compression::None, i as _);
        }
        w.split();

        for i in 20..30u64 {
            w.push(&i, &vec![i as u8; i as usize], i, Compression::None, i as _);
        }
        let (_, blobs) = w.finish();

        let mut off = 0;
        let (indices, size) = BlobReader::read(&buffer[off..]).unwrap();
        assert_indices(&buffer[off..], &indices, 0..10, &metrics);

        off += size;
        let (indices, size) = BlobReader::read(&buffer[off..]).unwrap();
        assert_indices(&buffer[off..], &indices, 10..20, &metrics);

        off += size;
        let (indices, _) = BlobReader::read(&buffer[off..]).unwrap();
        assert_indices(&buffer[off..], &indices, 20..30, &metrics);

        // TODO(MrCroxx): check blob infos.
        drop(blobs);
    }

    #[test_log::test]
    fn test_blob_serde_w_split() {
        const KB: usize = 1024;

        let assert_indices = |blob: &[u8], indices: &[BlobEntryIndex], ids: RangeInclusive<u64>, metrics: &Metrics| {
            for (index, id) in indices.iter().zip(ids) {
                let buf = &blob[index.offset as usize..index.offset as usize + index.len as usize];
                let header = EntryHeader::read(buf).unwrap();
                let (k, v) = EntryDeserializer::deserialize::<u64, Vec<u8>>(
                    &buf[EntryHeader::serialized_len()..],
                    header.key_len as _,
                    header.value_len as _,
                    header.compression,
                    Some(header.checksum),
                    metrics,
                )
                .unwrap();
                assert_eq!(k, id);
                assert_eq!(v, vec![id as u8; id as usize * KB]);
            }
        };

        let metrics = Arc::new(Metrics::noop());

        let mut buffer = vec![0; 1024 * 1024];

        // window: 20K, remain: 10K
        //
        // blob 0: (4K meta) | e1 (4K)
        // blob 1: (4K meta) | e2 (4K) | e3 (4K) | e4 (8K)
        // blob 2: (4K meta) | e5 (8K) | e6 (8K)
        // blob 2: (4K meta) | e7 (8K)
        // blob 2: (4K meta) | e8 (12K)

        let mut w = MultiBlobWriter::new(&mut buffer, WindowSpliter::new(20 * KB, 10 * KB), metrics.clone());

        for i in 1..=8u64 {
            w.push(&i, &vec![i as u8; i as usize * KB], i, Compression::None, i as _);
        }
        let (_, blobs) = w.finish();

        let mut off = 0;
        let (indices, size) = BlobReader::read(&buffer[off..]).unwrap();
        assert_indices(&buffer[off..], &indices, 1..=1, &metrics);

        off += size;
        let (indices, size) = BlobReader::read(&buffer[off..]).unwrap();
        assert_indices(&buffer[off..], &indices, 2..=4, &metrics);

        off += size;
        let (indices, size) = BlobReader::read(&buffer[off..]).unwrap();
        assert_indices(&buffer[off..], &indices, 5..=6, &metrics);

        off += size;
        let (indices, size) = BlobReader::read(&buffer[off..]).unwrap();
        assert_indices(&buffer[off..], &indices, 7..=7, &metrics);

        off += size;
        let (indices, _) = BlobReader::read(&buffer[off..]).unwrap();
        assert_indices(&buffer[off..], &indices, 8..=8, &metrics);

        // TODO(MrCroxx): check blob infos.
        drop(blobs);
    }

    #[test_log::test]
    fn test_blob_serde_w_split_empty() {
        const KB: usize = 1024;

        let assert_indices = |blob: &[u8], indices: &[BlobEntryIndex], ids: RangeInclusive<u64>, metrics: &Metrics| {
            for (index, id) in indices.iter().zip(ids) {
                let buf = &blob[index.offset as usize..index.offset as usize + index.len as usize];
                let header = EntryHeader::read(buf).unwrap();
                let (k, v) = EntryDeserializer::deserialize::<u64, Vec<u8>>(
                    &buf[EntryHeader::serialized_len()..],
                    header.key_len as _,
                    header.value_len as _,
                    header.compression,
                    Some(header.checksum),
                    metrics,
                )
                .unwrap();
                assert_eq!(k, id);
                assert_eq!(v, vec![id as u8; id as usize * KB]);
            }
        };

        let metrics = Arc::new(Metrics::noop());

        let mut buffer = vec![0; 1024 * 1024];

        // window: 24K, remain: 6K
        //
        // [win 0]
        // (split empty)
        // [win 1] blob 0: (4K meta) | e1 (4K) | e2 (4K) | e3 (4K) | e4 (8K)
        // [win 2] blob 1: (4K meta) | e5 (8K)
        // (force split)
        // [win 2] blob 2: (4K meta) | e6 (8K)
        // (split exact)
        // [win 3] blob 3: (4K meta) | e7 (8K) | e8 (12K)

        let mut w = MultiBlobWriter::new(&mut buffer, WindowSpliter::new(24 * KB, 6 * KB), metrics.clone());

        for i in 1..=8u64 {
            w.push(&i, &vec![i as u8; i as usize * KB], i, Compression::None, i as _);
            if i == 5 || i == 6 {
                w.split();
            }
        }
        let (_, blobs) = w.finish();

        let mut off = 0;
        let (indices, size) = BlobReader::read(&buffer[off..]).unwrap();
        assert_indices(&buffer[off..], &indices, 1..=4, &metrics);

        off += size;
        let (indices, size) = BlobReader::read(&buffer[off..]).unwrap();
        assert_indices(&buffer[off..], &indices, 5..=5, &metrics);

        off += size;
        let (indices, size) = BlobReader::read(&buffer[off..]).unwrap();
        assert_indices(&buffer[off..], &indices, 6..=6, &metrics);

        off += size;
        let (indices, _) = BlobReader::read(&buffer[off..]).unwrap();
        assert_indices(&buffer[off..], &indices, 7..=8, &metrics);

        assert_eq!(blobs[0].window, 1);
        assert_eq!(blobs[1].window, 2);
        assert_eq!(blobs[2].window, 2);
        assert_eq!(blobs[3].window, 3);

        // TODO(MrCroxx): check blob infos.
        drop(blobs);
    }
}
