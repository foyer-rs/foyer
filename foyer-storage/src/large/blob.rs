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

use std::{ops::Range, sync::Arc};

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
    pub offset: u32,
    pub len: u32,
    pub sequence: Sequence,
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

#[derive(Debug)]
struct BlobWriter<W> {
    buffer: W,

    blob_offset: usize,

    blob_data_len: usize,
    blob_entry_count: u32,

    // FIXME: Remove metrics here.
    metrics: Arc<Metrics>,
}

impl<W> BlobWriter<W>
where
    W: AsMut<[u8]>,
{
    pub fn new(buffer: W, metrics: Arc<Metrics>) -> Self {
        Self {
            buffer,
            blob_offset: 0,
            blob_data_len: 0,
            blob_entry_count: 0,
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
        // Entry count overflow, split and retry.
        if self.blob_entry_count as usize >= BLOB_ENTRY_CAPACITY {
            self.split();
            return self.push(key, value, hash, compression, sequence);
        }

        let entry_offset = self.blob_data_offset() + self.blob_data_len;

        // If there is no space even for entry header, skip
        if entry_offset + EntryHeader::serialized_len() >= self.buffer.as_mut().len() {
            return false;
        }

        // Serialize entry error (serde error or buffer overflow), log it and skip.
        let info = match EntrySerializer::serialize(
            key,
            value,
            compression,
            &mut self.buffer.as_mut()[entry_offset + EntryHeader::serialized_len()..],
            &self.metrics,
        ) {
            Ok(info) => info,
            Err(e) => {
                tracing::warn!("[lodc batch]: serialize entry error: {e}");
                return false;
            }
        };

        let checksum = Checksummer::checksum64(
            &self.buffer.as_mut()[entry_offset + EntryHeader::serialized_len()
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
        header.write(&mut self.buffer.as_mut()[entry_offset..entry_offset + EntryHeader::serialized_len()]);

        let entry_len = EntryHeader::serialized_len() + info.key_len + info.value_len;
        let blob_entry_offset = entry_offset - self.blob_offset;

        let index = BlobEntryIndex {
            offset: blob_entry_offset as _,
            len: entry_len as _,
            sequence,
        };
        let index_offset = self.blob_next_entry_index_offset();
        index.write(&mut self.buffer.as_mut()[index_offset..index_offset + BlobEntryIndex::serialized_len()]);

        let aligned = bits::align_up(
            ALIGN,
            EntryHeader::serialized_len() + info.key_len as usize + info.value_len as usize,
        );
        self.blob_data_len += aligned;
        self.blob_entry_count += 1;

        true
    }

    /// Seal the current blob and perpare a new one.
    ///
    /// The perparing doesn't write any data. It is okay to be called as the last blob.
    pub fn split(&mut self) {
        if self.blob_entry_count == 0 {
            return;
        }

        let blob_size = self.blob_data_offset() + self.blob_data_len - self.blob_offset;
        let range = self.blob_size_range();
        (&mut self.buffer.as_mut()[range]).put_u64(blob_size as _);

        let range = self.blob_entry_count_range();
        (&mut self.buffer.as_mut()[range]).put_u32(self.blob_entry_count);

        let range = self.blob_checksum_data_range();
        let checksum = Checksummer::checksum32(&self.buffer.as_mut()[range]);
        let range = self.blob_checksum_range();
        (&mut self.buffer.as_mut()[range]).put_u32(checksum);

        self.blob_offset += blob_size;
        self.blob_entry_count = 0;
        self.blob_data_len = 0;
    }

    // pub fn rotate(&mut self) {
    //     self.split();

    //     todo!()
    // }

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
        self.blob_entry_index_offset() + self.blob_entry_count as usize * BlobEntryIndex::serialized_len()
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
    use std::ops::Range;

    use super::*;
    use crate::serde::EntryDeserializer;

    fn assert_indices(blob: &[u8], indices: &[BlobEntryIndex], ids: Range<u64>, metrics: &Metrics) {
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
    }

    #[test]
    fn test_blob_serde() {
        let metrics = Arc::new(Metrics::noop());

        let mut buffer = vec![0; 1024 * 1024];

        let mut w = BlobWriter::new(&mut buffer, metrics.clone());

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
        w.split();

        let mut off = 0;
        let (indices, size) = BlobReader::read(&buffer[off..]).unwrap();
        assert_indices(&buffer[off..], &indices, 0..10, &metrics);

        off += size;
        let (indices, size) = BlobReader::read(&buffer[off..]).unwrap();
        assert_indices(&buffer[off..], &indices, 10..20, &metrics);

        off += size;
        let (indices, _) = BlobReader::read(&buffer[off..]).unwrap();
        assert_indices(&buffer[off..], &indices, 20..30, &metrics);
    }
}
