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

use std::{sync::Arc, time::Instant};

use bytes::{Buf, BufMut};
use foyer_common::{
    bits,
    code::{CodeError, StorageKey, StorageValue},
    metrics::Metrics,
};

use crate::{
    compress::Compression,
    error::Error,
    io::{buffer::IoBuffer, PAGE},
    large::serde::{EntryHeader, Sequence},
    serde::{Checksummer, EntrySerializer},
    SharedIoSlice,
};

/// [`EntryIndex`] index entry in a blob, which can be used to speed up recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobEntryIndex {
    /// Entry hash.
    pub hash: u64,
    /// Entry sequence.
    pub sequence: Sequence,
    /// Offset to the blob.
    pub offset: u32,
    /// Page Length of the entry.
    pub len: u32,
}

impl BlobEntryIndex {
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

    pub fn aligned(&self) -> usize {
        bits::align_up(PAGE, self.len as _)
    }
}

/// # Format
///
/// ```plain
/// | checksum (8B) | count (4B) | index 0 | index 1 | ... | index N |
/// ```
#[derive(Debug)]
pub struct BlobIndex {
    io_buffer: IoBuffer,
    count: usize,
}

impl BlobIndex {
    pub const CHECKSUM_BYTES: usize = 8;

    pub const COUNT_OFFSET: usize = 8;
    pub const COUNT_BYTES: usize = 4;

    pub const INDEX_OFFSET: usize = 12;

    pub fn new(io_buffer: IoBuffer) -> Self {
        Self { io_buffer, count: 0 }
    }

    pub fn capacity(&self) -> usize {
        (self.io_buffer.len() - BlobIndex::INDEX_OFFSET) / BlobEntryIndex::serialized_len()
    }

    pub fn is_full(&self) -> bool {
        self.count >= self.capacity()
    }

    /// # Panics
    ///
    /// Panics if the buffer is already full.
    pub fn write(&mut self, index: &BlobEntryIndex) {
        assert!(!self.is_full());
        let start = BlobIndex::INDEX_OFFSET + self.count * BlobEntryIndex::serialized_len();
        let end = start + BlobEntryIndex::serialized_len();
        index.write(&mut self.io_buffer[start..end]);
        self.count += 1;
    }

    pub fn seal(&mut self) -> IoBuffer {
        (&mut self.io_buffer[BlobIndex::COUNT_OFFSET..BlobIndex::COUNT_OFFSET + BlobIndex::COUNT_BYTES])
            .put_u32(self.count as _);
        let checksum = Checksummer::checksum64(&self.io_buffer[BlobIndex::CHECKSUM_BYTES..]);
        (&mut self.io_buffer[0..BlobIndex::CHECKSUM_BYTES]).put_u64(checksum);
        self.io_buffer.clone()
    }

    pub fn reset(&mut self) {
        self.count = 0;
    }
}

#[derive(Debug)]
pub struct BlobIndexReader;

impl BlobIndexReader {
    pub fn read(buf: &[u8]) -> Option<Vec<BlobEntryIndex>> {
        let checksum = Checksummer::checksum64(&buf[BlobIndex::CHECKSUM_BYTES..]);
        let expected = (&buf[0..BlobIndex::CHECKSUM_BYTES]).get_u64();
        if checksum != expected {
            tracing::trace!(checksum, expected, "[blob index reader]: checksum mismatch");
            return None;
        }
        let count =
            (&buf[BlobIndex::COUNT_OFFSET..BlobIndex::COUNT_OFFSET + BlobIndex::COUNT_BYTES]).get_u32() as usize;
        let indices = buf[BlobIndex::INDEX_OFFSET..BlobIndex::INDEX_OFFSET + count * BlobEntryIndex::serialized_len()]
            .chunks_exact(BlobEntryIndex::serialized_len())
            .map(BlobEntryIndex::read)
            .collect();
        Some(indices)
    }
}

#[derive(Debug, Clone)]
pub struct BufferEntryInfo {
    pub hash: u64,
    pub sequence: Sequence,
    pub offset: usize,
    pub len: usize,
}

impl BufferEntryInfo {
    pub fn aligned(&self) -> usize {
        bits::align_up(PAGE, self.len)
    }
}

#[derive(Debug)]
pub struct Buffer {
    io_buffer: IoBuffer,
    written: usize,
    entry_infos: Vec<BufferEntryInfo>,

    max_entry_size: usize,

    metrics: Arc<Metrics>,
}

impl Buffer {
    pub fn new(io_buffer: IoBuffer, max_entry_size: usize, metrics: Arc<Metrics>) -> Self {
        Self {
            io_buffer,
            written: 0,
            entry_infos: vec![],
            max_entry_size,
            metrics,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.entry_infos.is_empty()
    }

    pub fn push<K, V>(&mut self, key: &K, value: &V, hash: u64, compression: Compression, sequence: Sequence) -> bool
    where
        K: StorageKey,
        V: StorageValue,
    {
        tracing::trace!(hash, "[blob writer]: push");

        let offset = self.written;
        let buf = &mut self.io_buffer[offset..];

        // If there is no space even for entry header, skip
        if buf.len() < EntryHeader::serialized_len() {
            return false;
        }

        let ser = Instant::now();

        let info = match EntrySerializer::serialize(key, value, compression, &mut buf[EntryHeader::serialized_len()..])
        {
            Ok(info) => info,
            Err(Error::Code(CodeError::SizeLimit)) => return false,
            Err(e) => {
                tracing::warn!(?e, "[blob writer]: serialize entry kv error");
                return false;
            }
        };
        let checksum = Checksummer::checksum64(
            &buf[EntryHeader::serialized_len()
                ..EntryHeader::serialized_len() + info.key_len as usize + info.value_len as usize],
        );
        let header = EntryHeader {
            key_len: info.key_len as _,
            value_len: info.value_len as _,
            hash,
            sequence,
            checksum,
            compression,
        };
        header.write(&mut buf[..EntryHeader::serialized_len()]);

        self.metrics
            .storage_entry_serialize_duration
            .record(ser.elapsed().as_secs_f64());

        let len = EntryHeader::serialized_len() + info.key_len as usize + info.value_len as usize;
        let aligned = bits::align_up(PAGE, len);

        if aligned > self.max_entry_size {
            return false;
        }

        let info = BufferEntryInfo {
            hash,
            sequence,
            offset,
            len,
        };
        self.entry_infos.push(info);
        self.written += aligned;

        tracing::trace!(hash, "[blob writer]: push finish");

        true
    }

    /// Serialize an serialized kv entry slice into the dest.
    pub fn push_slice(&mut self, slice: &[u8], hash: u64, sequence: Sequence) -> bool {
        tracing::trace!(hash, "[blob writer]: push slice");

        let offset = self.written;
        let buf = &mut self.io_buffer[offset..];

        let len = slice.len();
        let aligned = bits::align_up(PAGE, slice.len());

        if aligned > self.max_entry_size || aligned > buf.len() {
            return false;
        }

        buf[..slice.len()].copy_from_slice(slice);

        let info = BufferEntryInfo {
            hash,
            sequence,
            offset,
            len,
        };
        self.entry_infos.push(info);
        self.written += aligned;

        tracing::trace!(hash, "[blob writer]: push slice finish");

        true
    }

    pub fn finish(self) -> (IoBuffer, Vec<BufferEntryInfo>) {
        tracing::trace!(infos=?self.entry_infos, "[buffer]: finish");
        (self.io_buffer, self.entry_infos)
    }
}

#[derive(Debug)]
pub struct SplitCtx {
    current_part_blob_offset: usize,
    current_blob_index: BlobIndex,

    current_blob_region_offset: usize,

    region_size: usize,
    blob_index_size: usize,
}

impl SplitCtx {
    pub fn new(region_size: usize, blob_index_size: usize) -> Self {
        Self {
            current_part_blob_offset: blob_index_size,
            current_blob_index: BlobIndex::new(IoBuffer::new(blob_index_size)),
            current_blob_region_offset: 0,
            region_size,
            blob_index_size,
        }
    }
}

#[derive(Debug)]
pub struct Splitter;

impl Splitter {
    pub fn split(ctx: &mut SplitCtx, mut shared_io_slice: SharedIoSlice, entry_infos: Vec<BufferEntryInfo>) -> Batch {
        let mut batch = Batch {
            regions: vec![Region { blob_parts: vec![] }],
            io_slice: shared_io_slice.clone(),
        };

        assert!(
            !ctx.current_blob_index.is_full(),
            "[splitter] Blob index writer cannot be full at this point, it is supposed to be flushed in the last batch."
        );

        let mut part_size = 0;
        let mut indices = vec![];

        for info in entry_infos.into_iter() {
            tracing::trace!(?info, "[splitter]: handle entry");

            'handle: loop {
                // Split blob if blob index is full.
                if ctx.current_blob_index.is_full() {
                    if let Some(part) = Self::split_blob(ctx, &mut indices, &mut part_size, &mut shared_io_slice) {
                        batch.regions.last_mut().unwrap().blob_parts.push(part);
                    }
                    continue 'handle;
                }

                // Split blob and region if region is full.
                if ctx.current_blob_region_offset + ctx.current_part_blob_offset + part_size + info.aligned()
                    > ctx.region_size
                {
                    if let Some(part) = Self::split_blob(ctx, &mut indices, &mut part_size, &mut shared_io_slice) {
                        batch.regions.last_mut().unwrap().blob_parts.push(part);
                    }
                    Self::split_region(ctx, &mut batch);
                    continue 'handle;
                }

                let index = BlobEntryIndex {
                    hash: info.hash,
                    sequence: info.sequence,
                    offset: ctx.current_part_blob_offset as u32 + part_size as u32,
                    len: info.len as u32,
                };

                tracing::trace!(?index, ?info, "[splitter]: append entry");

                ctx.current_blob_index.write(&index);
                indices.push(index);
                part_size += info.aligned();

                break 'handle;
            }
        }

        if let Some(part) = Self::seal_blob(ctx, &mut indices, &mut part_size, &mut shared_io_slice) {
            batch.regions.last_mut().unwrap().blob_parts.push(part);
        }

        batch
    }

    fn split_blob(
        ctx: &mut SplitCtx,
        indices: &mut Vec<BlobEntryIndex>,
        part_size: &mut usize,
        shared_io_slice: &mut SharedIoSlice,
    ) -> Option<BlobPart> {
        tracing::trace!("[splitter]: split blob");
        if indices.is_empty() {
            // The blob part is empty, only need to set the state.
            assert_eq!(*part_size, 0);
            ctx.current_blob_index.reset();
            ctx.current_blob_region_offset += ctx.current_part_blob_offset;
            ctx.current_part_blob_offset = ctx.blob_index_size;

            None
        } else {
            // Seal and clear the blob index to prepare for the next blob.
            let index = ctx.current_blob_index.seal();
            ctx.current_blob_index.reset();

            let indices = std::mem::take(indices);

            let part = BlobPart {
                blob_region_offset: ctx.current_blob_region_offset,
                index,
                part_blob_offset: ctx.current_part_blob_offset,
                data: shared_io_slice.slice(..*part_size),
                indices,
            };

            ctx.current_blob_region_offset = ctx.current_blob_region_offset + ctx.current_part_blob_offset + *part_size;
            ctx.current_part_blob_offset = ctx.blob_index_size;
            *shared_io_slice = shared_io_slice.slice(*part_size..);
            *part_size = 0;

            Some(part)
        }
    }

    fn split_region(ctx: &mut SplitCtx, batch: &mut Batch) {
        tracing::trace!("[splitter]; split region");
        batch.regions.push(Region { blob_parts: vec![] });
        ctx.current_blob_region_offset = 0;
    }

    fn seal_blob(
        ctx: &mut SplitCtx,
        indices: &mut Vec<BlobEntryIndex>,
        part_size: &mut usize,
        shared_io_slice: &mut SharedIoSlice,
    ) -> Option<BlobPart> {
        tracing::trace!("[splitter]: seal blob");

        if indices.is_empty() {
            return None;
        }

        // Seal the last blob.
        let index = ctx.current_blob_index.seal();

        let indices = std::mem::take(indices);

        let part = BlobPart {
            blob_region_offset: ctx.current_blob_region_offset,
            index,
            part_blob_offset: ctx.current_part_blob_offset,
            data: shared_io_slice.slice(..*part_size),
            indices,
        };

        if ctx.current_blob_index.is_full() {
            tracing::trace!("[splitter]: seal blob, split blob because index is full");
            ctx.current_blob_index.reset();
            ctx.current_blob_region_offset = ctx.current_blob_region_offset + ctx.current_part_blob_offset + *part_size;
            ctx.current_part_blob_offset = ctx.blob_index_size;
        } else {
            ctx.current_part_blob_offset += *part_size;
        }

        Some(part)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct BlobPart {
    /// Blob offset to the region.
    pub blob_region_offset: usize,
    pub index: IoBuffer,
    /// Blob part offset to the blob.
    pub part_blob_offset: usize,
    pub data: SharedIoSlice,

    pub indices: Vec<BlobEntryIndex>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Region {
    pub blob_parts: Vec<BlobPart>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Batch {
    pub regions: Vec<Region>,
    pub io_slice: SharedIoSlice,
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::*;

    const KB: usize = 1024;

    #[test_log::test]
    fn test_blob_index_serde() {
        let mut bi = BlobIndex::new(IoBuffer::new(PAGE * 2));
        let indices = (0..bi.capacity() / 2)
            .map(|i| BlobEntryIndex {
                hash: i as u64,
                sequence: i as u64,
                offset: i as u32,
                len: i as u32,
            })
            .collect_vec();

        indices.iter().take(bi.capacity() / 2).for_each(|index| bi.write(index));
        let buf = bi.seal();
        let ins = BlobIndexReader::read(&buf).unwrap();
        assert_eq!(indices.iter().take(bi.capacity() / 2).cloned().collect_vec(), ins);

        indices.iter().skip(bi.capacity() / 2).for_each(|index| bi.write(index));
        let buf = bi.seal();
        let ins = BlobIndexReader::read(&buf).unwrap();
        assert_eq!(indices, ins);
    }

    #[test_log::test]
    fn test_buffer() {
        const REGION_SIZE: usize = 16 * KB;
        const BLOB_INDEX_SIZE: usize = 4 * KB;
        const MAX_ENTRY_SIZE: usize = REGION_SIZE - BLOB_INDEX_SIZE;
        const BATCH_SIZE: usize = 64 * KB;

        let mut ctx = SplitCtx::new(REGION_SIZE, BLOB_INDEX_SIZE);

        // 1. Test write single blob part.

        let mut buffer = Buffer::new(IoBuffer::new(BATCH_SIZE), MAX_ENTRY_SIZE, Arc::new(Metrics::noop()));

        // 4K
        assert!(buffer.push(&1u64, &vec![1u8; 3 * KB], 1, Compression::None, 1));

        // 16K (deny)
        assert!(!buffer.push(&2u64, &vec![2u8; 13 * KB], 2, Compression::None, 2));

        // 4K
        assert!(buffer.push(&3u64, &vec![3u8; 3 * KB], 3, Compression::None, 3));

        let (buf, infos) = buffer.finish();
        let buf = buf.into_shared_io_slice();
        let batch = Splitter::split(&mut ctx, buf.clone(), infos.clone());

        assert_eq!(
            batch,
            Batch {
                io_slice: buf.clone(),
                regions: vec![Region {
                    blob_parts: vec![BlobPart {
                        blob_region_offset: 0,
                        index: batch.regions[0].blob_parts[0].index.clone(),
                        part_blob_offset: BLOB_INDEX_SIZE,
                        data: buf.slice(0..8 * KB),
                        indices: vec![
                            BlobEntryIndex {
                                hash: 1,
                                sequence: 1,
                                offset: BLOB_INDEX_SIZE as u32,
                                len: infos[0].len as u32,
                            },
                            BlobEntryIndex {
                                hash: 3,
                                sequence: 3,
                                offset: BLOB_INDEX_SIZE as u32 + 4 * KB as u32,
                                len: infos[1].len as u32,
                            }
                        ]
                    }]
                }]
            }
        );

        // 2. Test continue to write blob part and region split.

        let mut buffer = Buffer::new(IoBuffer::new(BATCH_SIZE), MAX_ENTRY_SIZE, Arc::new(Metrics::noop()));

        // 4K, region split
        assert!(buffer.push(&4u64, &vec![4u8; 3 * KB], 4, Compression::None, 4));

        // 8K
        assert!(buffer.push(&5u64, &vec![5u8; 7 * KB], 5, Compression::None, 5));

        // 8K, region early split
        assert!(buffer.push(&6u64, &vec![6u8; 7 * KB], 6, Compression::None, 6));

        let (buf, infos) = buffer.finish();
        let buf = buf.into_shared_io_slice();
        let batch = Splitter::split(&mut ctx, buf.clone(), infos.clone());

        assert_eq!(
            batch,
            Batch {
                io_slice: buf.clone(),
                regions: vec![
                    Region {
                        blob_parts: vec![BlobPart {
                            blob_region_offset: 0,
                            index: batch.regions[0].blob_parts[0].index.clone(),
                            part_blob_offset: BLOB_INDEX_SIZE + 8 * KB,
                            data: buf.slice(0..4 * KB),
                            indices: vec![BlobEntryIndex {
                                hash: 4,
                                sequence: 4,
                                offset: BLOB_INDEX_SIZE as u32 + 8 * KB as u32,
                                len: infos[0].len as u32,
                            }]
                        }]
                    },
                    Region {
                        blob_parts: vec![BlobPart {
                            blob_region_offset: 0,
                            index: batch.regions[1].blob_parts[0].index.clone(),
                            part_blob_offset: BLOB_INDEX_SIZE,
                            data: buf.slice(4 * KB..12 * KB),
                            indices: vec![BlobEntryIndex {
                                hash: 5,
                                sequence: 5,
                                offset: BLOB_INDEX_SIZE as u32,
                                len: infos[1].len as u32,
                            }]
                        }]
                    },
                    Region {
                        blob_parts: vec![BlobPart {
                            blob_region_offset: 0,
                            index: batch.regions[2].blob_parts[0].index.clone(),
                            part_blob_offset: BLOB_INDEX_SIZE,
                            data: buf.slice(12 * KB..20 * KB),
                            indices: vec![BlobEntryIndex {
                                hash: 6,
                                sequence: 6,
                                offset: BLOB_INDEX_SIZE as u32,
                                len: infos[2].len as u32,
                            }]
                        }]
                    }
                ]
            }
        );

        // 3. Test leave first region empty.

        let mut buffer = Buffer::new(IoBuffer::new(BATCH_SIZE), MAX_ENTRY_SIZE, Arc::new(Metrics::noop()));

        // 8K, region split
        assert!(buffer.push(&7u64, &vec![7u8; 7 * KB], 7, Compression::None, 7));

        let (buf, infos) = buffer.finish();
        let buf = buf.into_shared_io_slice();
        let batch = Splitter::split(&mut ctx, buf.clone(), infos.clone());

        assert_eq!(
            batch,
            Batch {
                io_slice: buf.clone(),
                regions: vec![
                    Region { blob_parts: vec![] },
                    Region {
                        blob_parts: vec![BlobPart {
                            blob_region_offset: 0,
                            index: batch.regions[1].blob_parts[0].index.clone(),
                            part_blob_offset: BLOB_INDEX_SIZE,
                            data: buf.slice(0..8 * KB),
                            indices: vec![BlobEntryIndex {
                                hash: 7,
                                sequence: 7,
                                offset: BLOB_INDEX_SIZE as u32,
                                len: infos[0].len as u32,
                            }]
                        }]
                    },
                ]
            }
        );
    }

    #[test_log::test]
    fn test_split_region_last_entry() {
        const KB: usize = 1 << 10;

        const REGION_SIZE: usize = 64 * KB;
        const BLOB_INDEX_SIZE: usize = 4 * KB;
        const BATCH_SIZE: usize = 128 * KB;

        // Remain 4 KB in size and 1 entry in count.
        let mut ctx = SplitCtx::new(REGION_SIZE, BLOB_INDEX_SIZE);
        ctx.current_blob_region_offset = 40 * KB;
        ctx.current_part_blob_offset = 16 * KB;
        ctx.current_blob_index.count = ctx.current_blob_index.capacity() - 1;

        let shared_io_slice = IoBuffer::new(BATCH_SIZE).into_shared_io_slice();
        let batch = Splitter::split(
            &mut ctx,
            shared_io_slice.clone(),
            vec![
                BufferEntryInfo {
                    hash: 0,
                    sequence: 0,
                    offset: 0,
                    len: 1234,
                },
                BufferEntryInfo {
                    hash: 1,
                    sequence: 1,
                    offset: 4 * KB,
                    len: 1234,
                },
            ],
        );

        println!("{batch:?}");
        assert_eq!(
            batch,
            Batch {
                regions: vec![
                    Region {
                        blob_parts: vec![BlobPart {
                            blob_region_offset: 40 * KB,
                            index: batch.regions[0].blob_parts[0].index.clone(),
                            part_blob_offset: 16 * KB,
                            data: shared_io_slice.slice(0..4 * KB),
                            indices: vec![BlobEntryIndex {
                                hash: 0,
                                sequence: 0,
                                offset: 16 * KB as u32,
                                len: 1234,
                            }]
                        }]
                    },
                    Region {
                        blob_parts: vec![BlobPart {
                            blob_region_offset: 0,
                            index: batch.regions[1].blob_parts[0].index.clone(),
                            part_blob_offset: 4 * KB,
                            data: shared_io_slice.slice(4 * KB..8 * KB),
                            indices: vec![BlobEntryIndex {
                                hash: 1,
                                sequence: 1,
                                offset: 4 * KB as u32,
                                len: 1234,
                            }]
                        }]
                    }
                ],
                io_slice: shared_io_slice,
            }
        );
    }
}
