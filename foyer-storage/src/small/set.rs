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
    collections::HashSet,
    fmt::Debug,
    ops::Range,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::{Buf, BufMut};
use foyer_common::{
    code::{StorageKey, StorageValue},
    metrics::Metrics,
};

use super::{batch::Item, bloom_filter::BloomFilterU64, serde::EntryHeader};
use crate::{
    error::Result,
    io::IoBuffer,
    serde::{Checksummer, EntryDeserializer},
    Compression,
};

pub type SetId = u64;

/// # Format
///
/// ```plain
/// | checksum (4B) | ns timestamp (16B) | len (4B) |
/// | bloom filter (4 * 8B = 32B) |
/// ```
pub struct SetStorage {
    /// Set checksum.
    checksum: u32,

    /// Set written data length.
    len: usize,
    /// Set data length capacity.
    capacity: usize,
    /// Set size.
    size: usize,
    /// Set last updated timestamp.
    timestamp: u128,
    /// Set bloom filter.
    bloom_filter: BloomFilterU64<4>,

    buffer: IoBuffer,

    metrics: Arc<Metrics>,
}

impl Debug for SetStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SetStorage")
            .field("checksum", &self.checksum)
            .field("len", &self.len)
            .field("capacity", &self.capacity)
            .field("size", &self.size)
            .field("timestamp", &self.timestamp)
            .field("bloom_filter", &self.bloom_filter)
            .finish()
    }
}

impl SetStorage {
    pub const SET_HEADER_SIZE: usize = 56;

    /// Load the set storage from buffer.
    ///
    /// If `after` is set and the set storage is before the timestamp, load an empty set storage.
    pub fn load(buffer: IoBuffer, watermark: u128, metrics: Arc<Metrics>) -> Self {
        assert!(buffer.len() >= Self::SET_HEADER_SIZE);

        let checksum = (&buffer[0..4]).get_u32();
        let timestamp = (&buffer[4..20]).get_u128();
        let len = (&buffer[20..24]).get_u32() as usize;
        let bloom_filter = BloomFilterU64::read(&buffer[24..56]);

        let mut this = Self {
            checksum,
            len,
            capacity: buffer.len() - Self::SET_HEADER_SIZE,
            size: buffer.len(),
            timestamp,
            bloom_filter,
            buffer,
            metrics,
        };

        this.verify(watermark);

        this
    }

    fn verify(&mut self, watermark: u128) {
        if Self::SET_HEADER_SIZE + self.len >= self.buffer.len() || self.timestamp < watermark {
            // invalid len
            self.clear();
        } else {
            let c = Checksummer::checksum32(&self.buffer[4..Self::SET_HEADER_SIZE + self.len]);
            if c != self.checksum {
                // checksum mismatch
                self.clear();
            }
        }
    }

    pub fn update(&mut self) {
        self.bloom_filter.write(&mut self.buffer[24..56]);
        (&mut self.buffer[20..24]).put_u32(self.len as _);
        self.timestamp = SetTimestamp::current();
        (&mut self.buffer[4..20]).put_u128(self.timestamp);
        self.checksum = Checksummer::checksum32(&self.buffer[4..Self::SET_HEADER_SIZE + self.len]);
        (&mut self.buffer[0..4]).put_u32(self.checksum);
    }

    pub fn bloom_filter(&self) -> &BloomFilterU64<4> {
        &self.bloom_filter
    }

    #[cfg_attr(not(test), expect(dead_code))]
    pub fn len(&self) -> usize {
        self.len
    }

    #[cfg_attr(not(test), expect(dead_code))]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn clear(&mut self) {
        self.len = 0;
        self.bloom_filter.clear();
    }

    pub fn into_io_buffer(self) -> IoBuffer {
        self.buffer
    }

    pub fn apply(&mut self, deletions: &HashSet<u64>, items: Vec<Item>) {
        self.deletes(deletions);
        self.append(items);
    }

    fn deletes(&mut self, deletes: &HashSet<u64>) {
        if deletes.is_empty() {
            return;
        }

        let mut rcursor = 0;
        let mut wcursor = 0;
        // Rebuild bloom filter.
        self.bloom_filter.clear();

        while rcursor < self.len {
            let header = EntryHeader::read(
                &self.buffer
                    [Self::SET_HEADER_SIZE + rcursor..Self::SET_HEADER_SIZE + rcursor + EntryHeader::ENTRY_HEADER_SIZE],
            );

            if !deletes.contains(&header.hash()) {
                if rcursor != wcursor {
                    self.buffer.copy_within(
                        Self::SET_HEADER_SIZE + rcursor..Self::SET_HEADER_SIZE + header.entry_len(),
                        wcursor,
                    );
                }
                wcursor += header.entry_len();
                self.bloom_filter.insert(header.hash());
            }

            rcursor += header.entry_len();
        }

        self.len = wcursor;
    }

    fn append(&mut self, items: Vec<Item>) {
        let (skip, size, _) = items
            .iter()
            .rev()
            .fold((items.len(), 0, true), |(skip, size, proceed), item| {
                let proceed = proceed && size + item.slice.len() <= self.size - Self::SET_HEADER_SIZE;
                if proceed {
                    (skip - 1, size + item.slice.len(), proceed)
                } else {
                    (skip, size, proceed)
                }
            });

        self.reserve(size);
        let mut cursor = Self::SET_HEADER_SIZE + self.len;
        for item in items.iter().skip(skip) {
            self.buffer[cursor..cursor + item.slice.len()].copy_from_slice(&item.slice);
            self.bloom_filter.insert(item.hash);
            cursor += item.slice.len();
        }
        self.len = cursor - Self::SET_HEADER_SIZE;
    }

    pub fn get<K, V>(&self, hash: u64) -> Result<Option<(K, V)>>
    where
        K: StorageKey,
        V: StorageValue,
    {
        if !self.bloom_filter.lookup(hash) {
            return Ok(None);
        }
        for entry in self.iter() {
            if hash == entry.hash {
                let (k, v) = EntryDeserializer::deserialize(
                    &entry.buf[EntryHeader::ENTRY_HEADER_SIZE..],
                    entry.key_len,
                    entry.value_len,
                    Compression::None,
                    None,
                    &self.metrics,
                )?;
                return Ok(Some((k, v)));
            }
        }
        Ok(None)
    }

    /// from:
    ///
    /// ```plain
    /// 0        wipe          len       capacity
    /// |_________|ooooooooooooo|___________|
    /// ```
    ///
    /// to:
    ///
    /// ```plain
    /// 0     new len = len - wipe       capacity
    /// |ooooooooooooo|_____________________|
    /// ```
    fn reserve(&mut self, required: usize) {
        let remains = self.capacity - self.len;
        if remains >= required {
            return;
        }

        let mut wipe = 0;
        for entry in self.iter() {
            wipe += entry.len();
            if remains + wipe >= required {
                break;
            }
        }
        self.buffer.copy_within(
            Self::SET_HEADER_SIZE + wipe..Self::SET_HEADER_SIZE + self.len,
            Self::SET_HEADER_SIZE,
        );
        self.len -= wipe;
        assert!(self.capacity - self.len >= required);
        let mut bloom_filter = BloomFilterU64::default();
        for entry in self.iter() {
            bloom_filter.insert(entry.hash);
        }
        self.bloom_filter = bloom_filter;
    }

    fn iter(&self) -> SetIter<'_> {
        SetIter::open(self)
    }

    fn data(&self) -> &[u8] {
        &self.buffer[Self::SET_HEADER_SIZE..self.size]
    }
}

pub struct SetEntry<'a> {
    offset: usize,
    hash: u64,
    buf: &'a [u8],
    key_len: usize,
    value_len: usize,
}

impl SetEntry<'_> {
    /// Length of the entry with header, key and value included.
    pub fn len(&self) -> usize {
        debug_assert_eq!(
            self.buf.len(),
            EntryHeader::ENTRY_HEADER_SIZE + self.key_len + self.value_len
        );
        self.buf.len()
    }

    /// Range of the entry in the set data.
    #[expect(unused)]
    pub fn range(&self) -> Range<usize> {
        self.offset..self.offset + self.len()
    }
}

pub struct SetIter<'a> {
    set: &'a SetStorage,
    offset: usize,
}

impl<'a> SetIter<'a> {
    fn open(set: &'a SetStorage) -> Self {
        Self { set, offset: 0 }
    }

    fn is_valid(&self) -> bool {
        self.offset < self.set.len
    }

    fn next(&mut self) -> Option<SetEntry<'a>> {
        if !self.is_valid() {
            return None;
        }
        let header = EntryHeader::read(&self.set.data()[self.offset..self.offset + EntryHeader::ENTRY_HEADER_SIZE]);
        let entry = SetEntry {
            offset: self.offset,
            hash: header.hash(),
            buf: &self.set.data()
                [self.offset..self.offset + EntryHeader::ENTRY_HEADER_SIZE + header.key_len() + header.value_len()],
            key_len: header.key_len(),
            value_len: header.value_len(),
        };
        self.offset += entry.len();
        Some(entry)
    }
}

impl<'a> Iterator for SetIter<'a> {
    type Item = SetEntry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next()
    }
}

pub struct SetTimestamp;

impl SetTimestamp {
    pub fn current() -> u128 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
    }
}

#[cfg(test)]
mod tests {

    use bytes::Bytes;
    use foyer_common::metrics::Metrics;
    use foyer_memory::{Cache, CacheBuilder, CacheEntry};

    use super::*;
    use crate::{io::PAGE, serde::EntrySerializer, Compression};

    fn to_bytes(entry: &CacheEntry<u64, Vec<u8>>) -> Bytes {
        let mut buf = vec![];

        // reserve header
        let header = EntryHeader::new(0, 0, 0);
        header.write(&mut buf);

        let info = EntrySerializer::serialize(
            entry.key(),
            entry.value(),
            Compression::None,
            &mut buf,
            &Metrics::noop(),
        )
        .unwrap();

        let header = EntryHeader::new(entry.hash(), info.key_len, info.value_len);
        header.write(&mut buf[0..EntryHeader::ENTRY_HEADER_SIZE]);

        Bytes::from(buf)
    }

    fn assert_some(storage: &SetStorage, entry: &CacheEntry<u64, Vec<u8>>) {
        let ret = storage.get::<u64, Vec<u8>>(entry.hash()).unwrap();
        let (k, v) = ret.unwrap();
        assert_eq!(&k, entry.key());
        assert_eq!(&v, entry.value());
    }

    fn assert_none(storage: &SetStorage, hash: u64) {
        let ret = storage.get::<u64, Vec<u8>>(hash).unwrap();
        assert!(ret.is_none());
    }

    fn memory_for_test() -> Cache<u64, Vec<u8>> {
        CacheBuilder::new(100).build()
    }

    #[test]
    fn test_set_storage_basic() {
        let memory = memory_for_test();

        // load will result in an empty set
        let buf = IoBuffer::new(PAGE);
        let mut storage = SetStorage::load(buf, 0, Arc::new(Metrics::noop()));
        assert!(storage.is_empty());

        let e1 = memory.insert(1, vec![b'1'; 42]);
        let s1 = to_bytes(&e1);
        storage.apply(
            &HashSet::from_iter([2, 4]),
            vec![Item {
                slice: s1.clone(),
                hash: e1.hash(),
            }],
        );
        assert_eq!(storage.len(), s1.len());
        assert_some(&storage, &e1);

        let e2 = memory.insert(2, vec![b'2'; 97]);
        let s2 = to_bytes(&e2);
        storage.apply(
            &HashSet::from_iter([e1.hash(), 3, 5]),
            vec![Item {
                slice: s2.clone(),
                hash: e2.hash(),
            }],
        );
        assert_eq!(storage.len(), s2.len());
        assert_none(&storage, e1.hash());
        assert_some(&storage, &e2);

        let e3 = memory.insert(3, vec![b'3'; 211]);
        let s3 = to_bytes(&e3);
        storage.apply(
            &HashSet::from_iter([e1.hash()]),
            vec![Item {
                slice: s3.clone(),
                hash: e3.hash(),
            }],
        );
        assert_eq!(storage.len(), s2.len() + s3.len());
        assert_none(&storage, e1.hash());
        assert_some(&storage, &e2);
        assert_some(&storage, &e3);

        let e4 = memory.insert(4, vec![b'4'; 3800]);
        let s4 = to_bytes(&e4);
        storage.apply(
            &HashSet::from_iter([e1.hash()]),
            vec![Item {
                slice: s4.clone(),
                hash: e4.hash(),
            }],
        );
        assert_eq!(storage.len(), s4.len());
        assert_none(&storage, e1.hash());
        assert_none(&storage, e2.hash());
        assert_none(&storage, e3.hash());
        assert_some(&storage, &e4);

        // test recovery
        storage.update();
        let bytes = storage.into_io_buffer();
        let mut buf = IoBuffer::new(PAGE);
        buf[0..bytes.len()].copy_from_slice(&bytes);
        let mut storage = SetStorage::load(buf, 0, Arc::new(Metrics::noop()));

        assert_eq!(storage.len(), s4.len());
        assert_none(&storage, e1.hash());
        assert_none(&storage, e2.hash());
        assert_none(&storage, e3.hash());
        assert_some(&storage, &e4);

        // test oversize entry
        let e5 = memory.insert(5, vec![b'5'; 20 * 1024]);
        let s5 = to_bytes(&e5);
        storage.apply(
            &HashSet::new(),
            vec![Item {
                slice: s5.clone(),
                hash: e5.hash(),
            }],
        );
        assert_eq!(storage.len(), s4.len());
        assert_none(&storage, e1.hash());
        assert_none(&storage, e2.hash());
        assert_none(&storage, e3.hash());
        assert_some(&storage, &e4);
    }
}
