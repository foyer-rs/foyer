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
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::{Buf, BufMut};
use foyer_common::code::{StorageKey, StorageValue};

use super::{batch::Item, bloom_filter::BloomFilterU64, serde::EntryHeader};
use crate::{
    error::Result,
    serde::{Checksummer, EntryDeserializer},
    IoBytes, IoBytesMut,
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

    buffer: IoBytesMut,
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
    pub fn load(buffer: IoBytesMut, watermark: u128) -> Self {
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

    pub fn freeze(self) -> IoBytes {
        self.buffer.freeze()
    }

    pub fn apply<K, V>(&mut self, deletions: &HashSet<u64>, items: Vec<Item<K, V>>)
    where
        K: StorageKey,
        V: StorageValue,
    {
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

    fn append<K, V>(&mut self, items: Vec<Item<K, V>>)
    where
        K: StorageKey,
        V: StorageValue,
    {
        let (skip, size, _) = items
            .iter()
            .rev()
            .fold((items.len(), 0, true), |(skip, size, proceed), item| {
                let proceed = proceed && size + item.buffer.len() <= self.size - Self::SET_HEADER_SIZE;
                if proceed {
                    (skip - 1, size + item.buffer.len(), proceed)
                } else {
                    (skip, size, proceed)
                }
            });

        self.reserve(size);
        let mut cursor = Self::SET_HEADER_SIZE + self.len;
        for item in items.iter().skip(skip) {
            self.buffer[cursor..cursor + item.buffer.len()].copy_from_slice(&item.buffer);
            self.bloom_filter.insert(item.piece.hash());
            cursor += item.buffer.len();
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
                let k = EntryDeserializer::deserialize_key::<K>(entry.key)?;
                let v = EntryDeserializer::deserialize_value::<V>(entry.value, crate::Compression::None)?;
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
    pub hash: u64,
    pub key: &'a [u8],
    pub value: &'a [u8],
}

impl SetEntry<'_> {
    /// Length of the entry with header, key and value included.
    pub fn len(&self) -> usize {
        EntryHeader::ENTRY_HEADER_SIZE + self.key.len() + self.value.len()
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
        let mut cursor = self.offset;
        let header = EntryHeader::read(&self.set.data()[cursor..cursor + EntryHeader::ENTRY_HEADER_SIZE]);
        cursor += EntryHeader::ENTRY_HEADER_SIZE;
        let value = &self.set.data()[cursor..cursor + header.value_len()];
        cursor += header.value_len();
        let key = &self.set.data()[cursor..cursor + header.key_len()];
        let entry = SetEntry {
            offset: self.offset,
            hash: header.hash(),
            key,
            value,
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

    use foyer_common::metrics::Metrics;
    use foyer_memory::{Cache, CacheBuilder, CacheEntry};

    use super::*;
    use crate::{serde::EntrySerializer, Compression};

    const PAGE: usize = 4096;

    fn buffer(entry: &CacheEntry<u64, Vec<u8>>) -> IoBytes {
        let mut buf = IoBytesMut::new();

        // reserve header
        let header = EntryHeader::new(0, 0, 0);
        header.write(&mut buf);

        let info = EntrySerializer::serialize(
            entry.key(),
            entry.value(),
            &Compression::None,
            &mut buf,
            &Metrics::noop(),
        )
        .unwrap();

        let header = EntryHeader::new(entry.hash(), info.key_len, info.value_len);
        header.write(&mut buf[0..EntryHeader::ENTRY_HEADER_SIZE]);

        buf.freeze()
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
    #[should_panic]
    fn test_set_storage_empty() {
        let buffer = IoBytesMut::new();
        SetStorage::load(buffer, 0);
    }

    #[test]
    fn test_set_storage_basic() {
        let memory = memory_for_test();

        let mut buf = IoBytesMut::with_capacity(PAGE);
        unsafe { buf.set_len(PAGE) };

        // load will result in an empty set
        let mut storage = SetStorage::load(buf, 0);
        assert!(storage.is_empty());

        let e1 = memory.insert(1, vec![b'1'; 42]);
        let b1 = buffer(&e1);
        storage.apply(
            &HashSet::from_iter([2, 4]),
            vec![Item {
                buffer: b1.clone(),
                piece: e1.piece(),
            }],
        );
        assert_eq!(storage.len(), b1.len());
        assert_some(&storage, &e1);

        let e2 = memory.insert(2, vec![b'2'; 97]);
        let b2 = buffer(&e2);
        storage.apply(
            &HashSet::from_iter([e1.hash(), 3, 5]),
            vec![Item {
                buffer: b2.clone(),
                piece: e2.piece(),
            }],
        );
        assert_eq!(storage.len(), b2.len());
        assert_none(&storage, e1.hash());
        assert_some(&storage, &e2);

        let e3 = memory.insert(3, vec![b'3'; 211]);
        let b3 = buffer(&e3);
        storage.apply(
            &HashSet::from_iter([e1.hash()]),
            vec![Item {
                buffer: b3.clone(),
                piece: e3.piece(),
            }],
        );
        assert_eq!(storage.len(), b2.len() + b3.len());
        assert_none(&storage, e1.hash());
        assert_some(&storage, &e2);
        assert_some(&storage, &e3);

        let e4 = memory.insert(4, vec![b'4'; 3800]);
        let b4 = buffer(&e4);
        storage.apply(
            &HashSet::from_iter([e1.hash()]),
            vec![Item {
                buffer: b4.clone(),
                piece: e4.piece(),
            }],
        );
        assert_eq!(storage.len(), b4.len());
        assert_none(&storage, e1.hash());
        assert_none(&storage, e2.hash());
        assert_none(&storage, e3.hash());
        assert_some(&storage, &e4);

        // test recovery
        storage.update();
        let bytes = storage.freeze();
        let mut buf = IoBytesMut::with_capacity(PAGE);
        unsafe { buf.set_len(PAGE) };
        buf[0..bytes.len()].copy_from_slice(&bytes);
        let mut storage = SetStorage::load(buf, 0);

        assert_eq!(storage.len(), b4.len());
        assert_none(&storage, e1.hash());
        assert_none(&storage, e2.hash());
        assert_none(&storage, e3.hash());
        assert_some(&storage, &e4);

        // test oversize entry
        let e5 = memory.insert(5, vec![b'5'; 20 * 1024]);
        let b5 = buffer(&e5);
        storage.apply(
            &HashSet::new(),
            vec![Item {
                buffer: b5.clone(),
                piece: e5.piece(),
            }],
        );
        assert_eq!(storage.len(), b4.len());
        assert_none(&storage, e1.hash());
        assert_none(&storage, e2.hash());
        assert_none(&storage, e3.hash());
        assert_some(&storage, &e4);
    }
}
