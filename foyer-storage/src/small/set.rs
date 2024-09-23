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

use std::{
    collections::HashSet,
    fmt::Debug,
    ops::{Deref, DerefMut, Range},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::{Buf, BufMut};
use foyer_common::code::{StorageKey, StorageValue};

use super::{bloom_filter::BloomFilterU64, serde::EntryHeader};
use crate::{
    error::Result,
    serde::{Checksummer, EntryDeserializer},
    IoBytes, IoBytesMut,
};

pub type SetId = u64;

#[derive(Debug)]
pub struct Set {
    storage: Arc<SetStorage>,
}

impl Deref for Set {
    type Target = SetStorage;

    fn deref(&self) -> &Self::Target {
        &self.storage
    }
}

impl Set {
    pub fn new(storage: Arc<SetStorage>) -> Self {
        Self { storage }
    }
}

#[derive(Debug)]
pub struct SetMut {
    storage: SetStorage,
}

impl Deref for SetMut {
    type Target = SetStorage;

    fn deref(&self) -> &Self::Target {
        &self.storage
    }
}

impl DerefMut for SetMut {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.storage
    }
}

impl SetMut {
    pub fn new(storage: SetStorage) -> Self {
        Self { storage }
    }

    pub fn into_storage(self) -> SetStorage {
        self.storage
    }
}

/// # Format
///
/// ```plain
/// | checksum (4B) | timestamp (8B) | len (4B) |
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
    timestamp: u64,
    /// Set bloom filter.
    bloom_filter: BloomFilterU64,

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
    pub const SET_HEADER_SIZE: usize = 24;

    pub fn load(buffer: IoBytesMut) -> Self {
        assert!(buffer.len() >= Self::SET_HEADER_SIZE);

        let checksum = (&buffer[0..4]).get_u32();
        let timestamp = (&buffer[4..12]).get_u64();
        let len = (&buffer[12..16]).get_u32() as usize;
        let bloom_filter_value = (&buffer[16..24]).get_u64();
        let bloom_filter = BloomFilterU64::from(bloom_filter_value);

        let mut this = Self {
            checksum,
            len,
            capacity: buffer.len() - Self::SET_HEADER_SIZE,
            size: buffer.len(),
            timestamp,
            bloom_filter,
            buffer,
        };

        if Self::SET_HEADER_SIZE + this.len >= this.buffer.len() {
            // invalid len
            this.clear();
        } else {
            let c = Checksummer::checksum32(&this.buffer[4..Self::SET_HEADER_SIZE + this.len]);
            if c != checksum {
                // checksum mismatch
                this.clear();
            }
        }

        this
    }

    pub fn update(&mut self) {
        (&mut self.buffer[16..24]).put_u64(self.bloom_filter.value());
        (&mut self.buffer[12..16]).put_u32(self.len as _);
        self.timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        (&mut self.buffer[4..12]).put_u64(self.timestamp);
        self.checksum = Checksummer::checksum32(&self.buffer[4..Self::SET_HEADER_SIZE + self.len]);
        (&mut self.buffer[0..4]).put_u32(self.checksum);
    }

    pub fn bloom_filter(&self) -> &BloomFilterU64 {
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

    pub fn apply(&mut self, deletions: &HashSet<u64>, insertions: impl Iterator<Item = u64>, buffer: &[u8]) {
        assert!(buffer.len() < self.capacity);

        self.deletes(deletions);
        self.append(insertions, buffer);
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

    fn append(&mut self, insertions: impl Iterator<Item = u64>, buffer: &[u8]) {
        if buffer.is_empty() {
            return;
        }

        self.reserve(buffer.len());
        self.buffer[Self::SET_HEADER_SIZE + self.len..Self::SET_HEADER_SIZE + self.len + buffer.len()]
            .copy_from_slice(buffer);
        self.len += buffer.len();
        for hash in insertions {
            self.bloom_filter.insert(hash);
        }
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

impl<'a> SetEntry<'a> {
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

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{serde::EntrySerializer, test_utils::metrics_for_test, Compression};

    const PAGE: usize = 4096;

    fn key(len: usize) -> Vec<u8> {
        vec![b'k'; len]
    }

    fn value(len: usize) -> Vec<u8> {
        vec![b'v'; len]
    }

    fn entry(hash: u64, key_len: usize, value_len: usize) -> IoBytesMut {
        let mut buf = IoBytesMut::new();

        // reserve header
        let header = EntryHeader::new(0, 0, 0);
        header.write(&mut buf);

        let key = key(key_len);
        let value = value(value_len);

        let info = EntrySerializer::serialize(&key, &value, &Compression::None, &mut buf, metrics_for_test()).unwrap();

        let header = EntryHeader::new(hash, info.key_len, info.value_len);
        header.write(&mut buf[0..EntryHeader::ENTRY_HEADER_SIZE]);

        buf
    }

    fn assert_some(storage: &SetStorage, hash: u64, key: Vec<u8>, value: Vec<u8>) {
        let ret = storage.get::<Vec<u8>, Vec<u8>>(hash).unwrap();
        let (k, v) = ret.unwrap();
        assert_eq!(k, key);
        assert_eq!(v, value);
    }

    fn assert_none(storage: &SetStorage, hash: u64) {
        let ret = storage.get::<Vec<u8>, Vec<u8>>(hash).unwrap();
        assert!(ret.is_none());
    }

    #[test]
    #[should_panic]
    fn test_set_storage_empty() {
        let buffer = IoBytesMut::new();
        SetStorage::load(buffer);
    }

    #[test]
    fn test_set_storage_basic() {
        let mut buffer = IoBytesMut::with_capacity(PAGE);
        unsafe { buffer.set_len(PAGE) };

        // load will result in an empty set
        let mut storage = SetStorage::load(buffer);
        assert!(storage.is_empty());

        let e1 = entry(1, 1, 42);
        storage.apply(&HashSet::from_iter([2, 4]), std::iter::once(1), &e1);
        assert_eq!(storage.len(), e1.len());
        assert_some(&storage, 1, key(1), value(42));

        let e2 = entry(2, 2, 97);
        storage.apply(&HashSet::from_iter([1, 3, 5]), std::iter::once(2), &e2);
        assert_eq!(storage.len(), e2.len());
        assert_none(&storage, 1);
        assert_some(&storage, 2, key(2), value(97));

        let e3 = entry(3, 100, 100);
        storage.apply(&HashSet::from_iter([1]), std::iter::once(3), &e3);
        assert_eq!(storage.len(), e2.len() + e3.len());
        assert_none(&storage, 1);
        assert_some(&storage, 2, key(2), value(97));
        assert_some(&storage, 3, key(100), value(100));

        let e4 = entry(4, 100, 3800);
        storage.apply(&HashSet::from_iter([1]), std::iter::once(4), &e4);
        assert_eq!(storage.len(), e4.len());
        assert_none(&storage, 1);
        assert_none(&storage, 2);
        assert_none(&storage, 3);
        assert_some(&storage, 4, key(100), value(3800));

        storage.update();

        let buf = storage.freeze();
        println!("{buf:?}");

        let mut buffer = IoBytesMut::with_capacity(PAGE);
        unsafe { buffer.set_len(PAGE) };
        buffer[0..buf.len()].copy_from_slice(&buf);
        let storage = SetStorage::load(buffer);
        assert_eq!(storage.len(), e4.len());
        assert_none(&storage, 1);
        assert_none(&storage, 2);
        assert_none(&storage, 3);
        assert_some(&storage, 4, key(100), value(3800));
    }
}
