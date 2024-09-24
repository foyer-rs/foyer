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
    collections::{hash_map::HashMap, HashSet},
    fmt::Debug,
};

use foyer_common::code::{HashBuilder, StorageKey, StorageValue};
use foyer_memory::CacheEntry;
use tokio::sync::oneshot;

use super::set::SetId;
use crate::{serde::KvInfo, small::serde::EntryHeader, IoBytes, IoBytesMut};

struct Insertion<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    entry: CacheEntry<K, V, S>,
    buffer: IoBytes,
    info: KvInfo,
}

struct Deletion {
    hash: u64,
}

struct Entry<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    insertion: Insertion<K, V, S>,
    prev_hash: Option<u64>,
    next_hash: Option<u64>,
}

struct SetBatchMut<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    entries: HashMap<u64, Entry<K, V, S>>,
    deletions: HashMap<u64, Deletion>,

    head_hash: Option<u64>,
    tail_hash: Option<u64>,

    len: usize,
    capacity: usize,
}

impl<K, V, S> Debug for SetBatchMut<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SetMut")
            .field("count", &self.entries.len())
            .field("len", &self.len)
            .finish()
    }
}

impl<K, V, S> SetBatchMut<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::new(),
            deletions: HashMap::new(),
            head_hash: None,
            tail_hash: None,
            len: 0,
            capacity,
        }
    }

    fn insert(&mut self, insertion: Insertion<K, V, S>) {
        self.deletions.remove(&insertion.entry.hash());

        let mut entry = Entry {
            insertion,
            prev_hash: None,
            next_hash: None,
        };

        self.list_push(&mut entry);
        self.len += EntryHeader::ENTRY_HEADER_SIZE + entry.insertion.buffer.len();
        if let Some(mut old) = self.entries.insert(entry.insertion.entry.hash(), entry) {
            self.list_unlink(&mut old);
            self.len -= EntryHeader::ENTRY_HEADER_SIZE + old.insertion.buffer.len();
        }

        while self.len > self.capacity {
            let entry = self.pop().unwrap();
            self.len -= EntryHeader::ENTRY_HEADER_SIZE + entry.insertion.buffer.len();
        }
    }

    fn delete(&mut self, deletion: Deletion) {
        if let Some(mut entry) = self.entries.remove(&deletion.hash) {
            self.list_unlink(&mut entry);
            self.len -= EntryHeader::ENTRY_HEADER_SIZE + entry.insertion.buffer.len();
        }

        self.deletions.insert(deletion.hash, deletion);
    }

    fn freeze(mut self) -> SetBatch<K, V, S> {
        let mut buf = IoBytesMut::with_capacity(self.len);
        let mut entries = Vec::with_capacity(self.entries.len());
        let mut deletions = HashSet::with_capacity(self.entries.len() + self.deletions.len());
        let mut insertions = Vec::with_capacity(self.entries.len());

        while let Some(entry) = self.pop() {
            let header = EntryHeader::new(
                entry.insertion.entry.hash(),
                entry.insertion.info.key_len,
                entry.insertion.info.value_len,
            );
            header.write(&mut buf);
            deletions.insert(entry.insertion.entry.hash());
            insertions.push(entry.insertion.entry.hash());
            buf.extend_from_slice(&entry.insertion.buffer);
            entries.push(entry.insertion.entry);
        }

        for deletion in self.deletions.into_values() {
            deletions.insert(deletion.hash);
        }

        assert_eq!(buf.len(), self.len);

        SetBatch {
            deletions,
            insertions,
            bytes: buf.freeze(),
            entries,
        }
    }

    fn list_unlink(&mut self, entry: &mut Entry<K, V, S>) {
        if let Some(prev_hash) = entry.prev_hash {
            self.entries.get_mut(&prev_hash).unwrap().next_hash = entry.next_hash;
        } else {
            assert_eq!(self.head_hash, Some(entry.insertion.entry.hash()));
            self.head_hash = entry.next_hash;
        }
        if let Some(next_hash) = entry.next_hash {
            self.entries.get_mut(&next_hash).unwrap().prev_hash = entry.prev_hash;
        } else {
            assert_eq!(self.tail_hash, Some(entry.insertion.entry.hash()));
            self.tail_hash = entry.prev_hash;
        }
        entry.prev_hash = None;
        entry.next_hash = None;
    }

    fn list_push(&mut self, entry: &mut Entry<K, V, S>) {
        assert!(entry.prev_hash.is_none());
        assert!(entry.next_hash.is_none());

        if let Some(tail_hash) = self.tail_hash {
            let tail = self.entries.get_mut(&tail_hash).unwrap();

            tail.next_hash = Some(entry.insertion.entry.hash());
            entry.prev_hash = Some(tail_hash);

            self.tail_hash = Some(entry.insertion.entry.hash());
        } else {
            assert!(self.head_hash.is_none());

            self.head_hash = Some(entry.insertion.entry.hash());
            self.tail_hash = Some(entry.insertion.entry.hash());
        }
    }

    fn pop(&mut self) -> Option<Entry<K, V, S>> {
        let head_hash = self.head_hash?;
        let mut entry = self.entries.remove(&head_hash).unwrap();
        self.list_unlink(&mut entry);
        Some(entry)
    }
}

#[derive(Debug)]
pub struct BatchMut<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    sets: HashMap<SetId, SetBatchMut<K, V, S>>,
    /// Total set count.
    total: SetId,
    /// Set data capacity.
    set_capacity: usize,

    waiters: Vec<oneshot::Sender<()>>,
}

impl<K, V, S> BatchMut<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    pub fn new(total: SetId, set_data_size: usize) -> Self {
        Self {
            sets: HashMap::new(),
            total,
            set_capacity: set_data_size,
            waiters: vec![],
        }
    }

    pub fn append(&mut self, entry: CacheEntry<K, V, S>, buffer: IoBytes, info: KvInfo) {
        let sid = entry.hash() % self.total;
        let set = self.sets.entry(sid).or_insert(SetBatchMut::new(self.set_capacity));
        let insertion = Insertion { entry, buffer, info };
        set.insert(insertion);
    }

    pub fn delete(&mut self, hash: u64) {
        let sid = hash % self.total;
        let set = self.sets.entry(sid).or_insert(SetBatchMut::new(self.set_capacity));
        set.delete(Deletion { hash })
    }

    pub fn rotate(&mut self) -> Batch<K, V, S> {
        let sets = std::mem::take(&mut self.sets);
        let sets = sets.into_iter().map(|(id, set)| (id, set.freeze())).collect();
        let waiters = std::mem::take(&mut self.waiters);
        Batch { sets, waiters }
    }

    /// Register a waiter to be notified after the batch is finished.
    pub fn wait(&mut self) -> oneshot::Receiver<()> {
        tracing::trace!("[batch]: register waiter");
        let (tx, rx) = oneshot::channel();
        self.waiters.push(tx);
        rx
    }
}

pub struct Batch<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    pub sets: HashMap<SetId, SetBatch<K, V, S>>,
    pub waiters: Vec<oneshot::Sender<()>>,
}

pub struct SetBatch<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    pub deletions: HashSet<u64>,
    pub insertions: Vec<u64>,
    pub bytes: IoBytes,
    pub entries: Vec<CacheEntry<K, V, S>>,
}

#[cfg(test)]
mod tests {
    use foyer_memory::{Cache, CacheBuilder};

    use crate::{serde::EntrySerializer, test_utils::metrics_for_test, Compression};

    use super::*;

    fn cache_for_test() -> Cache<u64, Vec<u8>> {
        CacheBuilder::new(10).build()
    }

    fn serialize(entry: &CacheEntry<u64, Vec<u8>>) -> (IoBytes, KvInfo) {
        let mut bytes = IoBytesMut::new();
        let info = EntrySerializer::serialize(
            entry.key(),
            entry.value(),
            &Compression::None,
            &mut bytes,
            &metrics_for_test(),
        )
        .unwrap();
        (bytes.freeze(), info)
    }

    #[test]
    fn test_batch_insert() {
        let cache = cache_for_test();
        let mut batch: BatchMut<u64, Vec<u8>, ahash::RandomState> = BatchMut::new(4, 1000);

        let e1 = cache.insert(1, vec![b'1'; 10]);
        let (b1, i1) = serialize(&e1);

        batch.append(e1, b1, i1);
    }
}
