//  Copyright 2024 foyer Project Authors
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
    collections::{HashMap, HashSet},
    fmt::Debug,
    ops::Range,
    sync::Arc,
    time::Instant,
};

use foyer_common::{
    bits,
    code::{HashBuilder, StorageKey, StorageValue},
    metrics::Metrics,
};
use foyer_memory::CacheEntry;
use itertools::Itertools;
use tokio::sync::oneshot;

use crate::{
    device::ALIGN,
    io_buffer_pool::IoBufferPool,
    serde::EntrySerializer,
    small::{serde::EntryHeader, set::SetId, set_manager::SetPicker},
    Compression, IoBuffer, IoBytes,
};

type Sequence = usize;

#[derive(Debug)]
struct ItemMut<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    range: Range<usize>,
    entry: CacheEntry<K, V, S>,
    sequence: Sequence,
}

#[derive(Debug)]
struct SetBatchMut<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    items: Vec<ItemMut<K, V, S>>,
    deletes: HashMap<u64, Sequence>,
}

impl<K, V, S> Default for SetBatchMut<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn default() -> Self {
        Self {
            items: vec![],
            deletes: HashMap::new(),
        }
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
    buffer: IoBuffer,
    len: usize,
    sequence: Sequence,

    /// Cache write buffer between rotation to reduce page fault.
    buffer_pool: IoBufferPool,
    set_picker: SetPicker,

    waiters: Vec<oneshot::Sender<()>>,

    init: Option<Instant>,

    metrics: Arc<Metrics>,
}

impl<K, V, S> BatchMut<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    pub fn new(sets: usize, buffer_size: usize, metrics: Arc<Metrics>) -> Self {
        let buffer_size = bits::align_up(ALIGN, buffer_size);

        Self {
            sets: HashMap::new(),
            buffer: IoBuffer::new(buffer_size),
            len: 0,
            sequence: 0,
            buffer_pool: IoBufferPool::new(buffer_size, 1),
            set_picker: SetPicker::new(sets),
            waiters: vec![],
            init: None,
            metrics,
        }
    }

    pub fn insert(&mut self, entry: CacheEntry<K, V, S>, estimated_size: usize) -> bool {
        // For the small object disk cache does NOT compress entries, `estimated_size` is actually `exact_size`.
        tracing::trace!("[sodc batch]: insert entry");

        if self.init.is_none() {
            self.init = Some(Instant::now());
        }
        self.sequence += 1;

        let sid = self.sid(entry.hash());
        let len = EntryHeader::ENTRY_HEADER_SIZE + estimated_size;

        let set = &mut self.sets.entry(sid).or_default();

        set.deletes.insert(entry.hash(), self.sequence);

        if entry.is_outdated() || self.len + len > self.buffer.len() {
            return false;
        }

        let info = match EntrySerializer::serialize(
            entry.key(),
            entry.value(),
            &Compression::None,
            &mut self.buffer[self.len + EntryHeader::ENTRY_HEADER_SIZE..self.len + len],
            &self.metrics,
        ) {
            Ok(info) => info,
            Err(e) => {
                tracing::warn!("[sodc batch]: serialize entry error: {e}");
                return false;
            }
        };
        assert_eq!(info.key_len + info.value_len + EntryHeader::ENTRY_HEADER_SIZE, len);
        let header = EntryHeader::new(entry.hash(), info.key_len, info.value_len);
        header.write(&mut self.buffer[self.len..self.len + EntryHeader::ENTRY_HEADER_SIZE]);

        set.items.push(ItemMut {
            range: self.len..self.len + len,
            entry,
            sequence: self.sequence,
        });
        self.len += len;

        true
    }

    pub fn delete(&mut self, hash: u64) {
        tracing::trace!("[sodc batch]: delete entry");

        if self.init.is_none() {
            self.init = Some(Instant::now());
        }
        self.sequence += 1;

        let sid = self.sid(hash);
        self.sets.entry(sid).or_default().deletes.insert(hash, self.sequence);
    }

    /// Register a waiter to be notified after the batch is finished.
    pub fn wait(&mut self, tx: oneshot::Sender<()>) {
        tracing::trace!("[sodc batch]: register waiter");
        if self.init.is_none() {
            self.init = Some(Instant::now());
        }
        self.waiters.push(tx);
    }

    fn sid(&self, hash: u64) -> SetId {
        self.set_picker.sid(hash)
    }

    pub fn is_empty(&self) -> bool {
        self.init.is_none()
    }

    pub fn rotate(&mut self) -> Option<Batch<K, V, S>> {
        if self.is_empty() {
            return None;
        }

        let mut buffer = self.buffer_pool.acquire();
        std::mem::swap(&mut self.buffer, &mut buffer);
        self.len = 0;
        self.sequence = 0;
        let buffer = IoBytes::from(buffer);
        self.buffer_pool.release(buffer.clone());

        let sets = self
            .sets
            .drain()
            .map(|(sid, batch)| {
                let items = batch
                    .items
                    .into_iter()
                    .filter(|item| item.sequence >= batch.deletes.get(&item.entry.hash()).copied().unwrap_or_default())
                    .map(|item| Item {
                        buffer: buffer.slice(item.range),
                        entry: item.entry,
                    })
                    .collect_vec();
                let deletes = batch.deletes.keys().copied().collect();
                (
                    sid,
                    SetBatch {
                        deletions: deletes,
                        items,
                    },
                )
            })
            .collect();

        let waiters = std::mem::take(&mut self.waiters);
        let init = self.init.take();

        Some(Batch { sets, waiters, init })
    }
}

pub struct Item<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    pub buffer: IoBytes,
    pub entry: CacheEntry<K, V, S>,
}

impl<K, V, S> Debug for Item<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Item").field("hash", &self.entry.hash()).finish()
    }
}

pub struct SetBatch<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    pub deletions: HashSet<u64>,
    pub items: Vec<Item<K, V, S>>,
}

impl<K, V, S> Debug for SetBatch<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SetBatch")
            .field("deletes", &self.deletions)
            .field("items", &self.items)
            .finish()
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
    pub init: Option<Instant>,
}

impl<K, V, S> Default for Batch<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn default() -> Self {
        Self {
            sets: HashMap::new(),
            waiters: vec![],
            init: None,
        }
    }
}

impl<K, V, S> Debug for Batch<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Batch")
            .field("sets", &self.sets)
            .field("waiters", &self.waiters)
            .field("init", &self.init)
            .finish()
    }
}
