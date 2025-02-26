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
    collections::{HashMap, HashSet},
    fmt::Debug,
    ops::Range,
    sync::Arc,
    time::Instant,
};

use foyer_common::{
    bits,
    code::{StorageKey, StorageValue},
    metrics::Metrics,
};
use foyer_memory::Piece;
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
struct ItemMut {
    range: Range<usize>,
    hash: u64,
    sequence: Sequence,
}

#[derive(Debug, Default)]
struct SetBatchMut {
    items: Vec<ItemMut>,
    deletes: HashMap<u64, Sequence>,
}

#[derive(Debug)]
pub struct BatchMut {
    sets: HashMap<SetId, SetBatchMut>,
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

impl BatchMut {
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

    pub fn insert<K, V>(&mut self, piece: Piece<K, V>, estimated_size: usize) -> bool
    where
        K: StorageKey,
        V: StorageValue,
    {
        // For the small object disk cache does NOT compress entries, `estimated_size` is actually `exact_size`.
        tracing::trace!("[sodc batch]: insert entry");

        if self.init.is_none() {
            self.init = Some(Instant::now());
        }
        self.sequence += 1;

        let sid = self.sid(piece.hash());
        let len = EntryHeader::ENTRY_HEADER_SIZE + estimated_size;

        let set = &mut self.sets.entry(sid).or_default();

        set.deletes.insert(piece.hash(), self.sequence);

        if self.len + len > self.buffer.len() {
            tracing::trace!("[sodc batch]: insert {} ignored, reason: buffer overflow", piece.hash());
            return false;
        }

        let info = match EntrySerializer::serialize(
            piece.key(),
            piece.value(),
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
        let header = EntryHeader::new(piece.hash(), info.key_len, info.value_len);
        header.write(&mut self.buffer[self.len..self.len + EntryHeader::ENTRY_HEADER_SIZE]);

        set.items.push(ItemMut {
            range: self.len..self.len + len,
            hash: piece.hash(),
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

    pub fn rotate(&mut self) -> Option<Batch> {
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
                    .filter(|item| item.sequence >= batch.deletes.get(&item.hash).copied().unwrap_or_default())
                    .map(|item| Item {
                        buffer: buffer.slice(item.range),
                        hash: item.hash,
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

pub struct Item {
    pub buffer: IoBytes,
    pub hash: u64,
}

impl Debug for Item {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Item").field("hash", &self.hash).finish()
    }
}

pub struct SetBatch {
    pub deletions: HashSet<u64>,
    pub items: Vec<Item>,
}

impl Debug for SetBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SetBatch")
            .field("deletes", &self.deletions)
            .field("items", &self.items)
            .finish()
    }
}

#[derive(Debug, Default)]
pub struct Batch {
    pub sets: HashMap<SetId, SetBatch>,
    pub waiters: Vec<oneshot::Sender<()>>,
    pub init: Option<Instant>,
}
