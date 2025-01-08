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

use std::{fmt::Debug, ops::Range, sync::Arc, time::Instant};

use foyer_common::{
    bits,
    code::{StorageKey, StorageValue},
    metrics::model::Metrics,
    range::RangeBoundsExt,
    strict_assert_eq,
};
use foyer_memory::Piece;
use itertools::Itertools;
use tokio::sync::oneshot;

use super::{
    indexer::{EntryAddress, Indexer},
    reclaimer::Reinsertion,
    serde::Sequence,
    tombstone::Tombstone,
};
use crate::{
    device::{bytes::IoBytes, MonitoredDevice, RegionId},
    io_buffer_pool::IoBufferPool,
    large::{indexer::HashedEntryAddress, serde::EntryHeader},
    region::{GetCleanRegionHandle, RegionManager},
    serde::{Checksummer, EntrySerializer},
    Compression, Dev, DevExt, IoBuffer,
};

pub struct BatchMut<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    buffer: IoBuffer,
    len: usize,
    groups: Vec<GroupMut<K, V>>,
    tombstones: Vec<TombstoneInfo>,
    waiters: Vec<oneshot::Sender<()>>,
    init: Option<Instant>,

    /// Cache write buffer between rotation to reduce page fault.
    buffer_pool: IoBufferPool,

    region_manager: RegionManager,
    device: MonitoredDevice,
    indexer: Indexer,
    metrics: Arc<Metrics>,
}

impl<K, V> Debug for BatchMut<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchMut")
            .field("len", &self.len)
            .field("groups", &self.groups)
            .field("tombstones", &self.tombstones)
            .field("waiters", &self.waiters)
            .field("init", &self.init)
            .finish()
    }
}

impl<K, V> BatchMut<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    pub fn new(
        buffer_size: usize,
        region_manager: RegionManager,
        device: MonitoredDevice,
        indexer: Indexer,
        metrics: Arc<Metrics>,
    ) -> Self {
        let capacity = bits::align_up(device.align(), buffer_size);
        let mut batch = Self {
            buffer: IoBuffer::new(capacity),
            len: 0,
            groups: vec![],
            tombstones: vec![],
            waiters: vec![],
            init: None,
            buffer_pool: IoBufferPool::new(capacity, 1),
            region_manager,
            device,
            indexer,
            metrics,
        };
        batch.append_group();
        batch
    }

    pub fn piece(&mut self, piece: Piece<K, V>, compression: &Compression, sequence: Sequence) -> bool {
        tracing::trace!("[batch]: append entry with sequence: {sequence}");

        self.may_init();

        let pos = self.len;

        if pos + EntryHeader::serialized_len() >= self.buffer.len() {
            // Only handle start position overflow. End position overflow will be handled by serde.
            return false;
        }

        let info = match EntrySerializer::serialize(
            piece.key(),
            piece.value(),
            compression,
            &mut self.buffer[pos + EntryHeader::serialized_len()..],
            &self.metrics,
        ) {
            Ok(info) => info,
            Err(e) => {
                tracing::warn!("[lodc batch]: serialize entry error: {e}");
                return false;
            }
        };

        let header = EntryHeader {
            key_len: info.key_len as _,
            value_len: info.value_len as _,
            hash: piece.hash(),
            sequence,
            checksum: Checksummer::checksum64(
                &self.buffer[pos + EntryHeader::serialized_len()
                    ..pos + EntryHeader::serialized_len() + info.key_len + info.value_len],
            ),
            compression: *compression,
        };
        header.write(&mut self.buffer[pos..pos + EntryHeader::serialized_len()]);

        let aligned = bits::align_up(self.device.align(), header.entry_len());
        self.advance(aligned);

        let group = self.groups.last_mut().unwrap();
        group.indices.push(HashedEntryAddress {
            hash: piece.hash(),
            address: EntryAddress {
                region: RegionId::MAX,
                offset: group.region.offset as u32 + group.region.len as u32,
                len: header.entry_len() as _,
                sequence,
            },
        });
        group.pieces.push(piece);
        group.region.len += aligned;
        group.range.end += aligned;

        true
    }

    pub fn tombstone(&mut self, tombstone: Tombstone, stats: Option<InvalidStats>) {
        tracing::trace!("[batch]: append tombstone");

        self.may_init();

        self.tombstones.push(TombstoneInfo { tombstone, stats });
    }

    pub fn reinsertion(&mut self, reinsertion: &Reinsertion) -> bool {
        tracing::trace!("[batch]: submit reinsertion");

        self.may_init();

        let aligned = bits::align_up(self.device.align(), reinsertion.buffer.len());

        // Skip if the entry is no longer in the indexer.
        // Skip if the batch buffer size exceeds the threshold.
        if self.indexer.get(reinsertion.hash).is_none() || self.len + aligned > self.buffer.len() {
            return false;
        }

        let pos = self.len;

        self.buffer[pos..pos + reinsertion.buffer.len()].copy_from_slice(&reinsertion.buffer);

        self.advance(aligned);

        let group = self.groups.last_mut().unwrap();
        // Reserve buffer space for entry.
        group.indices.push(HashedEntryAddress {
            hash: reinsertion.hash,
            address: EntryAddress {
                region: RegionId::MAX,
                offset: group.region.offset as u32 + group.region.len as u32,
                len: reinsertion.buffer.len() as _,
                sequence: reinsertion.sequence,
            },
        });
        group.region.len += aligned;
        group.range.end += aligned;

        true
    }

    /// Register a waiter to be notified after the batch is finished.
    pub fn wait(&mut self, tx: oneshot::Sender<()>) {
        tracing::trace!("[batch]: register waiter");
        self.may_init();
        self.waiters.push(tx);
    }

    pub fn rotate(&mut self) -> Option<Batch<K, V>> {
        if self.is_empty() {
            return None;
        }

        let mut buffer = self.buffer_pool.acquire();
        std::mem::swap(&mut self.buffer, &mut buffer);
        self.len = 0;
        let buffer = IoBytes::from(buffer);
        self.buffer_pool.release(buffer.clone());

        let init = self.init.take();

        let tombstones = std::mem::take(&mut self.tombstones);

        let waiters = std::mem::take(&mut self.waiters);

        let next = self.groups.last().map(|last| {
            assert!(!last.region.is_full);
            let next = GroupMut {
                region: RegionHandle {
                    handle: last.region.handle.clone(),
                    offset: last.region.offset + last.region.len as u64,
                    len: 0,
                    is_full: false,
                },
                indices: vec![],
                pieces: vec![],
                range: 0..0,
            };
            tracing::trace!("[batch]: try to reuse the last region with: {next:?}");
            next
        });

        let groups = self
            .groups
            .drain(..)
            .map(|group| {
                // TODO(MrCroxx): Refine to logic.
                // Do not filter empty group here.
                // An empty group can be used to trigger marking evictable region in flusher.
                strict_assert_eq!(group.region.len, group.range.size().unwrap());
                Group {
                    region: group.region,
                    bytes: buffer.slice(group.range),
                    indices: group.indices,
                    pieces: group.pieces,
                }
            })
            .collect_vec();

        match next {
            Some(next) => self.groups.push(next),
            None => self.append_group(),
        }

        Some(Batch {
            groups,
            tombstones,
            waiters,
            init,
        })
    }

    fn advance(&mut self, len: usize) {
        assert!(bits::is_aligned(self.device.align(), len));
        assert!(bits::is_aligned(self.device.align(), self.len));

        // Rotate group if the current one is full.
        let group = self.groups.last_mut().unwrap();
        if group.region.offset as usize + group.region.len + len > self.device.region_size() {
            group.region.is_full = true;
            self.append_group();
        }

        self.len += len;
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.tombstones.is_empty() && self.groups.iter().all(|group| group.range.is_empty()) && self.waiters.is_empty()
    }

    #[inline]
    fn may_init(&mut self) {
        if self.init.is_none() {
            self.init = Some(Instant::now());
        }
    }

    #[inline]
    fn append_group(&mut self) {
        self.groups.push(GroupMut {
            region: RegionHandle {
                handle: self.region_manager.get_clean_region(),
                offset: 0,
                len: 0,
                is_full: false,
            },
            indices: vec![],
            pieces: vec![],
            range: self.len..self.len,
        })
    }
}

#[derive(Debug)]
pub struct InvalidStats {
    pub region: RegionId,
    pub size: usize,
}

pub struct RegionHandle {
    /// Handle of the region to write.
    pub handle: GetCleanRegionHandle,
    /// Offset of the region to write.
    pub offset: u64,
    /// Length of the buffer to write.
    pub len: usize,
    /// If the region was full.
    pub is_full: bool,
}

impl Debug for RegionHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegionHandle")
            .field("offset", &self.offset)
            .field("size", &self.len)
            .field("is_full", &self.is_full)
            .finish()
    }
}

struct GroupMut<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    /// Reusable Clean region handle.
    region: RegionHandle,
    /// Entry indices to be inserted.
    indices: Vec<HashedEntryAddress>,
    /// Hold entries until flush finishes to avoid in-memory cache lookup miss.
    pieces: Vec<Piece<K, V>>,
    /// Tracks the group bytes range of the batch buffer.
    range: Range<usize>,
}

impl<K, V> Debug for GroupMut<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Group")
            .field("handle", &self.region)
            .field("indices", &self.indices)
            .field("range", &self.range)
            .finish()
    }
}

pub struct Group<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    /// Reusable Clean region handle.
    pub region: RegionHandle,
    /// Buffer to flush.
    pub bytes: IoBytes,
    /// Entry indices to be inserted.
    pub indices: Vec<HashedEntryAddress>,
    /// Hold entries until flush finishes to avoid in-memory cache lookup miss.
    pub pieces: Vec<Piece<K, V>>,
}

impl<K, V> Debug for Group<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Group")
            .field("handle", &self.region)
            .field("indices", &self.indices)
            .finish()
    }
}

#[derive(Debug)]
pub struct TombstoneInfo {
    pub tombstone: Tombstone,
    pub stats: Option<InvalidStats>,
}

pub struct Batch<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    pub groups: Vec<Group<K, V>>,
    pub tombstones: Vec<TombstoneInfo>,
    pub waiters: Vec<oneshot::Sender<()>>,
    pub init: Option<Instant>,
}

impl<K, V> Debug for Batch<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Batch")
            .field("groups", &self.groups)
            .field("tombstones", &self.tombstones)
            .field("waiters", &self.waiters)
            .field("init", &self.init)
            .finish()
    }
}

#[cfg(test)]
mod tests {

    use std::path::Path;

    use bytesize::ByteSize;
    use foyer_memory::{Cache, CacheBuilder, FifoConfig};
    use tokio::sync::Semaphore;

    use super::*;
    use crate::{
        device::monitor::{Monitored, MonitoredConfig},
        DirectFsDeviceOptions, FifoPicker, Runtime,
    };

    async fn device_for_test(dir: impl AsRef<Path>) -> MonitoredDevice {
        let runtime = Runtime::current();
        Monitored::open(
            MonitoredConfig {
                config: DirectFsDeviceOptions::new(dir)
                    .with_capacity(ByteSize::kib(64).as_u64() as _)
                    .with_file_size(ByteSize::kib(16).as_u64() as _)
                    .into(),
                metrics: Arc::new(Metrics::noop()),
            },
            runtime,
        )
        .await
        .unwrap()
    }

    fn cache_for_test() -> Cache<u64, Vec<u8>> {
        CacheBuilder::new(10)
            .with_shards(1)
            .with_eviction_config(FifoConfig::default())
            .build()
    }

    #[test_log::test(tokio::test)]
    async fn test_batch_mut_entry_overflow() {
        let dir = tempfile::tempdir().unwrap();
        let device = device_for_test(dir.path()).await;
        let metrics = Arc::new(Metrics::noop());
        let region_manager = RegionManager::new(
            device.clone(),
            vec![Box::new(FifoPicker::default())],
            Arc::new(Semaphore::new(0)),
            Arc::new(Metrics::noop()),
        );
        let indexer = Indexer::new(1);

        let mem = cache_for_test();
        let mut b = BatchMut::new(64 * 1024, region_manager, device, indexer, metrics);

        let e = mem.insert(1, vec![1; 128 * 1024]);
        let inserted = b.piece(e.piece(), &Compression::None, 1);
        assert!(!inserted);
    }
}
