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

use foyer_common::{
    bits,
    code::{HashBuilder, StorageKey, StorageValue},
    range::RangeBoundsExt,
    strict_assert_eq,
    wait_group::{WaitGroup, WaitGroupFuture, WaitGroupGuard},
};
use foyer_memory::CacheEntry;
use itertools::Itertools;
use std::{
    fmt::Debug,
    mem::ManuallyDrop,
    ops::{Deref, DerefMut, Range},
    time::Instant,
};
use tokio::sync::oneshot;

use crate::{
    device::{bytes::IoBytes, MonitoredDevice, RegionId},
    error::Result,
    large::indexer::HashedEntryAddress,
    region::{GetCleanRegionHandle, RegionManager},
    Dev, DevExt, IoBuffer,
};

use super::{
    indexer::{EntryAddress, Indexer},
    reclaimer::Reinsertion,
    serde::Sequence,
    tombstone::Tombstone,
};

pub struct Allocation {
    _guard: WaitGroupGuard,
    slice: ManuallyDrop<Box<[u8]>>,
}

impl Deref for Allocation {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.slice.as_ref()
    }
}

impl DerefMut for Allocation {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.slice.as_mut()
    }
}

impl Allocation {
    unsafe fn new(buffer: &mut [u8], guard: WaitGroupGuard) -> Self {
        let fake = Vec::from_raw_parts(buffer.as_mut_ptr(), buffer.len(), buffer.len());
        let slice = ManuallyDrop::new(fake.into_boxed_slice());
        Self { _guard: guard, slice }
    }
}

pub struct BatchMut<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    buffer: IoBuffer,
    len: usize,
    groups: Vec<GroupMut<K, V, S>>,
    tombstones: Vec<TombstoneInfo>,
    init: Option<Instant>,
    wait: WaitGroup,

    /// Cache write buffer between rotation to reduce page fault.
    buffer_pool: Option<IoBytes>,

    region_manager: RegionManager,
    device: MonitoredDevice,
    indexer: Indexer,
}

impl<K, V, S> Debug for BatchMut<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchMut")
            .field("len", &self.len)
            .field("groups", &self.groups)
            .field("tombstones", &self.tombstones)
            .field("init", &self.init)
            .finish()
    }
}

impl<K, V, S> BatchMut<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    pub fn new(capacity: usize, region_manager: RegionManager, device: MonitoredDevice, indexer: Indexer) -> Self {
        let mut batch = Self {
            buffer: IoBuffer::new(capacity),
            len: 0,
            groups: vec![],
            tombstones: vec![],
            init: None,
            wait: WaitGroup::default(),
            buffer_pool: Some(IoBytes::from(IoBuffer::new(capacity))),
            region_manager,
            device,
            indexer,
        };
        batch.append_group();
        batch
    }

    pub fn entry(
        &mut self,
        size: usize,
        entry: CacheEntry<K, V, S>,
        tx: oneshot::Sender<Result<bool>>,
        sequence: Sequence,
    ) -> Option<Allocation> {
        tracing::trace!("[batch]: append entry with sequence: {sequence}");

        let aligned = bits::align_up(self.device.align(), size);

        if entry.is_outdated() || self.len + aligned > self.buffer.len() {
            let _ = tx.send(Ok(false));
            return None;
        }

        self.may_init();
        assert!(bits::is_aligned(self.device.align(), self.len));

        // Rotate group if the current one is full.
        let group = self.groups.last_mut().unwrap();
        if group.region.offset as usize + group.region.len + aligned > self.device.region_size() {
            group.region.is_full = true;
            self.append_group();
        }

        let group = self.groups.last_mut().unwrap();
        // Reserve buffer space for entry.
        let start = self.len;
        let end = start + aligned;
        self.len = end;
        group.indices.push(HashedEntryAddress {
            hash: entry.hash(),
            address: EntryAddress {
                region: RegionId::MAX,
                offset: group.region.offset as u32 + group.region.len as u32,
                len: size as _,
                sequence,
            },
        });
        group.txs.push(tx);
        group.entries.push(entry);
        group.region.len += aligned;
        group.range.end += aligned;

        let allocation = unsafe { Allocation::new(&mut self.buffer[start..end], self.wait.acquire()) };

        Some(allocation)
    }

    pub fn tombstone(&mut self, tombstone: Tombstone, stats: Option<InvalidStats>, tx: oneshot::Sender<Result<bool>>) {
        tracing::trace!("[batch]: append tombstone");
        self.may_init();
        self.tombstones.push(TombstoneInfo { tombstone, stats, tx });
    }

    // FIXME(MrCroxx): merge into `entry`. Rename to allocate (?).
    pub fn reinsertion(&mut self, reinsertion: &Reinsertion, tx: oneshot::Sender<Result<bool>>) -> Option<Allocation> {
        tracing::trace!("[batch]: submit reinsertion");

        let aligned = bits::align_up(self.device.align(), reinsertion.buffer.len());

        // Skip if the entry is no longer in the indexer.
        // Skip if the batch buffer size exceeds the threshold.
        if self.indexer.get(reinsertion.hash).is_none() || self.len + aligned > self.buffer.len() {
            let _ = tx.send(Ok(false));
            return None;
        }

        self.may_init();
        assert!(bits::is_aligned(self.device.align(), self.len));

        // Rotate group if the current one is full.
        let group = self.groups.last_mut().unwrap();
        if group.region.offset as usize + group.region.len + aligned > self.device.region_size() {
            group.region.is_full = true;
            self.append_group();
        }

        let group = self.groups.last_mut().unwrap();
        // Reserve buffer space for entry.
        let start = self.len;
        let end = start + aligned;
        self.len = end;
        group.indices.push(HashedEntryAddress {
            hash: reinsertion.hash,
            address: EntryAddress {
                region: RegionId::MAX,
                offset: group.region.offset as u32 + group.region.len as u32,
                len: reinsertion.buffer.len() as _,
                sequence: reinsertion.sequence,
            },
        });
        group.txs.push(tx);
        group.region.len += aligned;
        group.range.end += aligned;

        let allocation = unsafe { Allocation::new(&mut self.buffer[start..end], self.wait.acquire()) };

        Some(allocation)
    }

    /// Rotate the batch.
    ///
    /// # Panics
    ///
    /// The caller must guarantee all io bytes from the last batch are dropped.
    pub fn rotate(&mut self) -> Option<(Batch<K, V, S>, WaitGroupFuture)> {
        if self.is_empty() {
            return None;
        }

        let buffer = self.buffer_pool.take().unwrap();
        let mut buffer = buffer.try_into_io_buffer().unwrap();

        std::mem::swap(&mut self.buffer, &mut buffer);
        self.len = 0;
        let buffer = IoBytes::from(buffer);
        self.buffer_pool = Some(buffer.clone());

        let wait = std::mem::take(&mut self.wait);

        let init = self.init.take();

        let tombstones = std::mem::take(&mut self.tombstones);

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
                txs: vec![],
                entries: vec![],
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
                    txs: group.txs,
                    entries: group.entries,
                }
            })
            .collect_vec();

        match next {
            Some(next) => self.groups.push(next),
            None => self.append_group(),
        }

        Some((
            Batch {
                groups,
                tombstones,
                init,
            },
            wait.wait(),
        ))
    }

    fn is_empty(&self) -> bool {
        self.tombstones.is_empty() && self.groups.iter().all(|group| group.range.is_empty())
    }

    fn may_init(&mut self) {
        if self.init.is_none() {
            self.init = Some(Instant::now());
        }
    }

    fn append_group(&mut self) {
        self.groups.push(GroupMut {
            region: RegionHandle {
                handle: self.region_manager.get_clean_region(),
                offset: 0,
                len: 0,
                is_full: false,
            },
            indices: vec![],
            txs: vec![],
            entries: vec![],
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
    /// Offoset of the region to write.
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

struct GroupMut<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    /// Reusable Clean region handle.
    region: RegionHandle,
    /// Entry indices to be inserted.
    indices: Vec<HashedEntryAddress>,
    /// Writer notify tx.
    txs: Vec<oneshot::Sender<Result<bool>>>,
    /// Hold entries until flush finishes to avoid in-memory cache lookup miss.
    entries: Vec<CacheEntry<K, V, S>>,
    /// Tracks the group bytes range of the batch buffer.
    range: Range<usize>,
}

impl<K, V, S> Debug for GroupMut<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Group")
            .field("handle", &self.region)
            .field("indices", &self.indices)
            .field("range", &self.range)
            .finish()
    }
}

pub struct Group<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    /// Reusable Clean region handle.
    pub region: RegionHandle,
    /// Buffer to flush.
    pub bytes: IoBytes,
    /// Entry indices to be inserted.
    pub indices: Vec<HashedEntryAddress>,
    /// Writer notify tx.
    pub txs: Vec<oneshot::Sender<Result<bool>>>,
    /// Hold entries until flush finishes to avoid in-memory cache lookup miss.
    pub entries: Vec<CacheEntry<K, V, S>>,
}

impl<K, V, S> Debug for Group<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
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
    pub tx: oneshot::Sender<Result<bool>>,
}

pub struct Batch<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    pub groups: Vec<Group<K, V, S>>,
    pub tombstones: Vec<TombstoneInfo>,
    pub init: Option<Instant>,
}

impl<K, V, S> Debug for Batch<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Batch")
            .field("groups", &self.groups)
            .field("tombstones", &self.tombstones)
            .field("init", &self.init)
            .finish()
    }
}
