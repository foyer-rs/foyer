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

use std::fmt::Debug;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use crate::compress::Compression;
use crate::device::allocator::WritableVecA;
use crate::device::monitor::Monitored;
use crate::error::{Error, Result};
use crate::serde::EntrySerializer;
use crate::statistics::Statistics;
use crate::Sequence;

use foyer_common::async_batch_pipeline::{AsyncBatchPipeline, LeaderToken};
use foyer_common::code::{HashBuilder, StorageKey, StorageValue};
use foyer_common::metrics::Metrics;
use foyer_common::{bits, strict_assert_eq};
use foyer_memory::CacheEntry;
use futures::future::{try_join, try_join_all};

use tokio::runtime::Handle;
use tokio::sync::oneshot;

use super::indexer::{EntryAddress, Indexer};
use super::reclaimer::Reinsertion;
use crate::device::{Device, DeviceExt, IoBuffer, RegionId, IO_BUFFER_ALLOCATOR};
use crate::large::generic::GenericStoreConfig;
use crate::region::{GetCleanRegionHandle, RegionManager};
use crate::tombstone::{Tombstone, TombstoneLog};

struct BatchState<K, V, S, D>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
    D: Device,
{
    groups: Vec<WriteGroup<K, V, S, D>>,
    tombstones: Vec<(Tombstone, Option<InvalidStats>, oneshot::Sender<Result<bool>>)>,
    init_time: Option<Instant>,
}

impl<K, V, S, D> Debug for BatchState<K, V, S, D>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
    D: Device,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchState").field("groups", &self.groups).finish()
    }
}

impl<K, V, S, D> Default for BatchState<K, V, S, D>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
    D: Device,
{
    fn default() -> Self {
        Self {
            groups: vec![],
            tombstones: vec![],
            init_time: None,
        }
    }
}

struct WriteGroup<K, V, S, D>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder,
    D: Device,
{
    writer: RegionHandle<D>,

    buffer: IoBuffer,
    indices: Vec<(u64, EntryAddress)>,
    txs: Vec<oneshot::Sender<Result<bool>>>,
    // hold the entries to avoid memory cache lookup miss?
    entries: Vec<CacheEntry<K, V, S>>,
}

impl<K, V, S, D> Debug for WriteGroup<K, V, S, D>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder,
    D: Device,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteGroup")
            .field("writer", &self.writer)
            .field("buffer_len", &self.buffer.len())
            .field("indices", &self.indices)
            .finish()
    }
}

struct RegionHandle<D>
where
    D: Device,
{
    handle: GetCleanRegionHandle<D>,
    offset: u64,
    size: usize,
    is_full: bool,
}

impl<D> Debug for RegionHandle<D>
where
    D: Device,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegionHandle")
            .field("offset", &self.offset)
            .field("size", &self.size)
            .field("is_full", &self.is_full)
            .finish()
    }
}

impl<D> Clone for RegionHandle<D>
where
    D: Device,
{
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            offset: self.offset,
            size: self.size,
            is_full: self.is_full,
        }
    }
}

#[derive(Debug)]
pub enum Submission<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    CacheEntry {
        entry: CacheEntry<K, V, S>,
        tx: oneshot::Sender<Result<bool>>,
    },
    Tombstone {
        tombstone: Tombstone,
        stats: Option<InvalidStats>,
        tx: oneshot::Sender<Result<bool>>,
    },
    Reinsertion {
        reinsertion: Reinsertion,
        tx: oneshot::Sender<Result<bool>>,
    },
}

#[derive(Debug)]
pub struct InvalidStats {
    pub region: RegionId,
    pub size: usize,
}

#[derive(Debug)]
pub struct Flusher<K, V, S, D>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
    D: Device,
{
    batch: AsyncBatchPipeline<BatchState<K, V, S, D>, Result<()>>,
    indexer: Indexer,
    region_manager: RegionManager<D>,

    device: Monitored<D>,
    tombstone_log: Option<TombstoneLog>,

    stats: Arc<Statistics>,

    compression: Compression,
    flush: bool,

    metrics: Arc<Metrics>,
}

impl<K, V, S, D> Clone for Flusher<K, V, S, D>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
    D: Device,
{
    fn clone(&self) -> Self {
        Self {
            batch: self.batch.clone(),
            indexer: self.indexer.clone(),
            region_manager: self.region_manager.clone(),
            device: self.device.clone(),
            tombstone_log: self.tombstone_log.clone(),
            stats: self.stats.clone(),
            compression: self.compression,
            flush: self.flush,
            metrics: self.metrics.clone(),
        }
    }
}

impl<K, V, S, D> Flusher<K, V, S, D>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
    D: Device,
{
    // TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
    #[allow(clippy::too_many_arguments)]
    pub async fn open(
        config: &GenericStoreConfig<K, V, S, D>,
        indexer: Indexer,
        region_manager: RegionManager<D>,
        device: Monitored<D>,
        tombstone_log: Option<TombstoneLog>,
        stats: Arc<Statistics>,
        metrics: Arc<Metrics>,
        runtime: Handle,
    ) -> Result<Self> {
        let batch = AsyncBatchPipeline::with_runtime(BatchState::default(), runtime);

        Ok(Self {
            batch,
            indexer,
            region_manager,
            device,
            tombstone_log,
            stats,
            compression: config.compression,
            flush: config.flush,
            metrics,
        })
    }

    pub fn submit(&self, submission: Submission<K, V, S>, sequence: Sequence) {
        match submission {
            Submission::CacheEntry { entry, tx } => self.entry(entry, tx, sequence),
            Submission::Tombstone { tombstone, stats, tx } => self.tombstone(tombstone, stats, tx, sequence),
            Submission::Reinsertion { reinsertion, tx } => self.reinsertion(reinsertion, tx, sequence),
        }
    }

    fn tombstone(
        &self,
        tombstone: Tombstone,
        stats: Option<InvalidStats>,
        tx: oneshot::Sender<Result<bool>>,
        sequence: Sequence,
    ) {
        tracing::trace!("[flusher]: submit tombstone with sequence: {sequence}");

        strict_assert_eq!(tombstone.sequence, sequence);

        let append = |state: &mut BatchState<K, V, S, D>| {
            state.tombstones.push((tombstone, stats, tx));
            if state.init_time.is_none() {
                state.init_time = Some(Instant::now());
            }
        };

        if let Some(token) = self.batch.accumulate(append) {
            self.metrics.storage_queue_leader.increment(1);
            tracing::trace!("[flusher]: tombstone with sequence: {sequence} becomes leader");
            self.commit(token);
        } else {
            self.metrics.storage_queue_follower.increment(1);
        }
    }

    fn entry(&self, entry: CacheEntry<K, V, S>, tx: oneshot::Sender<Result<bool>>, sequence: Sequence) {
        tracing::trace!("[flusher]: submit entry with sequence: {sequence}");

        let append = |state: &mut BatchState<K, V, S, D>| {
            if entry.is_outdated() {
                let _ = tx.send(Ok(false));
                return;
            }

            self.may_init_batch_state(state);
            if state.init_time.is_none() {
                state.init_time = Some(Instant::now());
            }

            // Attempt to pick the latest group to write.
            let group = state.groups.last_mut().unwrap();

            // Write to the latest group and check aligns.
            bits::debug_assert_aligned(self.device.align(), group.buffer.len());
            let mut boffset = group.buffer.len();

            if let Err(e) = EntrySerializer::serialize(
                entry.key(),
                entry.value(),
                entry.hash(),
                &sequence,
                &self.compression,
                WritableVecA(&mut group.buffer),
            ) {
                let _ = tx.send(Err(e));
                return;
            }

            let len = group.buffer.len() - boffset;
            let aligned = bits::align_up(self.device.align(), len);
            group.buffer.reserve(aligned - len);
            unsafe { group.buffer.set_len(boffset + aligned) };

            bits::debug_assert_aligned(self.device.align(), group.buffer.len());
            tracing::trace!(
                "[flusher]: sequence: {sequence}, len: {len}, aligned: {aligned}, buf len: {buf_len}",
                buf_len = group.buffer.len()
            );

            // Split the latest group if it exceeds the current region.
            if group.writer.offset as usize + group.buffer.len() > self.device.region_size() {
                tracing::trace!("[flusher]: (sequence: {sequence}) split group at size: {size}, buf len: {buf_len}, total (if not split): {total}, exceeds region size: {region_size}", 
                    size = group.writer.size,
                    buf_len = group.buffer.len(),
                    total = group.writer.offset as usize + group.buffer.len() ,
                    region_size = self.device.region_size(),
                );

                group.writer.is_full = true;

                let buffer = group.buffer.split_off(boffset);

                bits::debug_assert_aligned(self.device.align(), group.buffer.len());
                bits::debug_assert_aligned(self.device.align(), buffer.len());

                self.append_groups_with_buffer(state, buffer);
                boffset = 0;
            }

            // Re-reference the latest group in case it may be out-dated.
            let group = state.groups.last_mut().unwrap();
            group.indices.push((
                entry.hash(),
                EntryAddress {
                    region: RegionId::MAX,
                    offset: group.writer.offset as u32 + boffset as u32,
                    len: len as _,
                    sequence,
                },
            ));
            group.txs.push(tx);
            group.entries.push(entry);
            group.writer.size += aligned;
        };

        if let Some(token) = self.batch.accumulate(append) {
            self.metrics.storage_queue_leader.increment(1);
            tracing::trace!("[flusher]: entry with sequence: {sequence} becomes leader");
            self.commit(token);
        } else {
            self.metrics.storage_queue_follower.increment(1);
        }
    }

    fn reinsertion(&self, mut reinsertion: Reinsertion, tx: oneshot::Sender<Result<bool>>, sequence: Sequence) {
        tracing::trace!("[flusher]: submit reinsertion with sequence: {sequence}");
        strict_assert_eq!(sequence, 0);

        let append = |state: &mut BatchState<K, V, S, D>| {
            self.may_init_batch_state(state);
            if state.init_time.is_none() {
                state.init_time = Some(Instant::now());
            }

            if self.indexer.get(reinsertion.hash).is_none() {
                let _ = tx.send(Ok(false));
                return;
            }

            // Attempt to pick the latest group to write.
            let group = state.groups.last_mut().unwrap();
            bits::debug_assert_aligned(self.device.align(), group.buffer.len());

            // Rotate group early for we know the len of the buffer to write.
            let aligned = bits::align_up(self.device.align(), reinsertion.buffer.len());
            if group.writer.offset as usize + group.buffer.len() + aligned > self.device.region_size() {
                tracing::trace!("[flusher]: split group at size: {size}, acc buf len: {acc_buf_len}, buf len: {buf_len}, total (if not split): {total}, exceeds region size: {region_size}",
                    size = group.writer.size,
                    acc_buf_len = group.buffer.len(),
                    buf_len = reinsertion.buffer.len(),
                    total = group.writer.offset as usize + group.buffer.len() + reinsertion.buffer.len(),
                    region_size = self.device.region_size(),
                );

                group.writer.is_full = true;

                bits::debug_assert_aligned(self.device.align(), group.buffer.len());

                self.append_groups(state);
            }

            // Re-reference the latest group in case it may be out-dated.
            let group = state.groups.last_mut().unwrap();
            let boffset = group.buffer.len();
            let len = reinsertion.buffer.len();
            group.buffer.reserve(aligned);
            group.buffer.append(&mut reinsertion.buffer);
            unsafe { group.buffer.set_len(boffset + aligned) };
            group.indices.push((
                reinsertion.hash,
                EntryAddress {
                    region: RegionId::MAX,
                    offset: group.writer.offset as u32 + boffset as u32,
                    len: len as _,
                    sequence: reinsertion.sequence,
                },
            ));
            group.txs.push(tx);
            group.writer.size += aligned;
        };

        if let Some(token) = self.batch.accumulate(append) {
            self.metrics.storage_queue_leader.increment(1);
            tracing::trace!("[flusher]: reinsertion with sequence: {sequence} becomes leader");
            self.commit(token);
        } else {
            self.metrics.storage_queue_follower.increment(1);
        }
    }

    fn commit(&self, token: LeaderToken<BatchState<K, V, S, D>, Result<()>>) {
        let indexer = self.indexer.clone();
        let flush = self.flush;
        let region_manager = self.region_manager.clone();
        let tombstone_log: Option<TombstoneLog> = self.tombstone_log.clone();
        let stats = self.stats.clone();
        let metrics = self.metrics.clone();
        let device = self.device.clone();

        token.pipeline(
            move |state| {
                tracing::trace!("[flusher]: create new state based on old state: {state:?}");

                let mut s = BatchState::default();
                if let Some(group) = state.groups.last() {
                    let mut writer = group.writer.clone();
                    strict_assert_eq!(
                        writer.offset as usize + group.buffer.len(),
                        writer.size,
                        "offset ({offset}) + buffer len ({buf_len}) = total ({total}) != size ({size})",
                        offset = writer.offset,
                        buf_len = group.buffer.len(),
                        total = writer.offset as usize + group.buffer.len(),
                        size = writer.size,
                    );
                    writer.offset = writer.size as u64;
                    s.groups.push(WriteGroup {
                        writer,
                        buffer: IoBuffer::with_capacity_in(device.region_size(), &IO_BUFFER_ALLOCATOR),
                        indices: vec![],
                        txs: vec![],
                        entries: vec![],
                    });
                }
                s
            },
            |res| match res {
                Ok(_) => {}
                Err(e) => tracing::error!("batch pipeline error: {e}"),
            },
            move |state| async move {
                // Write regions concurrently.
                let futures = state.groups.into_iter().map(|group| {
                    let indexer = indexer.clone();
                    let region_manager = region_manager.clone();
                    let stats = stats.clone();
                    async move {
                        // Wait for region is clean.
                        let region = group.writer.handle.await;

                        tracing::trace!(
                            "[flusher]: write region: {id}, at offset: {offset}, buffer len: {buf_len}",
                            id = region.id(),
                            offset = group.writer.size,
                            buf_len = group.buffer.len(),
                        );

                        // Write buffet to device.
                        if !group.buffer.is_empty() {
                            let aligned = group.buffer.len();

                            region.write(group.buffer, group.writer.offset).await?;
                            if flush {
                                region.flush().await?;
                            }

                            stats.cache_write_bytes.fetch_add(aligned, Ordering::Relaxed);

                            let mut indices = group.indices;
                            for (_, addr) in indices.iter_mut() {
                                addr.region = region.id();
                            }
                            indexer.insert_batch(indices);

                            for tx in group.txs {
                                let _ = tx.send(Ok(true));
                            }
                        }

                        if group.writer.is_full {
                            region_manager.mark_evictable(region.id());
                        }

                        // Make sure entries are dropped after written.
                        drop(group.entries);

                        tracing::trace!("[flusher]: write region {id} finish.", id = region.id());

                        Ok::<_, Error>(())
                    }
                });
                let future = {
                    let tombstones = state.tombstones;
                    let has_tombstone_log = tombstone_log.is_some();
                    let region_manager = region_manager.clone();
                    async move {
                        if let Some(log) = tombstone_log {
                            log.append(tombstones.iter().map(|(tombstone, _, _)| tombstone)).await?;
                        }
                        for (_, stats, tx) in tombstones {
                            if let Some(stats) = stats {
                                region_manager
                                    .region(stats.region)
                                    .stats()
                                    .invalid
                                    .fetch_add(stats.size, Ordering::Relaxed);
                            }
                            let _ = tx.send(Ok(has_tombstone_log));
                        }
                        Ok::<_, Error>(())
                    }
                };
                try_join(try_join_all(futures), future).await?;

                if let Some(init_time) = state.init_time.as_ref() {
                    metrics.storage_queue_rotate.increment(1);
                    metrics.storage_queue_rotate_duration.record(init_time.elapsed());
                }

                Ok(())
            },
        );
    }

    /// Wait for the current batch to finish.
    pub async fn wait(&self) -> Result<()> {
        if let Some(handle) = self.batch.wait() {
            match handle.await.unwrap() {
                Ok(_) => {}
                Err(e) => tracing::error!("batch pipeline error: {e}"),
            }
        }
        Ok(())
    }

    /// Initialize the batch state if needed.
    fn may_init_batch_state(&self, state: &mut BatchState<K, V, S, D>) {
        if state.groups.is_empty() {
            self.append_groups(state);
        }
    }

    fn append_groups(&self, state: &mut BatchState<K, V, S, D>) {
        self.append_groups_with_buffer(
            state,
            IoBuffer::with_capacity_in(self.device.region_size(), &IO_BUFFER_ALLOCATOR),
        );
    }

    fn append_groups_with_buffer(&self, state: &mut BatchState<K, V, S, D>, buffer: IoBuffer) {
        let handle = self.region_manager.get_clean_region();
        state.groups.push(WriteGroup {
            writer: RegionHandle {
                handle,
                offset: 0,
                size: 0,
                is_full: false,
            },
            buffer,
            indices: vec![],
            txs: vec![],
            entries: vec![],
        });
    }
}
