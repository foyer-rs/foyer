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

use crate::{
    device::{MonitoredDevice, RegionId},
    error::{Error, Result},
    large::serde::EntryHeader,
    region::{GetCleanRegionHandle, RegionManager},
    serde::{Checksummer, KvInfo},
    Compression, IoBytes, IoBytesMut, Statistics,
};
use foyer_common::{
    bits,
    code::{HashBuilder, StorageKey, StorageValue},
    metrics::Metrics,
    strict_assert, strict_assert_eq,
};
use foyer_memory::CacheEntry;
use futures::future::{try_join, try_join_all};
use std::{
    fmt::Debug,
    sync::{atomic::Ordering, Arc},
    time::Instant,
};
use tokio::{
    runtime::Handle,
    sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore},
};

use crate::{Dev, DevExt};

use super::{
    generic::GenericLargeStorageConfig,
    indexer::{EntryAddress, Indexer},
    reclaimer::Reinsertion,
    serde::Sequence,
    tombstone::{Tombstone, TombstoneLog},
};

#[derive(Debug)]
pub struct InvalidStats {
    pub region: RegionId,
    pub size: usize,
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
        buffer: IoBytes,
        info: KvInfo,
        sequence: Sequence,
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

struct WriteGroup<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder,
{
    writer: RegionHandle,

    buffer: IoBytesMut,
    indices: Vec<(u64, EntryAddress)>,
    txs: Vec<oneshot::Sender<Result<bool>>>,
    // hold the entries to avoid memory cache lookup miss?
    entries: Vec<CacheEntry<K, V, S>>,
}

impl<K, V, S> Debug for WriteGroup<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteGroup")
            .field("writer", &self.writer)
            .field("buffer_len", &self.buffer.len())
            .field("indices", &self.indices)
            .finish()
    }
}

struct RegionHandle {
    handle: GetCleanRegionHandle,
    offset: u64,
    size: usize,
    is_full: bool,
}

impl Debug for RegionHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegionHandle")
            .field("offset", &self.offset)
            .field("size", &self.size)
            .field("is_full", &self.is_full)
            .finish()
    }
}

impl Clone for RegionHandle {
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
pub struct Flusher<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    tx: mpsc::UnboundedSender<Submission<K, V, S>>,

    flight: Arc<Semaphore>,

    metrics: Arc<Metrics>,
}

impl<K, V, S> Clone for Flusher<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            flight: self.flight.clone(),
            metrics: self.metrics.clone(),
        }
    }
}

impl<K, V, S> Flusher<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    // TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
    #[allow(clippy::too_many_arguments)]
    pub async fn open(
        config: &GenericLargeStorageConfig<K, V, S>,
        indexer: Indexer,
        region_manager: RegionManager,
        device: MonitoredDevice,
        tombstone_log: Option<TombstoneLog>,
        stats: Arc<Statistics>,
        metrics: Arc<Metrics>,
        runtime: Handle,
    ) -> Result<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        let batch = Batch::default();
        let flight = Arc::new(Semaphore::new(1));

        let runner = Runner {
            batch,
            rx,
            region_manager,
            indexer,
            device,
            tombstone_log,
            compression: config.compression,
            flush: config.flush,
            stats,
            metrics: metrics.clone(),
            runtime: runtime.clone(),
            flight: flight.clone(),
            threshold: config.buffer_threshold / config.flushers,
        };

        runtime.spawn(async move {
            if let Err(e) = runner.run().await {
                tracing::error!("[flusher]: flusher exit with error: {e}");
            }
        });

        Ok(Self { tx, flight, metrics })
    }

    pub fn submit(&self, submission: Submission<K, V, S>) {
        if let Err(e) = self.tx.send(submission) {
            tracing::warn!("[flusher]: submit error, e: {e}");
        }
    }

    pub async fn wait(&self) -> Result<()> {
        // TODO(MrCroxx): Consider a better implementation?
        let _permit = self.flight.acquire().await;
        Ok(())
    }
}

struct Batch<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    groups: Vec<WriteGroup<K, V, S>>,
    tombstones: Vec<(Tombstone, Option<InvalidStats>, oneshot::Sender<Result<bool>>)>,
    init_time: Option<Instant>,
}

impl<K, V, S> Debug for Batch<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Batch").field("groups", &self.groups).finish()
    }
}

impl<K, V, S> Default for Batch<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn default() -> Self {
        Self {
            groups: vec![],
            tombstones: vec![],
            init_time: None,
        }
    }
}

impl<K, V, S> Batch<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn buffer_size(&self) -> usize {
        self.groups.iter().map(|group| group.buffer.len()).sum()
    }
}

struct Runner<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    batch: Batch<K, V, S>,

    rx: mpsc::UnboundedReceiver<Submission<K, V, S>>,

    region_manager: RegionManager,
    indexer: Indexer,
    device: MonitoredDevice,
    tombstone_log: Option<TombstoneLog>,

    compression: Compression,
    flush: bool,
    threshold: usize,

    stats: Arc<Statistics>,
    metrics: Arc<Metrics>,

    runtime: Handle,

    flight: Arc<Semaphore>,
}

impl<K, V, S> Runner<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    pub async fn run(mut self) -> Result<()> {
        loop {
            let flight = self.flight.clone();
            tokio::select! {
                biased;
                Ok(permit) = flight.acquire_owned(), if self.batch.init_time.is_some() => {
                    self.commit(permit);
                }
                Some(submission) = self.rx.recv() => {
                    self.submission(submission);
                }
                else => break,
            }
        }
        Ok(())
    }

    fn submission(&mut self, submission: Submission<K, V, S>) {
        match submission {
            Submission::CacheEntry {
                entry,
                buffer,
                info,
                sequence,
                tx,
            } => self.entry(entry, buffer, info, sequence, tx),
            Submission::Tombstone { tombstone, stats, tx } => self.tombstone(tombstone, stats, tx),
            Submission::Reinsertion { reinsertion, tx } => self.reinsertion(reinsertion, tx),
        }
    }

    fn tombstone(&mut self, tombstone: Tombstone, stats: Option<InvalidStats>, tx: oneshot::Sender<Result<bool>>) {
        tracing::trace!("[flusher]: submit tombstone");
        self.may_init_batch_state();
        self.batch.tombstones.push((tombstone, stats, tx));
    }

    fn entry(
        &mut self,
        entry: CacheEntry<K, V, S>,
        buffer: IoBytes,
        info: KvInfo,
        sequence: Sequence,
        tx: oneshot::Sender<Result<bool>>,
    ) {
        tracing::trace!("[flusher]: submit entry with sequence: {sequence}");

        // Skip if the entry is already outdated.
        // Skip if the batch buffer size exceeds the threshold.
        if entry.is_outdated() || self.batch.buffer_size() > self.threshold {
            let _ = tx.send(Ok(false));
            return;
        }

        self.may_init_batch_state();
        if self.batch.init_time.is_none() {
            self.batch.init_time = Some(Instant::now());
        }

        // Attempt to pick the latest group to write.
        let group = self.batch.groups.last_mut().unwrap();

        // Write to the latest group and check aligns.
        bits::debug_assert_aligned(self.device.align(), group.buffer.len());
        let mut boffset = group.buffer.len();

        let header = EntryHeader {
            key_len: info.key_len as _,
            value_len: info.value_len as _,
            hash: entry.hash(),
            sequence,
            checksum: Checksummer::checksum(&buffer),
            compression: self.compression,
        };

        group
            .buffer
            .reserve(bits::align_up(self.device.align(), header.entry_len()));
        // TODO(MrCroxx): impl `BufMut` for `WritableVecA` to avoid manually reservation?
        unsafe { group.buffer.set_len(boffset + EntryHeader::serialized_len()) };
        header.write(&mut group.buffer[boffset..]);
        group.buffer.extend_from_slice(&buffer);

        let len = group.buffer.len() - boffset;
        let aligned = bits::align_up(self.device.align(), len);
        strict_assert!(group.buffer.capacity() >= boffset + aligned);
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

            self.append_groups_with_buffer(buffer);
            boffset = 0;
        }

        // Re-reference the latest group in case it may be out-dated.
        let group = self.batch.groups.last_mut().unwrap();
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
    }

    fn reinsertion(&mut self, reinsertion: Reinsertion, tx: oneshot::Sender<Result<bool>>) {
        tracing::trace!("[flusher]: submit reinsertion");

        // Skip if the entry is no longer in the indexer.
        // Skip if the batch buffer size exceeds the threshold.
        if self.indexer.get(reinsertion.hash).is_none() || self.batch.buffer_size() > self.threshold {
            let _ = tx.send(Ok(false));
            return;
        }

        self.may_init_batch_state();
        if self.batch.init_time.is_none() {
            self.batch.init_time = Some(Instant::now());
        }

        // Attempt to pick the latest group to write.
        let group = self.batch.groups.last_mut().unwrap();
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

            self.append_groups();
        }

        // Re-reference the latest group in case it may be out-dated.
        let group = self.batch.groups.last_mut().unwrap();
        let boffset = group.buffer.len();
        let len = reinsertion.buffer.len();
        group.buffer.reserve(aligned);
        group.buffer.extend_from_slice(&reinsertion.buffer);
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
    }

    fn commit(&mut self, permit: OwnedSemaphorePermit) {
        // Rotate batch.
        tracing::trace!("[flusher]: create new batch based on old batch: {:?}", self.batch);

        let mut batch = Batch::default();

        if let Some(group) = self.batch.groups.last() {
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
            batch.groups.push(WriteGroup {
                writer,
                buffer: IoBytesMut::with_capacity(self.device.region_size()),
                indices: vec![],
                txs: vec![],
                entries: vec![],
            });
        }
        std::mem::swap(&mut self.batch, &mut batch);

        // Commit batch.

        let indexer = self.indexer.clone();
        let flush = self.flush;
        let region_manager = self.region_manager.clone();
        let tombstone_log: Option<TombstoneLog> = self.tombstone_log.clone();
        let stats = self.stats.clone();
        let metrics = self.metrics.clone();

        self.runtime.spawn(async move {
            // Write regions concurrently.
            let futures = batch.groups.into_iter().map(|group| {
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
                        region.write(group.buffer.freeze(), group.writer.offset).await?;
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
                let tombstones = batch.tombstones;
                let has_tombstone_log = tombstone_log.is_some();
                let region_manager = region_manager.clone();
                let tombstone_log = tombstone_log.clone();
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
            if let Some(init_time) = batch.init_time.as_ref() {
                metrics.storage_queue_rotate.increment(1);
                metrics.storage_queue_rotate_duration.record(init_time.elapsed());
            }
            drop(permit);
            Ok::<_, Error>(())
        });
    }

    /// Initialize the batch state if needed.
    fn may_init_batch_state(&mut self) {
        if self.batch.init_time.is_none() {
            self.batch.init_time = Some(Instant::now());
        }
        if self.batch.groups.is_empty() {
            self.append_groups();
        }
    }

    fn append_groups(&mut self) {
        self.append_groups_with_buffer(IoBytesMut::with_capacity(self.device.region_size()));
    }

    fn append_groups_with_buffer(&mut self, buffer: IoBytesMut) {
        let handle = self.region_manager.get_clean_region();
        self.batch.groups.push(WriteGroup {
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
