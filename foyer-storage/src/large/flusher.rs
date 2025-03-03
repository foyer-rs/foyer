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
    fmt::Debug,
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use foyer_common::{
    bits,
    code::{StorageKey, StorageValue},
    metrics::Metrics,
};
use foyer_memory::Piece;
use futures_util::future::{try_join, try_join_all};
use itertools::Itertools;
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};

use super::{
    batch::{Batch, BatchWriter, EntryWriter, Op},
    generic::GenericLargeStorageConfig,
    indexer::Indexer,
    reclaimer::Reinsertion,
    serde::Sequence,
    tombstone::{Tombstone, TombstoneLog},
};
#[cfg(test)]
use crate::large::test_utils::*;
use crate::{
    device::{MonitoredDevice, RegionId},
    error::{Error, Result},
    io::{IoBuffer, PAGE},
    large::indexer::{EntryAddress, HashedEntryAddress},
    region::{GetCleanRegionHandle, RegionManager},
    runtime::Runtime,
    Compression, Dev, Statistics,
};

pub enum Submission<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    CacheEntry {
        piece: Piece<K, V>,
        estimated_size: usize,
        sequence: Sequence,
    },
    Tombstone {
        tombstone: Tombstone,
        stats: Option<InvalidStats>,
    },
    Reinsertion {
        reinsertion: Reinsertion,
    },
    Wait {
        tx: oneshot::Sender<()>,
    },
}

impl<K, V> Debug for Submission<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CacheEntry {
                piece,
                estimated_size,
                sequence,
            } => f
                .debug_struct("CacheEntry")
                .field("piece", piece)
                .field("estimated_size", estimated_size)
                .field("sequence", sequence)
                .finish(),
            Self::Tombstone { tombstone, stats } => f
                .debug_struct("Tombstone")
                .field("tombstone", tombstone)
                .field("stats", stats)
                .finish(),
            Self::Reinsertion { reinsertion } => {
                f.debug_struct("Reinsertion").field("reinsertion", reinsertion).finish()
            }
            Self::Wait { .. } => f.debug_struct("Wait").finish(),
        }
    }
}

#[derive(Debug)]
pub struct Flusher<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    tx: flume::Sender<Submission<K, V>>,
    submit_queue_size: Arc<AtomicUsize>,

    metrics: Arc<Metrics>,
}

impl<K, V> Clone for Flusher<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            submit_queue_size: self.submit_queue_size.clone(),
            metrics: self.metrics.clone(),
        }
    }
}

impl<K, V> Flusher<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    #[expect(clippy::too_many_arguments)]
    pub fn open(
        config: &GenericLargeStorageConfig<K, V>,
        indexer: Indexer,
        region_manager: RegionManager,
        device: MonitoredDevice,
        submit_queue_size: Arc<AtomicUsize>,
        tombstone_log: Option<TombstoneLog>,
        stats: Arc<Statistics>,
        metrics: Arc<Metrics>,
        runtime: &Runtime,
        #[cfg(test)] flush_holder: FlushHolder,
    ) -> Result<Self> {
        let (tx, rx) = flume::unbounded();

        let buffer_size = config.buffer_pool_size / config.flushers;
        let writer = create_writer(buffer_size, device.region_size(), device.region_size(), metrics.clone());
        let writer = Some(writer);

        let current_region_state = CleanRegionState {
            handle: region_manager.get_clean_region(),
            remain: device.region_size(),
        };

        let runner = Runner {
            rx,
            writer,
            tombstone_infos: vec![],
            waiters: vec![],
            buffer_size,
            flight: Arc::new(Semaphore::new(1)),
            submit_queue_size: submit_queue_size.clone(),
            current_region_state,
            region_manager,
            device,
            indexer,
            tombstone_log,
            compression: config.compression,
            flush: config.flush,
            stats,
            metrics: metrics.clone(),
            #[cfg(test)]
            flush_holder,
        };

        runtime.write().spawn(async move {
            if let Err(e) = runner.run().await {
                tracing::error!("[flusher]: flusher exit with error: {e}");
            }
        });

        Ok(Self {
            tx,
            submit_queue_size,
            metrics,
        })
    }

    pub fn submit(&self, submission: Submission<K, V>) {
        tracing::trace!("[lodc flusher]: submit task: {submission:?}");
        if let Submission::CacheEntry { estimated_size, .. } = &submission {
            self.submit_queue_size.fetch_add(*estimated_size, Ordering::Relaxed);
        }
        if let Err(e) = self.tx.send(submission) {
            tracing::error!("[lodc flusher]: error raised when submitting task, error: {e}");
        }
    }

    pub fn wait(&self) -> impl Future<Output = ()> + Send + 'static {
        let (tx, rx) = oneshot::channel();
        self.submit(Submission::Wait { tx });
        async move {
            let _ = rx.await;
        }
    }
}

fn create_writer(
    capacity: usize,
    region_size: usize,
    current_region_remain: usize,
    metrics: Arc<Metrics>,
) -> BatchWriter {
    // TODO(MrCroxx): optimize buffer allocation.
    BatchWriter::new(IoBuffer::new(capacity), region_size, current_region_remain, metrics)
}

#[derive(Debug, Clone)]
struct CleanRegionState {
    handle: GetCleanRegionHandle,
    remain: usize,
}

#[derive(Debug)]
pub struct InvalidStats {
    pub region: RegionId,
    pub size: usize,
}

#[derive(Debug)]
pub struct TombstoneInfo {
    pub tombstone: Tombstone,
    pub stats: Option<InvalidStats>,
}

struct Runner<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    rx: flume::Receiver<Submission<K, V>>,

    // NOTE: writer is always `Some(..)`.
    writer: Option<BatchWriter>,
    tombstone_infos: Vec<TombstoneInfo>,
    waiters: Vec<oneshot::Sender<()>>,

    flight: Arc<Semaphore>,
    submit_queue_size: Arc<AtomicUsize>,

    current_region_state: CleanRegionState,
    buffer_size: usize,

    region_manager: RegionManager,
    indexer: Indexer,
    tombstone_log: Option<TombstoneLog>,

    compression: Compression,
    flush: bool,

    device: MonitoredDevice,

    stats: Arc<Statistics>,
    metrics: Arc<Metrics>,

    #[cfg(test)]
    flush_holder: FlushHolder,
}

impl<K, V> Runner<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn dirty(&self) -> bool {
        !self.writer.as_ref().unwrap().is_empty() || !self.tombstone_infos.is_empty() || !self.waiters.is_empty()
    }

    pub async fn run(mut self) -> Result<()> {
        loop {
            let flight = self.flight.clone();

            #[cfg(not(test))]
            let can_flush = true;
            #[cfg(test)]
            let can_flush = !self.flush_holder.is_held() && self.rx.is_empty();

            tokio::select! {
                biased;
                Ok(permit) = flight.acquire_owned(), if self.dirty() && can_flush => {
                    let (buf, batch) = self.writer.take().unwrap().finish();

                    let tombstone_infos = std::mem::take(&mut self.tombstone_infos);
                    let waiters = std::mem::take(&mut self.waiters);

                    self.commit(buf, batch, tombstone_infos, waiters, permit).await;

                    let writer = create_writer(
                        self.buffer_size,
                        self.device.region_size(),
                        self.current_region_state.remain,
                        self.metrics.clone()
                    );
                    self.writer = Some(writer);

                }
                Ok(submission) = self.rx.recv_async() => {
                    self.submit(submission);
                }
                // Graceful shutdown.
                else => break,
            }
        }
        Ok(())
    }

    fn submit(&mut self, submission: Submission<K, V>) {
        tracing::trace!(?submission, "[lodc flush runner]: recv submission");

        let report = |op: Op| {
            if matches! {op, Op::Skip} {
                self.metrics.storage_queue_drop.increase(1);
            }
        };

        match submission {
            Submission::CacheEntry {
                piece,
                estimated_size,
                sequence,
            } => {
                report(self.writer.as_mut().unwrap().push(
                    piece.key(),
                    piece.value(),
                    piece.hash(),
                    self.compression,
                    sequence,
                ));
                self.submit_queue_size.fetch_sub(estimated_size, Ordering::Relaxed);
            }

            Submission::Tombstone { tombstone, stats } => self.tombstone_infos.push(TombstoneInfo { tombstone, stats }),
            Submission::Reinsertion { reinsertion } => {
                // Skip reinsertion if the entry is not in the indexer.
                if self.indexer.get(reinsertion.hash).is_some() {
                    report(self.writer.as_mut().unwrap().push_slice(
                        &reinsertion.slice,
                        reinsertion.hash,
                        reinsertion.sequence,
                    ));
                }
            }
            Submission::Wait { tx } => self.waiters.push(tx),
        }
    }

    async fn commit(
        &mut self,
        buf: IoBuffer,
        batch: Batch,
        tombstone_infos: Vec<TombstoneInfo>,
        waiters: Vec<oneshot::Sender<()>>,
        permit: OwnedSemaphorePermit,
    ) {
        tracing::trace!(
            ?batch,
            ?tombstone_infos,
            waiters = waiters.len(),
            "[flusher] commit batch"
        );

        let shared = buf.into_shared_io_slice();

        let region_states_iter =
            std::iter::once(self.current_region_state.clone()).chain((0..batch.windows.len() - 1).map(|_| {
                CleanRegionState {
                    handle: self.region_manager.get_clean_region(),
                    remain: self.device.region_size(),
                }
            }));

        let window_count = batch.windows.len();

        tracing::trace!(current_region_state = ?self.current_region_state,"[lodc flusher]: pre flush");

        // Write regions concurrently.
        let futures =
            batch
                .windows
                .into_iter()
                .zip_eq(region_states_iter)
                .enumerate()
                .map(|(i, (window, mut region_state))| {
                    let indexer = self.indexer.clone();
                    let region_manager = self.region_manager.clone();
                    let stats = self.stats.clone();
                    let flush = self.flush;
                    let slice = shared.absolute_slice(window.absolute_dirty_range.clone());

                    async move {
                        // Wait for region is clean.
                        let region = region_state.handle.clone().await;

                        let offset = region.size() - region_state.remain;
                        let len = slice.len();

                        bits::assert_aligned(PAGE, offset);
                        bits::assert_aligned(PAGE, len);

                        tracing::trace!(region = region.id(), offset, len, "[flusher]: prepare to write region");

                        if !window.is_empty() {
                            let (_, res) = region.write(slice, offset as _).await;
                            res?;

                            if flush {
                                region.flush().await?;
                            }
                            stats.cache_write_bytes.fetch_add(len, Ordering::Relaxed);
                        } else {
                            tracing::trace!(
                                region = region.id(),
                                "[flusher]: skip write region, because the window is empty"
                            );
                        }

                        let mut addrs =
                            Vec::with_capacity(window.blobs.iter().map(|blob| blob.entry_indices.len()).sum());
                        let mut blob_offset = offset as u32;
                        for blob in window.blobs {
                            for index in blob.entry_indices {
                                let addr = HashedEntryAddress {
                                    hash: index.hash,
                                    address: EntryAddress {
                                        region: region.id(),
                                        offset: blob_offset + index.offset,
                                        len: index.len,
                                        sequence: index.sequence,
                                    },
                                };
                                addrs.push(addr);
                            }
                            blob_offset += blob.size as u32;
                        }
                        indexer.insert_batch(addrs);

                        region_state.remain -= len;

                        // Window expect window is full, make it evictable.
                        if i != window_count - 1 {
                            region_manager.mark_evictable(region.id());
                        }
                        tracing::trace!("[flusher]: write region {id} finish.", id = region.id());

                        // region_state.remain -= len;
                        // if region_state.remain == 0 {
                        //     region_manager.mark_evictable(region.id());
                        // }
                        Ok::<_, Error>(region_state)
                    }
                });

        let future = {
            let region_manager = self.region_manager.clone();
            let tombstone_log = self.tombstone_log.clone();
            async move {
                if let Some(log) = tombstone_log {
                    log.append(tombstone_infos.iter().map(|info| &info.tombstone)).await?;
                }
                for TombstoneInfo { tombstone: _, stats } in tombstone_infos {
                    if let Some(stats) = stats {
                        region_manager
                            .region(stats.region)
                            .stats()
                            .invalid
                            .fetch_add(stats.size, Ordering::Relaxed);
                    }
                }
                Ok::<_, Error>(())
            }
        };

        let state = match try_join(try_join_all(futures), future).await {
            Ok((mut states, _)) => states.pop(),
            Err(e) => {
                tracing::error!("[flusher]: error raised when committing batch, error: {e}");
                None
            }
        };

        if let Some(state) = state {
            self.current_region_state = state;
        }

        for waiter in waiters {
            let _ = waiter.send(());
        }

        // TODO(MrCroxx): Fix the metrics.
        // if let Some(init) = batch.init.as_ref() {
        //     self.metrics.storage_queue_rotate.increase(1);
        //     self.metrics
        //         .storage_queue_rotate_duration
        //         .record(init.elapsed().as_secs_f64());
        // }

        drop(permit);
    }
}
