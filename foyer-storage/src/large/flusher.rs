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
    fmt::Debug,
    future::Future,
    sync::{atomic::Ordering, Arc},
};

use foyer_common::{
    code::{HashBuilder, StorageKey, StorageValue},
    metrics::Metrics,
};
use foyer_memory::CacheEntry;
use futures::future::{try_join, try_join_all};
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};

use super::{
    batch::{Batch, BatchMut, InvalidStats, TombstoneInfo},
    generic::GenericLargeStorageConfig,
    indexer::Indexer,
    reclaimer::Reinsertion,
    serde::Sequence,
    tombstone::{Tombstone, TombstoneLog},
};
use crate::{
    device::MonitoredDevice,
    error::{Error, Result},
    region::RegionManager,
    runtime::Runtime,
    Compression, Statistics,
};

pub enum Submission<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    CacheEntry {
        entry: CacheEntry<K, V, S>,
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

impl<K, V, S> Debug for Submission<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CacheEntry {
                entry: _,
                estimated_size,
                sequence,
            } => f
                .debug_struct("CacheEntry")
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
pub struct Flusher<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    tx: flume::Sender<Submission<K, V, S>>,

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
    #[expect(clippy::too_many_arguments)]
    pub fn open(
        config: &GenericLargeStorageConfig<K, V, S>,
        indexer: Indexer,
        region_manager: RegionManager,
        device: MonitoredDevice,
        tombstone_log: Option<TombstoneLog>,
        stats: Arc<Statistics>,
        metrics: Arc<Metrics>,
        runtime: &Runtime,
    ) -> Result<Self> {
        let (tx, rx) = flume::unbounded();

        let buffer_size = config.buffer_pool_size / config.flushers;
        let batch = BatchMut::new(
            buffer_size,
            region_manager.clone(),
            device.clone(),
            indexer.clone(),
            metrics.clone(),
        );

        let runner = Runner {
            rx,
            batch,
            flight: Arc::new(Semaphore::new(1)),
            region_manager,
            indexer,
            tombstone_log,
            compression: config.compression,
            flush: config.flush,
            stats,
            metrics: metrics.clone(),
        };

        runtime.write().spawn(async move {
            if let Err(e) = runner.run().await {
                tracing::error!("[flusher]: flusher exit with error: {e}");
            }
        });

        Ok(Self { tx, metrics })
    }

    pub fn submit(&self, submission: Submission<K, V, S>) {
        tracing::trace!("[lodc flusher]: submit task: {submission:?}");
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

struct Runner<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    rx: flume::Receiver<Submission<K, V, S>>,
    batch: BatchMut<K, V, S>,
    flight: Arc<Semaphore>,

    region_manager: RegionManager,
    indexer: Indexer,
    tombstone_log: Option<TombstoneLog>,

    compression: Compression,
    flush: bool,

    stats: Arc<Statistics>,
    metrics: Arc<Metrics>,
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
                Ok(permit) = flight.acquire_owned(), if !self.batch.is_empty() => {
                    // TODO(MrCroxx): `rotate()` should always return a `Some(..)` here.
                    if let Some(batch) = self.batch.rotate() {
                        self.commit(batch, permit).await;
                    }
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

    fn submit(&mut self, submission: Submission<K, V, S>) {
        let report = |enqueued: bool| {
            if !enqueued {
                self.metrics.storage_queue_drop.increment(1);
            }
        };

        match submission {
            Submission::CacheEntry {
                entry,
                estimated_size: _,
                sequence,
            } => report(self.batch.entry(entry, &self.compression, sequence)),
            Submission::Tombstone { tombstone, stats } => self.batch.tombstone(tombstone, stats),
            Submission::Reinsertion { reinsertion } => report(self.batch.reinsertion(&reinsertion)),
            Submission::Wait { tx } => self.batch.wait(tx),
        }
    }

    async fn commit(&mut self, batch: Batch<K, V, S>, permit: OwnedSemaphorePermit) {
        tracing::trace!("[flusher] commit batch: {batch:?}");

        // Write regions concurrently.
        let futures = batch.groups.into_iter().map(|group| {
            let indexer = self.indexer.clone();
            let region_manager = self.region_manager.clone();
            let stats = self.stats.clone();
            let flush = self.flush;
            async move {
                // Wait for region is clean.
                let region = group.region.handle.await;
                tracing::trace!(
                    "[flusher]: write region: {id}, at offset: {offset}, buffer len: {buf_len}",
                    id = region.id(),
                    offset = group.region.offset,
                    buf_len = group.bytes.len(),
                );

                // Write buffer to device.
                let size: usize = group.bytes.len();
                if size > 0 {
                    region.write(group.bytes, group.region.offset).await?;
                    if flush {
                        region.flush().await?;
                    }
                    stats.cache_write_bytes.fetch_add(size, Ordering::Relaxed);
                }
                let mut indices = group.indices;
                for haddr in indices.iter_mut() {
                    haddr.address.region = region.id();
                }
                indexer.insert_batch(indices);

                if group.region.is_full {
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
            let region_manager = self.region_manager.clone();
            let tombstone_log = self.tombstone_log.clone();
            async move {
                if let Some(log) = tombstone_log {
                    log.append(tombstones.iter().map(|info| &info.tombstone)).await?;
                }
                for TombstoneInfo { tombstone: _, stats } in tombstones {
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
        if let Err(e) = try_join(try_join_all(futures), future).await {
            tracing::error!("[flusher]: error raised when committing batch, error: {e}");
        }

        for waiter in batch.waiters {
            let _ = waiter.send(());
        }

        if let Some(init) = batch.init.as_ref() {
            self.metrics.storage_queue_rotate.increment(1);
            self.metrics.storage_queue_rotate_duration.record(init.elapsed());
        }

        drop(permit);
    }
}
