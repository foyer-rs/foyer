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
    collections::VecDeque,
    fmt::Debug,
    future::{poll_fn, Future},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{ready, Poll},
    time::Instant,
};

use foyer_common::{
    bits,
    code::{StorageKey, StorageValue},
    metrics::Metrics,
};
use foyer_memory::Piece;
use futures_core::future::BoxFuture;
use futures_util::{
    future::{try_join, try_join_all},
    FutureExt,
};
use itertools::Itertools;
use tokio::sync::oneshot;

#[cfg(test)]
use crate::large::test_utils::*;
use crate::{
    device::{MonitoredDevice, RegionId},
    error::{Error, Result},
    io::{buffer::IoBuffer, PAGE},
    large::{
        buffer::{Batch, BlobPart, Buffer, Region, SplitCtx, Splitter},
        generic::GenericLargeStorageConfig,
        indexer::{EntryAddress, HashedEntryAddress, Indexer},
        reclaimer::Reinsertion,
        serde::Sequence,
        tombstone::{Tombstone, TombstoneLog},
    },
    region::{GetCleanRegionHandle, RegionManager},
    runtime::Runtime,
    Compression, Dev, SharedIoSlice, Statistics,
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
    id: usize,
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
            id: self.id,
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
        id: usize,
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

        let io_buffer_size = config.buffer_pool_size / config.flushers;
        bits::assert_aligned(PAGE, io_buffer_size);
        bits::assert_aligned(PAGE, config.blob_index_size);

        let max_entry_size = device.region_size() - config.blob_index_size;

        let io_buffer = IoBuffer::new(io_buffer_size);
        let rotate_buffer = Some(IoBuffer::new(io_buffer_size));

        let buffer = Buffer::new(io_buffer, max_entry_size, metrics.clone());
        let buffer = Some(buffer);

        let current_region_handle = region_manager.get_clean_region();

        let ctx = SplitCtx::new(device.region_size(), config.blob_index_size);

        let runner = Runner {
            id,
            rx: Some(rx),
            buffer,
            ctx,
            tombstone_infos: vec![],
            waiters: vec![],
            rotate_buffer,
            queue_init: None,
            submit_queue_size: submit_queue_size.clone(),
            region_manager,
            _device: device,
            indexer,
            tombstone_log,
            compression: config.compression,
            flush: config.flush,
            runtime: runtime.clone(),
            stats,
            metrics: metrics.clone(),
            io_tasks: VecDeque::with_capacity(1),
            current_region_handle,
            max_entry_size,
            #[cfg(test)]
            flush_holder,
        };

        runtime.write().spawn(async move {
            if let Err(e) = runner.run().await {
                tracing::error!(id, "[flusher]: flusher exit with error: {e}");
            }
        });

        Ok(Self {
            id,
            tx,
            submit_queue_size,
            metrics,
        })
    }

    pub fn submit(&self, submission: Submission<K, V>) {
        tracing::trace!(id = self.id, "[lodc flusher]: submit task: {submission:?}");
        if let Submission::CacheEntry { estimated_size, .. } = &submission {
            self.submit_queue_size.fetch_add(*estimated_size, Ordering::Relaxed);
        }
        if let Err(e) = self.tx.send(submission) {
            tracing::error!(
                id = self.id,
                "[lodc flusher]: error raised when submitting task, error: {e}"
            );
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

struct IoTaskCtx {
    handle: Option<GetCleanRegionHandle>,
    waiters: Vec<oneshot::Sender<()>>,
    init: Instant,
    io_slice: SharedIoSlice,
}

struct Runner<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    id: usize,

    rx: Option<flume::Receiver<Submission<K, V>>>,

    // NOTE: writer is always `Some(..)`.
    buffer: Option<Buffer>,
    ctx: SplitCtx,
    tombstone_infos: Vec<TombstoneInfo>,
    waiters: Vec<oneshot::Sender<()>>,
    queue_init: Option<Instant>,

    /// IoBuffer rotates between writer and inflight io task.
    ///
    /// Use this field to avoid allocation.
    rotate_buffer: Option<IoBuffer>,

    submit_queue_size: Arc<AtomicUsize>,

    current_region_handle: GetCleanRegionHandle,

    region_manager: RegionManager,
    indexer: Indexer,
    tombstone_log: Option<TombstoneLog>,

    compression: Compression,
    flush: bool,

    _device: MonitoredDevice,

    runtime: Runtime,

    stats: Arc<Statistics>,
    metrics: Arc<Metrics>,

    io_tasks: VecDeque<BoxFuture<'static, IoTaskCtx>>,

    max_entry_size: usize,

    #[cfg(test)]
    flush_holder: FlushHolder,
}

impl<K, V> Runner<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn next_io_task_finish(&mut self) -> impl Future<Output = IoTaskCtx> + '_ {
        poll_fn(|cx| {
            if let Some(io_task) = self.io_tasks.front_mut() {
                let res = ready!(io_task.poll_unpin(cx));
                drop(self.io_tasks.pop_front().unwrap());
                return Poll::Ready(res);
            }
            Poll::Pending
        })
    }

    pub async fn run(mut self) -> Result<()> {
        let rx = self.rx.take().unwrap();

        loop {
            #[cfg(not(test))]
            let can_flush = true;
            #[cfg(test)]
            let can_flush = !self.flush_holder.is_held() && rx.is_empty();

            let need_flush = !self.buffer.as_ref().unwrap().is_empty()
                || !self.waiters.is_empty()
                || !self.tombstone_infos.is_empty();
            let no_io_task = self.io_tasks.is_empty();

            if can_flush && need_flush && no_io_task {
                let (io_buffer, infos) = self.buffer.take().unwrap().finish();

                let efficiency =
                    infos.last().map(|info| info.offset + info.len).unwrap_or_default() as f64 / io_buffer.len() as f64;
                self.metrics.storage_lodc_buffer_efficiency.record(efficiency);

                let shared_io_slice = io_buffer.into_shared_io_slice();
                let batch = Splitter::split(&mut self.ctx, shared_io_slice, infos);

                let tombstone_infos = std::mem::take(&mut self.tombstone_infos);
                let waiters = std::mem::take(&mut self.waiters);

                let init = self.queue_init.take().unwrap();

                let io_task = self.submit_io_task(batch, tombstone_infos, waiters, init);
                self.io_tasks.push_back(io_task);

                let io_buffer = self.rotate_buffer.take().unwrap();
                let buffer = Buffer::new(io_buffer, self.max_entry_size, self.metrics.clone());
                self.buffer = Some(buffer);
            }

            tokio::select! {
                biased;
                IoTaskCtx { handle, waiters, init, io_slice } = self.next_io_task_finish() => {
                    if let Some(handle) = handle {
                        self.current_region_handle = handle;
                    }
                    self.handle_io_complete(waiters, init);
                    // `try_into_io_buffer` must return `Some(..)` here.
                    self.rotate_buffer = io_slice.try_into_io_buffer();
                }
                Ok(submission) = rx.recv_async() => {
                    self.recv(submission);
                }
                // Graceful shutdown.
                else => break,
            }
        }
        Ok(())
    }

    fn recv(&mut self, submission: Submission<K, V>) {
        tracing::trace!(id = self.id, ?submission, "[lodc flush runner]: recv submission");

        if self.queue_init.is_none() {
            self.queue_init = Some(Instant::now());
        }

        let report = |written: bool| {
            if !written {
                self.metrics.storage_queue_buffer_overflow.increase(1);
            }
        };

        match submission {
            Submission::CacheEntry {
                piece,
                estimated_size,
                sequence,
            } => {
                report(self.buffer.as_mut().unwrap().push(
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
                    report(self.buffer.as_mut().unwrap().push_slice(
                        &reinsertion.slice,
                        reinsertion.hash,
                        reinsertion.sequence,
                    ));
                }
            }
            Submission::Wait { tx } => self.waiters.push(tx),
        }
    }

    fn submit_io_task(
        &self,
        batch: Batch,
        tombstone_infos: Vec<TombstoneInfo>,
        waiters: Vec<oneshot::Sender<()>>,
        init: Instant,
    ) -> BoxFuture<'static, IoTaskCtx> {
        let id = self.id;

        tracing::trace!(
            id,
            ?batch,
            ?tombstone_infos,
            waiters = waiters.len(),
            "[flusher] commit batch"
        );

        let region_handle_iter = if batch.regions.is_empty() {
            vec![]
        } else {
            std::iter::once(self.current_region_handle.clone())
                .chain((0..batch.regions.len() - 1).map(|_| self.region_manager.get_clean_region()))
                .collect_vec()
        };

        let io_slice = batch.io_slice;
        let regions = batch.regions.len();
        // Write regions concurrently.
        let futures = batch
            .regions
            .into_iter()
            .zip_eq(region_handle_iter)
            .enumerate()
            .map(|(i, (Region { blob_parts }, region_handle))| {
                let indexer = self.indexer.clone();
                let region_manager = self.region_manager.clone();
                let stats = self.stats.clone();
                let flush = self.flush;

                async move {
                    // Wait for region is clean.
                    let region = region_handle.clone().await;

                    let tasks = blob_parts.into_iter().map(
                        |BlobPart {
                             blob_region_offset,
                             index,
                             part_blob_offset,
                             data,
                             indices,
                         }| {
                            let offset = blob_region_offset + part_blob_offset;
                            let len = data.len();

                            bits::assert_aligned(PAGE, offset);
                            bits::assert_aligned(PAGE, len);

                            let region = region.clone();
                            let stats = stats.clone();
                            async move {
                                if len > 0 {
                                    tracing::trace!(
                                        id,
                                        region = region.id(),
                                        offset,
                                        len,
                                        "[flusher]: write blob data"
                                    );

                                    let (_, res) = region.write(data, offset as _).await;
                                    if let Err(e) = res.as_ref() {
                                        tracing::error!(
                                            id,
                                            blob_region_offset,
                                            part_blob_offset,
                                            ?indices,
                                            ?res,
                                            ?e,
                                            "[flusher]: flush data error"
                                        );
                                    }
                                    res?;

                                    tracing::trace!(id, offset = blob_region_offset, "[flusher]: write blob index");

                                    let (_, res) = region.write(index, blob_region_offset as _).await;
                                    if let Err(e) = res.as_ref() {
                                        tracing::error!(
                                            id,
                                            blob_region_offset,
                                            part_blob_offset,
                                            ?indices,
                                            ?res,
                                            ?e,
                                            "[flusher]: flush data error"
                                        );
                                    }
                                    res?;

                                    if flush {
                                        region.flush().await?;
                                    }
                                    stats.record_write_io(len);
                                } else {
                                    tracing::trace!(
                                        id,
                                        region = region.id(),
                                        "[flusher]: skip write region, because the window is empty"
                                    );
                                }

                                Ok::<_, Error>((region.id(), blob_region_offset, indices))
                            }
                        },
                    );
                    let infos = try_join_all(tasks).await?;

                    let mut addrs = Vec::with_capacity(infos.iter().map(|(_, _, indices)| indices.len()).sum());
                    for (region, blob_offset, indices) in infos {
                        for index in indices {
                            let addr = HashedEntryAddress {
                                hash: index.hash,
                                address: EntryAddress {
                                    region,
                                    offset: blob_offset as u32 + index.offset,
                                    len: index.len,
                                    sequence: index.sequence,
                                },
                            };
                            tracing::trace!(id, ?addr, "[flusher]: append address");
                            addrs.push(addr);
                        }
                    }

                    indexer.insert_batch(addrs);

                    // Window expect window is full, make it evictable.
                    if i != regions - 1 {
                        region_manager.mark_evictable(region.id());
                    }
                    tracing::trace!(id, "[flusher]: write region {id} finish.", id = region.id());

                    Ok::<_, Error>(region_handle)
                }
            })
            .collect_vec();

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

        let f: BoxFuture<'_, Result<(Vec<GetCleanRegionHandle>, ())>> = try_join(try_join_all(futures), future).boxed();
        let handle = self
            .runtime
            .write()
            .spawn(f)
            .map(move |jres| match jres {
                Ok(Ok((mut states, ()))) => IoTaskCtx {
                    handle: states.pop(),
                    waiters,
                    init,
                    io_slice,
                },
                Ok(Err(e)) => {
                    tracing::error!(id, ?e, "[lodc flusher]: io task error");
                    IoTaskCtx {
                        handle: None,
                        waiters,
                        init,
                        io_slice,
                    }
                }
                Err(e) => {
                    tracing::error!(id, ?e, "[lodc flusher]: join io task error");
                    IoTaskCtx {
                        handle: None,
                        waiters,
                        init,
                        io_slice,
                    }
                }
            })
            .boxed();

        handle
    }

    fn handle_io_complete(&self, waiters: Vec<oneshot::Sender<()>>, init: Instant) {
        for waiter in waiters {
            let _ = waiter.send(());
        }

        self.metrics.storage_queue_rotate.increase(1);
        self.metrics
            .storage_queue_rotate_duration
            .record(init.elapsed().as_secs_f64());
    }
}
