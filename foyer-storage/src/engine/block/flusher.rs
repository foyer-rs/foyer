// Copyright 2026 foyer Project Authors
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
    future::{Future, poll_fn},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Poll, ready},
    time::Instant,
};

use asyncband::{
    mpsc::{UnboundedReceiver, UnboundedSender},
    oneshot,
};
use foyer_common::{
    bits,
    code::{StorageKey, StorageValue},
    error::{Error, Result},
    metrics::Metrics,
    properties::Properties,
    spawn::Spawner,
};
use futures_core::future::BoxFuture;
use futures_util::{
    FutureExt,
    future::{try_join, try_join_all},
};
use itertools::Itertools;

#[cfg(any(test, feature = "test_utils"))]
use crate::test_utils::*;
use crate::{
    Compression,
    engine::block::{
        buffer::{Batch, BlobPart, Block, Buffer, Push, SplitCtx, Splitter},
        indexer::{EntryAddress, HashedEntryAddress, Indexer},
        manager::{BlockId, BlockManager, GetCleanBlockHandle},
        reclaimer::Reinsertion,
        serde::Sequence,
        tombstone::{Tombstone, TombstoneLog},
    },
    io::{
        PAGE,
        bytes::{IoSlice, IoSliceMut},
    },
    keeper::PieceRef,
};

pub enum Submission<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    CacheEntry {
        piece: PieceRef<K, V, P>,
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

impl<K, V, P> Debug for Submission<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
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
pub struct Flusher<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    id: usize,
    tx: UnboundedSender<Submission<K, V, P>>,
    submit_queue_size: Arc<AtomicUsize>,

    metrics: Arc<Metrics>,
}

impl<K, V, P> Clone for Flusher<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
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

impl<K, V, P> Flusher<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    pub fn new(
        id: usize,
        submit_queue_size: Arc<AtomicUsize>,
        metrics: Arc<Metrics>,
    ) -> (Self, UnboundedReceiver<Submission<K, V, P>>) {
        let (tx, rx) = asyncband::mpsc::unbounded();
        let this = Self {
            id,
            tx,
            submit_queue_size,
            metrics,
        };
        (this, rx)
    }

    #[expect(clippy::too_many_arguments)]
    pub fn run(
        &self,
        rx: UnboundedReceiver<Submission<K, V, P>>,
        block_size: usize,
        io_buffer_size: usize,
        blob_index_size: usize,
        compression: Compression,
        indexer: Indexer,
        block_manager: BlockManager,
        tombstone_log: Option<TombstoneLog>,
        metrics: Arc<Metrics>,
        spawner: &Spawner,
        #[cfg(any(test, feature = "test_utils"))] flush_switch: Switch,
    ) -> Result<()> {
        let id = self.id;
        let io_buffer_size = bits::align_down(PAGE, io_buffer_size);
        assert!(io_buffer_size > 0);

        bits::assert_aligned(PAGE, io_buffer_size);
        bits::assert_aligned(PAGE, blob_index_size);

        let max_entry_size = block_size - blob_index_size;

        let bytes = IoSliceMut::new(io_buffer_size);
        let rotate_buffer = Some(IoSliceMut::new(io_buffer_size));

        let buffer = Buffer::new(bytes, max_entry_size, metrics.clone());
        let buffer = Some(buffer);

        let current_block_handle = block_manager.get_clean_block();

        let ctx = SplitCtx::new(block_size, blob_index_size);

        let runner = Runner {
            id,
            rx: Some(rx),
            buffer,
            ctx,
            pending: VecDeque::new(),
            tombstone_infos: vec![],
            waiters: vec![],
            piece_refs: vec![],
            rotate_buffer,
            queue_init: None,
            submit_queue_size: self.submit_queue_size.clone(),
            block_manager,
            indexer,
            tombstone_log,
            compression,
            spawner: spawner.clone(),
            metrics: metrics.clone(),
            io_tasks: VecDeque::with_capacity(1),
            current_block_handle,
            max_entry_size,
            #[cfg(any(test, feature = "test_utils"))]
            flush_switch,
        };

        spawner.spawn(async move {
            if let Err(e) = runner.run().await {
                tracing::error!(id, "[flusher]: flusher exit with error: {e}");
            }
        });

        Ok(())
    }

    pub fn submit(&self, submission: Submission<K, V, P>) {
        tracing::trace!(id = self.id, "[block engine flusher]: submit task: {submission:?}");
        if let Submission::CacheEntry { estimated_size, .. } = &submission {
            self.submit_queue_size.fetch_add(*estimated_size, Ordering::Relaxed);
        }
        if let Err(e) = self.tx.send(submission) {
            tracing::error!(
                id = self.id,
                "[block engine flusher]: error raised when submitting task, error: {e}"
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

#[derive(Debug, Clone)]
pub struct InvalidStats {
    pub block: BlockId,
    pub size: usize,
}

#[derive(Debug, Clone)]
pub struct TombstoneInfo {
    pub tombstone: Tombstone,
    pub stats: Option<InvalidStats>,
}

struct IoTaskCtx<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    handle: Option<GetCleanBlockHandle>,
    waiters: Vec<oneshot::Sender<()>>,
    piece_refs: Vec<PieceRef<K, V, P>>,
    init: Instant,
    io_slice: IoSlice,
    tombstone_infos: Vec<TombstoneInfo>,
}

struct Runner<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    id: usize,

    rx: Option<UnboundedReceiver<Submission<K, V, P>>>,

    // NOTE: writer is always `Some(..)`.
    buffer: Option<Buffer>,
    ctx: SplitCtx,
    /// The submissions that are received but not handled yet.
    ///
    /// A submission that doesn't fit the remaining space of the current buffer is kept here and retried after the
    /// buffer rotates. The submissions received after it are kept here, too, to preserve the submission order.
    pending: VecDeque<Submission<K, V, P>>,
    tombstone_infos: Vec<TombstoneInfo>,
    piece_refs: Vec<PieceRef<K, V, P>>,
    waiters: Vec<oneshot::Sender<()>>,
    queue_init: Option<Instant>,

    /// IoBuffer rotates between writer and inflight io task.
    ///
    /// Use this field to avoid allocation.
    rotate_buffer: Option<IoSliceMut>,

    submit_queue_size: Arc<AtomicUsize>,

    current_block_handle: GetCleanBlockHandle,

    block_manager: BlockManager,
    indexer: Indexer,
    tombstone_log: Option<TombstoneLog>,

    compression: Compression,

    spawner: Spawner,

    metrics: Arc<Metrics>,

    io_tasks: VecDeque<BoxFuture<'static, IoTaskCtx<K, V, P>>>,

    max_entry_size: usize,

    #[cfg(any(test, feature = "test_utils"))]
    flush_switch: Switch,
}

impl<K, V, P> Runner<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    fn next_io_task_finish(&mut self) -> impl Future<Output = IoTaskCtx<K, V, P>> + '_ {
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
        let mut rx = self.rx.take().unwrap();

        loop {
            while let Ok(submission) = rx.try_recv() {
                self.pending.push_back(submission);
            }
            self.handle_pending();

            #[cfg(not(any(test, feature = "test_utils")))]
            let can_flush = true;
            #[cfg(any(test, feature = "test_utils"))]
            let can_flush = !self.flush_switch.is_on();

            // NOTE: A pending submission always implies a non-empty buffer, for a submission that is rejected by an
            // empty buffer is dropped instead of pending. It is listed here anyway to make sure that a pending
            // submission always triggers the rotation that it is waiting for.
            let need_flush = !self.buffer.as_ref().unwrap().is_empty()
                || !self.waiters.is_empty()
                || !self.tombstone_infos.is_empty()
                || !self.pending.is_empty();
            let no_io_task = self.io_tasks.is_empty();

            if can_flush && need_flush && no_io_task {
                let (io_buffer, infos) = self.buffer.take().unwrap().finish();

                let efficiency =
                    infos.last().map(|info| info.offset + info.len).unwrap_or_default() as f64 / io_buffer.len() as f64;
                self.metrics.storage_block_engine_buffer_efficiency.record(efficiency);

                let shared_io_slice = io_buffer.into_io_slice();
                let batch = Splitter::split(&mut self.ctx, shared_io_slice, infos);

                let tombstone_infos = std::mem::take(&mut self.tombstone_infos);
                let waiters = std::mem::take(&mut self.waiters);
                let piece_refs = std::mem::take(&mut self.piece_refs);

                let init = self.queue_init.take().unwrap();

                let io_task = self.submit_io_task(batch, piece_refs, tombstone_infos, waiters, init);
                self.io_tasks.push_back(io_task);

                let io_buffer = self.rotate_buffer.take().unwrap();
                let buffer = Buffer::new(io_buffer, self.max_entry_size, self.metrics.clone());
                self.buffer = Some(buffer);

                // Feed the submissions that the sealed buffer cannot hold into the rotated buffer.
                self.handle_pending();
            }

            tokio::select! {
                biased;
                IoTaskCtx { handle, waiters, init, io_slice, tombstone_infos, piece_refs } = self.next_io_task_finish() => {
                    if let Some(handle) = handle {
                        self.current_block_handle = handle;
                    }
                    self.handle_io_complete(piece_refs, waiters, tombstone_infos, init);
                    // `try_into_io_buffer` must return `Some(..)` here.
                    self.rotate_buffer = io_slice.try_into_io_slice_mut();
                }
                Ok(submission) = rx.recv() => {
                    self.pending.push_back(submission);
                }
                // Graceful shutdown.
                else => break,
            }
        }
        Ok(())
    }

    /// Handle the pending submissions in order, until the current buffer cannot hold the next one.
    fn handle_pending(&mut self) {
        while let Some(submission) = self.pending.pop_front() {
            if let Some(submission) = self.recv(submission) {
                self.pending.push_front(submission);
                break;
            }
        }
    }

    /// Handle a submission.
    ///
    /// Returns the submission back if the current buffer cannot hold it, in which case it must be retried after the
    /// buffer rotates.
    fn recv(&mut self, submission: Submission<K, V, P>) -> Option<Submission<K, V, P>> {
        tracing::trace!(
            id = self.id,
            ?submission,
            "[block engine flush runner]: recv submission"
        );

        if self.queue_init.is_none() {
            self.queue_init = Some(Instant::now());
        }

        match submission {
            Submission::CacheEntry {
                piece,
                estimated_size,
                sequence,
            } => {
                let buffer = self.buffer.as_mut().unwrap();
                let push = buffer.push(piece.key(), piece.value(), piece.hash(), self.compression, sequence);
                // NOTE: The push doesn't modify the buffer if it doesn't succeed, so this tells if the entry was
                // rejected by an empty buffer.
                let rejected_by_empty_buffer = buffer.is_empty();

                match push {
                    Push::Ok => self.piece_refs.push(piece),
                    // The entry doesn't fit the tail of the current buffer, retry it after the buffer rotates.
                    Push::NoSpace if !rejected_by_empty_buffer => {
                        tracing::trace!(
                            id = self.id,
                            hash = piece.hash(),
                            "[block engine flush runner]: defer entry to the next buffer"
                        );
                        return Some(Submission::CacheEntry {
                            piece,
                            estimated_size,
                            sequence,
                        });
                    }
                    // The entry is rejected by an empty buffer or by the max entry size, it can never be stored.
                    Push::NoSpace | Push::Unstorable => {
                        tracing::warn!(
                            id = self.id,
                            hash = piece.hash(),
                            "[block engine flush runner]: entry is too large to be stored, drop it"
                        );
                        self.metrics.storage_queue_buffer_overflow.increase(1);
                    }
                }

                // NOTE: The submit queue size must be decreased exactly once per submission, so it is only decreased
                // after the submission is handled, never after a deferred push attempt.
                self.submit_queue_size.fetch_sub(estimated_size, Ordering::Relaxed);
            }

            Submission::Tombstone { tombstone, stats } => self.tombstone_infos.push(TombstoneInfo { tombstone, stats }),
            Submission::Reinsertion { reinsertion } => {
                // Skip reinsertion if the entry is not in the indexer.
                if self.indexer.get(reinsertion.hash).is_some() {
                    let buffer = self.buffer.as_mut().unwrap();
                    let push = buffer.push_slice(
                        &reinsertion.slice[..reinsertion.len],
                        reinsertion.hash,
                        reinsertion.sequence,
                    );
                    let rejected_by_empty_buffer = buffer.is_empty();

                    match push {
                        Push::Ok => {}
                        // The reinsertion doesn't fit the tail of the current buffer, retry it after the buffer
                        // rotates.
                        Push::NoSpace if !rejected_by_empty_buffer => {
                            tracing::trace!(
                                id = self.id,
                                hash = reinsertion.hash,
                                "[block engine flush runner]: defer reinsertion to the next buffer"
                            );
                            return Some(Submission::Reinsertion { reinsertion });
                        }
                        // The reinsertion is rejected by an empty buffer or by the max entry size, it can never be
                        // stored.
                        Push::NoSpace | Push::Unstorable => {
                            tracing::warn!(
                                id = self.id,
                                hash = reinsertion.hash,
                                "[block engine flush runner]: reinsertion is too large to be stored, drop it"
                            );
                            self.metrics.storage_queue_buffer_overflow.increase(1);
                        }
                    }
                }
            }
            Submission::Wait { tx } => self.waiters.push(tx),
        }

        None
    }

    fn submit_io_task(
        &self,
        batch: Batch,
        piece_refs: Vec<PieceRef<K, V, P>>,
        tombstone_infos: Vec<TombstoneInfo>,
        waiters: Vec<oneshot::Sender<()>>,
        init: Instant,
    ) -> BoxFuture<'static, IoTaskCtx<K, V, P>> {
        let id = self.id;

        tracing::trace!(
            id,
            ?batch,
            ?tombstone_infos,
            waiters = waiters.len(),
            "[flusher] commit batch"
        );

        let block_handle_iter = if batch.blocks.is_empty() {
            vec![]
        } else {
            std::iter::once(self.current_block_handle.clone())
                .chain((0..batch.blocks.len() - 1).map(|_| self.block_manager.get_clean_block()))
                .collect_vec()
        };

        let bytes = batch.bytes;
        let blocks = batch.blocks.len();
        // Write blocks concurrently.
        let futures = batch
            .blocks
            .into_iter()
            .zip_eq(block_handle_iter)
            .enumerate()
            .map(|(i, (Block { blob_parts }, block_handle))| {
                let indexer = self.indexer.clone();
                let block_manager = self.block_manager.clone();
                let metrics = self.metrics.clone();

                async move {
                    // Wait for block is clean.
                    let block = block_handle.clone().await;

                    let tasks = blob_parts.into_iter().map(
                        |BlobPart {
                             blob_block_offset,
                             index,
                             part_blob_offset,
                             data,
                             indices,
                         }| {
                            let offset = blob_block_offset + part_blob_offset;
                            let len = data.len();

                            bits::assert_aligned(PAGE, offset);
                            bits::assert_aligned(PAGE, len);

                            let block = block.clone();
                            async move {
                                if len > 0 {
                                    tracing::trace!(id, block = block.id(), offset, len, "[flusher]: write blob data");

                                    let (_, res) = block.write(Box::new(data), offset as _).await;
                                    if let Err(e) = res.as_ref() {
                                        tracing::error!(
                                            id,
                                            blob_block_offset,
                                            part_blob_offset,
                                            ?indices,
                                            ?res,
                                            ?e,
                                            "[flusher]: flush data error"
                                        );
                                    }
                                    res?;

                                    tracing::trace!(id, offset = blob_block_offset, "[flusher]: write blob index");

                                    let (_, res) = block.write(Box::new(index), blob_block_offset as _).await;
                                    if let Err(e) = res.as_ref() {
                                        tracing::error!(
                                            id,
                                            blob_block_offset,
                                            part_blob_offset,
                                            ?indices,
                                            ?res,
                                            ?e,
                                            "[flusher]: flush data error"
                                        );
                                    }
                                    res?;
                                } else {
                                    tracing::trace!(
                                        id,
                                        block = block.id(),
                                        "[flusher]: skip write block, because the window is empty"
                                    );
                                }

                                Ok::<_, Error>((block.id(), blob_block_offset, indices))
                            }
                        },
                    );
                    let infos = try_join_all(tasks).await?;

                    let mut addrs = Vec::with_capacity(infos.iter().map(|(_, _, indices)| indices.len()).sum());
                    for (block, blob_offset, indices) in infos {
                        for index in indices {
                            let addr = HashedEntryAddress {
                                hash: index.hash,
                                address: EntryAddress {
                                    block,
                                    offset: blob_offset as u32 + index.offset,
                                    len: index.len,
                                    sequence: index.sequence,
                                },
                            };
                            tracing::trace!(id, ?addr, "[flusher]: append address");
                            addrs.push(addr);
                        }
                    }

                    let olds = indexer.insert_batch(addrs);
                    metrics.storage_block_engine_indexer_conflict.increase(olds.len() as _);

                    // Window expect window is full, make it evictable.
                    let id = block.id();
                    if i != blocks - 1 {
                        block_manager.on_writing_finish(block);
                    }
                    tracing::trace!(id, "[flusher]: write block finish.");

                    Ok::<_, Error>(block_handle)
                }
            })
            .collect_vec();

        let future = {
            let block_manager = self.block_manager.clone();
            let tombstone_log = self.tombstone_log.clone();
            let tombstone_infos = tombstone_infos.clone();
            async move {
                if let Some(log) = tombstone_log {
                    log.append(tombstone_infos.iter().map(|info| &info.tombstone)).await?;
                }
                for TombstoneInfo { tombstone: _, stats } in tombstone_infos {
                    if let Some(stats) = stats {
                        block_manager
                            .block(stats.block)
                            .statistics()
                            .invalid
                            .fetch_add(stats.size, Ordering::Relaxed);
                    }
                }
                Ok::<_, Error>(())
            }
        };

        let handle = self
            .spawner
            .spawn(Box::pin(try_join(try_join_all(futures), future)))
            .map(move |jres| match jres {
                Ok(Ok((mut states, ()))) => IoTaskCtx {
                    handle: states.pop(),
                    piece_refs,
                    waiters,
                    init,
                    io_slice: bytes,
                    tombstone_infos,
                },
                Ok(Err(e)) => {
                    tracing::error!(id, ?e, "[block engine flusher]: io task error");
                    IoTaskCtx {
                        handle: None,
                        piece_refs,
                        waiters,
                        init,
                        io_slice: bytes,
                        tombstone_infos,
                    }
                }
                Err(e) => {
                    tracing::error!(id, ?e, "[block engine flusher]: join io task error");
                    IoTaskCtx {
                        handle: None,
                        piece_refs,
                        waiters,
                        init,
                        io_slice: bytes,
                        tombstone_infos,
                    }
                }
            });

        Box::pin(handle)
    }

    fn handle_io_complete(
        &self,
        piece_refs: Vec<PieceRef<K, V, P>>,
        waiters: Vec<oneshot::Sender<()>>,
        tombstone_infos: Vec<TombstoneInfo>,
        init: Instant,
    ) {
        drop(piece_refs);

        self.indexer.remove_batch(
            tombstone_infos
                .iter()
                .map(|info| (info.tombstone.hash, info.tombstone.sequence)),
        );

        for waiter in waiters {
            let _ = waiter.send(());
        }

        self.metrics.storage_queue_rotate.increase(1);
        self.metrics
            .storage_queue_rotate_duration
            .record(init.elapsed().as_secs_f64());
    }
}

#[cfg(test)]
mod tests {
    use std::{borrow::Cow, collections::HashMap, path::Path, sync::atomic::AtomicU64, time::Duration};

    use foyer_common::hasher::ModHasher;
    use foyer_memory::{Cache, CacheBuilder, CacheEntry, FifoConfig, TestProperties};
    use mixtrics::metrics::{
        BoxedCounter, BoxedCounterVec, BoxedGauge, BoxedGaugeVec, BoxedHistogram, BoxedHistogramVec, BoxedRegistry,
        CounterOps, CounterVecOps, GaugeOps, GaugeVecOps, HistogramOps, HistogramVecOps, RegistryOps,
    };
    use parking_lot::Mutex;

    use super::*;
    use crate::{
        PsyncIoEngineConfig, RejectAll, StorageFilter,
        engine::block::{
            eviction::FifoPicker,
            reclaimer::{Reclaimer, ReclaimerTrait},
        },
        io::{
            device::{DeviceBuilder, fs::FsDeviceBuilder},
            engine::{IoEngineBuildContext, IoEngineConfig},
        },
        serde::EntrySerializer,
    };

    const KB: usize = 1024;
    /// A 3 KiB entry is serialized into a bit more than 3 KiB, so it takes exactly one page.
    const ONE_PAGE_ENTRY_VALUE_SIZE: usize = 3 * KB;

    /* metrics registry that counts the counter increments, so that the tests can assert on them */

    #[derive(Debug, Default, Clone)]
    struct TestCounters {
        counters: Arc<Mutex<HashMap<String, Arc<AtomicU64>>>>,
    }

    impl TestCounters {
        fn key(name: &str, labels: &[&str]) -> String {
            format!("{name}{{{}}}", labels.join(","))
        }

        fn counter(&self, key: String) -> Arc<AtomicU64> {
            self.counters.lock().entry(key).or_default().clone()
        }

        fn get(&self, name: &str, labels: &[&str]) -> u64 {
            self.counter(Self::key(name, labels)).load(Ordering::Relaxed)
        }
    }

    #[derive(Debug)]
    struct TestCounter(Arc<AtomicU64>);

    impl CounterOps for TestCounter {
        fn increase(&self, val: u64) {
            self.0.fetch_add(val, Ordering::Relaxed);
        }
    }

    #[derive(Debug)]
    struct TestCounterVec {
        name: String,
        counters: TestCounters,
    }

    impl CounterVecOps for TestCounterVec {
        fn counter(&self, labels: &[Cow<'static, str>]) -> BoxedCounter {
            let labels = labels.iter().map(|label| label.as_ref()).collect_vec();
            Box::new(TestCounter(
                self.counters.counter(TestCounters::key(&self.name, &labels)),
            ))
        }
    }

    /// Only the counters are recorded, the other metrics are ignored.
    #[derive(Debug)]
    struct TestIgnored;

    impl GaugeOps for TestIgnored {
        fn increase(&self, _: u64) {}

        fn decrease(&self, _: u64) {}

        fn absolute(&self, _: u64) {}
    }

    impl GaugeVecOps for TestIgnored {
        fn gauge(&self, _: &[Cow<'static, str>]) -> BoxedGauge {
            Box::new(TestIgnored)
        }
    }

    impl HistogramOps for TestIgnored {
        fn record(&self, _: f64) {}
    }

    impl HistogramVecOps for TestIgnored {
        fn histogram(&self, _: &[Cow<'static, str>]) -> BoxedHistogram {
            Box::new(TestIgnored)
        }
    }

    #[derive(Debug)]
    struct TestRegistry {
        counters: TestCounters,
    }

    impl RegistryOps for TestRegistry {
        fn register_counter_vec(
            &self,
            name: Cow<'static, str>,
            _: Cow<'static, str>,
            _: &'static [&'static str],
        ) -> BoxedCounterVec {
            Box::new(TestCounterVec {
                name: name.into_owned(),
                counters: self.counters.clone(),
            })
        }

        fn register_gauge_vec(
            &self,
            _: Cow<'static, str>,
            _: Cow<'static, str>,
            _: &'static [&'static str],
        ) -> BoxedGaugeVec {
            Box::new(TestIgnored)
        }

        fn register_histogram_vec(
            &self,
            _: Cow<'static, str>,
            _: Cow<'static, str>,
            _: &'static [&'static str],
        ) -> BoxedHistogramVec {
            Box::new(TestIgnored)
        }

        fn register_histogram_vec_with_buckets(
            &self,
            _: Cow<'static, str>,
            _: Cow<'static, str>,
            _: &'static [&'static str],
            _: Vec<f64>,
        ) -> BoxedHistogramVec {
            Box::new(TestIgnored)
        }
    }

    /* test utils */

    type TestCache = Cache<u64, Vec<u8>, ModHasher, TestProperties>;
    type TestCacheEntry = CacheEntry<u64, Vec<u8>, ModHasher, TestProperties>;

    fn cache_for_test() -> TestCache {
        CacheBuilder::new(1024)
            .with_shards(1)
            .with_eviction_config(FifoConfig::default())
            .with_hash_builder(ModHasher::default())
            .build()
    }

    /// Push an entry into an empty buffer, to tell why the buffer rejects it.
    fn push_into_empty_buffer(entry: &TestCacheEntry, io_buffer_size: usize, max_entry_size: usize) -> Push {
        let mut buffer = Buffer::new(
            IoSliceMut::new(io_buffer_size),
            max_entry_size,
            Arc::new(Metrics::noop()),
        );
        buffer.push(entry.key(), entry.value(), entry.hash(), Compression::None, 0)
    }

    /// Await `future`, but panic if it doesn't finish in time, so that a flusher that makes no progress fails the test
    /// instead of hanging it.
    async fn with_deadline<F>(future: F) -> F::Output
    where
        F: Future,
    {
        const DEADLINE: Duration = Duration::from_secs(60);

        let start = Instant::now();
        let mut future = std::pin::pin!(future);
        poll_fn(move |cx| {
            if let Poll::Ready(output) = future.as_mut().poll(cx) {
                return Poll::Ready(output);
            }
            assert!(start.elapsed() < DEADLINE, "the future doesn't finish in {DEADLINE:?}");
            // NOTE: Reschedule instead of sleeping, for `tokio::time` is not available in this crate.
            cx.waker().wake_by_ref();
            Poll::Pending
        })
        .await
    }

    /// A running flusher with everything it needs to write to a device on a temporary directory.
    struct FlusherHarness {
        flusher: Flusher<u64, Vec<u8>, TestProperties>,
        indexer: Indexer,
        submit_queue_size: Arc<AtomicUsize>,
        counters: TestCounters,
        flush_switch: Switch,
        sequence: AtomicU64,
    }

    impl FlusherHarness {
        /// Run a flusher on a device on the given directory.
        ///
        /// NOTE: The max entry size is `block_size - blob_index_size`, while the buffer capacity is `io_buffer_size`.
        /// They are configured separately, just like [`crate::BlockEngineConfig`] does.
        async fn open(dir: impl AsRef<Path>, block_size: usize, blob_index_size: usize, io_buffer_size: usize) -> Self {
            /// Enough blocks to make sure that no reclamation interferes with the tests.
            const CAPACITY: usize = 256 * KB;

            let device = FsDeviceBuilder::new(dir).with_capacity(CAPACITY).build().unwrap();
            let spawner = Spawner::current();
            let io_engine = PsyncIoEngineConfig::new()
                .boxed()
                .build(IoEngineBuildContext {
                    spawner: spawner.clone(),
                })
                .await
                .unwrap();

            let counters = TestCounters::default();
            let registry: BoxedRegistry = Box::new(TestRegistry {
                counters: counters.clone(),
            });
            let metrics = Arc::new(Metrics::new("test", &registry));

            let indexer = Indexer::new(4);
            let submit_queue_size = Arc::<AtomicUsize>::default();
            let (flusher, rx) = Flusher::new(0, submit_queue_size.clone(), metrics.clone());

            let reclaimer = Reclaimer::new(
                indexer.clone(),
                vec![flusher.clone()],
                Arc::new(StorageFilter::new().with_condition(RejectAll)),
                blob_index_size,
                device.statistics().clone(),
                spawner.clone(),
            );
            let block_manager = BlockManager::open(
                device,
                io_engine,
                block_size,
                vec![Box::<FifoPicker>::default()],
                Arc::new(reclaimer) as Arc<dyn ReclaimerTrait>,
                1,
                1,
                metrics.clone(),
                spawner.clone(),
            )
            .unwrap();
            // There is nothing to recover from a device on a temporary directory, all blocks are clean.
            block_manager.init(&(0..block_manager.blocks() as BlockId).collect_vec());

            let flush_switch = Switch::default();
            flusher
                .run(
                    rx,
                    block_size,
                    io_buffer_size,
                    blob_index_size,
                    Compression::None,
                    indexer.clone(),
                    block_manager,
                    None,
                    metrics,
                    &spawner,
                    flush_switch.clone(),
                )
                .unwrap();

            Self {
                flusher,
                indexer,
                submit_queue_size,
                counters,
                flush_switch,
                sequence: AtomicU64::new(0),
            }
        }

        fn enqueue(&self, entry: &TestCacheEntry) {
            let estimated_size = EntrySerializer::estimated_size(entry.key(), entry.value());
            self.flusher.submit(Submission::CacheEntry {
                piece: entry.piece().into(),
                estimated_size,
                sequence: self.sequence.fetch_add(1, Ordering::Relaxed),
            });
        }

        /// The count of the entries that are dropped for they can never be stored.
        fn buffer_overflow(&self) -> u64 {
            self.counters
                .get("foyer_storage_inner_op_total", &["test", "buffer_overflow"])
        }

        fn submit_queue_size(&self) -> usize {
            self.submit_queue_size.load(Ordering::Relaxed)
        }
    }

    /// An entry that doesn't fit the tail of the current buffer must be deferred to the next buffer, not dropped.
    #[test_log::test(tokio::test)]
    async fn test_defer_entry_that_does_not_fit_buffer_tail() {
        const BLOCK_SIZE: usize = 16 * KB;
        const BLOB_INDEX_SIZE: usize = 4 * KB;
        const IO_BUFFER_SIZE: usize = 16 * KB;
        const ENTRIES_PER_BUFFER: u64 = (IO_BUFFER_SIZE / PAGE) as _;
        /// Fill more than one buffer, so that the entries have to be deferred more than once.
        const ENTRIES: u64 = ENTRIES_PER_BUFFER * 3;

        let dir = tempfile::tempdir().unwrap();
        let harness = FlusherHarness::open(dir.path(), BLOCK_SIZE, BLOB_INDEX_SIZE, IO_BUFFER_SIZE).await;
        let memory = cache_for_test();

        let entries = (0..ENTRIES)
            .map(|i| memory.insert(i, vec![i as u8; ONE_PAGE_ENTRY_VALUE_SIZE]))
            .collect_vec();

        // Hold the flush, so that the buffer cannot rotate while the entries are submitted. The buffer is filled up by
        // the first `ENTRIES_PER_BUFFER` entries, and all the entries after them find no space in the buffer tail.
        harness.flush_switch.on();
        for entry in &entries {
            harness.enqueue(entry);
        }
        harness.flush_switch.off();
        with_deadline(harness.flusher.wait()).await;

        // 1. No entry is silently dropped.
        let addrs = (0..ENTRIES)
            .map(|i| (i, harness.indexer.get(memory.hash(&i))))
            .collect_vec();
        let lost = addrs
            .iter()
            .filter(|(_, addr)| addr.is_none())
            .map(|(i, _)| *i)
            .collect_vec();
        assert!(lost.is_empty(), "entries {lost:?} are dropped");

        // 2. The submission order is preserved.
        let addrs = addrs.into_iter().map(|(_, addr)| addr.unwrap()).collect_vec();
        assert!(
            addrs
                .windows(2)
                .all(|w| (w[0].block, w[0].offset) < (w[1].block, w[1].offset)),
            "entries are written out of the submission order: {addrs:?}"
        );

        // 3. A deferred entry is not an overflow, and the submit queue size is decreased exactly once per submission.
        assert_eq!(harness.buffer_overflow(), 0);
        assert_eq!(harness.submit_queue_size(), 0);
    }

    /// An entry that is larger than the max entry size can never be stored, it must be dropped and counted.
    #[test_log::test(tokio::test)]
    async fn test_drop_entry_larger_than_max_entry_size() {
        const BLOCK_SIZE: usize = 16 * KB;
        const BLOB_INDEX_SIZE: usize = 4 * KB;
        const MAX_ENTRY_SIZE: usize = BLOCK_SIZE - BLOB_INDEX_SIZE;
        const IO_BUFFER_SIZE: usize = 16 * KB;

        let dir = tempfile::tempdir().unwrap();
        let harness = FlusherHarness::open(dir.path(), BLOCK_SIZE, BLOB_INDEX_SIZE, IO_BUFFER_SIZE).await;
        let memory = cache_for_test();

        // The entry fits the buffer, but its aligned size exceeds the max entry size.
        let large = memory.insert(0, vec![0u8; MAX_ENTRY_SIZE]);
        let small = memory.insert(1, vec![1u8; ONE_PAGE_ENTRY_VALUE_SIZE]);
        assert_eq!(
            push_into_empty_buffer(&large, IO_BUFFER_SIZE, MAX_ENTRY_SIZE),
            Push::Unstorable
        );

        harness.enqueue(&large);
        harness.enqueue(&small);
        with_deadline(harness.flusher.wait()).await;

        assert!(harness.indexer.get(memory.hash(&0)).is_none());
        assert!(harness.indexer.get(memory.hash(&1)).is_some());
        assert_eq!(harness.buffer_overflow(), 1);
        assert_eq!(harness.submit_queue_size(), 0);
    }

    /// An entry that fits the max entry size but is larger than the whole buffer can never be stored, either. It must
    /// be dropped instead of being deferred forever.
    #[test_log::test(tokio::test)]
    async fn test_drop_entry_larger_than_buffer() {
        const BLOCK_SIZE: usize = 16 * KB;
        const BLOB_INDEX_SIZE: usize = 4 * KB;
        const MAX_ENTRY_SIZE: usize = BLOCK_SIZE - BLOB_INDEX_SIZE;
        // NOTE: The buffer is smaller than the max entry size, for they are configured separately.
        const IO_BUFFER_SIZE: usize = 8 * KB;

        let dir = tempfile::tempdir().unwrap();
        let harness = FlusherHarness::open(dir.path(), BLOCK_SIZE, BLOB_INDEX_SIZE, IO_BUFFER_SIZE).await;
        let memory = cache_for_test();

        // The entry fits the max entry size, but it is larger than an empty buffer. `NoSpace` against an empty buffer
        // is the only signal that the entry can never be stored.
        let large = memory.insert(0, vec![0u8; IO_BUFFER_SIZE]);
        let small = memory.insert(1, vec![1u8; ONE_PAGE_ENTRY_VALUE_SIZE]);
        assert_eq!(
            push_into_empty_buffer(&large, IO_BUFFER_SIZE, MAX_ENTRY_SIZE),
            Push::NoSpace
        );

        harness.enqueue(&large);
        harness.enqueue(&small);
        // The deadline turns an endless defer-and-retry loop into a failure instead of a hang.
        with_deadline(harness.flusher.wait()).await;

        assert!(harness.indexer.get(memory.hash(&0)).is_none());
        assert!(harness.indexer.get(memory.hash(&1)).is_some());
        assert_eq!(harness.buffer_overflow(), 1);
        assert_eq!(harness.submit_queue_size(), 0);
    }

    /// The submit queue size must be decreased exactly once per submission, no matter if the entry is written, deferred
    /// and written, or dropped.
    #[test_log::test(tokio::test)]
    async fn test_submit_queue_size_accounting() {
        const BLOCK_SIZE: usize = 16 * KB;
        const BLOB_INDEX_SIZE: usize = 4 * KB;
        const MAX_ENTRY_SIZE: usize = BLOCK_SIZE - BLOB_INDEX_SIZE;
        const IO_BUFFER_SIZE: usize = 16 * KB;
        const ENTRIES_PER_BUFFER: u64 = (IO_BUFFER_SIZE / PAGE) as _;

        let dir = tempfile::tempdir().unwrap();
        let harness = FlusherHarness::open(dir.path(), BLOCK_SIZE, BLOB_INDEX_SIZE, IO_BUFFER_SIZE).await;
        let memory = cache_for_test();

        // Written directly, deferred then written, and dropped, all in one batch.
        let entries = (0..ENTRIES_PER_BUFFER * 2)
            .map(|i| memory.insert(i, vec![i as u8; ONE_PAGE_ENTRY_VALUE_SIZE]))
            .collect_vec();
        let unstorable = memory.insert(ENTRIES_PER_BUFFER * 2, vec![0u8; MAX_ENTRY_SIZE]);

        harness.flush_switch.on();
        for entry in &entries {
            harness.enqueue(entry);
        }
        harness.enqueue(&unstorable);
        harness.flush_switch.off();
        with_deadline(harness.flusher.wait()).await;

        // A double decrease underflows the counter, a missing decrease leaves it positive.
        assert_eq!(harness.submit_queue_size(), 0);
        assert_eq!(harness.buffer_overflow(), 1);
        for i in 0..ENTRIES_PER_BUFFER * 2 {
            assert!(harness.indexer.get(memory.hash(&i)).is_some(), "entry {i} is dropped");
        }
        assert!(harness.indexer.get(memory.hash(&(ENTRIES_PER_BUFFER * 2))).is_none());
    }
}
