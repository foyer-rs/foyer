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

//! An independent, fixed-bucket small-object disk cache.

mod bucket;

use std::{
    collections::{BTreeSet, VecDeque},
    fmt::Debug,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use foyer_common::{
    code::{StorageKey, StorageValue},
    error::{Error, ErrorKind, Result},
    metrics::Metrics,
    properties::{Age, Properties},
    spawn::Spawner,
};
use futures_util::{FutureExt, StreamExt, TryStreamExt, future::BoxFuture, stream};
use parking_lot::Mutex;
use tokio::sync::{Mutex as AsyncMutex, Notify};

use crate::{
    Compression, Device, Engine, EngineBuildContext, EngineConfig, IoEngine, IoEngineBuildContext, Load, PieceRef,
    Populated, RecoverMode, Statistics, StorageFilter, StorageFilterResult,
    filter::conditions::IoThrottle,
    io::{PAGE, bytes::IoSliceMut, device::Partition, engine::monitor::MonitoredIoEngine},
    serde::EntryDeserializer,
};

/// Configuration for a BigHash-style small-object engine, independent of BlockEngine.
///
/// Hashes select fixed-size buckets. Records share bucket space and are evicted
/// FIFO within their bucket. Each mutation reads and rewrites one whole bucket.
/// An 8-byte bloom filter per bucket replaces a per-object address index.
/// The engine allocates one contiguous partition from the device's free space.
/// Use a dedicated device namespace and keep its capacity, bucket size, and key
/// hashing stable when reopening. This format is separate from BlockEngine's.
///
/// Recovery scans and checksums all buckets to rebuild the bloom filters.
/// [`RecoverMode::Strict`] returns corruption/I/O errors; [`RecoverMode::Quiet`]
/// clears unreadable buckets; [`RecoverMode::None`] clears the entire partition.
/// Deletes rewrite the bucket directly, so no separate tombstone log is needed.
///
/// Operations in one shard are serialized. Pending mutations temporarily hide
/// their bucket from disk lookups; Store can still serve queued values from its
/// keeper. `wait` drains work but cannot report failures through the Engine API.
/// `close` rejects new mutations, drains workers, and reports background errors
/// (including on repeated calls). Call it before reopening the device. Canceling
/// a wait/close/destroy future does not cancel already-submitted mutations.
/// Draining under continuous concurrent submissions may wait indefinitely.
///
/// This is a disposable cache, not durable storage: there is no fsync or WAL.
/// A torn bucket is detected by its checksum, and its entire contents may be lost.
/// After a failed close, clear the cache before reuse if stale values are unsafe.
#[derive(Debug)]
pub struct SmallEngineConfig {
    device: Arc<dyn Device>,
    bucket_size: usize,
    shards: usize,
    queue_capacity: usize,
    queue_bytes: usize,
    compression: Compression,
    admission: StorageFilter,
}

impl SmallEngineConfig {
    /// Create a small-object engine with 4 KiB buckets and 16 worker shards.
    pub fn new(device: Arc<dyn Device>) -> Self {
        Self {
            device,
            bucket_size: PAGE,
            shards: 16,
            queue_capacity: 1024,
            queue_bytes: 4 * 1024 * 1024,
            compression: Compression::None,
            admission: StorageFilter::new(),
        }
    }

    /// Set the bucket size, a nonzero multiple of 4 KiB (at most 1 MiB).
    /// A serialized key/value plus its 20-byte record header must fit in the
    /// bucket after its 40-byte header. Larger buckets increase read/write I/O.
    pub fn with_bucket_size(mut self, size: usize) -> Self {
        self.bucket_size = size;
        self
    }

    /// Set the number of independent workers and I/O serialization shards.
    pub fn with_shards(mut self, shards: usize) -> Self {
        self.shards = shards;
        self
    }

    /// Set the maximum number of queued mutations per shard (default: 1024).
    /// On overflow the affected bucket is invalidated and scheduled for clearing;
    /// this also prevents an older version of a dropped update from resurfacing.
    pub fn with_queue_capacity(mut self, capacity: usize) -> Self {
        self.queue_capacity = capacity;
        self
    }

    /// Set the estimated queued-byte limit per shard (default: 4 MiB).
    /// Each mutation is charged at least 64 bytes. This is not an RSS limit.
    pub fn with_queue_bytes(mut self, bytes: usize) -> Self {
        self.queue_bytes = bytes;
        self
    }

    /// Set compression for values written by this engine.
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Set the admission filter. Device write throttling is also applied.
    pub fn with_admission_filter(mut self, filter: StorageFilter) -> Self {
        self.admission = filter;
        self
    }

    async fn open<K: StorageKey, V: StorageValue, P: Properties>(
        self,
        ctx: EngineBuildContext,
    ) -> Result<Arc<dyn Engine<K, V, P>>> {
        if self.bucket_size == 0
            || !self.bucket_size.is_multiple_of(PAGE)
            || self.bucket_size > 1024 * 1024
            || self.shards == 0
            || self.queue_capacity == 0
            || self.queue_bytes < 64
        {
            return Err(Error::new(
                ErrorKind::Config,
                "invalid small-object engine configuration",
            ));
        }
        let buckets = self.device.free() / self.bucket_size;
        if buckets == 0 {
            return Err(Error::new(
                ErrorKind::Config,
                "device cannot fit one small-object bucket",
            ));
        }
        let partition = self.device.create_partition(buckets * self.bucket_size)?;
        let io = ctx
            .io_engine_config
            .build(IoEngineBuildContext {
                spawner: ctx.spawner.clone(),
            })
            .await?;
        let shared = Arc::new(Shared {
            device: self.device,
            partition,
            io: MonitoredIoEngine::new(io, ctx.metrics.clone()),
            bucket_size: self.bucket_size,
            blooms: (0..buckets).map(|_| AtomicU64::new(0)).collect(),
            compression: self.compression,
            admission: self.admission.with_condition(IoThrottle),
            metrics: ctx.metrics,
        });
        let shards = self.shards.min(buckets);
        stream::iter(0..buckets)
            .map(|id| {
                let shared = shared.clone();
                async move {
                    if matches!(ctx.recover_mode, RecoverMode::None) {
                        shared.write(id, &[]).await?;
                    } else {
                        match shared.read(id).await {
                            Ok(entries) => {
                                shared.blooms[id].store(bucket::filter(&entries), Ordering::Relaxed);
                            }
                            Err(e) if matches!(ctx.recover_mode, RecoverMode::Quiet) => {
                                tracing::warn!(id, ?e, "[small engine]: skip unrecoverable bucket");
                                // Repair it before accepting mutations; a failed repair must be reported.
                                shared.write(id, &[]).await?;
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    Ok::<_, Error>(())
                }
            })
            .buffer_unordered(shards)
            .try_collect::<Vec<_>>()
            .await?;
        let workers: Vec<_> = (0..shards)
            .map(|_| {
                Arc::new(Shard {
                    state: Mutex::new(State {
                        queue: VecDeque::new(),
                        resets: BTreeSet::new(),
                        poisoned: BTreeSet::new(),
                        clear_next: None,
                        bytes: 0,
                        busy: false,
                        closed: false,
                        stopped: false,
                        failure: None,
                    }),
                    wake: Notify::new(),
                    progress: Notify::new(),
                    io: AsyncMutex::new(()),
                    queue_capacity: self.queue_capacity,
                    queue_bytes: self.queue_bytes,
                    stride: shards,
                })
            })
            .collect();
        for shard in &workers {
            let shard = shard.clone();
            let shared = shared.clone();
            ctx.spawner.spawn(async move {
                shard.run(shared).await;
            });
        }
        Ok(Arc::new(SmallEngine {
            shared,
            shards: workers,
            _spawner: ctx.spawner,
        }))
    }
}

impl<K: StorageKey, V: StorageValue, P: Properties> EngineConfig<K, V, P> for SmallEngineConfig {
    fn build(self: Box<Self>, ctx: EngineBuildContext) -> BoxFuture<'static, Result<Arc<dyn Engine<K, V, P>>>> {
        self.open(ctx).boxed()
    }
}

impl<K: StorageKey, V: StorageValue, P: Properties> From<SmallEngineConfig> for Box<dyn EngineConfig<K, V, P>> {
    fn from(config: SmallEngineConfig) -> Self {
        Box::new(config)
    }
}

#[derive(Debug)]
struct Shared {
    device: Arc<dyn Device>,
    partition: Arc<dyn Partition>,
    io: Arc<dyn IoEngine>,
    bucket_size: usize,
    blooms: Vec<AtomicU64>,
    compression: Compression,
    admission: StorageFilter,
    metrics: Arc<Metrics>,
}

impl Shared {
    fn bucket(&self, hash: u64) -> usize {
        (hash % self.blooms.len() as u64) as usize
    }
    fn contains(&self, hash: u64) -> bool {
        let bits = bucket::bloom(hash);
        self.blooms[self.bucket(hash)].load(Ordering::Acquire) & bits == bits
    }
    async fn read(&self, id: usize) -> Result<Vec<bucket::Entry>> {
        let (buf, res) = self
            .io
            .read(
                Box::new(IoSliceMut::new(self.bucket_size)),
                self.partition.as_ref(),
                (id * self.bucket_size) as u64,
            )
            .await;
        res?;
        bucket::decode(&buf, id, self.blooms.len())
    }
    async fn write(&self, id: usize, entries: &[bucket::Entry]) -> Result<()> {
        let bytes = bucket::encode(entries, self.bucket_size, id, self.blooms.len());
        let (_, res) = self
            .io
            .write(Box::new(bytes), self.partition.as_ref(), (id * self.bucket_size) as u64)
            .await;
        res
    }
}

#[derive(Debug)]
struct Mutation<K: StorageKey, V, P> {
    hash: u64,
    bucket: usize,
    piece: Option<PieceRef<K, V, P>>,
    charge: usize,
}

#[derive(Debug)]
struct State<K: StorageKey, V, P> {
    queue: VecDeque<Mutation<K, V, P>>,
    // Deduplicated by bucket, bounded by device capacity, with no retained values.
    resets: BTreeSet<usize>,
    // A failed write may leave the previous image intact. Do not read it into
    // a subsequent mutation and accidentally resurrect a deleted/replaced key.
    poisoned: BTreeSet<usize>,
    // Sweep destroy without allocating a reset-set entry for every disk bucket.
    clear_next: Option<usize>,
    bytes: usize,
    busy: bool,
    closed: bool,
    stopped: bool,
    failure: Option<String>,
}

#[derive(Debug)]
struct Shard<K: StorageKey, V, P> {
    state: Mutex<State<K, V, P>>,
    wake: Notify,
    progress: Notify,
    io: AsyncMutex<()>,
    queue_capacity: usize,
    queue_bytes: usize,
    stride: usize,
}

impl<K: StorageKey, V: StorageValue, P: Properties> Shard<K, V, P> {
    fn submit(&self, shared: &Shared, mutation: Mutation<K, V, P>) {
        let mut state = self.state.lock();
        if state.closed {
            return;
        }
        shared.blooms[mutation.bucket].store(0, Ordering::Release);
        if state.queue.len() >= self.queue_capacity || mutation.charge > self.queue_bytes.saturating_sub(state.bytes) {
            state.resets.insert(mutation.bucket);
            let mut removed = 0;
            state.queue.retain(|queued| {
                if queued.bucket == mutation.bucket {
                    removed += queued.charge;
                    false
                } else {
                    true
                }
            });
            state.bytes -= removed;
            shared.metrics.storage_small_engine_enqueue_skip.increase(1);
        } else {
            state.bytes += mutation.charge;
            state.queue.push_back(mutation);
        }
        self.wake.notify_one();
    }

    async fn drained(&self, stopped: bool) -> Result<()> {
        loop {
            let notified = self.progress.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let state = self.state.lock();
                if !state.busy
                    && state.queue.is_empty()
                    && state.resets.is_empty()
                    && state.clear_next.is_none()
                    && (!stopped || state.stopped)
                {
                    return match &state.failure {
                        Some(message) => Err(Error::new(ErrorKind::Io, message.clone())),
                        None => Ok(()),
                    };
                }
            }
            notified.await;
        }
    }

    async fn run(self: Arc<Self>, shared: Arc<Shared>) {
        loop {
            let wake = self.wake.notified();
            let work = {
                let mut state = self.state.lock();
                let work = if let Some(id) = state.clear_next {
                    let next = id + self.stride;
                    state.clear_next = (next < shared.blooms.len()).then_some(next);
                    Some((id, None))
                } else if let Some(id) = state.resets.pop_first() {
                    Some((id, None))
                } else if let Some(mutation) = state.queue.pop_front() {
                    state.bytes -= mutation.charge;
                    Some((mutation.bucket, Some(mutation)))
                } else {
                    None
                };
                state.busy = work.is_some();
                if work.is_none() && state.closed {
                    state.stopped = true;
                    self.progress.notify_waiters();
                    return;
                }
                work
            };
            let Some((id, mutation)) = work else {
                wake.await;
                continue;
            };
            let _io = self.io.lock().await;
            let result = self.apply(&shared, id, mutation.as_ref()).await;
            {
                let mut state = self.state.lock();
                match result {
                    Ok(bits) => {
                        state.poisoned.remove(&id);
                        if !state.resets.contains(&id)
                            && state.clear_next.is_none_or(|next| id < next)
                            && !state.queue.iter().any(|m| m.bucket == id)
                        {
                            shared.blooms[id].store(bits, Ordering::Release);
                        }
                    }
                    Err(e) => {
                        state.poisoned.insert(id);
                        shared.blooms[id].store(0, Ordering::Release);
                        shared.metrics.storage_error.increase(1);
                        tracing::warn!(id, ?e, "[small engine]: mutation failed");
                        state.failure.get_or_insert_with(|| e.to_string());
                    }
                }
                // Release keeper registrations before making the drain observable.
                drop(mutation);
                state.busy = false;
            }
            self.progress.notify_waiters();
        }
    }

    async fn apply(&self, shared: &Shared, id: usize, mutation: Option<&Mutation<K, V, P>>) -> Result<u64> {
        let Some(mutation) = mutation else {
            shared.write(id, &[]).await?;
            shared.metrics.storage_small_engine_bucket_reset.increase(1);
            return Ok(0);
        };
        let poisoned = self.state.lock().poisoned.contains(&id);
        let mut entries = if poisoned {
            vec![]
        } else {
            match shared.read(id).await {
                Ok(entries) => entries,
                Err(e)
                    if matches!(
                        e.kind(),
                        ErrorKind::ChecksumMismatch | ErrorKind::Parse | ErrorKind::MagicMismatch
                    ) =>
                {
                    vec![]
                }
                Err(e) => return Err(e),
            }
        };
        entries.retain(|entry| entry.hash != mutation.hash);
        if let Some(piece) = &mutation.piece {
            match bucket::Entry::encode(
                mutation.hash,
                piece.key(),
                piece.value(),
                shared.compression,
                shared.bucket_size - bucket::HEADER,
            ) {
                Ok(entry) => {
                    let before = entries.len();
                    bucket::insert(&mut entries, entry, shared.bucket_size);
                    shared
                        .metrics
                        .storage_small_engine_evict
                        .increase((before + 1 - entries.len()) as u64);
                }
                Err(e) => {
                    // Rejected replacements still remove an older disk value.
                    tracing::debug!(?e, "[small engine]: entry cannot be encoded into a bucket");
                    shared.metrics.storage_small_engine_enqueue_skip.increase(1);
                }
            }
        }
        shared.write(id, &entries).await?;
        Ok(bucket::filter(&entries))
    }
}

struct SmallEngine<K: StorageKey, V, P> {
    shared: Arc<Shared>,
    shards: Vec<Arc<Shard<K, V, P>>>,
    _spawner: Spawner,
}

impl<K: StorageKey, V, P> Debug for SmallEngine<K, V, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SmallEngine")
            .field("buckets", &self.shared.blooms.len())
            .field("shards", &self.shards.len())
            .finish()
    }
}

impl<K: StorageKey, V, P> Drop for SmallEngine<K, V, P> {
    fn drop(&mut self) {
        for shard in &self.shards {
            shard.state.lock().closed = true;
            shard.wake.notify_one();
        }
    }
}

impl<K: StorageKey, V: StorageValue, P: Properties> Engine<K, V, P> for SmallEngine<K, V, P> {
    fn device(&self) -> Option<&Arc<dyn Device>> {
        Some(&self.shared.device)
    }
    fn statistics(&self) -> &Arc<Statistics> {
        self.shared.device.statistics()
    }
    fn filter(&self, hash: u64, estimated_size: usize) -> StorageFilterResult {
        self.shared.admission.filter(self.statistics(), hash, estimated_size)
    }
    fn enqueue(&self, piece: PieceRef<K, V, P>, estimated_size: usize) {
        let hash = piece.hash();
        let id = self.shared.bucket(hash);
        self.shards[id % self.shards.len()].submit(
            &self.shared,
            Mutation {
                hash,
                bucket: id,
                piece: Some(piece),
                charge: estimated_size.max(64),
            },
        );
    }
    fn delete(&self, hash: u64) {
        let id = self.shared.bucket(hash);
        self.shards[id % self.shards.len()].submit(
            &self.shared,
            Mutation {
                hash,
                bucket: id,
                piece: None,
                charge: 64,
            },
        );
    }
    fn may_contains(&self, hash: u64) -> bool {
        self.shared.contains(hash)
    }
    fn load(&self, hash: u64) -> BoxFuture<'static, Result<Load<K, V, P>>> {
        let shared = self.shared.clone();
        let id = shared.bucket(hash);
        let shard = self.shards[id % self.shards.len()].clone();
        async move {
            if !shared.contains(hash) {
                return Ok(Load::Miss);
            }
            if shared.device.statistics().is_read_throttled() {
                return Ok(Load::Throttled);
            }
            let _io = shard.io.lock().await;
            if !shared.contains(hash) {
                return Ok(Load::Miss);
            }
            let entries = shared.read(id).await?;
            let Some(entry) = entries.into_iter().find(|entry| entry.hash == hash) else {
                return Ok(Load::Miss);
            };
            let (key, value) =
                EntryDeserializer::deserialize(&entry.data, entry.key_len, entry.value_len, entry.compression, None)?;
            // Serialize the final visibility check with synchronous invalidation.
            let _state = shard.state.lock();
            if !shared.contains(hash) {
                return Ok(Load::Miss);
            }
            Ok(Load::Entry {
                key,
                value,
                populated: Populated { age: Age::Young },
            })
        }
        .boxed()
    }
    fn wait(&self) -> BoxFuture<'static, ()> {
        let shards = self.shards.clone();
        async move {
            for shard in shards {
                let _ = shard.drained(false).await;
            }
        }
        .boxed()
    }
    fn close(&self) -> BoxFuture<'static, Result<()>> {
        let shards = self.shards.clone();
        async move {
            for shard in &shards {
                shard.state.lock().closed = true;
                shard.wake.notify_one();
            }
            let mut error = None;
            for shard in shards {
                if let Err(e) = shard.drained(true).await {
                    error.get_or_insert(e);
                }
            }
            error.map_or(Ok(()), Err)
        }
        .boxed()
    }
    fn destroy(&self) -> BoxFuture<'static, Result<()>> {
        let shards = self.shards.clone();
        let shared = self.shared.clone();
        async move {
            for (index, shard) in shards.iter().enumerate() {
                let mut state = shard.state.lock();
                if state.closed {
                    return Err(Error::new(ErrorKind::Closed, "small-object engine is closed"));
                }
                state.queue.clear();
                state.bytes = 0;
                state.resets.clear();
                state.clear_next = Some(index);
                for id in (index..shared.blooms.len()).step_by(shards.len()) {
                    shared.blooms[id].store(0, Ordering::Release);
                }
                shard.wake.notify_one();
            }
            let mut error = None;
            for shard in shards {
                if let Err(e) = shard.drained(false).await {
                    error.get_or_insert(e);
                }
            }
            error.map_or(Ok(()), Err)
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests;
