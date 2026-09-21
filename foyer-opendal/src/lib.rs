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

//! Experimental OpenDAL-backed secondary cache for immutable content.
//!
//! This engine supports `String` keys and `Vec<u8>` values with a single writer,
//! a process-local index, and FIFO eviction. Each key must always identify the
//! same bytes, including the source namespace, object version, and range.
//! A source fetch must read that exact version.
//!
//! Use a fresh, exclusive namespace for each cache instance and
//! [`RecoverMode::None`]. Restart recovery and shared writers are not supported.
//! Failed cache writes are discarded; a successful source fetch is not failed by
//! admission rejection. Timed-out writes and dropped deletes can leave objects
//! for the caller or operator to remove with the namespace.
//!
//! `capacity` bounds indexed encoded bytes and, unless overridden, the maximum
//! encoded object size. `max_object_size` is compared only with encoded object
//! bytes. `queue_limit` bounds admitted work using a per-entry accounting charge
//! of at least 64 bytes so tiny entries cannot grow the queue without bound; that
//! charge is not an object-size limit. Concurrent whole-object reads and the
//! single in-flight write buffer are capped. These limits do not measure process
//! RSS or physical backend usage.
//!
//! [`Engine::wait`] drains commands admitted before the barrier and does not
//! report their errors. [`Engine::close`] rejects new work, drains already-admitted
//! commands (bounded by the command-slot limit and the I/O timeout), and returns
//! recorded background failures. I/O statistics count successful object reads
//! and writes and their encoded bytes, excluding deletes, failed calls, and
//! backend-internal retries. No block device or I/O engine is used.

mod codec;
mod namespace;

use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use foyer::HybridCacheProperties;
use foyer_common::{
    error::{Error, ErrorKind, Result},
    properties::Age,
};
use foyer_storage::{
    Engine, EngineBuildContext, EngineConfig, Load, PieceRef, Populated, RecoverMode, Statistics, StorageFilterResult,
    Throttle,
};
use futures_util::future::BoxFuture;
use opendal_core::Operator;
use parking_lot::Mutex;
use tokio::sync::{Semaphore, mpsc, oneshot};

type Piece = PieceRef<String, Vec<u8>, HybridCacheProperties>;
type CacheEngine = dyn Engine<String, Vec<u8>, HybridCacheProperties>;

/// Minimum byte charge so empty keys cannot create unbounded metadata or work.
const MIN_CHARGE: usize = 64;
const IO_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_ERROR_RECORDS: usize = 8;
const MAX_COMMANDS: usize = 32;
const MAX_ENTRIES: usize = 65_536;
const MAX_CONCURRENT_READS: usize = 8;
const MAX_INLINE_DELETES: usize = 8;

fn external(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::External, message.into())
}

fn count_limit(bytes: usize, cap: usize) -> usize {
    (bytes / MIN_CHARGE).clamp(1, cap)
}

fn encoded_len(key: &str, value: &[u8]) -> Option<usize> {
    codec::HEADER_LEN.checked_add(key.len())?.checked_add(value.len())
}

fn estimated_encoded_len(estimated_size: usize) -> usize {
    // Foyer estimates include two `usize` length prefixes; the codec uses a fixed header.
    estimated_size
        .saturating_sub(2 * std::mem::size_of::<usize>())
        .saturating_add(codec::HEADER_LEN)
}

fn queue_charge(encoded: usize, queue_limit: usize) -> usize {
    if encoded > queue_limit {
        encoded
    } else {
        encoded.max(MIN_CHARGE).min(queue_limit)
    }
}

fn read_bound(max_object_size: usize) -> u64 {
    u64::try_from(max_object_size)
        .ok()
        .and_then(|n| n.checked_add(1))
        .unwrap_or(u64::MAX)
}

fn object_path(namespace: &str, hash: u64, sequence: u64) -> String {
    format!("{namespace}/{hash:016x}/{sequence:016x}")
}

fn record_failure(state: &mut State, error: impl ToString) {
    if state.failures.len() >= MAX_ERROR_RECORDS {
        state.failures.pop_front();
        state.dropped_failures = state.dropped_failures.saturating_add(1);
    }
    state.failures.push_back(error.to_string());
}

#[derive(Debug, Clone)]
struct Object {
    key: String,
    path: String,
    size: usize,
    sequence: u64,
}

#[derive(Debug)]
struct Pending {
    sequence: u64,
    // One keeper registration per logical write, not one per waiting caller.
    piece: Piece,
}

#[derive(Debug, Default)]
struct State {
    next: u64,
    // Also serializes mutation publication with command insertion.
    pending: HashMap<u64, Pending>,
    objects: HashMap<u64, Object>,
    fifo: VecDeque<(u64, u64)>,
    indexed_bytes: usize,
    failures: VecDeque<String>,
    dropped_failures: usize,
    closed: bool,
}

impl State {
    fn take_object(&mut self, hash: u64) -> Option<Object> {
        self.fifo.retain(|(key, _)| *key != hash);
        let object = self.objects.remove(&hash)?;
        self.indexed_bytes = self.indexed_bytes.saturating_sub(object.size);
        Some(object)
    }

    fn evict_until(
        &mut self,
        extra_bytes: usize,
        extra_objects: usize,
        capacity: usize,
        max_entries: usize,
    ) -> Vec<String> {
        let mut removed = Vec::new();
        while self.indexed_bytes.saturating_add(extra_bytes) > capacity
            || self.objects.len().saturating_add(extra_objects) > max_entries
        {
            let Some((victim, version)) = self.fifo.pop_front() else {
                break;
            };
            if self
                .objects
                .get(&victim)
                .is_some_and(|object| object.sequence == version)
                && let Some(object) = self.take_object(victim)
            {
                removed.push(object.path);
            }
        }
        removed
    }
}

#[derive(Debug)]
struct Shared {
    op: Operator,
    namespace: String,
    capacity: usize,
    queue_limit: usize,
    max_object_size: usize,
    max_entries: usize,
    timeout: Duration,
    state: Mutex<State>,
    queued_bytes: AtomicUsize,
    reads: Semaphore,
    statistics: Arc<Statistics>,
}

impl Shared {
    fn fail(&self, error: impl ToString) {
        record_failure(&mut self.state.lock(), error);
    }

    fn owns_path(&self, path: &str) -> bool {
        path.starts_with(&self.namespace) && path.as_bytes().get(self.namespace.len()) == Some(&b'/')
    }

    fn request_delete(&self, tx: &mpsc::Sender<Command>, path: String) {
        if !self.owns_path(&path) {
            self.fail("refusing to delete a path outside the cache namespace");
            return;
        }
        if tx.try_send(Command::Delete(vec![path])).is_err() {
            self.fail("cleanup dropped because the command queue is full");
        }
    }

    fn request_deletes(&self, tx: &mpsc::Sender<Command>, paths: Vec<String>) {
        for path in paths {
            self.request_delete(tx, path);
        }
    }

    async fn delete_object(&self, path: &str) {
        if !self.owns_path(path) {
            self.fail("refusing to delete a path outside the cache namespace");
            return;
        }
        match tokio::time::timeout(self.timeout, self.op.delete(path)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) if e.kind() == opendal_core::ErrorKind::NotFound => {}
            Ok(Err(e)) => self.fail(e),
            Err(e) => self.fail(e),
        }
    }

    async fn delete_paths(&self, paths: Vec<String>) {
        for (index, path) in paths.into_iter().enumerate() {
            if index >= MAX_INLINE_DELETES {
                self.fail("cleanup dropped after the in-flight delete bound");
                break;
            }
            self.delete_object(&path).await;
        }
    }

    fn forget_object(&self, hash: u64, sequence: u64) -> Option<Object> {
        let mut state = self.state.lock();
        if state
            .objects
            .get(&hash)
            .is_some_and(|object| object.sequence == sequence)
        {
            state.take_object(hash)
        } else {
            None
        }
    }

    fn release_queued(&self, n: usize) {
        let _ = self
            .queued_bytes
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| Some(v.saturating_sub(n)));
    }

    fn abandon_inflight(&self) {
        self.state.lock().pending.clear();
        self.queued_bytes.store(0, Ordering::SeqCst);
    }

    fn finish_put(&self, hash: u64, sequence: u64, reserved: usize) {
        {
            let mut state = self.state.lock();
            if state
                .pending
                .get(&hash)
                .is_some_and(|pending| pending.sequence == sequence)
            {
                state.pending.remove(&hash);
            }
        }
        self.release_queued(reserved);
    }

    async fn put(&self, hash: u64, sequence: u64, reserved: usize) {
        let piece = self
            .state
            .lock()
            .pending
            .get(&hash)
            .filter(|pending| pending.sequence == sequence)
            .map(|pending| (*pending.piece).clone());
        if let Some(piece) = piece {
            let path = object_path(&self.namespace, hash, sequence);
            match codec::encode(piece.key(), piece.value(), self.max_object_size) {
                Ok(bytes) => {
                    let size = bytes.len();
                    match tokio::time::timeout(self.timeout, self.op.write(&path, bytes)).await {
                        Ok(Ok(_)) => {
                            self.statistics.record_disk_write(size);
                            let removed = self.publish(hash, sequence, piece.key().clone(), path.clone(), size);
                            self.delete_paths(removed).await;
                        }
                        Ok(Err(e)) => self.fail(e),
                        Err(e) => {
                            // The write may have succeeded remotely; drop the logical entry
                            // and try one bounded delete. Residual objects belong to the operator.
                            self.fail(e);
                            self.delete_object(&path).await;
                        }
                    }
                }
                Err(e) => self.fail(e),
            }
        }
        self.finish_put(hash, sequence, reserved);
    }

    fn publish(&self, hash: u64, sequence: u64, key: String, path: String, size: usize) -> Vec<String> {
        let mut state = self.state.lock();
        if !state
            .pending
            .get(&hash)
            .is_some_and(|pending| pending.sequence == sequence)
        {
            return vec![path];
        }
        let mut removed = Vec::new();
        if let Some(old) = state.take_object(hash) {
            removed.push(old.path);
        }
        removed.extend(state.evict_until(size, 1, self.capacity, self.max_entries));
        if state.indexed_bytes.saturating_add(size) <= self.capacity && state.objects.len() < self.max_entries {
            state.objects.insert(
                hash,
                Object {
                    key,
                    path,
                    size,
                    sequence,
                },
            );
            state.indexed_bytes = state.indexed_bytes.saturating_add(size);
            state.fifo.push_back((hash, sequence));
        } else {
            removed.push(path);
        }
        removed
    }
}

enum Command {
    Put { hash: u64, sequence: u64, reserved: usize },
    Delete(Vec<String>),
    Barrier(oneshot::Sender<()>),
    Stop(oneshot::Sender<()>),
}

#[derive(Debug)]
struct OpenDalEngine {
    shared: Arc<Shared>,
    tx: mpsc::Sender<Command>,
}

/// Configuration for the experimental OpenDAL secondary cache.
///
/// Only immutable `String` keys and `Vec<u8>` values are supported. The caller
/// must supply an unused namespace exclusively owned by this cache instance.
#[derive(Debug)]
pub struct OpenDalEngineConfig {
    op: Operator,
    namespace: String,
    capacity: usize,
    queue_limit: usize,
    max_object_size: Option<usize>,
}

impl OpenDalEngineConfig {
    /// Create a cache configuration using a caller-supplied OpenDAL operator.
    ///
    /// `capacity` limits indexed encoded bytes and defaults as the maximum
    /// encoded object size. `queue_limit` limits admitted work; each queued write
    /// charges at least 64 bytes so tiny entries cannot grow without bound. That
    /// charge is not applied to `max_object_size`, which is exact encoded bytes.
    /// Both capacity and queue_limit must be nonzero. Command slots, indexed
    /// entry counts, retained error records, concurrent reads, and in-flight
    /// cleanup deletes are derived from these limits and fixed ceilings.
    /// They do not include process RSS or physical backend usage.
    ///
    /// The namespace must be a nonempty relative path. Validation rejects
    /// leading or trailing whitespace, NUL bytes, backslashes, empty path
    /// components, and `.` or `..` segments. The operator must support reading,
    /// writing and deleting objects. Each I/O has a two-second timeout. Only
    /// [`RecoverMode::None`] is accepted.
    ///
    /// [`Engine::wait`] drains earlier commands and does not report failures.
    /// [`Engine::close`] rejects new work, drains already-admitted commands,
    /// and returns a bounded set of background write/cleanup errors.
    pub fn new(op: Operator, namespace: String, capacity: usize, queue_limit: usize) -> Self {
        Self {
            op,
            namespace,
            capacity,
            queue_limit,
            max_object_size: None,
        }
    }

    /// Limit one encoded object, and therefore one write or read buffer, to `bytes`.
    ///
    /// Compared with the encoded object only; the queue's 64-byte minimum charge
    /// does not apply. Must be nonzero and at most `capacity`. Defaults to `capacity`.
    pub fn with_max_object_size(mut self, bytes: usize) -> Self {
        self.max_object_size = Some(bytes);
        self
    }
}

impl From<OpenDalEngineConfig> for Box<dyn EngineConfig<String, Vec<u8>, HybridCacheProperties>> {
    fn from(config: OpenDalEngineConfig) -> Self {
        Box::new(config)
    }
}

impl EngineConfig<String, Vec<u8>, HybridCacheProperties> for OpenDalEngineConfig {
    fn build(self: Box<Self>, ctx: EngineBuildContext) -> BoxFuture<'static, Result<Arc<CacheEngine>>> {
        Box::pin(async move {
            if !matches!(ctx.recover_mode, RecoverMode::None) {
                return Err(Error::new(
                    ErrorKind::Config,
                    "OpenDAL cache requires RecoverMode::None and an exclusive fresh namespace",
                ));
            }
            if self.capacity == 0 || self.queue_limit == 0 {
                return Err(Error::new(
                    ErrorKind::Config,
                    "cache capacity and queue limit must be nonzero",
                ));
            }
            namespace::validate(&self.namespace)?;
            let max_object_size = self.max_object_size.unwrap_or(self.capacity);
            if max_object_size == 0 || max_object_size > self.capacity {
                return Err(Error::new(
                    ErrorKind::Config,
                    "max object size must be nonzero and at most cache capacity",
                ));
            }
            let max_entries = count_limit(self.capacity, MAX_ENTRIES);
            let max_commands = count_limit(self.queue_limit, MAX_COMMANDS);
            let shared = Arc::new(Shared {
                op: self.op,
                namespace: self.namespace,
                capacity: self.capacity,
                queue_limit: self.queue_limit,
                max_object_size,
                max_entries,
                timeout: IO_TIMEOUT,
                state: Mutex::new(State::default()),
                queued_bytes: AtomicUsize::new(0),
                reads: Semaphore::new(MAX_CONCURRENT_READS),
                statistics: Arc::new(Statistics::new(Throttle::default())),
            });
            let (tx, mut receiver) = mpsc::channel(max_commands);
            let engine = Arc::new(OpenDalEngine {
                shared: shared.clone(),
                tx,
            });
            ctx.spawner.spawn(async move {
                while let Some(command) = receiver.recv().await {
                    match command {
                        Command::Put {
                            hash,
                            sequence,
                            reserved,
                        } => shared.put(hash, sequence, reserved).await,
                        Command::Delete(paths) => shared.delete_paths(paths).await,
                        Command::Barrier(done) => {
                            let _ = done.send(());
                        }
                        Command::Stop(done) => {
                            while let Ok(command) = receiver.try_recv() {
                                match command {
                                    Command::Put {
                                        hash,
                                        sequence,
                                        reserved,
                                    } => shared.finish_put(hash, sequence, reserved),
                                    Command::Delete(_) => {
                                        shared.fail("cleanup dropped because the cache is closing");
                                    }
                                    Command::Barrier(done) | Command::Stop(done) => {
                                        let _ = done.send(());
                                    }
                                }
                            }
                            let _ = done.send(());
                            break;
                        }
                    }
                }
            });
            Ok(engine as Arc<CacheEngine>)
        })
    }
}

impl Engine<String, Vec<u8>, HybridCacheProperties> for OpenDalEngine {
    fn statistics(&self) -> &Arc<Statistics> {
        &self.shared.statistics
    }

    fn filter(&self, _: u64, estimated_size: usize) -> StorageFilterResult {
        // Reject only entries whose encoded size cannot fit. Queue/count misses
        // are handled in `enqueue`: Store treats filter rejection as a delete.
        if self.shared.state.lock().closed || estimated_encoded_len(estimated_size) > self.shared.max_object_size {
            StorageFilterResult::Reject
        } else {
            StorageFilterResult::Admit
        }
    }

    fn enqueue(&self, piece: Piece, _estimated_size: usize) {
        let hash = piece.hash();
        let Some(encoded) = encoded_len(piece.key(), piece.value()) else {
            return;
        };
        if encoded > self.shared.max_object_size {
            return;
        }
        let reserved = queue_charge(encoded, self.shared.queue_limit);
        if reserved > self.shared.queue_limit {
            return;
        }
        let mut deletes = Vec::new();
        {
            let mut state = self.shared.state.lock();
            if state.closed {
                return;
            }
            if let Some(pending) = state.pending.get_mut(&hash)
                && pending.piece.key() == piece.key()
            {
                // Engine arrival order can differ from keeper registration order.
                if piece.is_current() {
                    pending.piece = piece;
                }
                return;
            }
            if state
                .objects
                .get(&hash)
                .is_some_and(|object| &object.key == piece.key())
            {
                return;
            }
            if self
                .shared
                .queued_bytes
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                    n.checked_add(reserved).filter(|n| *n <= self.shared.queue_limit)
                })
                .is_err()
            {
                return;
            }
            let replacing = state.pending.contains_key(&hash) || state.objects.contains_key(&hash);
            if !replacing {
                deletes.extend(state.evict_until(0, 1, self.shared.capacity, self.shared.max_entries));
            }
            if !replacing && state.pending.len().saturating_add(state.objects.len()) >= self.shared.max_entries {
                self.shared.release_queued(reserved);
            } else {
                state.next += 1;
                let sequence = state.next;
                if let Some(old) = state.take_object(hash) {
                    deletes.push(old.path);
                }
                state.pending.insert(hash, Pending { piece, sequence });
                if self
                    .tx
                    .try_send(Command::Put {
                        hash,
                        sequence,
                        reserved,
                    })
                    .is_err()
                {
                    state.pending.remove(&hash);
                    self.shared.release_queued(reserved);
                    record_failure(&mut state, "write dropped because the command queue is full");
                }
            }
        }
        self.shared.request_deletes(&self.tx, deletes);
    }

    fn load(&self, hash: u64) -> BoxFuture<'static, Result<Load<String, Vec<u8>, HybridCacheProperties>>> {
        let shared = self.shared.clone();
        let tx = self.tx.clone();
        Box::pin(async move {
            let Some(object) = shared.state.lock().objects.get(&hash).cloned() else {
                return Ok(Load::Miss);
            };
            let permit = match tokio::time::timeout(shared.timeout, shared.reads.acquire()).await {
                Ok(Ok(permit)) => permit,
                Ok(Err(_)) => return Err(external("read semaphore closed")),
                Err(_) => return Ok(Load::Throttled),
            };
            let limit = read_bound(shared.max_object_size);
            let bytes =
                match tokio::time::timeout(shared.timeout, shared.op.read_with(&object.path).range(0..limit)).await {
                    Ok(Ok(bytes)) => bytes.to_vec(),
                    Ok(Err(e)) if e.kind() == opendal_core::ErrorKind::NotFound => {
                        drop(permit);
                        shared.forget_object(hash, object.sequence);
                        return Ok(Load::Miss);
                    }
                    Ok(Err(e)) => return Err(external(e.to_string())),
                    Err(e) => return Err(external(e.to_string())),
                };
            drop(permit);
            if bytes.len() > shared.max_object_size {
                if let Some(object) = shared.forget_object(hash, object.sequence) {
                    shared.request_delete(&tx, object.path);
                }
                return Err(Error::new(
                    ErrorKind::OutOfRange,
                    "cache object exceeds max object size",
                ));
            }
            shared.statistics.record_disk_read(bytes.len());
            let value = match codec::decode(&bytes, &object.key, shared.max_object_size) {
                Ok(value) => value,
                Err(error) => {
                    if let Some(object) = shared.forget_object(hash, object.sequence) {
                        shared.request_delete(&tx, object.path);
                    }
                    return Err(error);
                }
            };
            let current = shared
                .state
                .lock()
                .objects
                .get(&hash)
                .is_some_and(|o| o.sequence == object.sequence);
            if !current {
                return Ok(Load::Miss);
            }
            Ok(Load::Entry {
                key: object.key,
                value,
                populated: Populated { age: Age::Young },
            })
        })
    }

    fn delete(&self, hash: u64) {
        let path = {
            let mut state = self.shared.state.lock();
            if state.closed {
                return;
            }
            state.pending.remove(&hash);
            state.take_object(hash).map(|object| object.path)
        };
        if let Some(path) = path {
            self.shared.request_delete(&self.tx, path);
        }
    }

    fn may_contains(&self, hash: u64) -> bool {
        self.shared.state.lock().objects.contains_key(&hash)
    }

    fn destroy(&self) -> BoxFuture<'static, Result<()>> {
        let shared = self.shared.clone();
        let tx = self.tx.clone();
        Box::pin(async move {
            let paths = {
                let mut state = shared.state.lock();
                if state.closed {
                    return Err(Error::new(ErrorKind::Closed, "opendal cache is closed"));
                }
                state.pending.clear();
                state.fifo.clear();
                state.indexed_bytes = 0;
                state.objects.drain().map(|(_, object)| object.path).collect::<Vec<_>>()
            };
            if !paths.is_empty() {
                let _ = tx.send(Command::Delete(paths)).await;
            }
            let (done, rx) = oneshot::channel();
            if tx.send(Command::Barrier(done)).await.is_err() {
                shared.queued_bytes.store(0, Ordering::SeqCst);
                return Err(external("opendal cache worker closed"));
            }
            rx.await.map_err(|e| {
                shared.queued_bytes.store(0, Ordering::SeqCst);
                external(e.to_string())
            })
        })
    }

    fn wait(&self) -> BoxFuture<'static, ()> {
        let tx = self.tx.clone();
        Box::pin(async move {
            let (done, rx) = oneshot::channel();
            if tx.send(Command::Barrier(done)).await.is_err() {
                return;
            }
            let _ = rx.await;
        })
    }

    fn close(&self) -> BoxFuture<'static, Result<()>> {
        let shared = self.shared.clone();
        let tx = self.tx.clone();
        Box::pin(async move {
            {
                let mut state = shared.state.lock();
                if state.closed {
                    return Ok(());
                }
                state.closed = true;
            }
            let (done, rx) = oneshot::channel();
            if tx.send(Command::Stop(done)).await.is_err() {
                shared.abandon_inflight();
                return Err(external("opendal cache worker closed"));
            }
            if let Err(e) = rx.await {
                shared.abandon_inflight();
                return Err(external(e.to_string()));
            }
            let (failures, dropped) = {
                let state = shared.state.lock();
                (
                    state.failures.iter().cloned().collect::<Vec<_>>(),
                    state.dropped_failures,
                )
            };
            if failures.is_empty() {
                Ok(())
            } else {
                let mut message = failures.join("; ");
                if dropped > 0 {
                    message = format!("{message}; {dropped} older errors dropped");
                }
                Err(external(message))
            }
        })
    }
}
