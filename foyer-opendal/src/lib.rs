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
//! This draft supports `String` keys and `Vec<u8>` values with a single writer,
//! a process-local index, and FIFO eviction. Each key must always identify the
//! same bytes, including the source namespace, object version, and range.
//! A source fetch must read that exact version.
//!
//! Use a fresh, exclusive namespace for each cache instance and
//! [`RecoverMode::None`]. Restart recovery and shared writers are not supported.
//! Logical capacity and queued payload bytes do not bound process memory or
//! physical backend usage. Timed-out writes and failed deletes can leave objects.
//! The no-op device only satisfies the existing [`Engine`] interface.

use foyer::{Code, HybridCacheProperties};
use foyer_common::{
    error::{Error, ErrorKind, Result},
    properties::Age,
};
use foyer_storage::{
    Device, DeviceBuilder, Engine, EngineBuildContext, EngineConfig, Load, NoopDeviceBuilder, PieceRef, Populated,
    RecoverMode, StorageFilterResult,
};
use futures_util::future::BoxFuture;
use opendal_core::Operator;
use parking_lot::Mutex;
use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};

type Piece = PieceRef<String, Vec<u8>, HybridCacheProperties>;
type CacheEngine = dyn Engine<String, Vec<u8>, HybridCacheProperties>;

fn external(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::External, message.into())
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
    failures: Vec<String>,
    closed: bool,
}

#[derive(Debug)]
struct Shared {
    op: Operator,
    namespace: String,
    capacity: usize,
    queue_limit: usize,
    timeout: Duration,
    state: Mutex<State>,
    queued_bytes: AtomicUsize,
}

impl Shared {
    fn fail(&self, error: impl ToString) {
        self.state.lock().failures.push(error.to_string());
    }

    async fn delete_object(&self, path: &str) {
        match tokio::time::timeout(self.timeout, self.op.delete(path)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) if e.kind() == opendal_core::ErrorKind::NotFound => {}
            Ok(Err(e)) => self.fail(e),
            Err(e) => self.fail(e),
        }
    }

    fn forget_object(&self, hash: u64, sequence: u64) -> Option<Object> {
        let mut state = self.state.lock();
        if state.objects.get(&hash).is_some_and(|o| o.sequence == sequence) {
            state.fifo.retain(|(key, _)| *key != hash);
            state.objects.remove(&hash)
        } else {
            None
        }
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
            let path = format!("{}/{hash:016x}/{sequence:016x}", self.namespace);
            let mut bytes = b"FOYODL01".to_vec();
            let encoded = piece
                .key()
                .encode(&mut bytes)
                .and_then(|_| piece.value().encode(&mut bytes));
            if let Err(e) = encoded {
                self.fail(e);
            } else if bytes.len() <= self.capacity {
                let size = bytes.len();
                match tokio::time::timeout(self.timeout, self.op.write(&path, bytes)).await {
                    Ok(Ok(_)) => {
                        let removed = {
                            let mut state = self.state.lock();
                            let mut removed = Vec::new();
                            if state.pending.get(&hash).is_some_and(|p| p.sequence == sequence) {
                                if let Some(old) = state.objects.insert(
                                    hash,
                                    Object {
                                        key: piece.key().clone(),
                                        path: path.clone(),
                                        size,
                                        sequence,
                                    },
                                ) {
                                    removed.push(old.path);
                                }
                                state.fifo.push_back((hash, sequence));
                                while state.objects.values().map(|o| o.size).sum::<usize>() > self.capacity {
                                    let Some((victim, version)) = state.fifo.pop_front() else {
                                        break;
                                    };
                                    if state.objects.get(&victim).is_some_and(|o| o.sequence == version) {
                                        removed.push(state.objects.remove(&victim).unwrap().path);
                                    }
                                }
                            } else {
                                removed.push(path.clone());
                            }
                            removed
                        };
                        for path in removed {
                            self.delete_object(&path).await;
                        }
                    }
                    Ok(Err(e)) => self.fail(e),
                    Err(e) => self.fail(e),
                }
            }
        }
        {
            let mut state = self.state.lock();
            if state.pending.get(&hash).is_some_and(|p| p.sequence == sequence) {
                state.pending.remove(&hash);
            }
        }
        self.queued_bytes.fetch_sub(reserved, Ordering::SeqCst);
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
    tx: mpsc::UnboundedSender<Command>,
    device: Arc<dyn Device>,
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
}

impl OpenDalEngineConfig {
    /// Create a cache configuration using a caller-supplied OpenDAL operator.
    ///
    /// `capacity` limits indexed encoded bytes; `queue_limit` limits admitted
    /// payload bytes until their commands finish. Both must be nonzero. These
    /// limits do not include serialization buffers, metadata, or orphan objects.
    /// The namespace must be a nonempty relative path without `.` or `..` segments.
    ///
    /// The operator must support reading, writing and deleting objects. Each IO
    /// has a two-second timeout. Only [`RecoverMode::None`] is accepted.
    pub fn new(op: Operator, namespace: String, capacity: usize, queue_limit: usize) -> Self {
        Self {
            op,
            namespace,
            capacity,
            queue_limit,
        }
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
            if self
                .namespace
                .split('/')
                .any(|segment| segment.is_empty() || segment == "." || segment == "..")
            {
                return Err(Error::new(
                    ErrorKind::Config,
                    "cache namespace must be a nonempty relative path",
                ));
            }
            let shared = Arc::new(Shared {
                op: self.op,
                namespace: self.namespace,
                capacity: self.capacity,
                queue_limit: self.queue_limit,
                timeout: Duration::from_secs(2),
                state: Mutex::new(State::default()),
                queued_bytes: AtomicUsize::new(0),
            });
            let (tx, mut receiver) = mpsc::unbounded_channel();
            let engine = Arc::new(OpenDalEngine {
                shared: shared.clone(),
                tx,
                device: NoopDeviceBuilder::new(self.capacity).build()?,
            });
            ctx.spawner.spawn(async move {
                while let Some(command) = receiver.recv().await {
                    match command {
                        Command::Put {
                            hash,
                            sequence,
                            reserved,
                        } => {
                            shared.put(hash, sequence, reserved).await;
                        }
                        Command::Delete(paths) => {
                            for path in paths {
                                shared.delete_object(&path).await;
                            }
                        }
                        Command::Barrier(done) => {
                            let _ = done.send(());
                        }
                        Command::Stop(done) => {
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
    fn device(&self) -> &Arc<dyn Device> {
        &self.device
    }

    fn filter(&self, _: u64, estimated_size: usize) -> StorageFilterResult {
        if estimated_size.saturating_add(8) > self.shared.capacity || self.shared.state.lock().closed {
            StorageFilterResult::Reject
        } else {
            StorageFilterResult::Admit
        }
    }

    fn enqueue(&self, piece: Piece, estimated_size: usize) {
        let hash = piece.hash();
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
        let reserved = estimated_size.max(piece.key().estimated_size() + piece.value().estimated_size() + 8);
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
        state.next += 1;
        let sequence = state.next;
        state.fifo.retain(|(key, _)| *key != hash);
        if let Some(old) = state.objects.remove(&hash) {
            let _ = self.tx.send(Command::Delete(vec![old.path]));
        }
        state.pending.insert(hash, Pending { piece, sequence });
        if self
            .tx
            .send(Command::Put {
                hash,
                sequence,
                reserved,
            })
            .is_err()
        {
            state.pending.remove(&hash);
            self.shared.queued_bytes.fetch_sub(reserved, Ordering::SeqCst);
        }
    }

    fn load(&self, hash: u64) -> BoxFuture<'static, Result<Load<String, Vec<u8>, HybridCacheProperties>>> {
        let shared = self.shared.clone();
        let tx = self.tx.clone();
        Box::pin(async move {
            let Some(object) = shared.state.lock().objects.get(&hash).cloned() else {
                return Ok(Load::Miss);
            };
            let bytes = match tokio::time::timeout(shared.timeout, shared.op.read(&object.path)).await {
                Ok(Ok(bytes)) => bytes.to_vec(),
                Ok(Err(e)) if e.kind() == opendal_core::ErrorKind::NotFound => {
                    shared.forget_object(hash, object.sequence);
                    return Ok(Load::Miss);
                }
                Ok(Err(e)) => return Err(external(e.to_string())),
                Err(e) => return Err(external(e.to_string())),
            };
            let decoded = (|| -> Result<_> {
                if bytes.len() < 8 || &bytes[..8] != b"FOYODL01" {
                    return Err(Error::new(ErrorKind::Parse, "invalid cache object magic"));
                }
                let mut cursor = std::io::Cursor::new(&bytes[8..]);
                let key = String::decode(&mut cursor)?;
                let value = Vec::<u8>::decode(&mut cursor)?;
                if cursor.position() as usize != bytes.len() - 8 || key != object.key {
                    return Err(Error::new(ErrorKind::Parse, "invalid cache object identity or framing"));
                }
                Ok((key, value))
            })();
            let (key, value) = match decoded {
                Ok(entry) => entry,
                Err(error) => {
                    if let Some(object) = shared.forget_object(hash, object.sequence) {
                        let _ = tx.send(Command::Delete(vec![object.path]));
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
                key,
                value,
                populated: Populated { age: Age::Young },
            })
        })
    }

    fn delete(&self, hash: u64) {
        let mut state = self.shared.state.lock();
        state.pending.remove(&hash);
        state.fifo.retain(|(key, _)| *key != hash);
        if let Some(old) = state.objects.remove(&hash) {
            let _ = self.tx.send(Command::Delete(vec![old.path]));
        }
    }

    fn may_contains(&self, hash: u64) -> bool {
        self.shared.state.lock().objects.contains_key(&hash)
    }

    fn destroy(&self) -> BoxFuture<'static, Result<()>> {
        let rx = {
            let mut state = self.shared.state.lock();
            state.pending.clear();
            state.fifo.clear();
            let paths = state.objects.drain().map(|(_, o)| o.path).collect();
            let (tx, rx) = oneshot::channel();
            let _ = self.tx.send(Command::Delete(paths));
            let _ = self.tx.send(Command::Barrier(tx));
            rx
        };
        Box::pin(async move { rx.await.map_err(|e| external(e.to_string())) })
    }

    fn wait(&self) -> BoxFuture<'static, ()> {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(Command::Barrier(tx));
        Box::pin(async move {
            let _ = rx.await;
        })
    }

    fn close(&self) -> BoxFuture<'static, Result<()>> {
        let shared = self.shared.clone();
        let mut state = shared.state.lock();
        if state.closed {
            return Box::pin(async { Ok(()) });
        }
        state.closed = true;
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(Command::Stop(tx));
        drop(state);
        Box::pin(async move {
            rx.await.map_err(|e| external(e.to_string()))?;
            let failures = shared.state.lock().failures.clone();
            if failures.is_empty() {
                Ok(())
            } else {
                Err(external(failures.join("; ")))
            }
        })
    }
}
