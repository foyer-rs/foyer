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
    sync::{atomic::Ordering, Arc},
};

use foyer_common::{
    code::{StorageKey, StorageValue},
    metrics::Metrics,
};
use foyer_memory::Piece;
use futures_util::future::try_join_all;
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};

use super::{
    batch::{Batch, BatchMut, SetBatch},
    generic::GenericSmallStorageConfig,
    set_manager::SetManager,
};
use crate::{
    error::{Error, Result},
    Statistics,
};

pub enum Submission<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    Insertion { piece: Piece<K, V>, estimated_size: usize },
    Deletion { hash: u64 },
    Wait { tx: oneshot::Sender<()> },
}

impl<K, V> Debug for Submission<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Insertion {
                piece: _,
                estimated_size,
            } => f
                .debug_struct("Insertion")
                .field("estimated_size", estimated_size)
                .finish(),
            Self::Deletion { hash } => f.debug_struct("Deletion").field("hash", hash).finish(),
            Self::Wait { .. } => f.debug_struct("Wait").finish(),
        }
    }
}

pub struct Flusher<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    tx: flume::Sender<Submission<K, V>>,
}

impl<K, V> Flusher<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    pub fn open(
        config: &GenericSmallStorageConfig<K, V>,
        set_manager: SetManager,
        stats: Arc<Statistics>,
        metrics: Arc<Metrics>,
    ) -> Self {
        let (tx, rx) = flume::unbounded();

        let buffer_size = config.buffer_pool_size / config.flushers;

        let batch = BatchMut::new(set_manager.sets() as _, buffer_size, metrics.clone());

        let runner = Runner {
            rx,
            batch,
            flight: Arc::new(Semaphore::new(1)),
            set_manager,
            stats,
            metrics,
        };

        config.runtime.write().spawn(async move {
            if let Err(e) = runner.run().await {
                tracing::error!("[sodc flusher]: flusher exit with error: {e}");
            }
        });

        Self { tx }
    }

    pub fn submit(&self, submission: Submission<K, V>) {
        tracing::trace!("[sodc flusher]: submit task: {submission:?}");
        if let Err(e) = self.tx.send(submission) {
            tracing::error!("[sodc flusher]: error raised when submitting task, error: {e}");
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

struct Runner<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    rx: flume::Receiver<Submission<K, V>>,
    batch: BatchMut<K, V>,
    flight: Arc<Semaphore>,

    set_manager: SetManager,

    stats: Arc<Statistics>,
    metrics: Arc<Metrics>,
}

impl<K, V> Runner<K, V>
where
    K: StorageKey,
    V: StorageValue,
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

    fn submit(&mut self, submission: Submission<K, V>) {
        let report = |enqueued: bool| {
            if !enqueued {
                self.metrics.storage_queue_drop.increase(1);
            }
        };

        match submission {
            Submission::Insertion {
                piece: entry,
                estimated_size,
            } => report(self.batch.insert(entry, estimated_size)),
            Submission::Deletion { hash } => self.batch.delete(hash),
            Submission::Wait { tx } => self.batch.wait(tx),
        }
    }

    pub async fn commit(&self, batch: Batch<K, V>, permit: OwnedSemaphorePermit) {
        tracing::trace!("[sodc flusher] commit batch: {batch:?}");

        let futures = batch.sets.into_iter().map(|(sid, SetBatch { deletions, items })| {
            let set_manager = self.set_manager.clone();
            let stats = self.stats.clone();
            async move {
                set_manager.update(sid, &deletions, items).await?;

                stats
                    .cache_write_bytes
                    .fetch_add(set_manager.set_size(), Ordering::Relaxed);

                Ok::<_, Error>(())
            }
        });

        if let Err(e) = try_join_all(futures).await {
            tracing::error!("[sodc flusher]: error raised when committing batch, error: {e}");
        }

        for waiter in batch.waiters {
            let _ = waiter.send(());
        }

        drop(permit);
    }
}
