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

use std::{fmt::Debug, hash::BuildHasher, sync::Arc, time::Duration};

use foyer_common::code::{StorageKey, StorageValue};
use futures::future::try_join_all;
use tokio::{
    runtime::Handle,
    sync::{mpsc, oneshot, Semaphore, SemaphorePermit},
};

use super::{flusher::Flusher, indexer::Indexer};
use crate::{
    device::{Device, DeviceExt, IoBuffer, IO_BUFFER_ALLOCATOR},
    error::Result,
    large::{flusher::Submission, scanner::RegionScanner},
    picker::ReinsertionPicker,
    region::{Region, RegionManager},
    statistics::Statistics,
    EnqueueFuture, Sequence,
};

#[derive(Debug)]
pub struct Reclaimer {
    wait_tx: mpsc::UnboundedSender<oneshot::Sender<()>>,
}

impl Reclaimer {
    // TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
    #[allow(clippy::too_many_arguments)]
    pub async fn open<K, V, S, D>(
        region_manager: RegionManager<D>,
        reclaim_semaphore: Arc<Semaphore>,
        reinsertion_picker: Arc<dyn ReinsertionPicker<Key = K>>,
        indexer: Indexer,
        flushers: Vec<Flusher<K, V, S, D>>,
        stats: Arc<Statistics>,
        flush: bool,
        runtime: Handle,
    ) -> Self
    where
        K: StorageKey,
        V: StorageValue,
        S: BuildHasher + Send + Sync + 'static + Debug,
        D: Device,
    {
        let (wait_tx, wait_rx) = mpsc::unbounded_channel();

        let runner = ReclaimRunner {
            region_manager,
            reclaim_semaphore,
            indexer,
            flushers,
            reinsertion_picker,
            stats,
            flush,
            wait_rx,
            runtime: runtime.clone(),
        };

        let _handle = runtime.spawn(async move { runner.run().await });

        Self { wait_tx }
    }

    // Wait for the current reclaim job to finish.
    pub async fn wait(&self) {
        let (tx, rx) = oneshot::channel();
        let _ = self.wait_tx.send(tx);
        let _ = rx.await;
    }
}

struct ReclaimRunner<K, V, S, D>
where
    K: StorageKey,
    V: StorageValue,
    S: BuildHasher + Send + Sync + 'static + Debug,
    D: Device,
{
    reinsertion_picker: Arc<dyn ReinsertionPicker<Key = K>>,

    region_manager: RegionManager<D>,
    reclaim_semaphore: Arc<Semaphore>,

    indexer: Indexer,

    flushers: Vec<Flusher<K, V, S, D>>,

    stats: Arc<Statistics>,

    flush: bool,

    wait_rx: mpsc::UnboundedReceiver<oneshot::Sender<()>>,

    runtime: Handle,
}

impl<K, V, S, D> ReclaimRunner<K, V, S, D>
where
    K: StorageKey,
    V: StorageValue,
    S: BuildHasher + Send + Sync + 'static + Debug,
    D: Device,
{
    const RETRY_INTERVAL: Duration = Duration::from_millis(10);

    async fn run(mut self) {
        loop {
            tokio::select! {
                biased;
                tx = self.wait_rx.recv() => {
                    match tx {
                        None => {
                            tracing::info!("[reclaimer]: Reclaimer exits.");
                            return;
                        },
                        Some(tx) => {
                            let _ = tx.send(());
                        },
                    }

                }
                permit = self.reclaim_semaphore.acquire() => {
                    match permit {
                        Err(_) => {
                            tracing::info!("[reclaimer]: Reclaimer exits.");
                            return;
                        },
                        Ok(permit) => self.handle(permit).await,
                    }
                }
            }
        }
    }

    async fn handle<'s>(&self, permit: SemaphorePermit<'s>) {
        let Some(region) = self.region_manager.evict() else {
            // There is no evictable region, which means all regions are being written or being reclaiming.
            //
            // Wait for a while in this case.
            tracing::warn!(
                "[reclaimer]: No evictable region at the moment, sleep for {interval:?}.",
                interval = Self::RETRY_INTERVAL
            );
            tokio::time::sleep(Self::RETRY_INTERVAL).await;
            return;
        };

        let id = region.id();

        tracing::debug!("[reclaimer]: Start reclaiming region {id}.");

        // TODO(MrCroxx): Special impl for case : `if self.reinsertion_picker.type_id() != TypeId::of::<Arc<DenyAllPicker<K>>>()` ?
        let mut scanner = RegionScanner::new(region.id(), region.device().clone());
        let mut futures = vec![];
        let mut unpicked = vec![];
        // The loop will ends when:
        //
        // 1. no subsequent entries
        // 2. on error
        //
        // If the loop ends on error, the subsequent indices cannot be removed while reclaiming.
        // They will be removed when a query find a mismatch entry.
        'reinsert: loop {
            let (info, key) = match scanner.next_key().await {
                Ok(None) => break 'reinsert,
                Err(e) => {
                    tracing::warn!(
                        "[reclaimer]: Error raised when reclaiming region {id}, skip the subsequent entries, err: {e}",
                        id = region.id()
                    );
                    break 'reinsert;
                }
                Ok(Some((info, key))) => (info, key),
            };
            if self.reinsertion_picker.pick(&self.stats, &key) {
                let buffer = match region
                    .device()
                    .read(region.id(), info.addr.offset as _, info.addr.len as _)
                    .await
                {
                    Err(e) => {
                        tracing::warn!(
                            "[reclaimer]: error raised when reclaiming region {id}, skip the subsequent entries, err: {e}",
                            id = region.id()
                        );
                        break 'reinsert;
                    }
                    Ok(buf) => buf,
                };
                let flusher = self.flushers[futures.len() % self.flushers.len()].clone();
                let (tx, rx) = oneshot::channel();
                let future = EnqueueFuture::new(rx);
                flusher.submit(
                    Submission::Reinsertion {
                        reinsertion: Reinsertion {
                            hash: info.hash,
                            sequence: info.sequence,
                            buffer,
                        },
                        tx,
                    },
                    0,
                );
                futures.push(future);
            } else {
                unpicked.push(info.hash);
            }
        }

        self.runtime.spawn(async move {
            if let Err(e) = try_join_all(futures).await {
                tracing::warn!(
                    "[reclaimer]: error raised when reinserting entries, the entries may be dropped, err: {e}"
                );
            }
        });
        self.indexer.remove_batch(&unpicked);

        if let Err(e) = RegionCleaner::clean(&region, self.flush).await {
            tracing::warn!("reclaimer]: mark region {id} clean error: {e}", id = region.id());
        }

        tracing::debug!("[reclaimer]: Finish reclaiming region {id}.");

        self.region_manager.mark_clean(id).await;
        // These operations should be atomic:
        //
        // 1. Reclaim runner releases 1 permit on finish. (+1)
        // 2. There is a new clean region. (-1)
        //
        // Because the total permits to modify is 0 and to avlid concurrent corner case, just forget the permit.
        //
        // The permit only increase when the a clean region is taken to write.
        permit.forget();
    }
}

#[derive(Debug)]
pub struct RegionCleaner;

impl RegionCleaner {
    pub async fn clean<D>(region: &Region<D>, flush: bool) -> Result<()>
    where
        D: Device,
    {
        let buf = allocator_api2::vec![
            in &IO_BUFFER_ALLOCATOR;
            0;
            region.device().align()
        ];
        region.device().write(buf, region.id(), 0).await?;
        if flush {
            region.device().flush(Some(region.id())).await?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct Reinsertion {
    pub hash: u64,
    pub sequence: Sequence,
    pub buffer: IoBuffer,
}
