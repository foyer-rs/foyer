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

use std::{fmt::Debug, future::Future, sync::Arc, time::Duration};

use foyer_common::{
    code::{HashBuilder, StorageKey, StorageValue},
    metrics::Metrics,
};
use futures::future::join_all;
use itertools::Itertools;
use tokio::sync::{mpsc, oneshot, Semaphore, SemaphorePermit};

use crate::{
    device::IO_BUFFER_ALLOCATOR,
    error::Result,
    large::{
        flusher::{Flusher, Submission},
        indexer::Indexer,
        scanner::RegionScanner,
        serde::Sequence,
    },
    picker::ReinsertionPicker,
    region::{Region, RegionManager},
    runtime::Runtime,
    statistics::Statistics,
    IoBytes,
};

#[derive(Debug)]
pub struct Reclaimer {
    wait_tx: mpsc::UnboundedSender<oneshot::Sender<()>>,
}

impl Reclaimer {
    #[expect(clippy::too_many_arguments)]
    pub async fn open<K, V, S>(
        region_manager: RegionManager,
        reclaim_semaphore: Arc<Semaphore>,
        reinsertion_picker: Arc<dyn ReinsertionPicker<Key = K>>,
        indexer: Indexer,
        flushers: Vec<Flusher<K, V, S>>,
        stats: Arc<Statistics>,
        flush: bool,
        metrics: Arc<Metrics>,
        runtime: &Runtime,
    ) -> Self
    where
        K: StorageKey,
        V: StorageValue,
        S: HashBuilder + Debug,
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
            metrics,
            wait_rx,
            runtime: runtime.clone(),
        };

        let _handle = runtime.write().spawn(async move { runner.run().await });

        Self { wait_tx }
    }

    // Wait for the current reclaim job to finish.
    pub fn wait(&self) -> impl Future<Output = ()> + Send + 'static {
        let (tx, rx) = oneshot::channel();
        let _ = self.wait_tx.send(tx);
        async move {
            let _ = rx.await;
        }
    }
}

struct ReclaimRunner<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    reinsertion_picker: Arc<dyn ReinsertionPicker<Key = K>>,

    region_manager: RegionManager,
    reclaim_semaphore: Arc<Semaphore>,

    indexer: Indexer,

    flushers: Vec<Flusher<K, V, S>>,

    stats: Arc<Statistics>,

    flush: bool,

    metrics: Arc<Metrics>,

    wait_rx: mpsc::UnboundedReceiver<oneshot::Sender<()>>,

    runtime: Runtime,
}

impl<K, V, S> ReclaimRunner<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
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

        let mut scanner = RegionScanner::new(region.clone(), self.metrics.clone());
        let mut picked_count = 0;
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
                let buffer = match region.read(info.addr.offset as _, info.addr.len as _).await {
                    Err(e) => {
                        tracing::warn!(
                            "[reclaimer]: error raised when reclaiming region {id}, skip the subsequent entries, err: {e}",
                            id = region.id()
                        );
                        break 'reinsert;
                    }
                    Ok(buf) => buf.freeze(),
                };
                let flusher = self.flushers[picked_count % self.flushers.len()].clone();
                flusher.submit(Submission::Reinsertion {
                    reinsertion: Reinsertion {
                        hash: info.hash,
                        sequence: info.sequence,
                        buffer,
                    },
                });
                picked_count += 1;
            } else {
                unpicked.push(info.hash);
            }
        }

        let unpicked_count = unpicked.len();

        let waits = self.flushers.iter().map(|flusher| flusher.wait()).collect_vec();
        self.runtime.write().spawn(async move {
            join_all(waits).await;
        });
        self.indexer.remove_batch(&unpicked);

        if let Err(e) = RegionCleaner::clean(&region, self.flush).await {
            tracing::warn!("reclaimer]: mark region {id} clean error: {e}", id = region.id());
        }

        tracing::debug!(
            "[reclaimer]: Finish reclaiming region {id}, picked: {picked_count}, unpicked: {unpicked_count}."
        );

        region.stats().reset();

        self.region_manager.mark_clean(id).await;
        // These operations should be atomic:
        //
        // 1. Reclaim runner releases 1 permit on finish. (+1)
        // 2. There is a new clean region. (-1)
        //
        // Because the total permits to modify is 0 and to avoid concurrent corner case, just forget the permit.
        //
        // The permit only increase when the a clean region is taken to write.
        permit.forget();
    }
}

#[derive(Debug)]
pub struct RegionCleaner;

impl RegionCleaner {
    pub async fn clean(region: &Region, flush: bool) -> Result<()> {
        let buf = allocator_api2::vec::from_elem_in(0, region.align(), &IO_BUFFER_ALLOCATOR).into();
        region.write(buf, 0).await?;
        if flush {
            region.flush().await?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct Reinsertion {
    pub hash: u64,
    pub sequence: Sequence,
    pub buffer: IoBytes,
}
