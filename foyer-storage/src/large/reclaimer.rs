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

use std::{fmt::Debug, future::Future, sync::Arc, time::Duration};

use foyer_common::{
    bits,
    code::{StorageKey, StorageValue},
    metrics::Metrics,
    properties::Properties,
};
use futures_util::future::join_all;
use itertools::Itertools;
use tokio::sync::{mpsc, oneshot, Semaphore, SemaphorePermit};

use crate::{
    device::MonitoredDevice,
    error::Result,
    io::{
        buffer::{IoBuffer, OwnedSlice},
        PAGE,
    },
    large::{
        flusher::{Flusher, Submission},
        generic::GenericLargeStorageConfig,
        indexer::Indexer,
        scanner::RegionScanner,
        serde::Sequence,
    },
    picker::ReinsertionPicker,
    region::{Region, RegionManager},
    runtime::Runtime,
};

#[derive(Debug)]
pub struct Reclaimer {
    wait_tx: mpsc::UnboundedSender<oneshot::Sender<()>>,
}

impl Reclaimer {
    pub fn open<K, V, P>(
        config: &GenericLargeStorageConfig<K, V>,
        region_manager: RegionManager,
        reclaim_semaphore: Arc<Semaphore>,
        indexer: Indexer,
        flushers: Vec<Flusher<K, V, P>>,
        metrics: Arc<Metrics>,
    ) -> Self
    where
        K: StorageKey,
        V: StorageValue,
        P: Properties,
    {
        let (wait_tx, wait_rx) = mpsc::unbounded_channel();

        let runner = ReclaimRunner {
            device: config.device.clone(),
            region_manager,
            reclaim_semaphore,
            indexer,
            flushers,
            reinsertion_picker: config.reinsertion_picker.clone(),
            blob_index_size: config.blob_index_size,
            _metrics: metrics,
            wait_rx,
            runtime: config.runtime.clone(),
        };

        let _handle = config.runtime.write().spawn(async move { runner.run().await });

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

struct ReclaimRunner<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    device: MonitoredDevice,

    reinsertion_picker: Arc<dyn ReinsertionPicker>,

    region_manager: RegionManager,
    reclaim_semaphore: Arc<Semaphore>,

    indexer: Indexer,

    flushers: Vec<Flusher<K, V, P>>,

    blob_index_size: usize,

    _metrics: Arc<Metrics>,

    wait_rx: mpsc::UnboundedReceiver<oneshot::Sender<()>>,

    runtime: Runtime,
}

impl<K, V, P> ReclaimRunner<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
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

    async fn handle(&self, permit: SemaphorePermit<'_>) {
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

        let mut scanner = RegionScanner::new(region.clone(), self.blob_index_size);
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
            let infos = match scanner.next().await {
                Ok(None) => break 'reinsert,
                Err(e) => {
                    tracing::warn!(
                        "[reclaimer]: Error raised when reclaiming region {id}, skip the subsequent entries, err: {e}",
                        id = region.id()
                    );
                    break 'reinsert;
                }
                Ok(Some(infos)) => infos,
            };
            for info in infos {
                if self
                    .reinsertion_picker
                    .pick(self.device.statistics(), info.hash)
                    .admitted()
                {
                    let buf = IoBuffer::new(bits::align_up(PAGE, info.addr.len as _));
                    let (buf, res) = region.read(buf, info.addr.offset as _).await;
                    if let Err(e) = res {
                        tracing::warn!(
                        "[reclaimer]: error raised when reclaiming region {id}, skip the subsequent entries, err: {e}",
                        id = region.id()
                    );
                        break 'reinsert;
                    }

                    let slice = buf.into_owned_slice().slice(..info.addr.len as usize);
                    let flusher = self.flushers[picked_count % self.flushers.len()].clone();
                    flusher.submit(Submission::Reinsertion {
                        reinsertion: Reinsertion {
                            hash: info.hash,
                            sequence: info.addr.sequence,
                            slice,
                        },
                    });
                    picked_count += 1;
                } else {
                    unpicked.push(info.hash);
                }
            }
        }

        let unpicked_count = unpicked.len();

        let waits = self.flushers.iter().map(|flusher| flusher.wait()).collect_vec();
        self.runtime.write().spawn(async move {
            join_all(waits).await;
        });
        self.indexer.remove_batch(&unpicked);

        if let Err(e) = RegionCleaner::clean(&region).await {
            tracing::warn!("reclaimer]: mark region {id} clean error: {e}", id = region.id());
        }

        tracing::debug!(
            "[reclaimer]: Finish reclaiming region {id}, picked: {picked_count}, unpicked: {unpicked_count}."
        );

        region.statistics().reset();

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
    pub async fn clean(region: &Region) -> Result<()> {
        let mut page = IoBuffer::new(PAGE);
        page.fill(0);
        let (_, res) = region.write(page, 0).await;
        res?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct Reinsertion {
    pub hash: u64,
    pub sequence: Sequence,
    pub slice: OwnedSlice,
}
