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

use std::{sync::Arc, time::Duration};

use tokio::sync::{mpsc, oneshot, Semaphore, SemaphorePermit};

use super::{
    device::{Device, DeviceExt, IO_BUFFER_ALLOCATOR},
    region::RegionManager,
};

#[derive(Debug)]
pub struct Reclaimer {
    wait_tx: mpsc::UnboundedSender<oneshot::Sender<()>>,
}

impl Reclaimer {
    pub async fn open<D>(region_manager: RegionManager<D>, reclaim_semaphore: Arc<Semaphore>) -> Self
    where
        D: Device,
    {
        let (wait_tx, wait_rx) = mpsc::unbounded_channel();

        let runner = ReclaimRunner {
            region_manager,
            reclaim_semaphore,
            wait_rx,
        };

        let _handle = tokio::spawn(async move { runner.run().await });

        Self { wait_tx }
    }

    // Wait for the current reclaim job to finish.
    pub async fn wait(&self) {
        let (tx, rx) = oneshot::channel();
        let _ = self.wait_tx.send(tx);
        let _ = rx.await;
    }
}

struct ReclaimRunner<D>
where
    D: Device,
{
    region_manager: RegionManager<D>,
    reclaim_semaphore: Arc<Semaphore>,

    wait_rx: mpsc::UnboundedReceiver<oneshot::Sender<()>>,
}

impl<D> ReclaimRunner<D>
where
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
            tokio::time::sleep(Self::RETRY_INTERVAL).await;
            return;
        };

        let align = region.device().align();
        let id = region.id();

        tracing::debug!("[reclaimer]: Start reclaiming region {id}.");

        // TODO(MrCroxx): reclaim entries

        // mark region clean
        let buf = allocator_api2::vec![in &IO_BUFFER_ALLOCATOR;0;align];
        if let Err(e) = region.device().write(buf, id, 0).await {
            tracing::warn!("mark region {id} clean error: {e}");
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
