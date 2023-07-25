//  Copyright 2023 MrCroxx
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

use std::sync::Arc;

use foyer_common::{queue::AsyncQueue, rate::RateLimiter};
use foyer_intrusive::{core::adapter::Link, eviction::EvictionPolicy};
use itertools::Itertools;
use tokio::{
    sync::{broadcast, mpsc, Mutex},
    task::JoinHandle,
};

use crate::{
    device::{BufferAllocator, Device},
    error::{Error, Result},
    metrics::Metrics,
    region::RegionId,
    region_manager::{RegionEpItemAdapter, RegionManager},
    slice::Slice,
};

#[derive(Debug)]
pub struct FlushTask {
    pub region_id: RegionId,
}

struct FlusherInner {
    sequence: usize,

    task_txs: Vec<mpsc::UnboundedSender<FlushTask>>,
}

pub struct Flusher {
    runners: usize,

    inner: Mutex<FlusherInner>,
}

impl Flusher {
    pub fn new(runners: usize) -> Self {
        let inner = FlusherInner {
            sequence: 0,
            task_txs: Vec::with_capacity(runners),
        };
        Self {
            runners,
            inner: Mutex::new(inner),
        }
    }

    pub async fn run<D, E, EL>(
        &self,
        buffers: Arc<AsyncQueue<Vec<u8, D::IoBufferAllocator>>>,
        region_manager: Arc<RegionManager<D, E, EL>>,
        rate_limiter: Option<Arc<RateLimiter>>,
        stop_rxs: Vec<broadcast::Receiver<()>>,
        metrics: Arc<Metrics>,
    ) -> Vec<JoinHandle<()>>
    where
        D: Device,
        E: EvictionPolicy<Adapter = RegionEpItemAdapter<EL>>,
        EL: Link,
    {
        let mut inner = self.inner.lock().await;

        #[allow(clippy::type_complexity)]
        let (mut txs, rxs): (
            Vec<mpsc::UnboundedSender<FlushTask>>,
            Vec<mpsc::UnboundedReceiver<FlushTask>>,
        ) = (0..self.runners).map(|_| mpsc::unbounded_channel()).unzip();
        inner.task_txs.append(&mut txs);

        let runners = rxs
            .into_iter()
            .zip_eq(stop_rxs.into_iter())
            .map(|(task_rx, stop_rx)| Runner {
                task_rx,
                buffers: buffers.clone(),
                region_manager: region_manager.clone(),
                rate_limiter: rate_limiter.clone(),
                stop_rx,
                metrics: metrics.clone(),
            })
            .collect_vec();

        let mut handles = vec![];
        for runner in runners {
            let handle = tokio::spawn(async move {
                runner.run().await.unwrap();
            });
            handles.push(handle);
        }
        handles
    }

    pub fn runners(&self) -> usize {
        self.runners
    }

    pub async fn submit(&self, task: FlushTask) -> Result<()> {
        let mut inner = self.inner.lock().await;
        let submittee = inner.sequence % inner.task_txs.len();
        inner.sequence += 1;
        inner.task_txs[submittee].send(task).map_err(Error::other)
    }
}

struct Runner<D, E, EL>
where
    D: Device,
    E: EvictionPolicy<Adapter = RegionEpItemAdapter<EL>>,
    EL: Link,
{
    task_rx: mpsc::UnboundedReceiver<FlushTask>,
    buffers: Arc<AsyncQueue<Vec<u8, D::IoBufferAllocator>>>,

    region_manager: Arc<RegionManager<D, E, EL>>,

    rate_limiter: Option<Arc<RateLimiter>>,

    stop_rx: broadcast::Receiver<()>,

    metrics: Arc<Metrics>,
}

impl<D, E, EL> Runner<D, E, EL>
where
    D: Device,
    E: EvictionPolicy<Adapter = RegionEpItemAdapter<EL>>,
    EL: Link,
{
    async fn run(mut self) -> Result<()> {
        loop {
            tokio::select! {
                biased;
                Some(task) = self.task_rx.recv() => {
                    self.handle(task).await?;
                }
                _ = self.stop_rx.recv() => {
                    tracing::info!("[flusher] exit");
                    return Ok(())
                }
            }
        }
    }

    async fn handle(&self, task: FlushTask) -> Result<()> {
        tracing::info!("[flusher] receive flush task, region: {}", task.region_id);

        let region = self.region_manager.region(&task.region_id);

        tracing::trace!("[flusher] step 1");

        // step 1: write buffer back to device
        let slice = region.load(.., 0).await?.unwrap();

        {
            // wait all physical readers (from previous version) and writers done
            let _ = region.exclusive(false, true, false).await;
        }

        tracing::trace!("[flusher] write region {} back to device", task.region_id);

        let mut offset = 0;
        let len = region.device().io_size();
        while offset < region.device().region_size() {
            let start = offset;
            let end = std::cmp::min(offset + len, region.device().region_size());

            let s = unsafe { Slice::new(&slice.as_ref()[start..end]) };
            if let Some(limiter) = &self.rate_limiter && let Some(duration) = limiter.consume(len as f64) {
                tokio::time::sleep(duration).await;
            }
            region
                .device()
                .write(s, region.id(), offset as u64, len)
                .await?;
            offset += len;
        }
        slice.destroy().await;

        tracing::trace!("[flusher] step 2");

        let buffer = {
            // step 2: detach buffer
            let mut guard = region.exclusive(false, false, true).await;
            guard.detach_buffer()
        };

        tracing::trace!("[flusher] step 3");

        // step 3: release buffer
        self.buffers.release(buffer);
        self.region_manager.set_region_evictable(&region.id()).await;

        tracing::info!("[flusher] finish flush task, region: {}", task.region_id);

        self.metrics
            .bytes_flush
            .inc_by(region.device().region_size() as u64);
        self.metrics.size.add(region.device().region_size() as i64);

        Ok(())
    }
}
