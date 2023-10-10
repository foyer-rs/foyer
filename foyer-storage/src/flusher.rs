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

use foyer_common::{rate::RateLimiter, runtime::tokio};
use foyer_intrusive::{core::adapter::Link, eviction::EvictionPolicy};

use tokio::sync::broadcast;

use crate::{
    device::Device,
    error::Result,
    metrics::Metrics,
    region::RegionId,
    region_manager::{RegionEpItemAdapter, RegionManager},
    slice::Slice,
};

#[derive(Debug)]
pub struct Flusher<D, EP, EL>
where
    D: Device,
    EP: EvictionPolicy<Adapter = RegionEpItemAdapter<EL>>,
    EL: Link,
{
    region_manager: Arc<RegionManager<D, EP, EL>>,

    rate_limiter: Option<Arc<RateLimiter>>,

    metrics: Arc<Metrics>,

    stop_rx: broadcast::Receiver<()>,
}

impl<D, EP, EL> Flusher<D, EP, EL>
where
    D: Device,
    EP: EvictionPolicy<Adapter = RegionEpItemAdapter<EL>>,
    EL: Link,
{
    pub fn new(
        region_manager: Arc<RegionManager<D, EP, EL>>,
        rate_limiter: Option<Arc<RateLimiter>>,
        metrics: Arc<Metrics>,
        stop_rx: broadcast::Receiver<()>,
    ) -> Self {
        Self {
            region_manager,
            rate_limiter,
            metrics,
            stop_rx,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        loop {
            tokio::select! {
                biased;
                region_id = self.region_manager.dirty_regions().acquire() => {
                    self.handle(region_id).await?;
                }
                _ = self.stop_rx.recv() => {
                    tracing::info!("[flusher] exit");
                    return Ok(())
                }
            }
        }
    }

    async fn handle(&self, region_id: RegionId) -> Result<()> {
        let _timer = self.metrics.slow_op_duration_flush.start_timer();

        tracing::info!("[flusher] receive flush task, region: {}", region_id);

        let region = self.region_manager.region(&region_id);

        tracing::trace!("[flusher] step 1");

        // step 1: write buffer back to device
        let slice = region.load(.., 0).await?.unwrap();

        {
            // wait all physical readers (from previous version) and writers done
            let _ = region.exclusive(false, true, false).await;
        }

        tracing::trace!("[flusher] write region {} back to device", region_id);

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
        drop(slice);

        tracing::trace!("[flusher] step 2");

        let buffer = {
            // step 2: detach buffer
            let mut guard = region.exclusive(false, false, true).await;
            guard.detach_buffer()
        };

        tracing::trace!("[flusher] step 3");

        // step 3: release buffer
        self.region_manager.buffers().release(buffer);
        self.region_manager.eviction_push(region.id());

        tracing::info!("[flusher] finish flush task, region: {}", region_id);

        self.metrics
            .op_bytes_flush
            .inc_by(region.device().region_size() as u64);
        self.metrics
            .total_bytes
            .add(region.device().region_size() as u64);

        Ok(())
    }
}
