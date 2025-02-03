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
    collections::HashMap,
    fmt::Debug,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
};

use async_channel::{Receiver, Sender};
use foyer_common::{countdown::Countdown, metrics::Metrics};
use futures_core::future::BoxFuture;
use futures_util::{future::Shared, FutureExt};
use itertools::Itertools;
use parking_lot::Mutex;
use pin_project::pin_project;
use rand::seq::IteratorRandom;
use tokio::sync::Semaphore;

use crate::{
    device::{Dev, DevExt, MonitoredDevice, RegionId},
    error::Result,
    picker::EvictionPicker,
    IoBytes, IoBytesMut,
};

#[derive(Debug, Default)]
pub struct RegionStats {
    pub invalid: AtomicUsize,
    pub access: AtomicUsize,
}

impl RegionStats {
    pub fn reset(&self) {
        self.invalid.store(0, Ordering::Relaxed);
        self.access.store(0, Ordering::Relaxed);
    }
}

/// # Region format:
///
/// ```plain
/// [ Header | Tombstone Pool | Entries ]
/// ```
///
/// ## Header
///
/// ```plain
/// [ MAGIC | Tombstone Pool Size ]
/// ```
#[derive(Debug, Clone)]
pub struct Region {
    id: RegionId,
    device: MonitoredDevice,
    stats: Arc<RegionStats>,
}

impl Region {
    pub fn id(&self) -> RegionId {
        self.id
    }

    pub fn stats(&self) -> &Arc<RegionStats> {
        &self.stats
    }

    pub async fn write(&self, buf: IoBytes, offset: u64) -> Result<()> {
        self.device.write(buf, self.id, offset).await
    }

    pub async fn read(&self, offset: u64, len: usize) -> Result<IoBytesMut> {
        self.stats.access.fetch_add(1, Ordering::Relaxed);
        self.device.read(self.id, offset, len).await
    }

    pub async fn flush(&self) -> Result<()> {
        self.device.flush(Some(self.id)).await
    }

    pub fn size(&self) -> usize {
        self.device.region_size()
    }

    pub fn align(&self) -> usize {
        self.device.align()
    }
}

#[cfg(test)]
impl Region {
    pub fn new_for_test(id: RegionId, device: MonitoredDevice, stats: Arc<RegionStats>) -> Self {
        Self { id, device, stats }
    }
}

#[derive(Clone)]
pub struct RegionManager {
    inner: Arc<RegionManagerInner>,
}

struct Eviction {
    evictable: HashMap<RegionId, Arc<RegionStats>>,
    eviction_pickers: Vec<Box<dyn EvictionPicker>>,
}

struct RegionManagerInner {
    regions: Vec<Region>,

    eviction: Mutex<Eviction>,

    clean_region_tx: Sender<Region>,
    clean_region_rx: Receiver<Region>,

    reclaim_semaphore: Arc<Semaphore>,
    reclaim_semaphore_countdown: Arc<Countdown>,

    metrics: Arc<Metrics>,
}

impl Debug for RegionManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegionManager").finish()
    }
}

impl RegionManager {
    pub fn new(
        device: MonitoredDevice,
        eviction_pickers: Vec<Box<dyn EvictionPicker>>,
        reclaim_semaphore: Arc<Semaphore>,
        metrics: Arc<Metrics>,
    ) -> Self {
        let regions = (0..device.regions() as RegionId)
            .map(|id| Region {
                id,
                device: device.clone(),
                stats: Arc::new(RegionStats::default()),
            })
            .collect_vec();
        let (clean_region_tx, clean_region_rx) = async_channel::unbounded();

        metrics.storage_region_total.absolute(device.regions() as _);
        metrics.storage_region_size_bytes.absolute(device.region_size() as _);

        Self {
            inner: Arc::new(RegionManagerInner {
                regions,
                eviction: Mutex::new(Eviction {
                    evictable: HashMap::new(),
                    eviction_pickers,
                }),
                clean_region_tx,
                clean_region_rx,
                reclaim_semaphore,
                reclaim_semaphore_countdown: Arc::new(Countdown::new(0)),
                metrics,
            }),
        }
    }

    pub fn mark_evictable(&self, region: RegionId) {
        let mut eviction = self.inner.eviction.lock();

        // Update evictable map.
        let res = eviction
            .evictable
            .insert(region, self.inner.regions[region as usize].stats.clone());
        assert!(res.is_none());

        // Temporarily take pickers to make borrow checker happy.
        let mut pickers = std::mem::take(&mut eviction.eviction_pickers);

        // Notify pickers.
        for picker in pickers.iter_mut() {
            picker.on_region_evictable(&eviction.evictable, region);
        }

        // Restore taken pickers after operations.

        std::mem::swap(&mut eviction.eviction_pickers, &mut pickers);
        assert!(pickers.is_empty());

        self.inner.metrics.storage_region_evictable.increase(1);

        tracing::debug!("[region manager]: Region {region} is marked evictable.");
    }

    pub fn evict(&self) -> Option<Region> {
        let mut picked = None;

        let mut eviction = self.inner.eviction.lock();

        if eviction.evictable.is_empty() {
            // No evictable region now.
            return None;
        }

        // Temporarily take pickers to make borrow checker happy.
        let mut pickers = std::mem::take(&mut eviction.eviction_pickers);

        // Pick a region to evict with pickers.
        for picker in pickers.iter_mut() {
            if let Some(region) = picker.pick(&eviction.evictable) {
                picked = Some(region);
                break;
            }
        }

        // If no region is selected, just randomly pick one.
        let picked = picked.unwrap_or_else(|| {
            eviction
                .evictable
                .keys()
                .choose(&mut rand::rng())
                .copied()
                .unwrap()
        });

        // Update evictable map.
        eviction.evictable.remove(&picked).unwrap();
        self.inner.metrics.storage_region_evictable.decrease(1);

        // Notify pickers.
        for picker in pickers.iter_mut() {
            picker.on_region_evict(&eviction.evictable, picked);
        }

        // Restore taken pickers after operations.
        std::mem::swap(&mut eviction.eviction_pickers, &mut pickers);
        assert!(pickers.is_empty());

        let region = self.region(picked).clone();
        tracing::debug!("[region manager]: Region {picked} is evicted.");

        Some(region)
    }

    pub async fn mark_clean(&self, region: RegionId) {
        self.inner
            .clean_region_tx
            .send(self.region(region).clone())
            .await
            .unwrap();
        self.inner.metrics.storage_region_clean.increase(1);
    }

    pub fn get_clean_region(&self) -> GetCleanRegionHandle {
        let clean_region_rx = self.inner.clean_region_rx.clone();
        let reclaim_semaphore = self.inner.reclaim_semaphore.clone();
        let reclaim_semaphore_countdown = self.inner.reclaim_semaphore_countdown.clone();
        let metrics = self.inner.metrics.clone();
        GetCleanRegionHandle::new(
            async move {
                let region = clean_region_rx.recv().await.unwrap();
                // The only place to increase the permit.
                //
                // See comments in `ReclaimRunner::handle()` and `RecoverRunner::run()`.
                if reclaim_semaphore_countdown.countdown() {
                    reclaim_semaphore.add_permits(1);
                }
                metrics.storage_region_clean.decrease(1);
                region
            }
            .boxed(),
        )
    }

    pub fn regions(&self) -> usize {
        self.inner.regions.len()
    }

    #[expect(dead_code)]
    pub fn evictable_regions(&self) -> usize {
        self.inner.eviction.lock().evictable.len()
    }

    #[expect(dead_code)]
    pub fn clean_regions(&self) -> usize {
        self.inner.clean_region_rx.len()
    }

    pub fn region(&self, id: RegionId) -> &Region {
        &self.inner.regions[id as usize]
    }

    pub fn reclaim_semaphore(&self) -> &Arc<Semaphore> {
        &self.inner.reclaim_semaphore
    }

    pub fn reclaim_semaphore_countdown(&self) -> &Countdown {
        &self.inner.reclaim_semaphore_countdown
    }
}

#[derive(Debug)]
#[pin_project]
pub struct GetCleanRegionHandle {
    #[pin]
    future: Shared<BoxFuture<'static, Region>>,
}

impl Clone for GetCleanRegionHandle {
    fn clone(&self) -> Self {
        Self {
            future: self.future.clone(),
        }
    }
}

impl GetCleanRegionHandle {
    pub fn new(future: BoxFuture<'static, Region>) -> Self {
        Self {
            future: future.shared(),
        }
    }
}

impl Future for GetCleanRegionHandle {
    type Output = Region;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        this.future.poll(cx)
    }
}
