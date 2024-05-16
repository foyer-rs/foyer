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
use foyer_common::countdown::Countdown;
use futures::{
    future::{BoxFuture, Shared},
    FutureExt,
};
use itertools::Itertools;
use parking_lot::Mutex;
use pin_project::pin_project;
use rand::seq::IteratorRandom;
use tokio::sync::Semaphore;

use crate::{
    device::{monitor::Monitored, Device, DeviceExt, IoBuffer, RegionId},
    error::Result,
    picker::EvictionPicker,
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
pub struct Region<D>
where
    D: Device,
{
    id: RegionId,
    device: Monitored<D>,
    stats: Arc<RegionStats>,
}

impl<D> Region<D>
where
    D: Device,
{
    pub fn id(&self) -> RegionId {
        self.id
    }

    pub fn stats(&self) -> &Arc<RegionStats> {
        &self.stats
    }

    pub async fn write(&self, buf: IoBuffer, offset: u64) -> Result<()> {
        self.device.write(buf, self.id, offset).await
    }

    pub async fn read(&self, offset: u64, len: usize) -> Result<IoBuffer> {
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

#[derive(Clone)]
pub struct RegionManager<D>
where
    D: Device,
{
    inner: Arc<RegionManagerInner<D>>,
}

struct Eviction {
    evictable: HashMap<RegionId, Arc<RegionStats>>,
    eviction_pickers: Vec<Box<dyn EvictionPicker>>,
}

struct RegionManagerInner<D>
where
    D: Device,
{
    regions: Vec<Region<D>>,

    eviction: Mutex<Eviction>,

    clean_region_tx: Sender<Region<D>>,
    clean_region_rx: Receiver<Region<D>>,

    reclaim_semaphore: Arc<Semaphore>,
    reclaim_semaphore_countdown: Arc<Countdown>,
}

impl<D> Debug for RegionManager<D>
where
    D: Device,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegionManager").finish()
    }
}

impl<D> RegionManager<D>
where
    D: Device,
{
    pub fn new(
        device: Monitored<D>,
        eviction_pickers: Vec<Box<dyn EvictionPicker>>,
        reclaim_semaphore: Arc<Semaphore>,
    ) -> Self {
        let regions = (0..device.regions() as RegionId)
            .map(|id| Region {
                id,
                device: device.clone(),
                stats: Arc::new(RegionStats::default()),
            })
            .collect_vec();
        let (clean_region_tx, clean_region_rx) = async_channel::unbounded();

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

        // Noitfy pickers.
        for picker in pickers.iter_mut() {
            picker.on_region_evictable(&eviction.evictable, region);
        }

        // Restore taken pickers after operations.

        std::mem::swap(&mut eviction.eviction_pickers, &mut pickers);
        assert!(pickers.is_empty());

        tracing::debug!("[region manager]: Region {region} is marked evictable.");
    }

    pub fn evict(&self) -> Option<Region<D>> {
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

        // If no region is selected, just ramdomly pick one.
        let picked = picked.unwrap_or_else(|| {
            eviction
                .evictable
                .keys()
                .choose(&mut rand::thread_rng())
                .copied()
                .unwrap()
        });

        // Update evictable map.
        eviction.evictable.remove(&picked).unwrap();

        // Noitfy pickers.
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
    }

    pub fn get_clean_region(&self) -> GetCleanRegionHandle<D> {
        let clean_region_rx = self.inner.clean_region_rx.clone();
        let reclaim_semaphore = self.inner.reclaim_semaphore.clone();
        let reclaim_semaphore_countdown = self.inner.reclaim_semaphore_countdown.clone();
        GetCleanRegionHandle::new(
            async move {
                let region = clean_region_rx.recv().await.unwrap();
                // The only place to increase the permit.
                //
                // See comments in `ReclaimRunner::handle()` and `RecoverRunner::run()`.
                if reclaim_semaphore_countdown.countdown() {
                    reclaim_semaphore.add_permits(1);
                }
                region
            }
            .boxed(),
        )
    }

    pub fn regions(&self) -> usize {
        self.inner.regions.len()
    }

    // TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
    #[allow(dead_code)]
    pub fn evictable_regions(&self) -> usize {
        self.inner.eviction.lock().evictable.len()
    }

    // TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
    #[allow(dead_code)]
    pub fn clean_regions(&self) -> usize {
        self.inner.clean_region_rx.len()
    }

    pub fn region(&self, id: RegionId) -> &Region<D> {
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
pub struct GetCleanRegionHandle<D>
where
    D: Device,
{
    #[pin]
    future: Shared<BoxFuture<'static, Region<D>>>,
}

impl<D> Clone for GetCleanRegionHandle<D>
where
    D: Device,
{
    fn clone(&self) -> Self {
        Self {
            future: self.future.clone(),
        }
    }
}

impl<D> GetCleanRegionHandle<D>
where
    D: Device,
{
    pub fn new(future: BoxFuture<'static, Region<D>>) -> Self {
        Self {
            future: future.shared(),
        }
    }
}

impl<D> Future for GetCleanRegionHandle<D>
where
    D: Device,
{
    type Output = Region<D>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        this.future.poll(cx)
    }
}
