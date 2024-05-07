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
    ops::Deref,
    pin::Pin,
    sync::{atomic::AtomicUsize, Arc},
    task::{Context, Poll},
};

use async_channel::{Receiver, Sender};
use futures::{
    future::{BoxFuture, Shared},
    FutureExt,
};
use itertools::Itertools;
use parking_lot::Mutex;
use pin_project::pin_project;
use rand::seq::IteratorRandom;
use tokio::sync::Semaphore;

use super::device::{Device, DeviceExt};
use crate::large::device::RegionId;

pub trait EvictionPicker: Send + Sync + 'static + Debug {
    fn pick(&self, evictable: &HashMap<RegionId, Arc<RegionStats>>) -> Option<RegionId>;
}

#[derive(Debug, Default)]
pub struct RegionStats {
    pub invalid: AtomicUsize,
    pub access: AtomicUsize,
}

#[derive(Debug, Clone)]
pub struct Region<D>
where
    D: Device,
{
    id: RegionId,
    device: D,
    stats: Arc<RegionStats>,
}

impl<D> Region<D>
where
    D: Device,
{
    pub fn id(&self) -> RegionId {
        self.id
    }

    pub fn device(&self) -> &D {
        &self.device
    }

    pub fn stats(&self) -> &Arc<RegionStats> {
        &self.stats
    }
}

#[derive(Clone)]
pub struct RegionManager<D>
where
    D: Device,
{
    inner: Arc<RegionManagerInner<D>>,
}

struct RegionManagerInner<D>
where
    D: Device,
{
    regions: Vec<Region<D>>,

    evictable: Mutex<HashMap<RegionId, Arc<RegionStats>>>,

    pickers: Vec<Box<dyn EvictionPicker>>,

    clean_region_tx: Sender<Region<D>>,
    clean_region_rx: Receiver<Region<D>>,

    reclaim_semaphore: Arc<Semaphore>,
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
    pub fn new(device: D, pickers: Vec<Box<dyn EvictionPicker>>, reclaim_semaphore: Arc<Semaphore>) -> Self {
        let regions = (0..device.regions() as RegionId)
            .map(|id| Region {
                id,
                device: device.clone(),
                stats: Arc::new(RegionStats::default()),
            })
            .collect_vec();
        let (clean_region_tx, clean_region_rx) = async_channel::unbounded();
        let evictable = Mutex::new(HashMap::new());

        Self {
            inner: Arc::new(RegionManagerInner {
                regions,
                evictable,
                pickers,
                clean_region_tx,
                clean_region_rx,
                reclaim_semaphore,
            }),
        }
    }

    pub fn mark_evictable(&self, region: RegionId) {
        let mut evictable = self.inner.evictable.lock();
        let res = evictable.insert(region, self.inner.regions[region as usize].stats.clone());
        assert!(res.is_none());
        tracing::debug!("[region manager]: Region {region} is marked evictable.");
    }

    pub fn evict(&self) -> Option<Region<D>> {
        let mut picked = None;

        let mut evictable = self.inner.evictable.lock();
        if evictable.is_empty() {
            return None;
        }

        for picker in self.inner.pickers.iter() {
            if let Some(region) = picker.pick(evictable.deref()) {
                picked = Some(region);
                break;
            }
        }

        let picked = picked.unwrap_or_else(|| evictable.keys().choose(&mut rand::thread_rng()).copied().unwrap());

        evictable.remove(&picked).unwrap();

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
        GetCleanRegionHandle::new(
            async move {
                let region = clean_region_rx.recv().await.unwrap();
                // The only place to increase the permit.
                //
                // See comments in `ReclaimRunner::handle`.
                reclaim_semaphore.add_permits(1);
                region
            }
            .boxed(),
        )
    }

    pub fn regions(&self) -> usize {
        self.inner.regions.len()
    }

    pub fn evictable_regions(&self) -> usize {
        self.inner.evictable.lock().len()
    }

    pub fn clean_regions(&self) -> usize {
        self.inner.clean_region_rx.len()
    }

    pub fn region(&self, id: RegionId) -> &Region<D> {
        &self.inner.regions[id as usize]
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
