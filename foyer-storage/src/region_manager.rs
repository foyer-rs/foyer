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

use std::{marker::PhantomData, sync::Arc};

use foyer_common::queue::AsyncQueue;
use foyer_intrusive::{
    core::{adapter::Link, pointer::PointerOps},
    eviction::EvictionPolicy,
    intrusive_adapter, key_adapter, priority_adapter,
};
use parking_lot::RwLock;
use tokio::sync::RwLock as AsyncRwLock;

use crate::{
    device::{BufferAllocator, Device},
    flusher::{FlushTask, Flusher},
    reclaimer::{ReclaimTask, Reclaimer},
    region::{AllocateResult, Region, RegionId},
};

#[derive(Debug)]
pub struct RegionEpItem<L>
where
    L: Link,
{
    link: L,
    id: RegionId,
    priority: usize,
}

intrusive_adapter! { pub RegionEpItemAdapter<L> = Arc<RegionEpItem<L>>: RegionEpItem<L> { link: L } where L: Link }
key_adapter! { RegionEpItemAdapter<L> = RegionEpItem<L> { id: RegionId } where L: Link }
priority_adapter! { RegionEpItemAdapter<L> = RegionEpItem<L> { priority: usize } where L: Link }

struct RegionManagerInner {
    current: Option<RegionId>,
}

pub struct RegionManager<A, D, E, EL>
where
    A: BufferAllocator,
    D: Device<IoBufferAllocator = A>,
    E: EvictionPolicy<RegionEpItemAdapter<EL>, Link = EL>,
    EL: Link,
{
    inner: Arc<AsyncRwLock<RegionManagerInner>>,

    buffers: Arc<AsyncQueue<Vec<u8, A>>>,
    clean_regions: Arc<AsyncQueue<RegionId>>,

    regions: Vec<Region<A, D>>,
    items: Vec<Arc<RegionEpItem<EL>>>,

    flusher: Arc<Flusher>,
    reclaimer: Arc<Reclaimer>,

    eviction: RwLock<E>,
    _marker: PhantomData<EL>,
}

impl<A, D, E, EL> RegionManager<A, D, E, EL>
where
    A: BufferAllocator,
    D: Device<IoBufferAllocator = A>,
    E: EvictionPolicy<RegionEpItemAdapter<EL>, Link = EL>,
    EL: Link,
{
    pub fn new(
        region_nums: usize,
        eviction_config: E::Config,
        buffers: Arc<AsyncQueue<Vec<u8, A>>>,
        clean_regions: Arc<AsyncQueue<RegionId>>,
        device: D,
        flusher: Arc<Flusher>,
        reclaimer: Arc<Reclaimer>,
    ) -> Self {
        let eviction = E::new(eviction_config);

        let mut regions = Vec::with_capacity(region_nums);
        let mut items = Vec::with_capacity(region_nums);

        for id in 0..region_nums as RegionId {
            let region = Region::new(id, device.clone());
            let item = Arc::new(RegionEpItem {
                link: E::Link::default(),
                id,
                priority: 0,
            });

            regions.push(region);
            items.push(item);
        }

        let inner = RegionManagerInner { current: None };

        Self {
            inner: Arc::new(AsyncRwLock::new(inner)),
            buffers,
            clean_regions,
            regions,
            items,
            flusher,
            reclaimer,
            eviction: RwLock::new(eviction),
            _marker: PhantomData,
        }
    }

    /// Allocate a buffer slice with given size in an active region to write.
    #[tracing::instrument(skip(self))]
    pub async fn allocate(&self, size: usize) -> AllocateResult {
        let mut inner = self.inner.write().await;

        // try allocate from current region
        if let Some(region_id) = inner.current {
            let region = self.region(&region_id);
            match region.allocate(size).await {
                AllocateResult::Ok(slice) => {
                    return AllocateResult::Ok(slice);
                }
                AllocateResult::Full { slice, remain } => {
                    // current region is full, schedule flushing
                    self.submit_flush_task(FlushTask { region_id }).await;
                    inner.current = None;
                    return AllocateResult::Full { slice, remain };
                }
            }
        }

        assert!(inner.current.is_none());

        tracing::debug!("clean regions: {}", self.clean_regions.len());
        if self.clean_regions.len() < self.reclaimer.runners() {
            let item = self.eviction.write().pop();
            if let Some(item) = item {
                self.submit_reclaim_task(ReclaimTask { region_id: item.id })
                    .await;
            }
        }

        let region_id = self.clean_regions.acquire().await;
        tracing::info!("switch to clean region: {}", region_id);

        let region = self.region(&region_id);
        region.advance().await;

        let buffer = self.buffers.acquire().await;
        region.attach_buffer(buffer).await;

        let slice = region.allocate(size).await.unwrap();

        inner.current = Some(region_id);

        AllocateResult::Ok(slice)
    }

    #[tracing::instrument(skip(self))]
    pub fn region(&self, id: &RegionId) -> &Region<A, D> {
        &self.regions[*id as usize]
    }

    #[tracing::instrument(skip(self))]
    pub fn record_access(&self, id: &RegionId) {
        let mut eviction = self.eviction.write();
        let item = &self.items[*id as usize];
        if item.link.is_linked() {
            eviction.access(&self.items[*id as usize]);
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn set_region_evictable(&self, id: &RegionId) {
        let to_reclaim = {
            let mut eviction = self.eviction.write();
            let item = &self.items[*id as usize];
            if !item.link.is_linked() {
                eviction.push(item.clone());
            }
            if self.clean_regions.len() < self.reclaimer.runners() {
                Some(eviction.pop().unwrap())
            } else {
                None
            }
        };

        if let Some(item) = to_reclaim {
            self.submit_reclaim_task(ReclaimTask { region_id: item.id })
                .await
        }
    }

    #[tracing::instrument(skip(self))]
    pub fn clean_regions(&self) -> &AsyncQueue<RegionId> {
        &self.clean_regions
    }

    async fn submit_reclaim_task(&self, task: ReclaimTask) {
        if let Err(e) = self.reclaimer.submit(task).await {
            tracing::warn!("fail to submit reclaim task: {:?}", e);
        }
    }

    async fn submit_flush_task(&self, task: FlushTask) {
        if let Err(e) = self.flusher.submit(task).await {
            tracing::warn!("fail to submit reclaim task: {:?}", e);
        }
    }
}
