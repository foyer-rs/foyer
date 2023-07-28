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

use foyer_common::queue::AsyncQueue;
use foyer_intrusive::{
    core::adapter::Link,
    eviction::{EvictionPolicy, EvictionPolicyExt},
    intrusive_adapter, key_adapter, priority_adapter,
};
use parking_lot::RwLock;
use tokio::sync::RwLock as AsyncRwLock;

use crate::{
    device::Device,
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

#[derive(Debug)]
struct RegionManagerInner {
    current: Option<RegionId>,
}

/// Manager of regions and buffer pools.
///
/// # Region Lifetime
///
/// `clean` ==(allocate)=> `dirty` ==(flush)=> `evictable` ==(reclaim)=> `clean`
#[derive(Debug)]
pub struct RegionManager<D, EP, EL>
where
    D: Device,
    EP: EvictionPolicy<Adapter = RegionEpItemAdapter<EL>>,
    EL: Link,
{
    inner: Arc<AsyncRwLock<RegionManagerInner>>,

    buffers: Arc<AsyncQueue<Vec<u8, D::IoBufferAllocator>>>,

    /// Empty regions.
    clean_regions: Arc<AsyncQueue<RegionId>>,

    /// Regions with dirty buffer waiting for flushing.
    dirty_regions: Arc<AsyncQueue<RegionId>>,

    regions: Vec<Region<D>>,
    items: Vec<Arc<RegionEpItem<EL>>>,

    eviction: RwLock<EP>,
}

impl<D, EP, EL> RegionManager<D, EP, EL>
where
    D: Device,
    EP: EvictionPolicy<Adapter = RegionEpItemAdapter<EL>>,
    EL: Link,
{
    pub fn new(
        region_nums: usize,
        eviction_config: EP::Config,
        buffers: Arc<AsyncQueue<Vec<u8, D::IoBufferAllocator>>>,
        clean_regions: Arc<AsyncQueue<RegionId>>,
        device: D,
    ) -> Self {
        let eviction = EP::new(eviction_config);
        let dirty_regions = Arc::new(AsyncQueue::new());

        let mut regions = Vec::with_capacity(region_nums);
        let mut items = Vec::with_capacity(region_nums);

        for id in 0..region_nums as RegionId {
            let region = Region::new(id, device.clone());
            let item = Arc::new(RegionEpItem {
                link: EL::default(),
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
            dirty_regions,
            regions,
            items,
            eviction: RwLock::new(eviction),
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
                    // current region is full, append dirty regions
                    self.dirty_regions.release(region_id);
                    inner.current = None;
                    return AllocateResult::Full { slice, remain };
                }
            }
        }

        assert!(inner.current.is_none());

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
    pub fn region(&self, id: &RegionId) -> &Region<D> {
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

    pub fn buffers(&self) -> &AsyncQueue<Vec<u8, D::IoBufferAllocator>> {
        &self.buffers
    }

    pub fn clean_regions(&self) -> &AsyncQueue<RegionId> {
        &self.clean_regions
    }

    pub fn dirty_regions(&self) -> &AsyncQueue<RegionId> {
        &self.dirty_regions
    }

    pub fn eviction_push(&self, region_id: RegionId) {
        self.eviction
            .write()
            .push(self.items[region_id as usize].clone());
    }

    pub fn eviction_pop(&self) -> Option<RegionId> {
        self.eviction.write().pop().map(|item| item.id)
    }
}
