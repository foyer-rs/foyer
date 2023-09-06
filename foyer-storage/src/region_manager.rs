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

use foyer_common::{
    batch::{Batch, Identity},
    queue::AsyncQueue,
};
use foyer_intrusive::{
    core::adapter::Link,
    eviction::{EvictionPolicy, EvictionPolicyExt},
    intrusive_adapter, key_adapter,
};
use parking_lot::{Mutex, RwLock};
use tracing::Instrument;

use crate::{
    device::Device,
    metrics::Metrics,
    region::{Region, RegionId, WriteSlice},
};

#[derive(Debug)]
pub struct RegionEpItem<L>
where
    L: Link,
{
    link: L,
    id: RegionId,
}

intrusive_adapter! { pub RegionEpItemAdapter<L> = Arc<RegionEpItem<L>>: RegionEpItem<L> { link: L } where L: Link }
key_adapter! { RegionEpItemAdapter<L> = RegionEpItem<L> { id: RegionId } where L: Link }

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
    current: Mutex<Option<RegionId>>,
    rotate_batch: Batch<(), ()>,

    /// Buffer pool for dirty buffers.
    buffers: AsyncQueue<Vec<u8, D::IoBufferAllocator>>,

    /// Empty regions.
    clean_regions: AsyncQueue<RegionId>,

    /// Regions with dirty buffer waiting for flushing.
    dirty_regions: AsyncQueue<RegionId>,

    regions: Vec<Region<D>>,
    items: Vec<Arc<RegionEpItem<EL>>>,

    /// Eviction policy.
    eviction: RwLock<EP>,

    metrics: Arc<Metrics>,
}

impl<D, EP, EL> RegionManager<D, EP, EL>
where
    D: Device,
    EP: EvictionPolicy<Adapter = RegionEpItemAdapter<EL>>,
    EL: Link,
{
    pub fn new(
        buffer_count: usize,
        region_count: usize,
        eviction_config: EP::Config,
        device: D,
        metrics: Arc<Metrics>,
    ) -> Self {
        let buffers = AsyncQueue::new();
        for _ in 0..buffer_count {
            let len = device.region_size();
            let buffer = device.io_buffer(len, len);
            buffers.release(buffer);
        }

        let eviction = EP::new(eviction_config);
        let clean_regions = AsyncQueue::new();
        let dirty_regions = AsyncQueue::new();

        let mut regions = Vec::with_capacity(region_count);
        let mut items = Vec::with_capacity(region_count);

        for id in 0..region_count as RegionId {
            let region = Region::new(id, device.clone());
            let item = Arc::new(RegionEpItem {
                link: EL::default(),
                id,
            });

            regions.push(region);
            items.push(item);
        }

        Self {
            current: Mutex::new(None),
            rotate_batch: Batch::new(),
            buffers,
            clean_regions,
            dirty_regions,
            regions,
            items,
            eviction: RwLock::new(eviction),
            metrics,
        }
    }

    /// Allocate a buffer slice with given size in an active region to write.
    #[tracing::instrument(skip(self))]
    pub async fn allocate(&self, size: usize, must_allocate: bool) -> Option<WriteSlice> {
        loop {
            let res = self.allocate_inner(size);

            if res.is_some() || !must_allocate {
                return res;
            }

            self.rotate().await;
        }
    }

    #[tracing::instrument(skip(self))]
    pub fn allocate_inner(&self, size: usize) -> Option<WriteSlice> {
        let mut current = self.current.lock();

        if let Some(region_id) = *current {
            match self.region(&region_id).allocate(size) {
                crate::region::AllocateResult::Ok(slice) => Some(slice),
                crate::region::AllocateResult::Full { .. } => {
                    self.dirty_regions.release(region_id);
                    *current = None;
                    None
                }
                crate::region::AllocateResult::None => None,
            }
        } else {
            None
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn rotate(&self) {
        match self.rotate_batch.push(()) {
            Identity::Leader(rx) => {
                // Wait a clean region to be released.
                let timer = self
                    .metrics
                    .inner_op_duration_acquire_clean_region
                    .start_timer();
                let region_id = self
                    .clean_regions
                    .acquire()
                    .instrument(tracing::debug_span!("acquire_clean_region"))
                    .await;
                drop(timer);

                tracing::info!("switch to clean region: {}", region_id);

                let region = self.region(&region_id);
                region.advance().await;

                let buffer = self
                    .buffers
                    .acquire()
                    .instrument(tracing::debug_span!("acquire_clean_buffer"))
                    .await;
                region.attach_buffer(buffer).await;

                *self.current.lock() = Some(region_id);

                // Notify the batch to advance.
                let items = self.rotate_batch.rotate();
                tracing::Span::current().record("followers", items.len());
                for item in items {
                    item.tx.send(()).unwrap();
                }
                rx.await.unwrap();
            }
            Identity::Follower(rx) => rx.await.unwrap(),
        }
    }

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
