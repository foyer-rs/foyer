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

use foyer_intrusive::{
    core::{adapter::Link, pointer::PointerOps},
    eviction::EvictionPolicy,
    intrusive_adapter, key_adapter,
};
use foyer_utils::queue::AsyncQueue;
use tokio::sync::RwLock;

use crate::{
    device::{BufferAllocator, Device},
    flusher::{FlushTask, Flusher},
    reclaimer::{ReclaimTask, Reclaimer},
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

struct RegionManagerInner<A, E, EL>
where
    A: BufferAllocator,
    E: EvictionPolicy<RegionEpItemAdapter<EL>, Link = EL>,
    EL: Link,
{
    current: Option<RegionId>,

    buffers: Arc<AsyncQueue<Vec<u8, A>>>,
    clean_regions: Arc<AsyncQueue<RegionId>>,

    eviction: E,

    _marker: PhantomData<EL>,
}

pub struct RegionManager<A, D, E, EL>
where
    A: BufferAllocator,
    D: Device<IoBufferAllocator = A>,
    E: EvictionPolicy<RegionEpItemAdapter<EL>, Link = EL>,
    EL: Link,
{
    inner: Arc<RwLock<RegionManagerInner<A, E, EL>>>,

    regions: Vec<Region<A, D>>,
    items: Vec<Arc<RegionEpItem<EL>>>,

    flusher: Arc<Flusher>,
    reclaimer: Arc<Reclaimer>,
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
            });

            regions.push(region);
            items.push(item);
        }

        let inner = RegionManagerInner {
            current: None,
            buffers,
            clean_regions,
            eviction,
            _marker: PhantomData,
        };

        Self {
            inner: Arc::new(RwLock::new(inner)),
            regions,
            items,
            flusher,
            reclaimer,
        }
    }

    /// Allocate a buffer slice with given size in an active region to write.
    pub async fn allocate(&self, size: usize) -> WriteSlice {
        let mut inner = self.inner.write().await;

        // try allocate from current region
        if let Some(region_id) = inner.current {
            let region = self.region(&region_id);
            if let Some(slice) = region.allocate(size) {
                return slice;
            }

            // current region is full, schedule flushing
            self.flusher.submit(FlushTask { region_id }).await.unwrap();
            inner.current = None;
        }

        assert!(inner.current.is_none());

        tracing::debug!("clean regions: {}", inner.clean_regions.len());
        if inner.clean_regions.len() < self.reclaimer.runners() {
            if let Some(item) = inner.eviction.pop() {
                self.reclaimer
                    .submit(ReclaimTask { region_id: item.id })
                    .await
                    .unwrap();
            }
        }

        let region_id = inner.clean_regions.acquire().await;
        tracing::info!("switch to clean region: {}", region_id);

        let region = self.region(&region_id);
        let buffer = inner.buffers.acquire().await;
        region.attach_buffer(buffer);

        let slice = region.allocate(size).unwrap();

        region.advance();
        inner.current = Some(region_id);

        slice
    }

    pub fn region(&self, id: &RegionId) -> &Region<A, D> {
        &self.regions[*id as usize]
    }

    pub async fn record_access(&self, id: &RegionId) {
        let mut inner = self.inner.write().await;
        let item = &self.items[*id as usize];
        if item.link.is_linked() {
            inner.eviction.access(&self.items[*id as usize]);
        }
    }

    pub async fn post_flush(&self, id: &RegionId) {
        let mut inner = self.inner.write().await;
        inner.eviction.push(self.items[*id as usize].clone());
    }
}
