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

use foyer_common::async_queue::AsyncQueue;
use foyer_memory::{Cache, CacheBuilder, EvictionConfig};

use itertools::Itertools;

use crate::{
    device::Device,
    region::{Region, RegionId},
};

#[derive(Debug)]
pub struct RegionManager<D>
where
    D: Device,
{
    /// Empty regions.
    clean_regions: AsyncQueue<RegionId>,

    regions: Vec<Region<D>>,

    eviction: Cache<RegionId, ()>,
}

impl<D> RegionManager<D>
where
    D: Device,
{
    pub fn new(region_count: usize, eviction_config: EvictionConfig, device: D) -> Self {
        let clean_regions = AsyncQueue::new();

        let eviction = CacheBuilder::new(region_count)
            .with_object_pool_capacity(region_count)
            .with_eviction_config(eviction_config)
            .with_shards(1)
            .build();

        let regions = (0..region_count as RegionId)
            .map(|id| Region::new(id, device.clone()))
            .collect_vec();

        Self {
            clean_regions,
            regions,
            eviction,
        }
    }

    pub fn region(&self, id: &RegionId) -> &Region<D> {
        &self.regions[*id as usize]
    }

    #[tracing::instrument(skip(self))]
    pub fn record_access(&self, id: &RegionId) {
        self.eviction.touch(id);
    }

    pub fn clean_regions(&self) -> &AsyncQueue<RegionId> {
        &self.clean_regions
    }

    pub fn eviction_push(&self, region_id: RegionId) {
        self.eviction.insert(region_id, (), 1);
    }

    pub fn eviction_pop(&self) -> Option<RegionId> {
        self.eviction.pop().map(|entry| *entry.key())
    }
}
