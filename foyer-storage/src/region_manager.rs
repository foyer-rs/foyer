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
use foyer_memory::{
    Cache, CacheConfig, DefaultCacheEventListener, FifoCacheConfig, FifoConfig, LfuCacheConfig, LfuConfig,
    LruCacheConfig, LruConfig, S3FifoCacheConfig, S3FifoConfig,
};

use itertools::Itertools;

use crate::{
    device::Device,
    region::{Region, RegionId},
};

#[derive(Debug, Clone)]
pub enum EvictionConfg {
    Fifo(FifoConfig),
    Lru(LruConfig),
    Lfu(LfuConfig),
    S3Fifo(S3FifoConfig),
}

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
    pub fn new(region_count: usize, eviction_config: EvictionConfg, device: D) -> Self {
        let clean_regions = AsyncQueue::new();

        // TODO(MrCroxx): REFINE ME!!!
        let cache_config = match eviction_config {
            EvictionConfg::Fifo(eviction_config) => CacheConfig::Fifo(FifoCacheConfig {
                capacity: region_count,
                shards: 1,
                eviction_config,
                object_pool_capacity: region_count,
                hash_builder: ahash::RandomState::default(),
                event_listener: DefaultCacheEventListener::default(),
            }),
            EvictionConfg::Lru(eviction_config) => CacheConfig::Lru(LruCacheConfig {
                capacity: region_count,
                shards: 1,
                eviction_config,
                object_pool_capacity: region_count,
                hash_builder: ahash::RandomState::default(),
                event_listener: DefaultCacheEventListener::default(),
            }),
            EvictionConfg::Lfu(eviction_config) => CacheConfig::Lfu(LfuCacheConfig {
                capacity: region_count,
                shards: 1,
                eviction_config,
                object_pool_capacity: region_count,
                hash_builder: ahash::RandomState::default(),
                event_listener: DefaultCacheEventListener::default(),
            }),
            EvictionConfg::S3Fifo(eviction_config) => CacheConfig::S3Fifo(S3FifoCacheConfig {
                capacity: region_count,
                shards: 1,
                eviction_config,
                object_pool_capacity: region_count,
                hash_builder: ahash::RandomState::default(),
                event_listener: DefaultCacheEventListener::default(),
            }),
        };
        let eviction = Cache::new(cache_config);

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
        let _ = self.eviction.get(id);
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
