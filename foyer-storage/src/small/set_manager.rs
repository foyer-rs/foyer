//  Copyright 2024 foyer Project Authors
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

use std::{collections::HashSet, fmt::Debug, ops::Range, sync::Arc};

use foyer_common::code::{HashBuilder, StorageKey, StorageValue};
use itertools::Itertools;
use parking_lot::RwLock;
use tokio::sync::RwLock as AsyncRwLock;

use super::{
    batch::Item,
    bloom_filter::BloomFilterU64,
    generic::GenericSmallStorageConfig,
    set::{SetId, SetStorage},
    set_cache::SetCache,
};
use crate::{
    device::{Dev, MonitoredDevice, RegionId},
    error::Result,
    manifest::{Manifest, Metadata},
};

/// # Lock Order
///
/// load (async set cache, not good):
///
/// ```plain
///                                                                  |------------ requires async mutex -------------|
/// lock(R) bloom filter => unlock(R) bloom filter => lock(R) set => lock(e) set cache => load => unlock(e) set cache => unlock(r) set
/// ```
///
/// load (sync set cache, good):
///
/// ```plain
/// lock(R) bloom filter => unlock(R) bloom filter => lock(R) set => lock(e) set cache => unlock(e) set cache => load => lock(e) set cache => unlock(e) set cache => unlock(r) set
/// ```
///
/// update:
///
/// ```plain
/// lock(W) set => lock(e) set cache => invalid set cache => unlock(e) set cache => update set => lock(w) bloom filter => unlock(w) bloom filter => unlock(w) set
/// ```
struct SetManagerInner {
    // TODO(MrCroxx): Refine this!!! Make `Set` a RAII type.
    sets: Vec<AsyncRwLock<()>>,
    /// As a cache, it is okay that the bloom filter returns a false-negative result, which doesn't break the
    /// correctness.
    loose_bloom_filters: Vec<RwLock<BloomFilterU64<4>>>,
    set_cache: SetCache,
    set_picker: SetPicker,
    set_size: usize,

    device: MonitoredDevice,
    manifest: Manifest,

    regions: Range<RegionId>,
    flush: bool,
}

#[derive(Clone)]
pub struct SetManager {
    inner: Arc<SetManagerInner>,
}

impl Debug for SetManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SetManager")
            .field("sets", &self.inner.sets)
            .field("loose_bloom_filters", &self.inner.loose_bloom_filters)
            .field("set_picker", &self.inner.set_picker)
            .field("set_cache", &self.inner.set_cache)
            .field("set_size", &self.inner.set_size)
            .field("device", &self.inner.device)
            .field("manifest", &self.inner.manifest)
            .field("regions", &self.inner.regions)
            .field("flush", &self.inner.flush)
            .finish()
    }
}

impl SetManager {
    pub async fn open<K, V, S>(config: &GenericSmallStorageConfig<K, V, S>) -> Result<Self>
    where
        K: StorageKey,
        V: StorageValue,
        S: HashBuilder + Debug,
    {
        let device = config.device.clone();
        let regions = config.regions.clone();

        let sets = (device.region_size() / config.set_size) * (regions.end - regions.start) as usize;
        assert!(sets > 0); // TODO: assert > 1? Set with id = 0 is used as metadata.

        let set_picker = SetPicker::new(sets);

        let set_cache = SetCache::new(config.set_cache_capacity, config.set_cache_shards);
        let loose_bloom_filters = (0..sets).map(|_| RwLock::new(BloomFilterU64::new())).collect_vec();

        let sets = (0..sets).map(|_| AsyncRwLock::default()).collect_vec();

        let inner = SetManagerInner {
            sets,
            loose_bloom_filters,
            set_cache,
            set_picker,
            set_size: config.set_size,
            device,
            manifest: config.manifest.clone(),
            regions,
            flush: config.flush,
        };
        let inner = Arc::new(inner);
        Ok(Self { inner })
    }

    pub fn may_contains(&self, hash: u64) -> bool {
        let sid = self.inner.set_picker.pick(hash);
        self.inner.loose_bloom_filters[sid as usize].read().lookup(hash)
    }

    pub async fn load<K, V>(&self, hash: u64) -> Result<Option<(K, V)>>
    where
        K: StorageKey,
        V: StorageValue,
    {
        let sid = self.inner.set_picker.pick(hash);

        // Query bloom filter.
        if !self.inner.loose_bloom_filters[sid as usize].read().lookup(hash) {
            return Ok(None);
        }

        // Acquire set lock.
        let set = self.inner.sets[sid as usize].read().await;

        // Query form set cache.
        if let Some(cached) = self.inner.set_cache.lookup(&sid) {
            return cached.get(hash);
        }

        // Set cache miss, load from disk.
        let storage = self.storage(sid).await?;
        let res = storage.get(hash);

        // Update set cache on cache miss.
        self.inner.set_cache.insert(sid, storage);

        // Release set lock.
        drop(set);

        res
    }

    pub async fn update<K, V, S>(&self, sid: SetId, deletions: &HashSet<u64>, items: Vec<Item<K, V, S>>) -> Result<()>
    where
        K: StorageKey,
        V: StorageValue,
        S: HashBuilder + Debug,
    {
        // Acquire set lock.
        let set = self.inner.sets[sid as usize].write().await;

        self.inner.set_cache.invalid(&sid);

        let mut storage = self.storage(sid).await?;
        storage.apply(deletions, items);
        storage.update();

        *self.inner.loose_bloom_filters[sid as usize].write() = storage.bloom_filter().clone();

        let buffer = storage.freeze();
        let (region, offset) = self.locate(sid);
        self.inner.device.write(buffer, region, offset).await?;
        if self.inner.flush {
            self.inner.device.flush(Some(region)).await?;
        }

        // Release set lock.
        drop(set);

        Ok(())
    }

    pub fn sets(&self) -> usize {
        self.inner.sets.len()
    }

    pub fn set_size(&self) -> usize {
        self.inner.set_size
    }

    pub async fn watermark(&self) -> u128 {
        self.inner.manifest.timestamp_watermark().await
    }

    pub async fn destroy(&self) -> Result<()> {
        self.update_watermark().await?;
        self.inner.set_cache.clear();
        Ok(())
    }

    async fn update_watermark(&self) -> Result<()> {
        self.inner
            .manifest
            .update_timestamp_watermark(Metadata::timestamp())
            .await
    }

    async fn storage(&self, id: SetId) -> Result<SetStorage> {
        let (region, offset) = self.locate(id);
        let buffer = self.inner.device.read(region, offset, self.inner.set_size).await?;
        let storage = SetStorage::load(buffer, self.watermark().await);
        Ok(storage)
    }

    #[inline]
    fn region_sets(&self) -> usize {
        self.inner.device.region_size() / self.inner.set_size
    }

    #[inline]
    fn locate(&self, id: SetId) -> (RegionId, u64) {
        let region_sets = self.region_sets();
        let region = id as RegionId / region_sets as RegionId;
        let offset = ((id as usize % region_sets) * self.inner.set_size) as u64;
        (region, offset)
    }
}

#[derive(Debug, Clone)]
pub struct SetPicker {
    sets: usize,
}

impl SetPicker {
    /// Create a [`SetPicker`] with a total size count.
    ///
    /// The `sets` should be the count of all sets.

    pub fn new(sets: usize) -> Self {
        Self { sets }
    }

    pub fn pick(&self, hash: u64) -> SetId {
        // skip the meta set
        hash % (self.sets as SetId)
    }
}
