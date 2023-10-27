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

use std::{
    collections::btree_map::{BTreeMap, Entry},
    hash::Hasher,
    sync::Arc,
    time::Instant,
};

use foyer_common::code::Key;
use itertools::Itertools;
use parking_lot::{Mutex, RwLock};
use twox_hash::XxHash64;

use crate::{
    device::BufferAllocator,
    metrics::Metrics,
    region::{RegionId, Version},
    ring::View,
};

pub type Sequence = u64;

#[derive(Debug, Clone)]
pub enum Index {
    RingBuffer {
        view: View,
    },
    Region {
        region: RegionId,
        version: Version,
        offset: u32,
        len: u32,
        key_len: u32,
        value_len: u32,
    },
}

#[derive(Debug, Clone)]
pub struct Item {
    sequence: Sequence,
    index: Index,

    inserted: Option<Instant>,
}

impl Item {
    pub fn new(sequence: Sequence, index: Index) -> Self {
        Self {
            sequence,
            index,
            inserted: None,
        }
    }

    pub fn sequence(&self) -> &Sequence {
        &self.sequence
    }

    pub fn index(&self) -> &Index {
        &self.index
    }
}

#[derive(Debug)]
pub struct Catalog<K>
where
    K: Key,
{
    /// `items` sharding bits.
    bits: usize,

    /// Sharded by key hash.
    items: Vec<RwLock<BTreeMap<Arc<K>, Item>>>,

    /// Sharded by region id.
    regions: Vec<Mutex<BTreeMap<Arc<K>, u64>>>,

    metrics: Arc<Metrics>,
}

impl<K> Catalog<K>
where
    K: Key,
{
    pub fn new(regions: usize, bits: usize, metrics: Arc<Metrics>) -> Self {
        let infos = (0..1 << bits)
            .map(|_| RwLock::new(BTreeMap::new()))
            .collect_vec();
        let regions = (0..regions)
            .map(|_| Mutex::new(BTreeMap::new()))
            .collect_vec();
        Self {
            bits,
            items: infos,
            regions,

            metrics,
        }
    }

    pub fn insert(&self, key: Arc<K>, mut item: Item) {
        // TODO(MrCroxx): compare sequence.

        if let Index::Region { region, .. } = item.index {
            self.regions[region as usize]
                .lock()
                .insert(key.clone(), item.sequence);
        };

        let shard = self.shard(&key);
        // TODO(MrCroxx): handle old key?
        let old = {
            let mut guard = self.items[shard].write();
            item.inserted = Some(Instant::now());
            guard.insert(key.clone(), item)
        };
        if let Some(old) = old && let Index::RingBuffer { .. } = old.index() {
            self.metrics.inner_op_duration_entry_flush.observe(old.inserted.unwrap().elapsed().as_secs_f64());
        }
    }

    pub fn lookup(&self, key: &K) -> Option<Item> {
        let shard = self.shard(key);
        self.items[shard].read().get(key).cloned()
    }

    pub fn remove(&self, key: &K) -> Option<Item> {
        let shard = self.shard(key);
        let info: Option<Item> = self.items[shard].write().remove(key);
        if let Some(info) = &info && let Index::Region { region,..} = info.index {
            self.regions[region as usize].lock().remove(key);
        }
        info
    }

    pub fn take_region(&self, region: &RegionId) -> Vec<(Arc<K>, Item)> {
        let mut keys = BTreeMap::new();
        std::mem::swap(&mut *self.regions[*region as usize].lock(), &mut keys);

        let mut items = Vec::with_capacity(keys.len());
        for (key, sequence) in keys {
            let shard = self.shard(&key);
            match self.items[shard].write().entry(key.clone()) {
                Entry::Vacant(_) => continue,
                Entry::Occupied(o) => {
                    if o.get().sequence == sequence {
                        let item = o.remove();
                        items.push((key.clone(), item));
                    }
                }
            };
        }
        items
    }

    pub fn clear(&self) {
        for shard in self.items.iter() {
            shard.write().clear();
        }
        for region in self.regions.iter() {
            region.lock().clear();
        }
    }

    fn shard(&self, key: &K) -> usize {
        self.hash(key) as usize & ((1 << self.bits) - 1)
    }

    fn hash(&self, key: &K) -> u64 {
        let mut hasher = XxHash64::default();
        key.hash(&mut hasher);
        hasher.finish()
    }
}
