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
};

use foyer_common::code::Key;
use itertools::Itertools;
use parking_lot::{Mutex, RwLock};
use twox_hash::XxHash64;

use crate::region::{RegionId, Version};

pub type Sequence = u64;

#[derive(Debug, Clone)]
pub enum Index {
    RingBuffer {},
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
pub struct IndexInfo {
    pub sequence: Sequence,
    pub index: Index,
}

#[derive(Debug)]
pub struct Catalog<K>
where
    K: Key,
{
    /// `items` sharding bits.
    bits: usize,

    /// Sharded by key hash.
    infos: Vec<RwLock<BTreeMap<Arc<K>, IndexInfo>>>,

    /// Sharded by region id.
    regions: Vec<Mutex<BTreeMap<Arc<K>, u64>>>,
}

impl<K> Catalog<K>
where
    K: Key,
{
    pub fn new(regions: usize, bits: usize) -> Self {
        let infos = (0..1 << bits)
            .map(|_| RwLock::new(BTreeMap::new()))
            .collect_vec();
        let regions = (0..regions)
            .map(|_| Mutex::new(BTreeMap::new()))
            .collect_vec();
        Self {
            bits,
            infos,
            regions,
        }
    }

    pub fn insert(&self, key: K, info: IndexInfo) {
        // TODO(MrCroxx): compare sequence.
        let key = Arc::new(key);

        if let Index::Region { region, .. } = info.index {
            self.regions[region as usize]
                .lock()
                .insert(key.clone(), info.sequence);
        };

        let shard = self.shard(&key);
        // TODO(MrCroxx): handle old key?
        let _ = self.infos[shard].write().insert(key.clone(), info);
    }

    pub fn lookup(&self, key: &K) -> Option<IndexInfo> {
        let shard = self.shard(key);
        self.infos[shard].read().get(key).cloned()
    }

    pub fn remove(&self, key: &K) -> Option<IndexInfo> {
        let shard = self.shard(key);
        let info: Option<IndexInfo> = self.infos[shard].write().remove(key);
        if let Some(info) = &info && let Index::Region { region,..} = info.index {
            self.regions[region as usize].lock().remove(key);
        }
        info
    }

    pub fn take_region(&self, region: &RegionId) -> Vec<IndexInfo> {
        let mut keys = BTreeMap::new();
        std::mem::swap(&mut *self.regions[*region as usize].lock(), &mut keys);

        let mut infos = Vec::with_capacity(keys.len());
        for (key, sequence) in keys {
            let shard = self.shard(&key);
            match self.infos[shard].write().entry(key.clone()) {
                Entry::Vacant(_) => continue,
                Entry::Occupied(o) => {
                    if o.get().sequence == sequence {
                        let info = o.remove();
                        infos.push(info);
                    }
                }
            };
        }
        infos
    }

    pub fn clear(&self) {
        for shard in self.infos.iter() {
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
