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

use std::{collections::HashMap, sync::Arc};

use itertools::Itertools;
use parking_lot::{Mutex, RwLock};

use super::device::RegionId;

#[derive(Debug, Clone)]
pub struct EntryAddress {
    pub region: RegionId,
    pub offset: u32,
    pub len: u32,
}

/// [`Indexer`] records key hash to entry address on fs.
#[derive(Debug, Clone)]
pub struct Indexer {
    regions: Arc<Vec<Mutex<Vec<u64>>>>,
    shards: Arc<Vec<RwLock<HashMap<u64, EntryAddress>>>>,
}

impl Indexer {
    pub fn new(regions: usize, shards: usize) -> Self {
        let regions = (0..regions).map(|_| Mutex::new(vec![])).collect_vec();
        let shards = (0..shards).map(|_| RwLock::new(HashMap::new())).collect_vec();
        Self {
            regions: Arc::new(regions),
            shards: Arc::new(shards),
        }
    }

    pub fn insert(&self, hash: u64, addr: EntryAddress) -> Option<EntryAddress> {
        let s = self.shard(hash);
        let r = addr.region as usize;

        let mut region = self.regions[r].lock();
        let mut shard = self.shards[s].write();

        region.push(hash);
        // The old region hash cannot be removed to prevent from phantom entry on hash collision.
        shard.insert(hash, addr)
    }

    pub fn insert_batch(&self, batch: Vec<(u64, EntryAddress)>) -> Vec<(u64, EntryAddress)> {
        let regions = batch
            .iter()
            .map(|(hash, addr)| (addr.region as usize, *hash))
            .into_group_map();
        let shards: HashMap<usize, Vec<(u64, EntryAddress)>> =
            batch.into_iter().into_group_map_by(|(hash, _)| self.shard(*hash));

        for (r, mut hashes) in regions {
            self.regions[r].lock().append(&mut hashes);
        }

        let mut olds = vec![];
        for (s, batch) in shards {
            let mut shard = self.shards[s].write();
            for (hash, addr) in batch {
                if let Some(old) = shard.insert(hash, addr) {
                    olds.push((hash, old));
                }
            }
        }
        olds
    }

    pub fn get(&self, hash: u64) -> Option<EntryAddress> {
        let shard = self.shard(hash);
        self.shards[shard].read().get(&hash).cloned()
    }

    pub fn remove(&self, hash: u64) -> Option<EntryAddress> {
        let shard = self.shard(hash);
        self.shards[shard].write().remove(&hash)
    }

    pub fn remove_batch(&self, hashes: &[u64]) -> Vec<EntryAddress> {
        let shards = hashes.iter().into_group_map_by(|&hash| self.shard(*hash));

        let mut olds = vec![];
        for (s, hashes) in shards {
            let mut shard = self.shards[s].write();
            for hash in hashes {
                if let Some(old) = shard.remove(hash) {
                    olds.push(old);
                }
            }
        }
        olds
    }

    pub fn take_region(&self, region: RegionId) -> Vec<u64> {
        std::mem::take(self.regions[region as usize].lock().as_mut())
    }

    pub fn clear(&self) {
        self.regions.iter().for_each(|region| region.lock().clear());
        self.shards.iter().for_each(|shard| shard.write().clear());
    }

    pub fn take(&self) -> Vec<(u64, EntryAddress)> {
        self.regions.iter().for_each(|region| region.lock().clear());

        let mut res = vec![];
        for shard in self.shards.iter() {
            let mut shard = shard.write();
            res.extend(shard.drain());
        }
        res
    }

    #[inline(always)]
    fn shard(&self, hash: u64) -> usize {
        hash as usize % self.shards.len()
    }
}
