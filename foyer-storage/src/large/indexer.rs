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
use parking_lot::RwLock;

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
    shards: Arc<Vec<RwLock<HashMap<u64, EntryAddress>>>>,
}

impl Indexer {
    pub fn new(shards: usize) -> Self {
        let shards = (0..shards).map(|_| RwLock::new(HashMap::new())).collect_vec();
        Self {
            shards: Arc::new(shards),
        }
    }

    pub fn insert(&self, hash: u64, addr: EntryAddress) -> Option<EntryAddress> {
        let shard = self.shard(hash);
        self.shards[shard].write().insert(hash, addr)
    }

    pub fn insert_batch(&self, batch: Vec<(u64, EntryAddress)>) -> Vec<(u64, EntryAddress)> {
        let shards: HashMap<usize, Vec<(u64, EntryAddress)>> =
            batch.into_iter().into_group_map_by(|(hash, _)| self.shard(*hash));
        let mut olds = vec![];
        for (shard, batch) in shards {
            let mut shard = self.shards[shard].write();
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

    #[inline(always)]
    fn shard(&self, hash: u64) -> usize {
        hash as usize % self.shards.len()
    }
}
