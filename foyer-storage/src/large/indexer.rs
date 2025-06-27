// Copyright 2025 foyer Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    collections::{HashMap, hash_map::Entry},
    sync::Arc,
};

use itertools::Itertools;
use parking_lot::RwLock;

use crate::{device::RegionId, large::serde::Sequence};

#[derive(Debug)]
pub struct HashedEntryAddress {
    pub hash: u64,
    pub address: EntryAddress,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryAddress {
    pub region: RegionId,
    pub offset: u32,
    pub len: u32,

    pub sequence: Sequence,
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

    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::storage::large::indexer::insert_batch")
    )]
    pub fn insert_batch(&self, batch: Vec<HashedEntryAddress>) -> Vec<HashedEntryAddress> {
        let shards: HashMap<usize, Vec<HashedEntryAddress>> =
            batch.into_iter().into_group_map_by(|haddr| self.shard(haddr.hash));

        let mut olds = vec![];
        for (s, batch) in shards {
            let mut shard = self.shards[s].write();
            for haddr in batch {
                if let Some(old) = self.insert_inner(&mut shard, haddr.hash, haddr.address) {
                    olds.push(HashedEntryAddress {
                        hash: haddr.hash,
                        address: old,
                    });
                }
            }
        }
        olds
    }

    #[cfg_attr(feature = "tracing", fastrace::trace(name = "foyer::storage::large::indexer::get"))]
    pub fn get(&self, hash: u64) -> Option<EntryAddress> {
        let shard = self.shard(hash);
        self.shards[shard].read().get(&hash).cloned()
    }

    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::storage::large::indexer::remove")
    )]
    pub fn remove(&self, hash: u64) -> Option<EntryAddress> {
        let shard = self.shard(hash);
        self.shards[shard].write().remove(&hash)
    }

    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::storage::large::indexer::remove_batch")
    )]
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

    #[cfg_attr(feature = "tracing", fastrace::trace(name = "foyer::storage::large::indexer::clear"))]
    pub fn clear(&self) {
        self.shards.iter().for_each(|shard| shard.write().clear());
    }

    #[inline(always)]
    fn shard(&self, hash: u64) -> usize {
        hash as usize % self.shards.len()
    }

    fn insert_inner(
        &self,
        shard: &mut HashMap<u64, EntryAddress>,
        hash: u64,
        addr: EntryAddress,
    ) -> Option<EntryAddress> {
        match shard.entry(hash) {
            Entry::Occupied(mut o) => {
                // `>` for updates.
                // '=' for reinsertions.
                if addr.sequence >= o.get().sequence {
                    Some(o.insert(addr))
                } else {
                    Some(addr)
                }
            }
            Entry::Vacant(v) => {
                v.insert(addr);
                None
            }
        }
    }
}
