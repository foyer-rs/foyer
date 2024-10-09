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

use itertools::Itertools;
use ordered_hash_map::OrderedHashMap;
use parking_lot::{MappedRwLockReadGuard, RwLock, RwLockReadGuard};

use super::set::{SetId, SetStorage};

#[derive(Debug)]
pub struct SetCacheShard {
    cache: OrderedHashMap<SetId, SetStorage>,
    capacity: usize,
}

/// In-memory set cache to reduce disk io.
///
/// Simple FIFO cache.
#[derive(Debug)]
pub struct SetCache {
    shards: Vec<RwLock<SetCacheShard>>,
}

impl SetCache {
    pub fn new(capacity: usize, shards: usize) -> Self {
        let shard_capacity = capacity / shards;
        let shards = (0..shards)
            .map(|_| {
                RwLock::new(SetCacheShard {
                    cache: OrderedHashMap::with_capacity(shard_capacity),
                    capacity: shard_capacity,
                })
            })
            .collect_vec();
        Self { shards }
    }

    pub fn insert(&self, id: SetId, storage: SetStorage) {
        let mut shard = self.shards[self.shard(&id)].write();
        if shard.cache.len() == shard.capacity {
            shard.cache.pop_front();
        }

        assert!(shard.cache.len() < shard.capacity);

        shard.cache.insert(id, storage);
    }

    pub fn invalid(&self, id: &SetId) {
        let mut shard = self.shards[self.shard(id)].write();
        shard.cache.remove(id);
    }

    pub fn lookup(&self, id: &SetId) -> Option<MappedRwLockReadGuard<'_, SetStorage>> {
        RwLockReadGuard::try_map(self.shards[self.shard(id)].read(), |shard| shard.cache.get(id)).ok()
    }

    fn shard(&self, id: &SetId) -> usize {
        *id as usize % self.shards.len()
    }
}
