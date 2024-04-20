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

use std::{borrow::Borrow, fmt::Debug, hash::Hash, sync::Arc, time::Instant};

use ahash::RandomState;
use foyer_common::{
    arc_key_hash_map::{ArcKeyHashMap, Entry},
    code::{StorageKey, StorageValue},
};
use itertools::Itertools;
use parking_lot::{Mutex, RwLock};

use crate::{
    metrics::Metrics,
    region::{RegionId, RegionView},
};

pub type Sequence = u64;

#[derive(Debug)]
pub enum Index<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    Inflight { key: Arc<K>, value: Arc<V> },
    Region { view: RegionView },
}

impl<K, V> Clone for Index<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn clone(&self) -> Self {
        match self {
            Self::Inflight { key, value } => Self::Inflight {
                key: key.clone(),
                value: value.clone(),
            },
            Self::Region { view } => Self::Region { view: view.clone() },
        }
    }
}

#[derive(Debug)]
pub struct Item<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    sequence: Sequence,
    index: Index<K, V>,

    inserted: Option<Instant>,
}

impl<K, V> Clone for Item<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn clone(&self) -> Self {
        Self {
            sequence: self.sequence,
            index: self.index.clone(),
            inserted: self.inserted,
        }
    }
}

impl<K, V> Item<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    pub fn new(sequence: Sequence, index: Index<K, V>) -> Self {
        Self {
            sequence,
            index,
            inserted: None,
        }
    }

    pub fn sequence(&self) -> &Sequence {
        &self.sequence
    }

    pub fn index(&self) -> &Index<K, V> {
        &self.index
    }

    pub fn consume(self) -> (Sequence, Index<K, V>) {
        (self.sequence, self.index)
    }
}

#[derive(Debug)]
pub struct Catalog<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    /// Sharded by key hash.
    items: Vec<RwLock<ArcKeyHashMap<K, Item<K, V>>>>,

    /// Sharded by region id.
    regions: Vec<Mutex<ArcKeyHashMap<K, u64>>>,

    hash_builder: RandomState,

    metrics: Arc<Metrics>,
}

impl<K, V> Catalog<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    pub fn new(regions: usize, shards: usize, metrics: Arc<Metrics>) -> Self {
        assert!(shards > 0, "catalog shard count must be > 0, given: {}", shards);

        let items = (0..shards).map(|_| RwLock::new(ArcKeyHashMap::new())).collect_vec();
        let regions = (0..regions).map(|_| Mutex::new(ArcKeyHashMap::new())).collect_vec();

        let hash_builder = RandomState::default();

        Self {
            items,
            regions,

            hash_builder,

            metrics,
        }
    }

    pub fn insert(&self, key: Arc<K>, mut item: Item<K, V>) {
        // TODO(MrCroxx): compare sequence.

        if let Index::Region { view } = &item.index {
            self.regions[*view.id() as usize]
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
        // TODO(MrCroxx): Use `let_chains` here after it is stable.
        if let Some(old) = old {
            if let Index::Inflight { .. } = old.index() {
                self.metrics
                    .inner_op_duration_entry_flush
                    .observe(old.inserted.unwrap().elapsed().as_secs_f64());
            }
        }
    }

    pub fn lookup<Q>(&self, key: &Q) -> Option<Item<K, V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let shard = self.shard(key);
        self.items[shard].read().get(key).cloned()
    }

    pub fn remove<Q>(&self, key: &Q) -> Option<Item<K, V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let shard = self.shard(key);
        let info: Option<Item<K, V>> = self.items[shard].write().remove(key);
        // TODO(MrCroxx): Use `let_chains` here after it is stable.
        if let Some(info) = &info {
            if let Index::Region { view } = &info.index {
                self.regions[*view.id() as usize].lock().remove(key);
            }
        }
        info
    }

    pub fn take_region(&self, region: &RegionId) -> Vec<(Arc<K>, Item<K, V>)> {
        let mut keys = ArcKeyHashMap::new();
        std::mem::swap(&mut *self.regions[*region as usize].lock(), &mut keys);

        let mut items = Vec::with_capacity(keys.len());
        for (key, sequence) in keys {
            let shard = self.shard(&key);
            match self.items[shard].write().entry(key.clone()) {
                Entry::Vacant(_) => continue,
                Entry::Occupied(o) => {
                    if o.get().value().sequence == sequence {
                        let (entry, _v) = o.remove();
                        let (key, item) = entry.take();
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

    #[inline(always)]
    fn shard<Q>(&self, key: &Q) -> usize
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.hash(key) as usize % self.items.len()
    }

    #[inline(always)]
    fn hash<Q>(&self, key: &Q) -> u64
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.hash_builder.hash_one(key)
    }
}
