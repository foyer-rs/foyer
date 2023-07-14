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

use std::sync::Arc;

use foyer_common::code::{Key, Value};
use foyer_intrusive::{
    collections::{
        duplicated_hashmap::{DuplicatedHashMap, DuplicatedHashMapLink},
        hashmap::{HashMap, HashMapLink},
    },
    intrusive_adapter, key_adapter,
};
use itertools::Itertools;
use parking_lot::RwLock;

use crate::region::{RegionId, Version};

const HASHMAP_BITS_MAX: usize = 8;

#[derive(Debug, Clone, Copy)]
pub struct Index {
    pub region: RegionId,
    pub version: Version,
    pub offset: u32,
    pub len: u32,
    pub key_len: u32,
    pub value_len: u32,
}

#[derive(Debug)]
pub struct Item<K>
where
    K: Key,
{
    link_hashmap_key: HashMapLink,
    link_duplicated_hashmap_region: DuplicatedHashMapLink,

    key: K,
    region: RegionId,

    index: Index,
}

impl<K> Value for Item<K> where K: Key {}
unsafe impl<K> Send for Item<K> where K: Key {}
unsafe impl<K> Sync for Item<K> where K: Key {}

impl<K> Item<K>
where
    K: Key,
{
    fn new(key: K, index: Index) -> Self {
        Self {
            link_hashmap_key: HashMapLink::default(),
            link_duplicated_hashmap_region: DuplicatedHashMapLink::default(),
            key,
            region: index.region,
            index,
        }
    }
}

intrusive_adapter! { ItemHashMapKeyAdapter<K> = Arc<Item<K>>: Item<K> { link_hashmap_key: HashMapLink } where K: Key }
key_adapter! { ItemHashMapKeyAdapter<K> = Item<K> { key: K } where K: Key }

intrusive_adapter! { ItemDuplicatedHashMapRegionAdapter<K> = Arc<Item<K>>: Item<K> { link_duplicated_hashmap_region: DuplicatedHashMapLink } where K: Key }
key_adapter! { ItemDuplicatedHashMapRegionAdapter<K> = Item<K> { region: RegionId } where K: Key }

#[derive(Debug, Clone, Copy)]
pub struct Slot {
    pub region: RegionId,
    pub sequence: u32,
}

struct IndicesInner<K>
where
    K: Key,
{
    key: HashMap<K, Item<K>, ItemHashMapKeyAdapter<K>>,
    region: DuplicatedHashMap<RegionId, Item<K>, ItemDuplicatedHashMapRegionAdapter<K>>,
}

unsafe impl<K> Send for IndicesInner<K> where K: Key {}
unsafe impl<K> Sync for IndicesInner<K> where K: Key {}

pub struct Indices<K>
where
    K: Key,
{
    inner: RwLock<IndicesInner<K>>,
}

impl<K> Indices<K>
where
    K: Key,
{
    pub fn new(bits: usize) -> Self {
        let bits = std::cmp::max(bits, HASHMAP_BITS_MAX);
        let inner = IndicesInner {
            key: HashMap::new(bits),
            region: DuplicatedHashMap::new(bits),
        };
        Self {
            inner: RwLock::new(inner),
        }
    }

    pub fn insert(&self, key: K, index: Index) {
        let item = Arc::new(Item::new(key, index));

        let mut inner = self.inner.write();
        if let Some(index) = inner.key.insert(item.clone()) {
            unsafe {
                inner
                    .region
                    .remove_in_place(index.link_duplicated_hashmap_region.raw());
            }
        }
        inner.region.insert(item);
    }

    pub fn lookup(&self, key: &K) -> Option<Index> {
        let inner = self.inner.read();
        inner.key.lookup(key).map(|item| item.index)
    }

    pub fn remove(&self, key: &K) -> Option<Index> {
        let mut inner = self.inner.write();
        match inner.key.remove(key) {
            Some(item) => {
                unsafe {
                    inner
                        .region
                        .remove_in_place(item.link_duplicated_hashmap_region.raw());
                }
                Some(item.index)
            }
            None => None,
        }
    }

    pub fn clear(&self) {
        let mut inner = self.inner.write();
        inner.key.clear();
        inner.region.clear();
    }

    pub fn take_region(&self, region: &RegionId) -> Vec<Index> {
        let mut inner = self.inner.write();
        let items = inner.region.remove(region);
        for item in items.iter() {
            unsafe {
                inner.key.remove_in_place(item.link_hashmap_key.raw());
            }
        }
        items.into_iter().map(|item| item.index).collect_vec()
    }
}
