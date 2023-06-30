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

use std::collections::BTreeMap;

use foyer_common::Key;
use itertools::Itertools;
use parking_lot::RwLock;

use crate::region::{RegionId, Version};

#[derive(Debug, Clone)]
pub struct Index<K>
where
    K: Key,
{
    pub key: K,

    pub region: RegionId,
    pub version: Version,
    pub offset: u32,
    pub len: u32,
    pub key_len: u32,
    pub value_len: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct Slot {
    pub region: RegionId,
    pub sequence: u32,
}

#[derive(Debug)]
struct IndicesInner<K>
where
    K: Key,
{
    slots: BTreeMap<K, Slot>,
    regions: Vec<BTreeMap<u32, Index<K>>>,
}

#[derive(Debug)]
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
    pub fn new(regions: usize) -> Self {
        let inner = IndicesInner {
            slots: BTreeMap::new(),
            regions: vec![BTreeMap::new(); regions],
        };
        Self {
            inner: RwLock::new(inner),
        }
    }

    pub fn insert(&self, index: Index<K>) {
        let region = index.region;
        let key = index.key.clone();

        let mut inner = self.inner.write();
        let sequence = inner.regions[region as usize].len() as u32;
        inner.regions[region as usize].insert(sequence, index);
        inner.slots.insert(key, Slot { region, sequence });
    }

    pub fn lookup(&self, key: &K) -> Option<Index<K>> {
        let inner = self.inner.read();
        let slot = inner.slots.get(key)?;
        inner.regions[slot.region as usize]
            .get(&slot.sequence)
            .cloned()
    }

    pub fn remove(&self, key: &K) -> Option<Index<K>> {
        let mut inner = self.inner.write();
        let slot = inner.slots.remove(key)?;
        inner.regions[slot.region as usize].remove(&slot.sequence)
    }

    pub fn take_region(&self, region: &RegionId) -> Vec<Index<K>> {
        let mut inner = self.inner.write();
        let mut indices = BTreeMap::new();
        std::mem::swap(&mut indices, &mut inner.regions[*region as usize]);

        for index in indices.values() {
            inner.slots.remove(&index.key);
        }

        indices.into_values().collect_vec()
    }
}
