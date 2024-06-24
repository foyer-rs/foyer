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

use std::{borrow::Borrow, collections::VecDeque, hash::Hash};

use foyer_common::code::{HashBuilder, Key, Value};
use hashbrown::{hash_table::Entry, HashTable};

use crate::{eviction::Eviction, generic::GenericCacheEntry, handle::KeyedHandle, indexer::Indexer};

/// Thread-local cache.
pub struct LocalCache<K, V, E, I, S>
where
    K: Key,
    V: Value,
    E: Eviction,
    E::Handle: KeyedHandle<Key = K, Data = (K, V)>,
    I: Indexer<Key = K, Handle = E::Handle>,
    S: HashBuilder,
{
    map: HashTable<GenericCacheEntry<K, V, E, I, S>>,
    queue: VecDeque<GenericCacheEntry<K, V, E, I, S>>,

    capacity: usize,
    weight: usize,
}

impl<K, V, E, I, S> LocalCache<K, V, E, I, S>
where
    K: Key,
    V: Value,
    E: Eviction,
    E::Handle: KeyedHandle<Key = K, Data = (K, V)>,
    I: Indexer<Key = K, Handle = E::Handle>,
    S: HashBuilder,
{
    pub fn new(capacity: usize) -> Self {
        Self {
            map: HashTable::default(),
            queue: VecDeque::default(),
            capacity,
            weight: 0,
        }
    }

    pub fn insert(&mut self, entry: &GenericCacheEntry<K, V, E, I, S>) {
        match self.map.entry(entry.hash(), |e| e.key() == entry.key(), |e| e.hash()) {
            Entry::Occupied(_) => return,
            Entry::Vacant(v) => {
                v.insert(entry.clone());
            }
        }
        self.weight += entry.weight();
        self.queue.push_back(entry.clone());
        while self.weight > self.capacity {
            let e = self.queue.pop_front().unwrap();
            self.weight -= e.weight();
        }
    }

    pub fn get<Q>(&self, hash: u64, key: &Q) -> Option<GenericCacheEntry<K, V, E, I, S>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.map.find(hash, |e| e.key().borrow() == key).cloned()
    }
}
