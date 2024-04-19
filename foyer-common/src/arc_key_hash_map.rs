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

use std::{
    borrow::Borrow,
    fmt::Debug,
    hash::{BuildHasher, Hash},
    ops::{Deref, DerefMut},
    sync::Arc,
};

use ahash::RandomState;
use hashbrown::hash_table::{HashTable, IntoIter as HashTableIntoIter};

pub use hashbrown::hash_table::Entry;

pub struct HashTableEntry<K, V> {
    key: Arc<K>,
    value: V,
    hash: u64,
}

impl<K, V> HashTableEntry<K, V> {
    pub fn key(&self) -> &Arc<K> {
        &self.key
    }

    pub fn value(&self) -> &V {
        &self.value
    }

    pub fn take(self) -> (Arc<K>, V) {
        (self.key, self.value)
    }
}

pub struct ArcKeyHashMap<K, V, S = RandomState> {
    inner: HashTable<HashTableEntry<K, V>>,
    build_hasher: S,
}

impl<K, V> Debug for ArcKeyHashMap<K, V, RandomState> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArcHashMap").finish()
    }
}

impl<K, V> Default for ArcKeyHashMap<K, V, RandomState> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> ArcKeyHashMap<K, V, RandomState> {
    pub fn new() -> Self {
        Self::new_with_hasher(RandomState::default())
    }
}

impl<K, V, S> ArcKeyHashMap<K, V, S> {
    pub fn new_with_hasher(build_hasher: S) -> Self {
        Self {
            inner: HashTable::new(),
            build_hasher,
        }
    }
}

impl<K, V, S> Deref for ArcKeyHashMap<K, V, S> {
    type Target = HashTable<HashTableEntry<K, V>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<K, V, S> DerefMut for ArcKeyHashMap<K, V, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<K, V, S> ArcKeyHashMap<K, V, S>
where
    K: Hash + Eq,
    S: BuildHasher + Send + Sync + 'static,
{
    pub fn insert(&mut self, key: Arc<K>, value: V) -> Option<V> {
        let hash = self.build_hasher.hash_one(&key);
        match self.inner.entry(
            hash,
            |entry| entry.key.as_ref().borrow() == key.as_ref(),
            |entry| entry.hash,
        ) {
            Entry::Occupied(mut o) => {
                let mut e = HashTableEntry { key, value, hash };
                std::mem::swap(o.get_mut(), &mut e);
                Some(e.value)
            }
            Entry::Vacant(v) => {
                v.insert(HashTableEntry { key, value, hash });
                None
            }
        }
    }

    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.build_hasher.hash_one(key);
        self.inner
            .find(hash, |entry| entry.key.as_ref().borrow() == key)
            .map(|entry| &entry.value)
    }

    pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.build_hasher.hash_one(key);
        match self
            .inner
            .entry(hash, |entry| entry.key.as_ref().borrow() == key, |entry| entry.hash)
        {
            Entry::Occupied(o) => {
                let (entry, _v) = o.remove();
                Some(entry.value)
            }
            Entry::Vacant(_) => None,
        }
    }

    pub fn entry(&mut self, key: Arc<K>) -> Entry<'_, HashTableEntry<K, V>> {
        let hash = self.build_hasher.hash_one(&key);
        self.inner.entry(
            hash,
            |entry| entry.key.as_ref().borrow() == key.as_ref(),
            |entry| entry.hash,
        )
    }
}

pub struct IntoIter<K, V> {
    inner: HashTableIntoIter<HashTableEntry<K, V>>,
}

impl<K, V> Iterator for IntoIter<K, V> {
    type Item = (Arc<K>, V);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|entry| (entry.key, entry.value))
    }
}

impl<K, V, S> IntoIterator for ArcKeyHashMap<K, V, S> {
    type Item = (Arc<K>, V);
    type IntoIter = IntoIter<K, V>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter {
            inner: self.inner.into_iter(),
        }
    }
}
