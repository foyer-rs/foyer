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

use crate::arcable::Arcable;

pub struct ArcKeyHashMapEntry<K, V> {
    key: Arc<K>,
    value: V,
    hash: u64,
}

impl<K, V> ArcKeyHashMapEntry<K, V> {
    pub fn new<AK>(hash: u64, key: AK, value: V) -> Self
    where
        AK: Into<Arcable<K>>,
    {
        Self {
            key: key.into().into_arc(),
            value,
            hash,
        }
    }

    pub fn key(&self) -> &Arc<K> {
        &self.key
    }

    pub fn value(&self) -> &V {
        &self.value
    }

    pub fn key_mut(&mut self) -> &mut Arc<K> {
        &mut self.key
    }

    pub fn value_mut(&mut self) -> &mut V {
        &mut self.value
    }

    pub fn take(self) -> (Arc<K>, V) {
        (self.key, self.value)
    }
}

pub struct RawArcKeyHashMap<K, V> {
    inner: HashTable<ArcKeyHashMapEntry<K, V>>,
}

impl<K, V> Debug for RawArcKeyHashMap<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawArcHashMap").finish()
    }
}

impl<K, V> Default for RawArcKeyHashMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> RawArcKeyHashMap<K, V> {
    pub fn new() -> Self {
        Self {
            inner: HashTable::new(),
        }
    }
}

impl<K, V> Deref for RawArcKeyHashMap<K, V> {
    type Target = HashTable<ArcKeyHashMapEntry<K, V>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<K, V> DerefMut for RawArcKeyHashMap<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<K, V> RawArcKeyHashMap<K, V>
where
    K: Hash + Eq,
{
    pub fn insert_with_hash<AK>(&mut self, hash: u64, key: AK, value: V) -> Option<V>
    where
        AK: Into<Arcable<K>>,
    {
        let key = key.into().into_arc();

        match self.inner.entry(
            hash,
            |entry| entry.key.as_ref().borrow() == key.as_ref(),
            |entry| entry.hash,
        ) {
            Entry::Occupied(mut o) => {
                let mut e = ArcKeyHashMapEntry { key, value, hash };
                std::mem::swap(o.get_mut(), &mut e);
                Some(e.value)
            }
            Entry::Vacant(v) => {
                v.insert(ArcKeyHashMapEntry { key, value, hash });
                None
            }
        }
    }

    pub fn get_with_hash<Q>(&self, hash: u64, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner
            .find(hash, |entry| entry.key.as_ref().borrow() == key)
            .map(|entry| &entry.value)
    }

    pub fn remove_with_hash<Q>(&mut self, hash: u64, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
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

    pub fn entry_with_hash<AK>(&mut self, hash: u64, key: AK) -> Entry<'_, ArcKeyHashMapEntry<K, V>>
    where
        AK: Into<Arcable<K>>,
    {
        let key = key.into().into_arc();

        self.inner.entry(
            hash,
            |entry| entry.key.as_ref().borrow() == key.as_ref(),
            |entry| entry.hash,
        )
    }

    pub fn drain(&mut self) -> impl Iterator<Item = (Arc<K>, V)> + '_ {
        self.inner.drain().map(|entry| (entry.key, entry.value))
    }
}

pub struct RawIntoIter<K, V> {
    inner: HashTableIntoIter<ArcKeyHashMapEntry<K, V>>,
}

impl<K, V> Iterator for RawIntoIter<K, V> {
    type Item = (Arc<K>, V);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|entry| (entry.key, entry.value))
    }
}

impl<K, V> IntoIterator for RawArcKeyHashMap<K, V> {
    type Item = (Arc<K>, V);
    type IntoIter = RawIntoIter<K, V>;

    fn into_iter(self) -> Self::IntoIter {
        RawIntoIter {
            inner: self.inner.into_iter(),
        }
    }
}

pub struct ArcKeyHashMap<K, V, S = RandomState> {
    raw: RawArcKeyHashMap<K, V>,
    build_hasher: S,
}

impl<K, V, S> Debug for ArcKeyHashMap<K, V, S> {
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
            raw: RawArcKeyHashMap::new(),
            build_hasher,
        }
    }
}

impl<K, V, S> Deref for ArcKeyHashMap<K, V, S> {
    type Target = HashTable<ArcKeyHashMapEntry<K, V>>;

    fn deref(&self) -> &Self::Target {
        &self.raw
    }
}

impl<K, V, S> DerefMut for ArcKeyHashMap<K, V, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.raw
    }
}

impl<K, V, S> ArcKeyHashMap<K, V, S>
where
    K: Hash + Eq,
    S: BuildHasher + Send + Sync + 'static,
{
    pub fn insert<AK>(&mut self, key: AK, value: V) -> Option<V>
    where
        AK: Into<Arcable<K>>,
    {
        let key = key.into().into_arc();
        let hash = self.build_hasher.hash_one(key.as_ref());
        self.raw.insert_with_hash(hash, key, value)
    }

    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.build_hasher.hash_one(key);
        self.raw.get_with_hash(hash, key)
    }

    pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.build_hasher.hash_one(key);
        self.raw.remove_with_hash(hash, key)
    }

    pub fn entry<AK>(&mut self, key: AK) -> Entry<'_, ArcKeyHashMapEntry<K, V>>
    where
        AK: Into<Arcable<K>>,
    {
        let key = key.into().into_arc();
        let hash = self.build_hasher.hash_one(key.as_ref());
        self.raw.entry_with_hash(hash, key)
    }

    pub fn drain(&mut self) -> impl Iterator<Item = (Arc<K>, V)> + '_ {
        self.raw.drain()
    }
}

pub struct IntoIter<K, V> {
    inner: RawIntoIter<K, V>,
}

impl<K, V> Iterator for IntoIter<K, V> {
    type Item = (Arc<K>, V);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl<K, V, S> IntoIterator for ArcKeyHashMap<K, V, S> {
    type Item = (Arc<K>, V);
    type IntoIter = IntoIter<K, V>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter {
            inner: self.raw.into_iter(),
        }
    }
}
