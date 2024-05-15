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
    ptr::NonNull,
};

use ahash::RandomState;
use foyer_common::code::{Key, Value};
use itertools::Itertools;

use crate::{
    eviction::{
        fifo::{Fifo, FifoHandle},
        lfu::{Lfu, LfuHandle},
        lru::{Lru, LruHandle},
        s3fifo::{S3Fifo, S3FifoHandle},
        Eviction,
    },
    handle::{Handle, HandleExt, KeyedHandle},
    indexer::{HashTableIndexer, Indexer},
    CacheContext, EvictionConfig,
};

pub struct GenericOrderMap<K, V, E, I, S>
where
    K: Key,
    V: Value,
    E: Eviction,
    E::Handle: KeyedHandle<Key = K, Data = (K, V)>,
    I: Indexer<Key = K, Handle = E::Handle>,
    S: BuildHasher + Send + Sync + 'static,
{
    indexer: I,
    eviction: E,
    hash_builder: S,
}

impl<K, V, E, I, S> GenericOrderMap<K, V, E, I, S>
where
    K: Key,
    V: Value,
    E: Eviction,
    E::Handle: KeyedHandle<Key = K, Data = (K, V)>,
    I: Indexer<Key = K, Handle = E::Handle>,
    S: BuildHasher + Send + Sync + 'static,
{
    fn new(capacity: usize, config: E::Config, hash_builder: S) -> Self {
        let eviction = unsafe { E::new(capacity, &config) };
        Self {
            indexer: I::new(),
            eviction,
            hash_builder,
        }
    }

    fn insert(&mut self, key: K, value: V) {
        self.insert_with_context(key, value, CacheContext::default())
    }

    fn insert_with_context(&mut self, key: K, value: V, context: CacheContext) {
        unsafe {
            let hash = self.hash_builder.hash_one(&key);

            let mut handle = Box::<E::Handle>::default();
            handle.init(hash, (key, value), 1, context.into());
            let ptr = NonNull::new_unchecked(Box::into_raw(handle));
            if let Some(old) = self.indexer.insert(ptr) {
                self.eviction.remove(old);
                debug_assert!(!old.as_ref().base().is_in_indexer());
                debug_assert!(!old.as_ref().base().is_in_eviction());
                let _ = Box::from_raw(old.as_ptr());
            }
            self.eviction.push(ptr);

            debug_assert!(ptr.as_ref().base().is_in_indexer());
            debug_assert!(ptr.as_ref().base().is_in_eviction());
        }
    }

    fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        unsafe {
            let hash = self.hash_builder.hash_one(key);
            self.indexer.get(hash, key).is_some()
        }
    }

    fn get<Q>(&mut self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        unsafe {
            let hash = self.hash_builder.hash_one(key);

            let ptr = self.indexer.get(hash, key)?;

            self.eviction.acquire(ptr);
            self.eviction.release(ptr);

            debug_assert!(ptr.as_ref().base().is_in_indexer());
            debug_assert!(ptr.as_ref().base().is_in_eviction());

            Some(&ptr.as_ref().base().data_unwrap_unchecked().1)
        }
    }

    fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        unsafe {
            let hash = self.hash_builder.hash_one(key);

            let ptr = self.indexer.remove(hash, key)?;
            self.eviction.remove(ptr);

            debug_assert!(!ptr.as_ref().base().is_in_indexer());
            debug_assert!(!ptr.as_ref().base().is_in_eviction());

            let mut handle = Box::from_raw(ptr.as_ptr());
            let ((_, v), _, _) = handle.base_mut().take();

            Some(v)
        }
    }

    fn pop(&mut self) -> Option<(K, V)> {
        unsafe {
            let ptr = self.eviction.pop()?;
            let p = self
                .indexer
                .remove(
                    ptr.as_ref().base().hash(),
                    &ptr.as_ref().base().data_unwrap_unchecked().0,
                )
                .unwrap();

            debug_assert_eq!(p, ptr);
            debug_assert!(!ptr.as_ref().base().is_in_indexer());
            debug_assert!(!ptr.as_ref().base().is_in_eviction());

            let mut handle = Box::from_raw(ptr.as_ptr());
            let ((k, v), _, _) = handle.base_mut().take();

            Some((k, v))
        }
    }

    fn clear(&mut self) {
        unsafe {
            // TODO(MrCroxx): Avoid collecting here?
            let ptrs = self.indexer.drain().collect_vec();
            let eptrs = self.eviction.clear();

            // Assert that the handles in the indexer covers the handles in the eviction container.
            if cfg!(debug_assertions) {
                use std::{collections::HashSet as StdHashSet, hash::RandomState as StdRandomState};
                let ptrs: StdHashSet<_, StdRandomState> = StdHashSet::from_iter(ptrs.iter().copied());
                let eptrs: StdHashSet<_, StdRandomState> = StdHashSet::from_iter(eptrs.iter().copied());
                debug_assert!((&eptrs - &ptrs).is_empty());
            }

            for ptr in ptrs {
                debug_assert!(!ptr.as_ref().base().is_in_indexer());
                debug_assert!(!ptr.as_ref().base().is_in_eviction());
                let _ = Box::from_raw(ptr.as_ptr());
            }
        }
    }

    fn len(&self) -> usize {
        self.eviction.len()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<K, V, E, I, S> Drop for GenericOrderMap<K, V, E, I, S>
where
    K: Key,
    V: Value,
    E: Eviction,
    E::Handle: KeyedHandle<Key = K, Data = (K, V)>,
    I: Indexer<Key = K, Handle = E::Handle>,
    S: BuildHasher + Send + Sync + 'static,
{
    fn drop(&mut self) {
        self.clear();
    }
}

pub type FifoOrderMap<K, V, S> = GenericOrderMap<K, V, Fifo<(K, V)>, HashTableIndexer<K, FifoHandle<(K, V)>>, S>;
pub type S3FifoOrderMap<K, V, S> = GenericOrderMap<K, V, S3Fifo<(K, V)>, HashTableIndexer<K, S3FifoHandle<(K, V)>>, S>;
pub type LruOrderMap<K, V, S> = GenericOrderMap<K, V, Lru<(K, V)>, HashTableIndexer<K, LruHandle<(K, V)>>, S>;
pub type LfuOrderMap<K, V, S> = GenericOrderMap<K, V, Lfu<(K, V)>, HashTableIndexer<K, LfuHandle<(K, V)>>, S>;

pub enum OrderMap<K, V, S = RandomState>
where
    K: Key,
    V: Value,
    S: BuildHasher + Send + Sync + 'static,
{
    Fifo(FifoOrderMap<K, V, S>),
    S3Fifo(S3FifoOrderMap<K, V, S>),
    Lru(LruOrderMap<K, V, S>),
    Lfu(LfuOrderMap<K, V, S>),
}

impl<K, V, S> Debug for OrderMap<K, V, S>
where
    K: Key,
    V: Value,
    S: BuildHasher + Send + Sync + 'static + Default,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fifo(_) => f.debug_tuple("Fifo").finish(),
            Self::S3Fifo(_) => f.debug_tuple("S3Fifo").finish(),
            Self::Lru(_) => f.debug_tuple("Lru").finish(),
            Self::Lfu(_) => f.debug_tuple("Lfu").finish(),
        }
    }
}

impl<K, V, S> OrderMap<K, V, S>
where
    K: Key,
    V: Value,
    S: BuildHasher + Send + Sync + 'static + Default,
{
    pub fn new(capacity: usize, config: impl Into<EvictionConfig>) -> Self {
        Self::with_hash_builder(capacity, config, S::default())
    }
}

impl<K, V, S> OrderMap<K, V, S>
where
    K: Key,
    V: Value,
    S: BuildHasher + Send + Sync + 'static,
{
    pub fn with_hash_builder(capacity: usize, config: impl Into<EvictionConfig>, hash_builder: S) -> Self {
        match config.into() {
            EvictionConfig::Fifo(config) => Self::Fifo(GenericOrderMap::new(capacity, config, hash_builder)),
            EvictionConfig::S3Fifo(config) => Self::S3Fifo(GenericOrderMap::new(capacity, config, hash_builder)),
            EvictionConfig::Lru(config) => Self::Lru(GenericOrderMap::new(capacity, config, hash_builder)),
            EvictionConfig::Lfu(config) => Self::Lfu(GenericOrderMap::new(capacity, config, hash_builder)),
        }
    }

    pub fn insert(&mut self, key: K, value: V) {
        match self {
            OrderMap::Fifo(om) => om.insert(key, value),
            OrderMap::S3Fifo(om) => om.insert(key, value),
            OrderMap::Lru(om) => om.insert(key, value),
            OrderMap::Lfu(om) => om.insert(key, value),
        }
    }

    pub fn insert_with_context(&mut self, key: K, value: V, context: CacheContext) {
        match self {
            OrderMap::Fifo(om) => om.insert_with_context(key, value, context),
            OrderMap::S3Fifo(om) => om.insert_with_context(key, value, context),
            OrderMap::Lru(om) => om.insert_with_context(key, value, context),
            OrderMap::Lfu(om) => om.insert_with_context(key, value, context),
        }
    }

    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        match self {
            OrderMap::Fifo(om) => om.contains(key),
            OrderMap::S3Fifo(om) => om.contains(key),
            OrderMap::Lru(om) => om.contains(key),
            OrderMap::Lfu(om) => om.contains(key),
        }
    }

    pub fn get<Q>(&mut self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        match self {
            OrderMap::Fifo(om) => om.get(key),
            OrderMap::S3Fifo(om) => om.get(key),
            OrderMap::Lru(om) => om.get(key),
            OrderMap::Lfu(om) => om.get(key),
        }
    }

    pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        match self {
            OrderMap::Fifo(om) => om.remove(key),
            OrderMap::S3Fifo(om) => om.remove(key),
            OrderMap::Lru(om) => om.remove(key),
            OrderMap::Lfu(om) => om.remove(key),
        }
    }

    pub fn pop(&mut self) -> Option<(K, V)> {
        match self {
            OrderMap::Fifo(om) => om.pop(),
            OrderMap::S3Fifo(om) => om.pop(),
            OrderMap::Lru(om) => om.pop(),
            OrderMap::Lfu(om) => om.pop(),
        }
    }

    pub fn clear(&mut self) {
        match self {
            OrderMap::Fifo(om) => om.clear(),
            OrderMap::S3Fifo(om) => om.clear(),
            OrderMap::Lru(om) => om.clear(),
            OrderMap::Lfu(om) => om.clear(),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            OrderMap::Fifo(om) => om.len(),
            OrderMap::S3Fifo(om) => om.len(),
            OrderMap::Lru(om) => om.len(),
            OrderMap::Lfu(om) => om.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            OrderMap::Fifo(om) => om.is_empty(),
            OrderMap::S3Fifo(om) => om.is_empty(),
            OrderMap::Lru(om) => om.is_empty(),
            OrderMap::Lfu(om) => om.is_empty(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{FifoConfig, LfuConfig, LruConfig, S3FifoConfig};

    use super::*;

    fn case(mut om: OrderMap<u64, u64>) {
        om.insert(1, 1);
        assert_eq!(om.get(&1), Some(&1));
        assert_eq!(om.len(), 1);

        om.insert(1, 1);
        assert_eq!(om.get(&1), Some(&1));
        assert_eq!(om.len(), 1);

        om.insert(1, 2);
        assert_eq!(om.get(&1), Some(&2));
        assert_eq!(om.len(), 1);

        om.insert(3, 3);
        assert_eq!(om.get(&3), Some(&3));
        assert_eq!(om.len(), 2);

        let res = om.remove(&1);
        assert_eq!(res, Some(2));
        assert_eq!(om.len(), 1);

        let res = om.pop();
        assert_eq!(res, Some((3, 3)));
        assert!(om.is_empty());

        om.insert(1, 1);
        assert_eq!(om.get(&1), Some(&1));
        assert_eq!(om.len(), 1);

        om.clear();
        assert!(om.is_empty());
        assert!(!om.contains(&1));
    }

    #[test]
    fn test_fifo_order_map() {
        let om = OrderMap::new(10, FifoConfig::default());
        case(om);
    }

    #[test]
    fn test_s3fifo_order_map() {
        let om = OrderMap::new(10, S3FifoConfig::default());
        case(om);
    }

    #[test]
    fn test_lru_order_map() {
        let om = OrderMap::new(10, LruConfig::default());
        case(om);
    }

    #[test]
    fn test_lfu_order_map() {
        let om = OrderMap::new(10, LfuConfig::default());
        case(om);
    }
}
