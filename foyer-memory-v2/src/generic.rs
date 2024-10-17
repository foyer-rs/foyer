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

use std::{
    collections::{HashMap, LinkedList},
    ptr::NonNull,
    sync::{atomic::Ordering, Arc},
};

use foyer_common::{
    code::{HashBuilder, Key, Value},
    scope::Scope,
    strict_assert,
};
use parking_lot::Mutex;
use slab::Slab;
use tokio::sync::oneshot;

use crate::{
    eviction::{Eviction, State},
    indexer::Indexer,
    record::Record,
    sync::Lock,
};

pub trait Weighter<K, V>: Fn(&K, &V) -> usize + Send + Sync + 'static {}
impl<K, V, T> Weighter<K, V> for T where T: Fn(&K, &V) -> usize + Send + Sync + 'static {}

struct GenericCacheShard<K, V, S, I, E, EH, ES> {
    slab: Slab<Record<K, V, EH, ES>>,

    eviction: E,
    indexer: I,

    capacity: usize,
    usage: usize,

    // TODO(MrCroxx): further sharding this mutex?
    waiters: Mutex<HashMap<K, Vec<oneshot::Sender<GenericCacheEntry<K, V, S, I, E, EH, ES>>>>>,
}

struct GenericCacheInner<K, V, S, I, E, EH, ES> {
    shards: Vec<Lock<GenericCacheShard<K, V, S, I, E, EH, ES>>>,
    weighter: Box<dyn Weighter<K, V>>,
    hasher: S,
}

pub struct GenericCache<K, V, S, I, E, EH, ES> {
    inner: Arc<GenericCacheInner<K, V, S, I, E, EH, ES>>,
}

impl<K, V, S, I, E, EH, ES> Clone for GenericCache<K, V, S, I, E, EH, ES> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<K, V, S, I, E, EH, ES> GenericCacheShard<K, V, S, I, E, EH, ES>
where
    E: Eviction<K, V, Hint = EH, State = ES>,
    ES: State,
    I: Indexer<K, V, EH, ES>,
    K: Key,
    V: Value,
    S: HashBuilder,
{
    fn evict(&mut self, weight: usize, to_release: &mut LinkedList<(K, V, EH, usize)>) {
        while self.usage + weight > self.capacity {
            let token = match self.eviction.pop() {
                Some(token) => token,
                None => break,
            };
            // FIXME: update memory evict metrics
            let release = self.slab[token].refs().load(Ordering::SeqCst) == 0;
            if release {
                // TODO(MrCroxx): try release handle
            }
        }
    }

    fn release(&mut self, token: usize, reinsert: bool) -> (K, V, EH, usize) {
        let record = &self.slab[token];

        assert_eq!(record.refs().load(Ordering::SeqCst), 0);

        // If the entry is deposit (emplace by deposit & never read), remove it from indexer to skip reinsertion.

        if self.slab[token].is_in_indexer(Ordering::SeqCst) && self.slab[token].is_deposit(Ordering::SeqCst) {
            // self.indexer
            //     .remove(&mut self.slab, self.slab[token].hash(), self.slab[token].key());
        }

        // self.slab.with_ref(|slab| {
        //     let record = slab.get(token.to_raw()).unwrap();

        //     assert_eq!(record.refs().load(Ordering::SeqCst), 0);

        //     // If the entry is deposit (emplace by deposit & never read), remove it from indexer to skip reinsertion.
        //     if record.is_in_indexer(Ordering::SeqCst) && record.is_deposit(Ordering::SeqCst) {
        //         strict_assert!(!record.is_in_eviction(Ordering::SeqCst));
        //         self.indexer.remove(slab, record.hash(), record.key());
        //     }
        // });

        todo!()
    }
}

impl<K, V, S, I, E, EH, ES> GenericCache<K, V, S, I, E, EH, ES>
where
    E: Eviction<K, V, Hint = EH, State = ES>,
    ES: State,
    K: Key,
    V: Value,
    S: HashBuilder,
{
    fn emplace(&self, key: K, value: V, hint: EH, deposit: bool) -> GenericCacheEntry<K, V, S, I, E, EH, ES> {
        let hash = self.inner.hasher.hash_one(&key);
        let weight = (self.inner.weighter)(&key, &value);

        todo!()
    }

    fn shard(&self, hash: u64) -> usize {
        hash as usize % self.inner.shards.len()
    }
}

pub struct GenericCacheEntry<K, V, S, I, E, EH, ES> {
    cache: GenericCache<K, V, S, I, E, EH, ES>,
    ptr: NonNull<Record<K, V, EH, ES>>,
}

impl<K, V, S, I, E, EH, ES> Clone for GenericCacheEntry<K, V, S, I, E, EH, ES> {
    fn clone(&self) -> Self {
        self.record().refs().fetch_add(1, Ordering::SeqCst);
        Self {
            cache: self.cache.clone(),
            ptr: self.ptr,
        }
    }
}

impl<K, V, S, I, E, EH, ES> Drop for GenericCacheEntry<K, V, S, I, E, EH, ES> {
    fn drop(&mut self) {
        if self.record().refs().fetch_sub(1, Ordering::SeqCst) == 0 {
            // TODO(MrCroxx) : Get the write lock of the shard and release the memory.
            todo!()
        }
    }
}

impl<K, V, S, I, E, EH, ES> GenericCacheEntry<K, V, S, I, E, EH, ES> {
    pub fn hash(&self) -> u64 {
        self.record().hash()
    }

    pub fn key(&self) -> &K {
        self.record().key()
    }

    pub fn value(&self) -> &V {
        self.record().value()
    }

    pub fn hint(&self) -> &EH {
        self.record().hint()
    }

    pub fn weight(&self) -> usize {
        self.record().weight()
    }

    fn record(&self) -> &Record<K, V, EH, ES> {
        unsafe { self.ptr.as_ref() }
    }

    // pub fn refs(&self) -> usize {
    //     unsafe { self.ptr.as_ref().base().refs() }
    // }

    // pub fn is_outdated(&self) -> bool {
    //     unsafe { !self.ptr.as_ref().base().is_in_indexer() }
    // }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test() {
        println!("{}", std::mem::size_of::<u64>());
        println!("{}", std::mem::size_of::<Option<u64>>());
    }
}
