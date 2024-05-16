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

use std::{borrow::Borrow, fmt::Debug, future::Future, hash::Hash};

use ahash::RandomState;
use foyer_common::code::{HashBuilder, StorageKey, StorageValue};
use foyer_memory::{Cache, CacheContext, CacheEntry, Fetch};
use foyer_storage::{Storage, Store};

pub type HybridCacheEntry<K, V, S = RandomState> = CacheEntry<K, V, S>;

pub struct HybridCache<K, V, S = RandomState>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    pub(crate) memory: Cache<K, V, S>,
    pub(crate) storage: Store<K, V, S>,
}

impl<K, V, S> Debug for HybridCache<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HybridCache")
            .field("memory", &self.memory)
            .field("storage", &self.storage)
            .finish()
    }
}

impl<K, V, S> Clone for HybridCache<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn clone(&self) -> Self {
        Self {
            memory: self.memory.clone(),
            storage: self.storage.clone(),
        }
    }
}

impl<K, V, S> HybridCache<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    pub fn memory(&self) -> &Cache<K, V, S> {
        &self.memory
    }

    pub fn insert(&self, key: K, value: V) -> HybridCacheEntry<K, V, S> {
        let entry = self.memory.insert(key, value);
        self.storage.enqueue(entry.clone());
        entry
    }

    pub fn insert_with_context(&self, key: K, value: V, context: CacheContext) -> HybridCacheEntry<K, V, S> {
        let entry = self.memory.insert_with_context(key, value, context);
        self.storage.enqueue(entry.clone());
        entry
    }

    pub fn insert_storage(&self, key: K, value: V) -> HybridCacheEntry<K, V, S> {
        let entry = self.memory.deposit(key, value);
        self.storage.enqueue(entry.clone());
        entry
    }

    pub fn insert_storage_with_context(&self, key: K, value: V, context: CacheContext) -> HybridCacheEntry<K, V, S> {
        let entry = self.memory.deposit_with_context(key, value, context);
        self.storage.enqueue(entry.clone());
        entry
    }

    pub async fn get<Q>(&self, key: &Q) -> anyhow::Result<Option<HybridCacheEntry<K, V, S>>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Send + Sync + 'static + Clone,
    {
        if let Some(entry) = self.memory.get(key) {
            return Ok(Some(entry));
        }

        if let Some((k, v)) = self.storage.load(key).await? {
            if k.borrow() != key {
                return Ok(None);
            }
            return Ok(Some(self.memory.insert(k, v)));
        }
        Ok(None)
    }

    pub async fn obtain(&self, key: K) -> anyhow::Result<Option<HybridCacheEntry<K, V, S>>>
    where
        K: Clone,
    {
        self.memory
            .fetch(key.clone(), || {
                let store = self.storage.clone();
                async move {
                    match store.load(&key).await.map_err(anyhow::Error::from) {
                        Err(e) => Err(e),
                        Ok(None) => Ok(None),
                        Ok(Some((k, _))) if key != k => Ok(None),
                        Ok(Some((_, v))) => Ok(Some((v, CacheContext::default()))),
                    }
                }
            })
            .await
    }

    pub fn remove<Q>(&self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Send + Sync + 'static,
    {
        self.memory.remove(key);
        self.storage.delete(key);
    }

    pub fn contains<Q>(&self, key: &Q) -> anyhow::Result<bool>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        Ok(self.memory.contains(key) || self.storage.may_contains(key))
    }

    pub async fn clear(&self) -> anyhow::Result<()> {
        self.memory.clear();
        self.storage.destroy().await?;
        Ok(())
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        self.storage.close().await?;
        Ok(())
    }
}

pub type HybridEntry<K, V, S = RandomState> = Fetch<K, V, anyhow::Error, S>;

impl<K, V, S> HybridCache<K, V, S>
where
    K: StorageKey + Clone,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    pub fn fetch<F, FU>(&self, key: K, f: F) -> HybridEntry<K, V, S>
    where
        F: FnOnce() -> FU,
        FU: Future<Output = anyhow::Result<Option<(V, CacheContext)>>> + Send + 'static,
    {
        let store = self.storage.clone();
        self.memory.fetch(key.clone(), || {
            let future = f();
            async move {
                match store.load(&key).await.map_err(anyhow::Error::from)? {
                    None => {}
                    Some((k, _)) if key != k => {}
                    Some((_, v)) => return Ok(Some((v, CacheContext::default()))),
                }
                future.await.map_err(anyhow::Error::from)
            }
        })
    }
}
