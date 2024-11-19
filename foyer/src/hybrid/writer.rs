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
    fmt::Debug,
    time::{Duration, Instant},
};

use ahash::RandomState;
use foyer_common::code::{HashBuilder, StorageKey, StorageValue};
use foyer_memory::CacheHint;

use crate::{HybridCache, HybridCacheEntry};

/// Writer for hybrid cache to support more flexible write APIs.
pub struct HybridCacheWriter<K, V, S = RandomState>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    hybrid: HybridCache<K, V, S>,
    key: K,
}

impl<K, V, S> HybridCacheWriter<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    pub(crate) fn new(hybrid: HybridCache<K, V, S>, key: K) -> Self {
        Self { hybrid, key }
    }

    /// Insert the entry to the hybrid cache.
    pub fn insert(self, value: V) -> HybridCacheEntry<K, V, S> {
        self.hybrid.insert(self.key, value)
    }

    /// Insert the entry with hint to the hybrid cache.
    pub fn insert_with_hint(self, value: V, hint: CacheHint) -> HybridCacheEntry<K, V, S> {
        self.hybrid.insert_with_hint(self.key, value, hint)
    }

    /// Convert [`HybridCacheWriter`] to [`HybridCacheStorageWriter`].
    pub fn storage(self) -> HybridCacheStorageWriter<K, V, S> {
        HybridCacheStorageWriter::new(self.hybrid, self.key)
    }
}

/// Writer for disk cache of a hybrid cache to support more flexible write APIs.
pub struct HybridCacheStorageWriter<K, V, S = RandomState>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    hybrid: HybridCache<K, V, S>,
    key: K,

    picked: Option<bool>,
    pick_duration: Duration,
}

impl<K, V, S> HybridCacheStorageWriter<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    pub(crate) fn new(hybrid: HybridCache<K, V, S>, key: K) -> Self {
        Self {
            hybrid,
            key,
            picked: None,
            pick_duration: Duration::default(),
        }
    }

    /// Check if the entry can be admitted by the admission picker of the disk cache.
    ///
    /// `pick` is idempotent (unless `force` is called). Which means the disk cache only check its admission at most
    /// once.
    pub fn pick(&mut self) -> bool {
        if let Some(picked) = self.picked {
            return picked;
        }
        let now = Instant::now();

        let picked = self.hybrid.storage().pick(&self.key);
        self.picked = Some(picked);

        self.pick_duration = now.elapsed();

        picked
    }

    /// Force the disk cache to admit the writer.
    pub fn force(mut self) -> Self {
        self.picked = Some(true);
        self
    }

    fn insert_inner(mut self, value: V, hint: Option<CacheHint>) -> Option<HybridCacheEntry<K, V, S>> {
        let now = Instant::now();

        if !self.pick() {
            return None;
        }

        let entry = match hint {
            Some(hint) => self.hybrid.memory().insert_ephemeral_with_hint(self.key, value, hint),
            None => self.hybrid.memory().insert_ephemeral(self.key, value),
        };
        self.hybrid.storage().enqueue(entry.clone(), true);

        self.hybrid.metrics().hybrid_insert.increment(1);
        self.hybrid
            .metrics()
            .hybrid_insert_duration
            .record(now.elapsed() + self.pick_duration);

        Some(entry)
    }

    /// Insert the entry to the disk cache only.
    pub fn insert(self, value: V) -> Option<HybridCacheEntry<K, V, S>> {
        self.insert_inner(value, None)
    }

    /// Insert the entry with context to the disk cache only.
    pub fn insert_with_context(self, value: V, context: CacheHint) -> Option<HybridCacheEntry<K, V, S>> {
        self.insert_inner(value, Some(context))
    }
}
