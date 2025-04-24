// Copyright 2025 foyer Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    fmt::Debug,
    time::{Duration, Instant},
};

use ahash::RandomState;
use foyer_common::code::{HashBuilder, StorageKey, StorageValue};
use foyer_storage::Pick;

use crate::{HybridCache, HybridCacheEntry, HybridCachePolicy, HybridCacheProperties};

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

    /// Insert the entry with properties to the hybrid cache.
    pub fn insert_with_properties(self, value: V, properties: HybridCacheProperties) -> HybridCacheEntry<K, V, S> {
        self.hybrid.insert_with_properties(self.key, value, properties)
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
    hash: u64,

    force: bool,
    picked: Option<Pick>,
    pick_duration: Duration,
}

impl<K, V, S> HybridCacheStorageWriter<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    pub(crate) fn new(hybrid: HybridCache<K, V, S>, key: K) -> Self {
        let hash = hybrid.memory().hash(&key);
        Self {
            hybrid,
            key,
            hash,
            force: false,
            picked: None,
            pick_duration: Duration::default(),
        }
    }

    /// Check if the entry can be admitted by the admission picker of the disk cache.
    ///
    /// After calling `pick`, the writer will not be checked by the admission picker again.
    pub fn pick(&mut self) -> Pick {
        let now = Instant::now();

        let picked = self.hybrid.storage().pick(self.hash);
        self.picked = Some(picked);

        self.pick_duration = now.elapsed();

        picked
    }

    fn may_pick(&mut self) -> Pick {
        if let Some(picked) = self.picked {
            picked
        } else {
            self.pick()
        }
    }

    /// Force the disk cache to admit the writer.
    ///
    /// Note: There is still chance that the entry is ignored because of the storage engine buffer full.
    pub fn force(mut self) -> Self {
        self.force = true;
        self
    }

    fn insert_inner(mut self, value: V, properties: HybridCacheProperties) -> Option<HybridCacheEntry<K, V, S>> {
        let now = Instant::now();

        if !self.force && !self.may_pick().admitted() {
            return None;
        }

        let entry = self
            .hybrid
            .memory()
            .insert_with_properties(self.key, value, properties.with_ephemeral(true));
        if self.hybrid.policy() == HybridCachePolicy::WriteOnInsertion {
            self.hybrid.storage().enqueue(entry.piece(), true);
        }
        self.hybrid.metrics().hybrid_insert.increase(1);
        self.hybrid
            .metrics()
            .hybrid_insert_duration
            .record((now.elapsed() + self.pick_duration).as_secs_f64());

        Some(entry)
    }

    /// Insert the entry to the disk cache only.
    pub fn insert(self, value: V) -> Option<HybridCacheEntry<K, V, S>> {
        self.insert_inner(value, HybridCacheProperties::default())
    }

    /// Insert the entry with properties to the disk cache only.
    pub fn insert_with_properties(
        self,
        value: V,
        properties: HybridCacheProperties,
    ) -> Option<HybridCacheEntry<K, V, S>> {
        self.insert_inner(value, properties)
    }
}
