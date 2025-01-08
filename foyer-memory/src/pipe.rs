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

use std::sync::Arc;

use crate::{record::Record, Eviction};

/// A piece of record that is irrelevant to the eviction algorithm.
///
/// With [`Piece`], the disk cache doesn't need to consider the eviction generic type.
pub struct Piece<K, V> {
    record: *const (),
    key: *const K,
    value: *const V,
    hash: u64,
}

impl<K, V> Drop for Piece<K, V> {
    fn drop(&mut self) {
        let _ = unsafe { Arc::from_raw(self.record as *const Self) };
    }
}

impl<K, V> Piece<K, V> {
    /// Create a record piece from an record wrapped by [`Arc`].
    pub fn new<E>(record: Arc<Record<E>>) -> Self
    where
        E: Eviction<Key = K, Value = V>,
    {
        let raw = Arc::into_raw(record);
        let record = raw as *const ();
        let key = unsafe { (&*raw).key() } as *const _;
        let value = unsafe { (&*raw).value() } as *const _;
        let hash = unsafe { (&*raw).hash() };
        Self {
            record,
            key,
            value,
            hash,
        }
    }

    /// Get the key of the record.
    pub fn key(&self) -> &K {
        unsafe { &*self.key }
    }

    /// Get the value of the record.
    pub fn value(&self) -> &V {
        unsafe { &*self.value }
    }

    /// Get the hash of the record.
    pub fn hash(&self) -> u64 {
        self.hash
    }
}

/// Pipe is used to notify disk cache to cache entries from the in-memory cache.
pub trait Pipe: Send + Sync + 'static {
    /// Type of the key of the record.
    type Key;
    /// Type of the value of the record.
    type Value;

    /// Send the piece to the disk cache.
    fn send(&self, piece: Piece<Self::Key, Self::Value>);
}
