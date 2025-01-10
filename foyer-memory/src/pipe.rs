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

use std::{fmt::Debug, marker::PhantomData, sync::Arc};

use foyer_common::code::{Key, Value};

use crate::{record::Record, Eviction};

/// A piece of record that is irrelevant to the eviction algorithm.
///
/// With [`Piece`], the disk cache doesn't need to consider the eviction generic type.
pub struct Piece<K, V> {
    record: *const (),
    key: *const K,
    value: *const V,
    hash: u64,
    drop_fn: fn(*const ()),
}

impl<K, V> Debug for Piece<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Piece")
            .field("record", &self.record)
            .field("hash", &self.hash)
            .finish()
    }
}

unsafe impl<K, V> Send for Piece<K, V> {}
unsafe impl<K, V> Sync for Piece<K, V> {}

impl<K, V> Drop for Piece<K, V> {
    fn drop(&mut self) {
        (self.drop_fn)(self.record);
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
        let key = unsafe { (*raw).key() } as *const _;
        let value = unsafe { (*raw).value() } as *const _;
        let hash = unsafe { (*raw).hash() };
        let drop_fn = |ptr| unsafe {
            let _ = Arc::from_raw(ptr as *const Record<E>);
        };
        Self {
            record,
            key,
            value,
            hash,
            drop_fn,
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

    /// Decide whether to send evicted entry to pipe.
    fn is_enabled(&self) -> bool;

    /// Send the piece to the disk cache.
    fn send(&self, piece: Piece<Self::Key, Self::Value>);
}

/// An no-op pipe that is never enabled.
#[derive(Debug)]
pub struct NoopPipe<K, V>(PhantomData<(K, V)>);

impl<K, V> Default for NoopPipe<K, V> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<K, V> Pipe for NoopPipe<K, V>
where
    K: Key,
    V: Value,
{
    type Key = K;
    type Value = V;

    fn is_enabled(&self) -> bool {
        false
    }

    fn send(&self, _: Piece<Self::Key, Self::Value>) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        eviction::fifo::{Fifo, FifoHint},
        record::Data,
    };

    #[test]
    fn test_piece() {
        let r1 = Arc::new(Record::new(Data::<Fifo<Arc<Vec<u8>>, Arc<Vec<u8>>>> {
            key: Arc::new(vec![b'k'; 4096]),
            value: Arc::new(vec![b'k'; 16384]),
            hint: FifoHint,
            hash: 1,
            weight: 1,
        }));

        let p1 = Piece::new(r1.clone());
        let k1 = p1.key().clone();
        let r2 = r1.clone();
        let p2 = Piece::new(r1.clone());

        drop(p1);
        drop(r2);
        drop(p2);
        drop(r1);
        drop(k1);
    }
}
