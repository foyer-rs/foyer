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

use std::{fmt::Debug, future::Future, marker::PhantomData, pin::Pin, sync::Arc};

use foyer_common::{
    code::{Key, Value},
    properties::Properties,
};

use crate::{record::Record, Eviction};

/// A piece of record that is irrelevant to the eviction algorithm.
///
/// With [`Piece`], the disk cache doesn't need to consider the eviction generic type.
pub struct Piece<K, V, P> {
    record: *const (),
    key: *const K,
    value: *const V,
    hash: u64,
    properties: *const P,
    drop_fn: fn(*const ()),
}

impl<K, V, P> Debug for Piece<K, V, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Piece")
            .field("record", &self.record)
            .field("hash", &self.hash)
            .finish()
    }
}

unsafe impl<K, V, P> Send for Piece<K, V, P>
where
    K: Key,
    V: Value,
    P: Properties,
{
}
unsafe impl<K, V, P> Sync for Piece<K, V, P>
where
    K: Key,
    V: Value,
    P: Properties,
{
}

impl<K, V, P> Drop for Piece<K, V, P> {
    fn drop(&mut self) {
        (self.drop_fn)(self.record);
    }
}

impl<K, V, P> Clone for Piece<K, V, P> {
    fn clone(&self) -> Self {
        unsafe { Arc::increment_strong_count(self.record) };
        Self {
            record: self.record,
            key: self.key,
            value: self.value,
            hash: self.hash,
            properties: self.properties,
            drop_fn: self.drop_fn,
        }
    }
}

impl<K, V, P> Piece<K, V, P> {
    /// Create a record piece from an record wrapped by [`Arc`].
    pub fn new<E>(record: Arc<Record<E>>) -> Self
    where
        E: Eviction<Key = K, Value = V, Properties = P>,
    {
        let raw = Arc::into_raw(record);
        let record = raw as *const ();
        let key = unsafe { (*raw).key() } as *const _;
        let value = unsafe { (*raw).value() } as *const _;
        let hash = unsafe { (*raw).hash() };
        let properties = unsafe { (*raw).properties() } as *const _;
        let drop_fn = |ptr| unsafe {
            let _ = Arc::from_raw(ptr as *const Record<E>);
        };
        Self {
            record,
            key,
            value,
            hash,
            properties,
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

    /// Get the properties of the record.
    pub fn properties(&self) -> &P {
        unsafe { &*self.properties }
    }

    pub(crate) fn into_record<E>(mut self) -> Arc<Record<E>>
    where
        E: Eviction<Key = K, Value = V, Properties = P>,
    {
        self.drop_fn = |_| {};
        let record = self.record as *const Record<E>;
        let arc_record = unsafe { Arc::from_raw(record) };
        
        // Clear indexer and eviction flags since this record is being reinserted
        // from the pipe (e.g., from disk cache). This prevents race conditions
        // where the record still has flags set from a previous insertion.
        arc_record.set_in_indexer(false);
        arc_record.set_in_eviction(false);
        
        arc_record
    }
}

/// Pipe is used to notify disk cache to cache entries from the in-memory cache.
pub trait Pipe: Send + Sync + 'static + Debug {
    /// Type of the key of the record.
    type Key;
    /// Type of the value of the record.
    type Value;
    /// Type of the properties of the record.
    type Properties;

    /// Decide whether to send evicted entry to pipe.
    fn is_enabled(&self) -> bool;

    /// Send the piece to the disk cache.
    fn send(&self, piece: Piece<Self::Key, Self::Value, Self::Properties>);

    /// Flush all the pieces to the disk cache in a asynchronous manner.
    ///
    /// This function is called when the in-memory cache is flushed.
    /// It is expected to obey the io throttle of the disk cache.
    fn flush(
        &self,
        pieces: Vec<Piece<Self::Key, Self::Value, Self::Properties>>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>;
}

/// An no-op pipe that is never enabled.
pub struct NoopPipe<K, V, P>(PhantomData<(K, V, P)>);

impl<K, V, P> Debug for NoopPipe<K, V, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("NoopPipe").finish()
    }
}

impl<K, V, P> Default for NoopPipe<K, V, P> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<K, V, P> Pipe for NoopPipe<K, V, P>
where
    K: Key,
    V: Value,
    P: Properties,
{
    type Key = K;
    type Value = V;
    type Properties = P;

    fn is_enabled(&self) -> bool {
        false
    }

    fn send(&self, _: Piece<Self::Key, Self::Value, Self::Properties>) {}

    fn flush(
        &self,
        _: Vec<Piece<Self::Key, Self::Value, Self::Properties>>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async {})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        eviction::{fifo::Fifo, test_utils::TestProperties},
        record::Data,
    };

    #[test]
    fn test_piece() {
        let r1 = Arc::new(Record::new(Data::<Fifo<Arc<Vec<u8>>, Arc<Vec<u8>>, TestProperties>> {
            key: Arc::new(vec![b'k'; 4096]),
            value: Arc::new(vec![b'k'; 16384]),
            properties: TestProperties::default(),
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
