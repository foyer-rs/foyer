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

struct Inner<K, V, P> {
    record: *const (),
    key: Arc<K>,
    value: Arc<V>,
    hash: u64,
    properties: *const P,
    drop_fn: fn(*const ()),
}

/// A piece of record that is irrelevant to the eviction algorithm.
///
/// With [`Piece`], the disk cache doesn't need to consider the eviction generic type.
pub struct Piece<K, V, P> {
    inner: Arc<Inner<K, V, P>>,
}

impl<K, V, P> Debug for Piece<K, V, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Piece")
            .field("record", &self.inner.record)
            .field("hash", &self.inner.hash)
            .finish()
    }
}

impl<K, V, P> Clone for Piece<K, V, P> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

unsafe impl<K, V, P> Send for Inner<K, V, P>
where
    K: Key,
    V: Value,
    P: Properties,
{
}
unsafe impl<K, V, P> Sync for Inner<K, V, P>
where
    K: Key,
    V: Value,
    P: Properties,
{
}

impl<K, V, P> Drop for Inner<K, V, P> {
    fn drop(&mut self) {
        (self.drop_fn)(self.record);
    }
}

impl<K, V, P> Piece<K, V, P> {
    /// Create a record piece from an record wrapped by [`Arc`].
    pub fn new<E>(record: Arc<Record<E>>) -> Self
    where
        E: Eviction<Key = K, Value = V, Properties = P>,
    {
        let key = record.arc_key().clone();
        let value = record.arc_value().clone();
        let raw = Arc::into_raw(record);
        let record = raw as *const ();
        let hash = unsafe { (*raw).hash() };
        let properties = unsafe { (*raw).properties() } as *const _;
        let drop_fn = |ptr| unsafe {
            let _ = Arc::from_raw(ptr as *const Record<E>);
        };
        let inner = Inner {
            record,
            key,
            value,
            hash,
            properties,
            drop_fn,
        };
        let inner = Arc::new(inner);
        Self { inner }
    }

    /// Get the key of the record.
    pub fn key(&self) -> &K {
        &self.inner.key
    }

    /// Get the value of the record.
    pub fn value(&self) -> &V {
        &self.inner.value
    }

    /// Get the arc key of the record.
    pub fn arc_key(&self) -> &Arc<K> {
        &self.inner.key
    }

    /// Get the arc value of the record.
    pub fn arc_value(&self) -> &Arc<V> {
        &self.inner.value
    }

    /// Get the hash of the record.
    pub fn hash(&self) -> u64 {
        self.inner.hash
    }

    /// Get the properties of the record.
    pub fn properties(&self) -> &P {
        unsafe { &*self.inner.properties }
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
        let r1 = Arc::new(Record::new(Data::<Fifo<Vec<u8>, Vec<u8>, TestProperties>> {
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
