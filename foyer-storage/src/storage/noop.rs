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
    future::{ready, Future},
    marker::PhantomData,
    sync::Arc,
};

use foyer_common::code::{StorageKey, StorageValue};
use foyer_memory::Piece;

use crate::{device::monitor::DeviceStats, error::Result, storage::Storage};

pub struct Noop<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    _marker: PhantomData<(K, V)>,
}

impl<K, V> Debug for Noop<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("NoneStore").finish()
    }
}

impl<K, V> Clone for Noop<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn clone(&self) -> Self {
        Self { _marker: PhantomData }
    }
}

impl<K, V> Storage for Noop<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    type Key = K;
    type Value = V;
    type Config = ();

    async fn open(_: Self::Config) -> Result<Self> {
        Ok(Self { _marker: PhantomData })
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }

    fn enqueue(&self, _piece: Piece<Self::Key, Self::Value>, _estimated_size: usize) {}

    fn load(&self, _: u64) -> impl Future<Output = Result<Option<(Self::Key, Self::Value)>>> + Send + 'static {
        ready(Ok(None))
    }

    fn delete(&self, _: u64) {}

    fn may_contains(&self, _: u64) -> bool {
        false
    }

    async fn destroy(&self) -> Result<()> {
        Ok(())
    }

    fn stats(&self) -> Arc<DeviceStats> {
        Arc::default()
    }

    fn wait(&self) -> impl Future<Output = ()> + Send + 'static {
        ready(())
    }
}

#[cfg(test)]
mod tests {
    use foyer_memory::{Cache, CacheBuilder, FifoConfig};

    use super::*;

    fn cache_for_test() -> Cache<u64, Vec<u8>> {
        CacheBuilder::new(10)
            .with_eviction_config(FifoConfig::default())
            .build()
    }

    #[tokio::test]
    async fn test_none_store() {
        let memory = cache_for_test();
        let store: Noop<u64, Vec<u8>> = Noop::open(()).await.unwrap();

        store.enqueue(memory.insert(0, vec![b'x'; 16384]).piece(), 16384);
        store.wait().await;
        assert!(store.load(memory.hash(&0)).await.unwrap().is_none());
        store.delete(memory.hash(&0));
        store.wait().await;
        store.destroy().await.unwrap();
        store.close().await.unwrap();
    }
}
