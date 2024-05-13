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
    future::Future,
    hash::{BuildHasher, Hash},
    marker::PhantomData,
};

use foyer_common::code::{StorageKey, StorageValue};
use foyer_memory::CacheEntry;
use tokio::sync::oneshot;

use crate::storage::{EnqueueFuture, Storage};

use crate::error::Result;

pub struct NoneStore<K, V, S>(PhantomData<(K, V, S)>)
where
    K: StorageKey,
    V: StorageValue,
    S: BuildHasher + Send + Sync + 'static + Debug;

impl<K, V, S> Debug for NoneStore<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: BuildHasher + Send + Sync + 'static + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("NoneStore").finish()
    }
}

impl<K, V, S> Clone for NoneStore<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: BuildHasher + Send + Sync + 'static + Debug,
{
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

impl<K, V, S> Storage for NoneStore<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: BuildHasher + Send + Sync + 'static + Debug,
{
    type Key = K;
    type Value = V;
    type BuildHasher = S;
    type Config = ();

    async fn open(_: Self::Config) -> Result<Self> {
        Ok(Self(PhantomData))
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }

    fn enqueue(&self, _: CacheEntry<Self::Key, Self::Value, Self::BuildHasher>) -> EnqueueFuture {
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(Ok(false));
        EnqueueFuture::new(rx)
    }

    // TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
    // False-positive with `'static` lifetime requirement.
    #[allow(clippy::manual_async_fn)]
    fn load<Q>(&self, _: &Q) -> impl Future<Output = Result<Option<(Self::Key, Self::Value)>>> + Send + 'static
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Send + Sync + 'static,
    {
        async move { Ok(None) }
    }

    fn delete<Q>(&self, _: &Q) -> EnqueueFuture
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(Ok(false));
        EnqueueFuture::new(rx)
    }

    fn may_contains<Q>(&self, _: &Q) -> bool
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        false
    }

    async fn destroy(&self) -> Result<()> {
        Ok(())
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
        let store = NoneStore::open(()).await.unwrap();
        assert!(!store.enqueue(memory.insert(0, vec![b'x'; 16384])).await.unwrap());
        assert!(store.load(&0).await.unwrap().is_none());
        store.delete(&0).await.unwrap();
        store.destroy().await.unwrap();
        store.close().await.unwrap();
    }
}
