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
//  limitations under the License.use std::marker::PhantomData;

// FIXME: REMOVE ME!!!
#![allow(unused)]

use foyer_common::code::{HashBuilder, StorageKey, StorageValue};
use foyer_memory::CacheEntry;
use futures::Future;

use std::{borrow::Borrow, fmt::Debug, hash::Hash, marker::PhantomData, sync::Arc};

use crate::{device::Device, error::Result, storage::Storage, DeviceStats, EnqueueHandle};

pub struct GenericSmallStorageConfig<K, V, S, D>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
    D: Device,
{
    _marker: PhantomData<(K, V, S, D)>,
}

impl<K, V, S, D> Debug for GenericSmallStorageConfig<K, V, S, D>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
    D: Device,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenericSmallStorageConfig").finish()
    }
}

pub struct GenericSmallStorage<K, V, S, D>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
    D: Device,
{
    _marker: PhantomData<(K, V, S, D)>,
}

impl<K, V, S, D> Debug for GenericSmallStorage<K, V, S, D>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
    D: Device,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenericSmallStorage").finish()
    }
}

impl<K, V, S, D> Clone for GenericSmallStorage<K, V, S, D>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
    D: Device,
{
    fn clone(&self) -> Self {
        Self { _marker: PhantomData }
    }
}

impl<K, V, S, D> Storage for GenericSmallStorage<K, V, S, D>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
    D: Device,
{
    type Key = K;
    type Value = V;
    type BuildHasher = S;
    type Config = GenericSmallStorageConfig<K, V, S, D>;

    async fn open(config: Self::Config) -> Result<Self> {
        todo!()
    }

    async fn close(&self) -> Result<()> {
        todo!()
    }

    fn pick(&self, key: &Self::Key) -> bool {
        todo!()
    }

    fn enqueue(
        &self,
        entry: CacheEntry<Self::Key, Self::Value, Self::BuildHasher>,
        force: bool,
    ) -> crate::EnqueueHandle {
        todo!()
    }

    // FIXME: REMOVE THE CLIPPY IGNORE.
    // TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
    #[allow(clippy::manual_async_fn)]
    fn load<Q>(&self, key: &Q) -> impl Future<Output = Result<Option<(Self::Key, Self::Value)>>> + Send + 'static
    where
        Self::Key: std::borrow::Borrow<Q>,
        Q: std::hash::Hash + Eq + ?Sized + Send + Sync + 'static,
    {
        async { todo!() }
    }

    fn delete<Q>(&self, key: &Q) -> EnqueueHandle
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        todo!()
    }

    fn may_contains<Q>(&self, key: &Q) -> bool
    where
        Self::Key: std::borrow::Borrow<Q>,
        Q: std::hash::Hash + Eq + ?Sized,
    {
        todo!()
    }

    async fn destroy(&self) -> Result<()> {
        todo!()
    }

    fn stats(&self) -> Arc<DeviceStats> {
        todo!()
    }

    async fn wait(&self) -> Result<()> {
        todo!()
    }

    fn runtime(&self) -> &tokio::runtime::Handle {
        todo!()
    }
}
