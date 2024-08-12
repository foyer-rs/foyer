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

use foyer_common::code::{HashBuilder, StorageKey, StorageValue};
use foyer_memory::CacheEntry;
use futures::Future;

use std::{fmt::Debug, marker::PhantomData, sync::Arc};

use crate::{error::Result, serde::KvInfo, storage::Storage, DeviceStats, IoBytes};

pub struct GenericSmallStorageConfig<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    pub placeholder: PhantomData<(K, V, S)>,
}

impl<K, V, S> Debug for GenericSmallStorageConfig<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenericSmallStorageConfig").finish()
    }
}

pub struct GenericSmallStorage<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    _marker: PhantomData<(K, V, S)>,
}

impl<K, V, S> Debug for GenericSmallStorage<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenericSmallStorage").finish()
    }
}

impl<K, V, S> Clone for GenericSmallStorage<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn clone(&self) -> Self {
        Self { _marker: PhantomData }
    }
}

impl<K, V, S> Storage for GenericSmallStorage<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    type Key = K;
    type Value = V;
    type BuildHasher = S;
    type Config = GenericSmallStorageConfig<K, V, S>;

    async fn open(_config: Self::Config) -> Result<Self> {
        todo!()
    }

    async fn close(&self) -> Result<()> {
        todo!()
    }

    fn enqueue(&self, _entry: CacheEntry<Self::Key, Self::Value, Self::BuildHasher>, _buffer: IoBytes, _info: KvInfo) {
        todo!()
    }

    // FIXME: REMOVE THE CLIPPY IGNORE.
    // TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
    #[allow(clippy::manual_async_fn)]
    fn load(&self, _hash: u64) -> impl Future<Output = Result<Option<(Self::Key, Self::Value)>>> + Send + 'static {
        async { todo!() }
    }

    fn delete(&self, _hash: u64) {}

    fn may_contains(&self, _hash: u64) -> bool {
        todo!()
    }

    async fn destroy(&self) -> Result<()> {
        todo!()
    }

    fn stats(&self) -> Arc<DeviceStats> {
        todo!()
    }

    // TODO(MrCroxx): Remove the attr after impl.
    // TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
    #[allow(clippy::manual_async_fn)]
    fn wait(&self) -> impl Future<Output = ()> + Send + 'static {
        async { todo!() }
    }

    fn runtime(&self) -> &tokio::runtime::Handle {
        todo!()
    }
}
