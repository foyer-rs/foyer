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

use ahash::RandomState;
use foyer_common::code::{HashBuilder, StorageKey, StorageValue};
use futures::Future;
use std::{
    borrow::Borrow,
    fmt::Debug,
    hash::Hash,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crate::{
    error::Result,
    large::generic::{GenericLargeStorage, GenericLargeStorageConfig},
    small::generic::{GenericSmallStorage, GenericSmallStorageConfig},
    storage::{
        either::{Either, Selection, Selector},
        noop::Noop,
        runtime::{Runtime, RuntimeStoreConfig},
    },
    DeviceStats, DirectFsDevice, EnqueueHandle, Storage,
};

pub struct SizeSelector<K>
where
    K: StorageKey,
{
    threshold: usize,
    _marker: PhantomData<K>,
}

impl<K> Debug for SizeSelector<K>
where
    K: StorageKey,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SizeSelector")
            .field("threshold", &self.threshold)
            .finish()
    }
}

impl<K> Selector for SizeSelector<K>
where
    K: StorageKey,
{
    type Key = K;

    fn select<Q>(&self, #[allow(unused)] key: &Q) -> Selection
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        todo!()
    }
}

enum LoadFuture<F1, F2, F3, F4, F5, F6> {
    Noop(F1),
    LargeDirectFs(F2),
    LargeRuntimeDirectFs(F3),
    SmallDirectFs(F4),
    SmallRuntimeDirectFs(F5),
    CombinedRuntimeDirectFs(F6),
}

impl<F1, F2, F3, F4, F5, F6> LoadFuture<F1, F2, F3, F4, F5, F6> {
    // TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
    #[allow(clippy::type_complexity)]
    pub fn as_pin_mut(
        self: Pin<&mut Self>,
    ) -> LoadFuture<Pin<&mut F1>, Pin<&mut F2>, Pin<&mut F3>, Pin<&mut F4>, Pin<&mut F5>, Pin<&mut F6>> {
        unsafe {
            match *Pin::get_unchecked_mut(self) {
                LoadFuture::Noop(ref mut inner) => LoadFuture::Noop(Pin::new_unchecked(inner)),
                LoadFuture::LargeDirectFs(ref mut inner) => LoadFuture::LargeDirectFs(Pin::new_unchecked(inner)),
                LoadFuture::LargeRuntimeDirectFs(ref mut inner) => {
                    LoadFuture::LargeRuntimeDirectFs(Pin::new_unchecked(inner))
                }
                LoadFuture::SmallDirectFs(ref mut inner) => LoadFuture::SmallDirectFs(Pin::new_unchecked(inner)),
                LoadFuture::SmallRuntimeDirectFs(ref mut inner) => {
                    LoadFuture::SmallRuntimeDirectFs(Pin::new_unchecked(inner))
                }
                LoadFuture::CombinedRuntimeDirectFs(ref mut inner) => {
                    LoadFuture::CombinedRuntimeDirectFs(Pin::new_unchecked(inner))
                }
            }
        }
    }
}

impl<F1, F2, F3, F4, F5, F6> Future for LoadFuture<F1, F2, F3, F4, F5, F6>
where
    F1: Future,
    F2: Future<Output = F1::Output>,
    F3: Future<Output = F1::Output>,
    F4: Future<Output = F1::Output>,
    F5: Future<Output = F1::Output>,
    F6: Future<Output = F1::Output>,
{
    type Output = F1::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.as_pin_mut() {
            LoadFuture::Noop(future) => future.poll(cx),
            LoadFuture::LargeDirectFs(future) => future.poll(cx),
            LoadFuture::LargeRuntimeDirectFs(future) => future.poll(cx),
            LoadFuture::SmallDirectFs(future) => future.poll(cx),
            LoadFuture::SmallRuntimeDirectFs(future) => future.poll(cx),
            LoadFuture::CombinedRuntimeDirectFs(future) => future.poll(cx),
        }
    }
}

// TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
#[allow(clippy::type_complexity)]
pub enum EngineConfig<K, V, S = RandomState>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    Noop,
    LargeDirectFs(GenericLargeStorageConfig<K, V, S, DirectFsDevice>),
    LargeRuntimeDirectFs(RuntimeStoreConfig<GenericLargeStorage<K, V, S, DirectFsDevice>>),
    #[allow(unused)]
    SmallDirectFs(GenericSmallStorageConfig<K, V, S, DirectFsDevice>),
    #[allow(unused)]
    SmallRuntimeDirectFs(RuntimeStoreConfig<GenericSmallStorage<K, V, S, DirectFsDevice>>),
    #[allow(unused)]
    CombinedRuntimeDirectFs(
        RuntimeStoreConfig<
            Either<
                K,
                V,
                S,
                GenericLargeStorage<K, V, S, DirectFsDevice>,
                GenericSmallStorage<K, V, S, DirectFsDevice>,
                SizeSelector<K>,
            >,
        >,
    ),
}

impl<K, V, S> Debug for EngineConfig<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Noop => write!(f, "Noop"),
            Self::LargeDirectFs(config) => f.debug_tuple("LargeDirectFs").field(config).finish(),
            Self::LargeRuntimeDirectFs(config) => f.debug_tuple("LargeRuntimeDirectFs").field(config).finish(),
            Self::SmallDirectFs(config) => f.debug_tuple("SmallDirectFs").field(config).finish(),
            Self::SmallRuntimeDirectFs(config) => f.debug_tuple("SmallRuntimeDirectFs").field(config).finish(),
            Self::CombinedRuntimeDirectFs(config) => f.debug_tuple("CombinedRuntimeDirectFs").field(config).finish(),
        }
    }
}

// TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
#[allow(clippy::type_complexity)]
pub enum Engine<K, V, S = RandomState>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    /// No-op disk cache.
    Noop(Noop<K, V, S>),
    /// Large object disk cache with direct fs device.
    LargeDirectFs(GenericLargeStorage<K, V, S, DirectFsDevice>),
    /// Large object disk cache with direct fs device and a dedicated runtime.
    LargeRuntimeDirectFs(Runtime<GenericLargeStorage<K, V, S, DirectFsDevice>>),
    /// Small object disk cache with direct fs device.
    SmallDirectFs(GenericSmallStorage<K, V, S, DirectFsDevice>),
    /// Small object disk cache with direct fs device and a dedicated runtime.
    SmallRuntimeDirectFs(Runtime<GenericSmallStorage<K, V, S, DirectFsDevice>>),
    /// Combined large and small object disk cache with direct fs device and a dedicated runtime.
    CombinedRuntimeDirectFs(
        Runtime<
            Either<
                K,
                V,
                S,
                GenericLargeStorage<K, V, S, DirectFsDevice>,
                GenericSmallStorage<K, V, S, DirectFsDevice>,
                SizeSelector<K>,
            >,
        >,
    ),
}

impl<K, V, S> Debug for Engine<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Noop(storage) => f.debug_tuple("Noop").field(storage).finish(),
            Self::LargeDirectFs(storage) => f.debug_tuple("LargeDirectFs").field(storage).finish(),
            Self::LargeRuntimeDirectFs(storage) => f.debug_tuple("LargeRuntimeDirectFs").field(storage).finish(),
            Self::SmallDirectFs(storage) => f.debug_tuple("SmallDirectFs").field(storage).finish(),
            Self::SmallRuntimeDirectFs(storage) => f.debug_tuple("SmallRuntimeDirectFs").field(storage).finish(),
            Self::CombinedRuntimeDirectFs(storage) => f.debug_tuple("CombinedRuntimeDirectFs").field(storage).finish(),
        }
    }
}

impl<K, V, S> Clone for Engine<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn clone(&self) -> Self {
        match self {
            Self::Noop(storage) => Self::Noop(storage.clone()),
            Self::LargeDirectFs(storage) => Self::LargeDirectFs(storage.clone()),
            Self::LargeRuntimeDirectFs(storage) => Self::LargeRuntimeDirectFs(storage.clone()),
            Self::SmallDirectFs(storage) => Self::SmallDirectFs(storage.clone()),
            Self::SmallRuntimeDirectFs(storage) => Self::SmallRuntimeDirectFs(storage.clone()),
            Self::CombinedRuntimeDirectFs(storage) => Self::CombinedRuntimeDirectFs(storage.clone()),
        }
    }
}

impl<K, V, S> Storage for Engine<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    type Key = K;
    type Value = V;
    type BuildHasher = S;
    type Config = EngineConfig<K, V, S>;

    async fn open(config: Self::Config) -> Result<Self> {
        match config {
            EngineConfig::Noop => Ok(Self::Noop(Noop::open(()).await?)),
            EngineConfig::LargeDirectFs(config) => Ok(Self::LargeDirectFs(GenericLargeStorage::open(config).await?)),
            EngineConfig::LargeRuntimeDirectFs(config) => Ok(Self::LargeRuntimeDirectFs(Runtime::open(config).await?)),
            EngineConfig::SmallDirectFs(config) => Ok(Self::SmallDirectFs(GenericSmallStorage::open(config).await?)),
            EngineConfig::SmallRuntimeDirectFs(config) => Ok(Self::SmallRuntimeDirectFs(Runtime::open(config).await?)),
            EngineConfig::CombinedRuntimeDirectFs(config) => {
                Ok(Self::CombinedRuntimeDirectFs(Runtime::open(config).await?))
            }
        }
    }

    async fn close(&self) -> Result<()> {
        match self {
            Engine::Noop(storage) => storage.close().await,
            Engine::LargeDirectFs(storage) => storage.close().await,
            Engine::LargeRuntimeDirectFs(storage) => storage.close().await,
            Engine::SmallDirectFs(storage) => storage.close().await,
            Engine::SmallRuntimeDirectFs(storage) => storage.close().await,
            Engine::CombinedRuntimeDirectFs(storage) => storage.close().await,
        }
    }

    fn pick(&self, key: &Self::Key) -> bool {
        match self {
            Engine::Noop(storage) => storage.pick(key),
            Engine::LargeDirectFs(storage) => storage.pick(key),
            Engine::LargeRuntimeDirectFs(storage) => storage.pick(key),
            Engine::SmallDirectFs(storage) => storage.pick(key),
            Engine::SmallRuntimeDirectFs(storage) => storage.pick(key),
            Engine::CombinedRuntimeDirectFs(storage) => storage.pick(key),
        }
    }

    fn enqueue(
        &self,
        entry: foyer_memory::CacheEntry<Self::Key, Self::Value, Self::BuildHasher>,
        force: bool,
    ) -> EnqueueHandle {
        match self {
            Engine::Noop(storage) => storage.enqueue(entry, force),
            Engine::LargeDirectFs(storage) => storage.enqueue(entry, force),
            Engine::LargeRuntimeDirectFs(storage) => storage.enqueue(entry, force),
            Engine::SmallDirectFs(storage) => storage.enqueue(entry, force),
            Engine::SmallRuntimeDirectFs(storage) => storage.enqueue(entry, force),
            Engine::CombinedRuntimeDirectFs(storage) => storage.enqueue(entry, force),
        }
    }

    fn load<Q>(&self, key: &Q) -> impl Future<Output = Result<Option<(Self::Key, Self::Value)>>> + Send + 'static
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Send + Sync + 'static,
    {
        match self {
            Engine::Noop(storage) => LoadFuture::Noop(storage.load(key)),
            Engine::LargeDirectFs(storage) => LoadFuture::LargeDirectFs(storage.load(key)),
            Engine::LargeRuntimeDirectFs(storage) => LoadFuture::LargeRuntimeDirectFs(storage.load(key)),
            Engine::SmallDirectFs(storage) => LoadFuture::SmallDirectFs(storage.load(key)),
            Engine::SmallRuntimeDirectFs(storage) => LoadFuture::SmallRuntimeDirectFs(storage.load(key)),
            Engine::CombinedRuntimeDirectFs(storage) => LoadFuture::CombinedRuntimeDirectFs(storage.load(key)),
        }
    }

    fn delete<Q>(&self, key: &Q) -> crate::EnqueueHandle
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        match self {
            Engine::Noop(storage) => storage.delete(key),
            Engine::LargeDirectFs(storage) => storage.delete(key),
            Engine::LargeRuntimeDirectFs(storage) => storage.delete(key),
            Engine::SmallDirectFs(storage) => storage.delete(key),
            Engine::SmallRuntimeDirectFs(storage) => storage.delete(key),
            Engine::CombinedRuntimeDirectFs(storage) => storage.delete(key),
        }
    }

    fn may_contains<Q>(&self, key: &Q) -> bool
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        match self {
            Engine::Noop(storage) => storage.may_contains(key),
            Engine::LargeDirectFs(storage) => storage.may_contains(key),
            Engine::LargeRuntimeDirectFs(storage) => storage.may_contains(key),
            Engine::SmallDirectFs(storage) => storage.may_contains(key),
            Engine::SmallRuntimeDirectFs(storage) => storage.may_contains(key),
            Engine::CombinedRuntimeDirectFs(storage) => storage.may_contains(key),
        }
    }

    async fn destroy(&self) -> Result<()> {
        match self {
            Engine::Noop(storage) => storage.destroy().await,
            Engine::LargeDirectFs(storage) => storage.destroy().await,
            Engine::LargeRuntimeDirectFs(storage) => storage.destroy().await,
            Engine::SmallDirectFs(storage) => storage.destroy().await,
            Engine::SmallRuntimeDirectFs(storage) => storage.destroy().await,
            Engine::CombinedRuntimeDirectFs(storage) => storage.destroy().await,
        }
    }

    fn stats(&self) -> Arc<DeviceStats> {
        match self {
            Engine::Noop(storage) => storage.stats(),
            Engine::LargeDirectFs(storage) => storage.stats(),
            Engine::LargeRuntimeDirectFs(storage) => storage.stats(),
            Engine::SmallDirectFs(storage) => storage.stats(),
            Engine::SmallRuntimeDirectFs(storage) => storage.stats(),
            Engine::CombinedRuntimeDirectFs(storage) => storage.stats(),
        }
    }

    async fn wait(&self) -> Result<()> {
        match self {
            Engine::Noop(storage) => storage.wait().await,
            Engine::LargeDirectFs(storage) => storage.wait().await,
            Engine::LargeRuntimeDirectFs(storage) => storage.wait().await,
            Engine::SmallDirectFs(storage) => storage.wait().await,
            Engine::SmallRuntimeDirectFs(storage) => storage.wait().await,
            Engine::CombinedRuntimeDirectFs(storage) => storage.wait().await,
        }
    }

    fn runtime(&self) -> &tokio::runtime::Handle {
        match self {
            Engine::Noop(storage) => storage.runtime(),
            Engine::LargeDirectFs(storage) => storage.runtime(),
            Engine::LargeRuntimeDirectFs(storage) => storage.runtime(),
            Engine::SmallDirectFs(storage) => storage.runtime(),
            Engine::SmallRuntimeDirectFs(storage) => storage.runtime(),
            Engine::CombinedRuntimeDirectFs(storage) => storage.runtime(),
        }
    }
}
