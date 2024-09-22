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
    fmt::Debug,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use ahash::RandomState;
use foyer_common::code::{HashBuilder, StorageKey, StorageValue};
use foyer_memory::CacheEntry;
use futures::Future;

use crate::{
    error::Result,
    large::generic::{GenericLargeStorage, GenericLargeStorageConfig},
    serde::KvInfo,
    small::generic::{GenericSmallStorage, GenericSmallStorageConfig},
    storage::{
        either::{Either, EitherConfig, Selection, Selector},
        noop::Noop,
    },
    DeviceStats, IoBytes, Storage,
};

pub struct SizeSelector<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    threshold: usize,
    _marker: PhantomData<(K, V, S)>,
}

impl<K, V, S> Debug for SizeSelector<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SizeSelector")
            .field("threshold", &self.threshold)
            .finish()
    }
}

impl<K, V, S> SizeSelector<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    pub fn new(threshold: usize) -> Self {
        Self {
            threshold,
            _marker: PhantomData,
        }
    }
}

impl<K, V, S> Selector for SizeSelector<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    type Key = K;
    type Value = V;
    type BuildHasher = S;

    fn select(&self, _entry: &CacheEntry<Self::Key, Self::Value, Self::BuildHasher>, buffer: &IoBytes) -> Selection {
        if buffer.len() < self.threshold {
            Selection::Left
        } else {
            Selection::Right
        }
    }
}

enum StoreFuture<F1, F2, F3, F4> {
    Noop(F1),
    Large(F2),
    Small(F3),
    Mixed(F4),
}

impl<F1, F2, F3, F4> StoreFuture<F1, F2, F3, F4> {
    pub fn as_pin_mut(self: Pin<&mut Self>) -> StoreFuture<Pin<&mut F1>, Pin<&mut F2>, Pin<&mut F3>, Pin<&mut F4>> {
        unsafe {
            match *Pin::get_unchecked_mut(self) {
                StoreFuture::Noop(ref mut inner) => StoreFuture::Noop(Pin::new_unchecked(inner)),
                StoreFuture::Large(ref mut inner) => StoreFuture::Large(Pin::new_unchecked(inner)),
                StoreFuture::Small(ref mut inner) => StoreFuture::Small(Pin::new_unchecked(inner)),
                StoreFuture::Mixed(ref mut inner) => StoreFuture::Mixed(Pin::new_unchecked(inner)),
            }
        }
    }
}

impl<F1, F2, F3, F4> Future for StoreFuture<F1, F2, F3, F4>
where
    F1: Future,
    F2: Future<Output = F1::Output>,
    F3: Future<Output = F1::Output>,
    F4: Future<Output = F1::Output>,
{
    type Output = F1::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.as_pin_mut() {
            StoreFuture::Noop(future) => future.poll(cx),
            StoreFuture::Large(future) => future.poll(cx),
            StoreFuture::Small(future) => future.poll(cx),
            StoreFuture::Mixed(future) => future.poll(cx),
        }
    }
}

#[expect(clippy::type_complexity)]
pub enum EngineConfig<K, V, S = RandomState>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    Noop,
    Large(GenericLargeStorageConfig<K, V, S>),
    Small(GenericSmallStorageConfig<K, V, S>),
    Mixed(EitherConfig<K, V, S, GenericSmallStorage<K, V, S>, GenericLargeStorage<K, V, S>, SizeSelector<K, V, S>>),
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
            Self::Large(config) => f.debug_tuple("Large").field(config).finish(),
            Self::Small(config) => f.debug_tuple("Small").field(config).finish(),
            Self::Mixed(config) => f.debug_tuple("Mixed").field(config).finish(),
        }
    }
}

#[expect(clippy::type_complexity)]
pub enum EngineEnum<K, V, S = RandomState>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    /// No-op disk cache.
    Noop(Noop<K, V, S>),
    /// Large object disk cache.
    Large(GenericLargeStorage<K, V, S>),
    /// Small object disk cache.
    Small(GenericSmallStorage<K, V, S>),
    /// Mixed large and small object disk cache.
    Mixed(Either<K, V, S, GenericSmallStorage<K, V, S>, GenericLargeStorage<K, V, S>, SizeSelector<K, V, S>>),
}

impl<K, V, S> Debug for EngineEnum<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Noop(storage) => f.debug_tuple("Noop").field(storage).finish(),
            Self::Large(storage) => f.debug_tuple("Large").field(storage).finish(),
            Self::Small(storage) => f.debug_tuple("Small").field(storage).finish(),
            Self::Mixed(storage) => f.debug_tuple("Mixed").field(storage).finish(),
        }
    }
}

impl<K, V, S> Clone for EngineEnum<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn clone(&self) -> Self {
        match self {
            Self::Noop(storage) => Self::Noop(storage.clone()),
            Self::Large(storage) => Self::Large(storage.clone()),
            Self::Small(storage) => Self::Small(storage.clone()),
            Self::Mixed(storage) => Self::Mixed(storage.clone()),
        }
    }
}

impl<K, V, S> Storage for EngineEnum<K, V, S>
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
            EngineConfig::Large(config) => Ok(Self::Large(GenericLargeStorage::open(config).await?)),
            EngineConfig::Small(config) => Ok(Self::Small(GenericSmallStorage::open(config).await?)),
            EngineConfig::Mixed(config) => Ok(Self::Mixed(Either::open(config).await?)),
        }
    }

    async fn close(&self) -> Result<()> {
        match self {
            EngineEnum::Noop(storage) => storage.close().await,
            EngineEnum::Large(storage) => storage.close().await,
            EngineEnum::Small(storage) => storage.close().await,
            EngineEnum::Mixed(storage) => storage.close().await,
        }
    }

    fn enqueue(&self, entry: CacheEntry<Self::Key, Self::Value, Self::BuildHasher>, buffer: IoBytes, info: KvInfo) {
        match self {
            EngineEnum::Noop(storage) => storage.enqueue(entry, buffer, info),
            EngineEnum::Large(storage) => storage.enqueue(entry, buffer, info),
            EngineEnum::Small(storage) => storage.enqueue(entry, buffer, info),
            EngineEnum::Mixed(storage) => storage.enqueue(entry, buffer, info),
        }
    }

    fn load(&self, hash: u64) -> impl Future<Output = Result<Option<(Self::Key, Self::Value)>>> + Send + 'static {
        match self {
            EngineEnum::Noop(storage) => StoreFuture::Noop(storage.load(hash)),
            EngineEnum::Large(storage) => StoreFuture::Large(storage.load(hash)),
            EngineEnum::Small(storage) => StoreFuture::Small(storage.load(hash)),
            EngineEnum::Mixed(storage) => StoreFuture::Mixed(storage.load(hash)),
        }
    }

    fn delete(&self, hash: u64) {
        match self {
            EngineEnum::Noop(storage) => storage.delete(hash),
            EngineEnum::Large(storage) => storage.delete(hash),
            EngineEnum::Small(storage) => storage.delete(hash),
            EngineEnum::Mixed(storage) => storage.delete(hash),
        }
    }

    fn may_contains(&self, hash: u64) -> bool {
        match self {
            EngineEnum::Noop(storage) => storage.may_contains(hash),
            EngineEnum::Large(storage) => storage.may_contains(hash),
            EngineEnum::Small(storage) => storage.may_contains(hash),
            EngineEnum::Mixed(storage) => storage.may_contains(hash),
        }
    }

    async fn destroy(&self) -> Result<()> {
        match self {
            EngineEnum::Noop(storage) => storage.destroy().await,
            EngineEnum::Large(storage) => storage.destroy().await,
            EngineEnum::Small(storage) => storage.destroy().await,
            EngineEnum::Mixed(storage) => storage.destroy().await,
        }
    }

    fn stats(&self) -> Arc<DeviceStats> {
        match self {
            EngineEnum::Noop(storage) => storage.stats(),
            EngineEnum::Large(storage) => storage.stats(),
            EngineEnum::Small(storage) => storage.stats(),
            EngineEnum::Mixed(storage) => storage.stats(),
        }
    }

    fn wait(&self) -> impl Future<Output = ()> + Send + 'static {
        match self {
            EngineEnum::Noop(storage) => StoreFuture::Noop(storage.wait()),
            EngineEnum::Large(storage) => StoreFuture::Large(storage.wait()),
            EngineEnum::Small(storage) => StoreFuture::Small(storage.wait()),
            EngineEnum::Mixed(storage) => StoreFuture::Mixed(storage.wait()),
        }
    }
}
