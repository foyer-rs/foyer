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

use std::{fmt::Debug, future::Future, marker::PhantomData, sync::Arc};

use auto_enums::auto_enum;
use foyer_common::{
    code::{StorageKey, StorageValue},
    properties::Properties,
};
use foyer_memory::Piece;

use crate::{
    error::Result,
    large::generic::{GenericLargeStorage, GenericLargeStorageConfig},
    small::generic::{GenericSmallStorage, GenericSmallStorageConfig},
    storage::{
        either::{Either, EitherConfig, Selection, Selector},
        noop::Noop,
    },
    Statistics, Storage, Throttle,
};

pub struct SizeSelector<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    threshold: usize,
    _marker: PhantomData<(K, V, P)>,
}

impl<K, V, P> Debug for SizeSelector<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SizeSelector")
            .field("threshold", &self.threshold)
            .finish()
    }
}

impl<K, V, P> SizeSelector<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    pub fn new(threshold: usize) -> Self {
        Self {
            threshold,
            _marker: PhantomData,
        }
    }
}

impl<K, V, P> Selector for SizeSelector<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    type Key = K;
    type Value = V;
    type Properties = P;

    fn select(&self, _piece: &Piece<Self::Key, Self::Value, Self::Properties>, estimated_size: usize) -> Selection {
        if estimated_size < self.threshold {
            Selection::Left
        } else {
            Selection::Right
        }
    }
}

#[expect(clippy::type_complexity)]
pub enum EngineConfig<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    Noop,
    Large(GenericLargeStorageConfig<K, V>),
    Small(GenericSmallStorageConfig<K, V>),
    Mixed(EitherConfig<K, V, GenericSmallStorage<K, V, P>, GenericLargeStorage<K, V, P>, SizeSelector<K, V, P>>),
}

impl<K, V, P> Debug for EngineConfig<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
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
pub enum EngineEnum<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    /// No-op disk cache.
    Noop(Noop<K, V, P>),
    /// Large object disk cache.
    Large(GenericLargeStorage<K, V, P>),
    /// Small object disk cache.
    Small(GenericSmallStorage<K, V, P>),
    /// Mixed large and small object disk cache.
    Mixed(Either<K, V, P, GenericSmallStorage<K, V, P>, GenericLargeStorage<K, V, P>, SizeSelector<K, V, P>>),
}

impl<K, V, P> Debug for EngineEnum<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
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

impl<K, V, P> Clone for EngineEnum<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
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

impl<K, V, P> Storage for EngineEnum<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    type Key = K;
    type Value = V;
    type Properties = P;
    type Config = EngineConfig<K, V, P>;

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

    fn enqueue(&self, piece: Piece<Self::Key, Self::Value, Self::Properties>, estimated_size: usize) {
        match self {
            EngineEnum::Noop(storage) => storage.enqueue(piece, estimated_size),
            EngineEnum::Large(storage) => storage.enqueue(piece, estimated_size),
            EngineEnum::Small(storage) => storage.enqueue(piece, estimated_size),
            EngineEnum::Mixed(storage) => storage.enqueue(piece, estimated_size),
        }
    }

    #[auto_enum(Future)]
    fn load(&self, hash: u64) -> impl Future<Output = Result<Option<(Self::Key, Self::Value)>>> + Send + 'static {
        match self {
            EngineEnum::Noop(storage) => storage.load(hash),
            EngineEnum::Large(storage) => storage.load(hash),
            EngineEnum::Small(storage) => storage.load(hash),
            EngineEnum::Mixed(storage) => storage.load(hash),
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

    fn throttle(&self) -> &Throttle {
        match self {
            EngineEnum::Noop(storage) => storage.throttle(),
            EngineEnum::Large(storage) => storage.throttle(),
            EngineEnum::Small(storage) => storage.throttle(),
            EngineEnum::Mixed(storage) => storage.throttle(),
        }
    }

    fn statistics(&self) -> &Arc<Statistics> {
        match self {
            EngineEnum::Noop(storage) => storage.statistics(),
            EngineEnum::Large(storage) => storage.statistics(),
            EngineEnum::Small(storage) => storage.statistics(),
            EngineEnum::Mixed(storage) => storage.statistics(),
        }
    }

    #[auto_enum(Future)]
    fn wait(&self) -> impl Future<Output = ()> + Send + 'static {
        match self {
            EngineEnum::Noop(storage) => storage.wait(),
            EngineEnum::Large(storage) => storage.wait(),
            EngineEnum::Small(storage) => storage.wait(),
            EngineEnum::Mixed(storage) => storage.wait(),
        }
    }
}
