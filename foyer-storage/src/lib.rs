//  Copyright 2023 MrCroxx
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

#![feature(allocator_api)]
#![feature(strict_provenance)]
#![feature(trait_alias)]
#![feature(get_mut_unchecked)]
#![feature(let_chains)]
#![feature(error_generic_member_access)]
#![feature(lazy_cell)]
#![feature(lint_reasons)]
#![feature(async_fn_in_trait)]
#![feature(return_position_impl_trait_in_trait)]
#![feature(associated_type_defaults)]

use std::{
    fmt::Debug,
    sync::{Arc, OnceLock},
};

use foyer_common::code::{Key, Value};

use error::Result;
use futures::Future;

pub mod admission;
pub mod device;
pub mod error;
pub mod flusher;
pub mod indices;
pub mod judge;
pub mod metrics;
pub mod reclaimer;
pub mod region;
pub mod region_manager;
pub mod reinsertion;
pub mod slice;
pub mod store;

pub type LruFsStore<K, V> = store::Store<
    K,
    V,
    device::fs::FsDevice,
    foyer_intrusive::eviction::lru::Lru<
        region_manager::RegionEpItemAdapter<foyer_intrusive::eviction::lru::LruLink>,
    >,
    foyer_intrusive::eviction::lru::LruLink,
>;

pub type LruFsStoreConfig<K, V> = store::StoreConfig<
    K,
    V,
    device::fs::FsDevice,
    foyer_intrusive::eviction::lru::Lru<
        region_manager::RegionEpItemAdapter<foyer_intrusive::eviction::lru::LruLink>,
    >,
>;

pub type LruFsStoreWriter<'w, K, V> = store::StoreWriter<
    'w,
    K,
    V,
    device::fs::FsDevice,
    foyer_intrusive::eviction::lru::Lru<
        region_manager::RegionEpItemAdapter<foyer_intrusive::eviction::lru::LruLink>,
    >,
    foyer_intrusive::eviction::lru::LruLink,
>;

pub type LfuFsStore<K, V> = store::Store<
    K,
    V,
    device::fs::FsDevice,
    foyer_intrusive::eviction::lfu::Lfu<
        region_manager::RegionEpItemAdapter<foyer_intrusive::eviction::lfu::LfuLink>,
    >,
    foyer_intrusive::eviction::lfu::LfuLink,
>;

pub type LfuFsStoreConfig<K, V> = store::StoreConfig<
    K,
    V,
    device::fs::FsDevice,
    foyer_intrusive::eviction::lfu::Lfu<
        region_manager::RegionEpItemAdapter<foyer_intrusive::eviction::lfu::LfuLink>,
    >,
>;

pub type LfuFsStoreWriter<'w, K, V> = store::StoreWriter<
    'w,
    K,
    V,
    device::fs::FsDevice,
    foyer_intrusive::eviction::lfu::Lfu<
        region_manager::RegionEpItemAdapter<foyer_intrusive::eviction::lfu::LfuLink>,
    >,
    foyer_intrusive::eviction::lfu::LfuLink,
>;

pub type FifoFsStore<K, V> = store::Store<
    K,
    V,
    device::fs::FsDevice,
    foyer_intrusive::eviction::fifo::Fifo<
        region_manager::RegionEpItemAdapter<foyer_intrusive::eviction::fifo::FifoLink>,
    >,
    foyer_intrusive::eviction::fifo::FifoLink,
>;

pub type FifoFsStoreConfig<K, V> = store::StoreConfig<
    K,
    V,
    device::fs::FsDevice,
    foyer_intrusive::eviction::fifo::Fifo<
        region_manager::RegionEpItemAdapter<foyer_intrusive::eviction::fifo::FifoLink>,
    >,
>;

pub type FifoFsStoreWriter<'w, K, V> = store::StoreWriter<
    'w,
    K,
    V,
    device::fs::FsDevice,
    foyer_intrusive::eviction::fifo::Fifo<
        region_manager::RegionEpItemAdapter<foyer_intrusive::eviction::fifo::FifoLink>,
    >,
    foyer_intrusive::eviction::fifo::FifoLink,
>;

pub trait FetchValueFuture<V> = Future<Output = anyhow::Result<V>> + Send + 'static;

pub trait StorageWriter: Send + Sync + Debug {
    type Key: Key;
    type Value: Value;

    fn judge(&mut self) -> bool;

    fn finish(self, value: Self::Value) -> impl Future<Output = Result<bool>> + Send;
}

pub trait Storage: Send + Sync + Debug + 'static {
    type Key: Key;
    type Value: Value;
    type Config: Send + Debug;
    type Owned: Send + Sync + Debug + 'static;
    type Writer<'a>: StorageWriter<Key = Self::Key, Value = Self::Value>;

    #[must_use]
    fn open(config: Self::Config) -> impl Future<Output = Result<Self::Owned>> + Send;

    #[must_use]
    fn close(&self) -> impl Future<Output = Result<()>> + Send;

    fn writer(&self, key: Self::Key, weight: usize) -> Self::Writer<'_>;

    fn exists(&self, key: &Self::Key) -> Result<bool>;

    #[must_use]
    fn lookup(&self, key: &Self::Key) -> impl Future<Output = Result<Option<Self::Value>>> + Send;

    fn remove(&self, key: &Self::Key) -> Result<bool>;

    fn clear(&self) -> Result<()>;
}

pub trait StorageExt: Storage {
    #[must_use]
    #[tracing::instrument(skip(self, value))]
    fn insert(
        &self,
        key: Self::Key,
        value: Self::Value,
    ) -> impl Future<Output = Result<bool>> + Send {
        let weight = key.serialized_len() + value.serialized_len();
        self.writer(key, weight).finish(value)
    }

    #[must_use]
    #[tracing::instrument(skip(self, value))]
    fn insert_if_not_exists(
        &self,
        key: Self::Key,
        value: Self::Value,
    ) -> impl Future<Output = Result<bool>> + Send {
        async move {
            if self.exists(&key)? {
                return Ok(false);
            }
            self.insert(key, value).await
        }
    }

    /// First judge if the entry will be admitted with `key` and `weight` by admission policies.
    /// Then `f` will be called and entry will be inserted.
    ///
    /// # Safety
    ///
    /// `weight` MUST be equal to `key.serialized_len() + value.serialized_len()`
    #[must_use]
    #[tracing::instrument(skip(self, f))]
    fn insert_with<F>(
        &self,
        key: Self::Key,
        f: F,
        weight: usize,
    ) -> impl Future<Output = Result<bool>> + Send
    where
        F: FnOnce() -> anyhow::Result<Self::Value> + Send,
    {
        async move {
            let mut writer = self.writer(key, weight);
            if !writer.judge() {
                return Ok(false);
            }
            let value = match f() {
                Ok(value) => value,
                Err(e) => {
                    tracing::warn!("fetch value error: {:?}", e);
                    return Ok(false);
                }
            };
            writer.finish(value).await
        }
    }

    /// First judge if the entry will be admitted with `key` and `weight` by admission policies.
    /// Then `f` will be called to fetch value, and entry will be inserted.
    ///
    /// # Safety
    ///
    /// `weight` MUST be equal to `key.serialized_len() + value.serialized_len()`
    #[tracing::instrument(skip(self, f))]
    fn insert_with_future<F, FU>(
        &self,
        key: Self::Key,
        f: F,
        weight: usize,
    ) -> impl Future<Output = Result<bool>> + Send
    where
        F: FnOnce() -> FU + Send,
        FU: FetchValueFuture<Self::Value>,
    {
        async move {
            let mut writer = self.writer(key, weight);
            if !writer.judge() {
                return Ok(false);
            }
            let value = match f().await {
                Ok(value) => value,
                Err(e) => {
                    tracing::warn!("fetch value error: {:?}", e);
                    return Ok(false);
                }
            };
            writer.finish(value).await
        }
    }

    #[tracing::instrument(skip(self, f))]
    fn insert_if_not_exists_with<F>(
        &self,
        key: Self::Key,
        f: F,
        weight: usize,
    ) -> impl Future<Output = Result<bool>> + Send
    where
        F: FnOnce() -> anyhow::Result<Self::Value> + Send,
    {
        async move {
            if self.exists(&key)? {
                return Ok(false);
            }
            self.insert_with(key, f, weight).await
        }
    }

    #[tracing::instrument(skip(self, f))]
    fn insert_if_not_exists_with_future<F, FU>(
        &self,
        key: Self::Key,
        f: F,
        weight: usize,
    ) -> impl Future<Output = Result<bool>> + Send
    where
        F: FnOnce() -> FU + Send,
        FU: FetchValueFuture<Self::Value>,
    {
        async move {
            if self.exists(&key)? {
                return Ok(false);
            }
            self.insert_with_future(key, f, weight).await
        }
    }
}

impl<S: Storage> StorageExt for S {}

pub trait ForceStorageWriter: StorageWriter {
    fn set_force(&mut self);
}

pub trait ForceStorageExt: Storage
where
    for<'w> Self::Writer<'w>: ForceStorageWriter,
{
    #[tracing::instrument(skip(self, value))]
    fn insert_force(
        &self,
        key: Self::Key,
        value: Self::Value,
    ) -> impl Future<Output = Result<bool>> + Send {
        let weight = key.serialized_len() + value.serialized_len();
        let mut writer = self.writer(key, weight);
        writer.set_force();
        writer.finish(value)
    }

    /// First judge if the entry will be admitted with `key` and `weight` by admission policies.
    /// Then `f` will be called and entry will be inserted.
    ///
    /// # Safety
    ///
    /// `weight` MUST be equal to `key.serialized_len() + value.serialized_len()`
    #[tracing::instrument(skip(self, f))]
    fn insert_force_with<F>(
        &self,
        key: Self::Key,
        f: F,
        weight: usize,
    ) -> impl Future<Output = Result<bool>> + Send
    where
        F: FnOnce() -> anyhow::Result<Self::Value> + Send,
    {
        async move {
            let mut writer = self.writer(key, weight);
            writer.set_force();
            if !writer.judge() {
                return Ok(false);
            }
            let value = match f() {
                Ok(value) => value,
                Err(e) => {
                    tracing::warn!("fetch value error: {:?}", e);
                    return Ok(false);
                }
            };
            let inserted = writer.finish(value).await?;
            Ok(inserted)
        }
    }

    /// First judge if the entry will be admitted with `key` and `weight` by admission policies.
    /// Then `f` will be called to fetch value, and entry will be inserted.
    ///
    /// # Safety
    ///
    /// `weight` MUST be equal to `key.serialized_len() + value.serialized_len()`
    #[tracing::instrument(skip(self, f))]
    fn insert_force_with_future<F, FU>(
        &self,
        key: Self::Key,
        f: F,
        weight: usize,
    ) -> impl Future<Output = Result<bool>> + Send
    where
        F: FnOnce() -> FU + Send,
        FU: FetchValueFuture<Self::Value>,
    {
        async move {
            let mut writer = self.writer(key, weight);
            writer.set_force();
            if !writer.judge() {
                return Ok(false);
            }
            let value = match f().await {
                Ok(value) => value,
                Err(e) => {
                    tracing::warn!("fetch value error: {:?}", e);
                    return Ok(false);
                }
            };
            let inserted = writer.finish(value).await?;
            Ok(inserted)
        }
    }
}

impl<S> ForceStorageExt for S
where
    S: Storage,
    for<'w> S::Writer<'w>: ForceStorageWriter,
{
}

#[derive(Debug)]
pub enum StoreConfig<K, V>
where
    K: Key,
    V: Value,
{
    LruFsStoreConfig { config: LruFsStoreConfig<K, V> },
    LfuFsStoreConfig { config: LfuFsStoreConfig<K, V> },
    FifoFsStoreConfig { config: FifoFsStoreConfig<K, V> },
    None,
}

impl<K, V> From<LruFsStoreConfig<K, V>> for StoreConfig<K, V>
where
    K: Key,
    V: Value,
{
    fn from(config: LruFsStoreConfig<K, V>) -> Self {
        StoreConfig::LruFsStoreConfig { config }
    }
}

impl<K, V> From<LfuFsStoreConfig<K, V>> for StoreConfig<K, V>
where
    K: Key,
    V: Value,
{
    fn from(config: LfuFsStoreConfig<K, V>) -> Self {
        StoreConfig::LfuFsStoreConfig { config }
    }
}

impl<K, V> From<FifoFsStoreConfig<K, V>> for StoreConfig<K, V>
where
    K: Key,
    V: Value,
{
    fn from(config: FifoFsStoreConfig<K, V>) -> Self {
        StoreConfig::FifoFsStoreConfig { config }
    }
}

#[derive(Debug)]
pub enum StoreWriter<'a, K, V>
where
    K: Key,
    V: Value,
{
    LruFsStorWriter { writer: LruFsStoreWriter<'a, K, V> },
    LfuFsStorWriter { writer: LfuFsStoreWriter<'a, K, V> },
    FifoFsStoreWriter { writer: FifoFsStoreWriter<'a, K, V> },
    None,
}

impl<'a, K, V> From<LruFsStoreWriter<'a, K, V>> for StoreWriter<'a, K, V>
where
    K: Key,
    V: Value,
{
    fn from(writer: LruFsStoreWriter<'a, K, V>) -> Self {
        StoreWriter::LruFsStorWriter { writer }
    }
}

impl<'a, K, V> From<LfuFsStoreWriter<'a, K, V>> for StoreWriter<'a, K, V>
where
    K: Key,
    V: Value,
{
    fn from(writer: LfuFsStoreWriter<'a, K, V>) -> Self {
        StoreWriter::LfuFsStorWriter { writer }
    }
}

impl<'a, K, V> From<FifoFsStoreWriter<'a, K, V>> for StoreWriter<'a, K, V>
where
    K: Key,
    V: Value,
{
    fn from(writer: FifoFsStoreWriter<'a, K, V>) -> Self {
        StoreWriter::FifoFsStoreWriter { writer }
    }
}

#[derive(Debug)]
pub enum Store<K, V>
where
    K: Key,
    V: Value,
{
    LruFsStore { store: Arc<LruFsStore<K, V>> },
    LfuFsStore { store: Arc<LfuFsStore<K, V>> },
    FifoFsStore { store: Arc<FifoFsStore<K, V>> },
    None,
}

impl<K, V> Clone for Store<K, V>
where
    K: Key,
    V: Value,
{
    fn clone(&self) -> Self {
        match self {
            Self::LruFsStore { store } => Self::LruFsStore {
                store: Arc::clone(store),
            },
            Self::LfuFsStore { store } => Self::LfuFsStore {
                store: Arc::clone(store),
            },
            Self::FifoFsStore { store } => Self::FifoFsStore {
                store: Arc::clone(store),
            },
            Self::None => Self::None,
        }
    }
}

impl<'a, K, V> StorageWriter for StoreWriter<'a, K, V>
where
    K: Key,
    V: Value,
{
    type Key = K;
    type Value = V;

    fn judge(&mut self) -> bool {
        match self {
            StoreWriter::LruFsStorWriter { writer } => writer.judge(),
            StoreWriter::LfuFsStorWriter { writer } => writer.judge(),
            StoreWriter::FifoFsStoreWriter { writer } => writer.judge(),
            StoreWriter::None => false,
        }
    }

    async fn finish(self, value: Self::Value) -> Result<bool> {
        match self {
            StoreWriter::LruFsStorWriter { writer } => writer.finish(value).await,
            StoreWriter::LfuFsStorWriter { writer } => writer.finish(value).await,
            StoreWriter::FifoFsStoreWriter { writer } => writer.finish(value).await,
            StoreWriter::None => Ok(false),
        }
    }
}

impl<'a, K, V> ForceStorageWriter for StoreWriter<'a, K, V>
where
    K: Key,
    V: Value,
{
    fn set_force(&mut self) {
        match self {
            StoreWriter::LruFsStorWriter { writer } => writer.set_force(),
            StoreWriter::LfuFsStorWriter { writer } => writer.set_force(),
            StoreWriter::FifoFsStoreWriter { writer } => writer.set_force(),
            StoreWriter::None => {}
        }
    }
}

impl<K, V> Storage for Store<K, V>
where
    K: Key,
    V: Value,
{
    type Key = K;
    type Value = V;
    type Config = StoreConfig<K, V>;
    type Owned = Self;
    type Writer<'a> = StoreWriter<'a, K, V>;

    async fn open(config: Self::Config) -> Result<Self::Owned> {
        match config {
            StoreConfig::LruFsStoreConfig { config } => {
                let store = LruFsStore::open(config).await?;
                Ok(Self::LruFsStore { store })
            }
            StoreConfig::LfuFsStoreConfig { config } => {
                let store = LfuFsStore::open(config).await?;
                Ok(Self::LfuFsStore { store })
            }
            StoreConfig::FifoFsStoreConfig { config } => {
                let store = FifoFsStore::open(config).await?;
                Ok(Self::FifoFsStore { store })
            }
            StoreConfig::None => Ok(Self::None),
        }
    }

    async fn close(&self) -> Result<()> {
        match self {
            Store::LruFsStore { store } => store.close().await,
            Store::LfuFsStore { store } => store.close().await,
            Store::FifoFsStore { store } => store.close().await,
            Store::None => Ok(()),
        }
    }

    fn writer(&self, key: Self::Key, weight: usize) -> Self::Writer<'_> {
        match self {
            Store::LruFsStore { store } => store.writer(key, weight).into(),
            Store::LfuFsStore { store } => store.writer(key, weight).into(),
            Store::FifoFsStore { store } => store.writer(key, weight).into(),
            Store::None => StoreWriter::None,
        }
    }

    fn exists(&self, key: &Self::Key) -> Result<bool> {
        match self {
            Store::LruFsStore { store } => store.exists(key),
            Store::LfuFsStore { store } => store.exists(key),
            Store::FifoFsStore { store } => store.exists(key),
            Store::None => Ok(false),
        }
    }

    async fn lookup(&self, key: &Self::Key) -> Result<Option<Self::Value>> {
        match self {
            Store::LruFsStore { store } => store.lookup(key).await,
            Store::LfuFsStore { store } => store.lookup(key).await,
            Store::FifoFsStore { store } => store.lookup(key).await,
            Store::None => Ok(None),
        }
    }

    fn remove(&self, key: &Self::Key) -> Result<bool> {
        match self {
            Store::LruFsStore { store } => store.remove(key),
            Store::LfuFsStore { store } => store.remove(key),
            Store::FifoFsStore { store } => store.remove(key),
            Store::None => Ok(false),
        }
    }

    fn clear(&self) -> Result<()> {
        match self {
            Store::LruFsStore { store } => store.clear(),
            Store::LfuFsStore { store } => store.clear(),
            Store::FifoFsStore { store } => store.clear(),
            Store::None => Ok(()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LazyStore<K, V>
where
    K: Key,
    V: Value,
{
    once: OnceLock<Store<K, V>>,
    none: Store<K, V>,
}

impl<K, V> LazyStore<K, V>
where
    K: Key,
    V: Value,
{
    pub fn lazy(config: StoreConfig<K, V>) -> Self {
        let once = OnceLock::new();

        tokio::spawn({
            let once = once.clone();
            async move {
                let store = match Store::open(config).await {
                    Ok(store) => store,
                    Err(e) => {
                        tracing::error!("Lazy open store fail: {}", e);
                        return;
                    }
                };
                once.set(store).unwrap();
            }
        });

        Self {
            once,
            none: Store::None,
        }
    }
}

impl<K, V> Storage for LazyStore<K, V>
where
    K: Key,
    V: Value,
{
    type Key = K;
    type Value = V;
    type Config = StoreConfig<K, V>;
    type Owned = Self;
    type Writer<'a> = StoreWriter<'a, K, V>;

    async fn open(config: Self::Config) -> Result<Self::Owned> {
        let once = OnceLock::new();
        let store = Store::open(config).await?;
        once.set(store).unwrap();
        Ok(Self {
            once,
            none: Store::None,
        })
    }

    async fn close(&self) -> Result<()> {
        match self.once.get() {
            Some(store) => store.close().await,
            None => self.none.close().await,
        }
    }

    fn writer(&self, key: Self::Key, weight: usize) -> Self::Writer<'_> {
        match self.once.get() {
            Some(store) => store.writer(key, weight),
            None => self.none.writer(key, weight),
        }
    }

    fn exists(&self, key: &Self::Key) -> Result<bool> {
        match self.once.get() {
            Some(store) => store.exists(key),
            None => self.none.exists(key),
        }
    }

    async fn lookup(&self, key: &Self::Key) -> Result<Option<Self::Value>> {
        match self.once.get() {
            Some(store) => store.lookup(key).await,
            None => self.none.lookup(key).await,
        }
    }

    fn remove(&self, key: &Self::Key) -> Result<bool> {
        match self.once.get() {
            Some(store) => store.remove(key),
            None => self.none.remove(key),
        }
    }

    fn clear(&self) -> Result<()> {
        match self.once.get() {
            Some(store) => store.clear(),
            None => self.none.clear(),
        }
    }
}
