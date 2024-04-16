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

use std::marker::PhantomData;

use foyer_common::code::{StorageKey, StorageValue};

use crate::{
    compress::Compression,
    device::fs::FsDevice,
    error::Result,
    generic::{GenericStore, GenericStoreConfig, GenericStoreWriter},
    storage::{Storage, StorageWriter},
};

pub type FsStore<K, V> = GenericStore<K, V, FsDevice>;
pub type FsStoreConfig<K, V> = GenericStoreConfig<K, V, FsDevice>;
pub type FsStoreWriter<K, V> = GenericStoreWriter<K, V, FsDevice>;

#[derive(Debug)]
pub struct NoneStoreWriter<K: StorageKey, V: StorageValue> {
    key: K,
    weight: usize,
    _marker: PhantomData<V>,
}

impl<K: StorageKey, V: StorageValue> NoneStoreWriter<K, V> {
    pub fn new(key: K, weight: usize) -> Self {
        Self {
            key,
            weight,
            _marker: PhantomData,
        }
    }
}

impl<K: StorageKey, V: StorageValue> StorageWriter for NoneStoreWriter<K, V> {
    type Key = K;
    type Value = V;

    fn key(&self) -> &Self::Key {
        &self.key
    }

    fn weight(&self) -> usize {
        self.weight
    }

    fn judge(&mut self) -> bool {
        false
    }

    fn force(&mut self) {}

    async fn finish(self, _: Self::Value) -> Result<bool> {
        Ok(false)
    }

    fn compression(&self) -> Compression {
        Compression::None
    }

    fn set_compression(&mut self, _: Compression) {}
}

#[derive(Debug)]
pub struct NoneStore<K: StorageKey, V: StorageValue>(PhantomData<(K, V)>);

impl<K: StorageKey, V: StorageValue> Default for NoneStore<K, V> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<K: StorageKey, V: StorageValue> Clone for NoneStore<K, V> {
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

impl<K: StorageKey, V: StorageValue> Storage for NoneStore<K, V> {
    type Key = K;
    type Value = V;
    type Config = ();
    type Writer = NoneStoreWriter<K, V>;

    async fn open(_: Self::Config) -> Result<Self> {
        Ok(NoneStore(PhantomData))
    }

    fn is_ready(&self) -> bool {
        true
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }

    fn writer(&self, key: Self::Key, weight: usize) -> Self::Writer {
        NoneStoreWriter::new(key, weight)
    }

    fn exists(&self, _: &Self::Key) -> Result<bool> {
        Ok(false)
    }

    async fn lookup(&self, _: &Self::Key) -> Result<Option<Self::Value>> {
        Ok(None)
    }

    fn remove(&self, _: &Self::Key) -> Result<bool> {
        Ok(false)
    }

    fn clear(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
pub enum StoreConfig<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    FsStoreConfig(Box<FsStoreConfig<K, V>>),
    NoneStoreConfig,
}

impl<K, V> Clone for StoreConfig<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn clone(&self) -> Self {
        match self {
            Self::FsStoreConfig(config) => Self::FsStoreConfig(config.clone()),
            Self::NoneStoreConfig => Self::NoneStoreConfig,
        }
    }
}

impl<K, V> From<FsStoreConfig<K, V>> for StoreConfig<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn from(config: FsStoreConfig<K, V>) -> Self {
        StoreConfig::FsStoreConfig(Box::new(config))
    }
}

#[derive(Debug)]
pub enum StoreWriter<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    FsStoreWriter { writer: FsStoreWriter<K, V> },
    NoneStoreWriter { writer: NoneStoreWriter<K, V> },
}

impl<K, V> From<FsStoreWriter<K, V>> for StoreWriter<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn from(writer: FsStoreWriter<K, V>) -> Self {
        StoreWriter::FsStoreWriter { writer }
    }
}

impl<K, V> From<NoneStoreWriter<K, V>> for StoreWriter<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn from(writer: NoneStoreWriter<K, V>) -> Self {
        StoreWriter::NoneStoreWriter { writer }
    }
}

#[derive(Debug)]
pub enum Store<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    FsStore { store: FsStore<K, V> },
    NoneStore { store: NoneStore<K, V> },
}

impl<K, V> Clone for Store<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn clone(&self) -> Self {
        match self {
            Self::FsStore { store } => Self::FsStore { store: store.clone() },
            Self::NoneStore { store } => Self::NoneStore { store: store.clone() },
        }
    }
}

impl<K, V> StorageWriter for StoreWriter<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    type Key = K;
    type Value = V;

    fn key(&self) -> &Self::Key {
        match self {
            StoreWriter::FsStoreWriter { writer } => writer.key(),
            StoreWriter::NoneStoreWriter { writer } => writer.key(),
        }
    }

    fn weight(&self) -> usize {
        match self {
            StoreWriter::FsStoreWriter { writer } => writer.weight(),
            StoreWriter::NoneStoreWriter { writer } => writer.weight(),
        }
    }

    fn judge(&mut self) -> bool {
        match self {
            StoreWriter::FsStoreWriter { writer } => writer.judge(),
            StoreWriter::NoneStoreWriter { writer } => writer.judge(),
        }
    }

    fn force(&mut self) {
        match self {
            StoreWriter::FsStoreWriter { writer } => writer.force(),
            StoreWriter::NoneStoreWriter { writer } => writer.force(),
        }
    }

    async fn finish(self, value: Self::Value) -> Result<bool> {
        match self {
            StoreWriter::FsStoreWriter { writer } => writer.finish(value).await,
            StoreWriter::NoneStoreWriter { writer } => writer.finish(value).await,
        }
    }

    fn compression(&self) -> Compression {
        match self {
            StoreWriter::FsStoreWriter { writer } => writer.compression(),
            StoreWriter::NoneStoreWriter { writer } => writer.compression(),
        }
    }

    fn set_compression(&mut self, compression: Compression) {
        match self {
            StoreWriter::FsStoreWriter { writer } => writer.set_compression(compression),
            StoreWriter::NoneStoreWriter { writer } => writer.set_compression(compression),
        }
    }
}

impl<K, V> Storage for Store<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    type Key = K;
    type Value = V;
    type Config = StoreConfig<K, V>;
    type Writer = StoreWriter<K, V>;

    async fn open(config: Self::Config) -> Result<Self> {
        match config {
            StoreConfig::FsStoreConfig(config) => {
                let store = FsStore::open(*config).await?;
                Ok(Self::FsStore { store })
            }
            StoreConfig::NoneStoreConfig => {
                let store = NoneStore::open(()).await?;
                Ok(Self::NoneStore { store })
            }
        }
    }

    fn is_ready(&self) -> bool {
        match self {
            Store::FsStore { store } => store.is_ready(),
            Store::NoneStore { store } => store.is_ready(),
        }
    }

    async fn close(&self) -> Result<()> {
        match self {
            Store::FsStore { store } => store.close().await,
            Store::NoneStore { store } => store.close().await,
        }
    }

    fn writer(&self, key: Self::Key, weight: usize) -> Self::Writer {
        match self {
            Store::FsStore { store } => store.writer(key, weight).into(),
            Store::NoneStore { store } => store.writer(key, weight).into(),
        }
    }

    fn exists(&self, key: &Self::Key) -> Result<bool> {
        match self {
            Store::FsStore { store } => store.exists(key),
            Store::NoneStore { store } => store.exists(key),
        }
    }

    async fn lookup(&self, key: &Self::Key) -> Result<Option<Self::Value>> {
        match self {
            Store::FsStore { store } => store.lookup(key).await,
            Store::NoneStore { store } => store.lookup(key).await,
        }
    }

    fn remove(&self, key: &Self::Key) -> Result<bool> {
        match self {
            Store::FsStore { store } => store.remove(key),
            Store::NoneStore { store } => store.remove(key),
        }
    }

    fn clear(&self) -> Result<()> {
        match self {
            Store::FsStore { store } => store.clear(),
            Store::NoneStore { store } => store.clear(),
        }
    }
}
