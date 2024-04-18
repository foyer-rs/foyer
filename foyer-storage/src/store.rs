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

use foyer_common::code::{StorageKey, StorageValue};
use std::{borrow::Borrow, fmt::Debug, hash::Hash};

use crate::{
    compress::Compression,
    device::fs::FsDevice,
    error::Result,
    generic::{GenericStore, GenericStoreConfig, GenericStoreWriter},
    lazy::{Lazy, LazyStoreWriter},
    none::{NoneStore, NoneStoreWriter},
    runtime::{Runtime, RuntimeStoreConfig, RuntimeStoreWriter},
    storage::{Storage, StorageWriter},
};

pub type FsStore<K, V> = GenericStore<K, V, FsDevice>;
pub type FsStoreConfig<K, V> = GenericStoreConfig<K, V, FsDevice>;
pub type FsStoreWriter<K, V> = GenericStoreWriter<K, V, FsDevice>;

pub enum StoreConfig<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    None,

    Fs(FsStoreConfig<K, V>),
    LazyFs(FsStoreConfig<K, V>),
    RuntimeFs(RuntimeStoreConfig<K, V, FsStore<K, V>>),
    RuntimeLazyFs(RuntimeStoreConfig<K, V, Lazy<K, V, FsStore<K, V>>>),
}

impl<K, V> Debug for StoreConfig<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "None"),
            Self::Fs(config) => f.debug_tuple("Fs").field(config).finish(),
            Self::LazyFs(config) => f.debug_tuple("LazyFs").field(config).finish(),
            Self::RuntimeFs(config) => f.debug_tuple("RuntimeFs").field(config).finish(),
            Self::RuntimeLazyFs(config) => f.debug_tuple("RuntimeLazyFs").field(config).finish(),
        }
    }
}

impl<K, V> Clone for StoreConfig<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn clone(&self) -> Self {
        match self {
            StoreConfig::None => StoreConfig::None,
            StoreConfig::Fs(config) => StoreConfig::Fs(config.clone()),
            StoreConfig::LazyFs(config) => StoreConfig::LazyFs(config.clone()),
            StoreConfig::RuntimeFs(config) => StoreConfig::RuntimeFs(config.clone()),
            StoreConfig::RuntimeLazyFs(config) => StoreConfig::RuntimeLazyFs(config.clone()),
        }
    }
}

pub enum StoreWriter<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    None(NoneStoreWriter<K, V>),

    Fs(FsStoreWriter<K, V>),
    LazyFs(LazyStoreWriter<K, V, FsStore<K, V>>),
    RuntimeFs(RuntimeStoreWriter<K, V, FsStore<K, V>>),
    RuntimeLazyFs(RuntimeStoreWriter<K, V, Lazy<K, V, FsStore<K, V>>>),
}

impl<K, V> Debug for StoreWriter<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None(writer) => f.debug_tuple("None").field(writer).finish(),
            Self::Fs(writer) => f.debug_tuple("Fs").field(writer).finish(),
            Self::LazyFs(writer) => f.debug_tuple("LazyFs").field(writer).finish(),
            Self::RuntimeFs(writer) => f.debug_tuple("RuntimeFs").field(writer).finish(),
            Self::RuntimeLazyFs(writer) => f.debug_tuple("RuntimeLazyFs").field(writer).finish(),
        }
    }
}

#[derive(Clone)]
pub enum Store<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    None(NoneStore<K, V>),

    Fs(FsStore<K, V>),
    LazyFs(Lazy<K, V, FsStore<K, V>>),
    RuntimeFs(Runtime<K, V, FsStore<K, V>>),
    RuntimeLazyFs(Runtime<K, V, Lazy<K, V, FsStore<K, V>>>),
}

impl<K, V> Debug for Store<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None(store) => f.debug_tuple("None").field(store).finish(),
            Self::Fs(store) => f.debug_tuple("Fs").field(store).finish(),
            Self::LazyFs(store) => f.debug_tuple("LazyFs").field(store).finish(),
            Self::RuntimeFs(store) => f.debug_tuple("RuntimeFs").field(store).finish(),
            Self::RuntimeLazyFs(store) => f.debug_tuple("RuntimeLazyFs").field(store).finish(),
        }
    }
}

impl<K, V> StorageWriter<K, V> for StoreWriter<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn key(&self) -> &K {
        match self {
            StoreWriter::None(writer) => writer.key(),
            StoreWriter::Fs(writer) => writer.key(),
            StoreWriter::LazyFs(writer) => writer.key(),
            StoreWriter::RuntimeFs(writer) => writer.key(),
            StoreWriter::RuntimeLazyFs(writer) => writer.key(),
        }
    }

    fn weight(&self) -> usize {
        match self {
            StoreWriter::None(writer) => writer.weight(),
            StoreWriter::Fs(writer) => writer.weight(),
            StoreWriter::LazyFs(writer) => writer.weight(),
            StoreWriter::RuntimeFs(writer) => writer.weight(),
            StoreWriter::RuntimeLazyFs(writer) => writer.weight(),
        }
    }

    fn judge(&mut self) -> bool {
        match self {
            StoreWriter::None(writer) => writer.judge(),
            StoreWriter::Fs(writer) => writer.judge(),
            StoreWriter::LazyFs(writer) => writer.judge(),
            StoreWriter::RuntimeFs(writer) => writer.judge(),
            StoreWriter::RuntimeLazyFs(writer) => writer.judge(),
        }
    }

    fn force(&mut self) {
        match self {
            StoreWriter::None(writer) => writer.force(),
            StoreWriter::Fs(writer) => writer.force(),
            StoreWriter::LazyFs(writer) => writer.force(),
            StoreWriter::RuntimeFs(writer) => writer.force(),
            StoreWriter::RuntimeLazyFs(writer) => writer.force(),
        }
    }

    fn compression(&self) -> Compression {
        match self {
            StoreWriter::None(writer) => writer.compression(),
            StoreWriter::Fs(writer) => writer.compression(),
            StoreWriter::LazyFs(writer) => writer.compression(),
            StoreWriter::RuntimeFs(writer) => writer.compression(),
            StoreWriter::RuntimeLazyFs(writer) => writer.compression(),
        }
    }

    fn set_compression(&mut self, compression: Compression) {
        match self {
            StoreWriter::None(writer) => writer.set_compression(compression),
            StoreWriter::Fs(writer) => writer.set_compression(compression),
            StoreWriter::LazyFs(writer) => writer.set_compression(compression),
            StoreWriter::RuntimeFs(writer) => writer.set_compression(compression),
            StoreWriter::RuntimeLazyFs(writer) => writer.set_compression(compression),
        }
    }

    async fn finish(self, value: V) -> Result<bool> {
        match self {
            StoreWriter::None(writer) => writer.finish(value).await,
            StoreWriter::Fs(writer) => writer.finish(value).await,
            StoreWriter::LazyFs(writer) => writer.finish(value).await,
            StoreWriter::RuntimeFs(writer) => writer.finish(value).await,
            StoreWriter::RuntimeLazyFs(writer) => writer.finish(value).await,
        }
    }
}

impl<K, V> Storage<K, V> for Store<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    type Config = StoreConfig<K, V>;
    type Writer = StoreWriter<K, V>;

    async fn open(config: Self::Config) -> Result<Self> {
        let store = match config {
            StoreConfig::None => Self::None(NoneStore::open(()).await?),
            StoreConfig::Fs(config) => Self::Fs(FsStore::open(config).await?),
            StoreConfig::LazyFs(config) => Self::LazyFs(Lazy::open(config).await?),
            StoreConfig::RuntimeFs(config) => Self::RuntimeFs(Runtime::open(config).await?),
            StoreConfig::RuntimeLazyFs(config) => Self::RuntimeLazyFs(Runtime::open(config).await?),
        };
        Ok(store)
    }

    fn is_ready(&self) -> bool {
        match self {
            Store::None(store) => store.is_ready(),
            Store::Fs(store) => store.is_ready(),
            Store::LazyFs(store) => store.is_ready(),
            Store::RuntimeFs(store) => store.is_ready(),
            Store::RuntimeLazyFs(store) => store.is_ready(),
        }
    }

    async fn close(&self) -> Result<()> {
        match self {
            Store::None(store) => store.close().await,
            Store::Fs(store) => store.close().await,
            Store::LazyFs(store) => store.close().await,
            Store::RuntimeFs(store) => store.close().await,
            Store::RuntimeLazyFs(store) => store.close().await,
        }
    }

    fn writer(&self, key: K, weight: usize) -> Self::Writer {
        match self {
            Store::None(store) => StoreWriter::None(store.writer(key, weight)),
            Store::Fs(store) => StoreWriter::Fs(store.writer(key, weight)),
            Store::LazyFs(store) => StoreWriter::LazyFs(store.writer(key, weight)),
            Store::RuntimeFs(store) => StoreWriter::RuntimeFs(store.writer(key, weight)),
            Store::RuntimeLazyFs(store) => StoreWriter::RuntimeLazyFs(store.writer(key, weight)),
        }
    }

    fn exists<Q>(&self, key: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        match self {
            Store::None(store) => store.exists(key),
            Store::Fs(store) => store.exists(key),
            Store::LazyFs(store) => store.exists(key),
            Store::RuntimeFs(store) => store.exists(key),
            Store::RuntimeLazyFs(store) => store.exists(key),
        }
    }

    async fn lookup<Q>(&self, key: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Send + Sync + Clone + 'static,
    {
        match self {
            Store::None(store) => store.lookup(key).await,
            Store::Fs(store) => store.lookup(key).await,
            Store::LazyFs(store) => store.lookup(key).await,
            Store::RuntimeFs(store) => store.lookup(key).await,
            Store::RuntimeLazyFs(store) => store.lookup(key).await,
        }
    }

    fn remove<Q>(&self, key: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        match self {
            Store::None(store) => store.remove(key),
            Store::Fs(store) => store.remove(key),
            Store::LazyFs(store) => store.remove(key),
            Store::RuntimeFs(store) => store.remove(key),
            Store::RuntimeLazyFs(store) => store.remove(key),
        }
    }

    fn clear(&self) -> Result<()> {
        match self {
            Store::None(store) => store.clear(),
            Store::Fs(store) => store.clear(),
            Store::LazyFs(store) => store.clear(),
            Store::RuntimeFs(store) => store.clear(),
            Store::RuntimeLazyFs(store) => store.clear(),
        }
    }
}
