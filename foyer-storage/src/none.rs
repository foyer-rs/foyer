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

use std::{borrow::Borrow, hash::Hash, marker::PhantomData, sync::Arc};

use foyer_common::{
    arcable::Arcable,
    code::{StorageKey, StorageValue},
};

use crate::{
    compress::Compression,
    error::Result,
    storage::{CachedEntry, Storage, StorageWriter},
};

#[derive(Debug)]
pub struct NoneStoreWriter<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    key: Arc<K>,
    _marker: PhantomData<V>,
}

impl<K, V> NoneStoreWriter<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    pub fn new(key: impl Into<Arcable<K>>) -> Self {
        Self {
            key: key.into().into_arc(),
            _marker: PhantomData,
        }
    }
}

impl<K, V> StorageWriter<K, V> for NoneStoreWriter<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn key(&self) -> &K {
        &self.key
    }

    fn judge(&mut self) -> bool {
        false
    }

    fn force(&mut self) {}

    async fn finish<AV>(self, _: AV) -> Result<Option<CachedEntry<K, V>>>
    where
        AV: Into<Arcable<V>> + Send + 'static,
    {
        Ok(None)
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

impl<K: StorageKey, V: StorageValue> Storage<K, V> for NoneStore<K, V> {
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

    fn writer<AK>(&self, key: AK) -> Self::Writer
    where
        AK: Into<Arcable<K>> + Send + 'static,
    {
        NoneStoreWriter::new(key)
    }

    fn exists<Q>(&self, _: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        Ok(false)
    }

    async fn get<Q>(&self, _: &Q) -> Result<Option<CachedEntry<K, V>>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        Ok(None)
    }

    fn remove<Q>(&self, _: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        Ok(false)
    }

    fn clear(&self) -> Result<()> {
        Ok(())
    }
}
