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

use std::fmt::Debug;

use foyer_common::code::{Key, Value};

use crate::error::Result;
use futures::Future;

pub trait FetchValueFuture<V> = Future<Output = anyhow::Result<V>> + Send + 'static;

pub trait StorageWriter: Send + Sync + Debug {
    type Key: Key;
    type Value: Value;

    fn key(&self) -> &Self::Key;

    fn weight(&self) -> usize;

    fn judge(&mut self) -> bool;

    fn force(&mut self);

    fn finish(self, value: Self::Value) -> impl Future<Output = Result<bool>> + Send;
}

pub trait Storage: Send + Sync + Debug + Clone + 'static {
    type Key: Key;
    type Value: Value;
    type Config: Send + Clone + Debug;
    type Writer: StorageWriter<Key = Self::Key, Value = Self::Value>;

    #[must_use]
    fn open(config: Self::Config) -> impl Future<Output = Result<Self>> + Send;

    fn is_ready(&self) -> bool;

    #[must_use]
    fn close(&self) -> impl Future<Output = Result<()>> + Send;

    fn writer(&self, key: Self::Key, weight: usize) -> Self::Writer;

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

pub trait AsyncStorageExt: Storage {
    #[tracing::instrument(skip(self, value))]
    fn insert_async(&self, key: Self::Key, value: Self::Value) {
        let weight = key.serialized_len() + value.serialized_len();
        let store = self.clone();
        tokio::spawn(async move {
            if let Err(e) = store.writer(key, weight).finish(value).await {
                tracing::warn!("async storage insert error: {}", e);
            }
        });
    }
}

impl<S: Storage> AsyncStorageExt for S {}

pub trait ForceStorageExt: Storage {
    #[tracing::instrument(skip(self, value))]
    fn insert_force(
        &self,
        key: Self::Key,
        value: Self::Value,
    ) -> impl Future<Output = Result<bool>> + Send {
        let weight = key.serialized_len() + value.serialized_len();
        let mut writer = self.writer(key, weight);
        writer.force();
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
            writer.force();
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
            writer.force();
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

impl<S> ForceStorageExt for S where S: Storage {}
