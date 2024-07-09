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

use foyer_common::code::{HashBuilder, StorageKey, StorageValue};
use foyer_memory::CacheEntry;
use futures::{
    future::{try_join, Either as EitherFuture},
    Future,
};
use tokio::runtime::Handle;

use crate::{error::Result, storage::Storage, DeviceStats, EnqueueHandle};

use std::{borrow::Borrow, fmt::Debug, hash::Hash, sync::Arc};

pub struct EitherConfig<K, V, S, SL, SR, SE>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
    SL: Storage<Key = K, Value = V, BuildHasher = S>,
    SR: Storage<Key = K, Value = V, BuildHasher = S>,
    SE: Selector<Key = K>,
{
    pub selector: SE,
    pub left: SL::Config,
    pub right: SR::Config,
}

impl<K, V, S, SL, SR, SE> Debug for EitherConfig<K, V, S, SL, SR, SE>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
    SL: Storage<Key = K, Value = V, BuildHasher = S>,
    SR: Storage<Key = K, Value = V, BuildHasher = S>,
    SE: Selector<Key = K>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EitherStoreConfig")
            .field("selector", &self.selector)
            .field("left", &self.left)
            .field("right", &self.right)
            .finish()
    }
}

#[allow(unused)]
pub enum Selection {
    Left,
    Right,
}

pub trait Selector: Send + Sync + 'static + Debug {
    type Key;

    fn select<Q>(&self, key: &Q) -> Selection
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized;
}

pub struct Either<K, V, S, SL, SR, SE>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
    SL: Storage<Key = K, Value = V, BuildHasher = S>,
    SR: Storage<Key = K, Value = V, BuildHasher = S>,
    SE: Selector<Key = K>,
{
    selector: Arc<SE>,

    left: SL,
    right: SR,
}

impl<K, V, S, SL, SR, SE> Debug for Either<K, V, S, SL, SR, SE>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
    SL: Storage<Key = K, Value = V, BuildHasher = S>,
    SR: Storage<Key = K, Value = V, BuildHasher = S>,
    SE: Selector<Key = K>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EitherStore")
            .field("left", &self.left)
            .field("right", &self.right)
            .finish()
    }
}

impl<K, V, S, SL, SR, SE> Clone for Either<K, V, S, SL, SR, SE>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
    SL: Storage<Key = K, Value = V, BuildHasher = S>,
    SR: Storage<Key = K, Value = V, BuildHasher = S>,
    SE: Selector<Key = K>,
{
    fn clone(&self) -> Self {
        Self {
            selector: self.selector.clone(),
            left: self.left.clone(),
            right: self.right.clone(),
        }
    }
}

impl<K, V, S, SL, SR, SE> Storage for Either<K, V, S, SL, SR, SE>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
    SL: Storage<Key = K, Value = V, BuildHasher = S>,
    SR: Storage<Key = K, Value = V, BuildHasher = S>,
    SE: Selector<Key = K>,
{
    type Key = K;
    type Value = V;
    type BuildHasher = S;
    type Config = EitherConfig<K, V, S, SL, SR, SE>;

    async fn open(config: Self::Config) -> Result<Self> {
        let selector = Arc::new(config.selector);
        let left = SL::open(config.left).await?;
        let right = SR::open(config.right).await?;

        Ok(Self { selector, left, right })
    }

    async fn close(&self) -> Result<()> {
        self.left.close().await?;
        self.right.close().await?;
        Ok(())
    }

    fn enqueue(&self, entry: CacheEntry<Self::Key, Self::Value, Self::BuildHasher>, force: bool) -> EnqueueHandle {
        match self.selector.select(entry.key()) {
            Selection::Left => self.left.enqueue(entry, force),
            Selection::Right => self.right.enqueue(entry, force),
        }
    }

    fn load<Q>(&self, key: &Q) -> impl Future<Output = Result<Option<(Self::Key, Self::Value)>>> + Send + 'static
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Send + Sync + 'static,
    {
        match self.selector.select(key) {
            Selection::Left => EitherFuture::Left(self.left.load(key)),
            Selection::Right => EitherFuture::Right(self.right.load(key)),
        }
    }

    fn delete<Q>(&self, key: &Q) -> EnqueueHandle
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        match self.selector.select(key) {
            Selection::Left => self.left.delete(key),
            Selection::Right => self.right.delete(key),
        }
    }

    fn may_contains<Q>(&self, key: &Q) -> bool
    where
        Self::Key: std::borrow::Borrow<Q>,
        Q: std::hash::Hash + Eq + ?Sized,
    {
        match self.selector.select(key) {
            Selection::Left => self.left.may_contains(key),
            Selection::Right => self.right.may_contains(key),
        }
    }

    async fn destroy(&self) -> Result<()> {
        try_join(self.left.destroy(), self.right.destroy()).await?;
        Ok(())
    }

    fn stats(&self) -> std::sync::Arc<DeviceStats> {
        // FIXME(MrCroxx): This interface needs to be refactored for it is device level stats, QwQ.
        unimplemented!("This interface needs to be refactored for it is device level stats, QwQ.")
    }

    async fn wait(&self) -> Result<()> {
        try_join(self.left.wait(), self.right.wait()).await?;
        Ok(())
    }

    fn runtime(&self) -> &Handle {
        if cfg!(debug_assertions) {
            let hleft = self.left.runtime();
            let hright = self.right.runtime();
            assert_eq!(hleft.id(), hright.id());
            hleft
        } else {
            self.left.runtime()
        }
    }
}
