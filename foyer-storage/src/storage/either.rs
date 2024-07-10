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
    future::{ready, select, try_join, Either as EitherFuture},
    pin_mut, Future, FutureExt,
};
use tokio::{runtime::Handle, sync::oneshot};

use crate::{device::IoBuffer, error::Result, serde::KvInfo, storage::Storage, DeviceStats};

use std::{borrow::Borrow, fmt::Debug, hash::Hash, sync::Arc};

use super::WaitHandle;

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

pub enum Selection {
    Left,
    Right,
}

pub trait Selector: Send + Sync + 'static + Debug {
    type Key: StorageKey;
    type Value: StorageValue;
    type BuildHasher: HashBuilder;

    fn select(&self, entry: &CacheEntry<Self::Key, Self::Value, Self::BuildHasher>, buffer: &IoBuffer) -> Selection;
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
    SE: Selector<Key = K, Value = V, BuildHasher = S>,
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

    fn enqueue(
        &self,
        entry: CacheEntry<Self::Key, Self::Value, Self::BuildHasher>,
        buffer: IoBuffer,
        info: KvInfo,
        tx: oneshot::Sender<Result<bool>>,
    ) {
        match self.selector.select(&entry, &buffer) {
            Selection::Left => {
                self.right.delete(entry.key());
                self.left.enqueue(entry, buffer, info, tx);
            }
            Selection::Right => {
                self.right.delete(entry.key());
                self.right.enqueue(entry, buffer, info, tx);
            }
        }
    }

    fn load<Q>(&self, key: &Q) -> impl Future<Output = Result<Option<(Self::Key, Self::Value)>>> + Send + 'static
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Send + Sync + 'static,
    {
        let fleft = self.left.load(key);
        let fright = self.right.load(key);

        async move {
            pin_mut!(fleft);
            pin_mut!(fright);

            // Returns a 4-way `Either` by nesting `Either` in `Either`.
            select(fleft, fright)
                .then(|either| match either {
                    EitherFuture::Left((res, fr)) => match res {
                        Ok(Some(kv)) => ready(Ok(Some(kv))).left_future().left_future(),
                        Err(e) => ready(Err(e)).left_future().left_future(),
                        Ok(None) => fr.right_future().left_future(),
                    },
                    EitherFuture::Right((res, fl)) => match res {
                        Ok(Some(kv)) => ready(Ok(Some(kv))).left_future().right_future(),
                        Err(e) => ready(Err(e)).left_future().right_future(),
                        Ok(None) => fl.right_future().right_future(),
                    },
                })
                .await
        }
    }

    fn delete<Q>(&self, key: &Q) -> WaitHandle<impl Future<Output = Result<bool>> + Send + 'static>
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hleft = self.left.delete(key);
        let hright = self.right.delete(key);
        WaitHandle::new(try_join(hleft, hright).map(|res| res.map(|(l, r)| l || r)))
    }

    fn may_contains<Q>(&self, key: &Q) -> bool
    where
        Self::Key: std::borrow::Borrow<Q>,
        Q: std::hash::Hash + Eq + ?Sized,
    {
        self.left.may_contains(key) || self.right.may_contains(key)
    }

    async fn destroy(&self) -> Result<()> {
        try_join(self.left.destroy(), self.right.destroy()).await?;
        Ok(())
    }

    fn stats(&self) -> std::sync::Arc<DeviceStats> {
        // The two engines share the same device, so it is okay to use either device stats of those.
        self.left.stats()
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
