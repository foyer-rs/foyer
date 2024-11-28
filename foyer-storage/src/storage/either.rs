//  Copyright 2024 foyer Project Authors
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

use std::{fmt::Debug, sync::Arc};

use auto_enums::auto_enum;
use foyer_common::code::{HashBuilder, StorageKey, StorageValue};
use foyer_memory::CacheEntry;
use futures::{
    future::{join, ready, select, try_join, Either as EitherFuture},
    pin_mut, Future, FutureExt,
};
use serde::{Deserialize, Serialize};

use crate::{error::Result, storage::Storage, DeviceStats};

/// Order of ops.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Order {
    /// Use the left engine first.
    ///
    /// If the op does returns the expected result, use then right engine then.
    LeftFirst,
    /// Use the right engine first.
    ///
    /// If the op does returns the expected result, use then left engine then.
    RightFirst,
    /// Use the left engine and the right engine in parallel.
    ///
    /// If any engine returns the expected result, the future returns immediately.
    Parallel,
}

pub struct EitherConfig<K, V, S, SL, SR, SE>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
    SL: Storage<Key = K, Value = V, BuildHasher = S>,
    SR: Storage<Key = K, Value = V, BuildHasher = S>,
    SE: Selector<Key = K, Value = V, BuildHasher = S>,
{
    pub selector: SE,
    pub left: SL::Config,
    pub right: SR::Config,
    pub load_order: Order,
}

impl<K, V, S, SL, SR, SE> Debug for EitherConfig<K, V, S, SL, SR, SE>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
    SL: Storage<Key = K, Value = V, BuildHasher = S>,
    SR: Storage<Key = K, Value = V, BuildHasher = S>,
    SE: Selector<Key = K, Value = V, BuildHasher = S>,
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

    fn select(&self, entry: &CacheEntry<Self::Key, Self::Value, Self::BuildHasher>, estimated_size: usize)
        -> Selection;
}

pub struct Either<K, V, S, SL, SR, SE>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
    SL: Storage<Key = K, Value = V, BuildHasher = S>,
    SR: Storage<Key = K, Value = V, BuildHasher = S>,
    SE: Selector<Key = K, Value = V, BuildHasher = S>,
{
    selector: Arc<SE>,

    left: SL,
    right: SR,

    load_order: Order,
}

impl<K, V, S, SL, SR, SE> Debug for Either<K, V, S, SL, SR, SE>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
    SL: Storage<Key = K, Value = V, BuildHasher = S>,
    SR: Storage<Key = K, Value = V, BuildHasher = S>,
    SE: Selector<Key = K, Value = V, BuildHasher = S>,
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
    SE: Selector<Key = K, Value = V, BuildHasher = S>,
{
    fn clone(&self) -> Self {
        Self {
            selector: self.selector.clone(),
            left: self.left.clone(),
            right: self.right.clone(),
            load_order: self.load_order,
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

        Ok(Self {
            selector,
            left,
            right,
            load_order: config.load_order,
        })
    }

    async fn close(&self) -> Result<()> {
        self.left.close().await?;
        self.right.close().await?;
        Ok(())
    }

    fn enqueue(&self, entry: CacheEntry<Self::Key, Self::Value, Self::BuildHasher>, estimated_size: usize) {
        match self.selector.select(&entry, estimated_size) {
            Selection::Left => {
                self.right.delete(entry.hash());
                self.left.enqueue(entry, estimated_size);
            }
            Selection::Right => {
                self.right.delete(entry.hash());
                self.right.enqueue(entry, estimated_size);
            }
        }
    }

    #[auto_enum(Future)]
    fn load(&self, hash: u64) -> impl Future<Output = Result<Option<(Self::Key, Self::Value)>>> + Send + 'static {
        let fleft = self.left.load(hash);
        let fright = self.right.load(hash);
        match self.load_order {
            // FIXME(MrCroxx): false-positive on hash collision.
            Order::LeftFirst => fleft.then(|res| match res {
                Ok(Some(kv)) => ready(Ok(Some(kv))).left_future(),
                Err(e) => ready(Err(e)).left_future(),
                Ok(None) => fright.right_future(),
            }),
            // FIXME(MrCroxx): false-positive on hash collision.
            Order::RightFirst => fright.then(|res| match res {
                Ok(Some(kv)) => ready(Ok(Some(kv))).left_future(),
                Err(e) => ready(Err(e)).left_future(),
                Ok(None) => fleft.right_future(),
            }),
            Order::Parallel => {
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
        }
    }

    fn delete(&self, hash: u64) {
        self.left.delete(hash);
        self.right.delete(hash);
    }

    fn may_contains(&self, hash: u64) -> bool {
        self.left.may_contains(hash) || self.right.may_contains(hash)
    }

    async fn destroy(&self) -> Result<()> {
        try_join(self.left.destroy(), self.right.destroy()).await?;
        Ok(())
    }

    fn stats(&self) -> std::sync::Arc<DeviceStats> {
        // The two engines share the same device, so it is okay to use either device stats of those.
        self.left.stats()
    }

    fn wait(&self) -> impl Future<Output = ()> + Send + 'static {
        join(self.left.wait(), self.right.wait()).map(|_| ())
    }
}
