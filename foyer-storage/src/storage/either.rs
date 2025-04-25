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

use std::{
    fmt::Debug,
    future::{ready, Future},
    pin::pin,
    sync::Arc,
};

use auto_enums::auto_enum;
use foyer_common::{
    code::{StorageKey, StorageValue},
    properties::Properties,
};
use foyer_memory::Piece;
use futures_util::{
    future::{join, select, try_join, Either as EitherFuture},
    FutureExt,
};

use crate::{error::Result, storage::Storage, Load, Statistics, Throttle};

/// Order of ops.
#[derive(Debug, Clone, Copy)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Order {
    /// Use the left engine first.
    ///
    /// If the op does return the expected result, use then right engine then.
    LeftFirst,
    /// Use the right engine first.
    ///
    /// If the op does return the expected result, use then left engine then.
    RightFirst,
    /// Use the left engine and the right engine in parallel.
    ///
    /// If any engine returns the expected result, the future returns immediately.
    Parallel,
}

pub struct EitherConfig<K, V, SL, SR, SE>
where
    K: StorageKey,
    V: StorageValue,
    SL: Storage<Key = K, Value = V>,
    SR: Storage<Key = K, Value = V>,
    SE: Selector<Key = K, Value = V>,
{
    pub selector: SE,
    pub left: SL::Config,
    pub right: SR::Config,
    pub load_order: Order,
}

impl<K, V, SL, SR, SE> Debug for EitherConfig<K, V, SL, SR, SE>
where
    K: StorageKey,
    V: StorageValue,
    SL: Storage<Key = K, Value = V>,
    SR: Storage<Key = K, Value = V>,
    SE: Selector<Key = K, Value = V>,
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
    type Properties: Properties;

    fn select(&self, piece: &Piece<Self::Key, Self::Value, Self::Properties>, estimated_size: usize) -> Selection;
}

pub struct Either<K, V, P, SL, SR, SE>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
    SL: Storage<Key = K, Value = V, Properties = P>,
    SR: Storage<Key = K, Value = V, Properties = P>,
    SE: Selector<Key = K, Value = V, Properties = P>,
{
    selector: Arc<SE>,

    left: SL,
    right: SR,

    load_order: Order,
}

impl<K, V, P, SL, SR, SE> Debug for Either<K, V, P, SL, SR, SE>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
    SL: Storage<Key = K, Value = V, Properties = P>,
    SR: Storage<Key = K, Value = V, Properties = P>,
    SE: Selector<Key = K, Value = V, Properties = P>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EitherStore")
            .field("left", &self.left)
            .field("right", &self.right)
            .finish()
    }
}

impl<K, V, P, SL, SR, SE> Clone for Either<K, V, P, SL, SR, SE>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
    SL: Storage<Key = K, Value = V, Properties = P>,
    SR: Storage<Key = K, Value = V, Properties = P>,
    SE: Selector<Key = K, Value = V, Properties = P>,
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

impl<K, V, P, SL, SR, SE> Storage for Either<K, V, P, SL, SR, SE>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
    SL: Storage<Key = K, Value = V, Properties = P>,
    SR: Storage<Key = K, Value = V, Properties = P>,
    SE: Selector<Key = K, Value = V, Properties = P>,
{
    type Key = K;
    type Value = V;
    type Properties = P;
    type Config = EitherConfig<K, V, SL, SR, SE>;

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

    fn enqueue(&self, piece: Piece<Self::Key, Self::Value, Self::Properties>, estimated_size: usize) {
        match self.selector.select(&piece, estimated_size) {
            Selection::Left => {
                self.right.delete(piece.hash());
                self.left.enqueue(piece, estimated_size);
            }
            Selection::Right => {
                self.right.delete(piece.hash());
                self.right.enqueue(piece, estimated_size);
            }
        }
    }

    #[auto_enum(Future)]
    fn load(&self, hash: u64) -> impl Future<Output = Result<Load<Self::Key, Self::Value>>> + Send + 'static {
        let fleft = self.left.load(hash);
        let fright = self.right.load(hash);
        match self.load_order {
            // FIXME(MrCroxx): false-positive on hash collision.
            Order::LeftFirst => fleft.then(|res| match res {
                Err(e) => ready(Err(e)).left_future(),
                Ok(l @ Load::Entry { .. }) => ready(Ok(l)).left_future(),
                Ok(_) => fright.right_future(),
            }),
            // FIXME(MrCroxx): false-positive on hash collision.
            Order::RightFirst => fright.then(|res| match res {
                Err(e) => ready(Err(e)).left_future(),
                Ok(l @ Load::Entry { .. }) => ready(Ok(l)).left_future(),
                Ok(_) => fleft.right_future(),
            }),
            Order::Parallel => {
                async move {
                    let fleft = pin!(fleft);
                    let fright = pin!(fright);
                    // Returns a 4-way `Either` by nesting `Either` in `Either`.
                    select(fleft, fright)
                        .then(|either| match either {
                            EitherFuture::Left((res, fr)) => match res {
                                Err(e) => ready(Err(e)).left_future().left_future(),
                                Ok(l @ Load::Entry { .. }) => ready(Ok(l)).left_future().left_future(),
                                Ok(_) => fr.right_future().left_future(),
                            },
                            EitherFuture::Right((res, fl)) => match res {
                                Err(e) => ready(Err(e)).left_future().right_future(),
                                Ok(l @ Load::Entry { .. }) => ready(Ok(l)).left_future().right_future(),
                                Ok(_) => fl.right_future().right_future(),
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

    fn throttle(&self) -> &Throttle {
        // The two engines share the same device, so it is okay to use either device stats of those.
        self.left.throttle()
    }

    fn statistics(&self) -> &Arc<Statistics> {
        // The two engines share the same device, so it is okay to use either device stats of those.
        self.left.statistics()
    }

    fn wait(&self) -> impl Future<Output = ()> + Send + 'static {
        join(self.left.wait(), self.right.wait()).map(|_| ())
    }
}
