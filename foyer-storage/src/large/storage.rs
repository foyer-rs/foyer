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

use std::{
    borrow::Borrow,
    fmt::Debug,
    future::Future,
    hash::{BuildHasher, Hash},
    pin::Pin,
    task::{ready, Context, Poll},
};

use foyer_common::code::{StorageKey, StorageValue};
use foyer_memory::{CacheEntry, DefaultCacheEventListener};
use pin_project::pin_project;
use tokio::sync::oneshot;

use crate::error::Result;

#[pin_project]
pub struct EnqueueHandle {
    #[pin]
    rx: oneshot::Receiver<Result<bool>>,
}

impl EnqueueHandle {
    pub(crate) fn new(rx: oneshot::Receiver<Result<bool>>) -> Self {
        Self { rx }
    }
}

impl Future for EnqueueHandle {
    type Output = Result<bool>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let res = ready!(self.project().rx.poll(cx));
        Poll::Ready(res.unwrap())
    }
}

// TODO(MrCroxx): Remove this after in-memory cache event listener is removed.
#[allow(clippy::type_complexity)]
pub trait Storage: Send + Sync + 'static + Clone {
    type Key: StorageKey;
    type Value: StorageValue;
    type BuildHasher: BuildHasher + Send + Sync + 'static;
    type Config: Send + Debug + 'static;

    #[must_use]
    fn open(config: Self::Config) -> impl Future<Output = Result<Self>> + Send + 'static;

    #[must_use]
    fn close(&self) -> impl Future<Output = Result<()>> + Send;

    fn enqueue(
        &self,
        entry: CacheEntry<Self::Key, Self::Value, DefaultCacheEventListener<Self::Key, Self::Value>, Self::BuildHasher>,
    ) -> EnqueueHandle;

    #[must_use]
    fn lookup<Q>(&self, key: &Q) -> impl Future<Output = Result<Option<Self::Value>>> + Send
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Send + Sync + 'static;

    fn remove<Q>(&self, key: &Q)
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Send + Sync + 'static;

    #[must_use]
    fn delete<Q>(&self, key: &Q) -> impl Future<Output = Result<()>> + Send
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Send + Sync + 'static;

    // TODO(MrCroxx): clear & destroy
}
