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
    hash::Hash,
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
};

use foyer_common::code::{HashBuilder, StorageKey, StorageValue};
use foyer_memory::CacheEntry;
use pin_project::pin_project;
use tokio::{runtime::Handle, sync::oneshot};

use crate::{device::monitor::DeviceStats, error::Result};

/// The handle created by [`Storage::enqueue`].
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

/// The storage trait for the disk cache storage engine.
// TODO(MrCroxx): Remove this after in-memory cache event listener is removed.
#[allow(clippy::type_complexity)]
pub trait Storage: Send + Sync + 'static + Clone + Debug {
    /// Disk cache key type.
    type Key: StorageKey;
    /// Disk cache value type.
    type Value: StorageValue;
    /// Disk cache hash builder type.
    type BuildHasher: HashBuilder;
    /// Disk cache config type.
    type Config: Send + Debug + 'static;

    /// Open the disk cache with the given configurations.
    #[must_use]
    fn open(config: Self::Config) -> impl Future<Output = Result<Self>> + Send + 'static;

    /// Close the disk cache gracefully.
    ///
    /// `close` will wait for all ongoing flush and reclaim tasks to finish.
    #[must_use]
    fn close(&self) -> impl Future<Output = Result<()>> + Send;

    /// Return if the given key can be picked by the admission picker.
    fn pick(&self, key: &Self::Key) -> bool;

    /// Push a in-memory cache entry to the disk cache write queue.
    fn enqueue(&self, entry: CacheEntry<Self::Key, Self::Value, Self::BuildHasher>, force: bool) -> EnqueueHandle;

    /// Load a cache entry from the disk cache.
    ///
    /// `load` may return a false-positive result on entry key hash collision. It's the caller's responsibility to
    /// check if the returned key matches the given key.
    #[must_use]
    fn load<Q>(&self, key: &Q) -> impl Future<Output = Result<Option<(Self::Key, Self::Value)>>> + Send + 'static
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Send + Sync + 'static;

    /// Delete the cache entry with the given key from the disk cache.
    fn delete<Q>(&self, key: &Q) -> EnqueueHandle
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized;

    /// Check if the disk cache contains a cached entry with the given key.
    ///
    /// `contains` may return a false-positive result if there is a hash collision with the given key.
    fn may_contains<Q>(&self, key: &Q) -> bool
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized;

    /// Delete all cached entries of the disk cache.
    #[must_use]
    fn destroy(&self) -> impl Future<Output = Result<()>> + Send;

    /// Get the statistics information of the disk cache.
    fn stats(&self) -> Arc<DeviceStats>;

    /// Wait for the ongoing flush and reclaim tasks to finish.
    #[must_use]
    fn wait(&self) -> impl Future<Output = Result<()>> + Send;

    /// Get disk cache runtime handle.
    ///
    /// The runtime is determined during the opening phase.
    fn runtime(&self) -> &Handle;
}
