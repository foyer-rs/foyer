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
    future::Future,
    hash::Hash,
    ops::Deref,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{ready, Context, Poll},
    time::Instant,
};

use ahash::RandomState;
use equivalent::Equivalent;
use fastrace::prelude::*;
use foyer_common::{
    code::{HashBuilder, StorageKey, StorageValue},
    future::Diversion,
    location::CacheLocation,
    metrics::Metrics,
    tracing::{InRootSpan, TracingConfig, TracingOptions},
};
use foyer_memory::{Cache, CacheEntry, CacheHint, Fetch, FetchMark, FetchState, Piece, Pipe};
use foyer_storage::{Load, Statistics, Store};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use super::writer::HybridCacheStorageWriter;
use crate::HybridCacheWriter;

macro_rules! root_span {
    ($self:ident, mut $name:ident, $label:expr) => {
        root_span!($self, (mut) $name, $label)
    };
    ($self:ident, $name:ident, $label:expr) => {
        root_span!($self, () $name, $label)
    };
    ($self:ident, ($($mut:tt)?) $name:ident, $label:expr) => {
        let $name = if $self.tracing.load(std::sync::atomic::Ordering::Relaxed) {
            Span::root($label, SpanContext::random())
        } else {
            Span::noop()
        };
    };
}

macro_rules! try_cancel {
    ($self:ident, $span:ident, $threshold:ident) => {
        if let Some(elapsed) = $span.elapsed() {
            if elapsed < $self.tracing_config.$threshold() {
                $span.cancel();
            }
        }
    };
}

/// Control the cache policy of the hybrid cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HybridCachePolicy {
    /// Write disk cache on entry eviction. (Default)
    WriteOnEviction,
    /// Write disk cache on entry insertion.
    WriteOnInsertion,
}

impl Default for HybridCachePolicy {
    fn default() -> Self {
        Self::WriteOnEviction
    }
}

pub struct HybridCachePipe<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    store: Store<K, V, S>,
}

impl<K, V, S> HybridCachePipe<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    pub fn new(store: Store<K, V, S>) -> Self {
        Self { store }
    }
}

impl<K, V, S> Pipe for HybridCachePipe<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    type Key = K;
    type Value = V;

    fn is_enabled(&self) -> bool {
        true
    }

    fn send(&self, piece: Piece<Self::Key, Self::Value>) {
        match piece.location() {
            CacheLocation::InMem => return,
            CacheLocation::Default | CacheLocation::OnDisk => {}
        }
        self.store.enqueue(piece, false);
    }
}

/// A cached entry holder of the hybrid cache.
pub type HybridCacheEntry<K, V, S = RandomState> = CacheEntry<K, V, S>;

/// Hybrid cache that integrates in-memory cache and disk cache.
///
/// # NOTE
///
/// Please be careful not to create a circular reference between `memory` and `storage`.
///
/// Currently:
///
/// ```text
/// memory => pipe => storage
/// ```
///
/// So, `storage` must not hold the reference of `memory`.
pub struct HybridCache<K, V, S = RandomState>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    policy: HybridCachePolicy,
    memory: Cache<K, V, S>,
    storage: Store<K, V, S>,
    metrics: Arc<Metrics>,
    tracing_config: Arc<TracingConfig>,
    tracing: Arc<AtomicBool>,
}

impl<K, V, S> Debug for HybridCache<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HybridCache")
            .field("memory", &self.memory)
            .field("storage", &self.storage)
            .field("tracing_config", &self.tracing_config)
            .field("tracing", &self.tracing)
            .finish()
    }
}

impl<K, V, S> Clone for HybridCache<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn clone(&self) -> Self {
        Self {
            policy: self.policy,
            memory: self.memory.clone(),
            storage: self.storage.clone(),
            metrics: self.metrics.clone(),
            tracing_config: self.tracing_config.clone(),
            tracing: self.tracing.clone(),
        }
    }
}

impl<K, V, S> HybridCache<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    pub(crate) fn new(
        policy: HybridCachePolicy,
        memory: Cache<K, V, S>,
        storage: Store<K, V, S>,
        tracing_options: TracingOptions,
        metrics: Arc<Metrics>,
    ) -> Self {
        let tracing_config = Arc::<TracingConfig>::default();
        tracing_config.update(tracing_options);
        let tracing = Arc::new(AtomicBool::new(false));
        Self {
            policy,
            memory,
            storage,
            metrics,
            tracing_config,
            tracing,
        }
    }

    /// Access the trace config with options.
    pub fn update_tracing_options(&self, options: TracingOptions) {
        self.tracing_config.update(options);
    }

    /// Access the in-memory cache.
    pub fn memory(&self) -> &Cache<K, V, S> {
        &self.memory
    }

    /// Enable tracing.
    pub fn enable_tracing(&self) {
        self.tracing.store(true, Ordering::Relaxed);
    }

    /// Disable tracing.
    pub fn disable_tracing(&self) {
        self.tracing.store(true, Ordering::Relaxed);
    }

    /// Return `true` if tracing is enabled.
    pub fn is_tracing_enabled(&self) -> bool {
        self.tracing.load(Ordering::Relaxed)
    }

    /// Insert cache entry to the hybrid cache.
    pub fn insert(&self, key: K, value: V) -> HybridCacheEntry<K, V, S> {
        root_span!(self, span, "foyer::hybrid::cache::insert");

        let _guard = span.set_local_parent();

        let now = Instant::now();

        let entry = self.memory.insert(key, value);
        if self.policy == HybridCachePolicy::WriteOnInsertion {
            self.storage.enqueue(entry.piece(), false);
        }

        self.metrics.hybrid_insert.increase(1);
        self.metrics.hybrid_insert_duration.record(now.elapsed().as_secs_f64());

        try_cancel!(self, span, record_hybrid_insert_threshold);

        entry
    }

    /// Insert cache entry with cache hint to the hybrid cache.
    pub fn insert_with_hint(&self, key: K, value: V, hint: CacheHint) -> HybridCacheEntry<K, V, S> {
        root_span!(self, span, "foyer::hybrid::cache::insert_with_hint");

        let _guard = span.set_local_parent();

        let now = Instant::now();

        let entry = self.memory.insert_with_hint(key, value, hint);
        if self.policy == HybridCachePolicy::WriteOnInsertion {
            self.storage.enqueue(entry.piece(), false);
        }

        self.metrics.hybrid_insert.increase(1);
        self.metrics.hybrid_insert_duration.record(now.elapsed().as_secs_f64());

        try_cancel!(self, span, record_hybrid_insert_threshold);

        entry
    }

    /// Insert cache entry with preferred location to the hybrid cache.
    pub fn insert_with_location(&self, key: K, value: V, location: CacheLocation) -> HybridCacheEntry<K, V, S> {
        root_span!(self, span, "foyer::hybrid::cache::insert_with_location");

        let _guard = span.set_local_parent();

        let now = Instant::now();

        let entry = self.memory.insert_with_location(key, value, location);
        if self.policy == HybridCachePolicy::WriteOnInsertion && location != CacheLocation::InMem {
            self.storage.enqueue(entry.piece(), false);
        }

        self.metrics.hybrid_insert.increase(1);
        self.metrics.hybrid_insert_duration.record(now.elapsed().as_secs_f64());

        try_cancel!(self, span, record_hybrid_insert_threshold);

        entry
    }

    /// Get cached entry with the given key from the hybrid cache.
    pub async fn get<Q>(&self, key: &Q) -> anyhow::Result<Option<HybridCacheEntry<K, V, S>>>
    where
        Q: Hash + Equivalent<K> + Send + Sync + 'static + Clone,
    {
        root_span!(self, span, "foyer::hybrid::cache::get");

        let now = Instant::now();

        let record_hit = || {
            self.metrics.hybrid_hit.increase(1);
            self.metrics.hybrid_hit_duration.record(now.elapsed().as_secs_f64());
        };
        let record_miss = || {
            self.metrics.hybrid_miss.increase(1);
            self.metrics.hybrid_miss_duration.record(now.elapsed().as_secs_f64());
        };
        let record_throttled = || {
            self.metrics.hybrid_throttled.increase(1);
            self.metrics
                .hybrid_throttled_duration
                .record(now.elapsed().as_secs_f64());
        };

        let guard = span.set_local_parent();
        if let Some(entry) = self.memory.get(key) {
            record_hit();
            try_cancel!(self, span, record_hybrid_get_threshold);
            return Ok(Some(entry));
        }
        drop(guard);

        let entry = match self
            .storage
            .load(key)
            .in_span(Span::enter_with_parent("foyer::hybrid::cache::get::poll", &span))
            .await?
        {
            Load::Entry { key, value } => {
                record_hit();
                Some(self.memory.insert(key, value))
            }
            Load::Throttled => {
                record_throttled();
                None
            }
            Load::Miss => {
                record_miss();
                None
            }
        };

        try_cancel!(self, span, record_hybrid_get_threshold);

        Ok(entry)
    }

    /// Get cached entry with the given key from the hybrid cache.
    ///
    /// Different from `get`, `obtain` deduplicates the disk cache queries.
    ///
    /// `obtain` is always supposed to be used instead of `get` if the overhead of getting the ownership of the given
    /// key is acceptable.
    pub async fn obtain(&self, key: K) -> anyhow::Result<Option<HybridCacheEntry<K, V, S>>>
    where
        K: Clone,
    {
        root_span!(self, span, "foyer::hybrid::cache::obtain");

        let now = Instant::now();

        let guard = span.set_local_parent();
        let fetch = self.memory.fetch(key.clone(), || {
            let store = self.storage.clone();
            async move {
                match store.load(&key).await.map_err(anyhow::Error::from) {
                    Ok(Load::Entry { key: _, value }) => Ok(value),
                    Ok(Load::Throttled) => Err(ObtainFetchError::Throttled),
                    Ok(Load::Miss) => Err(ObtainFetchError::NotExist),
                    Err(e) => Err(ObtainFetchError::Err(e)),
                }
            }
        });
        drop(guard);

        let res = fetch.await;

        match res {
            Ok(entry) => {
                self.metrics.hybrid_hit.increase(1);
                self.metrics.hybrid_hit_duration.record(now.elapsed().as_secs_f64());
                try_cancel!(self, span, record_hybrid_obtain_threshold);
                Ok(Some(entry))
            }
            Err(ObtainFetchError::Throttled) => {
                self.metrics.hybrid_throttled.increase(1);
                self.metrics
                    .hybrid_throttled_duration
                    .record(now.elapsed().as_secs_f64());
                try_cancel!(self, span, record_hybrid_obtain_threshold);
                Ok(None)
            }
            Err(ObtainFetchError::NotExist) => {
                self.metrics.hybrid_miss.increase(1);
                self.metrics.hybrid_miss_duration.record(now.elapsed().as_secs_f64());
                try_cancel!(self, span, record_hybrid_obtain_threshold);
                Ok(None)
            }
            Err(ObtainFetchError::RecvError(_)) => {
                try_cancel!(self, span, record_hybrid_obtain_threshold);
                Ok(None)
            }
            Err(ObtainFetchError::Err(e)) => {
                try_cancel!(self, span, record_hybrid_obtain_threshold);
                Err(e)
            }
        }
    }

    /// Remove a cached entry with the given key from the hybrid cache.
    pub fn remove<Q>(&self, key: &Q)
    where
        Q: Hash + Equivalent<K> + ?Sized + Send + Sync + 'static,
    {
        root_span!(self, span, "foyer::hybrid::cache::remove");

        let _guard = span.set_local_parent();

        let now = Instant::now();

        self.memory.remove(key);
        self.storage.delete(key);

        self.metrics.hybrid_remove.increase(1);
        self.metrics.hybrid_remove_duration.record(now.elapsed().as_secs_f64());

        try_cancel!(self, span, record_hybrid_remove_threshold);
    }

    /// Check if the hybrid cache contains a cached entry with the given key.
    ///
    /// `contains` may return a false-positive result if there is a hash collision with the given key.
    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        Q: Hash + Equivalent<K> + ?Sized,
    {
        self.memory.contains(key) || self.storage.may_contains(key)
    }

    /// Clear the hybrid cache.
    pub async fn clear(&self) -> anyhow::Result<()> {
        self.memory.clear();
        self.storage.destroy().await?;
        Ok(())
    }

    /// Gracefully close the hybrid cache.
    ///
    /// `close` will wait for the ongoing flush and reclaim tasks to finish.
    pub async fn close(&self) -> anyhow::Result<()> {
        self.storage.close().await?;
        Ok(())
    }

    /// Return the statistics information of the hybrid cache.
    pub fn statistics(&self) -> &Arc<Statistics> {
        self.storage.statistics()
    }

    /// Create a new [`HybridCacheWriter`].
    pub fn writer(&self, key: K) -> HybridCacheWriter<K, V, S> {
        HybridCacheWriter::new(self.clone(), key)
    }

    /// Create a new [`HybridCacheStorageWriter`].
    pub fn storage_writer(&self, key: K) -> HybridCacheStorageWriter<K, V, S> {
        HybridCacheStorageWriter::new(self.clone(), key)
    }

    pub(crate) fn storage(&self) -> &Store<K, V, S> {
        &self.storage
    }

    pub(crate) fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }
}

#[derive(Debug)]
enum ObtainFetchError {
    Throttled,
    NotExist,
    RecvError(oneshot::error::RecvError),
    Err(anyhow::Error),
}

impl From<oneshot::error::RecvError> for ObtainFetchError {
    fn from(e: oneshot::error::RecvError) -> Self {
        Self::RecvError(e)
    }
}

/// The future generated by [`HybridCache::fetch`].
pub type HybridFetch<K, V, S = RandomState> = InRootSpan<HybridFetchInner<K, V, S>>;

/// A future that is used to get entry value from the remote storage for the hybrid cache.
#[pin_project]
pub struct HybridFetchInner<K, V, S = RandomState>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    #[pin]
    inner: Fetch<K, V, anyhow::Error, S>,
    policy: HybridCachePolicy,
    storage: Store<K, V, S>,
}

impl<K, V, S> Future for HybridFetchInner<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    type Output = anyhow::Result<CacheEntry<K, V, S>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        let res = ready!(this.inner.as_mut().poll(cx));

        if let Ok(entry) = res.as_ref() {
            if entry.location() != CacheLocation::InMem
                && *this.policy == HybridCachePolicy::WriteOnInsertion
                && this.inner.store().is_some()
            {
                this.storage.enqueue(entry.piece(), false);
            }
        }

        Poll::Ready(res)
    }
}

impl<K, V, S> Deref for HybridFetchInner<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    type Target = Fetch<K, V, anyhow::Error, S>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<K, V, S> HybridCache<K, V, S>
where
    K: StorageKey + Clone,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    /// Fetch and insert a cache entry with the given key and method if there is a cache miss.
    ///
    /// If the dedicated runtime of the foyer storage engine is enabled, `fetch` will spawn task with the dedicated
    /// runtime. Otherwise, the user's runtime will be used.
    pub fn fetch<F, FU>(&self, key: K, fetch: F) -> HybridFetch<K, V, S>
    where
        F: FnOnce() -> FU,
        FU: Future<Output = anyhow::Result<V>> + Send + 'static,
    {
        self.fetch_with_hint(key, CacheHint::Normal, fetch)
    }

    /// Fetch and insert a cache entry with the given key, hint, and method if there is a cache miss.
    ///
    /// If the dedicated runtime of the foyer storage engine is enabled, `fetch` will spawn task with the dedicated
    /// runtime. Otherwise, the user's runtime will be used.
    pub fn fetch_with_hint<F, FU>(&self, key: K, hint: CacheHint, fetch: F) -> HybridFetch<K, V, S>
    where
        F: FnOnce() -> FU,
        FU: Future<Output = anyhow::Result<V>> + Send + 'static,
    {
        root_span!(self, span, "foyer::hybrid::cache::fetch");

        let _guard = span.set_local_parent();

        let now = Instant::now();

        let store = self.storage.clone();

        let future = fetch();
        let inner = self.memory.fetch_inner(
            key.clone(),
            hint,
            || {
                let metrics = self.metrics.clone();
                let runtime = self.storage().runtime().clone();

                async move {
                    let refill = match store.load(&key).await.map_err(anyhow::Error::from) {
                        Ok(Load::Entry { key: _, value }) => {
                            metrics.hybrid_hit.increase(1);
                            metrics.hybrid_hit_duration.record(now.elapsed().as_secs_f64());
                            return Ok(value).into();
                        }
                        Ok(Load::Throttled) => false,
                        Ok(Load::Miss) => true,
                        Err(e) => return Err(e).into(),
                    };

                    metrics.hybrid_miss.increase(1);
                    metrics.hybrid_miss_duration.record(now.elapsed().as_secs_f64());

                    let fut = async move {
                        Diversion {
                            target: future.await,
                            store: Some(FetchMark),
                        }
                    }
                    .in_span(Span::enter_with_local_parent("foyer::hybrid::fetch::fn"));

                    runtime.user().spawn(fut).await.unwrap()
                }
            },
            self.storage().runtime().read(),
        );

        if inner.state() == FetchState::Hit {
            self.metrics.hybrid_hit.increase(1);
            self.metrics.hybrid_hit_duration.record(now.elapsed().as_secs_f64());
        }

        let inner = HybridFetchInner {
            inner,
            policy: self.policy,
            storage: self.storage.clone(),
        };

        InRootSpan::new(inner, span).with_threshold(self.tracing_config.record_hybrid_fetch_threshold())
    }
}

#[cfg(test)]
mod tests {

    use std::{path::Path, sync::Arc};

    use foyer_common::{hasher::ModRandomState, location::CacheLocation};
    use storage::test_utils::BiasedPicker;

    use crate::*;

    const KB: usize = 1024;
    const MB: usize = 1024 * 1024;

    async fn open(dir: impl AsRef<Path>) -> HybridCache<u64, Vec<u8>, ModRandomState> {
        HybridCacheBuilder::new()
            .with_name("test")
            .memory(4 * MB)
            .with_hash_builder(ModRandomState::default())
            // TODO(MrCroxx): Test with `Engine::Mixed`.
            .storage(Engine::Large)
            .with_device_options(
                DirectFsDeviceOptions::new(dir)
                    .with_capacity(16 * MB)
                    .with_file_size(MB),
            )
            .build()
            .await
            .unwrap()
    }

    async fn open_with_biased_admission_picker(
        dir: impl AsRef<Path>,
        admits: impl IntoIterator<Item = u64>,
    ) -> HybridCache<u64, Vec<u8>, ModRandomState> {
        HybridCacheBuilder::new()
            .with_name("test")
            .memory(4 * MB)
            .with_hash_builder(ModRandomState::default())
            // TODO(MrCroxx): Test with `Engine::Mixed`.
            .storage(Engine::Large)
            .with_device_options(
                DirectFsDeviceOptions::new(dir)
                    .with_capacity(16 * MB)
                    .with_file_size(MB),
            )
            .with_admission_picker(Arc::new(BiasedPicker::new(admits)))
            .build()
            .await
            .unwrap()
    }

    async fn open_with_policy(
        dir: impl AsRef<Path>,
        policy: HybridCachePolicy,
    ) -> HybridCache<u64, Vec<u8>, ModRandomState> {
        HybridCacheBuilder::new()
            .with_name("test")
            .with_policy(policy)
            .memory(4 * MB)
            .with_hash_builder(ModRandomState::default())
            // TODO(MrCroxx): Test with `Engine::Mixed`.
            .storage(Engine::Large)
            .with_device_options(
                DirectFsDeviceOptions::new(dir)
                    .with_capacity(16 * MB)
                    .with_file_size(MB),
            )
            .build()
            .await
            .unwrap()
    }

    #[test_log::test(tokio::test)]
    async fn test_hybrid_cache() {
        let dir = tempfile::tempdir().unwrap();

        let hybrid = open(dir.path()).await;

        let e1 = hybrid.insert(1, vec![1; 7 * KB]);
        let e2 = hybrid.insert_with_hint(2, vec![2; 7 * KB], CacheHint::Normal);
        assert_eq!(e1.value(), &vec![1; 7 * KB]);
        assert_eq!(e2.value(), &vec![2; 7 * KB]);

        let e3 = hybrid.storage_writer(3).insert(vec![3; 7 * KB]).unwrap();
        let e4 = hybrid
            .storage_writer(4)
            .insert_with_context(vec![4; 7 * KB], CacheHint::Normal)
            .unwrap();
        assert_eq!(e3.value(), &vec![3; 7 * KB]);
        assert_eq!(e4.value(), &vec![4; 7 * KB]);

        let e5 = hybrid.fetch(5, || async move { Ok(vec![5; 7 * KB]) }).await.unwrap();
        assert_eq!(e5.value(), &vec![5; 7 * KB]);

        let e1g = hybrid.get(&1).await.unwrap().unwrap();
        assert_eq!(e1g.value(), &vec![1; 7 * KB]);
        let e2g = hybrid.obtain(2).await.unwrap().unwrap();
        assert_eq!(e2g.value(), &vec![2; 7 * KB]);

        assert!(hybrid.contains(&3));
        hybrid.remove(&3);
        assert!(!hybrid.contains(&3));

        assert!(hybrid.contains(&4));
        hybrid.clear().await.unwrap();
        assert!(!hybrid.contains(&4));
    }

    #[test_log::test(tokio::test)]
    async fn test_hybrid_cache_writer() {
        let dir = tempfile::tempdir().unwrap();

        let hybrid = open_with_biased_admission_picker(dir.path(), [1, 2, 3, 4]).await;

        let e1 = hybrid.writer(1).insert(vec![1; 7 * KB]);
        let e2 = hybrid.writer(2).insert_with_hint(vec![2; 7 * KB], CacheHint::Normal);

        assert_eq!(e1.value(), &vec![1; 7 * KB]);
        assert_eq!(e2.value(), &vec![2; 7 * KB]);

        let e3 = hybrid.writer(3).storage().insert(vec![3; 7 * KB]).unwrap();
        let e4 = hybrid.writer(4).insert_with_hint(vec![4; 7 * KB], CacheHint::Normal);

        assert_eq!(e3.value(), &vec![3; 7 * KB]);
        assert_eq!(e4.value(), &vec![4; 7 * KB]);

        let r5 = hybrid.writer(5).storage().insert(vec![5; 7 * KB]);
        assert!(r5.is_none());

        let e5 = hybrid.writer(5).storage().force().insert(vec![5; 7 * KB]).unwrap();
        assert_eq!(e5.value(), &vec![5; 7 * KB]);
    }

    #[test_log::test(tokio::test)]
    async fn test_hybrid_cache_location() {
        // Test hybrid cache that write disk cache on eviction.

        let dir = tempfile::tempdir().unwrap();
        let hybrid = open_with_policy(dir.path(), HybridCachePolicy::WriteOnEviction).await;

        hybrid.insert_with_location(1, vec![1; 7 * KB], CacheLocation::Default);
        assert_eq!(hybrid.memory().get(&1).unwrap().value(), &vec![1; 7 * KB]);
        hybrid.memory().evict_all();
        hybrid.storage().wait().await;
        assert_eq!(hybrid.storage().load(&1).await.unwrap().unwrap().1, vec![1; 7 * KB]);

        hybrid.insert_with_location(2, vec![2; 7 * KB], CacheLocation::InMem);
        assert_eq!(hybrid.memory().get(&2).unwrap().value(), &vec![2; 7 * KB]);
        hybrid.memory().evict_all();
        hybrid.storage().wait().await;
        assert!(hybrid.storage().load(&2).await.unwrap().is_none());

        // Test hybrid cache that write disk cache on insertion.

        let dir = tempfile::tempdir().unwrap();
        let hybrid = open_with_policy(dir.path(), HybridCachePolicy::WriteOnInsertion).await;

        hybrid.insert_with_location(1, vec![1; 7 * KB], CacheLocation::Default);
        hybrid.storage().wait().await;
        assert_eq!(hybrid.memory().get(&1).unwrap().value(), &vec![1; 7 * KB]);
        assert_eq!(hybrid.storage().load(&1).await.unwrap().unwrap().1, vec![1; 7 * KB]);

        hybrid.insert_with_location(2, vec![2; 7 * KB], CacheLocation::InMem);
        hybrid.storage().wait().await;
        assert_eq!(hybrid.memory().get(&2).unwrap().value(), &vec![2; 7 * KB]);
        assert!(hybrid.storage().load(&2).await.unwrap().is_none());
    }
}
