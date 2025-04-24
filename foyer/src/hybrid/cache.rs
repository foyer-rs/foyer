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
    metrics::Metrics,
    properties::{Hint, Location, Properties, Source},
    tracing::{InRootSpan, TracingConfig, TracingOptions},
};
use foyer_memory::{Cache, CacheEntry, Fetch, FetchContext, FetchState, Piece, Pipe};
use foyer_storage::{IoThrottler, Load, Statistics, Store};
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

/// Entry properties for in-memory only cache.
#[derive(Debug, Clone, Default)]
pub struct HybridCacheProperties {
    ephemeral: bool,
    hint: Hint,
    location: Location,
    source: Source,
}

impl HybridCacheProperties {
    /// Set the entry to be ephemeral.
    ///
    /// An ephemeral entry will be evicted immediately after all its holders drop it,
    /// no matter if the capacity is reached.
    pub fn with_ephemeral(mut self, ephemeral: bool) -> Self {
        self.ephemeral = ephemeral;
        self
    }

    /// Get if the entry is ephemeral.
    pub fn ephemeral(&self) -> bool {
        self.ephemeral
    }

    /// Set entry hint.
    pub fn with_hint(mut self, hint: Hint) -> Self {
        self.hint = hint;
        self
    }

    /// Get entry hint.
    pub fn hint(&self) -> Hint {
        self.hint
    }

    /// Set entry location advice.
    pub fn with_location(mut self, location: Location) -> Self {
        self.location = location;
        self
    }

    /// Get entry location advice.
    pub fn location(&self) -> Location {
        self.location
    }

    /// Get entry source.
    pub fn source(&self) -> Source {
        self.source
    }
}

impl Properties for HybridCacheProperties {
    fn with_ephemeral(self, ephemeral: bool) -> Self {
        self.with_ephemeral(ephemeral)
    }

    fn ephemeral(&self) -> Option<bool> {
        Some(self.ephemeral())
    }

    fn with_hint(self, hint: Hint) -> Self {
        self.with_hint(hint)
    }

    fn hint(&self) -> Option<Hint> {
        Some(self.hint())
    }

    fn with_location(self, location: Location) -> Self {
        self.with_location(location)
    }

    fn location(&self) -> Option<Location> {
        Some(self.location())
    }

    fn with_source(mut self, source: Source) -> Self {
        self.source = source;
        self
    }

    fn source(&self) -> Option<Source> {
        Some(self.source())
    }
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
    store: Store<K, V, S, HybridCacheProperties>,
}

impl<K, V, S> Debug for HybridCachePipe<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HybridCachePipe").finish()
    }
}

impl<K, V, S> HybridCachePipe<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    pub fn new(store: Store<K, V, S, HybridCacheProperties>) -> Self {
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
    type Properties = HybridCacheProperties;

    fn is_enabled(&self) -> bool {
        true
    }

    fn send(&self, piece: Piece<Self::Key, Self::Value, HybridCacheProperties>) {
        match piece.properties().location() {
            Location::InMem => return,
            Location::Default | Location::OnDisk => {}
        }
        self.store.enqueue(piece, false);
    }

    fn flush(
        &self,
        pieces: Vec<Piece<Self::Key, Self::Value, HybridCacheProperties>>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let throttle = self.store.throttle().clone();
        let store = self.store.clone();
        Box::pin(async move {
            store.wait().await;
            let throttler = IoThrottler::new(throttle.write_throughput, throttle.write_iops);
            for piece in pieces {
                let bytes = store.entry_estimated_size(piece.key(), piece.value());
                let ios = throttle.iops_counter.count(bytes);
                let wait = throttler.consume(bytes as _, ios as _);
                if !wait.is_zero() {
                    tokio::time::sleep(wait).await
                }
                store.enqueue(piece, false);
            }
        })
    }
}

/// A cached entry holder of the hybrid cache.
pub type HybridCacheEntry<K, V, S = RandomState> = CacheEntry<K, V, S, HybridCacheProperties>;

#[derive(Debug)]
pub struct HybridCacheOptions {
    pub policy: HybridCachePolicy,
    pub tracing_options: TracingOptions,
    pub flush_on_close: bool,
}

impl Default for HybridCacheOptions {
    fn default() -> Self {
        Self {
            policy: HybridCachePolicy::default(),
            tracing_options: TracingOptions::default(),
            flush_on_close: true,
        }
    }
}

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
    flush_on_close: bool,
    metrics: Arc<Metrics>,
    tracing_config: Arc<TracingConfig>,
    tracing: Arc<AtomicBool>,
    memory: Cache<K, V, S, HybridCacheProperties>,
    storage: Store<K, V, S, HybridCacheProperties>,
}

impl<K, V, S> Debug for HybridCache<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HybridCache")
            .field("policy", &self.policy)
            .field("flush_on_close", &self.flush_on_close)
            .field("tracing", &self.tracing)
            .field("tracing_config", &self.tracing_config)
            .field("memory", &self.memory)
            .field("storage", &self.storage)
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
            flush_on_close: self.flush_on_close,
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
        options: HybridCacheOptions,
        memory: Cache<K, V, S, HybridCacheProperties>,
        storage: Store<K, V, S, HybridCacheProperties>,
        metrics: Arc<Metrics>,
    ) -> Self {
        let policy = options.policy;
        let flush_on_close = options.flush_on_close;
        let tracing_config = Arc::<TracingConfig>::default();
        tracing_config.update(options.tracing_options);
        let tracing = Arc::new(AtomicBool::new(false));
        Self {
            policy,
            flush_on_close,
            memory,
            storage,
            metrics,
            tracing_config,
            tracing,
        }
    }

    /// Get the hybrid cache policy
    pub fn policy(&self) -> HybridCachePolicy {
        self.policy
    }

    /// Access the trace config with options.
    pub fn update_tracing_options(&self, options: TracingOptions) {
        self.tracing_config.update(options);
    }

    /// Access the in-memory cache.
    pub fn memory(&self) -> &Cache<K, V, S, HybridCacheProperties> {
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

    /// Insert cache entry to the hybrid cache with properties.
    pub fn insert_with_properties(
        &self,
        key: K,
        value: V,
        properties: HybridCacheProperties,
    ) -> HybridCacheEntry<K, V, S> {
        root_span!(self, span, "foyer::hybrid::cache::insert");

        let _guard = span.set_local_parent();

        let now = Instant::now();

        let ephemeral = matches! { properties.location(), Location::OnDisk };
        let entry = self
            .memory
            .insert_with_properties(key, value, properties.with_ephemeral(ephemeral));
        if self.policy == HybridCachePolicy::WriteOnInsertion && entry.properties().location() != Location::InMem {
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
    ///
    /// Based on the `flush_on_close` option, `close` will flush all in-memory cached entries to the disk cache.
    ///
    /// For more details, please refer to [`super::builder::HybridCacheBuilder::with_flush_on_close()`].
    pub async fn close(&self) -> anyhow::Result<()> {
        let now = Instant::now();
        if self.flush_on_close {
            let bytes = self.memory.usage();
            tracing::info!(bytes, "[hybrid]: flush all in-memory cached entries to disk on close");
            self.memory.flush().await;
        }
        self.storage.close().await?;
        let elapsed = now.elapsed();
        tracing::info!("[hybrid]: close consumes {elapsed:?}",);
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

    pub(crate) fn storage(&self) -> &Store<K, V, S, HybridCacheProperties> {
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
    inner: Fetch<K, V, anyhow::Error, S, HybridCacheProperties>,
    policy: HybridCachePolicy,
    storage: Store<K, V, S, HybridCacheProperties>,
}

impl<K, V, S> Future for HybridFetchInner<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    type Output = anyhow::Result<CacheEntry<K, V, S, HybridCacheProperties>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        let res = ready!(this.inner.as_mut().poll(cx));

        if let Ok(entry) = res.as_ref() {
            if entry.properties().location() != Location::InMem
                && *this.policy == HybridCachePolicy::WriteOnInsertion
                && this.inner.store().is_some()
            {
                let throttled = this.inner.store().as_ref().unwrap().throttled;
                if !throttled {
                    this.storage.enqueue(entry.piece(), false);
                }
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
    type Target = Fetch<K, V, anyhow::Error, S, HybridCacheProperties>;

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
        self.fetch_inner(key, HybridCacheProperties::default(), fetch)
    }

    /// Fetch and insert a cache entry with the given key, properties, and method if there is a cache miss.
    ///
    /// If the dedicated runtime of the foyer storage engine is enabled, `fetch` will spawn task with the dedicated
    /// runtime. Otherwise, the user's runtime will be used.
    pub fn fetch_with_properties<F, FU>(
        &self,
        key: K,
        properties: HybridCacheProperties,
        fetch: F,
    ) -> HybridFetch<K, V, S>
    where
        F: FnOnce() -> FU,
        FU: Future<Output = anyhow::Result<V>> + Send + 'static,
    {
        self.fetch_inner(key, properties, fetch)
    }

    fn fetch_inner<F, FU>(&self, key: K, properties: HybridCacheProperties, fetch: F) -> HybridFetch<K, V, S>
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
            properties,
            || {
                let metrics = self.metrics.clone();
                let runtime = self.storage().runtime().clone();

                async move {
                    let throttled = match store.load(&key).await.map_err(anyhow::Error::from) {
                        Ok(Load::Entry { key: _, value }) => {
                            metrics.hybrid_hit.increase(1);
                            metrics.hybrid_hit_duration.record(now.elapsed().as_secs_f64());
                            return Diversion {
                                target: Ok(value),
                                store: Some(FetchContext {
                                    throttled: false,
                                    source: Source::Populated,
                                }),
                            };
                        }
                        Ok(Load::Throttled) => true,
                        Ok(Load::Miss) => false,
                        Err(e) => return Err(e).into(),
                    };

                    metrics.hybrid_miss.increase(1);
                    metrics.hybrid_miss_duration.record(now.elapsed().as_secs_f64());

                    let fut = async move {
                        Diversion {
                            target: future.await,
                            store: Some(FetchContext {
                                throttled,
                                source: Source::Outer,
                            }),
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

    use foyer_common::hasher::ModRandomState;
    use foyer_storage::test_utils::{Record, Recorder};
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

    async fn open_with_policy_and_recorder(
        dir: impl AsRef<Path>,
        policy: HybridCachePolicy,
    ) -> (HybridCache<u64, Vec<u8>, ModRandomState>, Arc<Recorder>) {
        let recorder = Arc::<Recorder>::default();
        let hybrid = HybridCacheBuilder::new()
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
            .with_admission_picker(recorder.clone())
            .build()
            .await
            .unwrap();
        (hybrid, recorder)
    }

    async fn open_with_flush_on_close(
        dir: impl AsRef<Path>,
        flush_on_close: bool,
    ) -> HybridCache<u64, Vec<u8>, ModRandomState> {
        HybridCacheBuilder::new()
            .with_name("test")
            .with_flush_on_close(flush_on_close)
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
        let e2 = hybrid.insert_with_properties(2, vec![2; 7 * KB], HybridCacheProperties::default());
        assert_eq!(e1.value(), &vec![1; 7 * KB]);
        assert_eq!(e2.value(), &vec![2; 7 * KB]);

        let e3 = hybrid.storage_writer(3).insert(vec![3; 7 * KB]).unwrap();
        let e4 = hybrid
            .storage_writer(4)
            .insert_with_properties(vec![4; 7 * KB], HybridCacheProperties::default())
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
        let e2 = hybrid
            .writer(2)
            .insert_with_properties(vec![2; 7 * KB], HybridCacheProperties::default());

        assert_eq!(e1.value(), &vec![1; 7 * KB]);
        assert_eq!(e2.value(), &vec![2; 7 * KB]);

        let e3 = hybrid.writer(3).storage().insert(vec![3; 7 * KB]).unwrap();
        let e4 = hybrid
            .writer(4)
            .insert_with_properties(vec![4; 7 * KB], HybridCacheProperties::default());

        assert_eq!(e3.value(), &vec![3; 7 * KB]);
        assert_eq!(e4.value(), &vec![4; 7 * KB]);

        let r5 = hybrid.writer(5).storage().insert(vec![5; 7 * KB]);
        assert!(r5.is_none());

        let e5 = hybrid.writer(5).storage().force().insert(vec![5; 7 * KB]).unwrap();
        assert_eq!(e5.value(), &vec![5; 7 * KB]);
    }

    #[test_log::test(tokio::test)]
    async fn test_hybrid_fetch_with_cache_location() {
        // Test hybrid cache that write disk cache on eviction.

        let dir = tempfile::tempdir().unwrap();
        let hybrid = open_with_policy(dir.path(), HybridCachePolicy::WriteOnEviction).await;

        hybrid
            .fetch_with_properties(
                1,
                HybridCacheProperties::default().with_location(Location::Default),
                || async move { Ok(vec![1; 7 * KB]) },
            )
            .await
            .unwrap();
        assert_eq!(hybrid.memory().get(&1).unwrap().value(), &vec![1; 7 * KB]);
        hybrid.memory().evict_all();
        hybrid.storage().wait().await;
        assert_eq!(
            hybrid.storage().load(&1).await.unwrap().entry().unwrap().1,
            vec![1; 7 * KB]
        );

        hybrid
            .fetch_with_properties(
                2,
                HybridCacheProperties::default().with_location(Location::InMem),
                || async move { Ok(vec![2; 7 * KB]) },
            )
            .await
            .unwrap();
        assert_eq!(hybrid.memory().get(&2).unwrap().value(), &vec![2; 7 * KB]);
        hybrid.memory().evict_all();
        hybrid.storage().wait().await;
        assert!(hybrid.storage().load(&2).await.unwrap().is_miss());

        hybrid
            .fetch_with_properties(
                3,
                HybridCacheProperties::default().with_location(Location::OnDisk),
                || async move { Ok(vec![3; 7 * KB]) },
            )
            .await
            .unwrap();
        hybrid.storage().wait().await;
        assert!(hybrid.memory().get(&3).is_none());
        assert_eq!(
            hybrid.storage().load(&3).await.unwrap().entry().unwrap().1,
            vec![3; 7 * KB]
        );

        // Test hybrid cache that write disk cache on insertion.

        let dir = tempfile::tempdir().unwrap();
        let hybrid = open_with_policy(dir.path(), HybridCachePolicy::WriteOnInsertion).await;

        hybrid
            .fetch_with_properties(
                1,
                HybridCacheProperties::default().with_location(Location::Default),
                || async move { Ok(vec![1; 7 * KB]) },
            )
            .await
            .unwrap();
        hybrid.storage().wait().await;
        assert_eq!(hybrid.memory().get(&1).unwrap().value(), &vec![1; 7 * KB]);
        assert_eq!(
            hybrid.storage().load(&1).await.unwrap().entry().unwrap().1,
            vec![1; 7 * KB]
        );

        hybrid
            .fetch_with_properties(
                2,
                HybridCacheProperties::default().with_location(Location::InMem),
                || async move { Ok(vec![2; 7 * KB]) },
            )
            .await
            .unwrap();
        hybrid.storage().wait().await;
        assert_eq!(hybrid.memory().get(&2).unwrap().value(), &vec![2; 7 * KB]);
        assert!(hybrid.storage().load(&2).await.unwrap().is_miss());

        hybrid
            .fetch_with_properties(
                3,
                HybridCacheProperties::default().with_location(Location::OnDisk),
                || async move { Ok(vec![3; 7 * KB]) },
            )
            .await
            .unwrap();
        hybrid.storage().wait().await;
        assert!(hybrid.memory().get(&3).is_none());
        assert_eq!(
            hybrid.storage().load(&3).await.unwrap().entry().unwrap().1,
            vec![3; 7 * KB]
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_hybrid_insert_with_cache_location() {
        // Test hybrid cache that write disk cache on eviction.

        let dir = tempfile::tempdir().unwrap();
        let hybrid = open_with_policy(dir.path(), HybridCachePolicy::WriteOnEviction).await;

        hybrid.insert_with_properties(
            1,
            vec![1; 7 * KB],
            HybridCacheProperties::default().with_location(Location::Default),
        );
        assert_eq!(hybrid.memory().get(&1).unwrap().value(), &vec![1; 7 * KB]);
        hybrid.memory().evict_all();
        hybrid.storage().wait().await;
        assert_eq!(
            hybrid.storage().load(&1).await.unwrap().entry().unwrap().1,
            vec![1; 7 * KB]
        );

        hybrid.insert_with_properties(
            2,
            vec![2; 7 * KB],
            HybridCacheProperties::default().with_location(Location::InMem),
        );
        assert_eq!(hybrid.memory().get(&2).unwrap().value(), &vec![2; 7 * KB]);
        hybrid.memory().evict_all();
        hybrid.storage().wait().await;
        assert!(hybrid.storage().load(&2).await.unwrap().is_miss());

        hybrid.insert_with_properties(
            3,
            vec![3; 7 * KB],
            HybridCacheProperties::default().with_location(Location::OnDisk),
        );
        hybrid.storage().wait().await;
        assert!(hybrid.memory().get(&3).is_none());
        assert_eq!(
            hybrid.storage().load(&3).await.unwrap().entry().unwrap().1,
            vec![3; 7 * KB]
        );

        // Test hybrid cache that write disk cache on insertion.

        let dir = tempfile::tempdir().unwrap();
        let hybrid = open_with_policy(dir.path(), HybridCachePolicy::WriteOnInsertion).await;

        hybrid.insert_with_properties(
            1,
            vec![1; 7 * KB],
            HybridCacheProperties::default().with_location(Location::Default),
        );
        hybrid.storage().wait().await;
        assert_eq!(hybrid.memory().get(&1).unwrap().value(), &vec![1; 7 * KB]);
        assert_eq!(
            hybrid.storage().load(&1).await.unwrap().entry().unwrap().1,
            vec![1; 7 * KB]
        );

        hybrid.insert_with_properties(
            2,
            vec![2; 7 * KB],
            HybridCacheProperties::default().with_location(Location::InMem),
        );
        hybrid.storage().wait().await;
        assert_eq!(hybrid.memory().get(&2).unwrap().value(), &vec![2; 7 * KB]);
        assert!(hybrid.storage().load(&2).await.unwrap().is_miss());

        hybrid.insert_with_properties(
            3,
            vec![3; 7 * KB],
            HybridCacheProperties::default().with_location(Location::OnDisk),
        );
        hybrid.storage().wait().await;
        assert!(hybrid.memory().get(&3).is_none());
        assert_eq!(
            hybrid.storage().load(&3).await.unwrap().entry().unwrap().1,
            vec![3; 7 * KB]
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_hybrid_read_throttled() {
        // Test hybrid cache that write disk cache on insertion.

        // 1. open empty hybrid cache
        let dir = tempfile::tempdir().unwrap();
        let (hybrid, recorder) = open_with_policy_and_recorder(dir.path(), HybridCachePolicy::WriteOnInsertion).await;

        // 2. insert e1 and flush it to disk.
        hybrid.insert_with_properties(
            1,
            vec![1; 7 * KB],
            HybridCacheProperties::default().with_location(Location::Default),
        );
        hybrid.storage().wait().await;
        assert_eq!(hybrid.memory().get(&1).unwrap().value(), &vec![1; 7 * KB]);
        assert_eq!(
            hybrid.storage().load(&1).await.unwrap().entry().unwrap().1,
            vec![1; 7 * KB]
        );

        // 3. throttle all reads
        hybrid.storage().load_throttle_switch().throttle();
        assert!(matches! {hybrid.storage().load(&1).await.unwrap(), Load::Throttled });

        // 4. assert fetch will not reinsert throttled but existed e1
        hybrid.fetch(1, || async move { Ok(vec![1; 7 * KB]) }).await.unwrap();
        hybrid.storage().wait().await;
        assert_eq!(hybrid.memory().get(&1).unwrap().value(), &vec![1; 7 * KB]);
        assert_eq!(recorder.dump(), vec![Record::Admit(1)]);

        // Test hybrid cache that write disk cache on eviction.

        // 1. open empty hybrid cache
        let dir = tempfile::tempdir().unwrap();
        let (hybrid, recorder) = open_with_policy_and_recorder(dir.path(), HybridCachePolicy::WriteOnEviction).await;

        // 2. insert e1 and flush it to disk.
        hybrid.insert_with_properties(
            1,
            vec![1; 7 * KB],
            HybridCacheProperties::default().with_location(Location::Default),
        );
        assert_eq!(hybrid.memory().get(&1).unwrap().value(), &vec![1; 7 * KB]);
        hybrid.memory().evict_all();
        hybrid.storage().wait().await;
        assert_eq!(
            hybrid.storage().load(&1).await.unwrap().entry().unwrap().1,
            vec![1; 7 * KB]
        );

        // 3. throttle all reads
        hybrid.storage().load_throttle_switch().throttle();
        assert!(matches! {hybrid.storage().load(&1).await.unwrap(), Load::Throttled });

        // 4. assert fetch will not reinsert throttled but existed e1
        hybrid.fetch(1, || async move { Ok(vec![1; 7 * KB]) }).await.unwrap();
        assert_eq!(hybrid.memory().get(&1).unwrap().value(), &vec![1; 7 * KB]);
        hybrid.memory().evict_all();
        hybrid.storage().wait().await;
        assert_eq!(recorder.dump(), vec![Record::Admit(1)]);
    }

    #[test_log::test(tokio::test)]
    async fn test_flush_on_close() {
        // check without flush on close

        let dir = tempfile::tempdir().unwrap();
        let hybrid = open_with_flush_on_close(dir.path(), false).await;
        hybrid.insert(1, vec![1; 7 * KB]);
        assert!(hybrid.storage().load(&1).await.unwrap().is_miss());
        hybrid.close().await.unwrap();
        let hybrid = open_with_flush_on_close(dir.path(), false).await;
        assert!(hybrid.storage().load(&1).await.unwrap().is_miss());

        // check with flush on close

        let dir = tempfile::tempdir().unwrap();
        let hybrid = open_with_flush_on_close(dir.path(), true).await;
        hybrid.insert(1, vec![1; 7 * KB]);
        assert!(hybrid.storage().load(&1).await.unwrap().is_miss());
        hybrid.close().await.unwrap();
        let hybrid = open_with_flush_on_close(dir.path(), true).await;
        assert_eq!(
            hybrid.storage().load(&1).await.unwrap().entry().unwrap(),
            (1, vec![1; 7 * KB])
        );
    }
}
