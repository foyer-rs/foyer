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
    borrow::Cow,
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

use equivalent::Equivalent;
#[cfg(feature = "tracing")]
use fastrace::prelude::*;
#[cfg(feature = "tracing")]
use foyer_common::tracing::{InRootSpan, TracingConfig, TracingOptions};
use foyer_common::{
    code::{DefaultHasher, HashBuilder, StorageKey, StorageValue},
    future::Diversion,
    metrics::Metrics,
    properties::{Hint, Location, Properties, Source},
};
use foyer_memory::{Cache, CacheEntry, Fetch, FetchContext, FetchState, Piece, Pipe};
use foyer_storage::{IoThrottler, Load, Statistics, Store};
use futures::FutureExt;
use pin_project::pin_project;
use serde::{Deserialize, Serialize};

use crate::hybrid::{
    builder::HybridCacheBuilder,
    error::{Error, Result},
    writer::{HybridCacheStorageWriter, HybridCacheWriter},
};

#[cfg(feature = "tracing")]
macro_rules! root_span {
    ($self:ident, mut $name:ident, $label:expr) => {
        root_span!($self, (mut) $name, $label)
    };
    ($self:ident, $name:ident, $label:expr) => {
        root_span!($self, () $name, $label)
    };
    ($self:ident, ($($mut:tt)?) $name:ident, $label:expr) => {
        let $name = if $self.inner.tracing.load(std::sync::atomic::Ordering::Relaxed) {
            Span::root($label, SpanContext::random())
        } else {
            Span::noop()
        };
    };
}

#[cfg(not(feature = "tracing"))]
macro_rules! root_span {
    ($self:ident, mut $name:ident, $label:expr) => {};
    ($self:ident, $name:ident, $label:expr) => {};
    ($self:ident, ($($mut:tt)?) $name:ident, $label:expr) => {};
}

#[cfg(feature = "tracing")]
macro_rules! try_cancel {
    ($self:ident, $span:ident, $threshold:ident) => {
        if let Some(elapsed) = $span.elapsed() {
            if elapsed < $self.inner.tracing_config.$threshold() {
                $span.cancel();
            }
        }
    };
}

#[cfg(not(feature = "tracing"))]
macro_rules! try_cancel {
    ($self:ident, $span:ident, $threshold:ident) => {};
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
pub type HybridCacheEntry<K, V, S = DefaultHasher> = CacheEntry<K, V, S, HybridCacheProperties>;

#[derive(Debug)]
pub struct HybridCacheOptions {
    pub policy: HybridCachePolicy,
    pub flush_on_close: bool,
    #[cfg(feature = "tracing")]
    pub tracing_options: TracingOptions,
}

impl Default for HybridCacheOptions {
    fn default() -> Self {
        Self {
            policy: HybridCachePolicy::default(),
            flush_on_close: true,
            #[cfg(feature = "tracing")]
            tracing_options: TracingOptions::default(),
        }
    }
}

struct Inner<K, V, S = DefaultHasher>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    name: Cow<'static, str>,
    policy: HybridCachePolicy,
    flush_on_close: bool,
    metrics: Arc<Metrics>,
    closed: Arc<AtomicBool>,
    memory: Cache<K, V, S, HybridCacheProperties>,
    storage: Store<K, V, S, HybridCacheProperties>,
    #[cfg(feature = "tracing")]
    tracing: std::sync::atomic::AtomicBool,
    #[cfg(feature = "tracing")]
    tracing_config: TracingConfig,
}

impl<K, V, S> Inner<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    async fn close_inner(
        closed: Arc<AtomicBool>,
        memory: Cache<K, V, S, HybridCacheProperties>,
        storage: Store<K, V, S, HybridCacheProperties>,
        flush_on_close: bool,
    ) -> Result<()> {
        if closed.fetch_or(true, Ordering::Relaxed) {
            return Ok(());
        }

        let now = Instant::now();
        if flush_on_close {
            let bytes = memory.usage();
            tracing::info!(bytes, "[hybrid]: flush all in-memory cached entries to disk on close");
            memory.flush().await;
        }
        storage.close().await?;

        let elapsed = now.elapsed();
        tracing::info!("[hybrid]: close consumes {elapsed:?}");
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        Self::close_inner(
            self.closed.clone(),
            self.memory.clone(),
            self.storage.clone(),
            self.flush_on_close,
        )
        .await
    }
}

impl<K, V, S> Drop for Inner<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn drop(&mut self) {
        let name = self.name.clone();
        let closed = self.closed.clone();
        let memory = self.memory.clone();
        let storage = self.storage.clone();
        let flush_on_close = self.flush_on_close;

        self.storage.runtime().user().spawn(async move {
            if let Err(e) = Self::close_inner(closed, memory, storage, flush_on_close).await {
                tracing::error!(?name, ?e, "[hybrid]: failed to close hybrid cache");
            }
        });
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
pub struct HybridCache<K, V, S = DefaultHasher>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    inner: Arc<Inner<K, V, S>>,
}

impl<K, V, S> Debug for HybridCache<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut r = f.debug_struct("HybridCache");
        r.field("policy", &self.inner.policy)
            .field("flush_on_close", &self.inner.flush_on_close)
            .field("memory", &self.inner.memory)
            .field("storage", &self.inner.storage);
        #[cfg(feature = "tracing")]
        r.field("tracing", &self.inner.tracing)
            .field("tracing_config", &self.inner.tracing_config);
        r.finish()
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
            inner: self.inner.clone(),
        }
    }
}

impl<K, V> HybridCache<K, V, DefaultHasher>
where
    K: StorageKey,
    V: StorageValue,
{
    /// Create a new hybrid cache builder.
    pub fn builder() -> HybridCacheBuilder<K, V> {
        HybridCacheBuilder::new()
    }
}

impl<K, V, S> HybridCache<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    pub(crate) fn new(
        name: Cow<'static, str>,
        options: HybridCacheOptions,
        memory: Cache<K, V, S, HybridCacheProperties>,
        storage: Store<K, V, S, HybridCacheProperties>,
        metrics: Arc<Metrics>,
    ) -> Self {
        let policy = options.policy;
        let flush_on_close = options.flush_on_close;
        #[cfg(feature = "tracing")]
        let tracing_config = {
            let cfg = TracingConfig::default();
            cfg.update(options.tracing_options);
            cfg
        };
        #[cfg(feature = "tracing")]
        let tracing = std::sync::atomic::AtomicBool::new(false);
        let closed = Arc::new(AtomicBool::new(false));
        let inner = Inner {
            name,
            policy,
            flush_on_close,
            closed,
            memory,
            storage,
            metrics,
            #[cfg(feature = "tracing")]
            tracing,
            #[cfg(feature = "tracing")]
            tracing_config,
        };
        let inner = Arc::new(inner);
        Self { inner }
    }

    /// Get the name of the hybrid cache.
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Get the hybrid cache policy
    pub fn policy(&self) -> HybridCachePolicy {
        self.inner.policy
    }

    /// Access the trace config with options.
    #[cfg(feature = "tracing")]
    pub fn update_tracing_options(&self, options: TracingOptions) {
        self.inner.tracing_config.update(options);
    }

    /// Access the in-memory cache.
    pub fn memory(&self) -> &Cache<K, V, S, HybridCacheProperties> {
        &self.inner.memory
    }

    /// Enable tracing.
    #[cfg(feature = "tracing")]
    pub fn enable_tracing(&self) {
        self.inner.tracing.store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// Disable tracing.
    #[cfg(feature = "tracing")]
    pub fn disable_tracing(&self) {
        self.inner.tracing.store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// Return `true` if tracing is enabled.
    #[cfg(feature = "tracing")]
    pub fn is_tracing_enabled(&self) -> bool {
        self.inner.tracing.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Insert cache entry to the hybrid cache.
    pub fn insert(&self, key: K, value: V) -> HybridCacheEntry<K, V, S> {
        root_span!(self, span, "foyer::hybrid::cache::insert");

        #[cfg(feature = "tracing")]
        let _guard = span.set_local_parent();

        let now = Instant::now();

        let entry = self.inner.memory.insert(key, value);
        if self.inner.policy == HybridCachePolicy::WriteOnInsertion {
            self.inner.storage.enqueue(entry.piece(), false);
        }

        self.inner.metrics.hybrid_insert.increase(1);
        self.inner
            .metrics
            .hybrid_insert_duration
            .record(now.elapsed().as_secs_f64());

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

        #[cfg(feature = "tracing")]
        let _guard = span.set_local_parent();

        let now = Instant::now();

        let ephemeral = matches! { properties.location(), Location::OnDisk };
        let entry = self
            .inner
            .memory
            .insert_with_properties(key, value, properties.with_ephemeral(ephemeral));
        if self.inner.policy == HybridCachePolicy::WriteOnInsertion && entry.properties().location() != Location::InMem
        {
            self.inner.storage.enqueue(entry.piece(), false);
        }

        self.inner.metrics.hybrid_insert.increase(1);
        self.inner
            .metrics
            .hybrid_insert_duration
            .record(now.elapsed().as_secs_f64());

        try_cancel!(self, span, record_hybrid_insert_threshold);

        entry
    }

    /// Get cached entry with the given key from the hybrid cache.
    pub async fn get<Q>(&self, key: &Q) -> Result<Option<HybridCacheEntry<K, V, S>>>
    where
        Q: Hash + Equivalent<K> + Send + Sync + 'static + Clone,
    {
        root_span!(self, span, "foyer::hybrid::cache::get");

        let now = Instant::now();

        let record_hit = || {
            self.inner.metrics.hybrid_hit.increase(1);
            self.inner
                .metrics
                .hybrid_hit_duration
                .record(now.elapsed().as_secs_f64());
        };
        let record_miss = || {
            self.inner.metrics.hybrid_miss.increase(1);
            self.inner
                .metrics
                .hybrid_miss_duration
                .record(now.elapsed().as_secs_f64());
        };
        let record_throttled = || {
            self.inner.metrics.hybrid_throttled.increase(1);
            self.inner
                .metrics
                .hybrid_throttled_duration
                .record(now.elapsed().as_secs_f64());
        };

        #[cfg(feature = "tracing")]
        let guard = span.set_local_parent();
        if let Some(entry) = self.inner.memory.get(key) {
            record_hit();
            try_cancel!(self, span, record_hybrid_get_threshold);
            return Ok(Some(entry));
        }
        #[cfg(feature = "tracing")]
        drop(guard);

        #[cfg(feature = "tracing")]
        let load = self
            .inner
            .storage
            .load(key)
            .in_span(Span::enter_with_parent("foyer::hybrid::cache::get::poll", &span));
        #[cfg(not(feature = "tracing"))]
        let load = self.inner.storage.load(key);

        let entry = match load.await? {
            Load::Entry { key, value, populated } => {
                record_hit();
                Some(self.inner.memory.insert_with_properties(
                    key,
                    value,
                    HybridCacheProperties::default().with_source(Source::Populated(populated)),
                ))
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
    pub async fn obtain(&self, key: K) -> Result<Option<HybridCacheEntry<K, V, S>>>
    where
        K: Clone,
    {
        root_span!(self, span, "foyer::hybrid::cache::obtain");

        let now = Instant::now();

        #[cfg(feature = "tracing")]
        let guard = span.set_local_parent();

        let fetch = self.inner.memory.fetch_inner(
            key.clone(),
            HybridCacheProperties::default(),
            || {
                let store = self.inner.storage.clone();
                async move {
                    match store.load(&key).await.map_err(Error::from) {
                        Ok(Load::Entry {
                            key: _,
                            value,
                            populated,
                        }) => Diversion {
                            target: Ok(value),
                            store: Some(FetchContext {
                                throttled: false,
                                source: Source::Populated(populated),
                            }),
                        },
                        Ok(Load::Throttled) => Err(ObtainFetchError::Throttled).into(),
                        Ok(Load::Miss) => Err(ObtainFetchError::NotExist).into(),
                        Err(e) => Err(ObtainFetchError::Other(e)).into(),
                    }
                }
            },
            &tokio::runtime::Handle::current().into(),
        );
        #[cfg(feature = "tracing")]
        drop(guard);

        let res = fetch.await;

        match res {
            Ok(entry) => {
                self.inner.metrics.hybrid_hit.increase(1);
                self.inner
                    .metrics
                    .hybrid_hit_duration
                    .record(now.elapsed().as_secs_f64());
                try_cancel!(self, span, record_hybrid_obtain_threshold);
                Ok(Some(entry))
            }
            Err(ObtainFetchError::Throttled) => {
                self.inner.metrics.hybrid_throttled.increase(1);
                self.inner
                    .metrics
                    .hybrid_throttled_duration
                    .record(now.elapsed().as_secs_f64());
                try_cancel!(self, span, record_hybrid_obtain_threshold);
                Ok(None)
            }
            Err(ObtainFetchError::NotExist) => {
                self.inner.metrics.hybrid_miss.increase(1);
                self.inner
                    .metrics
                    .hybrid_miss_duration
                    .record(now.elapsed().as_secs_f64());
                try_cancel!(self, span, record_hybrid_obtain_threshold);
                Ok(None)
            }
            Err(ObtainFetchError::Other(e)) => {
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

        #[cfg(feature = "tracing")]
        let _guard = span.set_local_parent();

        let now = Instant::now();

        self.inner.memory.remove(key);
        self.inner.storage.delete(key);

        self.inner.metrics.hybrid_remove.increase(1);
        self.inner
            .metrics
            .hybrid_remove_duration
            .record(now.elapsed().as_secs_f64());

        try_cancel!(self, span, record_hybrid_remove_threshold);
    }

    /// Check if the hybrid cache contains a cached entry with the given key.
    ///
    /// `contains` may return a false-positive result if there is a hash collision with the given key.
    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        Q: Hash + Equivalent<K> + ?Sized,
    {
        self.inner.memory.contains(key) || self.inner.storage.may_contains(key)
    }

    /// Clear the hybrid cache.
    pub async fn clear(&self) -> Result<()> {
        self.inner.memory.clear();
        self.inner.storage.destroy().await?;
        Ok(())
    }

    /// Gracefully close the hybrid cache.
    ///
    /// `close` will wait for the ongoing flush and reclaim tasks to finish.
    ///
    /// Based on the `flush_on_close` option, `close` will flush all in-memory cached entries to the disk cache.
    ///
    /// For more details, please refer to [`super::builder::HybridCacheBuilder::with_flush_on_close()`].
    ///
    /// If `close` is not called explicitly, the hybrid cache will be closed when its last copy is dropped.
    pub async fn close(&self) -> Result<()> {
        self.inner.close().await
    }

    /// Return the statistics information of the hybrid cache.
    pub fn statistics(&self) -> &Arc<Statistics> {
        self.inner.storage.statistics()
    }

    /// Create a new [`HybridCacheWriter`].
    pub fn writer(&self, key: K) -> HybridCacheWriter<K, V, S> {
        HybridCacheWriter::new(self.clone(), key)
    }

    /// Create a new [`HybridCacheStorageWriter`].
    pub fn storage_writer(&self, key: K) -> HybridCacheStorageWriter<K, V, S> {
        HybridCacheStorageWriter::new(self.clone(), key)
    }

    /// Return `true` if the hybrid cache is running in real hybrid cache mode.
    /// Return `false` if the hybrid cache is running in in-memory mode but with hybrid cache compatible APIs.
    pub fn is_hybrid(&self) -> bool {
        self.inner.storage.is_enabled()
    }

    pub(crate) fn storage(&self) -> &Store<K, V, S, HybridCacheProperties> {
        &self.inner.storage
    }

    pub(crate) fn metrics(&self) -> &Arc<Metrics> {
        &self.inner.metrics
    }
}

#[derive(Debug)]
enum ObtainFetchError {
    Throttled,
    NotExist,
    Other(Error),
}

impl From<foyer_memory::Error> for ObtainFetchError {
    fn from(e: foyer_memory::Error) -> Self {
        Self::Other(e.into())
    }
}

/// The future generated by [`HybridCache::fetch`].
#[cfg(feature = "tracing")]
pub type HybridFetch<K, V, S = DefaultHasher> = InRootSpan<HybridFetchInner<K, V, S>>;

/// The future generated by [`HybridCache::fetch`].
#[cfg(not(feature = "tracing"))]
pub type HybridFetch<K, V, S = DefaultHasher> = HybridFetchInner<K, V, S>;

/// A future that is used to get entry value from the remote storage for the hybrid cache.
#[pin_project]
pub struct HybridFetchInner<K, V, S = DefaultHasher>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    #[pin]
    inner: Fetch<K, V, Error, S, HybridCacheProperties>,
    policy: HybridCachePolicy,
    storage: Store<K, V, S, HybridCacheProperties>,
}

impl<K, V, S> Future for HybridFetchInner<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    type Output = Result<CacheEntry<K, V, S, HybridCacheProperties>>;

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
    type Target = Fetch<K, V, Error, S, HybridCacheProperties>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<K, V, S> HybridCache<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    /// Fetch and insert a cache entry with the given key and method if there is a cache miss.
    ///
    /// If the dedicated runtime of the foyer storage engine is enabled, `fetch` will spawn task with the dedicated
    /// runtime. Otherwise, the user's runtime will be used.
    pub fn fetch<F, FU, AK, AV>(&self, key: AK, fetch: F) -> HybridFetch<K, V, S>
    where
        F: FnOnce() -> FU,
        FU: Future<Output = Result<AV>> + Send + 'static,
        AK: Into<Arc<K>>,
        AV: Into<Arc<V>>,
    {
        self.fetch_inner(key, HybridCacheProperties::default(), fetch)
    }

    /// Fetch and insert a cache entry with the given key, properties, and method if there is a cache miss.
    ///
    /// If the dedicated runtime of the foyer storage engine is enabled, `fetch` will spawn task with the dedicated
    /// runtime. Otherwise, the user's runtime will be used.
    pub fn fetch_with_properties<F, FU, AK, AV>(
        &self,
        key: AK,
        properties: HybridCacheProperties,
        fetch: F,
    ) -> HybridFetch<K, V, S>
    where
        F: FnOnce() -> FU,
        FU: Future<Output = Result<AV>> + Send + 'static,
        AK: Into<Arc<K>>,
        AV: Into<Arc<V>>,
    {
        self.fetch_inner(key, properties, fetch)
    }

    fn fetch_inner<F, FU, AK, AV>(&self, key: AK, properties: HybridCacheProperties, fetch: F) -> HybridFetch<K, V, S>
    where
        F: FnOnce() -> FU,
        FU: Future<Output = Result<AV>> + Send + 'static,
        AK: Into<Arc<K>>,
        AV: Into<Arc<V>>,
    {
        root_span!(self, span, "foyer::hybrid::cache::fetch");

        #[cfg(feature = "tracing")]
        let _guard = span.set_local_parent();

        let now = Instant::now();

        let store = self.inner.storage.clone();
        let key = key.into();

        let future = fetch().map(|r| r.map(|v| v.into()));
        let inner = self.inner.memory.fetch_inner(
            key.clone(),
            properties,
            || {
                let metrics = self.inner.metrics.clone();
                let runtime = self.storage().runtime().clone();

                async move {
                    let throttled = match store.load(key.as_ref()).await.map_err(Error::from) {
                        Ok(Load::Entry {
                            key: _,
                            value,
                            populated,
                        }) => {
                            metrics.hybrid_hit.increase(1);
                            metrics.hybrid_hit_duration.record(now.elapsed().as_secs_f64());
                            return Diversion {
                                target: Ok(value),
                                store: Some(FetchContext {
                                    throttled: false,
                                    source: Source::Populated(populated),
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
                    };
                    #[cfg(feature = "tracing")]
                    let fut = fut.in_span(Span::enter_with_local_parent("foyer::hybrid::fetch::fn"));

                    runtime.user().spawn(fut).await.unwrap()
                }
            },
            self.storage().runtime().read(),
        );

        if inner.state() == FetchState::Hit {
            self.inner.metrics.hybrid_hit.increase(1);
            self.inner
                .metrics
                .hybrid_hit_duration
                .record(now.elapsed().as_secs_f64());
        }

        let inner = HybridFetchInner {
            inner,
            policy: self.inner.policy,
            storage: self.inner.storage.clone(),
        };

        let f = inner;
        #[cfg(feature = "tracing")]
        let f = InRootSpan::new(f, span).with_threshold(self.inner.tracing_config.record_hybrid_fetch_threshold());
        f
    }
}

#[cfg(test)]
mod tests {

    use std::{path::Path, sync::Arc};

    use foyer_common::hasher::ModHasher;
    use foyer_storage::test_utils::{Record, Recorder};
    use storage::test_utils::BiasedPicker;

    use crate::*;

    const KB: usize = 1024;
    const MB: usize = 1024 * 1024;

    async fn open(dir: impl AsRef<Path>) -> HybridCache<u64, Vec<u8>, ModHasher> {
        HybridCacheBuilder::new()
            .with_name("test")
            .memory(4 * MB)
            .with_hash_builder(ModHasher::default())
            // TODO(MrCroxx): Test with `Engine::Mixed`.
            .storage()
            .with_device_builder(FsDeviceBuilder::new(dir).with_capacity(16 * MB))
            .with_io_engine_builder(PsyncIoEngineBuilder::new())
            .with_engine_builder(LargeObjectEngineBuilder::new().with_region_size(MB))
            .build()
            .await
            .unwrap()
    }

    async fn open_with_biased_admission_picker(
        dir: impl AsRef<Path>,
        admits: impl IntoIterator<Item = u64>,
    ) -> HybridCache<u64, Vec<u8>, ModHasher> {
        HybridCacheBuilder::new()
            .with_name("test")
            .memory(4 * MB)
            .with_hash_builder(ModHasher::default())
            // TODO(MrCroxx): Test with `Engine::Mixed`.
            .storage()
            .with_device_builder(FsDeviceBuilder::new(dir).with_capacity(16 * MB))
            .with_io_engine_builder(PsyncIoEngineBuilder::new())
            .with_engine_builder(LargeObjectEngineBuilder::new().with_region_size(MB))
            .with_admission_picker(Arc::new(BiasedPicker::new(admits)))
            .build()
            .await
            .unwrap()
    }

    async fn open_with_policy(
        dir: impl AsRef<Path>,
        policy: HybridCachePolicy,
    ) -> HybridCache<u64, Vec<u8>, ModHasher> {
        HybridCacheBuilder::new()
            .with_name("test")
            .with_policy(policy)
            .memory(4 * MB)
            .with_hash_builder(ModHasher::default())
            // TODO(MrCroxx): Test with `Engine::Mixed`.
            .storage()
            .with_device_builder(FsDeviceBuilder::new(dir).with_capacity(16 * MB))
            .with_io_engine_builder(PsyncIoEngineBuilder::new())
            .with_engine_builder(LargeObjectEngineBuilder::new().with_region_size(MB))
            .build()
            .await
            .unwrap()
    }

    async fn open_with_policy_and_recorder(
        dir: impl AsRef<Path>,
        policy: HybridCachePolicy,
    ) -> (HybridCache<u64, Vec<u8>, ModHasher>, Arc<Recorder>) {
        let recorder = Arc::<Recorder>::default();
        let hybrid = HybridCacheBuilder::new()
            .with_name("test")
            .with_policy(policy)
            .memory(4 * MB)
            .with_hash_builder(ModHasher::default())
            // TODO(MrCroxx): Test with `Engine::Mixed`.
            .storage()
            .with_device_builder(FsDeviceBuilder::new(dir).with_capacity(16 * MB))
            .with_io_engine_builder(PsyncIoEngineBuilder::new())
            .with_engine_builder(LargeObjectEngineBuilder::new().with_region_size(MB))
            .with_admission_picker(recorder.clone())
            .build()
            .await
            .unwrap();
        (hybrid, recorder)
    }

    async fn open_with_flush_on_close(
        dir: impl AsRef<Path>,
        flush_on_close: bool,
    ) -> HybridCache<u64, Vec<u8>, ModHasher> {
        HybridCacheBuilder::new()
            .with_name("test")
            .with_flush_on_close(flush_on_close)
            .memory(4 * MB)
            .with_hash_builder(ModHasher::default())
            // TODO(MrCroxx): Test with `Engine::Mixed`.
            .storage()
            .with_device_builder(FsDeviceBuilder::new(dir).with_capacity(16 * MB))
            .with_io_engine_builder(PsyncIoEngineBuilder::new())
            .with_engine_builder(LargeObjectEngineBuilder::new().with_region_size(MB))
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
            vec![1; 7 * KB].into()
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
            vec![3; 7 * KB].into()
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
            vec![1; 7 * KB].into()
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
            vec![3; 7 * KB].into()
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
            vec![1; 7 * KB].into()
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
            vec![3; 7 * KB].into()
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
            vec![1; 7 * KB].into()
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
            vec![3; 7 * KB].into()
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
            vec![1; 7 * KB].into()
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
            vec![1; 7 * KB].into()
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
            hybrid.storage().load(&1).await.unwrap().kv().unwrap(),
            (1.into(), vec![1; 7 * KB].into())
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_load_after_recovery() {
        let open = |dir| async move {
            HybridCacheBuilder::new()
                .with_name("test")
                .with_policy(HybridCachePolicy::WriteOnInsertion)
                .memory(4 * MB)
                .storage()
                .with_device_builder(FsDeviceBuilder::new(dir).with_capacity(256 * KB))
                .with_io_engine_builder(PsyncIoEngineBuilder::new())
                .with_engine_builder(LargeObjectEngineBuilder::new().with_region_size(64 * KB))
                .build()
                .await
                .unwrap()
        };

        let dir = tempfile::tempdir().unwrap();

        let hybrid = open(&dir).await;
        hybrid.insert(1, vec![1; 3 * KB]);
        assert_eq!(*hybrid.get(&1).await.unwrap().unwrap(), vec![1; 3 * KB]);

        hybrid.close().await.unwrap();

        let hybrid = open(&dir).await;
        assert_eq!(*hybrid.get(&1).await.unwrap().unwrap(), vec![1; 3 * KB]);
    }
}
