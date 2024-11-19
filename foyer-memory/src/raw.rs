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

use std::{
    collections::hash_map::{Entry as HashMapEntry, HashMap},
    fmt::Debug,
    future::Future,
    hash::Hash,
    ops::Deref,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use equivalent::Equivalent;
use fastrace::{
    future::{FutureExt, InSpan},
    Span,
};
use foyer_common::{
    code::HashBuilder,
    event::{Event, EventListener},
    future::{Diversion, DiversionFuture},
    metrics::Metrics,
    runtime::SingletonHandle,
    scope::Scope,
    strict_assert,
};
use itertools::Itertools;
use parking_lot::{Mutex, RwLock};
use pin_project::pin_project;
use tokio::{sync::oneshot, task::JoinHandle};

use crate::{
    eviction::{Eviction, Operator},
    indexer::{hash_table::HashTableIndexer, sentry::Sentry, Indexer},
    record::{Data, Record},
};

/// The weighter for the in-memory cache.
///
/// The weighter is used to calculate the weight of the cache entry.
pub trait Weighter<K, V>: Fn(&K, &V) -> usize + Send + Sync + 'static {}
impl<K, V, T> Weighter<K, V> for T where T: Fn(&K, &V) -> usize + Send + Sync + 'static {}

pub struct RawCacheConfig<E, S>
where
    E: Eviction,
    S: HashBuilder,
{
    pub name: String,
    pub capacity: usize,
    pub shards: usize,
    pub eviction_config: E::Config,
    pub hash_builder: S,
    pub weighter: Arc<dyn Weighter<E::Key, E::Value>>,
    pub event_listener: Option<Arc<dyn EventListener<Key = E::Key, Value = E::Value>>>,
}

struct RawCacheShard<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    eviction: E,
    indexer: Sentry<I>,

    usage: usize,
    capacity: usize,

    #[expect(clippy::type_complexity)]
    waiters: Mutex<HashMap<E::Key, Vec<oneshot::Sender<RawCacheEntry<E, S, I>>>>>,

    metrics: Arc<Metrics>,
    _event_listener: Option<Arc<dyn EventListener<Key = E::Key, Value = E::Value>>>,
}

impl<E, S, I> RawCacheShard<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    fn emplace(
        &mut self,
        data: Data<E>,
        ephemeral: bool,
        garbages: &mut Vec<(Event, Arc<Record<E>>)>,
        waiters: &mut Vec<oneshot::Sender<RawCacheEntry<E, S, I>>>,
    ) -> Arc<Record<E>> {
        std::mem::swap(waiters, &mut self.waiters.lock().remove(&data.key).unwrap_or_default());

        let weight = data.weight;

        let record = Arc::new(Record::new(data));

        // Evict overflow records.
        while self.usage + weight > self.capacity {
            let evicted = match self.eviction.pop() {
                Some(evicted) => evicted,
                None => break,
            };
            self.metrics.memory_evict.increment(1);

            let e = self.indexer.remove(evicted.hash(), evicted.key()).unwrap();
            assert_eq!(Arc::as_ptr(&evicted), Arc::as_ptr(&e));

            strict_assert!(!evicted.as_ref().is_in_indexer());
            strict_assert!(!evicted.as_ref().is_in_eviction());

            self.usage -= evicted.weight();

            garbages.push((Event::Evict, evicted));
        }

        // Insert new record
        if let Some(old) = self.indexer.insert(record.clone()) {
            self.metrics.memory_replace.increment(1);

            strict_assert!(!old.is_in_indexer());

            if old.is_in_eviction() {
                self.eviction.remove(&old);
            }
            strict_assert!(!old.is_in_eviction());

            self.usage -= old.weight();

            garbages.push((Event::Replace, old));
        } else {
            self.metrics.memory_insert.increment(1);
        }
        strict_assert!(record.is_in_indexer());

        record.set_ephemeral(ephemeral);
        if !ephemeral {
            self.eviction.push(record.clone());
            strict_assert!(record.is_in_eviction());
        }

        self.usage += weight;
        self.metrics.memory_usage.increment(weight as f64);
        // Increase the reference count within the lock section.
        // The reference count of the new record must be at the moment.
        let refs = waiters.len() + 1;
        let inc = record.inc_refs(refs);
        assert_eq!(refs, inc);

        record
    }

    #[fastrace::trace(name = "foyer::memory::raw::shard::remove")]
    fn remove<Q>(&mut self, hash: u64, key: &Q) -> Option<Arc<Record<E>>>
    where
        Q: Hash + Equivalent<E::Key> + ?Sized,
    {
        let record = self.indexer.remove(hash, key)?;

        if record.is_in_eviction() {
            self.eviction.remove(&record);
        }
        strict_assert!(!record.is_in_indexer());
        strict_assert!(!record.is_in_eviction());

        self.usage -= record.weight();

        self.metrics.memory_remove.increment(1);

        record.inc_refs(1);

        Some(record)
    }

    #[fastrace::trace(name = "foyer::memory::raw::shard::get_noop")]
    fn get_noop<Q>(&self, hash: u64, key: &Q) -> Option<Arc<Record<E>>>
    where
        Q: Hash + Equivalent<E::Key> + ?Sized,
    {
        self.get_inner(hash, key)
    }

    #[fastrace::trace(name = "foyer::memory::raw::shard::get_immutable")]
    fn get_immutable<Q>(&self, hash: u64, key: &Q) -> Option<Arc<Record<E>>>
    where
        Q: Hash + Equivalent<E::Key> + ?Sized,
    {
        self.get_inner(hash, key)
            .inspect(|record| self.acquire_immutable(record))
    }

    #[fastrace::trace(name = "foyer::memory::raw::shard::get_mutable")]
    fn get_mutable<Q>(&mut self, hash: u64, key: &Q) -> Option<Arc<Record<E>>>
    where
        Q: Hash + Equivalent<E::Key> + ?Sized,
    {
        self.get_inner(hash, key).inspect(|record| self.acquire_mutable(record))
    }

    #[fastrace::trace(name = "foyer::memory::raw::shard::get_inner")]
    fn get_inner<Q>(&self, hash: u64, key: &Q) -> Option<Arc<Record<E>>>
    where
        Q: Hash + Equivalent<E::Key> + ?Sized,
    {
        let record = match self.indexer.get(hash, key).cloned() {
            Some(record) => {
                self.metrics.memory_hit.increment(1);
                record
            }
            None => {
                self.metrics.memory_miss.increment(1);
                return None;
            }
        };

        strict_assert!(record.is_in_indexer());

        record.set_ephemeral(false);

        record.inc_refs(1);

        Some(record)
    }

    #[fastrace::trace(name = "foyer::memory::raw::shard::clear")]
    fn clear(&mut self, garbages: &mut Vec<Arc<Record<E>>>) {
        let records = self.indexer.drain().collect_vec();
        self.eviction.clear();

        let mut count = 0;

        for record in records {
            count += 1;
            strict_assert!(!record.is_in_indexer());
            strict_assert!(!record.is_in_eviction());

            garbages.push(record);
        }

        self.metrics.memory_remove.increment(count);
    }

    #[fastrace::trace(name = "foyer::memory::raw::shard::acquire_immutable")]
    fn acquire_immutable(&self, record: &Arc<Record<E>>) {
        self.eviction.acquire_immutable(record);
    }

    #[fastrace::trace(name = "foyer::memory::raw::shard::acquire_mutable")]
    fn acquire_mutable(&mut self, record: &Arc<Record<E>>) {
        self.eviction.acquire_mutable(record);
    }

    #[fastrace::trace(name = "foyer::memory::raw::shard::release_immutable")]
    fn release_immutable(&self, record: &Arc<Record<E>>) {
        self.eviction.release_immutable(record);
    }

    #[fastrace::trace(name = "foyer::memory::raw::shard::release_mutable")]
    fn release_mutable(&mut self, record: &Arc<Record<E>>) {
        self.eviction.release_mutable(record);
    }

    #[fastrace::trace(name = "foyer::memory::raw::shard::fetch_noop")]
    fn fetch_noop(&self, hash: u64, key: &E::Key) -> RawShardFetch<E, S, I>
    where
        E::Key: Clone,
    {
        if let Some(record) = self.get_noop(hash, key) {
            return RawShardFetch::Hit(record);
        }

        self.fetch_queue(key.clone())
    }

    #[fastrace::trace(name = "foyer::memory::raw::shard::fetch_immutable")]
    fn fetch_immutable(&self, hash: u64, key: &E::Key) -> RawShardFetch<E, S, I>
    where
        E::Key: Clone,
    {
        if let Some(record) = self.get_immutable(hash, key) {
            return RawShardFetch::Hit(record);
        }

        self.fetch_queue(key.clone())
    }

    #[fastrace::trace(name = "foyer::memory::raw::shard::fetch_mutable")]
    fn fetch_mutable(&mut self, hash: u64, key: &E::Key) -> RawShardFetch<E, S, I>
    where
        E::Key: Clone,
    {
        if let Some(record) = self.get_mutable(hash, key) {
            return RawShardFetch::Hit(record);
        }

        self.fetch_queue(key.clone())
    }

    #[fastrace::trace(name = "foyer::memory::raw::shard::fetch_queue")]
    fn fetch_queue(&self, key: E::Key) -> RawShardFetch<E, S, I> {
        match self.waiters.lock().entry(key) {
            HashMapEntry::Occupied(mut o) => {
                let (tx, rx) = oneshot::channel();
                o.get_mut().push(tx);
                self.metrics.memory_queue.increment(1);
                RawShardFetch::Wait(rx.in_span(Span::enter_with_local_parent(
                    "foyer::memory::raw::fetch_with_runtime::wait",
                )))
            }
            HashMapEntry::Vacant(v) => {
                v.insert(vec![]);
                self.metrics.memory_fetch.increment(1);
                RawShardFetch::Miss
            }
        }
    }
}

struct RawCacheInner<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    shards: Vec<RwLock<RawCacheShard<E, S, I>>>,

    capacity: usize,

    hash_builder: S,
    weighter: Arc<dyn Weighter<E::Key, E::Value>>,

    metrics: Arc<Metrics>,
    event_listener: Option<Arc<dyn EventListener<Key = E::Key, Value = E::Value>>>,
}

impl<E, S, I> RawCacheInner<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    #[fastrace::trace(name = "foyer::memory::raw::inner::clear")]
    fn clear(&self) {
        let mut garbages = vec![];

        self.shards
            .iter()
            .map(|shard| shard.write())
            .for_each(|mut shard| shard.clear(&mut garbages));

        // Do not deallocate data within the lock section.
        if let Some(listener) = self.event_listener.as_ref() {
            for record in garbages {
                listener.on_leave(Event::Clear, record.key(), record.value());
            }
        }
    }
}

pub struct RawCache<E, S = ahash::RandomState, I = HashTableIndexer<E>>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    inner: Arc<RawCacheInner<E, S, I>>,
}

impl<E, S, I> Drop for RawCacheInner<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    fn drop(&mut self) {
        self.clear();
    }
}

impl<E, S, I> Clone for RawCache<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<E, S, I> RawCache<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    pub fn new(config: RawCacheConfig<E, S>) -> Self {
        let metrics = Arc::new(Metrics::new(&config.name));

        let shard_capacity = config.capacity / config.shards;

        let shards = (0..config.shards)
            .map(|_| RawCacheShard {
                eviction: E::new(shard_capacity, &config.eviction_config),
                indexer: Sentry::default(),
                usage: 0,
                capacity: shard_capacity,
                waiters: Mutex::default(),
                metrics: metrics.clone(),
                _event_listener: config.event_listener.clone(),
            })
            .map(RwLock::new)
            .collect_vec();

        let inner = RawCacheInner {
            shards,
            capacity: config.capacity,
            hash_builder: config.hash_builder,
            weighter: config.weighter,
            metrics,
            event_listener: config.event_listener,
        };

        Self { inner: Arc::new(inner) }
    }

    #[fastrace::trace(name = "foyer::memory::raw::insert")]
    pub fn insert(&self, key: E::Key, value: E::Value) -> RawCacheEntry<E, S, I> {
        self.insert_with_hint(key, value, Default::default())
    }

    #[fastrace::trace(name = "foyer::memory::raw::insert_with_hint")]
    pub fn insert_with_hint(&self, key: E::Key, value: E::Value, hint: E::Hint) -> RawCacheEntry<E, S, I> {
        self.emplace(key, value, hint, false)
    }

    #[fastrace::trace(name = "foyer::memory::raw::insert_ephemeral")]
    pub fn insert_ephemeral(&self, key: E::Key, value: E::Value) -> RawCacheEntry<E, S, I> {
        self.insert_ephemeral_with_hint(key, value, Default::default())
    }

    #[fastrace::trace(name = "foyer::memory::raw::insert_ephemeral_with_hint")]
    pub fn insert_ephemeral_with_hint(&self, key: E::Key, value: E::Value, hint: E::Hint) -> RawCacheEntry<E, S, I> {
        self.emplace(key, value, hint, true)
    }

    #[fastrace::trace(name = "foyer::memory::raw::emplace")]
    fn emplace(&self, key: E::Key, value: E::Value, hint: E::Hint, ephemeral: bool) -> RawCacheEntry<E, S, I> {
        let hash = self.inner.hash_builder.hash_one(&key);
        let weight = (self.inner.weighter)(&key, &value);

        let mut garbages = vec![];
        let mut waiters = vec![];

        let record = self.inner.shards[self.shard(hash)].write().with(|mut shard| {
            shard.emplace(
                Data {
                    key,
                    value,
                    hint,
                    hash,
                    weight,
                },
                ephemeral,
                &mut garbages,
                &mut waiters,
            )
        });

        // Notify waiters out of the lock critical section.
        for waiter in waiters {
            let _ = waiter.send(RawCacheEntry {
                record: record.clone(),
                inner: self.inner.clone(),
            });
        }

        // Deallocate data out of the lock critical section.
        if let Some(listener) = self.inner.event_listener.as_ref() {
            for (event, record) in garbages {
                listener.on_leave(event, record.key(), record.value());
            }
        }

        RawCacheEntry {
            record,
            inner: self.inner.clone(),
        }
    }

    #[fastrace::trace(name = "foyer::memory::raw::remove")]
    pub fn remove<Q>(&self, key: &Q) -> Option<RawCacheEntry<E, S, I>>
    where
        Q: Hash + Equivalent<E::Key> + ?Sized,
    {
        let hash = self.inner.hash_builder.hash_one(key);

        self.inner.shards[self.shard(hash)]
            .write()
            .with(|mut shard| {
                shard.remove(hash, key).map(|record| RawCacheEntry {
                    inner: self.inner.clone(),
                    record,
                })
            })
            .inspect(|record| {
                // Deallocate data out of the lock critical section.
                if let Some(listener) = self.inner.event_listener.as_ref() {
                    listener.on_leave(Event::Remove, record.key(), record.value());
                }
            })
    }

    #[fastrace::trace(name = "foyer::memory::raw::get")]
    pub fn get<Q>(&self, key: &Q) -> Option<RawCacheEntry<E, S, I>>
    where
        Q: Hash + Equivalent<E::Key> + ?Sized,
    {
        let hash = self.inner.hash_builder.hash_one(key);

        let record = match E::acquire_operator() {
            Operator::Noop => self.inner.shards[self.shard(hash)].read().get_noop(hash, key),
            Operator::Immutable => self.inner.shards[self.shard(hash)]
                .read()
                .with(|shard| shard.get_immutable(hash, key)),
            Operator::Mutable => self.inner.shards[self.shard(hash)]
                .write()
                .with(|mut shard| shard.get_mutable(hash, key)),
        }?;

        Some(RawCacheEntry {
            inner: self.inner.clone(),
            record,
        })
    }

    #[fastrace::trace(name = "foyer::memory::raw::contains")]
    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        Q: Hash + Equivalent<E::Key> + ?Sized,
    {
        let hash = self.inner.hash_builder.hash_one(key);

        self.inner.shards[self.shard(hash)]
            .read()
            .with(|shard| shard.indexer.get(hash, key).is_some())
    }

    #[fastrace::trace(name = "foyer::memory::raw::touch")]
    pub fn touch<Q>(&self, key: &Q) -> bool
    where
        Q: Hash + Equivalent<E::Key> + ?Sized,
    {
        let hash = self.inner.hash_builder.hash_one(key);

        match E::acquire_operator() {
            Operator::Noop => self.inner.shards[self.shard(hash)]
                .read()
                .with(|shard| shard.get_noop(hash, key)),
            Operator::Immutable => self.inner.shards[self.shard(hash)]
                .read()
                .with(|shard| shard.get_immutable(hash, key)),
            Operator::Mutable => self.inner.shards[self.shard(hash)]
                .write()
                .with(|mut shard| shard.get_mutable(hash, key)),
        }
        .is_some()
    }

    #[fastrace::trace(name = "foyer::memory::raw::clear")]
    pub fn clear(&self) {
        self.inner.clear();
    }

    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }

    pub fn usage(&self) -> usize {
        self.inner.shards.iter().map(|shard| shard.read().usage).sum()
    }

    pub fn metrics(&self) -> &Metrics {
        &self.inner.metrics
    }

    pub fn hash_builder(&self) -> &S {
        &self.inner.hash_builder
    }

    pub fn shards(&self) -> usize {
        self.inner.shards.len()
    }

    fn shard(&self, hash: u64) -> usize {
        hash as usize % self.inner.shards.len()
    }
}

pub struct RawCacheEntry<E, S = ahash::RandomState, I = HashTableIndexer<E>>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    inner: Arc<RawCacheInner<E, S, I>>,
    record: Arc<Record<E>>,
}

impl<E, S, I> Debug for RawCacheEntry<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawCacheEntry").field("record", &self.record).finish()
    }
}

impl<E, S, I> Drop for RawCacheEntry<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    fn drop(&mut self) {
        let hash = self.record.hash();
        let shard = &self.inner.shards[hash as usize % self.inner.shards.len()];

        if self.record.dec_refs(1) == 0 {
            match E::release_operator() {
                Operator::Noop => {}
                Operator::Immutable => shard.read().with(|shard| shard.release_immutable(&self.record)),
                Operator::Mutable => shard.write().with(|mut shard| shard.release_mutable(&self.record)),
            }

            if self.record.is_ephemeral() {
                shard
                    .write()
                    .with(|mut shard| shard.remove(hash, self.key()))
                    .inspect(|record| {
                        // Deallocate data out of the lock critical section.
                        if let Some(listener) = self.inner.event_listener.as_ref() {
                            listener.on_leave(Event::Remove, record.key(), record.value());
                        }
                    });
            }
        }
    }
}

impl<E, S, I> Clone for RawCacheEntry<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    fn clone(&self) -> Self {
        self.record.inc_refs(1);
        Self {
            inner: self.inner.clone(),
            record: self.record.clone(),
        }
    }
}

impl<E, S, I> Deref for RawCacheEntry<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    type Target = E::Value;

    fn deref(&self) -> &Self::Target {
        self.value()
    }
}

unsafe impl<E, S, I> Send for RawCacheEntry<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
}

unsafe impl<E, S, I> Sync for RawCacheEntry<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
}

impl<E, S, I> RawCacheEntry<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    pub fn hash(&self) -> u64 {
        self.record.hash()
    }

    pub fn key(&self) -> &E::Key {
        self.record.key()
    }

    pub fn value(&self) -> &E::Value {
        self.record.value()
    }

    pub fn hint(&self) -> &E::Hint {
        self.record.hint()
    }

    pub fn weight(&self) -> usize {
        self.record.weight()
    }

    pub fn refs(&self) -> usize {
        self.record.refs()
    }

    pub fn is_outdated(&self) -> bool {
        !self.record.is_in_indexer()
    }
}

/// The state of [`Fetch`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchState {
    /// Cache hit.
    Hit,
    /// Cache miss, but wait in queue.
    Wait,
    /// Cache miss, and there is no other waiters at the moment.
    Miss,
}

/// A mark for fetch calls.
pub struct FetchMark;

enum RawShardFetch<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    Hit(Arc<Record<E>>),
    Wait(InSpan<oneshot::Receiver<RawCacheEntry<E, S, I>>>),
    Miss,
}

pub type RawFetch<E, ER, S = ahash::RandomState, I = HashTableIndexer<E>> =
    DiversionFuture<RawFetchInner<E, ER, S, I>, std::result::Result<RawCacheEntry<E, S, I>, ER>, FetchMark>;

type RawFetchHit<E, S, I> = Option<RawCacheEntry<E, S, I>>;
type RawFetchWait<E, S, I> = InSpan<oneshot::Receiver<RawCacheEntry<E, S, I>>>;
type RawFetchMiss<E, I, S, ER, DFS> = JoinHandle<Diversion<std::result::Result<RawCacheEntry<E, S, I>, ER>, DFS>>;

#[pin_project(project = RawFetchInnerProj)]
pub enum RawFetchInner<E, ER, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    Hit(RawFetchHit<E, S, I>),
    Wait(#[pin] RawFetchWait<E, S, I>),
    Miss(#[pin] RawFetchMiss<E, I, S, ER, FetchMark>),
}

impl<E, ER, S, I> RawFetchInner<E, ER, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    pub fn state(&self) -> FetchState {
        match self {
            RawFetchInner::Hit(_) => FetchState::Hit,
            RawFetchInner::Wait(_) => FetchState::Wait,
            RawFetchInner::Miss(_) => FetchState::Miss,
        }
    }
}

impl<E, ER, S, I> Future for RawFetchInner<E, ER, S, I>
where
    E: Eviction,
    ER: From<oneshot::error::RecvError>,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    type Output = Diversion<std::result::Result<RawCacheEntry<E, S, I>, ER>, FetchMark>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project() {
            RawFetchInnerProj::Hit(opt) => Poll::Ready(Ok(opt.take().unwrap()).into()),
            RawFetchInnerProj::Wait(waiter) => waiter.poll(cx).map_err(|err| err.into()).map(Diversion::from),
            RawFetchInnerProj::Miss(handle) => handle.poll(cx).map(|join| join.unwrap()),
        }
    }
}

// TODO(MrCroxx): use `hashbrown::HashTable` with `Handle` may relax the `Clone` bound?
impl<E, S, I> RawCache<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
    E::Key: Clone,
{
    #[fastrace::trace(name = "foyer::memory::raw::fetch")]
    pub fn fetch<F, FU, ER>(&self, key: E::Key, fetch: F) -> RawFetch<E, ER, S, I>
    where
        F: FnOnce() -> FU,
        FU: Future<Output = std::result::Result<E::Value, ER>> + Send + 'static,
        ER: Send + 'static + Debug,
    {
        self.fetch_inner(
            key,
            Default::default(),
            fetch,
            &tokio::runtime::Handle::current().into(),
        )
    }

    #[fastrace::trace(name = "foyer::memory::raw::fetch_with_hint")]
    pub fn fetch_with_hint<F, FU, ER>(&self, key: E::Key, hint: E::Hint, fetch: F) -> RawFetch<E, ER, S, I>
    where
        F: FnOnce() -> FU,
        FU: Future<Output = std::result::Result<E::Value, ER>> + Send + 'static,
        ER: Send + 'static + Debug,
    {
        self.fetch_inner(key, hint, fetch, &tokio::runtime::Handle::current().into())
    }

    /// Internal fetch function, only for other foyer crates usages only, so the doc is hidden.
    #[doc(hidden)]
    #[fastrace::trace(name = "foyer::memory::raw::fetch_inner")]
    pub fn fetch_inner<F, FU, ER, ID>(
        &self,
        key: E::Key,
        hint: E::Hint,
        fetch: F,
        runtime: &SingletonHandle,
    ) -> RawFetch<E, ER, S, I>
    where
        F: FnOnce() -> FU,
        FU: Future<Output = ID> + Send + 'static,
        ER: Send + 'static + Debug,
        ID: Into<Diversion<std::result::Result<E::Value, ER>, FetchMark>>,
    {
        let hash = self.inner.hash_builder.hash_one(&key);

        let raw = match E::acquire_operator() {
            Operator::Noop => self.inner.shards[self.shard(hash)].read().fetch_noop(hash, &key),
            Operator::Immutable => self.inner.shards[self.shard(hash)].read().fetch_immutable(hash, &key),
            Operator::Mutable => self.inner.shards[self.shard(hash)].write().fetch_mutable(hash, &key),
        };

        match raw {
            RawShardFetch::Hit(record) => {
                return RawFetch::new(RawFetchInner::Hit(Some(RawCacheEntry {
                    record,
                    inner: self.inner.clone(),
                })))
            }
            RawShardFetch::Wait(future) => return RawFetch::new(RawFetchInner::Wait(future)),
            RawShardFetch::Miss => {}
        }

        let cache = self.clone();
        let future = fetch();
        let join = runtime.spawn(
            async move {
                let Diversion { target, store } = future
                    .in_span(Span::enter_with_local_parent("foyer::memory::raw::fetch_inner::fn"))
                    .await
                    .into();
                let value = match target {
                    Ok(value) => value,
                    Err(e) => {
                        cache.inner.shards[cache.shard(hash)].read().waiters.lock().remove(&key);
                        tracing::debug!("[fetch]: error raise while fetching, all waiter are dropped, err: {e:?}");
                        return Diversion { target: Err(e), store };
                    }
                };
                let entry = cache.insert_with_hint(key, value, hint);
                Diversion {
                    target: Ok(entry),
                    store,
                }
            }
            .in_span(Span::enter_with_local_parent(
                "foyer::memory::generic::fetch_with_runtime::spawn",
            )),
        );

        RawFetch::new(RawFetchInner::Miss(join))
    }
}

#[cfg(test)]
mod tests {

    use rand::{rngs::SmallRng, seq::SliceRandom, RngCore, SeedableRng};

    use super::*;
    use crate::eviction::{
        fifo::{Fifo, FifoConfig, FifoHint},
        lfu::{Lfu, LfuConfig, LfuHint},
        lru::{Lru, LruConfig, LruHint},
        s3fifo::{S3Fifo, S3FifoConfig, S3FifoHint},
    };

    fn is_send_sync_static<T: Send + Sync + 'static>() {}

    fn fifo_cache_for_test() -> RawCache<Fifo<u64, u64>> {
        RawCache::new(RawCacheConfig {
            name: "test".to_string(),
            capacity: 256,
            shards: 4,
            eviction_config: FifoConfig::default(),
            hash_builder: Default::default(),
            weighter: Arc::new(|_, _| 1),
            event_listener: None,
        })
    }

    #[test]
    fn test_send_sync_static() {
        is_send_sync_static::<RawCache<Fifo<(), ()>>>();
        is_send_sync_static::<RawCache<S3Fifo<(), ()>>>();
        is_send_sync_static::<RawCache<Lfu<(), ()>>>();
        is_send_sync_static::<RawCache<Lru<(), ()>>>();
    }

    #[test]
    fn test_insert_ephemeral() {
        let fifo = fifo_cache_for_test();

        let e1 = fifo.insert_ephemeral(1, 1);
        assert_eq!(fifo.usage(), 1);
        drop(e1);
        assert_eq!(fifo.usage(), 0);

        let e2a = fifo.insert_ephemeral(2, 2);
        assert_eq!(fifo.usage(), 1);
        let e2b = fifo.get(&2).expect("entry 2 should exist");
        drop(e2a);
        assert_eq!(fifo.usage(), 1);
        drop(e2b);
        assert_eq!(fifo.usage(), 1);
    }

    mod fuzzy {
        use super::*;

        fn fuzzy<E>(cache: RawCache<E>, hints: Vec<E::Hint>)
        where
            E: Eviction<Key = u64, Value = u64>,
        {
            let handles = (0..8)
                .map(|i| {
                    let c = cache.clone();
                    let hints = hints.clone();
                    std::thread::spawn(move || {
                        let mut rng = SmallRng::seed_from_u64(i);
                        for _ in 0..100000 {
                            let key = rng.next_u64();
                            if let Some(entry) = c.get(&key) {
                                assert_eq!(key, *entry);
                                drop(entry);
                                continue;
                            }
                            let hint = hints.choose(&mut rng).cloned().unwrap();
                            c.insert_with_hint(key, key, hint);
                        }
                    })
                })
                .collect_vec();

            handles.into_iter().for_each(|handle| handle.join().unwrap());

            assert_eq!(cache.usage(), cache.capacity());
        }

        #[test_log::test]
        fn test_fifo_cache_fuzzy() {
            let cache: RawCache<Fifo<u64, u64>> = RawCache::new(RawCacheConfig {
                name: "test".to_string(),
                capacity: 256,
                shards: 4,
                eviction_config: FifoConfig::default(),
                hash_builder: Default::default(),
                weighter: Arc::new(|_, _| 1),
                event_listener: None,
            });
            let hints = vec![FifoHint];
            fuzzy(cache, hints);
        }

        #[test_log::test]
        fn test_s3fifo_cache_fuzzy() {
            let cache: RawCache<S3Fifo<u64, u64>> = RawCache::new(RawCacheConfig {
                name: "test".to_string(),
                capacity: 256,
                shards: 4,
                eviction_config: S3FifoConfig::default(),
                hash_builder: Default::default(),
                weighter: Arc::new(|_, _| 1),
                event_listener: None,
            });
            let hints = vec![S3FifoHint];
            fuzzy(cache, hints);
        }

        #[test_log::test]
        fn test_lru_cache_fuzzy() {
            let cache: RawCache<Lru<u64, u64>> = RawCache::new(RawCacheConfig {
                name: "test".to_string(),
                capacity: 256,
                shards: 4,
                eviction_config: LruConfig::default(),
                hash_builder: Default::default(),
                weighter: Arc::new(|_, _| 1),
                event_listener: None,
            });
            let hints = vec![LruHint::HighPriority, LruHint::LowPriority];
            fuzzy(cache, hints);
        }

        #[test_log::test]
        fn test_lfu_cache_fuzzy() {
            let cache: RawCache<Lfu<u64, u64>> = RawCache::new(RawCacheConfig {
                name: "test".to_string(),
                capacity: 256,
                shards: 4,
                eviction_config: LfuConfig::default(),
                hash_builder: Default::default(),
                weighter: Arc::new(|_, _| 1),
                event_listener: None,
            });
            let hints = vec![LfuHint];
            fuzzy(cache, hints);
        }
    }
}
