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

//! # Atomic Reference Count Management
//!
//! [`RawCache`] uses an atomic reference count to management the release of an entry.
//!
//! The atomic reference count represents the *external* references of a cache record.
//!
//! When the reference count drops to zero, the related cache shard is locked to release to cache record.
//!
//! It is important to guarantee the correctness of the usage of the atomic reference count. Especially when triggering
//! the release of a record. Without any other synchronize mechanism, there would be dangling pointers or double frees:
//!
//! ```plain
//! Thread 1: [ decrease ARC to 0 ] ============> [ release record ]
//! Thread 2:                         [ increase ARC to 1 ] =======> dangling!! ==> double free!!
//! ```
//!
//! Thankfully, we can prevent it from happening with the usage of the shard lock:
//!
//! The only ops that will increase the atomic reference count are:
//! 1. Insert/get/fetch the [`RawCache`] and get external entries. (locked)
//! 2. Clone an external entry. (lock-free)
//!
//! The op 1 is always guarded by a mutex/rwlock, which means it is impossible to happen while releasing a record with
//! the shard is locked.
//!
//! The op 2 is lock-free, but only happens when there is at least 1 external reference. So, it cannot happen while
//! releasing a record. Because when releasing is happening, there must be no external reference.
//!
//! So, this case will never happen:
//!
//! ```plain
//! Thread 1: [ decrease ARC to 0 ] ================> [ release record ]
//! Thread 2:                           [ (op2) increase ARC to 1 ] ==> dangling!! ==> double free!!
//! ```
//!
//! When starting to release a record, after locking the shard, it's still required to check the atomic reference count.
//! Because this cause still exist:
//!
//! ```plain
//! Thread 1: [ decrease ARC to 0 ] ====================================> [ release record ]
//! Thread 2:                           [ (op1) increase ARC to 1 ] =======> dangling!! ==> double free!!
//! ```
//!
//! Although the op1 requires to be locked, but the release operation can be delayed after that. So the release
//! operation can be ignored after checking the atomic reference count is not zero.
//!
//! There is no need to be afraid of leaking, there is always to be a release operation following the increasing when
//! the atomic reference count drops to zero again.

use std::{
    collections::hash_map::{Entry as HashMapEntry, HashMap},
    fmt::Debug,
    future::Future,
    hash::Hash,
    ops::Deref,
    pin::Pin,
    ptr::NonNull,
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
    event::EventListener,
    future::{Diversion, DiversionFuture},
    metrics::Metrics,
    runtime::SingletonHandle,
    scope::Scope,
    strict_assert,
};
use itertools::Itertools;
use parking_lot::Mutex;
use pin_project::pin_project;
use tokio::{sync::oneshot, task::JoinHandle};

use crate::{
    eviction::{Eviction, Operator},
    indexer::{hash_table::HashTableIndexer, sentry::Sentry, Indexer},
    record::{Data, Record},
    slab::{Slab, SlabBuilder},
    sync::Lock,
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
    pub slab_initial_capacity: usize,
    pub slab_segment_size: usize,
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
    slab: Slab<Record<E>>,

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
    #[fastrace::trace(name = "foyer::memory::raw::shard::emplace")]
    fn emplace(
        &mut self,
        data: Data<E>,
        ephemeral: bool,
        garbages: &mut Vec<Data<E>>,
        waiters: &mut Vec<oneshot::Sender<RawCacheEntry<E, S, I>>>,
    ) -> NonNull<Record<E>> {
        std::mem::swap(waiters, &mut self.waiters.lock().remove(&data.key).unwrap_or_default());

        let weight = data.weight;

        // Allocate and setup new record.
        let token = self.slab.insert(Record::new(data));
        let mut ptr = self.slab.ptr(token);
        unsafe { ptr.as_mut().init(token) };

        // Evict overflow records.
        while self.usage + weight > self.capacity {
            let evicted = match self.eviction.pop() {
                Some(evicted) => evicted,
                None => break,
            };
            self.metrics.memory_evict.increment(1);
            strict_assert!(unsafe { evicted.as_ref().is_in_indexer() });
            strict_assert!(unsafe { !evicted.as_ref().is_in_eviction() });

            // Try to free the record if this thread get the permission.
            if unsafe { evicted.as_ref() }.need_reclaim() {
                if let Some(garbage) = self.reclaim(evicted, false) {
                    garbages.push(garbage);
                }
            }
        }

        // Insert new record
        if let Some(old) = self.indexer.insert(ptr) {
            self.metrics.memory_replace.increment(1);

            strict_assert!(!unsafe { old.as_ref() }.is_in_indexer());
            if unsafe { old.as_ref() }.is_in_eviction() {
                self.eviction.remove(old);
            }
            strict_assert!(!unsafe { old.as_ref() }.is_in_eviction());
            // Because the `old` handle is removed from the indexer, it will not be reinserted again.
            // Try to free the record if this thread get the permission.
            if unsafe { old.as_ref() }.need_reclaim() {
                if let Some(garbage) = self.reclaim(old, false) {
                    garbages.push(garbage);
                }
            }
        } else {
            self.metrics.memory_insert.increment(1);
        }
        strict_assert!(unsafe { ptr.as_ref() }.is_in_indexer());

        unsafe { ptr.as_mut().set_ephemeral(ephemeral) };
        if !ephemeral {
            self.eviction.push(ptr);
            strict_assert!(unsafe { ptr.as_ref() }.is_in_eviction());
        }

        self.usage += weight;
        self.metrics.memory_usage.increment(weight as f64);
        // Increase the reference count within the lock section.
        // The reference count of the new record must be at the moment.
        let refs = waiters.len() as isize + 1;
        let inc = unsafe { ptr.as_ref() }.inc_refs(refs);
        assert_eq!(refs, inc);

        ptr
    }

    #[fastrace::trace(name = "foyer::memory::raw::shard::release")]
    fn reclaim(&mut self, mut ptr: NonNull<Record<E>>, reinsert: bool) -> Option<Data<E>> {
        let record = unsafe { ptr.as_mut() };

        // Assert the record is in the reclamation phase.
        assert_eq!(record.refs(), -1);

        if record.is_in_indexer() && record.is_ephemeral() {
            // The entry is ephemeral, remove it from indexer. Ignore reinsertion.
            strict_assert!(!record.is_in_eviction());
            self.indexer.remove(record.hash(), record.key());
            strict_assert!(!record.is_in_indexer());
        }

        if record.is_in_indexer() {
            // The entry has no external refs, give it another chance by reinsertion if the cache is not busy
            // and the algorithm allows.

            // The usage is higher than the capacity means most handles are held externally,
            // the cache shard cannot release enough weight for the new inserted entries.
            // In this case, the reinsertion should be given up.
            if reinsert && self.usage <= self.capacity {
                let was_in_eviction = record.is_in_eviction();
                self.eviction.release(ptr);
                if record.is_in_eviction() {
                    if !was_in_eviction {
                        self.metrics.memory_reinsert.increment(1);
                    }
                    strict_assert!(record.is_in_indexer());
                    strict_assert!(record.is_in_eviction());
                    // Restore the reference count to exit the reclamation phase.
                    record.reset();
                    return None;
                }
            }

            // If the entry has not been reinserted, remove it from the indexer and the eviction container (if needed).
            self.indexer.remove(record.hash(), record.key());
            if record.is_in_eviction() {
                self.eviction.remove(ptr);
            }
        }

        // Here the handle is neither in the indexer nor in the eviction container.
        strict_assert!(!record.is_in_indexer());
        strict_assert!(!record.is_in_eviction());

        self.metrics.memory_release.increment(1);
        self.usage -= record.weight();
        self.metrics.memory_usage.decrement(record.weight() as f64);

        let token = record.token();

        let record = self.slab.remove(token);
        let data = record.into_data();

        Some(data)
    }

    #[fastrace::trace(name = "foyer::memory::raw::shard::remove")]
    fn remove<Q>(&mut self, hash: u64, key: &Q) -> Option<NonNull<Record<E>>>
    where
        Q: Hash + Equivalent<E::Key> + ?Sized,
    {
        let mut ptr = self.indexer.remove(hash, key)?;

        let record = unsafe { ptr.as_mut() };
        if record.is_in_eviction() {
            self.eviction.remove(ptr);
        }
        strict_assert!(!record.is_in_indexer());
        strict_assert!(!record.is_in_eviction());

        self.metrics.memory_remove.increment(1);

        record.inc_refs_cas(1)?;

        Some(ptr)
    }

    #[fastrace::trace(name = "foyer::memory::raw::shard::get_immutable")]
    fn get_immutable<Q>(&self, hash: u64, key: &Q) -> Option<NonNull<Record<E>>>
    where
        Q: Hash + Equivalent<E::Key> + ?Sized,
    {
        self.get_inner(hash, key).inspect(|&ptr| self.acquire_immutable(ptr))
    }

    #[fastrace::trace(name = "foyer::memory::raw::shard::get_mutable")]
    fn get_mutable<Q>(&mut self, hash: u64, key: &Q) -> Option<NonNull<Record<E>>>
    where
        Q: Hash + Equivalent<E::Key> + ?Sized,
    {
        self.get_inner(hash, key).inspect(|&ptr| self.acquire_mutable(ptr))
    }

    #[fastrace::trace(name = "foyer::memory::raw::shard::get_inner")]
    fn get_inner<Q>(&self, hash: u64, key: &Q) -> Option<NonNull<Record<E>>>
    where
        Q: Hash + Equivalent<E::Key> + ?Sized,
    {
        let ptr = match self.indexer.get(hash, key) {
            Some(ptr) => {
                self.metrics.memory_hit.increment(1);
                ptr
            }
            None => {
                self.metrics.memory_miss.increment(1);
                return None;
            }
        };

        let record = unsafe { ptr.as_ref() };

        strict_assert!(record.is_in_indexer());

        record.set_ephemeral(false);

        record.inc_refs_cas(1)?;

        Some(ptr)
    }

    #[fastrace::trace(name = "foyer::memory::raw::shard::acquire_immutable")]
    fn acquire_immutable(&self, ptr: NonNull<Record<E>>) {
        self.eviction.acquire_immutable(ptr);
    }

    #[fastrace::trace(name = "foyer::memory::raw::shard::acquire_mutable")]
    fn acquire_mutable(&mut self, ptr: NonNull<Record<E>>) {
        self.eviction.acquire_mutable(ptr);
    }

    #[fastrace::trace(name = "foyer::memory::raw::shard::clear")]
    fn clear(&mut self, garbages: &mut Vec<Data<E>>) {
        let ptrs = self.indexer.drain().collect_vec();
        self.eviction.clear();

        let mut count = 0;

        for ptr in ptrs {
            count += 1;
            strict_assert!(unsafe { !ptr.as_ref().is_in_indexer() });
            strict_assert!(unsafe { !ptr.as_ref().is_in_eviction() });
            if unsafe { ptr.as_ref() }.need_reclaim() {
                if let Some(garbage) = self.reclaim(ptr, false) {
                    garbages.push(garbage);
                }
            }
        }

        self.metrics.memory_remove.increment(count);
    }

    #[fastrace::trace(name = "foyer::memory::raw::shard::fetch_immutable")]
    fn fetch_immutable(&self, hash: u64, key: &E::Key) -> RawShardFetch<E, S, I>
    where
        E::Key: Clone,
    {
        if let Some(ptr) = self.get_immutable(hash, key) {
            return RawShardFetch::Hit(ptr);
        }

        self.fetch_queue(key.clone())
    }

    #[fastrace::trace(name = "foyer::memory::raw::shard::fetch_mutable")]
    fn fetch_mutable(&mut self, hash: u64, key: &E::Key) -> RawShardFetch<E, S, I>
    where
        E::Key: Clone,
    {
        if let Some(ptr) = self.get_mutable(hash, key) {
            return RawShardFetch::Hit(ptr);
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
    shards: Vec<Lock<RawCacheShard<E, S, I>>>,

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
            for Data { key, value, .. } in garbages {
                listener.on_memory_release(key, value);
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

        match E::acquire_operator() {
            Operator::Immutable => tracing::info!("[memory]: rwlock is used for foyer in-memory cache"),
            Operator::Mutable => tracing::info!("[memory]: mutex is used for foyer in-memory cache"),
        }

        let shards = (0..config.shards)
            .map(|_| RawCacheShard {
                slab: SlabBuilder::new()
                    .with_capacity(config.slab_initial_capacity)
                    .with_segment_size(config.slab_segment_size)
                    .build(),
                eviction: E::new(shard_capacity, &config.eviction_config),
                indexer: Sentry::default(),
                usage: 0,
                capacity: shard_capacity,
                waiters: Mutex::default(),
                metrics: metrics.clone(),
                _event_listener: config.event_listener.clone(),
            })
            .map(|shard| match E::acquire_operator() {
                Operator::Immutable => Lock::rwlock(shard),
                Operator::Mutable => Lock::mutex(shard),
            })
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

        let ptr = self.inner.shards[self.shard(hash)].write().with(|mut shard| {
            shard.emplace(
                Data {
                    key,
                    value,
                    hint,
                    state: Default::default(),
                    hash,
                    weight,
                },
                ephemeral,
                &mut garbages,
                &mut waiters,
            )
        });

        // Notify waiters out of the lock critical sectwion.
        for waiter in waiters {
            let _ = waiter.send(RawCacheEntry {
                inner: self.inner.clone(),
                ptr,
            });
        }

        // Deallocate data out of the lock critical section.
        if let Some(listener) = self.inner.event_listener.as_ref() {
            for Data { key, value, .. } in garbages {
                listener.on_memory_release(key, value);
            }
        }

        RawCacheEntry {
            inner: self.inner.clone(),
            ptr,
        }
    }

    #[fastrace::trace(name = "foyer::memory::raw::remove")]
    pub fn remove<Q>(&self, key: &Q) -> Option<RawCacheEntry<E, S, I>>
    where
        Q: Hash + Equivalent<E::Key> + ?Sized,
    {
        let hash = self.inner.hash_builder.hash_one(key);

        self.inner.shards[self.shard(hash)].write().with(|mut shard| {
            shard.remove(hash, key).map(|ptr| RawCacheEntry {
                inner: self.inner.clone(),
                ptr,
            })
        })
    }

    #[fastrace::trace(name = "foyer::memory::raw::get")]
    pub fn get<Q>(&self, key: &Q) -> Option<RawCacheEntry<E, S, I>>
    where
        Q: Hash + Equivalent<E::Key> + ?Sized,
    {
        let hash = self.inner.hash_builder.hash_one(key);

        let ptr = match E::acquire_operator() {
            Operator::Immutable => self.inner.shards[self.shard(hash)]
                .read()
                .with(|shard| shard.get_immutable(hash, key)),
            Operator::Mutable => self.inner.shards[self.shard(hash)]
                .write()
                .with(|mut shard| shard.get_mutable(hash, key)),
        }?;

        Some(RawCacheEntry {
            inner: self.inner.clone(),
            ptr,
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
            .with(|shard| shard.indexer.get(hash, key))
            .is_some()
    }

    #[fastrace::trace(name = "foyer::memory::raw::touch")]
    pub fn touch<Q>(&self, key: &Q) -> bool
    where
        Q: Hash + Equivalent<E::Key> + ?Sized,
    {
        let hash = self.inner.hash_builder.hash_one(key);

        match E::acquire_operator() {
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
        self.inner
            .shards
            .iter()
            .map(|shard| match E::acquire_operator() {
                Operator::Immutable => shard.read().usage,
                Operator::Mutable => shard.write().usage,
            })
            .sum()
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
    ptr: NonNull<Record<E>>,
}

impl<E, S, I> Debug for RawCacheEntry<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawCacheEntry").field("ptr", &self.ptr).finish()
    }
}

impl<E, S, I> Drop for RawCacheEntry<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    fn drop(&mut self) {
        let record = unsafe { self.ptr.as_ref() };

        if record.dec_ref_cas() == -1 {
            let hash = record.hash();
            let shard = hash as usize % self.inner.shards.len();
            let garbage = self.inner.shards[shard]
                .write()
                .with(|mut shard| shard.reclaim(self.ptr, true));
            // Do not deallocate data within the lock section.
            if let Some(listener) = self.inner.event_listener.as_ref() {
                if let Some(Data { key, value, .. }) = garbage {
                    listener.on_memory_release(key, value);
                }
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
        let old = unsafe { self.ptr.as_ref() }.inc_refs(1);
        assert!(old > 0);
        Self {
            inner: self.inner.clone(),
            ptr: self.ptr,
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
        unsafe { self.ptr.as_ref() }.hash()
    }

    pub fn key(&self) -> &E::Key {
        unsafe { self.ptr.as_ref() }.key()
    }

    pub fn value(&self) -> &E::Value {
        unsafe { self.ptr.as_ref() }.value()
    }

    pub fn hint(&self) -> &E::Hint {
        unsafe { self.ptr.as_ref() }.hint()
    }

    pub fn weight(&self) -> usize {
        unsafe { self.ptr.as_ref() }.weight()
    }

    pub fn refs(&self) -> isize {
        unsafe { self.ptr.as_ref() }.refs()
    }

    pub fn is_outdated(&self) -> bool {
        !unsafe { self.ptr.as_ref() }.is_in_indexer()
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
    Hit(NonNull<Record<E>>),
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
            Operator::Immutable => self.inner.shards[self.shard(hash)].read().fetch_immutable(hash, &key),
            Operator::Mutable => self.inner.shards[self.shard(hash)].write().fetch_mutable(hash, &key),
        };

        match raw {
            RawShardFetch::Hit(ptr) => {
                return RawFetch::new(RawFetchInner::Hit(Some(RawCacheEntry {
                    inner: self.inner.clone(),
                    ptr,
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

    #[test]
    fn test_send_sync_static() {
        is_send_sync_static::<RawCache<Fifo<(), ()>>>();
        is_send_sync_static::<RawCache<S3Fifo<(), ()>>>();
        is_send_sync_static::<RawCache<Lru<(), ()>>>();
        is_send_sync_static::<RawCache<Lfu<(), ()>>>();
    }

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
            eviction_config: FifoConfig,
            slab_initial_capacity: 0,
            slab_segment_size: 16 * 1024,
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
            slab_initial_capacity: 0,
            slab_segment_size: 16 * 1024,
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
            slab_initial_capacity: 0,
            slab_segment_size: 16 * 1024,
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
            slab_initial_capacity: 0,
            slab_segment_size: 16 * 1024,
            hash_builder: Default::default(),
            weighter: Arc::new(|_, _| 1),
            event_listener: None,
        });
        let hints = vec![LfuHint];
        fuzzy(cache, hints);
    }
}