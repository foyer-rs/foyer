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
    ops::Deref,
    ptr::NonNull,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use ahash::RandomState;
use foyer_common::{
    code::{HashBuilder, Key, Value},
    metrics::Metrics,
    object_pool::ObjectPool,
    strict_assert, strict_assert_eq,
};
use futures::FutureExt;
use hashbrown::hash_map::{Entry as HashMapEntry, HashMap};
use itertools::Itertools;
use parking_lot::Mutex;
use tokio::{sync::oneshot, task::JoinHandle};

use crate::{
    eviction::Eviction,
    handle::{Handle, HandleExt, KeyedHandle},
    indexer::Indexer,
    CacheContext,
};

// TODO(MrCroxx): Use `trait_alias` after stable.
/// The weighter for the in-memory cache.
///
/// The weighter is used to calculate the weight of the cache entry.
pub trait Weighter<K, V>: Fn(&K, &V) -> usize + Send + Sync + 'static {}
impl<K, V, T> Weighter<K, V> for T where T: Fn(&K, &V) -> usize + Send + Sync + 'static {}

struct CacheSharedState<T> {
    metrics: Arc<Metrics>,
    /// The object pool to avoid frequent handle allocating, shared by all shards.
    object_pool: ObjectPool<Box<T>>,
}

// TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
#[allow(clippy::type_complexity)]
struct CacheShard<K, V, E, I, S>
where
    K: Key,
    V: Value,
    E: Eviction,
    E::Handle: KeyedHandle<Key = K, Data = (K, V)>,
    I: Indexer<Key = K, Handle = E::Handle>,
    S: HashBuilder,
{
    indexer: I,
    eviction: E,

    capacity: usize,
    usage: Arc<AtomicUsize>,

    waiters: HashMap<K, Vec<oneshot::Sender<GenericCacheEntry<K, V, E, I, S>>>>,

    state: Arc<CacheSharedState<E::Handle>>,
}

impl<K, V, E, I, S> CacheShard<K, V, E, I, S>
where
    K: Key,
    V: Value,
    E: Eviction,
    E::Handle: KeyedHandle<Key = K, Data = (K, V)>,
    I: Indexer<Key = K, Handle = E::Handle>,
    S: HashBuilder,
{
    fn new(
        capacity: usize,
        eviction_config: &E::Config,
        usage: Arc<AtomicUsize>,
        context: Arc<CacheSharedState<E::Handle>>,
    ) -> Self {
        let indexer = I::new();
        let eviction = unsafe { E::new(capacity, eviction_config) };
        let waiters = HashMap::default();
        Self {
            indexer,
            eviction,
            capacity,
            usage,
            waiters,
            state: context,
        }
    }

    /// Insert a new entry into the cache. The handle for the new entry is returned.
    // TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
    #[allow(clippy::type_complexity)]
    #[allow(clippy::too_many_arguments)]
    unsafe fn emplace(
        &mut self,
        hash: u64,
        key: K,
        value: V,
        weight: usize,
        context: <E::Handle as Handle>::Context,
        deposit: bool,
        last_reference_entries: &mut Vec<(K, V, <E::Handle as Handle>::Context, usize)>,
    ) -> NonNull<E::Handle> {
        let mut handle = self.state.object_pool.acquire();
        strict_assert!(!handle.base().has_refs());
        strict_assert!(!handle.base().is_in_indexer());
        strict_assert!(!handle.base().is_in_eviction());

        handle.init(hash, (key, value), weight, context);
        let mut ptr = unsafe { NonNull::new_unchecked(Box::into_raw(handle)) };

        self.evict(weight, last_reference_entries);

        strict_assert!(!ptr.as_ref().base().is_in_indexer());
        if let Some(old) = self.indexer.insert(ptr) {
            self.state.metrics.memory_replace.increment(1);

            strict_assert!(!old.as_ref().base().is_in_indexer());
            if old.as_ref().base().is_in_eviction() {
                self.eviction.remove(old);
            }
            strict_assert!(!old.as_ref().base().is_in_eviction());
            // Because the `old` handle is removed from the indexer, it will not be reinserted again.
            if let Some(entry) = self.try_release_handle(old, false) {
                last_reference_entries.push(entry);
            }
        } else {
            self.state.metrics.memory_insert.increment(1);
        }
        strict_assert!(ptr.as_ref().base().is_in_indexer());

        ptr.as_mut().base_mut().set_deposit(deposit);
        if !deposit {
            self.eviction.push(ptr);
            strict_assert!(ptr.as_ref().base().is_in_eviction());
        }

        self.usage.fetch_add(weight, Ordering::Relaxed);
        self.state.metrics.memory_usage.increment(weight as f64);
        ptr.as_mut().base_mut().inc_refs();

        ptr
    }

    unsafe fn get<Q>(&mut self, hash: u64, key: &Q) -> Option<NonNull<E::Handle>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut ptr = match self.indexer.get(hash, key) {
            Some(ptr) => {
                self.state.metrics.memory_hit.increment(1);
                ptr
            }
            None => {
                self.state.metrics.memory_miss.increment(1);
                return None;
            }
        };
        let base = ptr.as_mut().base_mut();
        strict_assert!(base.is_in_indexer());

        base.set_deposit(false);
        base.inc_refs();
        self.eviction.acquire(ptr);

        Some(ptr)
    }

    unsafe fn contains<Q>(&mut self, hash: u64, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.indexer.get(hash, key).is_some()
    }

    unsafe fn touch<Q>(&mut self, hash: u64, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let res = self.indexer.get(hash, key);
        if let Some(ptr) = res {
            self.eviction.acquire(ptr);
        }
        res.is_some()
    }

    /// Remove a key from the cache.
    ///
    /// Return `Some(..)` if the handle is released, or `None` if the handle is still in use.
    unsafe fn remove<Q>(&mut self, hash: u64, key: &Q) -> Option<NonNull<E::Handle>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut ptr = self.indexer.remove(hash, key)?;
        let handle = ptr.as_mut();

        self.state.metrics.memory_remove.increment(1);

        if handle.base().is_in_eviction() {
            self.eviction.remove(ptr);
        }

        strict_assert!(!handle.base().is_in_indexer());
        strict_assert!(!handle.base().is_in_eviction());

        handle.base_mut().inc_refs();

        Some(ptr)
    }

    /// Remove a key based on the eviction algorithm if exists.s
    unsafe fn pop(&mut self) -> Option<NonNull<E::Handle>> {
        let ptr = self.eviction.pop()?;

        let handle = ptr.as_ref();

        // If the `ptr` is in the eviction container, it must be the latest version of the key and in the indexer.
        let p = self.remove(handle.base().hash(), handle.key()).unwrap();
        strict_assert_eq!(ptr, p);

        Some(ptr)
    }

    /// Clear all cache entries.
    // TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
    #[allow(clippy::type_complexity)]
    unsafe fn clear(&mut self, last_reference_entries: &mut Vec<(K, V, <E::Handle as Handle>::Context, usize)>) {
        // TODO(MrCroxx): Avoid collecting here?
        let ptrs = self.indexer.drain().collect_vec();
        let eptrs = self.eviction.clear();

        // Assert that the handles in the indexer covers the handles in the eviction container.
        if cfg!(debug_assertions) {
            use std::{collections::HashSet as StdHashSet, hash::RandomState as StdRandomState};
            let ptrs: StdHashSet<_, StdRandomState> = StdHashSet::from_iter(ptrs.iter().copied());
            let eptrs: StdHashSet<_, StdRandomState> = StdHashSet::from_iter(eptrs.iter().copied());
            assert!((&eptrs - &ptrs).is_empty());
        }

        self.state.metrics.memory_remove.increment(ptrs.len() as _);

        // The handles in the indexer covers the handles in the eviction container.
        // So only the handles drained from the indexer need to be released.
        for ptr in ptrs {
            strict_assert!(!ptr.as_ref().base().is_in_indexer());
            if let Some(entry) = self.try_release_handle(ptr, false) {
                last_reference_entries.push(entry);
            }
        }
    }

    // TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
    #[allow(clippy::type_complexity)]
    unsafe fn evict(
        &mut self,
        weight: usize,
        last_reference_entries: &mut Vec<(K, V, <E::Handle as Handle>::Context, usize)>,
    ) {
        // TODO(MrCroxx): Use `let_chains` here after it is stable.
        while self.usage.load(Ordering::Relaxed) + weight > self.capacity {
            let evicted = match self.eviction.pop() {
                Some(evicted) => evicted,
                None => break,
            };
            self.state.metrics.memory_evict.increment(1);
            let base = evicted.as_ref().base();
            strict_assert!(base.is_in_indexer());
            strict_assert!(!base.is_in_eviction());
            if let Some(entry) = self.try_release_handle(evicted, false) {
                last_reference_entries.push(entry);
            }
        }
    }

    /// Release a handle used by an external user.
    ///
    /// Return `Some(..)` if the handle is released, or `None` if the handle is still in use.
    // TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
    #[allow(clippy::type_complexity)]
    unsafe fn try_release_external_handle(
        &mut self,
        mut ptr: NonNull<E::Handle>,
    ) -> Option<(K, V, <E::Handle as Handle>::Context, usize)> {
        ptr.as_mut().base_mut().dec_refs();
        self.try_release_handle(ptr, true)
    }

    /// Try release handle if there is no external reference and no reinsertion is needed.
    ///
    /// Return the entry if the handle is released.
    ///
    /// Recycle it if possible.
    // TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
    #[allow(clippy::type_complexity)]
    unsafe fn try_release_handle(
        &mut self,
        mut ptr: NonNull<E::Handle>,
        reinsert: bool,
    ) -> Option<(K, V, <E::Handle as Handle>::Context, usize)> {
        let handle = ptr.as_mut();

        if handle.base().has_refs() {
            return None;
        }

        strict_assert!(handle.base().is_inited());
        strict_assert!(!handle.base().has_refs());

        if handle.base().is_deposit() {
            strict_assert!(!handle.base().is_in_eviction());
            self.indexer.remove(handle.base().hash(), handle.key());
            strict_assert!(!handle.base().is_in_indexer());
        }

        // If the entry is not updated or removed from the cache, try to reinsert it or remove it from the indexer and
        // the eviction container.
        if handle.base().is_in_indexer() {
            // The usage is higher than the capacity means most handles are held externally,
            // the cache shard cannot release enough weight for the new inserted entries.
            // In this case, the reinsertion should be given up.
            if reinsert && self.usage.load(Ordering::Relaxed) <= self.capacity {
                let was_in_eviction = handle.base().is_in_eviction();
                self.eviction.release(ptr);
                if ptr.as_ref().base().is_in_eviction() {
                    if was_in_eviction {
                        self.state.metrics.memory_reinsert.increment(1);
                    }
                    return None;
                }
            }

            // If the entry has not been reinserted, remove it from the indexer and the eviction container (if needed).
            self.indexer.remove(handle.base().hash(), handle.key());
            if ptr.as_ref().base().is_in_eviction() {
                self.eviction.remove(ptr);
            }
        }

        // Here the handle is neither in the indexer nor in the eviction container.
        strict_assert!(!handle.base().is_in_indexer());
        strict_assert!(!handle.base().is_in_eviction());
        strict_assert!(!handle.base().has_refs());

        self.state.metrics.memory_release.increment(1);

        self.usage.fetch_sub(handle.base().weight(), Ordering::Relaxed);
        self.state.metrics.memory_usage.decrement(handle.base().weight() as f64);
        let ((key, value), context, weight) = handle.base_mut().take();

        let handle = Box::from_raw(ptr.as_ptr());
        self.state.object_pool.release(handle);

        Some((key, value, context, weight))
    }
}

impl<K, V, E, I, S> Drop for CacheShard<K, V, E, I, S>
where
    K: Key,
    V: Value,
    E: Eviction,
    E::Handle: KeyedHandle<Key = K, Data = (K, V)>,
    I: Indexer<Key = K, Handle = E::Handle>,
    S: HashBuilder,
{
    fn drop(&mut self) {
        unsafe { self.clear(&mut vec![]) }
    }
}

pub struct GenericCacheConfig<K, V, E, S = RandomState>
where
    K: Key,
    V: Value,
    E: Eviction,
    E::Handle: KeyedHandle<Key = K, Data = (K, V)>,
    S: HashBuilder,
{
    pub name: String,
    pub capacity: usize,
    pub shards: usize,
    pub eviction_config: E::Config,
    pub object_pool_capacity: usize,
    pub hash_builder: S,
    pub weighter: Arc<dyn Weighter<K, V>>,
}

// TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
#[allow(clippy::type_complexity)]
pub enum GenericFetch<K, V, E, I, S, ER>
where
    K: Key,
    V: Value,
    E: Eviction,
    E::Handle: KeyedHandle<Key = K, Data = (K, V)>,
    I: Indexer<Key = K, Handle = E::Handle>,
    S: HashBuilder,
{
    Invalid,
    Hit(GenericCacheEntry<K, V, E, I, S>),
    Wait(oneshot::Receiver<GenericCacheEntry<K, V, E, I, S>>),
    Miss(JoinHandle<std::result::Result<GenericCacheEntry<K, V, E, I, S>, ER>>),
}

impl<K, V, E, I, S, ER> Default for GenericFetch<K, V, E, I, S, ER>
where
    K: Key,
    V: Value,
    E: Eviction,
    E::Handle: KeyedHandle<Key = K, Data = (K, V)>,
    I: Indexer<Key = K, Handle = E::Handle>,
    S: HashBuilder,
{
    fn default() -> Self {
        Self::Invalid
    }
}

impl<K, V, E, I, S, ER> Future for GenericFetch<K, V, E, I, S, ER>
where
    K: Key,
    V: Value,
    E: Eviction,
    E::Handle: KeyedHandle<Key = K, Data = (K, V)>,
    I: Indexer<Key = K, Handle = E::Handle>,
    S: HashBuilder,
    ER: From<oneshot::error::RecvError>,
{
    type Output = std::result::Result<GenericCacheEntry<K, V, E, I, S>, ER>;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        match &mut *self {
            Self::Invalid => unreachable!(),
            Self::Hit(_) => std::task::Poll::Ready(Ok(match std::mem::take(&mut *self) {
                GenericFetch::Hit(entry) => entry,
                _ => unreachable!(),
            })),
            Self::Wait(waiter) => waiter.poll_unpin(cx).map_err(|err| err.into()),
            Self::Miss(join_handle) => join_handle.poll_unpin(cx).map(|join_result| join_result.unwrap()),
        }
    }
}

// TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
#[allow(clippy::type_complexity)]
pub struct GenericCache<K, V, E, I, S = RandomState>
where
    K: Key,
    V: Value,
    E: Eviction,
    E::Handle: KeyedHandle<Key = K, Data = (K, V)>,
    I: Indexer<Key = K, Handle = E::Handle>,
    S: HashBuilder,
{
    shards: Vec<Mutex<CacheShard<K, V, E, I, S>>>,

    capacity: usize,
    usages: Vec<Arc<AtomicUsize>>,

    context: Arc<CacheSharedState<E::Handle>>,

    hash_builder: S,
    weighter: Arc<dyn Weighter<K, V>>,

    _metrics: Arc<Metrics>,
}

impl<K, V, E, I, S> GenericCache<K, V, E, I, S>
where
    K: Key,
    V: Value,
    E: Eviction,
    E::Handle: KeyedHandle<Key = K, Data = (K, V)>,
    I: Indexer<Key = K, Handle = E::Handle>,
    S: HashBuilder,
{
    pub fn new(config: GenericCacheConfig<K, V, E, S>) -> Self {
        let metrics = Arc::new(Metrics::new(&config.name));

        let usages = (0..config.shards).map(|_| Arc::new(AtomicUsize::new(0))).collect_vec();
        let context = Arc::new(CacheSharedState {
            metrics: metrics.clone(),
            object_pool: ObjectPool::new_with_create(config.object_pool_capacity, Box::default),
        });

        let shard_capacity = config.capacity / config.shards;

        let shards = usages
            .iter()
            .map(|usage| CacheShard::new(shard_capacity, &config.eviction_config, usage.clone(), context.clone()))
            .map(Mutex::new)
            .collect_vec();

        Self {
            shards,
            capacity: config.capacity,
            usages,
            context,
            hash_builder: config.hash_builder,
            weighter: config.weighter,
            _metrics: metrics,
        }
    }

    pub fn insert(self: &Arc<Self>, key: K, value: V) -> GenericCacheEntry<K, V, E, I, S> {
        self.insert_with_context(key, value, CacheContext::default())
    }

    pub fn insert_with_context(
        self: &Arc<Self>,
        key: K,
        value: V,
        context: CacheContext,
    ) -> GenericCacheEntry<K, V, E, I, S> {
        self.emplace(key, value, context, false)
    }

    pub fn deposit(self: &Arc<Self>, key: K, value: V) -> GenericCacheEntry<K, V, E, I, S> {
        self.deposit_with_context(key, value, CacheContext::default())
    }

    pub fn deposit_with_context(
        self: &Arc<Self>,
        key: K,
        value: V,
        context: CacheContext,
    ) -> GenericCacheEntry<K, V, E, I, S> {
        self.emplace(key, value, context, true)
    }

    fn emplace(
        self: &Arc<Self>,
        key: K,
        value: V,
        context: CacheContext,
        deposit: bool,
    ) -> GenericCacheEntry<K, V, E, I, S> {
        let hash = self.hash_builder.hash_one(&key);
        let weight = (self.weighter)(&key, &value);

        let mut to_deallocate = vec![];

        let (entry, waiters) = unsafe {
            let mut shard = self.shards[hash as usize % self.shards.len()].lock();
            let waiters = shard.waiters.remove(&key);
            let mut ptr = shard.emplace(hash, key, value, weight, context.into(), deposit, &mut to_deallocate);
            if let Some(waiters) = waiters.as_ref() {
                // Increase the reference count within the lock section.
                ptr.as_mut().base_mut().inc_refs_by(waiters.len());
            }
            let entry = GenericCacheEntry {
                cache: self.clone(),
                ptr,
            };
            (entry, waiters)
        };

        if let Some(waiters) = waiters {
            for waiter in waiters {
                let _ = waiter.send(GenericCacheEntry {
                    cache: self.clone(),
                    ptr: entry.ptr,
                });
            }
        }

        // Do not deallocate data within the lock section.
        drop(to_deallocate);

        entry
    }

    pub fn remove<Q>(self: &Arc<Self>, key: &Q) -> Option<GenericCacheEntry<K, V, E, I, S>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hash_builder.hash_one(key);

        unsafe {
            let mut shard = self.shards[hash as usize % self.shards.len()].lock();
            shard.remove(hash, key).map(|ptr| GenericCacheEntry {
                cache: self.clone(),
                ptr,
            })
        }
    }

    pub fn pop(self: &Arc<Self>) -> Option<GenericCacheEntry<K, V, E, I, S>> {
        let mut shards = self.shards.iter().map(|shard| shard.lock()).collect_vec();

        let shard = self
            .usages
            .iter()
            .enumerate()
            .fold((None, 0), |(largest_shard, largest_shard_usage), (shard, usage)| {
                let usage = usage.load(Ordering::Acquire);
                if usage > largest_shard_usage {
                    (Some(shard), usage)
                } else {
                    (largest_shard, largest_shard_usage)
                }
            })
            .0?;

        unsafe {
            shards[shard].pop().map(|ptr| GenericCacheEntry {
                cache: self.clone(),
                ptr,
            })
        }
    }

    pub fn pop_corase(self: &Arc<Self>) -> Option<GenericCacheEntry<K, V, E, I, S>> {
        let shard = self
            .usages
            .iter()
            .enumerate()
            .fold((None, 0), |(largest_shard, largest_shard_usage), (shard, usage)| {
                let usage = usage.load(Ordering::Relaxed);
                if usage > largest_shard_usage {
                    (Some(shard), usage)
                } else {
                    (largest_shard, largest_shard_usage)
                }
            })
            .0?;

        unsafe {
            let mut shard = self.shards[shard].lock();
            shard.pop().map(|ptr| GenericCacheEntry {
                cache: self.clone(),
                ptr,
            })
        }
    }

    pub fn get<Q>(self: &Arc<Self>, key: &Q) -> Option<GenericCacheEntry<K, V, E, I, S>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hash_builder.hash_one(key);

        unsafe {
            let mut shard = self.shards[hash as usize % self.shards.len()].lock();
            shard.get(hash, key).map(|ptr| GenericCacheEntry {
                cache: self.clone(),
                ptr,
            })
        }
    }

    pub fn contains<Q>(self: &Arc<Self>, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hash_builder.hash_one(key);

        unsafe {
            let mut shard = self.shards[hash as usize % self.shards.len()].lock();
            shard.contains(hash, key)
        }
    }

    pub fn touch<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hash_builder.hash_one(key);

        unsafe {
            let mut shard = self.shards[hash as usize % self.shards.len()].lock();
            shard.touch(hash, key)
        }
    }

    pub fn clear(&self) {
        let mut to_deallocate = vec![];
        for shard in self.shards.iter() {
            let mut shard = shard.lock();
            unsafe { shard.clear(&mut to_deallocate) };
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn usage(&self) -> usize {
        self.usages.iter().map(|usage| usage.load(Ordering::Relaxed)).sum()
    }

    pub fn metrics(&self) -> &Metrics {
        &self.context.metrics
    }

    pub fn hash_builder(&self) -> &S {
        &self.hash_builder
    }

    unsafe fn try_release_external_handle(&self, ptr: NonNull<E::Handle>) {
        let entry = {
            let base = ptr.as_ref().base();
            let mut shard = self.shards[base.hash() as usize % self.shards.len()].lock();
            shard.try_release_external_handle(ptr)
        };

        // Do not deallocate data within the lock section.
        drop(entry);
    }
}

// TODO(MrCroxx): use `hashbrown::HashTable` with `Handle` may relax the `Clone` bound?
impl<K, V, E, I, S> GenericCache<K, V, E, I, S>
where
    K: Key + Clone,
    V: Value,
    E: Eviction,
    E::Handle: KeyedHandle<Key = K, Data = (K, V)>,
    I: Indexer<Key = K, Handle = E::Handle>,
    S: HashBuilder,
{
    pub fn fetch<F, FU, ER>(self: &Arc<Self>, key: K, fetch: F) -> GenericFetch<K, V, E, I, S, ER>
    where
        F: FnOnce() -> FU,
        FU: Future<Output = std::result::Result<(V, CacheContext), ER>> + Send + 'static,
        ER: Send + 'static + Debug,
    {
        let hash = self.hash_builder.hash_one(&key);

        unsafe {
            let mut shard = self.shards[hash as usize % self.shards.len()].lock();
            if let Some(ptr) = shard.get(hash, &key) {
                return GenericFetch::Hit(GenericCacheEntry {
                    cache: self.clone(),
                    ptr,
                });
            }
            let entry = match shard.waiters.entry(key.clone()) {
                HashMapEntry::Occupied(mut o) => {
                    let (tx, rx) = oneshot::channel();
                    o.get_mut().push(tx);
                    GenericFetch::Wait(rx)
                }
                HashMapEntry::Vacant(v) => {
                    v.insert(vec![]);
                    let cache = self.clone();
                    let future = fetch();
                    let join = tokio::spawn(async move {
                        let (value, context) = match future.await {
                            Ok((value, context)) => (value, context),
                            Err(e) => {
                                let mut shard = cache.shards[hash as usize % cache.shards.len()].lock();
                                tracing::debug!(
                                    "[fetch]: error raise while fetching, all waiter are dropped, err: {e:?}"
                                );
                                shard.waiters.remove(&key);
                                return Err(e);
                            }
                        };
                        let entry = cache.insert_with_context(key, value, context);
                        Ok(entry)
                    });
                    GenericFetch::Miss(join)
                }
            };
            match entry {
                GenericFetch::Wait(_) => shard.state.metrics.memory_queue.increment(1),
                GenericFetch::Miss(_) => shard.state.metrics.memory_fetch.increment(1),
                _ => unreachable!(),
            };
            entry
        }
    }
}

pub struct GenericCacheEntry<K, V, E, I, S = RandomState>
where
    K: Key,
    V: Value,
    E: Eviction,
    E::Handle: KeyedHandle<Key = K, Data = (K, V)>,
    I: Indexer<Key = K, Handle = E::Handle>,
    S: HashBuilder,
{
    cache: Arc<GenericCache<K, V, E, I, S>>,
    ptr: NonNull<E::Handle>,
}

impl<K, V, E, I, S> Debug for GenericCacheEntry<K, V, E, I, S>
where
    K: Key,
    V: Value,
    E: Eviction,
    E::Handle: KeyedHandle<Key = K, Data = (K, V)>,
    I: Indexer<Key = K, Handle = E::Handle>,
    S: HashBuilder,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenericCacheEntry").finish()
    }
}

impl<K, V, E, I, S> GenericCacheEntry<K, V, E, I, S>
where
    K: Key,
    V: Value,
    E: Eviction,
    E::Handle: KeyedHandle<Key = K, Data = (K, V)>,
    I: Indexer<Key = K, Handle = E::Handle>,
    S: HashBuilder,
{
    pub fn hash(&self) -> u64 {
        unsafe { self.ptr.as_ref().base().hash() }
    }

    pub fn key(&self) -> &K {
        unsafe { &self.ptr.as_ref().base().data_unwrap_unchecked().0 }
    }

    pub fn value(&self) -> &V {
        unsafe { &self.ptr.as_ref().base().data_unwrap_unchecked().1 }
    }

    pub fn context(&self) -> &<E::Handle as Handle>::Context {
        unsafe { self.ptr.as_ref().base().context() }
    }

    pub fn weight(&self) -> usize {
        unsafe { self.ptr.as_ref().base().weight() }
    }

    pub fn refs(&self) -> usize {
        unsafe { self.ptr.as_ref().base().refs() }
    }

    pub fn is_outdated(&self) -> bool {
        unsafe { !self.ptr.as_ref().base().is_in_indexer() }
    }
}

impl<K, V, E, I, S> Clone for GenericCacheEntry<K, V, E, I, S>
where
    K: Key,
    V: Value,
    E: Eviction,
    E::Handle: KeyedHandle<Key = K, Data = (K, V)>,
    I: Indexer<Key = K, Handle = E::Handle>,
    S: HashBuilder,
{
    fn clone(&self) -> Self {
        let mut ptr = self.ptr;

        unsafe {
            let base = ptr.as_mut().base_mut();
            strict_assert!(base.has_refs());
            base.inc_refs();
        }

        Self {
            cache: self.cache.clone(),
            ptr,
        }
    }
}

impl<K, V, E, I, S> Drop for GenericCacheEntry<K, V, E, I, S>
where
    K: Key,
    V: Value,
    E: Eviction,
    E::Handle: KeyedHandle<Key = K, Data = (K, V)>,
    I: Indexer<Key = K, Handle = E::Handle>,
    S: HashBuilder,
{
    fn drop(&mut self) {
        unsafe { self.cache.try_release_external_handle(self.ptr) }
    }
}

impl<K, V, E, I, S> Deref for GenericCacheEntry<K, V, E, I, S>
where
    K: Key,
    V: Value,
    E: Eviction,
    E::Handle: KeyedHandle<Key = K, Data = (K, V)>,
    I: Indexer<Key = K, Handle = E::Handle>,
    S: HashBuilder,
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        self.value()
    }
}

unsafe impl<K, V, E, I, S> Send for GenericCacheEntry<K, V, E, I, S>
where
    K: Key,
    V: Value,
    E: Eviction,
    E::Handle: KeyedHandle<Key = K, Data = (K, V)>,
    I: Indexer<Key = K, Handle = E::Handle>,
    S: HashBuilder,
{
}
unsafe impl<K, V, E, I, S> Sync for GenericCacheEntry<K, V, E, I, S>
where
    K: Key,
    V: Value,
    E: Eviction,
    E::Handle: KeyedHandle<Key = K, Data = (K, V)>,
    I: Indexer<Key = K, Handle = E::Handle>,
    S: HashBuilder,
{
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::future::try_join_all;
    use rand::{rngs::SmallRng, Rng, RngCore, SeedableRng};

    use super::*;
    use crate::{
        cache::{FifoCache, FifoCacheEntry, LfuCache, LruCache, LruCacheEntry, S3FifoCache},
        eviction::{
            fifo::{FifoConfig, FifoHandle},
            lru::LruConfig,
            test_utils::TestEviction,
        },
        indexer::HashTableIndexer,
        LfuConfig, S3FifoConfig,
    };

    fn is_send_sync_static<T: Send + Sync + 'static>() {}

    #[test]
    fn test_send_sync_static() {
        is_send_sync_static::<FifoCache<(), ()>>();
        is_send_sync_static::<LruCache<(), ()>>();
    }

    // TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
    #[allow(clippy::type_complexity)]
    fn fuzzy<E>(cache: Arc<GenericCache<u64, u64, E, HashTableIndexer<u64, E::Handle>>>)
    where
        E: Eviction,
        E::Handle: KeyedHandle<Key = u64, Data = (u64, u64)>,
    {
        let handles = (0..8)
            .map(|i| {
                let c = cache.clone();
                std::thread::spawn(move || {
                    let mut rng = SmallRng::seed_from_u64(i);
                    for _ in 0..100000 {
                        let key = rng.next_u64();
                        if let Some(entry) = c.get(&key) {
                            assert_eq!(key, *entry);
                            drop(entry);
                            continue;
                        }
                        c.insert_with_context(
                            key,
                            key,
                            if rng.gen_bool(0.5) {
                                CacheContext::Default
                            } else {
                                CacheContext::LowPriority
                            },
                        );
                    }
                })
            })
            .collect_vec();

        handles.into_iter().for_each(|handle| handle.join().unwrap());

        assert_eq!(cache.usage(), cache.capacity());
    }

    #[test]
    fn test_fifo_cache_fuzzy() {
        fuzzy(Arc::new(FifoCache::<u64, u64>::new(GenericCacheConfig {
            name: "test".to_string(),
            capacity: 256,
            shards: 4,
            eviction_config: FifoConfig::default(),
            object_pool_capacity: 16,
            hash_builder: RandomState::default(),
            weighter: Arc::new(|_, _| 1),
        })))
    }

    #[test]
    fn test_lru_cache_fuzzy() {
        fuzzy(Arc::new(LruCache::<u64, u64>::new(GenericCacheConfig {
            name: "test".to_string(),
            capacity: 256,
            shards: 4,
            eviction_config: LruConfig::default(),
            object_pool_capacity: 16,
            hash_builder: RandomState::default(),
            weighter: Arc::new(|_, _| 1),
        })))
    }

    #[test]
    fn test_lfu_cache_fuzzy() {
        fuzzy(Arc::new(LfuCache::<u64, u64>::new(GenericCacheConfig {
            name: "test".to_string(),
            capacity: 256,
            shards: 4,
            eviction_config: LfuConfig::default(),
            object_pool_capacity: 16,
            hash_builder: RandomState::default(),
            weighter: Arc::new(|_, _| 1),
        })))
    }

    #[test]
    fn test_s3fifo_cache_fuzzy() {
        fuzzy(Arc::new(S3FifoCache::<u64, u64>::new(GenericCacheConfig {
            name: "test".to_string(),
            capacity: 256,
            shards: 4,
            eviction_config: S3FifoConfig::default(),
            object_pool_capacity: 16,
            hash_builder: RandomState::default(),
            weighter: Arc::new(|_, _| 1),
        })))
    }

    fn fifo(capacity: usize) -> Arc<FifoCache<u64, String>> {
        let config = GenericCacheConfig {
            name: "test".to_string(),
            capacity,
            shards: 1,
            eviction_config: FifoConfig {},
            object_pool_capacity: 1,
            hash_builder: RandomState::default(),
            weighter: Arc::new(|_, v: &String| v.len()),
        };
        Arc::new(FifoCache::<u64, String>::new(config))
    }

    fn lru(capacity: usize) -> Arc<LruCache<u64, String>> {
        let config = GenericCacheConfig {
            name: "test".to_string(),
            capacity,
            shards: 1,
            eviction_config: LruConfig {
                high_priority_pool_ratio: 0.0,
            },
            object_pool_capacity: 1,
            hash_builder: RandomState::default(),
            weighter: Arc::new(|_, v: &String| v.len()),
        };
        Arc::new(LruCache::<u64, String>::new(config))
    }

    fn insert_fifo(cache: &Arc<FifoCache<u64, String>>, key: u64, value: &str) -> FifoCacheEntry<u64, String> {
        cache.insert(key, value.to_string())
    }

    fn insert_lru(cache: &Arc<LruCache<u64, String>>, key: u64, value: &str) -> LruCacheEntry<u64, String> {
        cache.insert(key, value.to_string())
    }

    #[test]
    fn test_reference_count() {
        let cache = fifo(100);

        let refs = |ptr: NonNull<FifoHandle<(u64, String)>>| unsafe { ptr.as_ref().base().refs() };

        let e1 = insert_fifo(&cache, 42, "the answer to life, the universe, and everything");
        let ptr = e1.ptr;
        assert_eq!(refs(ptr), 1);

        let e2 = cache.get(&42).unwrap();
        assert_eq!(refs(ptr), 2);

        let e3 = e2.clone();
        assert_eq!(refs(ptr), 3);

        drop(e2);
        assert_eq!(refs(ptr), 2);

        drop(e3);
        assert_eq!(refs(ptr), 1);

        drop(e1);
        assert_eq!(refs(ptr), 0);
    }

    #[test]
    fn test_deposit() {
        let cache = lru(10);
        let e = cache.deposit(42, "answer".to_string());
        assert_eq!(cache.usage(), 6);
        drop(e);
        assert_eq!(cache.usage(), 0);

        let e = cache.deposit(42, "answer".to_string());
        assert_eq!(cache.usage(), 6);
        assert_eq!(cache.get(&42).unwrap().value(), "answer");
        drop(e);
        assert_eq!(cache.usage(), 6);
        assert_eq!(cache.get(&42).unwrap().value(), "answer");
    }

    #[test]
    fn test_replace() {
        let cache = fifo(10);

        insert_fifo(&cache, 114, "xx");
        assert_eq!(cache.usage(), 2);

        insert_fifo(&cache, 514, "QwQ");
        assert_eq!(cache.usage(), 5);

        insert_fifo(&cache, 114, "(0.0)");
        assert_eq!(cache.usage(), 8);

        assert_eq!(
            cache.shards[0].lock().eviction.dump(),
            vec![(514, "QwQ".to_string()), (114, "(0.0)".to_string())],
        );
    }

    #[test]
    fn test_replace_with_external_refs() {
        let cache = fifo(10);

        insert_fifo(&cache, 514, "QwQ");
        insert_fifo(&cache, 114, "(0.0)");

        let e4 = cache.get(&514).unwrap();
        let e5 = insert_fifo(&cache, 514, "bili");

        assert_eq!(e4.refs(), 1);
        assert_eq!(e5.refs(), 1);

        // remains: 514 => QwQ (3), 514 => bili (4)
        // evicted: 114 => (0.0) (5)
        assert_eq!(cache.usage(), 7);

        assert!(cache.get(&114).is_none());
        assert_eq!(cache.get(&514).unwrap().value(), "bili");
        assert_eq!(e4.value(), "QwQ");

        let e6 = cache.remove(&514).unwrap();
        assert_eq!(e6.value(), "bili");
        drop(e6);

        drop(e5);
        assert!(cache.get(&514).is_none());
        assert_eq!(e4.value(), "QwQ");

        assert_eq!(cache.usage(), 3);
        drop(e4);
        assert_eq!(cache.usage(), 0);
    }

    #[test]
    fn test_reinsert_while_all_referenced_lru() {
        let cache = lru(10);

        let e1 = insert_lru(&cache, 1, "111");
        let e2 = insert_lru(&cache, 2, "222");
        let e3 = insert_lru(&cache, 3, "333");
        assert_eq!(cache.usage(), 9);

        // No entry will be released because all of them are referenced externally.
        let e4 = insert_lru(&cache, 4, "444");
        assert_eq!(cache.usage(), 12);

        // `111`, `222` and `333` are evicted from the eviction container to make space for `444`.
        assert_eq!(cache.shards[0].lock().eviction.dump(), vec![(4, "444".to_string()),]);

        // `e1` cannot be reinserted for the usage has already exceeds the capacity.
        drop(e1);
        assert_eq!(cache.usage(), 9);

        // `222` and `333` will be reinserted
        drop(e2);
        drop(e3);
        assert_eq!(
            cache.shards[0].lock().eviction.dump(),
            vec![(4, "444".to_string()), (2, "222".to_string()), (3, "333".to_string()),]
        );
        assert_eq!(cache.usage(), 9);

        // `444` will be reinserted
        drop(e4);
        assert_eq!(
            cache.shards[0].lock().eviction.dump(),
            vec![(2, "222".to_string()), (3, "333".to_string()), (4, "444".to_string()),]
        );
        assert_eq!(cache.usage(), 9);
    }

    #[test]
    fn test_reinsert_while_all_referenced_fifo() {
        let cache = fifo(10);

        let e1 = insert_fifo(&cache, 1, "111");
        let e2 = insert_fifo(&cache, 2, "222");
        let e3 = insert_fifo(&cache, 3, "333");
        assert_eq!(cache.usage(), 9);

        // No entry will be released because all of them are referenced externally.
        let e4 = insert_fifo(&cache, 4, "444");
        assert_eq!(cache.usage(), 12);

        // `111`, `222` and `333` are evicted from the eviction container to make space for `444`.
        assert_eq!(cache.shards[0].lock().eviction.dump(), vec![(4, "444".to_string()),]);

        // `e1` cannot be reinserted for the usage has already exceeds the capacity.
        drop(e1);
        assert_eq!(cache.usage(), 9);

        // `222` and `333` will be not reinserted because fifo will ignore reinsert operations.
        drop([e2, e3, e4]);
        assert_eq!(cache.shards[0].lock().eviction.dump(), vec![(4, "444".to_string()),]);
        assert_eq!(cache.usage(), 3);

        // Note:
        //
        // For cache policy like FIFO, the entries will not be reinserted while all handles are referenced.
        // It's okay for this is not a common situation and is not supposed to happen in real workload.
    }

    #[test_log::test(tokio::test)]
    async fn test_fetch() {
        let cache = fifo(10);

        let fetch = |s: &'static str| async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok::<_, anyhow::Error>((s.to_string(), CacheContext::default()))
        };

        let e1s = try_join_all([
            cache.fetch(1, || fetch("111")),
            cache.fetch(1, || fetch("111")),
            cache.fetch(1, || fetch("111")),
        ])
        .await
        .unwrap();

        assert_eq!(e1s[0].value(), "111");
        assert_eq!(e1s[1].value(), "111");
        assert_eq!(e1s[2].value(), "111");

        let e1 = cache.fetch(1, || fetch("111")).await.unwrap();

        assert_eq!(e1.value(), "111");
        assert_eq!(e1.refs(), 4);

        let c = cache.clone();
        let h2 = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            c.insert(2, "222222".to_string())
        });
        let e2s = try_join_all([
            cache.fetch(2, || fetch("222")),
            cache.fetch(2, || fetch("222")),
            cache.fetch(2, || fetch("222")),
        ])
        .await
        .unwrap();
        let e2 = h2.await.unwrap();

        assert_eq!(e2s[0].value(), "222");
        assert_eq!(e2s[1].value(), "222222");
        assert_eq!(e2s[2].value(), "222222");
        assert_eq!(e2.value(), "222222");

        assert_eq!(e2s[0].refs(), 1);
        assert_eq!(e2s[1].refs(), 3);
        assert_eq!(e2s[2].refs(), 3);
        assert_eq!(e2.refs(), 3);

        let c = cache.clone();
        let h3a = tokio::spawn(async move { c.fetch(3, || fetch("333")).await.unwrap() });
        let c = cache.clone();
        let h3b = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            tokio::time::timeout(Duration::from_millis(10), c.fetch(3, || fetch("333"))).await
        });

        let _ = h3b.await.unwrap();
        let e3 = h3a.await.unwrap();
        assert_eq!(e3.value(), "333");
        assert_eq!(e3.refs(), 1);
    }
}
