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
    ops::Deref,
    ptr::NonNull,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use ahash::RandomState;
use foyer_common::{
    code::{Key, Value},
    object_pool::ObjectPool,
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
    metrics::Metrics,
    CacheContext,
};

// TODO(MrCroxx): Use `trait_alias` after stable.
pub trait Weighter<K, V>: Fn(&K, &V) -> usize + Send + Sync + 'static {}
impl<K, V, T> Weighter<K, V> for T where T: Fn(&K, &V) -> usize + Send + Sync + 'static {}

struct CacheSharedState<T> {
    metrics: Metrics,
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
    E::Handle: KeyedHandle<Key = Arc<K>, Data = (Arc<K>, Arc<V>)>,
    I: Indexer<Key = K, Handle = E::Handle>,
    S: BuildHasher + Send + Sync + 'static,
{
    indexer: I,
    eviction: E,

    capacity: usize,
    usage: Arc<AtomicUsize>,

    waiters: HashMap<Arc<K>, Vec<oneshot::Sender<Option<GenericCacheEntry<K, V, E, I, S>>>>>,

    state: Arc<CacheSharedState<E::Handle>>,
}

impl<K, V, E, I, S> CacheShard<K, V, E, I, S>
where
    K: Key,
    V: Value,
    E: Eviction,
    E::Handle: KeyedHandle<Key = Arc<K>, Data = (Arc<K>, Arc<V>)>,
    I: Indexer<Key = K, Handle = E::Handle>,
    S: BuildHasher + Send + Sync + 'static,
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
    unsafe fn emplace<AK, AV>(
        &mut self,
        hash: u64,
        key: AK,
        value: AV,
        weight: usize,
        context: <E::Handle as Handle>::Context,
        deposit: bool,
        last_reference_entries: &mut Vec<(Arc<K>, Arc<V>, <E::Handle as Handle>::Context, usize)>,
    ) -> NonNull<E::Handle>
    where
        AK: Into<Arc<K>>,
        AV: Into<Arc<V>>,
    {
        let key = key.into();
        let value = value.into();

        let mut handle = self.state.object_pool.acquire();
        handle.init(hash, (key.clone(), value), weight, context);
        let mut ptr = unsafe { NonNull::new_unchecked(Box::into_raw(handle)) };

        self.evict(weight, last_reference_entries);

        debug_assert!(!ptr.as_ref().base().is_in_indexer());
        if let Some(old) = self.indexer.insert(key, ptr) {
            self.state.metrics.replace.fetch_add(1, Ordering::Relaxed);

            debug_assert!(!old.as_ref().base().is_in_indexer());
            if old.as_ref().base().is_in_eviction() {
                self.eviction.remove(old);
            }
            debug_assert!(!old.as_ref().base().is_in_eviction());
            // Because the `old` handle is removed from the indexer, it will not be reinserted again.
            if let Some(entry) = self.try_release_handle(old, false) {
                last_reference_entries.push(entry);
            }
        } else {
            self.state.metrics.insert.fetch_add(1, Ordering::Relaxed);
        }
        debug_assert!(ptr.as_ref().base().is_in_indexer());

        if !deposit {
            self.eviction.push(ptr);
            debug_assert!(ptr.as_ref().base().is_in_eviction());
        }

        ptr.as_mut().base_mut().set_deposit(deposit);

        self.usage.fetch_add(weight, Ordering::Relaxed);
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
                self.state.metrics.hit.fetch_add(1, Ordering::Relaxed);
                ptr
            }
            None => {
                self.state.metrics.miss.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        };
        let base = ptr.as_mut().base_mut();
        debug_assert!(base.is_in_indexer());

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

        self.state.metrics.remove.fetch_add(1, Ordering::Relaxed);

        if handle.base().is_in_eviction() {
            self.eviction.remove(ptr);
        }

        debug_assert!(!handle.base().is_in_indexer());
        debug_assert!(!handle.base().is_in_eviction());

        handle.base_mut().inc_refs();

        Some(ptr)
    }

    /// Remove a key based on the eviction algorithm if exists.s
    unsafe fn pop(&mut self) -> Option<NonNull<E::Handle>> {
        let ptr = self.eviction.pop()?;

        let handle = ptr.as_ref();

        // If the `ptr` is in the eviction container, it must be the latest version of the key and in the indexer.
        let p = self.remove(handle.base().hash(), handle.key()).unwrap();
        debug_assert_eq!(ptr, p);

        Some(ptr)
    }

    /// Clear all cache entries.
    // TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
    #[allow(clippy::type_complexity)]
    unsafe fn clear(
        &mut self,
        last_reference_entries: &mut Vec<(Arc<K>, Arc<V>, <E::Handle as Handle>::Context, usize)>,
    ) {
        // TODO(MrCroxx): Avoid collecting here?
        let ptrs = self.indexer.drain().collect_vec();
        let eptrs = self.eviction.clear();

        // Assert that the handles in the indexer covers the handles in the eviction container.
        if cfg!(debug_assertions) {
            use std::{collections::HashSet as StdHashSet, hash::RandomState as StdRandomState};
            let ptrs: StdHashSet<_, StdRandomState> = StdHashSet::from_iter(ptrs.iter().copied());
            let eptrs: StdHashSet<_, StdRandomState> = StdHashSet::from_iter(eptrs.iter().copied());
            debug_assert!((&eptrs - &ptrs).is_empty());
        }

        self.state.metrics.remove.fetch_add(ptrs.len(), Ordering::Relaxed);

        // The handles in the indexer covers the handles in the eviction container.
        // So only the handles drained from the indexer need to be released.
        for ptr in ptrs {
            debug_assert!(!ptr.as_ref().base().is_in_indexer());
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
        last_reference_entries: &mut Vec<(Arc<K>, Arc<V>, <E::Handle as Handle>::Context, usize)>,
    ) {
        // TODO(MrCroxx): Use `let_chains` here after it is stable.
        while self.usage.load(Ordering::Relaxed) + weight > self.capacity {
            let evicted = match self.eviction.pop() {
                Some(evicted) => evicted,
                None => break,
            };
            self.state.metrics.evict.fetch_add(1, Ordering::Relaxed);
            let base = evicted.as_ref().base();
            debug_assert!(base.is_in_indexer());
            debug_assert!(!base.is_in_eviction());
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
    ) -> Option<(Arc<K>, Arc<V>, <E::Handle as Handle>::Context, usize)> {
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
    ) -> Option<(Arc<K>, Arc<V>, <E::Handle as Handle>::Context, usize)> {
        let handle = ptr.as_mut();

        if handle.base().has_refs() {
            return None;
        }

        debug_assert!(handle.base().is_inited());
        debug_assert!(!handle.base().has_refs());

        if handle.base().is_deposit() {
            self.indexer.remove(handle.base().hash(), handle.key());
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
                        self.state.metrics.reinsert.fetch_add(1, Ordering::Relaxed);
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
        debug_assert!(!handle.base().is_in_indexer());
        debug_assert!(!handle.base().is_in_eviction());
        debug_assert!(!handle.base().has_refs());

        self.state.metrics.release.fetch_add(1, Ordering::Relaxed);

        self.usage.fetch_sub(handle.base().weight(), Ordering::Relaxed);
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
    E::Handle: KeyedHandle<Key = Arc<K>, Data = (Arc<K>, Arc<V>)>,
    I: Indexer<Key = K, Handle = E::Handle>,
    S: BuildHasher + Send + Sync + 'static,
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
    E::Handle: KeyedHandle<Key = Arc<K>, Data = (Arc<K>, Arc<V>)>,
    S: BuildHasher + Send + Sync + 'static,
{
    pub capacity: usize,
    pub shards: usize,
    pub eviction_config: E::Config,
    pub object_pool_capacity: usize,
    pub hash_builder: S,
    pub weighter: Arc<dyn Weighter<K, V>>,
}

// TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
#[allow(clippy::type_complexity)]
pub enum GenericEntry<K, V, E, I, S, ER>
where
    K: Key,
    V: Value,
    E: Eviction,
    E::Handle: KeyedHandle<Key = Arc<K>, Data = (Arc<K>, Arc<V>)>,
    I: Indexer<Key = K, Handle = E::Handle>,
    S: BuildHasher + Send + Sync + 'static,
{
    Invalid,
    Hit(GenericCacheEntry<K, V, E, I, S>),
    Wait(oneshot::Receiver<Option<GenericCacheEntry<K, V, E, I, S>>>),
    Miss(JoinHandle<std::result::Result<Option<GenericCacheEntry<K, V, E, I, S>>, ER>>),
}

impl<K, V, E, I, S, ER> Default for GenericEntry<K, V, E, I, S, ER>
where
    K: Key,
    V: Value,
    E: Eviction,
    E::Handle: KeyedHandle<Key = Arc<K>, Data = (Arc<K>, Arc<V>)>,
    I: Indexer<Key = K, Handle = E::Handle>,
    S: BuildHasher + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::Invalid
    }
}

impl<K, V, E, I, S, ER> Future for GenericEntry<K, V, E, I, S, ER>
where
    K: Key,
    V: Value,
    E: Eviction,
    E::Handle: KeyedHandle<Key = Arc<K>, Data = (Arc<K>, Arc<V>)>,
    I: Indexer<Key = K, Handle = E::Handle>,
    S: BuildHasher + Send + Sync + 'static,
    ER: From<oneshot::error::RecvError>,
{
    type Output = std::result::Result<Option<GenericCacheEntry<K, V, E, I, S>>, ER>;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        match &mut *self {
            Self::Invalid => unreachable!(),
            Self::Hit(_) => std::task::Poll::Ready(Ok(match std::mem::take(&mut *self) {
                GenericEntry::Hit(entry) => Some(entry),
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
    E::Handle: KeyedHandle<Key = Arc<K>, Data = (Arc<K>, Arc<V>)>,
    I: Indexer<Key = K, Handle = E::Handle>,
    S: BuildHasher + Send + Sync + 'static,
{
    shards: Vec<Mutex<CacheShard<K, V, E, I, S>>>,

    capacity: usize,
    usages: Vec<Arc<AtomicUsize>>,

    context: Arc<CacheSharedState<E::Handle>>,

    hash_builder: S,
    weighter: Arc<dyn Weighter<K, V>>,
}

impl<K, V, E, I, S> GenericCache<K, V, E, I, S>
where
    K: Key,
    V: Value,
    E: Eviction,
    E::Handle: KeyedHandle<Key = Arc<K>, Data = (Arc<K>, Arc<V>)>,
    I: Indexer<Key = K, Handle = E::Handle>,
    S: BuildHasher + Send + Sync + 'static,
{
    pub fn new(config: GenericCacheConfig<K, V, E, S>) -> Self {
        let usages = (0..config.shards).map(|_| Arc::new(AtomicUsize::new(0))).collect_vec();
        let context = Arc::new(CacheSharedState {
            metrics: Metrics::default(),
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
        }
    }

    pub fn insert<AK, AV>(self: &Arc<Self>, key: AK, value: AV) -> GenericCacheEntry<K, V, E, I, S>
    where
        AK: Into<Arc<K>> + Send + 'static,
        AV: Into<Arc<V>> + Send + 'static,
    {
        self.insert_with_context(key, value, CacheContext::default())
    }

    pub fn insert_with_context<AK, AV>(
        self: &Arc<Self>,
        key: AK,
        value: AV,
        context: CacheContext,
    ) -> GenericCacheEntry<K, V, E, I, S>
    where
        AK: Into<Arc<K>> + Send + 'static,
        AV: Into<Arc<V>> + Send + 'static,
    {
        self.emplace(key, value, context, false)
    }

    pub fn deposit<AK, AV>(self: &Arc<Self>, key: AK, value: AV) -> GenericCacheEntry<K, V, E, I, S>
    where
        AK: Into<Arc<K>> + Send + 'static,
        AV: Into<Arc<V>> + Send + 'static,
    {
        self.deposit_with_context(key, value, CacheContext::default())
    }

    pub fn deposit_with_context<AK, AV>(
        self: &Arc<Self>,
        key: AK,
        value: AV,
        context: CacheContext,
    ) -> GenericCacheEntry<K, V, E, I, S>
    where
        AK: Into<Arc<K>> + Send + 'static,
        AV: Into<Arc<V>> + Send + 'static,
    {
        self.emplace(key, value, context, true)
    }

    fn emplace<AK, AV>(
        self: &Arc<Self>,
        key: AK,
        value: AV,
        context: CacheContext,
        deposit: bool,
    ) -> GenericCacheEntry<K, V, E, I, S>
    where
        AK: Into<Arc<K>> + Send + 'static,
        AV: Into<Arc<V>> + Send + 'static,
    {
        let key = key.into();
        let value = value.into();
        let hash = self.hash_builder.hash_one(&key);
        let weight = (self.weighter)(&key, &value);

        let mut to_deallocate = vec![];

        let (entry, waiters) = unsafe {
            let mut shard = self.shards[hash as usize % self.shards.len()].lock();
            let waiters = shard.waiters.remove(&key);
            let mut ptr = shard.emplace(hash, key, value, weight, context.into(), deposit, &mut to_deallocate);
            if let Some(waiters) = waiters.as_ref() {
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
                let _ = waiter.send(Some(GenericCacheEntry {
                    cache: self.clone(),
                    ptr: entry.ptr,
                }));
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
    K: Key,
    V: Value,
    E: Eviction,
    E::Handle: KeyedHandle<Key = Arc<K>, Data = (Arc<K>, Arc<V>)>,
    I: Indexer<Key = K, Handle = E::Handle>,
    S: BuildHasher + Send + Sync + 'static,
{
    pub fn entry<AK, AV, F, FU, ER>(self: &Arc<Self>, key: AK, f: F) -> GenericEntry<K, V, E, I, S, ER>
    where
        AK: Into<Arc<K>> + Send + 'static,
        AV: Into<Arc<V>> + Send + 'static,
        F: FnOnce() -> FU,
        FU: Future<Output = std::result::Result<Option<(AV, CacheContext)>, ER>> + Send + 'static,
        ER: Send + 'static,
    {
        let key = key.into();
        let hash = self.hash_builder.hash_one(&key);

        unsafe {
            let mut shard = self.shards[hash as usize % self.shards.len()].lock();
            if let Some(ptr) = shard.get(hash, &key) {
                return GenericEntry::Hit(GenericCacheEntry {
                    cache: self.clone(),
                    ptr,
                });
            }
            let entry = match shard.waiters.entry(key.clone()) {
                HashMapEntry::Occupied(mut o) => {
                    let (tx, rx) = oneshot::channel();
                    o.get_mut().push(tx);
                    GenericEntry::Wait(rx)
                }
                HashMapEntry::Vacant(v) => {
                    v.insert(vec![]);
                    let cache = self.clone();
                    let future = f();
                    let join = tokio::spawn(async move {
                        let res = match future.await {
                            Ok(res) => res,
                            Err(e) => {
                                let mut shard = cache.shards[hash as usize % cache.shards.len()].lock();
                                shard.waiters.remove(&key);
                                return Err(e);
                            }
                        };
                        match res {
                            Some((value, context)) => Ok(Some(cache.insert_with_context(key, value, context))),
                            None => {
                                if let Some(waiters) = cache.shards[hash as usize % cache.shards.len()]
                                    .lock()
                                    .waiters
                                    .remove(&key)
                                {
                                    for waiter in waiters {
                                        let _ = waiter.send(None);
                                    }
                                }
                                Ok(None)
                            }
                        }
                    });
                    GenericEntry::Miss(join)
                }
            };
            match entry {
                GenericEntry::Wait(_) => shard.state.metrics.queue.fetch_add(1, Ordering::Relaxed),
                GenericEntry::Miss(_) => shard.state.metrics.fetch.fetch_add(1, Ordering::Relaxed),
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
    E::Handle: KeyedHandle<Key = Arc<K>, Data = (Arc<K>, Arc<V>)>,
    I: Indexer<Key = K, Handle = E::Handle>,
    S: BuildHasher + Send + Sync + 'static,
{
    cache: Arc<GenericCache<K, V, E, I, S>>,
    ptr: NonNull<E::Handle>,
}

impl<K, V, E, I, S> Debug for GenericCacheEntry<K, V, E, I, S>
where
    K: Key,
    V: Value,
    E: Eviction,
    E::Handle: KeyedHandle<Key = Arc<K>, Data = (Arc<K>, Arc<V>)>,
    I: Indexer<Key = K, Handle = E::Handle>,
    S: BuildHasher + Send + Sync + 'static,
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
    E::Handle: KeyedHandle<Key = Arc<K>, Data = (Arc<K>, Arc<V>)>,
    I: Indexer<Key = K, Handle = E::Handle>,
    S: BuildHasher + Send + Sync + 'static,
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
    E::Handle: KeyedHandle<Key = Arc<K>, Data = (Arc<K>, Arc<V>)>,
    I: Indexer<Key = K, Handle = E::Handle>,
    S: BuildHasher + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        let mut ptr = self.ptr;

        unsafe {
            let base = ptr.as_mut().base_mut();
            debug_assert!(base.has_refs());
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
    E::Handle: KeyedHandle<Key = Arc<K>, Data = (Arc<K>, Arc<V>)>,
    I: Indexer<Key = K, Handle = E::Handle>,
    S: BuildHasher + Send + Sync + 'static,
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
    E::Handle: KeyedHandle<Key = Arc<K>, Data = (Arc<K>, Arc<V>)>,
    I: Indexer<Key = K, Handle = E::Handle>,
    S: BuildHasher + Send + Sync + 'static,
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
    E::Handle: KeyedHandle<Key = Arc<K>, Data = (Arc<K>, Arc<V>)>,
    I: Indexer<Key = K, Handle = E::Handle>,
    S: BuildHasher + Send + Sync + 'static,
{
}
unsafe impl<K, V, E, I, S> Sync for GenericCacheEntry<K, V, E, I, S>
where
    K: Key,
    V: Value,
    E: Eviction,
    E::Handle: KeyedHandle<Key = Arc<K>, Data = (Arc<K>, Arc<V>)>,
    I: Indexer<Key = K, Handle = E::Handle>,
    S: BuildHasher + Send + Sync + 'static,
{
}

#[cfg(test)]
mod tests {
    use rand::{rngs::SmallRng, RngCore, SeedableRng};

    use super::*;
    use crate::{
        cache::{FifoCache, FifoCacheEntry, LruCache, LruCacheEntry},
        eviction::{
            fifo::{FifoConfig, FifoHandle},
            lru::LruConfig,
            test_utils::TestEviction,
        },
    };

    fn is_send_sync_static<T: Send + Sync + 'static>() {}

    #[test]
    fn test_send_sync_static() {
        is_send_sync_static::<FifoCache<(), ()>>();
        is_send_sync_static::<LruCache<(), ()>>();
    }

    #[test]
    fn test_cache_fuzzy() {
        const CAPACITY: usize = 256;

        let config = GenericCacheConfig {
            capacity: CAPACITY,
            shards: 4,
            eviction_config: FifoConfig {},
            object_pool_capacity: 16,
            hash_builder: RandomState::default(),
            weighter: Arc::new(|_, _| 1),
        };
        let cache = Arc::new(FifoCache::<u64, u64>::new(config));

        let mut rng = SmallRng::seed_from_u64(114514);
        for _ in 0..100000 {
            let key = rng.next_u64();
            if let Some(entry) = cache.get(&key) {
                assert_eq!(key, *entry);
                drop(entry);
                continue;
            }
            cache.insert(key, key);
        }
        assert_eq!(cache.usage(), CAPACITY);
    }

    fn fifo(capacity: usize) -> Arc<FifoCache<u64, String>> {
        let config = GenericCacheConfig {
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

        let refs = |ptr: NonNull<FifoHandle<(Arc<u64>, Arc<String>)>>| unsafe { ptr.as_ref().base().refs() };

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
            vec![
                (514.into(), "QwQ".to_string().into()),
                (114.into(), "(0.0)".to_string().into())
            ],
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
        assert_eq!(
            cache.shards[0].lock().eviction.dump(),
            vec![(4.into(), "444".to_string().into()),]
        );

        // `e1` cannot be reinserted for the usage has already exceeds the capacity.
        drop(e1);
        assert_eq!(cache.usage(), 9);

        // `222` and `333` will be reinserted
        drop(e2);
        drop(e3);
        assert_eq!(
            cache.shards[0].lock().eviction.dump(),
            vec![
                (4.into(), "444".to_string().into()),
                (2.into(), "222".to_string().into()),
                (3.into(), "333".to_string().into()),
            ]
        );
        assert_eq!(cache.usage(), 9);

        // `444` will be reinserted
        drop(e4);
        assert_eq!(
            cache.shards[0].lock().eviction.dump(),
            vec![
                (2.into(), "222".to_string().into()),
                (3.into(), "333".to_string().into()),
                (4.into(), "444".to_string().into()),
            ]
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
        assert_eq!(
            cache.shards[0].lock().eviction.dump(),
            vec![(4.into(), "444".to_string().into()),]
        );

        // `e1` cannot be reinserted for the usage has already exceeds the capacity.
        drop(e1);
        assert_eq!(cache.usage(), 9);

        // `222` and `333` will be not reinserted because fifo will ignore reinsert operations.
        drop([e2, e3, e4]);
        assert_eq!(
            cache.shards[0].lock().eviction.dump(),
            vec![(4.into(), "444".to_string().into()),]
        );
        assert_eq!(cache.usage(), 3);

        // Note:
        //
        // For cache policy like FIFO, the entries will not be reinserted while all handles are referenced.
        // It's okay for this is not a common situation and is not supposed to happen in real workload.
    }
}
