//  Copyright 2024 MrCroxx
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
    future::Future,
    hash::BuildHasher,
    ops::Deref,
    ptr::NonNull,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use ahash::RandomState;
use crossbeam::queue::ArrayQueue;
use hashbrown::{
    hash_map::{Entry as HashMapEntry, HashMap},
    hash_table::{Entry as HashTableEntry, HashTable},
};
use itertools::Itertools;
use parking_lot::Mutex;
use tokio::{sync::oneshot, task::JoinHandle};

use crate::{eviction::Eviction, handle::Handle, Key, Value};

#[expect(clippy::missing_safety_doc)]
pub trait Indexer: Send + Sync + 'static {
    type Key: Key;
    type Handle: Handle<Key = Self::Key>;

    fn new() -> Self;
    unsafe fn insert(&mut self, handle: NonNull<Self::Handle>) -> Option<NonNull<Self::Handle>>;
    unsafe fn get(&self, hash: u64, key: &Self::Key) -> Option<NonNull<Self::Handle>>;
    unsafe fn remove(&mut self, hash: u64, key: &Self::Key) -> Option<NonNull<Self::Handle>>;
}

struct HashTableIndexer<K, H>
where
    K: Key,
    H: Handle<Key = K>,
{
    table: HashTable<NonNull<H>>,
}

unsafe impl<K, H> Send for HashTableIndexer<K, H>
where
    K: Key,
    H: Handle<Key = K>,
{
}

unsafe impl<K, H> Sync for HashTableIndexer<K, H>
where
    K: Key,
    H: Handle<Key = K>,
{
}

impl<K, H> Indexer for HashTableIndexer<K, H>
where
    K: Key,
    H: Handle<Key = K>,
{
    type Key = K;
    type Handle = H;

    fn new() -> Self {
        Self {
            table: HashTable::new(),
        }
    }

    unsafe fn insert(&mut self, mut ptr: NonNull<Self::Handle>) -> Option<NonNull<Self::Handle>> {
        let base = ptr.as_mut().base_mut();

        debug_assert!(!base.is_in_cache());
        base.set_in_cache(true);

        match self.table.entry(
            base.hash(),
            |p| p.as_ref().base().key() == base.key(),
            |p| p.as_ref().base().hash(),
        ) {
            HashTableEntry::Occupied(mut o) => {
                std::mem::swap(o.get_mut(), &mut ptr);
                let b = ptr.as_mut().base_mut();
                debug_assert!(b.is_in_cache());
                b.set_in_cache(false);
                Some(ptr)
            }
            HashTableEntry::Vacant(v) => {
                v.insert(ptr);
                None
            }
        }
    }

    unsafe fn get(&self, hash: u64, key: &Self::Key) -> Option<NonNull<Self::Handle>> {
        self.table
            .find(hash, |p| p.as_ref().base().key() == key)
            .copied()
    }

    unsafe fn remove(&mut self, hash: u64, key: &Self::Key) -> Option<NonNull<Self::Handle>> {
        match self.table.entry(
            hash,
            |p| p.as_ref().base().key() == key,
            |p| p.as_ref().base().hash(),
        ) {
            HashTableEntry::Occupied(o) => {
                let (mut p, _) = o.remove();
                let b = p.as_mut().base_mut();
                debug_assert!(b.is_in_cache());
                b.set_in_cache(false);
                Some(p)
            }
            HashTableEntry::Vacant(_) => None,
        }
    }
}

#[expect(clippy::type_complexity)]
struct CacheShard<K, V, H, E, I, S>
where
    K: Key,
    V: Value,
    H: Handle<Key = K, Value = V>,
    E: Eviction<Handle = H>,
    I: Indexer<Key = K, Handle = H>,
    S: BuildHasher + Send + Sync + 'static,
{
    indexer: I,
    eviction: E,

    capacity: usize,
    usage: Arc<AtomicUsize>,

    waiters: HashMap<K, Vec<oneshot::Sender<CacheEntry<K, V, H, E, I, S>>>>,

    /// The object pool to avoid frequent handle allocating, shared by all shards.
    object_pool: Arc<ArrayQueue<Box<H>>>,
}

impl<K, V, H, E, I, S> CacheShard<K, V, H, E, I, S>
where
    K: Key,
    V: Value,
    H: Handle<Key = K, Value = V>,
    E: Eviction<Handle = H>,
    I: Indexer<Key = K, Handle = H>,
    S: BuildHasher + Send + Sync + 'static,
{
    fn new(
        config: &CacheConfig<E, S>,
        usage: Arc<AtomicUsize>,
        object_pool: Arc<ArrayQueue<Box<H>>>,
    ) -> Self {
        let indexer = I::new();
        let eviction = unsafe { E::new(config) };
        let capacity = config.capacity / config.shards;
        let waiters = HashMap::default();
        Self {
            indexer,
            eviction,
            capacity,
            usage,
            waiters,
            object_pool,
        }
    }

    /// Insert a new entry into the cache. The handle for the new entry is returned.
    unsafe fn insert(
        &mut self,
        hash: u64,
        key: K,
        value: V,
        charge: usize,
        context: H::Context,
        last_reference_entries: &mut Vec<(K, V)>,
    ) -> NonNull<H> {
        let mut handle = self.object_pool.pop().unwrap_or_else(|| Box::new(H::new()));
        handle.init(hash, key, value, charge, context);
        let mut ptr = unsafe { NonNull::new_unchecked(Box::into_raw(handle)) };

        self.evict(charge, last_reference_entries);

        if let Some(old) = self.indexer.insert(ptr) {
            // There is no external refs of this handle, it MUST be in the eviction collection.
            if old.as_ref().base().refs() == 0 {
                self.eviction.remove(old);
                let (key, value) = self.clear_handle(old);
                last_reference_entries.push((key, value));
            }
        }

        self.usage.fetch_add(charge, Ordering::Relaxed);
        ptr.as_mut().base_mut().inc_refs();

        ptr
    }

    /// Release the usage of a handle.
    ///
    /// Return `Some(..)` if the handle is released, or `None` if the handle is still in use.
    unsafe fn release(&mut self, mut ptr: NonNull<H>) -> Option<(K, V)> {
        let base = ptr.as_mut().base_mut();

        debug_assert!(!base.is_in_eviction());

        if base.dec_refs() > 0 {
            // Do nothing if the handle is still referenced externally.
            return None;
        }

        // Keep the handle in eviction if it is still in the cache and the cache is not over-sized.
        if base.is_in_cache() {
            if self.usage.load(Ordering::Relaxed) <= self.capacity {
                self.eviction.push(ptr);
                return None;
            }
            // Emergency remove the handle if there is no space in cache.
            self.indexer.remove(base.hash(), base.key());
        }

        debug_assert!(!base.is_in_eviction());

        let (key, value) = self.clear_handle(ptr);
        Some((key, value))
    }

    unsafe fn get(&mut self, hash: u64, key: &K) -> Option<NonNull<H>> {
        let mut ptr = self.indexer.get(hash, key)?;
        let base = ptr.as_mut().base_mut();

        // If the handle previously has no reference, it must exist in eviction, remove it.
        if base.refs() == 0 {
            self.eviction.remove(ptr);
        }
        base.inc_refs();

        Some(ptr)
    }

    /// Remove a key from the cache.
    ///
    /// Return `Some(..)` if the handle is released, or `None` if the handle is still in use.
    unsafe fn remove(&mut self, hash: u64, key: &K) -> Option<(K, V)> {
        let mut ptr = self.indexer.remove(hash, key)?;
        let base = ptr.as_mut().base_mut();

        if base.refs() == 0 {
            self.eviction.remove(ptr);
            let (key, value) = self.clear_handle(ptr);
            return Some((key, value));
        }

        None
    }

    /// Clear all cache entries.
    ///
    /// # Safety
    ///
    /// This method is safe only if there is no entry referenced externally.
    ///
    /// # Panics
    ///
    /// Panics if there is any entry referenced externally.
    unsafe fn clear(&mut self) {
        let ptrs = self.eviction.clear();
        for mut ptr in ptrs {
            let base = ptr.as_mut().base_mut();
            let p = self.indexer.remove(base.hash(), base.key()).unwrap();
            debug_assert_eq!(ptr, p);
            self.clear_handle(ptr);
        }
    }

    unsafe fn evict(&mut self, charge: usize, last_reference_entries: &mut Vec<(K, V)>) {
        while self.usage.load(Ordering::Relaxed) + charge > self.capacity
            && let Some(evicted) = self.eviction.pop()
        {
            let base = evicted.as_ref().base();
            self.indexer.remove(base.hash(), base.key());
            let (key, value) = self.clear_handle(evicted);
            last_reference_entries.push((key, value));
        }
    }

    /// Clear a currently used handle and recycle it if possible.
    unsafe fn clear_handle(&self, mut ptr: NonNull<H>) -> (K, V) {
        let base = ptr.as_mut().base_mut();

        debug_assert!(base.is_inited());
        debug_assert!(!base.is_in_cache());
        debug_assert!(!base.is_in_eviction());
        debug_assert_eq!(base.refs(), 0);

        self.usage.fetch_sub(base.charge(), Ordering::Relaxed);
        let (key, value) = base.take();

        let handle = Box::from_raw(ptr.as_ptr());
        let _ = self.object_pool.push(handle);

        (key, value)
    }
}

impl<K, V, H, E, I, S> Drop for CacheShard<K, V, H, E, I, S>
where
    K: Key,
    V: Value,
    H: Handle<Key = K, Value = V>,
    E: Eviction<Handle = H>,
    I: Indexer<Key = K, Handle = H>,
    S: BuildHasher + Send + Sync + 'static,
{
    fn drop(&mut self) {
        // Since the shard is being drop, there must be no cache entries referenced outside. So we
        // are safe to call clear.
        unsafe { self.clear() }
    }
}

pub struct CacheConfig<E, S = RandomState>
where
    E: Eviction,
    S: BuildHasher + Send + Sync + 'static,
{
    pub capacity: usize,
    pub shards: usize,
    pub eviction_config: E::Config,
    pub object_pool_capacity: usize,
    pub hash_builder: S,
}

#[expect(clippy::type_complexity)]
pub enum Entry<K, V, H, E, I, S, ER>
where
    K: Key,
    V: Value,
    H: Handle<Key = K, Value = V>,
    E: Eviction<Handle = H>,
    I: Indexer<Key = K, Handle = H>,
    S: BuildHasher + Send + Sync + 'static,
    ER: std::error::Error,
{
    Hit(CacheEntry<K, V, H, E, I, S>),
    Wait(oneshot::Receiver<CacheEntry<K, V, H, E, I, S>>),
    Miss(JoinHandle<std::result::Result<CacheEntry<K, V, H, E, I, S>, ER>>),
}

#[expect(clippy::type_complexity)]
pub struct Cache<K, V, H, E, I, S = RandomState>
where
    K: Key,
    V: Value,
    H: Handle<Key = K, Value = V>,
    E: Eviction<Handle = H>,
    I: Indexer<Key = K, Handle = H>,
    S: BuildHasher + Send + Sync + 'static,
{
    shards: Vec<Mutex<CacheShard<K, V, H, E, I, S>>>,

    capacity: usize,
    usages: Vec<Arc<AtomicUsize>>,

    hash_builder: S,
}

impl<K, V, H, E, I, S> Cache<K, V, H, E, I, S>
where
    K: Key,
    V: Value,
    H: Handle<Key = K, Value = V>,
    E: Eviction<Handle = H>,
    I: Indexer<Key = K, Handle = H>,
    S: BuildHasher + Send + Sync + 'static,
{
    pub fn new(config: CacheConfig<E, S>) -> Self {
        let usages = (0..config.shards)
            .map(|_| Arc::new(AtomicUsize::new(0)))
            .collect_vec();
        let object_pool = Arc::new(ArrayQueue::new(config.object_pool_capacity));
        let shards = usages
            .iter()
            .map(|usage| CacheShard::new(&config, usage.clone(), object_pool.clone()))
            .map(Mutex::new)
            .collect_vec();

        Self {
            shards,
            capacity: config.capacity,
            usages,
            hash_builder: config.hash_builder,
        }
    }

    pub fn insert(
        self: &Arc<Self>,
        key: K,
        value: V,
        charge: usize,
    ) -> CacheEntry<K, V, H, E, I, S> {
        self.insert_with_context(key, value, charge, H::Context::default())
    }

    pub fn insert_with_context(
        self: &Arc<Self>,
        key: K,
        value: V,
        charge: usize,
        context: H::Context,
    ) -> CacheEntry<K, V, H, E, I, S> {
        let hash = self.hash_builder.hash_one(&key);

        let mut to_deallocate = vec![];

        let (entry, waiters) = unsafe {
            let mut shard = self.shards[hash as usize % self.shards.len()].lock();
            let waiters = shard.waiters.remove(&key);
            let mut ptr = shard.insert(hash, key, value, charge, context, &mut to_deallocate);
            if let Some(waiters) = waiters.as_ref() {
                ptr.as_mut().base_mut().inc_refs_by(waiters.len());
            }
            let entry = CacheEntry {
                cache: self.clone(),
                ptr,
            };
            (entry, waiters)
        };

        if let Some(waiters) = waiters {
            for waiter in waiters {
                let _ = waiter.send(CacheEntry {
                    cache: self.clone(),
                    ptr: entry.ptr,
                });
            }
        }

        // Do not deallocate data within the lock section.
        // TODO: call listener here.
        drop(to_deallocate);

        entry
    }

    pub fn remove(&self, key: &K) {
        let hash = self.hash_builder.hash_one(key);

        let kv = unsafe {
            let mut shard = self.shards[hash as usize % self.shards.len()].lock();
            shard.remove(hash, key)
        };

        // Do not deallocate data within the lock section.
        // TODO: call listener here.
        drop(kv);
    }

    pub fn get(self: &Arc<Self>, key: &K) -> Option<CacheEntry<K, V, H, E, I, S>> {
        let hash = self.hash_builder.hash_one(key);

        unsafe {
            let mut shard = self.shards[hash as usize % self.shards.len()].lock();
            shard.get(hash, key).map(|ptr| CacheEntry {
                cache: self.clone(),
                ptr,
            })
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn usage(&self) -> usize {
        self.usages
            .iter()
            .map(|usage| usage.load(Ordering::Relaxed))
            .sum()
    }

    unsafe fn release(&self, ptr: NonNull<H>) {
        let kv = {
            let base = ptr.as_ref().base();
            let mut shard = self.shards[base.hash() as usize % self.shards.len()].lock();
            shard.release(ptr)
        };

        // Do not deallocate data within the lock section.
        // TODO: call listener here.
        drop(kv);
    }
}

// TODO(MrCroxx): use `hashbrown::HashTable` with `Handle` may relax the `Clone` bound?
impl<K, V, H, E, I, S> Cache<K, V, H, E, I, S>
where
    K: Key + Clone,
    V: Value,
    H: Handle<Key = K, Value = V>,
    E: Eviction<Handle = H>,
    I: Indexer<Key = K, Handle = H>,
    S: BuildHasher + Send + Sync + 'static,
{
    pub fn entry<F, FU, ER>(self: &Arc<Self>, key: K, f: F) -> Entry<K, V, H, E, I, S, ER>
    where
        F: FnOnce() -> FU,
        FU: Future<Output = std::result::Result<(V, usize), ER>> + Send + 'static,
        ER: std::error::Error + Send + 'static,
    {
        let hash = self.hash_builder.hash_one(&key);

        unsafe {
            let mut shard = self.shards[hash as usize % self.shards.len()].lock();
            if let Some(ptr) = shard.get(hash, &key) {
                return Entry::Hit(CacheEntry {
                    cache: self.clone(),
                    ptr,
                });
            }
            match shard.waiters.entry(key.clone()) {
                HashMapEntry::Occupied(mut o) => {
                    let (tx, rx) = oneshot::channel();
                    o.get_mut().push(tx);
                    Entry::Wait(rx)
                }
                HashMapEntry::Vacant(v) => {
                    v.insert(vec![]);
                    let cache = self.clone();
                    let future = f();
                    let join = tokio::spawn(async move {
                        let (value, charge) = match future.await {
                            Ok((value, charge)) => (value, charge),
                            Err(e) => {
                                let mut shard =
                                    cache.shards[hash as usize % cache.shards.len()].lock();
                                shard.waiters.remove(&key);
                                return Err(e);
                            }
                        };
                        let entry = cache.insert(key, value, charge);
                        Ok(entry)
                    });
                    Entry::Miss(join)
                }
            }
        }
    }
}

pub struct CacheEntry<K, V, H, E, I, S = RandomState>
where
    K: Key,
    V: Value,
    H: Handle<Key = K, Value = V>,
    E: Eviction<Handle = H>,
    I: Indexer<Key = K, Handle = H>,
    S: BuildHasher + Send + Sync + 'static,
{
    cache: Arc<Cache<K, V, H, E, I, S>>,
    ptr: NonNull<H>,
}

impl<K, V, H, E, I, S> Drop for CacheEntry<K, V, H, E, I, S>
where
    K: Key,
    V: Value,
    H: Handle<Key = K, Value = V>,
    E: Eviction<Handle = H>,
    I: Indexer<Key = K, Handle = H>,
    S: BuildHasher + Send + Sync + 'static,
{
    fn drop(&mut self) {
        unsafe { self.cache.release(self.ptr) }
    }
}

impl<K, V, H, E, I, S> Deref for CacheEntry<K, V, H, E, I, S>
where
    K: Key,
    V: Value,
    H: Handle<Key = K, Value = V>,
    E: Eviction<Handle = H>,
    I: Indexer<Key = K, Handle = H>,
    S: BuildHasher + Send + Sync + 'static,
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        unsafe { self.ptr.as_ref().base().value() }
    }
}

unsafe impl<K, V, H, E, I, S> Send for CacheEntry<K, V, H, E, I, S>
where
    K: Key,
    V: Value,
    H: Handle<Key = K, Value = V>,
    E: Eviction<Handle = H>,
    I: Indexer<Key = K, Handle = H>,
    S: BuildHasher + Send + Sync + 'static,
{
}
unsafe impl<K, V, H, E, I, S> Sync for CacheEntry<K, V, H, E, I, S>
where
    K: Key,
    V: Value,
    H: Handle<Key = K, Value = V>,
    E: Eviction<Handle = H>,
    I: Indexer<Key = K, Handle = H>,
    S: BuildHasher + Send + Sync + 'static,
{
}

#[cfg(test)]
mod tests {
    use rand::{rngs::SmallRng, RngCore, SeedableRng};

    use super::*;
    use crate::eviction::fifo::{Fifo, FifoConfig, FifoHandle};

    type TestFifoCache = Cache<
        u64,
        u64,
        FifoHandle<u64, u64>,
        Fifo<u64, u64>,
        HashTableIndexer<u64, FifoHandle<u64, u64>>,
    >;
    type TestFifoCacheConfig = CacheConfig<Fifo<u64, u64>>;

    fn is_send_sync_static<T: Send + Sync + 'static>() {}

    #[test]
    fn test_send_sync_static() {
        is_send_sync_static::<TestFifoCache>();
        is_send_sync_static::<TestFifoCacheConfig>();
    }

    #[test]
    fn test_cache_fuzzy() {
        const CAPACITY: usize = 256;

        let config = TestFifoCacheConfig {
            capacity: CAPACITY,
            shards: 4,
            eviction_config: FifoConfig {
                default_removable_capacity: 16,
            },
            object_pool_capacity: 16,
            hash_builder: RandomState::default(),
        };
        let cache = Arc::new(TestFifoCache::new(config));

        let mut rng = SmallRng::seed_from_u64(114514);
        for _ in 0..100000 {
            let key = rng.next_u64();
            if let Some(entry) = cache.get(&key) {
                assert_eq!(key, *entry);
                drop(entry);
                continue;
            }
            cache.insert(key, key, 1);
        }
        assert_eq!(cache.usage(), CAPACITY);
    }
}
