//  Copyright 2023 MrCroxx
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

use std::{collections::BTreeMap, hash::Hasher};

use std::{ptr::NonNull, sync::Arc};

use futures::future::try_join_all;
use itertools::Itertools;
use tokio::sync::{Mutex, MutexGuard};
use twox_hash::XxHash64;

use crate::{store::Store, Data, Index, Metrics, WrappedNonNull};
use foyer_policy::eviction::{Handle, Policy};

// TODO(MrCroxx): wrap own result type
use crate::store::error::Result;

pub struct Config<I, P, H, S>
where
    I: Index,
    P: Policy<T = I, H = H>,
    H: Handle<T = I>,
    S: Store<I = I>,
{
    pub capacity: usize,

    pub pool_count_bits: usize,

    pub policy_config: P::C,

    pub store_config: S::C,
}

impl<I, P, H, S> std::fmt::Debug for Config<I, P, H, S>
where
    I: Index,
    P: Policy<T = I, H = H>,
    H: Handle<T = I>,
    S: Store<I = I>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("capacity", &self.capacity)
            .field("pool_count_bits", &self.pool_count_bits)
            .field("policy_config", &self.policy_config)
            .field("store_config", &self.store_config)
            .finish()
    }
}

#[allow(clippy::type_complexity)]
pub struct Container<I, P, H, D, S>
where
    I: Index,
    P: Policy<T = I, H = H>,
    H: Handle<T = I>,
    D: Data,
    S: Store<I = I, D = D>,
{
    pool_count_bits: usize,
    pools: Vec<Mutex<Pool<I, P, H, D, S>>>,

    metrics: Arc<Metrics>,
}

impl<I, P, H, D, S> Container<I, P, H, D, S>
where
    I: Index,
    P: Policy<T = I, H = H>,
    H: Handle<T = I>,
    D: Data,

    S: Store<I = I, D = D>,
{
    pub async fn open(config: Config<I, P, H, S>) -> Result<Self> {
        Self::open_with_registry(config, prometheus::Registry::new()).await
    }

    pub async fn open_with_registry(
        config: Config<I, P, H, S>,
        registry: prometheus::Registry,
    ) -> Result<Self> {
        tracing::info!("open foyer with config: \n{:#?}", config);

        let pool_count = 1 << config.pool_count_bits;
        let capacity = config.capacity >> config.pool_count_bits;

        let metrics = Arc::new(Metrics::new(registry));

        let stores = (0..pool_count)
            .map(|pool| S::open(pool, config.store_config.clone(), metrics.clone()))
            .collect_vec();
        let stores = try_join_all(stores).await?;

        let pools = stores
            .into_iter()
            .enumerate()
            .map(|(id, store)| Pool {
                _id: id,
                policy: P::new(config.policy_config.clone()),
                capacity,
                size: 0,
                handles: BTreeMap::new(),
                store,
                _metrics: metrics.clone(),
            })
            .map(Mutex::new)
            .collect_vec();

        Ok(Self {
            pool_count_bits: config.pool_count_bits,
            pools,
            metrics,
        })
    }

    pub async fn insert(&self, index: I, data: D) -> Result<bool> {
        let _timer = self.metrics.latency_insert.start_timer();

        let mut pool = self.pool(&index).await;

        if pool.handles.get(&index).is_some() {
            // already in cache
            return Ok(false);
        }

        let weight = data.weight();

        pool.make_room(weight).await?;

        pool.insert(index, weight, data).await?;

        Ok(true)
    }

    pub async fn remove(&self, index: &I) -> Result<bool> {
        let _timer = self.metrics.latency_remove.start_timer();

        let mut pool = self.pool(index).await;

        if pool.handles.get(index).is_none() {
            // not in cache
            return Ok(false);
        }

        pool.remove(index).await?;

        Ok(true)
    }

    pub async fn get(&self, index: &I) -> Result<Option<D>> {
        let _timer = self.metrics.latency_get.start_timer();

        let mut pool = self.pool(index).await;

        let res = pool.get(index).await?;

        if res.is_none() {
            self.metrics.miss.inc();
        }

        Ok(res)
    }

    // TODO(MrCroxx): optimize this
    pub async fn size(&self) -> usize {
        let mut size = 0;
        for pool in &self.pools {
            size += pool.lock().await.size
        }
        size
    }

    async fn pool(&self, index: &I) -> MutexGuard<'_, Pool<I, P, H, D, S>> {
        let mut hasher = XxHash64::default();
        index.hash(&mut hasher);
        let id = hasher.finish() as usize & ((1 << self.pool_count_bits) - 1);

        self.pools[id].lock().await
    }
}

struct Pool<I, P, H, D, S>
where
    I: Index,
    P: Policy<T = I, H = H>,
    H: Handle<T = I>,
    D: Data,
    S: Store<I = I, D = D>,
{
    _id: usize,

    policy: P,

    capacity: usize,

    size: usize,

    handles: BTreeMap<I, PoolHandle<I, H>>,

    store: S,

    _metrics: Arc<Metrics>,
}

impl<I, P, H, D, S> Pool<I, P, H, D, S>
where
    I: Index,
    P: Policy<T = I, H = H>,
    H: Handle<T = I>,
    D: Data,
    S: Store<I = I, D = D>,
{
    async fn make_room(&mut self, weight: usize) -> Result<()> {
        let mut handles = vec![];
        for index in self.policy.eviction_iter() {
            if self.size + weight <= self.capacity || self.size == 0 {
                break;
            }
            let pool_handle = self.handles.remove(index).unwrap();
            self.size -= pool_handle.weight;
            handles.push(pool_handle.handle);
            self.store.delete(index).await?;
        }
        for handle in handles {
            assert!(self.policy.remove(handle.0));
            unsafe { drop(Box::from_raw(handle.0.as_ptr())) };
        }
        Ok(())
    }

    async fn insert(&mut self, index: I, weight: usize, data: D) -> Result<()> {
        let handle = Box::new(H::new(index.clone()));
        let handle = unsafe { WrappedNonNull(NonNull::new_unchecked(Box::leak(handle))) };

        assert!(self.policy.insert(handle.0));
        self.handles
            .insert(index.clone(), PoolHandle { weight, handle });
        self.size += weight;

        self.store.store(index, data).await?;

        Ok(())
    }

    async fn remove(&mut self, index: &I) -> Result<()> {
        let PoolHandle { weight, handle } = self.handles.remove(index).unwrap();
        assert!(self.policy.remove(handle.0));
        self.size -= weight;

        self.store.delete(index).await?;

        Ok(())
    }

    async fn get(&mut self, index: &I) -> Result<Option<D>> {
        match self.handles.get(index) {
            Some(pool_handle) => {
                self.policy.access(pool_handle.handle.0);
                self.store.load(index).await
            }
            None => Ok(None),
        }
    }
}

impl<I, P, H, D, S> Drop for Pool<I, P, H, D, S>
where
    I: Index,
    P: Policy<T = I, H = H>,
    H: Handle<T = I>,
    D: Data,
    S: Store<I = I, D = D>,
{
    fn drop(&mut self) {
        let mut handles = BTreeMap::new();
        std::mem::swap(&mut handles, &mut self.handles);
        for PoolHandle { weight: _, handle } in handles.into_values() {
            unsafe { drop(Box::from_raw(handle.0.as_ptr())) }
        }
    }
}

struct PoolHandle<I, H>
where
    I: Index,
    H: Handle<T = I>,
{
    weight: usize,
    // Use `WrappedNonNull` for ptrs that needs to cross .await points.
    handle: WrappedNonNull<H>,
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{store::tests::MemoryStore, tests::is_send_sync_static};
    use foyer_policy::eviction::{
        lru::{Config as LruConfig, Handle as LruHandle, Lru},
        tinylfu::Handle as TinyLfuHandle,
    };

    #[tokio::test]
    async fn test_container_simple() {
        is_send_sync_static::<PoolHandle<u64, LruHandle<u64>>>();
        is_send_sync_static::<PoolHandle<u64, TinyLfuHandle<u64>>>();

        let policy_config = LruConfig {
            lru_insertion_point_fraction: 0.0,
        };

        let config = Config {
            capacity: 100,
            pool_count_bits: 0,
            policy_config,

            store_config: (),
        };

        let container: Container<u64, Lru<_>, LruHandle<_>, Vec<u8>, MemoryStore<_, _>> =
            Container::open(config).await.unwrap();

        assert!(container.insert(1, vec![b'x'; 40]).await.unwrap());
        assert!(!container.insert(1, vec![b'x'; 40]).await.unwrap());
        assert_eq!(container.get(&1).await.unwrap(), Some(vec![b'x'; 40]));

        assert!(container.insert(2, vec![b'x'; 60]).await.unwrap());
        assert!(!container.insert(2, vec![b'x'; 60]).await.unwrap());
        assert_eq!(container.get(&2).await.unwrap(), Some(vec![b'x'; 60]));

        // After insert 3, {1, 2} will be evicted.
        assert!(container.insert(3, vec![b'x'; 50]).await.unwrap());
        assert_eq!(container.size().await, 50);
        assert_eq!(container.get(&3).await.unwrap(), Some(vec![b'x'; 50]));
        assert_eq!(container.get(&1).await.unwrap(), None);
        assert_eq!(container.get(&2).await.unwrap(), None);

        assert!(container.remove(&3).await.unwrap());

        assert_eq!(container.size().await, 0);
    }
}
