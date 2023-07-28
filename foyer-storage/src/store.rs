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

use std::{
    fmt::Debug,
    marker::PhantomData,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::{Buf, BufMut};
use foyer_common::{bits, queue::AsyncQueue, rate::RateLimiter};
use foyer_intrusive::eviction::EvictionPolicy;
use futures::Future;
use itertools::Itertools;
use parking_lot::Mutex;
use tokio::{sync::broadcast, task::JoinHandle};
use twox_hash::XxHash64;

use crate::{
    admission::AdmissionPolicy,
    device::Device,
    error::{Error, Result},
    event::EventListener,
    flusher::Flusher,
    indices::{Index, Indices},
    judge::Judges,
    metrics::Metrics,
    reclaimer_v2::Reclaimer,
    region::{Region, RegionId},
    region_manager::{RegionEpItemAdapter, RegionManager},
    reinsertion::ReinsertionPolicy,
};
use foyer_common::code::{Key, Value};
use foyer_intrusive::core::adapter::Link;
use std::hash::Hasher;

const REGION_MAGIC: u64 = 0x19970327;

pub trait FetchValueFuture<V> = Future<Output = anyhow::Result<V>> + Send + 'static;

#[derive(Debug, Default)]
pub struct PrometheusConfig {
    pub registry: Option<prometheus::Registry>,
    pub namespace: Option<String>,
}

pub struct StoreConfig<K, V, D, EP>
where
    K: Key,
    V: Value,
    D: Device,
    EP: EvictionPolicy,
{
    pub eviction_config: EP::Config,
    pub device_config: D::Config,
    pub admissions: Vec<Arc<dyn AdmissionPolicy<Key = K, Value = V>>>,
    pub reinsertions: Vec<Arc<dyn ReinsertionPolicy<Key = K, Value = V>>>,
    pub buffer_pool_size: usize,
    pub flushers: usize,
    pub flush_rate_limit: usize,
    pub reclaimers: usize,
    pub reclaim_rate_limit: usize,
    pub recover_concurrency: usize,
    pub event_listeners: Vec<Arc<dyn EventListener<K = K, V = V>>>,
    pub prometheus_config: PrometheusConfig,
    pub clean_region_threshold: usize,
}

impl<K, V, D, EP> Debug for StoreConfig<K, V, D, EP>
where
    K: Key,
    V: Value,
    D: Device,
    EP: EvictionPolicy,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreConfig")
            .field("eviction_config", &self.eviction_config)
            .field("device_config", &self.device_config)
            .field("admissions", &self.admissions)
            .field("reinsertions", &self.reinsertions)
            .field("buffer_pool_size", &self.buffer_pool_size)
            .field("flushers", &self.flushers)
            .field("reclaimers", &self.reclaimers)
            .finish()
    }
}

#[derive(Debug)]
pub struct Store<K, V, D, EP, EL>
where
    K: Key,
    V: Value,
    D: Device,
    EP: EvictionPolicy<Adapter = RegionEpItemAdapter<EL>>,
    EL: Link,
{
    indices: Arc<Indices<K>>,

    region_manager: Arc<RegionManager<D, EP, EL>>,

    device: D,

    admissions: Vec<Arc<dyn AdmissionPolicy<Key = K, Value = V>>>,
    reinsertions: Vec<Arc<dyn ReinsertionPolicy<Key = K, Value = V>>>,

    event_listeners: Vec<Arc<dyn EventListener<K = K, V = V>>>,

    handles: Mutex<Vec<JoinHandle<()>>>,

    stop_tx: broadcast::Sender<()>,

    metrics: Arc<Metrics>,

    _marker: PhantomData<V>,
}

impl<K, V, D, EP, EL> Store<K, V, D, EP, EL>
where
    K: Key,
    V: Value,
    D: Device,
    EP: EvictionPolicy<Adapter = RegionEpItemAdapter<EL>>,
    EL: Link,
{
    pub async fn open(config: StoreConfig<K, V, D, EP>) -> Result<Arc<Self>> {
        tracing::info!("open store with config:\n{:#?}", config);

        let device = D::open(config.device_config).await?;

        let buffers = Arc::new(AsyncQueue::new());
        for _ in 0..(config.buffer_pool_size / device.region_size()) {
            let len = device.region_size();
            let buffer = device.io_buffer(len, len);
            buffers.release(buffer);
        }

        let clean_regions = Arc::new(AsyncQueue::new());

        let region_manager = Arc::new(RegionManager::new(
            device.regions(),
            config.eviction_config,
            buffers.clone(),
            clean_regions.clone(),
            device.clone(),
        ));

        let indices = Arc::new(Indices::new(device.regions()));

        let (stop_tx, _stop_rx) = broadcast::channel(config.flushers + config.reclaimers + 1);
        let flusher_stop_rxs = (0..config.flushers)
            .map(|_| stop_tx.subscribe())
            .collect_vec();
        let reclaimer_stop_rxs = (0..config.reclaimers)
            .map(|_| stop_tx.subscribe())
            .collect_vec();

        let metrics = match (
            config.prometheus_config.registry,
            config.prometheus_config.namespace,
        ) {
            (Some(registry), Some(namespace)) => {
                Metrics::with_registry_namespace(registry, namespace)
            }
            (Some(registry), None) => Metrics::with_registry(registry),
            (None, Some(namespace)) => Metrics::with_namespace(namespace),
            (None, None) => Metrics::new(),
        };
        let metrics = Arc::new(metrics);

        let store = Arc::new(Self {
            indices: indices.clone(),
            region_manager: region_manager.clone(),
            device: device.clone(),
            admissions: config.admissions,
            reinsertions: config.reinsertions,
            event_listeners: config.event_listeners.clone(),
            handles: Mutex::new(vec![]),
            stop_tx,
            metrics: metrics.clone(),
            _marker: PhantomData,
        });

        for admission in store.admissions.iter() {
            admission.init(&store.indices);
        }
        for reinsertion in store.reinsertions.iter() {
            reinsertion.init(&store.indices);
        }

        let flush_rate_limiter = match config.flush_rate_limit {
            0 => None,
            rate => Some(Arc::new(RateLimiter::new(rate as f64))),
        };
        let reclaim_rate_limiter = match config.reclaim_rate_limit {
            0 => None,
            rate => Some(Arc::new(RateLimiter::new(rate as f64))),
        };

        let flushers = flusher_stop_rxs
            .into_iter()
            .map(|stop_rx| {
                Flusher::new(
                    region_manager.clone(),
                    flush_rate_limiter.clone(),
                    metrics.clone(),
                    stop_rx,
                )
            })
            .collect_vec();
        let reclaimers = reclaimer_stop_rxs
            .into_iter()
            .map(|stop_rx| {
                Reclaimer::new(
                    config.clean_region_threshold,
                    store.clone(),
                    region_manager.clone(),
                    reclaim_rate_limiter.clone(),
                    config.event_listeners.clone(),
                    metrics.clone(),
                    stop_rx,
                )
            })
            .collect_vec();

        let mut handles = vec![];
        handles.append(
            &mut flushers
                .into_iter()
                .map(|flusher| tokio::spawn(async move { flusher.run().await.unwrap() }))
                .collect_vec(),
        );
        handles.append(
            &mut reclaimers
                .into_iter()
                .map(|reclaimer| tokio::spawn(async move { reclaimer.run().await.unwrap() }))
                .collect_vec(),
        );
        store.handles.lock().append(&mut handles);

        store.recover(config.recover_concurrency).await?;

        Ok(store)
    }

    pub async fn shutdown_runners(&self) -> Result<()> {
        self.seal().await?;
        self.stop_tx.send(()).unwrap();
        let handles = self.handles.lock().drain(..).collect_vec();
        for handle in handles {
            handle.await.unwrap();
        }
        Ok(())
    }

    #[tracing::instrument(skip(self, value))]
    pub async fn insert(&self, key: K, value: V) -> Result<bool> {
        let weight = key.serialized_len() + value.serialized_len();
        let writer = StoreWriter::new(self, key, weight);
        writer.finish(value).await
    }

    #[tracing::instrument(skip(self, value))]
    pub async fn insert_if_not_exists(&self, key: K, value: V) -> Result<bool> {
        if self.exists(&key)? {
            return Ok(false);
        }
        self.insert(key, value).await
    }

    /// First judge if the entry will be admitted with `key` and `weight` by admission policies.
    /// Then `f` will be called and entry will be inserted.
    ///
    /// # Safety
    ///
    /// `weight` MUST be equal to `key.serialized_len() + value.serialized_len()`
    #[tracing::instrument(skip(self, f))]
    pub async fn insert_with<F>(&self, key: K, f: F, weight: usize) -> Result<bool>
    where
        F: FnOnce() -> anyhow::Result<V>,
    {
        let mut writer = self.writer(key, weight);
        if !writer.judge() {
            return Ok(false);
        }
        let value = f().map_err(Error::fetch_value)?;
        writer.finish(value).await
    }

    /// First judge if the entry will be admitted with `key` and `weight` by admission policies.
    /// Then `f` will be called to fetch value, and entry will be inserted.
    ///
    /// # Safety
    ///
    /// `weight` MUST be equal to `key.serialized_len() + value.serialized_len()`
    #[tracing::instrument(skip(self, f))]
    pub async fn insert_with_future<F, FU>(&self, key: K, f: F, weight: usize) -> Result<bool>
    where
        F: FnOnce() -> FU,
        FU: FetchValueFuture<V>,
    {
        let mut writer = self.writer(key, weight);
        if !writer.judge() {
            return Ok(false);
        }
        let value = f().await.map_err(Error::fetch_value)?;
        writer.finish(value).await
    }

    #[tracing::instrument(skip(self, f))]
    pub async fn insert_if_not_exists_with<F>(&self, key: K, f: F, weight: usize) -> Result<bool>
    where
        F: FnOnce() -> anyhow::Result<V>,
    {
        if self.exists(&key)? {
            return Ok(false);
        }
        self.insert_with(key, f, weight).await
    }

    #[tracing::instrument(skip(self, f))]
    pub async fn insert_if_not_exists_with_future<F, FU>(
        &self,
        key: K,
        f: F,
        weight: usize,
    ) -> Result<bool>
    where
        F: FnOnce() -> FU,
        FU: FetchValueFuture<V>,
    {
        if self.exists(&key)? {
            return Ok(false);
        }
        self.insert_with_future(key, f, weight).await
    }

    /// `weight` MUST be equal to `key.serialized_len() + value.serialized_len()`
    #[tracing::instrument(skip(self))]
    pub fn writer(&self, key: K, weight: usize) -> StoreWriter<'_, K, V, D, EP, EL> {
        StoreWriter::new(self, key, weight)
    }

    #[tracing::instrument(skip(self))]
    pub fn exists(&self, key: &K) -> Result<bool> {
        Ok(self.indices.lookup(key).is_some())
    }

    #[tracing::instrument(skip(self))]
    pub async fn lookup(&self, key: &K) -> Result<Option<V>> {
        let now = Instant::now();

        let index = match self.indices.lookup(key) {
            Some(index) => index,
            None => {
                self.metrics
                    .latency_lookup_miss
                    .observe(now.elapsed().as_secs_f64());
                return Ok(None);
            }
        };

        self.region_manager.record_access(&index.region);
        let region = self.region_manager.region(&index.region);
        let start = index.offset as usize;
        let end = start + index.len as usize;

        // TODO(MrCroxx): read value only
        let slice = match region.load(start..end, index.version).await? {
            Some(slice) => slice,
            None => {
                self.metrics
                    .latency_lookup_miss
                    .observe(now.elapsed().as_secs_f64());
                return Ok(None);
            }
        };
        self.metrics.bytes_lookup.inc_by(slice.len() as u64);

        let res = match read_entry::<K, V>(slice.as_ref()) {
            Some((_key, value)) => Ok(Some(value)),
            None => Ok(None),
        };
        slice.destroy().await;

        self.metrics
            .latency_lookup_hit
            .observe(now.elapsed().as_secs_f64());

        res
    }

    #[tracing::instrument(skip(self))]
    pub async fn remove(&self, key: &K) -> Result<bool> {
        let _timer = self.metrics.latency_remove.start_timer();

        let res = self.indices.remove(key).is_some();

        if res {
            for listener in self.event_listeners.iter() {
                listener.on_remove(key).await?;
            }
        }

        Ok(res)
    }

    #[tracing::instrument(skip(self))]
    pub async fn clear(&self) -> Result<()> {
        let _timer = self.metrics.latency_remove.start_timer();

        self.indices.clear();

        for listener in self.event_listeners.iter() {
            listener.on_clear().await?;
        }

        // TODO(MrCroxx): set all regions as clean?

        Ok(())
    }

    pub(crate) fn indices(&self) -> &Arc<Indices<K>> {
        &self.indices
    }

    pub(crate) fn reinsertions(&self) -> &Vec<Arc<dyn ReinsertionPolicy<Key = K, Value = V>>> {
        &self.reinsertions
    }

    fn serialized_len(&self, key: &K, value: &V) -> usize {
        let unaligned =
            key.serialized_len() + value.serialized_len() + EntryFooter::serialized_len();
        bits::align_up(self.device.align(), unaligned)
    }

    async fn seal(&self) -> Result<()> {
        match self
            .region_manager
            .allocate(self.device.region_size() - self.device.align())
            .await
        {
            crate::region::AllocateResult::Full { mut slice, remain } => {
                // current region is full, write region footer and try allocate again
                let footer = RegionFooter {
                    magic: REGION_MAGIC,
                    padding: remain as u64,
                };
                footer.write(slice.as_mut());
                slice.destroy().await;
            }
            crate::region::AllocateResult::Ok(slice) => {
                // region is empty, skip
                slice.destroy().await
            }
        }
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn recover(&self, concurrency: usize) -> Result<()> {
        tracing::info!("start store recovery");

        let (tx, rx) = async_channel::bounded(concurrency);

        let mut handles = vec![];
        for region_id in 0..self.device.regions() as RegionId {
            let itx = tx.clone();
            let irx = rx.clone();
            let region_manager = self.region_manager.clone();
            let indices = self.indices.clone();
            let event_listeners = self.event_listeners.clone();
            let handle = tokio::spawn(async move {
                itx.send(()).await.unwrap();
                let res =
                    Self::recover_region(region_id, region_manager, indices, event_listeners).await;
                irx.recv().await.unwrap();
                res
            });
            handles.push(handle);
        }

        let mut recovered = 0;
        for (region_id, handle) in handles.into_iter().enumerate() {
            if handle.await.map_err(Error::other)?? {
                tracing::debug!("region {} is recovered", region_id);
                recovered += 1;
            }
        }

        tracing::info!("finish store recovery, {} region recovered", recovered);

        Ok(())
    }

    /// return `true` if region is valid, otherwise `false`
    async fn recover_region(
        region_id: RegionId,
        region_manager: Arc<RegionManager<D, EP, EL>>,
        indices: Arc<Indices<K>>,
        event_listeners: Vec<Arc<dyn EventListener<K = K, V = V>>>,
    ) -> Result<bool> {
        let region = region_manager.region(&region_id).clone();
        let res = if let Some(mut iter) = RegionEntryIter::<K, V, D>::open(region).await? {
            while let Some(index) = iter.next().await? {
                for listener in event_listeners.iter() {
                    listener.on_recover(&index.key).await?;
                }
                indices.insert(index);
            }
            region_manager.eviction_push(region_id);
            true
        } else {
            region_manager.clean_regions().release(region_id);
            false
        };
        Ok(res)
    }

    fn judge_inner(&self, writer: &mut StoreWriter<'_, K, V, D, EP, EL>) {
        for (index, admission) in self.admissions.iter().enumerate() {
            let judge = admission.judge(&writer.key, writer.weight, &self.metrics);
            writer.judges.set(index, judge);
        }
        writer.is_judged = true;
    }

    async fn apply_writer(
        &self,
        mut writer: StoreWriter<'_, K, V, D, EP, EL>,
        value: V,
    ) -> Result<bool> {
        debug_assert!(!writer.is_inserted);

        let now = Instant::now();

        if !writer.judge() {
            return Ok(false);
        }

        writer.is_inserted = true;
        let key = &writer.key;

        for (i, admission) in self.admissions.iter().enumerate() {
            let judge = writer.judges.get(i);
            admission.on_insert(key, writer.weight, &self.metrics, judge);
        }

        let serialized_len = self.serialized_len(key, &value);
        assert_eq!(key.serialized_len() + value.serialized_len(), writer.weight);

        self.metrics.bytes_insert.inc_by(serialized_len as u64);

        let mut slice = match self.region_manager.allocate(serialized_len).await {
            crate::region::AllocateResult::Ok(slice) => slice,
            crate::region::AllocateResult::Full { mut slice, remain } => {
                // current region is full, write region footer and try allocate again
                let footer = RegionFooter {
                    magic: REGION_MAGIC,
                    padding: remain as u64,
                };
                footer.write(slice.as_mut());
                slice.destroy().await;
                self.region_manager.allocate(serialized_len).await.unwrap()
            }
        };

        write_entry(slice.as_mut(), key, &value);

        let index = Index {
            region: slice.region_id(),
            version: slice.version(),
            offset: slice.offset() as u32,
            len: slice.len() as u32,
            key_len: key.serialized_len() as u32,
            value_len: value.serialized_len() as u32,

            key: key.clone(),
        };

        slice.destroy().await;

        self.indices.insert(index);

        for listener in self.event_listeners.iter() {
            listener.on_insert(key).await?;
        }

        let duration = now.elapsed() + writer.duration;
        self.metrics
            .latency_insert_inserted
            .observe(duration.as_secs_f64());

        Ok(true)
    }
}

pub struct StoreWriter<'a, K, V, D, EP, EL>
where
    K: Key,
    V: Value,
    D: Device,
    EP: EvictionPolicy<Adapter = RegionEpItemAdapter<EL>>,
    EL: Link,
{
    store: &'a Store<K, V, D, EP, EL>,
    key: K,
    weight: usize,

    judges: Judges,
    is_judged: bool,

    /// judge duration
    duration: Duration,

    is_inserted: bool,
}

impl<'a, K, V, D, EP, EL> StoreWriter<'a, K, V, D, EP, EL>
where
    K: Key,
    V: Value,
    D: Device,
    EP: EvictionPolicy<Adapter = RegionEpItemAdapter<EL>>,
    EL: Link,
{
    fn new(store: &'a Store<K, V, D, EP, EL>, key: K, weight: usize) -> Self {
        Self {
            store,
            key,
            weight,
            judges: Judges::new(store.admissions.len()),
            is_judged: false,
            duration: Duration::from_nanos(0),
            is_inserted: false,
        }
    }

    /// Judge if the entry can be admitted by configured admission policies.
    pub fn judge(&mut self) -> bool {
        let now = Instant::now();
        if !self.is_judged {
            self.store.judge_inner(self);
        }
        self.duration += now.elapsed();
        self.judges.judge()
    }

    pub async fn finish(self, value: V) -> Result<bool> {
        self.store.apply_writer(self, value).await
    }
}

impl<'a, K, V, D, EP, EL> Debug for StoreWriter<'a, K, V, D, EP, EL>
where
    K: Key,
    V: Value,
    D: Device,
    EP: EvictionPolicy<Adapter = RegionEpItemAdapter<EL>>,
    EL: Link,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreWriter")
            .field("key", &self.key)
            .field("weight", &self.weight)
            .field("judges", &self.judges)
            .field("is_judged", &self.is_judged)
            .field("duration", &self.duration)
            .field("inserted", &self.is_inserted)
            .finish()
    }
}

impl<'a, K, V, D, EP, EL> Drop for StoreWriter<'a, K, V, D, EP, EL>
where
    K: Key,
    V: Value,
    D: Device,
    EP: EvictionPolicy<Adapter = RegionEpItemAdapter<EL>>,
    EL: Link,
{
    fn drop(&mut self) {
        if !self.is_inserted {
            self.store
                .metrics
                .latency_insert_dropped
                .observe(self.duration.as_secs_f64());
            let mut filtered = false;
            if self.is_judged {
                for (i, admission) in self.store.admissions.iter().enumerate() {
                    let judge = self.judges.get(i);
                    admission.on_drop(&self.key, self.weight, &self.store.metrics, judge);
                }
                filtered = !self.judge();
            }
            if filtered {
                self.store
                    .metrics
                    .latency_insert_filtered
                    .observe(self.duration.as_secs_f64());
            } else {
                self.store
                    .metrics
                    .latency_insert_dropped
                    .observe(self.duration.as_secs_f64());
            }
        }
    }
}

#[derive(Debug)]
struct EntryFooter {
    key_len: u32,
    value_len: u32,
    padding: u32,
    checksum: u64,
}

impl EntryFooter {
    fn serialized_len() -> usize {
        4 + 4 + 4 + 8
    }

    fn write(&self, mut buf: &mut [u8]) {
        buf.put_u32(self.key_len);
        buf.put_u32(self.value_len);
        buf.put_u32(self.padding);
        buf.put_u64(self.checksum);
    }

    #[allow(dead_code)]
    fn read(mut buf: &[u8]) -> Self {
        let key_len = buf.get_u32();
        let value_len = buf.get_u32();
        let padding = buf.get_u32();
        let checksum = buf.get_u64();

        Self {
            key_len,
            value_len,
            padding,
            checksum,
        }
    }
}

#[derive(Debug)]
struct RegionFooter {
    /// magic number to decide a valid region
    magic: u64,

    /// padding from the end of the last entry footer to the end of region
    padding: u64,
}

impl RegionFooter {
    fn write(&self, buf: &mut [u8]) {
        let mut offset = buf.len();

        offset -= 8;
        (&mut buf[offset..offset + 8]).put_u64(self.magic);

        offset -= 8;
        (&mut buf[offset..offset + 8]).put_u64(self.padding);
    }

    fn read(buf: &[u8]) -> Self {
        let mut offset = buf.len();

        offset -= 8;
        let magic = (&buf[offset..offset + 8]).get_u64();

        offset -= 8;
        let padding = (&buf[offset..offset + 8]).get_u64();

        Self { magic, padding }
    }
}

/// | value | key | <padding> | footer |
///
/// # Safety
///
/// `buf.len()` must excatly fit entry size
fn write_entry<K, V>(buf: &mut [u8], key: &K, value: &V)
where
    K: Key,
    V: Value,
{
    let mut offset = 0;
    value.write(&mut buf[offset..offset + value.serialized_len()]);
    offset += value.serialized_len();
    key.write(&mut buf[offset..offset + key.serialized_len()]);
    offset += key.serialized_len();

    let checksum = checksum(&buf[..offset]);
    let padding = buf.len() as u32
        - key.serialized_len() as u32
        - value.serialized_len() as u32
        - EntryFooter::serialized_len() as u32;

    let footer = EntryFooter {
        key_len: key.serialized_len() as u32,
        value_len: value.serialized_len() as u32,
        padding,
        checksum,
    };
    offset = buf.len() - EntryFooter::serialized_len();
    footer.write(&mut buf[offset..]);
}

/// | value | key | <padding> | footer |
///
/// # Safety
///
/// `buf.len()` must excatly fit entry size
fn read_entry<K, V>(buf: &[u8]) -> Option<(K, V)>
where
    K: Key,
    V: Value,
{
    let mut offset = buf.len();

    offset -= EntryFooter::serialized_len();
    let footer = EntryFooter::read(&buf[offset..]);

    offset = 0;
    let value = V::read(&buf[offset..offset + footer.value_len as usize]);

    offset += footer.value_len as usize;
    let key = K::read(&buf[offset..offset + footer.key_len as usize]);

    offset += footer.key_len as usize;
    let checksum = checksum(&buf[..offset]);
    if checksum != footer.checksum {
        tracing::warn!(
            "read entry error: {}",
            Error::ChecksumMismatch {
                checksum,
                expected: footer.checksum,
            }
        );
        return None;
    }

    Some((key, value))
}

fn checksum(buf: &[u8]) -> u64 {
    let mut hasher = XxHash64::with_seed(0);
    hasher.write(buf);
    hasher.finish()
}

pub struct RegionEntryIter<K, V, D>
where
    K: Key,
    V: Value,
    D: Device,
{
    region: Region<D>,

    cursor: usize,

    _marker: PhantomData<(K, V)>,
}

impl<K, V, D> RegionEntryIter<K, V, D>
where
    K: Key,
    V: Value,
    D: Device,
{
    pub async fn open(region: Region<D>) -> Result<Option<Self>> {
        let region_size = region.device().region_size();
        let align = region.device().align();

        let slice = match region.load(region_size - align..region_size, 0).await? {
            Some(slice) => slice,
            None => return Ok(None),
        };

        let footer = RegionFooter::read(slice.as_ref());
        slice.destroy().await;

        if footer.magic != REGION_MAGIC {
            return Ok(None);
        }
        let cursor = region_size - footer.padding as usize;
        Ok(Some(Self {
            region,
            cursor,
            _marker: PhantomData,
        }))
    }

    pub async fn next(&mut self) -> Result<Option<Index<K>>> {
        if self.cursor == 0 {
            return Ok(None);
        }

        let align = self.region.device().align();

        let slice = self
            .region
            .load(self.cursor - align..self.cursor, 0)
            .await?
            .unwrap();

        let footer =
            EntryFooter::read(&slice.as_ref()[align - EntryFooter::serialized_len()..align]);
        let entry_len = (footer.value_len + footer.key_len + footer.padding) as usize
            + EntryFooter::serialized_len();

        let abs_start = self.cursor - entry_len + footer.value_len as usize;
        let abs_end = self.cursor - entry_len + (footer.value_len + footer.key_len) as usize;
        let align_start = bits::align_down(align, abs_start);
        let align_end = bits::align_up(align, abs_end);

        let key = if align_start == self.cursor - align && align_end == self.cursor {
            // key and foooter in the same block, read directly from slice
            let rel_start =
                align - EntryFooter::serialized_len() - (footer.padding + footer.key_len) as usize;
            let rel_end = align - EntryFooter::serialized_len() - footer.padding as usize;
            let key = K::read(&slice.as_ref()[rel_start..rel_end]);
            slice.destroy().await;
            key
        } else {
            slice.destroy().await;
            let s = self.region.load(align_start..align_end, 0).await?.unwrap();
            let rel_start = abs_start - align_start;
            let rel_end = abs_end - align_start;

            let key = K::read(&s.as_ref()[rel_start..rel_end]);
            s.destroy().await;
            key
        };

        self.cursor -= entry_len;

        Ok(Some(Index {
            key,
            region: self.region.id(),
            version: 0,
            offset: self.cursor as u32,
            len: entry_len as u32,
            key_len: footer.key_len,
            value_len: footer.value_len,
        }))
    }

    pub async fn next_kv(&mut self) -> Result<Option<(K, V)>> {
        let index = match self.next().await {
            Ok(Some(index)) => index,
            Ok(None) => return Ok(None),
            Err(e) => return Err(e),
        };

        // TODO(MrCroxx): Optimize if all key, value and footer are in the same read block.
        let start = index.offset as usize;
        let end = start + index.len as usize;
        let slice = self.region.load(start..end, 0).await?.unwrap();
        let kv = read_entry::<K, V>(slice.as_ref());
        slice.destroy().await;

        Ok(kv)
    }
}

#[cfg(test)]
pub mod tests {
    use std::{collections::HashSet, path::PathBuf};

    use foyer_intrusive::eviction::fifo::{Fifo, FifoConfig, FifoLink};

    use crate::device::fs::{FsDevice, FsDeviceConfig};

    use super::*;

    type TestStore = Store<u64, Vec<u8>, FsDevice, Fifo<RegionEpItemAdapter<FifoLink>>, FifoLink>;

    type TestStoreConfig = StoreConfig<u64, Vec<u8>, FsDevice, Fifo<RegionEpItemAdapter<FifoLink>>>;

    #[derive(Debug, Clone)]
    enum Record<K: Key> {
        Admit(K),
        Evict(K),
    }

    #[derive(Debug)]
    struct JudgeRecorder<K, V>
    where
        K: Key,
        V: Value,
    {
        records: Mutex<Vec<Record<K>>>,
        _marker: PhantomData<V>,
    }

    impl<K, V> JudgeRecorder<K, V>
    where
        K: Key,
        V: Value,
    {
        fn dump(&self) -> Vec<Record<K>> {
            self.records.lock().clone()
        }

        fn remains(&self) -> HashSet<K> {
            let records = self.dump();
            let mut res = HashSet::default();
            for record in records {
                match record {
                    Record::Admit(key) => {
                        res.insert(key);
                    }
                    Record::Evict(key) => {
                        res.remove(&key);
                    }
                }
            }
            res
        }
    }

    impl<K, V> Default for JudgeRecorder<K, V>
    where
        K: Key,
        V: Value,
    {
        fn default() -> Self {
            Self {
                records: Mutex::new(Vec::default()),
                _marker: PhantomData,
            }
        }
    }

    impl<K, V> AdmissionPolicy for JudgeRecorder<K, V>
    where
        K: Key,
        V: Value,
    {
        type Key = K;

        type Value = V;

        fn judge(&self, key: &K, _weight: usize, _metrics: &Arc<Metrics>) -> bool {
            self.records.lock().push(Record::Admit(key.clone()));
            true
        }

        fn on_insert(&self, _key: &K, _weight: usize, _metrics: &Arc<Metrics>, _judge: bool) {}

        fn on_drop(&self, _key: &K, _weight: usize, _metrics: &Arc<Metrics>, _judge: bool) {}
    }

    impl<K, V> ReinsertionPolicy for JudgeRecorder<K, V>
    where
        K: Key,
        V: Value,
    {
        type Key = K;

        type Value = V;

        fn judge(&self, key: &K, _weight: usize, _metrics: &Arc<Metrics>) -> bool {
            self.records.lock().push(Record::Evict(key.clone()));
            false
        }

        fn on_insert(
            &self,
            _key: &Self::Key,
            _weight: usize,
            _metrics: &Arc<crate::metrics::Metrics>,
            _judge: bool,
        ) {
        }

        fn on_drop(
            &self,
            _key: &Self::Key,
            _weight: usize,
            _metrics: &Arc<crate::metrics::Metrics>,
            _judge: bool,
        ) {
        }
    }

    #[tokio::test]
    #[allow(clippy::identity_op)]
    async fn test_recovery() {
        const KB: usize = 1024;
        const MB: usize = 1024 * 1024;

        let tempdir = tempfile::tempdir().unwrap();

        let recorder = Arc::new(JudgeRecorder::default());
        let admissions: Vec<Arc<dyn AdmissionPolicy<Key = u64, Value = Vec<u8>>>> =
            vec![recorder.clone()];
        let reinsertions: Vec<Arc<dyn ReinsertionPolicy<Key = u64, Value = Vec<u8>>>> =
            vec![recorder.clone()];

        let config = TestStoreConfig {
            eviction_config: FifoConfig {
                segment_ratios: vec![1],
            },
            device_config: FsDeviceConfig {
                dir: PathBuf::from(tempdir.path()),
                capacity: 16 * MB,
                file_capacity: 4 * MB,
                align: 4096,
                io_size: 4096 * KB,
            },
            admissions,
            reinsertions,
            buffer_pool_size: 8 * MB,
            flushers: 1,
            flush_rate_limit: 0,
            reclaimers: 1,
            reclaim_rate_limit: 0,
            recover_concurrency: 2,
            event_listeners: vec![],
            prometheus_config: PrometheusConfig::default(),
            clean_region_threshold: 1,
        };

        let store = TestStore::open(config).await.unwrap();

        // files:
        // [0, 1, 2]
        // [3, 4, 5]
        // [6, 7, 8]
        // [9, 10, 11]
        for i in 0..12 {
            store.insert(i, vec![i as u8; 1 * MB]).await.unwrap();
        }

        store.shutdown_runners().await.unwrap();

        let remains = recorder.remains();

        for i in 0..12 {
            if remains.contains(&i) {
                assert_eq!(
                    store.lookup(&i).await.unwrap().unwrap(),
                    vec![i as u8; 1 * MB],
                );
            } else {
                assert!(store.lookup(&i).await.unwrap().is_none());
            }
        }

        drop(store);

        let config = TestStoreConfig {
            eviction_config: FifoConfig {
                segment_ratios: vec![1],
            },
            device_config: FsDeviceConfig {
                dir: PathBuf::from(tempdir.path()),
                capacity: 16 * MB,
                file_capacity: 4 * MB,
                align: 4096,
                io_size: 4096 * KB,
            },
            admissions: vec![],
            reinsertions: vec![],
            buffer_pool_size: 8 * MB,
            flushers: 1,
            flush_rate_limit: 0,
            reclaimers: 0,
            reclaim_rate_limit: 0,
            recover_concurrency: 2,
            event_listeners: vec![],
            prometheus_config: PrometheusConfig::default(),
            clean_region_threshold: 1,
        };
        let store = TestStore::open(config).await.unwrap();

        store.shutdown_runners().await.unwrap();

        for i in 0..12 {
            if remains.contains(&i) {
                assert_eq!(
                    store.lookup(&i).await.unwrap().unwrap(),
                    vec![i as u8; 1 * MB],
                );
            } else {
                assert!(store.lookup(&i).await.unwrap().is_none());
            }
        }

        drop(store);
    }
}
