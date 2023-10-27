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
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use bitmaps::Bitmap;
use bytes::{Buf, BufMut};
use foyer_common::{bits, rate::RateLimiter};
use foyer_intrusive::eviction::EvictionPolicy;
use futures::future::try_join_all;
use itertools::Itertools;
use parking_lot::Mutex;
use tokio::{
    sync::{broadcast, mpsc, Semaphore},
    task::JoinHandle,
};
use twox_hash::XxHash64;

use crate::{
    admission::AdmissionPolicy,
    catalog::{Catalog, Index, Item, Sequence},
    device::Device,
    error::Result,
    flusher_v2::{Entry, Flusher},
    judge::Judges,
    metrics::{Metrics, METRICS},
    reclaimer::Reclaimer,
    region::{Region, RegionHeader, RegionId, REGION_MAGIC},
    region_manager::{RegionEpItemAdapter, RegionManager},
    reinsertion::ReinsertionPolicy,
    ring::RingBuffer,
    storage::{Storage, StorageWriter},
};
use foyer_common::code::{Key, Value};
use foyer_intrusive::core::adapter::Link;
use std::hash::Hasher;

const DEFAULT_BROADCAST_CAPACITY: usize = 4096;

pub struct GenericStoreConfig<K, V, D, EP>
where
    K: Key,
    V: Value,
    D: Device,
    EP: EvictionPolicy,
{
    /// For distinguish different foyer metrics.
    ///
    /// Metrics of this foyer instance has label `foyer = {{ name }}`.
    pub name: String,

    /// Evictino policy configurations.
    pub eviction_config: EP::Config,

    /// Device configurations.
    pub device_config: D::Config,

    /// The count of allocators is `2 ^ allocator bits`.
    ///
    /// Note: The count of allocators should be greater than buffer count.
    ///       (buffer count = buffer pool size / device region size)
    pub allocator_bits: usize,

    /// `ring_buffer_capacity` will be aligned up to device align.
    pub ring_buffer_capacity: usize,

    /// Catalog indices sharding bits.
    pub catalog_bits: usize,

    /// Admission policies.
    pub admissions: Vec<Arc<dyn AdmissionPolicy<Key = K, Value = V>>>,

    /// Reinsertion policies.
    pub reinsertions: Vec<Arc<dyn ReinsertionPolicy<Key = K, Value = V>>>,

    /// Buffer pool size, should be a multiplier of device region size.
    pub buffer_pool_size: usize,

    /// Count of flushers.
    pub flushers: usize,

    /// Flush rate limits.
    pub flush_rate_limit: usize,

    /// Count of reclaimers.
    pub reclaimers: usize,

    /// Flush rate limits.
    pub reclaim_rate_limit: usize,

    /// Allocation timout for skippable writers.
    pub allocation_timeout: Duration,

    /// Clean region count threshold to trigger reclamation.
    ///
    /// `clean_region_threshold` is recommended to be equal or larger than `reclaimers`.
    pub clean_region_threshold: usize,

    /// Concurrency of recovery.
    pub recover_concurrency: usize,
}

impl<K, V, D, EP> Debug for GenericStoreConfig<K, V, D, EP>
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
            .field("allocator_bits", &self.allocator_bits)
            .field("ring_buffer_capacity", &self.ring_buffer_capacity)
            .field("catalog_bits", &self.catalog_bits)
            .field("admissions", &self.admissions)
            .field("reinsertions", &self.reinsertions)
            .field("buffer_pool_size", &self.buffer_pool_size)
            .field("flushers", &self.flushers)
            .field("flush_rate_limit", &self.flush_rate_limit)
            .field("reclaimers", &self.reclaimers)
            .field("reclaim_rate_limit", &self.reclaim_rate_limit)
            .field("allocation_timeout", &self.allocation_timeout)
            .field("clean_region_threshold", &self.clean_region_threshold)
            .field("recover_concurrency", &self.recover_concurrency)
            .finish()
    }
}

impl<K, V, D, EP> Clone for GenericStoreConfig<K, V, D, EP>
where
    K: Key,
    V: Value,
    D: Device,
    EP: EvictionPolicy,
{
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            eviction_config: self.eviction_config.clone(),
            device_config: self.device_config.clone(),
            allocator_bits: self.allocator_bits,
            ring_buffer_capacity: self.ring_buffer_capacity,
            catalog_bits: self.catalog_bits,
            admissions: self.admissions.clone(),
            reinsertions: self.reinsertions.clone(),
            buffer_pool_size: self.buffer_pool_size,
            flushers: self.flushers,
            flush_rate_limit: self.flush_rate_limit,
            reclaimers: self.reclaimers,
            reclaim_rate_limit: self.reclaim_rate_limit,
            allocation_timeout: self.allocation_timeout,
            clean_region_threshold: self.clean_region_threshold,
            recover_concurrency: self.recover_concurrency,
        }
    }
}

#[derive(Debug)]
pub struct GenericStore<K, V, D, EP, EL>
where
    K: Key,
    V: Value,
    D: Device,
    EP: EvictionPolicy<Adapter = RegionEpItemAdapter<EL>>,
    EL: Link,
{
    inner: Arc<GenericStoreInner<K, V, D, EP, EL>>,
}

impl<K, V, D, EP, EL> Clone for GenericStore<K, V, D, EP, EL>
where
    K: Key,
    V: Value,
    D: Device,
    EP: EvictionPolicy<Adapter = RegionEpItemAdapter<EL>>,
    EL: Link,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

#[derive(Debug)]
pub struct GenericStoreInner<K, V, D, EP, EL>
where
    K: Key,
    V: Value,
    D: Device,
    EP: EvictionPolicy<Adapter = RegionEpItemAdapter<EL>>,
    EL: Link,
{
    sequence: AtomicU64,
    catalog: Arc<Catalog<K>>,

    region_manager: Arc<RegionManager<D, EP, EL>>,
    ring: Arc<RingBuffer<D::IoBufferAllocator>>,

    device: D,

    admissions: Vec<Arc<dyn AdmissionPolicy<Key = K, Value = V>>>,
    reinsertions: Vec<Arc<dyn ReinsertionPolicy<Key = K, Value = V>>>,

    flusher_entry_txs: Vec<mpsc::UnboundedSender<Entry>>,
    flusher_handles: Mutex<Vec<JoinHandle<()>>>,
    flushers_stop_tx: broadcast::Sender<()>,

    reclaimer_handles: Mutex<Vec<JoinHandle<()>>>,
    reclaimers_stop_tx: broadcast::Sender<()>,

    metrics: Arc<Metrics>,

    _marker: PhantomData<V>,
}

impl<K, V, D, EP, EL> GenericStore<K, V, D, EP, EL>
where
    K: Key,
    V: Value,
    D: Device,
    EP: EvictionPolicy<Adapter = RegionEpItemAdapter<EL>>,
    EL: Link,
{
    async fn open(config: GenericStoreConfig<K, V, D, EP>) -> Result<Self> {
        tracing::info!("open store with config:\n{:#?}", config);

        let metrics = Arc::new(METRICS.foyer(&config.name));

        let device = D::open(config.device_config).await?;
        assert!(device.regions() >= config.flushers * 2);

        let buffer_count = config.buffer_pool_size / device.region_size();

        if buffer_count < (1 << config.allocator_bits) {
            return Err(anyhow::anyhow!(
                "The count of allocators shoule be greater than buffer count."
            )
            .into());
        }

        let ring = Arc::new(RingBuffer::with_metrics_in(
            device.align(),
            config.ring_buffer_capacity,
            metrics.clone(),
            device.io_buffer_allocator().clone(),
        ));

        let region_manager = Arc::new(RegionManager::new(
            config.allocator_bits,
            buffer_count,
            device.regions(),
            config.eviction_config,
            device.clone(),
            config.allocation_timeout,
            metrics.clone(),
        ));

        let catalog = Arc::new(Catalog::new(
            device.regions(),
            config.catalog_bits,
            metrics.clone(),
        ));

        let (flushers_stop_tx, _) = broadcast::channel(DEFAULT_BROADCAST_CAPACITY);
        let flusher_stop_rxs = (0..config.flushers)
            .map(|_| flushers_stop_tx.subscribe())
            .collect_vec();
        let (flusher_entry_txs, flusher_entry_rxs): (
            Vec<mpsc::UnboundedSender<Entry>>,
            Vec<mpsc::UnboundedReceiver<Entry>>,
        ) = (0..config.flushers)
            .map(|_| mpsc::unbounded_channel())
            .unzip();

        let (reclaimers_stop_tx, _) = broadcast::channel(DEFAULT_BROADCAST_CAPACITY);
        let reclaimer_stop_rxs = (0..config.reclaimers)
            .map(|_| reclaimers_stop_tx.subscribe())
            .collect_vec();

        let inner = GenericStoreInner {
            sequence: AtomicU64::new(0),
            catalog: catalog.clone(),
            region_manager: region_manager.clone(),
            ring,
            device: device.clone(),
            admissions: config.admissions,
            reinsertions: config.reinsertions,
            flusher_entry_txs,
            flusher_handles: Mutex::new(vec![]),
            reclaimer_handles: Mutex::new(vec![]),
            flushers_stop_tx,
            reclaimers_stop_tx,
            metrics: metrics.clone(),
            _marker: PhantomData,
        };
        let store = Self {
            inner: Arc::new(inner),
        };

        for admission in store.inner.admissions.iter() {
            admission.init(&store.inner.catalog);
        }
        for reinsertion in store.inner.reinsertions.iter() {
            reinsertion.init(&store.inner.catalog);
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
            .zip_eq(flusher_entry_rxs.into_iter())
            .map(|(stop_rx, entry_rx)| {
                Flusher::new(
                    region_manager.clone(),
                    catalog.clone(),
                    device.clone(),
                    entry_rx,
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
                    metrics.clone(),
                    stop_rx,
                )
            })
            .collect_vec();

        let sequence = store.recover(config.recover_concurrency).await?;
        store.inner.sequence.store(sequence + 1, Ordering::Relaxed);

        let flusher_handles = flushers
            .into_iter()
            .map(|flusher| tokio::spawn(async move { flusher.run().await.unwrap() }))
            .collect_vec();
        let reclaimer_handles = reclaimers
            .into_iter()
            .map(|reclaimer| tokio::spawn(async move { reclaimer.run().await.unwrap() }))
            .collect_vec();

        *store.inner.flusher_handles.lock() = flusher_handles;
        *store.inner.reclaimer_handles.lock() = reclaimer_handles;

        Ok(store)
    }

    async fn close(&self) -> Result<()> {
        // seal current dirty buffer and trigger flushing
        self.seal().await;

        // stop and wait for flushers
        let handles = self.inner.flusher_handles.lock().drain(..).collect_vec();
        if !handles.is_empty() {
            self.inner.flushers_stop_tx.send(()).unwrap();
        }
        for handle in handles {
            handle.await.unwrap();
        }

        // stop and wait for reclaimers
        let handles = self.inner.reclaimer_handles.lock().drain(..).collect_vec();
        if !handles.is_empty() {
            self.inner.reclaimers_stop_tx.send(()).unwrap();
        }
        for handle in handles {
            handle.await.unwrap();
        }

        Ok(())
    }

    /// `weight` MUST be equal to `key.serialized_len() + value.serialized_len()`
    #[tracing::instrument(skip(self))]
    fn writer(&self, key: K, weight: usize) -> GenericStoreWriter<K, V, D, EP, EL> {
        GenericStoreWriter::new(self.clone(), key, weight)
    }

    #[tracing::instrument(skip(self))]
    fn exists(&self, key: &K) -> Result<bool> {
        Ok(self.inner.catalog.lookup(key).is_some())
    }

    #[tracing::instrument(skip(self))]
    async fn lookup(&self, key: &K) -> Result<Option<V>> {
        let now = Instant::now();

        let item = match self.inner.catalog.lookup(key) {
            Some(item) => item,
            None => {
                self.inner
                    .metrics
                    .op_duration_lookup_miss
                    .observe(now.elapsed().as_secs_f64());
                return Ok(None);
            }
        };

        match item.index() {
            crate::catalog::Index::RingBuffer { view } => {
                let res = match read_entry::<K, V>(view.as_ref()) {
                    Some((_key, value)) => Ok(Some(value)),
                    None => {
                        // Remove index if the storage layer fails to lookup it (because of entry magic mismatch).
                        self.inner.catalog.remove(key);
                        Ok(None)
                    }
                };

                self.inner
                    .metrics
                    .op_duration_lookup_hit
                    .observe(now.elapsed().as_secs_f64());

                res
            }
            // read from region
            crate::catalog::Index::Region {
                region,
                version,
                offset,
                len,
                key_len: _,
                value_len: _,
            } => {
                self.inner.region_manager.record_access(region);
                let region = self.inner.region_manager.region(region);
                let start = *offset as usize;
                let end = start + *len as usize;

                // TODO(MrCroxx): read value only
                let slice = match region.load(start..end, *version).await? {
                    Some(slice) => slice,
                    None => {
                        // Remove index if the storage layer fails to lookup it (because of region version mismatch).
                        self.inner.catalog.remove(key);
                        self.inner
                            .metrics
                            .op_duration_lookup_miss
                            .observe(now.elapsed().as_secs_f64());
                        return Ok(None);
                    }
                };

                self.inner
                    .metrics
                    .op_bytes_lookup
                    .inc_by(slice.len() as u64);

                let res = match read_entry::<K, V>(slice.as_ref()) {
                    Some((_key, value)) => Ok(Some(value)),
                    None => {
                        // Remove index if the storage layer fails to lookup it (because of entry magic mismatch).
                        self.inner.catalog.remove(key);
                        Ok(None)
                    }
                };
                drop(slice);

                self.inner
                    .metrics
                    .op_duration_lookup_hit
                    .observe(now.elapsed().as_secs_f64());

                res
            }
        }
    }

    #[tracing::instrument(skip(self))]
    fn remove(&self, key: &K) -> Result<bool> {
        let _timer = self.inner.metrics.op_duration_remove.start_timer();

        let res = self.inner.catalog.remove(key).is_some();

        Ok(res)
    }

    #[tracing::instrument(skip(self))]
    fn clear(&self) -> Result<()> {
        self.inner.catalog.clear();

        // TODO(MrCroxx): set all regions as clean?

        Ok(())
    }

    pub(crate) fn catalog(&self) -> &Arc<Catalog<K>> {
        &self.inner.catalog
    }

    pub(crate) fn reinsertions(&self) -> &Vec<Arc<dyn ReinsertionPolicy<Key = K, Value = V>>> {
        &self.inner.reinsertions
    }

    fn serialized_len(&self, key: &K, value: &V) -> usize {
        let unaligned =
            EntryHeader::serialized_len() + key.serialized_len() + value.serialized_len();
        bits::align_up(self.inner.device.align(), unaligned)
    }

    async fn seal(&self) {
        self.inner.region_manager.seal().await;
    }

    #[tracing::instrument(skip(self))]
    async fn recover(&self, concurrency: usize) -> Result<Sequence> {
        tracing::info!("start store recovery");

        let semaphore = Arc::new(Semaphore::new(concurrency));

        let mut handles = vec![];
        for region_id in 0..self.inner.device.regions() as RegionId {
            let semaphore = semaphore.clone();
            let region_manager = self.inner.region_manager.clone();
            let indices = self.inner.catalog.clone();
            let handle = tokio::spawn(async move {
                let permit = semaphore.acquire().await;
                let res = Self::recover_region(region_id, region_manager, indices).await;
                drop(permit);
                res
            });
            handles.push(handle);
        }

        let mut recovered = 0;
        let mut sequence = 0;

        let results = try_join_all(handles).await.map_err(anyhow::Error::from)?;

        for (region_id, result) in results.into_iter().enumerate() {
            if let Some(seq) = result? {
                tracing::debug!("region {} is recovered", region_id);
                recovered += 1;
                sequence = std::cmp::max(sequence, seq);
            }
        }

        tracing::info!("finish store recovery, {} region recovered", recovered);
        self.inner
            .metrics
            .total_bytes
            .set((recovered * self.inner.device.region_size()) as u64);

        // Force trigger reclamation.
        if recovered == self.inner.device.regions() {
            self.inner.region_manager.clean_regions().flash();
        }

        Ok(sequence)
    }

    /// Return `Some(max sequence)` if region is valid, otherwise `None`
    async fn recover_region(
        region_id: RegionId,
        region_manager: Arc<RegionManager<D, EP, EL>>,
        catalog: Arc<Catalog<K>>,
    ) -> Result<Option<Sequence>> {
        let region = region_manager.region(&region_id).clone();
        let mut sequence = 0;
        let res = if let Some(mut iter) = RegionEntryIter::<K, V, D>::open(region).await? {
            while let Some((key, item)) = iter.next().await? {
                sequence = std::cmp::max(sequence, *item.sequence());
                catalog.insert(Arc::new(key), item);
            }
            region_manager.eviction_push(region_id);
            Some(sequence)
        } else {
            region_manager.clean_regions().release(region_id);
            None
        };
        Ok(res)
    }

    fn judge_inner(&self, writer: &mut GenericStoreWriter<K, V, D, EP, EL>) {
        for (index, admission) in self.inner.admissions.iter().enumerate() {
            let judge = admission.judge(&writer.key, writer.weight, &self.inner.metrics);
            writer.judges.set(index, judge);
        }
        writer.is_judged = true;
    }

    #[tracing::instrument(skip(self, value))]
    async fn apply_writer(
        &self,
        mut writer: GenericStoreWriter<K, V, D, EP, EL>,
        value: V,
    ) -> Result<bool> {
        debug_assert!(!writer.is_inserted);

        if !writer.judge() {
            return Ok(false);
        }

        let now = Instant::now();

        let sequence = if let Some(sequence) = writer.sequence {
            sequence
        } else {
            self.inner.sequence.fetch_add(1, Ordering::Relaxed)
        };

        writer.is_inserted = true;
        let key = writer.key;

        for (i, admission) in self.inner.admissions.iter().enumerate() {
            let judge = writer.judges.get(i);
            admission.on_insert(&key, writer.weight, &self.inner.metrics, judge);
        }

        let serialized_len = self.serialized_len(&key, &value);

        if key.serialized_len() + value.serialized_len() != writer.weight {
            tracing::error!(
                "weight != key.serialized_len() + value.serialized_len(), weight: {}, key size: {}, value size: {}, key: {:?}",
                writer.weight, key.serialized_len(), value.serialized_len(), key
            );
        }

        self.inner
            .metrics
            .op_bytes_insert
            .inc_by(serialized_len as u64);

        let mut view = self.inner.ring.allocate(serialized_len, sequence).await;
        let written = write_entry(&mut view, &key, &value, sequence);
        view.shrink_to(written);
        let view = view.freeze();

        let key = Arc::new(key);

        self.inner.catalog.insert(
            key.clone(),
            Item::new(sequence, Index::RingBuffer { view: view.clone() }),
        );

        let flusher = sequence as usize % self.inner.flusher_entry_txs.len();
        self.inner.flusher_entry_txs[flusher]
            .send(Entry {
                key_len: key.serialized_len(),
                value_len: value.serialized_len(),
                sequence,
                key,
                view,
            })
            .unwrap();

        let duration = now.elapsed() + writer.duration;
        self.inner
            .metrics
            .op_duration_insert_inserted
            .observe(duration.as_secs_f64());

        Ok(true)
    }
}

pub struct GenericStoreWriter<K, V, D, EP, EL>
where
    K: Key,
    V: Value,
    D: Device,
    EP: EvictionPolicy<Adapter = RegionEpItemAdapter<EL>>,
    EL: Link,
{
    store: GenericStore<K, V, D, EP, EL>,
    key: K,
    weight: usize,

    sequence: Option<Sequence>,

    judges: Judges,
    is_judged: bool,

    /// judge duration
    duration: Duration,

    is_inserted: bool,
    is_skippable: bool,
}

impl<K, V, D, EP, EL> GenericStoreWriter<K, V, D, EP, EL>
where
    K: Key,
    V: Value,
    D: Device,
    EP: EvictionPolicy<Adapter = RegionEpItemAdapter<EL>>,
    EL: Link,
{
    fn new(store: GenericStore<K, V, D, EP, EL>, key: K, weight: usize) -> Self {
        let judges = Judges::new(store.inner.admissions.len());
        Self {
            store,
            key,
            weight,
            sequence: None,
            judges,
            is_judged: false,
            duration: Duration::from_nanos(0),
            is_inserted: false,
            is_skippable: false,
        }
    }

    /// Judge if the entry can be admitted by configured admission policies.
    pub fn judge(&mut self) -> bool {
        let store = self.store.clone();
        if !self.is_judged {
            let now = Instant::now();
            store.judge_inner(self);
            self.duration = now.elapsed();
        }
        self.judges.judge()
    }

    pub async fn finish(self, value: V) -> Result<bool> {
        let store = self.store.clone();
        store.apply_writer(self, value).await
    }

    pub fn force(&mut self) {
        self.judges.set_mask(Bitmap::new());
    }

    pub fn set_judge_mask(&mut self, mask: Bitmap<64>) {
        self.judges.set_mask(mask);
    }

    pub fn set_skippable(&mut self) {
        self.is_skippable = true
    }

    pub fn set_sequence(&mut self, sequence: Sequence) {
        self.sequence = Some(sequence);
    }
}

impl<K, V, D, EP, EL> Debug for GenericStoreWriter<K, V, D, EP, EL>
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

// impl<K, V, D, EP, EL> Drop for GenericStoreWriter<K, V, D, EP, EL>
// where
//     K: Key,
//     V: Value,
//     D: Device,
//     EP: EvictionPolicy<Adapter = RegionEpItemAdapter<EL>>,
//     EL: Link,
// {
//     fn drop(&mut self) {
//         if !self.is_inserted {
//             self.store
//                 .inner
//                 .metrics
//                 .op_duration_insert_dropped
//                 .observe(self.duration.as_secs_f64());
//             let mut filtered = false;
//             if self.is_judged {
//                 for (i, admission) in self.store.inner.admissions.iter().enumerate() {
//                     let judge = self.judges.get(i);
//                     admission.on_drop(&self.key, self.weight, &self.store.inner.metrics, judge);
//                 }
//                 filtered = !self.judge();
//             }
//             if filtered {
//                 self.store
//                     .inner
//                     .metrics
//                     .op_duration_insert_filtered
//                     .observe(self.duration.as_secs_f64());
//             } else {
//                 self.store
//                     .inner
//                     .metrics
//                     .op_duration_insert_dropped
//                     .observe(self.duration.as_secs_f64());
//             }
//         }
//     }
// }

const ENTRY_MAGIC: u32 = 0x97_00_00_00;
const ENTRY_MAGIC_MASK: u32 = 0xFF_00_00_00;

#[derive(Debug)]
struct EntryHeader {
    key_len: u32,
    value_len: u32,
    sequence: Sequence,
    checksum: u64,
}

impl EntryHeader {
    fn serialized_len() -> usize {
        4 + 4 + 8 + 8
    }

    fn write(&self, mut buf: &mut [u8]) {
        buf.put_u32(self.key_len | ENTRY_MAGIC);
        buf.put_u32(self.value_len);
        buf.put_u64(self.sequence);
        buf.put_u64(self.checksum);
    }

    fn read(mut buf: &[u8]) -> Option<Self> {
        let head = buf.get_u32();
        let magic = head & ENTRY_MAGIC_MASK;

        if magic != ENTRY_MAGIC {
            return None;
        }

        let key_len = head ^ ENTRY_MAGIC;
        let value_len = buf.get_u32();
        let sequence = buf.get_u64();
        let checksum = buf.get_u64();

        Some(Self {
            key_len,
            value_len,
            sequence,
            checksum,
        })
    }
}

/// | header | value | key | <padding> |
///
/// # Safety
///
/// `buf.len()` must excatly fit entry size
fn write_entry<K, V>(buf: &mut [u8], key: &K, value: &V, sequence: Sequence) -> usize
where
    K: Key,
    V: Value,
{
    let mut offset = EntryHeader::serialized_len();
    value.write(&mut buf[offset..offset + value.serialized_len()]);
    offset += value.serialized_len();
    key.write(&mut buf[offset..offset + key.serialized_len()]);
    offset += key.serialized_len();
    let checksum = checksum(&buf[EntryHeader::serialized_len()..offset]);

    let header = EntryHeader {
        key_len: key.serialized_len() as u32,
        value_len: value.serialized_len() as u32,
        sequence,
        checksum,
    };
    header.write(&mut buf[..EntryHeader::serialized_len()]);
    offset
}

/// | header | value | key | <padding> |
///
/// # Safety
///
/// `buf.len()` must excatly fit entry size
fn read_entry<K, V>(buf: &[u8]) -> Option<(K, V)>
where
    K: Key,
    V: Value,
{
    let header = EntryHeader::read(buf)?;

    let mut offset = EntryHeader::serialized_len();
    let value = V::read(&buf[offset..offset + header.value_len as usize]);
    offset += header.value_len as usize;
    let key = K::read(&buf[offset..offset + header.key_len as usize]);
    offset += header.key_len as usize;

    let checksum = checksum(&buf[EntryHeader::serialized_len()..offset]);
    if checksum != header.checksum {
        tracing::warn!(
            "checksum mismatch, checksum: {}, expected: {}",
            checksum,
            header.checksum,
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
        let align = region.device().align();

        let slice = match region.load(..align, 0).await? {
            Some(slice) => slice,
            None => return Ok(None),
        };

        let header = RegionHeader::read(slice.as_ref());
        drop(slice);

        if header.magic != REGION_MAGIC {
            return Ok(None);
        }

        Ok(Some(Self {
            region,
            cursor: align,
            _marker: PhantomData,
        }))
    }

    pub async fn next(&mut self) -> Result<Option<(K, Item)>> {
        let region_size = self.region.device().region_size();
        let align = self.region.device().align();

        if self.cursor + align >= region_size {
            return Ok(None);
        }

        let Some(slice) = self
            .region
            .load(self.cursor..self.cursor + align, 0)
            .await?
        else {
            return Ok(None);
        };

        let Some(header) = EntryHeader::read(slice.as_ref()) else {
            return Ok(None);
        };

        let entry_len = bits::align_up(
            align,
            (header.value_len + header.key_len) as usize + EntryHeader::serialized_len(),
        );

        let abs_start = self.cursor + EntryHeader::serialized_len() + header.value_len as usize;
        let abs_end = self.cursor
            + EntryHeader::serialized_len()
            + (header.key_len + header.value_len) as usize;

        if abs_start >= abs_end || abs_end > region_size {
            // Double check wrong entry.
            return Ok(None);
        }

        let align_start = bits::align_down(align, abs_start);
        let align_end = bits::align_up(align, abs_end);

        let key = if align_start == self.cursor - align && align_end == self.cursor {
            // header and key are in the same block, read directly from slice
            let rel_start = EntryHeader::serialized_len() + header.value_len as usize;
            let rel_end = rel_start + header.key_len as usize;
            let key = K::read(&slice.as_ref()[rel_start..rel_end]);
            drop(slice);
            key
        } else {
            drop(slice);
            let Some(s) = self.region.load(align_start..align_end, 0).await? else {
                return Ok(None);
            };
            let rel_start = abs_start - align_start;
            let rel_end = abs_end - align_start;

            let key = K::read(&s.as_ref()[rel_start..rel_end]);
            drop(s);
            key
        };

        let info = Item::new(
            header.sequence,
            Index::Region {
                region: self.region.id(),
                version: 0,
                offset: self.cursor as u32,
                len: entry_len as u32,
                key_len: header.key_len,
                value_len: header.value_len,
            },
        );

        self.cursor += entry_len;

        Ok(Some((key, info)))
    }

    pub async fn next_kv(&mut self) -> Result<Option<(K, V)>> {
        let (_, item) = match self.next().await {
            Ok(Some(res)) => res,
            Ok(None) => return Ok(None),
            Err(e) => return Err(e),
        };

        let Index::Region { offset, len, .. } = item.index() else {
            unreachable!("kv loaded from region must have index of region")
        };

        // TODO(MrCroxx): Optimize if all key, value and footer are in the same read block.
        let start = *offset as usize;
        let end = start + *len as usize;
        let Some(slice) = self.region.load(start..end, 0).await? else {
            return Ok(None);
        };
        let kv = read_entry::<K, V>(slice.as_ref());
        drop(slice);

        Ok(kv)
    }
}

impl<K, V, D, EP, EL> StorageWriter for GenericStoreWriter<K, V, D, EP, EL>
where
    K: Key,
    V: Value,
    D: Device,
    EP: EvictionPolicy<Adapter = RegionEpItemAdapter<EL>>,
    EL: Link,
{
    type Key = K;
    type Value = V;

    fn key(&self) -> &Self::Key {
        &self.key
    }

    fn weight(&self) -> usize {
        self.weight
    }

    fn judge(&mut self) -> bool {
        self.judge()
    }

    fn force(&mut self) {
        self.force()
    }

    async fn finish(self, value: Self::Value) -> Result<bool> {
        self.finish(value).await
    }
}

impl<K, V, D, EP, EL> Storage for GenericStore<K, V, D, EP, EL>
where
    K: Key,
    V: Value,
    D: Device,
    EP: EvictionPolicy<Adapter = RegionEpItemAdapter<EL>>,
    EL: Link,
{
    type Key = K;
    type Value = V;
    type Config = GenericStoreConfig<K, V, D, EP>;
    type Writer = GenericStoreWriter<K, V, D, EP, EL>;

    async fn open(config: Self::Config) -> Result<Self> {
        Self::open(config).await
    }

    fn is_ready(&self) -> bool {
        true
    }

    async fn close(&self) -> Result<()> {
        self.close().await
    }

    fn writer(&self, key: Self::Key, weight: usize) -> Self::Writer {
        self.writer(key, weight)
    }

    fn exists(&self, key: &Self::Key) -> Result<bool> {
        self.exists(key)
    }

    async fn lookup(&self, key: &Self::Key) -> Result<Option<Self::Value>> {
        self.lookup(key).await
    }

    fn remove(&self, key: &Self::Key) -> Result<bool> {
        self.remove(key)
    }

    fn clear(&self) -> Result<()> {
        self.clear()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use foyer_intrusive::eviction::fifo::{Fifo, FifoConfig, FifoLink};

    use crate::{
        device::fs::{FsDevice, FsDeviceConfig},
        storage::StorageExt,
        test_utils::JudgeRecorder,
    };

    use super::*;

    type TestStore =
        GenericStore<u64, Vec<u8>, FsDevice, Fifo<RegionEpItemAdapter<FifoLink>>, FifoLink>;

    type TestStoreConfig =
        GenericStoreConfig<u64, Vec<u8>, FsDevice, Fifo<RegionEpItemAdapter<FifoLink>>>;

    #[tokio::test]
    #[expect(clippy::identity_op)]
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
            name: "".to_string(),
            eviction_config: FifoConfig,
            device_config: FsDeviceConfig {
                dir: PathBuf::from(tempdir.path()),
                capacity: 16 * MB,
                file_capacity: 4 * MB,
                align: 4 * KB,
                io_size: 4 * KB,
            },
            allocator_bits: 1,
            ring_buffer_capacity: 16 * MB,
            catalog_bits: 1,
            admissions,
            reinsertions,
            buffer_pool_size: 8 * MB,
            flushers: 1,
            flush_rate_limit: 0,
            reclaimers: 1,
            reclaim_rate_limit: 0,
            recover_concurrency: 2,
            allocation_timeout: Duration::from_millis(10),
            clean_region_threshold: 1,
        };

        let store = TestStore::open(config).await.unwrap();

        // files:
        // [0, 1, 2]
        // [3, 4, 5]
        // [6, 7, 8]
        // [9, 10, 11]
        // ... ...
        for i in 0..21 {
            store.insert(i, vec![i as u8; 1 * MB]).await.unwrap();
        }

        store.close().await.unwrap();

        let remains = recorder.remains();

        for i in 0..21 {
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
            name: "".to_string(),
            eviction_config: FifoConfig,
            device_config: FsDeviceConfig {
                dir: PathBuf::from(tempdir.path()),
                capacity: 16 * MB,
                file_capacity: 4 * MB,
                align: 4096,
                io_size: 4096 * KB,
            },
            allocator_bits: 1,
            ring_buffer_capacity: 16 * MB,
            catalog_bits: 1,
            admissions: vec![],
            reinsertions: vec![],
            buffer_pool_size: 8 * MB,
            flushers: 1,
            flush_rate_limit: 0,
            reclaimers: 0,
            reclaim_rate_limit: 0,
            recover_concurrency: 2,
            allocation_timeout: Duration::from_millis(10),
            clean_region_threshold: 1,
        };
        let store = TestStore::open(config).await.unwrap();

        for i in 0..21 {
            if remains.contains(&i) {
                assert_eq!(
                    store.lookup(&i).await.unwrap().unwrap(),
                    vec![i as u8; 1 * MB],
                );
            } else {
                assert!(store.lookup(&i).await.unwrap().is_none());
            }
        }

        store.close().await.unwrap();

        drop(store);
    }
}
