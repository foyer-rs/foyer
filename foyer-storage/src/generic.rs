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
    hash::{Hash, Hasher},
    marker::PhantomData,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use anyhow::anyhow;
use bitmaps::Bitmap;
use bytes::{Buf, BufMut};
use foyer_common::{
    bits,
    code::{StorageKey, StorageValue},
};

use foyer_memory::EvictionConfig;
use futures::future::try_join_all;
use itertools::Itertools;
use parking_lot::Mutex;
use tokio::{
    sync::{broadcast, mpsc, Semaphore},
    task::JoinHandle,
};
use twox_hash::XxHash64;

use crate::{
    admission::{AdmissionContext, AdmissionPolicy},
    buffer::BufferError,
    catalog::{Catalog, Index, Item, Sequence},
    compress::Compression,
    device::Device,
    error::Result,
    flusher::{Entry, Flusher},
    judge::Judges,
    metrics::{Metrics, METRICS},
    reclaimer::Reclaimer,
    region::{Region, RegionHeader, RegionId},
    region_manager::RegionManager,
    reinsertion::{ReinsertionContext, ReinsertionPolicy},
    storage::{CachedEntry, Storage, StorageWriter},
};

const DEFAULT_BROADCAST_CAPACITY: usize = 4096;

pub struct GenericStoreConfig<K, V, D>
where
    K: StorageKey,
    V: StorageValue,
    D: Device,
{
    /// For distinguish different foyer metrics.
    ///
    /// Metrics of this foyer instance has label `foyer = {{ name }}`.
    pub name: String,

    /// Eviction policy configurations.
    pub eviction_config: EvictionConfig,

    /// Device configurations.
    pub device_config: D::Config,

    /// Catalog indices sharding count.
    pub catalog_shards: usize,

    /// Admission policies.
    pub admissions: Vec<Arc<dyn AdmissionPolicy<Key = K, Value = V>>>,

    /// Reinsertion policies.
    pub reinsertions: Vec<Arc<dyn ReinsertionPolicy<Key = K, Value = V>>>,

    /// Count of flushers.
    pub flushers: usize,

    /// Count of reclaimers.
    pub reclaimers: usize,

    /// Clean region count threshold to trigger reclamation.
    ///
    /// `clean_region_threshold` is recommended to be equal or larger than `reclaimers`.
    pub clean_region_threshold: usize,

    /// Concurrency of recovery.
    pub recover_concurrency: usize,

    /// Compression algorithm.
    pub compression: Compression,
}

impl<K, V, D> Debug for GenericStoreConfig<K, V, D>
where
    K: StorageKey,
    V: StorageValue,
    D: Device,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreConfig")
            .field("eviction_config", &self.eviction_config)
            .field("device_config", &self.device_config)
            .field("catalog_shards", &self.catalog_shards)
            .field("admissions", &self.admissions.len())
            .field("reinsertions", &self.reinsertions.len())
            .field("flushers", &self.flushers)
            .field("reclaimers", &self.reclaimers)
            .field("clean_region_threshold", &self.clean_region_threshold)
            .field("recover_concurrency", &self.recover_concurrency)
            .field("compression", &self.compression)
            .finish()
    }
}

impl<K, V, D> Clone for GenericStoreConfig<K, V, D>
where
    K: StorageKey,
    V: StorageValue,
    D: Device,
{
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            eviction_config: self.eviction_config.clone(),
            device_config: self.device_config.clone(),
            catalog_shards: self.catalog_shards,
            admissions: self.admissions.clone(),
            reinsertions: self.reinsertions.clone(),
            flushers: self.flushers,
            reclaimers: self.reclaimers,
            clean_region_threshold: self.clean_region_threshold,
            recover_concurrency: self.recover_concurrency,
            compression: self.compression,
        }
    }
}

pub struct GenericStore<K, V, D>
where
    K: StorageKey,
    V: StorageValue,
    D: Device,
{
    inner: Arc<GenericStoreInner<K, V, D>>,
}

impl<K, V, D> Clone for GenericStore<K, V, D>
where
    K: StorageKey,
    V: StorageValue,
    D: Device,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

pub struct GenericStoreInner<K, V, D>
where
    K: StorageKey,
    V: StorageValue,
    D: Device,
{
    sequence: AtomicU64,
    catalog: Arc<Catalog<K, V>>,

    region_manager: Arc<RegionManager<D>>,

    device: D,

    admissions: Vec<Arc<dyn AdmissionPolicy<Key = K, Value = V>>>,
    reinsertions: Vec<Arc<dyn ReinsertionPolicy<Key = K, Value = V>>>,

    flusher_entry_txs: Vec<mpsc::UnboundedSender<Entry<K, V>>>,
    flusher_handles: Mutex<Vec<JoinHandle<()>>>,
    flushers_stop_tx: broadcast::Sender<()>,

    reclaimer_handles: Mutex<Vec<JoinHandle<()>>>,
    reclaimers_stop_tx: broadcast::Sender<()>,

    metrics: Arc<Metrics>,

    compression: Compression,

    _marker: PhantomData<V>,
}

impl<K, V, D> GenericStore<K, V, D>
where
    K: StorageKey,
    V: StorageValue,
    D: Device,
{
    async fn open(config: GenericStoreConfig<K, V, D>) -> Result<Self> {
        let metrics = Arc::new(METRICS.foyer(&config.name));

        let device = D::open(config.device_config).await?;
        assert!(device.regions() >= config.flushers * 2);

        let region_manager = Arc::new(RegionManager::new(
            device.regions(),
            config.eviction_config,
            device.clone(),
        ));

        let catalog = Arc::new(Catalog::new(device.regions(), config.catalog_shards, metrics.clone()));

        let (flushers_stop_tx, _) = broadcast::channel(DEFAULT_BROADCAST_CAPACITY);
        let flusher_stop_rxs = (0..config.flushers).map(|_| flushers_stop_tx.subscribe()).collect_vec();
        // TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
        #[allow(clippy::type_complexity)]
        let (flusher_entry_txs, flusher_entry_rxs): (
            Vec<mpsc::UnboundedSender<Entry<K, V>>>,
            Vec<mpsc::UnboundedReceiver<Entry<K, V>>>,
        ) = (0..config.flushers).map(|_| mpsc::unbounded_channel()).unzip();

        let (reclaimers_stop_tx, _) = broadcast::channel(DEFAULT_BROADCAST_CAPACITY);
        let reclaimer_stop_rxs = (0..config.reclaimers)
            .map(|_| reclaimers_stop_tx.subscribe())
            .collect_vec();

        let inner = GenericStoreInner {
            sequence: AtomicU64::new(0),
            catalog: catalog.clone(),
            region_manager: region_manager.clone(),
            device: device.clone(),
            admissions: config.admissions,
            reinsertions: config.reinsertions,
            flusher_entry_txs,
            flusher_handles: Mutex::new(vec![]),
            reclaimer_handles: Mutex::new(vec![]),
            flushers_stop_tx,
            reclaimers_stop_tx,
            metrics: metrics.clone(),
            compression: config.compression,
            _marker: PhantomData,
        };
        let store = Self { inner: Arc::new(inner) };

        let admission_context = AdmissionContext {
            catalog: catalog.clone(),
            metrics: metrics.clone(),
        };
        let reinsertion_context = ReinsertionContext {
            catalog: catalog.clone(),
            metrics: metrics.clone(),
        };

        for admission in store.inner.admissions.iter() {
            admission.init(admission_context.clone());
        }
        for reinsertion in store.inner.reinsertions.iter() {
            reinsertion.init(reinsertion_context.clone());
        }

        let flushers = flusher_stop_rxs
            .into_iter()
            .zip_eq(flusher_entry_rxs.into_iter())
            .map(|(stop_rx, entry_rx)| {
                Flusher::new(
                    region_manager.clone(),
                    catalog.clone(),
                    device.clone(),
                    entry_rx,
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
    #[tracing::instrument(skip_all)]
    fn writer(&self, key: K) -> GenericStoreWriter<K, V, D> {
        GenericStoreWriter::new(self.clone(), key)
    }

    #[tracing::instrument(skip_all)]
    fn exists<Q>(&self, key: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        Ok(self.inner.catalog.get(key).is_some())
    }

    #[tracing::instrument(skip_all)]
    async fn get<Q>(&self, key: &Q) -> Result<Option<CachedEntry<K, V>>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let now = Instant::now();

        let (_sequence, index) = match self.inner.catalog.get(key) {
            Some(item) => item.consume(),
            None => {
                self.inner
                    .metrics
                    .op_duration_get_miss
                    .observe(now.elapsed().as_secs_f64());
                return Ok(None);
            }
        };

        match index {
            crate::catalog::Index::Inflight { key, value } => {
                let value = value.clone();

                self.inner
                    .metrics
                    .op_duration_get_hit
                    .observe(now.elapsed().as_secs_f64());

                Ok(Some(CachedEntry::Shared { key, value }))
            }
            crate::catalog::Index::Region { view } => {
                let region = view.id();

                self.inner.region_manager.record_access(region);
                let region = self.inner.region_manager.region(region);

                // TODO(MrCroxx): read value only
                let buf = match region.load(view).await? {
                    Some(buf) => buf,
                    None => {
                        // Remove index if the storage layer fails to get it (because of region version mismatch).
                        self.inner.catalog.remove(key);
                        self.inner
                            .metrics
                            .op_duration_get_miss
                            .observe(now.elapsed().as_secs_f64());
                        return Ok(None);
                    }
                };

                let res = match read_entry::<K, V>(buf.as_ref()) {
                    Ok((key, value)) => {
                        self.inner.metrics.op_bytes_get.inc_by(buf.len() as u64);
                        Ok(Some(CachedEntry::Owned {
                            key: Box::new(key),
                            value: Box::new(value),
                        }))
                    }
                    Err(e) => {
                        // Remove index if the storage layer fails to get it (because of entry magic mismatch).
                        self.inner.catalog.remove(key);
                        Err(e)
                    }
                };

                self.inner
                    .metrics
                    .op_duration_get_hit
                    .observe(now.elapsed().as_secs_f64());

                res
            }
        }
    }

    #[tracing::instrument(skip_all)]
    fn remove<Q>(&self, key: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
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

    pub(crate) fn catalog(&self) -> &Arc<Catalog<K, V>> {
        &self.inner.catalog
    }

    pub(crate) fn reinsertions(&self) -> &Vec<Arc<dyn ReinsertionPolicy<Key = K, Value = V>>> {
        &self.inner.reinsertions
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
        region_manager: Arc<RegionManager<D>>,
        catalog: Arc<Catalog<K, V>>,
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

    fn judge_inner(&self, writer: &mut GenericStoreWriter<K, V, D>) {
        for (index, admission) in self.inner.admissions.iter().enumerate() {
            let judge = admission.judge(writer.key.as_ref().unwrap());
            writer.judges.set(index, judge);
        }
        writer.is_judged = true;
    }

    #[tracing::instrument(skip_all)]
    async fn apply_writer(
        &self,
        mut writer: GenericStoreWriter<K, V, D>,
        value: V,
    ) -> Result<Option<CachedEntry<K, V>>> {
        debug_assert!(!writer.is_inserted);

        if !writer.judge() {
            return Ok(None);
        }

        let now = Instant::now();

        let sequence = if let Some(sequence) = writer.sequence {
            sequence
        } else {
            self.inner.sequence.fetch_add(1, Ordering::Relaxed)
        };

        writer.is_inserted = true;
        let key = Arc::new(writer.key.take().unwrap());
        let value = Arc::new(value);

        // TODO(MrCroxx): FIX ME!!!
        // record aligned header + key + value size for metrics
        // let len = bits::align_up(
        //     self.inner.device.align(),
        //     EntryHeader::serialized_len() + key.serialized_len() + value.serialized_len(),
        // );
        // self.inner.metrics.op_bytes_insert.inc_by(len as u64);
        // self.inner.metrics.insert_entry_bytes.observe(len as f64);

        self.inner.catalog.insert(
            key.clone(),
            Item::new(
                sequence,
                Index::Inflight {
                    key: key.clone(),
                    value: value.clone(),
                },
            ),
        );

        let flusher = sequence as usize % self.inner.flusher_entry_txs.len();
        self.inner.flusher_entry_txs[flusher]
            .send(Entry {
                sequence,
                key: key.clone(),
                value: value.clone(),
                compression: writer.compression,
            })
            .unwrap();

        let duration = now.elapsed() + writer.duration;
        self.inner
            .metrics
            .op_duration_insert_inserted
            .observe(duration.as_secs_f64());

        Ok(Some(CachedEntry::Shared { key, value }))
    }
}

pub struct GenericStoreWriter<K, V, D>
where
    K: StorageKey,
    V: StorageValue,
    D: Device,
{
    store: GenericStore<K, V, D>,
    /// `key` is always `Some` before `apply_writer`.
    key: Option<K>,

    sequence: Option<Sequence>,

    judges: Judges,
    is_judged: bool,

    /// judge duration
    duration: Duration,

    is_inserted: bool,
    is_skippable: bool,
    compression: Compression,
}

impl<K, V, D> GenericStoreWriter<K, V, D>
where
    K: StorageKey,
    V: StorageValue,
    D: Device,
{
    fn new(store: GenericStore<K, V, D>, key: K) -> Self {
        let judges = Judges::new(store.inner.admissions.len());
        let compression = store.inner.compression;
        Self {
            store,
            key: Some(key),
            sequence: None,
            judges,
            is_judged: false,
            duration: Duration::from_nanos(0),
            is_inserted: false,
            is_skippable: false,
            compression,
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

    pub async fn finish(self, value: V) -> Result<Option<CachedEntry<K, V>>> {
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

    pub fn compression(&self) -> Compression {
        self.compression
    }

    pub fn set_compression(&mut self, compression: Compression) {
        self.compression = compression
    }
}

impl<K, V, D> Debug for GenericStoreWriter<K, V, D>
where
    K: StorageKey + Debug,
    V: StorageValue,
    D: Device,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreWriter")
            .field("key", &self.key)
            .field("judges", &self.judges)
            .field("is_judged", &self.is_judged)
            .field("duration", &self.duration)
            .field("inserted", &self.is_inserted)
            .finish()
    }
}

impl<K, V, D> Drop for GenericStoreWriter<K, V, D>
where
    K: StorageKey,
    V: StorageValue,
    D: Device,
{
    fn drop(&mut self) {
        if !self.is_inserted {
            debug_assert!(self.key.is_some());

            let filtered = self.is_judged && !self.judge();

            if filtered {
                self.store
                    .inner
                    .metrics
                    .op_duration_insert_filtered
                    .observe(self.duration.as_secs_f64());
            } else {
                self.store
                    .inner
                    .metrics
                    .op_duration_insert_dropped
                    .observe(self.duration.as_secs_f64());
            }
        }
    }
}

const ENTRY_MAGIC: u32 = 0x97_03_27_00;
const ENTRY_MAGIC_MASK: u32 = 0xFF_FF_FF_00;

#[derive(Debug)]
pub struct EntryHeader {
    pub key_len: u32,
    pub value_len: u32,
    pub sequence: Sequence,
    pub checksum: u64,
    pub compression: Compression,
}

impl EntryHeader {
    pub const fn serialized_len() -> usize {
        4 + 4 + 8 + 8 + 4 /* magic & compression */
    }

    pub fn write(&self, mut buf: &mut [u8]) {
        buf.put_u32(self.key_len);
        buf.put_u32(self.value_len);
        buf.put_u64(self.sequence);
        buf.put_u64(self.checksum);

        let v = ENTRY_MAGIC | self.compression.to_u8() as u32;
        buf.put_u32(v);
    }

    pub fn read(mut buf: &[u8]) -> Result<Self> {
        let key_len = buf.get_u32();
        let value_len = buf.get_u32();
        let sequence = buf.get_u64();
        let checksum = buf.get_u64();

        let v = buf.get_u32();
        let magic = v & ENTRY_MAGIC_MASK;
        if magic != ENTRY_MAGIC {
            return Err(anyhow!("magic mismatch, expected: {}, got: {}", ENTRY_MAGIC, magic).into());
        }
        let compression = Compression::try_from(v as u8)?;

        Ok(Self {
            key_len,
            value_len,
            sequence,
            compression,
            checksum,
        })
    }
}

/// | header | value (compressed) | key | <padding> |
///
/// # Safety
///
/// `buf.len()` must exactly fit entry size
fn read_entry<K, V>(buf: &[u8]) -> Result<(K, V)>
where
    K: StorageKey,
    V: StorageValue,
{
    // read entry header
    let header = EntryHeader::read(buf)?;

    // TODO(MrCroxx): optimize buffer copy here.

    // read value
    let mut offset = EntryHeader::serialized_len();
    let compressed = &buf[offset..offset + header.value_len as usize];
    offset += header.value_len as usize;
    let value = match header.compression {
        Compression::None => bincode::deserialize_from(compressed).map_err(BufferError::from)?,
        Compression::Zstd => {
            let decoder = zstd::Decoder::new(compressed).map_err(BufferError::from)?;
            bincode::deserialize_from(decoder).map_err(BufferError::from)?
        }
        Compression::Lz4 => {
            let decoder = lz4::Decoder::new(compressed).map_err(BufferError::from)?;
            bincode::deserialize_from(decoder).map_err(BufferError::from)?
        }
    };

    // read key
    let compressed = &buf[offset..offset + header.key_len as usize];
    let key = bincode::deserialize_from(compressed).map_err(BufferError::from)?;
    offset += header.key_len as usize;

    let checksum = checksum(&buf[EntryHeader::serialized_len()..offset]);
    if checksum != header.checksum {
        return Err(anyhow!("magic mismatch, expected: {}, got: {}", header.checksum, checksum).into());
    }

    Ok((key, value))
}

pub fn checksum(buf: &[u8]) -> u64 {
    let mut hasher = XxHash64::with_seed(0);
    hasher.write(buf);
    hasher.finish()
}

pub struct RegionEntryIter<K, V, D>
where
    K: StorageKey,
    V: StorageValue,
    D: Device,
{
    region: Region<D>,

    cursor: usize,

    _marker: PhantomData<(K, V)>,
}

impl<K, V, D> RegionEntryIter<K, V, D>
where
    K: StorageKey,
    V: StorageValue,
    D: Device,
{
    pub async fn open(region: Region<D>) -> Result<Option<Self>> {
        let align = region.device().align();

        let slice = match region.load_range(0..align).await? {
            Some(slice) => slice,
            None => return Ok(None),
        };

        let Ok(_) = RegionHeader::read(slice.as_ref()) else {
            return Ok(None);
        };

        Ok(Some(Self {
            region,
            cursor: align,
            _marker: PhantomData,
        }))
    }

    pub async fn next(&mut self) -> Result<Option<(K, Item<K, V>)>> {
        let region_size = self.region.device().region_size();
        let align = self.region.device().align();

        if self.cursor + align >= region_size {
            return Ok(None);
        }

        let Some(slice) = self.region.load_range(self.cursor..self.cursor + align).await? else {
            return Ok(None);
        };

        let Ok(header) = EntryHeader::read(slice.as_ref()) else {
            return Ok(None);
        };

        let entry_len = bits::align_up(
            align,
            (header.value_len + header.key_len) as usize + EntryHeader::serialized_len(),
        );

        let abs_start = self.cursor + EntryHeader::serialized_len() + header.value_len as usize;
        let abs_end = self.cursor + EntryHeader::serialized_len() + (header.key_len + header.value_len) as usize;

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

            let Ok(key) = bincode::deserialize_from(&slice.as_ref()[rel_start..rel_end]) else {
                return Ok(None);
            };
            drop(slice);
            key
        } else {
            drop(slice);
            let Some(s) = self.region.load_range(align_start..align_end).await? else {
                return Ok(None);
            };
            let rel_start = abs_start - align_start;
            let rel_end = abs_end - align_start;

            let Ok(key) = bincode::deserialize_from(&s.as_ref()[rel_start..rel_end]) else {
                return Ok(None);
            };
            drop(s);
            key
        };

        let info = Item::new(
            header.sequence,
            Index::Region {
                view: self.region.view(self.cursor as u32, entry_len as u32),
            },
        );

        self.cursor += entry_len;

        Ok(Some((key, info)))
    }

    pub async fn next_kv(&mut self) -> Result<Option<(K, V, usize)>> {
        let (_, item) = match self.next().await {
            Ok(Some(res)) => res,
            Ok(None) => return Ok(None),
            Err(e) => return Err(e),
        };

        let Index::Region { view } = item.index() else {
            unreachable!("kv loaded from region must have index of region")
        };

        // TODO(MrCroxx): Optimize if all key, value and footer are in the same read block.
        let start = *view.offset() as usize;
        let end = start + *view.len() as usize;
        let Some(slice) = self.region.load_range(start..end).await? else {
            return Ok(None);
        };
        let res = read_entry::<K, V>(slice.as_ref())
            .ok()
            .map(|(k, v)| (k, v, slice.len()));
        drop(slice);

        Ok(res)
    }
}

impl<K, V, D> StorageWriter<K, V> for GenericStoreWriter<K, V, D>
where
    K: StorageKey,
    V: StorageValue,
    D: Device,
{
    fn key(&self) -> &K {
        self.key.as_ref().unwrap()
    }

    fn judge(&mut self) -> bool {
        self.judge()
    }

    fn force(&mut self) {
        self.force()
    }

    async fn finish(self, value: V) -> Result<Option<CachedEntry<K, V>>> {
        self.finish(value).await
    }

    fn compression(&self) -> Compression {
        self.compression()
    }

    fn set_compression(&mut self, compression: Compression) {
        self.set_compression(compression)
    }
}

impl<K, V, D> Storage<K, V> for GenericStore<K, V, D>
where
    K: StorageKey,
    V: StorageValue,
    D: Device,
{
    type Config = GenericStoreConfig<K, V, D>;
    type Writer = GenericStoreWriter<K, V, D>;

    async fn open(config: Self::Config) -> Result<Self> {
        Self::open(config).await
    }

    fn is_ready(&self) -> bool {
        true
    }

    async fn close(&self) -> Result<()> {
        self.close().await
    }

    fn writer(&self, key: K) -> Self::Writer {
        self.writer(key)
    }

    fn exists<Q>(&self, key: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.exists(key)
    }

    async fn get<Q>(&self, key: &Q) -> Result<Option<CachedEntry<K, V>>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.get(key).await
    }

    fn remove<Q>(&self, key: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.remove(key)
    }

    fn clear(&self) -> Result<()> {
        self.clear()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use foyer_memory::FifoConfig;

    use super::*;
    use crate::{
        device::fs::{FsDevice, FsDeviceConfig},
        storage::StorageExt,
        test_utils::JudgeRecorder,
    };

    type TestStore = GenericStore<u64, Vec<u8>, FsDevice>;
    type TestStoreConfig = GenericStoreConfig<u64, Vec<u8>, FsDevice>;

    #[tokio::test]
    // TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
    #[allow(clippy::identity_op)]
    async fn test_recovery() {
        const KB: usize = 1024;
        const MB: usize = 1024 * 1024;

        let tempdir = tempfile::tempdir().unwrap();

        let recorder = Arc::new(JudgeRecorder::default());
        let admissions: Vec<Arc<dyn AdmissionPolicy<Key = u64, Value = Vec<u8>>>> = vec![recorder.clone()];
        let reinsertions: Vec<Arc<dyn ReinsertionPolicy<Key = u64, Value = Vec<u8>>>> = vec![recorder.clone()];

        let config = TestStoreConfig {
            name: "".to_string(),
            eviction_config: FifoConfig {}.into(),
            device_config: FsDeviceConfig {
                dir: PathBuf::from(tempdir.path()),
                capacity: 16 * MB,
                file_size: 4 * MB,
                align: 4 * KB,
                io_size: 4 * KB,
            },
            catalog_shards: 1,
            admissions,
            reinsertions,
            flushers: 1,
            reclaimers: 1,
            recover_concurrency: 2,
            clean_region_threshold: 1,
            compression: Compression::None,
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
                assert_eq!(store.get(&i).await.unwrap().unwrap().value(), &vec![i as u8; 1 * MB],);
            } else {
                assert!(store.get(&i).await.unwrap().is_none());
            }
        }

        drop(store);

        let config = TestStoreConfig {
            name: "".to_string(),
            eviction_config: FifoConfig {}.into(),
            device_config: FsDeviceConfig {
                dir: PathBuf::from(tempdir.path()),
                capacity: 16 * MB,
                file_size: 4 * MB,
                align: 4096,
                io_size: 4096 * KB,
            },
            catalog_shards: 1,
            admissions: vec![],
            reinsertions: vec![],
            flushers: 1,
            reclaimers: 0,
            recover_concurrency: 2,
            clean_region_threshold: 1,
            compression: Compression::None,
        };
        let store = TestStore::open(config).await.unwrap();

        for i in 0..21 {
            if remains.contains(&i) {
                assert_eq!(store.get(&i).await.unwrap().unwrap().value(), &vec![i as u8; 1 * MB],);
            } else {
                assert!(store.get(&i).await.unwrap().is_none());
            }
        }

        store.close().await.unwrap();

        drop(store);
    }
}
