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

use std::{fmt::Debug, marker::PhantomData, sync::Arc, time::Instant};

use bytes::{Buf, BufMut};
use foyer_common::{bits, queue::AsyncQueue, rate::RateLimiter};
use foyer_intrusive::{core::adapter::Link, eviction::EvictionPolicy};
use itertools::Itertools;
use parking_lot::Mutex;
use tokio::{sync::broadcast, task::JoinHandle};
use twox_hash::XxHash64;

use crate::{
    admission::AdmissionPolicy,
    device::{BufferAllocator, Device},
    error::{Error, Result},
    flusher::Flusher,
    indices::{Index, Indices},
    metrics::Metrics,
    reclaimer::Reclaimer,
    region::{Region, RegionId},
    region_manager::{RegionEpItemAdapter, RegionManager},
    reinsertion::ReinsertionPolicy,
};
use foyer_common::code::{Key, Value};
use std::hash::Hasher;

const REGION_MAGIC: u64 = 0x19970327;

#[derive(Debug, Default)]
pub struct PrometheusConfig {
    pub registry: Option<prometheus::Registry>,
    pub namespace: Option<String>,
}

pub struct StoreConfig<K, V, D, EP, EL>
where
    K: Key,
    V: Value,
    D: Device,
    EP: EvictionPolicy<RegionEpItemAdapter<EL>, Link = EL>,
    EL: Link,
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
    pub prometheus_config: PrometheusConfig,
}

impl<K, V, D, EP, EL> Debug for StoreConfig<K, V, D, EP, EL>
where
    K: Key,
    V: Value,
    D: Device,
    EP: EvictionPolicy<RegionEpItemAdapter<EL>, Link = EL>,
    EL: Link,
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

pub struct Store<K, V, BA, D, EP, EL>
where
    K: Key,
    V: Value,
    BA: BufferAllocator,
    D: Device<IoBufferAllocator = BA>,
    EP: EvictionPolicy<RegionEpItemAdapter<EL>, Link = EL>,
    EL: Link,
{
    indices: Arc<Indices<K>>,

    region_manager: Arc<RegionManager<BA, D, EP, EL>>,

    device: D,

    admissions: Vec<Arc<dyn AdmissionPolicy<Key = K, Value = V>>>,

    handles: Mutex<Vec<JoinHandle<()>>>,

    stop_tx: broadcast::Sender<()>,

    metrics: Arc<Metrics>,

    _marker: PhantomData<V>,
}

impl<K, V, BA, D, EP, EL> Store<K, V, BA, D, EP, EL>
where
    K: Key,
    V: Value,
    BA: BufferAllocator,
    D: Device<IoBufferAllocator = BA>,
    EP: EvictionPolicy<RegionEpItemAdapter<EL>, Link = EL>,
    EL: Link,
{
    pub async fn open(config: StoreConfig<K, V, D, EP, EL>) -> Result<Arc<Self>> {
        tracing::info!("open store with config:\n{:#?}", config);

        let device = D::open(config.device_config).await?;

        let buffers = Arc::new(AsyncQueue::new());
        for _ in 0..(config.buffer_pool_size / device.region_size()) {
            let len = device.region_size();
            let buffer = device.io_buffer(len, len);
            buffers.release(buffer);
        }

        let clean_regions = Arc::new(AsyncQueue::new());

        let flusher = Arc::new(Flusher::new(config.flushers));
        let reclaimer = Arc::new(Reclaimer::new(config.reclaimers));

        let region_manager = Arc::new(RegionManager::new(
            device.regions(),
            config.eviction_config,
            buffers.clone(),
            clean_regions.clone(),
            device.clone(),
            flusher.clone(),
            reclaimer.clone(),
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
            handles: Mutex::new(vec![]),
            stop_tx,
            metrics: metrics.clone(),
            _marker: PhantomData,
        });

        let flush_rate_limiter = match config.flush_rate_limit {
            0 => None,
            rate => Some(Arc::new(RateLimiter::new(rate as f64))),
        };
        let reclaim_rate_limiter = match config.reclaim_rate_limit {
            0 => None,
            rate => Some(Arc::new(RateLimiter::new(rate as f64))),
        };

        let mut handles = vec![];
        handles.append(
            &mut flusher
                .run(
                    buffers,
                    region_manager.clone(),
                    flush_rate_limiter,
                    flusher_stop_rxs,
                    metrics.clone(),
                )
                .await,
        );
        handles.append(
            &mut reclaimer
                .run(
                    store.clone(),
                    region_manager,
                    clean_regions,
                    config.reinsertions,
                    indices,
                    reclaim_rate_limiter,
                    reclaimer_stop_rxs,
                    metrics,
                )
                .await,
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

    #[tracing::instrument(skip(self))]
    pub async fn insert(&self, key: K, value: V) -> Result<bool> {
        let _timer = self.metrics.latency_insert.start_timer();

        for admission in &self.admissions {
            if !admission.judge(&key, &value) {
                return Ok(false);
            }
        }
        for admission in &self.admissions {
            admission.admit(&key, &value);
        }

        let serialized_len = self.serialized_len(&key, &value);
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

        write_entry(slice.as_mut(), &key, &value);

        let index = Index {
            region: slice.region_id(),
            version: slice.version(),
            offset: slice.offset() as u32,
            len: slice.len() as u32,
            key_len: key.serialized_len() as u32,
            value_len: value.serialized_len() as u32,

            key,
        };

        slice.destroy().await;

        self.indices.insert(index);

        Ok(true)
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
    pub fn remove(&self, key: &K) {
        let _timer = self.metrics.latency_remove.start_timer();

        self.indices.remove(key);
    }

    #[tracing::instrument(skip(self))]
    pub fn clear(&self) {
        let _timer = self.metrics.latency_remove.start_timer();

        self.indices.clear();
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
            let handle = tokio::spawn(async move {
                itx.send(()).await.unwrap();
                let res = Self::recover_region(region_id, region_manager, indices).await;
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
        region_manager: Arc<RegionManager<BA, D, EP, EL>>,
        indices: Arc<Indices<K>>,
    ) -> Result<bool> {
        let region = region_manager.region(&region_id).clone();
        let res = if let Some(mut iter) = RegionEntryIter::<K, V, BA, D>::open(region).await? {
            while let Some(index) = iter.next().await? {
                indices.insert(index);
            }
            region_manager.set_region_evictable(&region_id).await;
            true
        } else {
            region_manager.clean_regions().release(region_id);
            false
        };
        Ok(res)
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

struct RegionEntryIter<K, V, BA, D>
where
    K: Key,
    V: Value,
    BA: BufferAllocator,
    D: Device<IoBufferAllocator = BA>,
{
    region: Region<BA, D>,

    cursor: usize,

    _marker: PhantomData<(K, V)>,
}

impl<K, V, BA, D> RegionEntryIter<K, V, BA, D>
where
    K: Key,
    V: Value,
    BA: BufferAllocator,
    D: Device<IoBufferAllocator = BA>,
{
    async fn open(region: Region<BA, D>) -> Result<Option<Self>> {
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

    async fn next(&mut self) -> Result<Option<Index<K>>> {
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
}

#[cfg(test)]
pub mod tests {
    use std::path::PathBuf;

    use foyer_intrusive::eviction::fifo::{Fifo, FifoConfig, FifoLink};

    use crate::device::{
        allocator::AlignedAllocator,
        fs::{FsDevice, FsDeviceConfig},
    };

    use super::*;

    type TestStore = Store<
        u64,
        Vec<u8>,
        AlignedAllocator,
        FsDevice,
        Fifo<RegionEpItemAdapter<FifoLink>>,
        FifoLink,
    >;

    type TestStoreConfig =
        StoreConfig<u64, Vec<u8>, FsDevice, Fifo<RegionEpItemAdapter<FifoLink>>, FifoLink>;

    #[tokio::test]
    #[allow(clippy::identity_op)]
    async fn test_recovery() {
        const KB: usize = 1024;
        const MB: usize = 1024 * 1024;

        let tempdir = tempfile::tempdir().unwrap();

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
            reclaimers: 1,
            reclaim_rate_limit: 0,
            recover_concurrency: 2,
            prometheus_config: PrometheusConfig::default(),
        };

        let store = TestStore::open(config).await.unwrap();

        // files:
        // [0, 1, 2] (evicted)
        // [3, 4, 5]
        // [6, 7, 8]
        // [9, 10, 11]
        for i in 0..12 {
            store.insert(i, vec![i as u8; 1 * MB]).await.unwrap();
        }

        for i in 0..3 {
            assert!(store.lookup(&i).await.unwrap().is_none());
        }
        for i in 3..12 {
            assert_eq!(
                store.lookup(&i).await.unwrap().unwrap(),
                vec![i as u8; 1 * MB],
            );
        }

        store.shutdown_runners().await.unwrap();
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
            prometheus_config: PrometheusConfig::default(),
        };
        let store = TestStore::open(config).await.unwrap();

        for i in 0..3 {
            assert!(store.lookup(&i).await.unwrap().is_none());
        }
        for i in 3..12 {
            assert_eq!(
                store.lookup(&i).await.unwrap().unwrap(),
                vec![i as u8; 1 * MB],
            );
        }

        store.shutdown_runners().await.unwrap();
        drop(store);
    }
}
