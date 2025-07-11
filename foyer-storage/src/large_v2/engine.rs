// Copyright 2025 foyer Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    fmt::Debug,
    future::Future,
    marker::PhantomData,
    ops::Range,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};

#[cfg(feature = "tracing")]
use fastrace::prelude::*;
use foyer_common::{
    bits,
    code::{StorageKey, StorageValue},
    metrics::Metrics,
    properties::{Age, Populated, Properties, Source},
};
use foyer_memory::Piece;
use futures_core::future::BoxFuture;
use futures_util::{
    future::{join_all, try_join_all},
    FutureExt,
};
use tokio::sync::Semaphore;

use super::{
    flusher::{Flusher, InvalidStats, Submission},
    indexer::Indexer,
    reclaimer::Reclaimer,
    recover::{RecoverMode, RecoverRunner},
};
#[cfg(test)]
use crate::large_v2::test_utils::*;
use crate::{
    compress::Compression,
    engine::{Engine, EngineBuilder},
    error::{Error, Result},
    io::{bytes::IoSliceMut, device::RegionId, engine::IoEngine, PAGE},
    large_v2::{
        reclaimer::RegionCleaner,
        serde::{AtomicSequence, EntryHeader},
        tombstone::{Tombstone, TombstoneLog, TombstoneLogConfig},
    },
    picker_v2::{EvictionPicker, ReinsertionPicker},
    region_v2::RegionManager,
    runtime::Runtime,
    serde::EntryDeserializer,
    statistics::Statistics,
    Load,
};

pub struct GenericLargeStorageConfig<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    pub io_engine: Arc<dyn IoEngine>,
    pub regions: Range<RegionId>,
    pub compression: Compression,
    pub indexer_shards: usize,
    pub recover_mode: RecoverMode,
    pub recover_concurrency: usize,
    pub flushers: usize,
    pub reclaimers: usize,
    pub buffer_pool_size: usize,
    pub blob_index_size: usize,
    pub submit_queue_size_threshold: usize,
    pub clean_region_threshold: usize,
    pub eviction_pickers: Vec<Box<dyn EvictionPicker>>,
    pub reinsertion_picker: Arc<dyn ReinsertionPicker>,
    pub tombstone_log_config: Option<TombstoneLogConfig>,
    pub runtime: Runtime,
    pub marker: PhantomData<(K, V)>,
}

impl<K, V> Debug for GenericLargeStorageConfig<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenericStoreConfig")
            .field("engine", &self.io_engine)
            .field("compression", &self.compression)
            .field("indexer_shards", &self.indexer_shards)
            .field("recover_mode", &self.recover_mode)
            .field("recover_concurrency", &self.recover_concurrency)
            .field("flushers", &self.flushers)
            .field("reclaimers", &self.reclaimers)
            .field("buffer_pool_size", &self.buffer_pool_size)
            .field("blob_index_size", &self.blob_index_size)
            .field("submit_queue_size_threshold", &self.submit_queue_size_threshold)
            .field("clean_region_threshold", &self.clean_region_threshold)
            .field("eviction_pickers", &self.eviction_pickers)
            .field("reinsertion_pickers", &self.reinsertion_picker)
            .field("tombstone_log_config", &self.tombstone_log_config)
            .field("runtime", &self.runtime)
            .finish()
    }
}

pub struct LargeObjectEngineBuilder<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    pub io_engine: Arc<dyn IoEngine>,
    pub regions: Range<RegionId>,
    pub compression: Compression,
    pub indexer_shards: usize,
    pub recover_mode: RecoverMode,
    pub recover_concurrency: usize,
    pub flushers: usize,
    pub reclaimers: usize,
    pub buffer_pool_size: usize,
    pub blob_index_size: usize,
    pub submit_queue_size_threshold: usize,
    pub clean_region_threshold: usize,
    pub eviction_pickers: Vec<Box<dyn EvictionPicker>>,
    pub reinsertion_picker: Arc<dyn ReinsertionPicker>,
    pub tombstone_log_config: Option<TombstoneLogConfig>,
    pub runtime: Runtime,
    pub metrics: Arc<Metrics>,
    pub statistics: Arc<Statistics>,
    pub marker: PhantomData<(K, V, P)>,
}

impl<K, V, P> Debug for LargeObjectEngineBuilder<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenericStoreConfig")
            .field("engine", &self.io_engine)
            .field("compression", &self.compression)
            .field("indexer_shards", &self.indexer_shards)
            .field("recover_mode", &self.recover_mode)
            .field("recover_concurrency", &self.recover_concurrency)
            .field("flushers", &self.flushers)
            .field("reclaimers", &self.reclaimers)
            .field("buffer_pool_size", &self.buffer_pool_size)
            .field("blob_index_size", &self.blob_index_size)
            .field("submit_queue_size_threshold", &self.submit_queue_size_threshold)
            .field("clean_region_threshold", &self.clean_region_threshold)
            .field("eviction_pickers", &self.eviction_pickers)
            .field("reinsertion_pickers", &self.reinsertion_picker)
            .field("tombstone_log_config", &self.tombstone_log_config)
            .field("metrics", &self.metrics)
            .field("statistics", &self.statistics)
            .field("runtime", &self.runtime)
            .finish()
    }
}

impl<K, V, P> EngineBuilder<K, V, P> for LargeObjectEngineBuilder<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    fn build(self) -> BoxFuture<'static, Result<Arc<dyn Engine<K, V, P>>>> {
        async move {
            let io_engine = self.io_engine.clone();
            let device = self.io_engine.device().clone();
            if self.flushers + self.clean_region_threshold > device.regions() / 2 {
                tracing::warn!("[lodc]: large object disk cache stable regions count is too small, flusher [{flushers}] + clean region threshold [{clean_region_threshold}] (default = reclaimers) is supposed to be much larger than the region count [{regions}]",
                    flushers = self.flushers,
                    clean_region_threshold = self.clean_region_threshold,
                    regions = device.regions()
                );
            }

            let mut tombstones = vec![];
            let tombstone_log = match &self.tombstone_log_config {
                None => None,
                Some(tombstone_log_config) => {
                    let log = TombstoneLog::open(
                        tombstone_log_config,
                        device.clone(),
                        &mut tombstones,
                        self.metrics.clone(),
                        self.runtime.clone(),
                    )
                    .await?;
                    Some(log)
                }
            };

            let indexer = Indexer::new(self.indexer_shards);
            let mut  eviction_pickers = self.eviction_pickers;
            for picker in eviction_pickers.iter_mut() {
                picker.init(0..device.regions() as RegionId, device.region_size());
            }
            let reclaim_semaphore = Arc::new(Semaphore::new(0));
            let region_manager = RegionManager::new(
                io_engine.clone(),
                eviction_pickers,
                reclaim_semaphore.clone(),
                self.metrics.clone(),
            );
            let sequence = AtomicSequence::default();
            let submit_queue_size = Arc::<AtomicUsize>::default();

            RecoverRunner::run(
                self.recover_concurrency,
                self.recover_mode,
                self.blob_index_size,
                self.clean_region_threshold,
                self.regions.clone(),
                &sequence,
                &indexer,
                &region_manager,
                &tombstones,
                self.runtime.clone(),
                self.metrics.clone(),
            )
            .await?;

            #[cfg(test)]
            let flush_holder = FlushHolder::default();

            let flushers = try_join_all((0..self.flushers).map(|id| {
                let indexer = indexer.clone();
                let region_manager = region_manager.clone();
                let submit_queue_size = submit_queue_size.clone();
                let tombstone_log = tombstone_log.clone();
                let metrics = self.metrics.clone();
                let io_buffer_size = self.buffer_pool_size / self.flushers;
                let blob_index_size = self.blob_index_size;
                let compression = self.compression;
                let io_engine = io_engine.clone();
                let runtime = self.runtime.clone();
                #[cfg(test)]
                let flush_holder = flush_holder.clone();
                async move {
                    Flusher::<K, V, P>::open(
                        id,
                        io_buffer_size,
                        blob_index_size,
                        compression,
                        indexer,
                        region_manager,
                        io_engine,
                        submit_queue_size,
                        tombstone_log,
                        metrics,
                        &runtime,
                        #[cfg(test)]
                        flush_holder,
                    )
                }
            }))
            .await?;

            let reclaimers = join_all((0..self.reclaimers).map(|_| async {
                Reclaimer::open(
                    io_engine.clone(),
                    region_manager.clone(),
                    reclaim_semaphore.clone(),
                    indexer.clone(),
                    flushers.clone(),
                    self.reinsertion_picker.clone(),
                    self.blob_index_size,
                    self.statistics.clone(),
                    self.metrics.clone(),
                    self.runtime.clone(),
                )
            }))
            .await;

            let inner = LargeObjectEngineInner {
                io_engine,
                indexer,
                region_manager,
                flushers,
                reclaimers,
                submit_queue_size,
                submit_queue_size_threshold: self.submit_queue_size_threshold,
                sequence,
                runtime: self.runtime,
                active: AtomicBool::new(true),
                 metrics: self.metrics ,
                 #[cfg(test)]
                 flush_holder
            };
            let inner = Arc::new(inner);
            let engine = LargeObjectEngine{inner};
            let engine = Arc::new(engine);
            let engine :Arc<dyn Engine<K, V, P>> = engine;
            Ok(engine)
        }.boxed()
    }
}

pub struct LargeObjectEngine<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    inner: Arc<LargeObjectEngineInner<K, V, P>>,
}

impl<K, V, P> Debug for LargeObjectEngine<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenericStore").finish()
    }
}

struct LargeObjectEngineInner<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    io_engine: Arc<dyn IoEngine>,
    indexer: Indexer,
    region_manager: RegionManager,

    flushers: Vec<Flusher<K, V, P>>,
    reclaimers: Vec<Reclaimer>,

    submit_queue_size: Arc<AtomicUsize>,
    submit_queue_size_threshold: usize,

    sequence: AtomicSequence,

    runtime: Runtime,

    active: AtomicBool,

    metrics: Arc<Metrics>,

    #[cfg(test)]
    flush_holder: FlushHolder,
}

impl<K, V, P> Clone for LargeObjectEngine<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<K, V, P> LargeObjectEngine<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    fn wait(&self) -> impl Future<Output = ()> + Send + 'static {
        let wait_flushers = join_all(self.inner.flushers.iter().map(|flusher| flusher.wait()));
        let wait_reclaimers = join_all(self.inner.reclaimers.iter().map(|reclaimer| reclaimer.wait()));
        async move {
            wait_flushers.await;
            wait_reclaimers.await;
        }
    }

    async fn close(&self) -> Result<()> {
        self.inner.active.store(false, Ordering::Relaxed);
        self.wait().await;
        Ok(())
    }

    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::storage::large_v2::generic::enqueue")
    )]
    fn enqueue(&self, piece: Piece<K, V, P>, estimated_size: usize) {
        if !self.inner.active.load(Ordering::Relaxed) {
            tracing::warn!("cannot enqueue new entry after closed");
            return;
        }

        tracing::trace!(
            hash = piece.hash(),
            source = ?piece.properties().source().unwrap_or_default(),
            "[lodc]: enqueue"
        );
        match piece.properties().source().unwrap_or_default() {
            Source::Populated(Populated { age }) => match age {
                Age::Young => {
                    // skip write lodc if the entry is still young
                    self.inner.metrics.storage_lodc_enqueue_skip.increase(1);
                    return;
                }
                Age::Old => {}
            },
            Source::Outer => {}
        }

        if self.inner.submit_queue_size.load(Ordering::Relaxed) > self.inner.submit_queue_size_threshold {
            self.inner.metrics.storage_queue_channel_overflow.increase(1);
            return;
        }

        let sequence = self.inner.sequence.fetch_add(1, Ordering::Relaxed);

        self.inner.flushers[piece.hash() as usize % self.inner.flushers.len()].submit(Submission::CacheEntry {
            piece,
            estimated_size,
            sequence,
        });
    }

    fn load(&self, hash: u64) -> impl Future<Output = Result<Load<K, V>>> + Send + 'static {
        tracing::trace!(hash, "[lodc]: load");

        let indexer = self.inner.indexer.clone();
        let metrics = self.inner.metrics.clone();
        let region_manager = self.inner.region_manager.clone();

        let load = async move {
            let addr = match indexer.get(hash) {
                Some(addr) => addr,
                None => {
                    return Ok(Load::Miss);
                }
            };

            tracing::trace!(hash, ?addr, "[lodc]: load");

            let region = region_manager.region(addr.region);
            let buf = IoSliceMut::new(bits::align_up(PAGE, addr.len as _));
            let (buf, res) = region.read(Box::new(buf), addr.offset as _).await;
            match res {
                Ok(_) => {}
                Err(e @ Error::InvalidIoRange { .. }) => {
                    tracing::warn!(?e, "[lodc load]: invalid io range, remove this entry and skip");
                    indexer.remove(hash);
                    return Ok(Load::Miss);
                }
                Err(e) => {
                    tracing::error!(hash, ?addr, ?e, "[lodc load]: load error");
                    return Err(e);
                }
            }

            let header = match EntryHeader::read(&buf[..EntryHeader::serialized_len()]) {
                Ok(header) => header,
                Err(e @ Error::MagicMismatch { .. })
                | Err(e @ Error::ChecksumMismatch { .. })
                | Err(e @ Error::CompressionAlgorithmNotSupported(_))
                | Err(e @ Error::OutOfRange { .. })
                | Err(e @ Error::InvalidIoRange { .. }) => {
                    tracing::warn!(
                        hash,
                        ?addr,
                        ?e,
                        "[lodc load]: deserialize read buffer raise error, remove this entry and skip"
                    );
                    indexer.remove(hash);
                    return Ok(Load::Miss);
                }
                Err(e) => {
                    tracing::error!(hash, ?addr, ?e, "[lodc load]: load error");
                    return Err(e);
                }
            };

            let (key, value) = {
                let now = Instant::now();
                let res = match EntryDeserializer::deserialize::<K, V>(
                    &buf[EntryHeader::serialized_len()..],
                    header.key_len as _,
                    header.value_len as _,
                    header.compression,
                    Some(header.checksum),
                ) {
                    Ok(res) => res,
                    Err(e @ Error::MagicMismatch { .. })
                    | Err(e @ Error::ChecksumMismatch { .. })
                    | Err(e @ Error::OutOfRange { .. })
                    | Err(e @ Error::InvalidIoRange { .. }) => {
                        tracing::warn!(
                            hash,
                            ?addr,
                            ?header,
                            ?e,
                            "[lodc load]: deserialize read buffer raise error, remove this entry and skip"
                        );
                        indexer.remove(hash);
                        return Ok(Load::Miss);
                    }
                    Err(e) => {
                        tracing::error!(hash, ?addr, ?header, ?e, "[lodc load]: load error");
                        return Err(e);
                    }
                };
                metrics
                    .storage_entry_deserialize_duration
                    .record(now.elapsed().as_secs_f64());
                res
            };

            let age = match region.statistics().probation.load(Ordering::Relaxed) {
                true => Age::Old,
                false => Age::Young,
            };

            Ok(Load::Entry {
                key,
                value,
                populated: Populated { age },
            })
        };
        #[cfg(feature = "tracing")]
        let load = load.in_span(Span::enter_with_local_parent("foyer::storage::large_v2::generic::load"));
        load
    }

    fn delete(&self, hash: u64) {
        if !self.inner.active.load(Ordering::Relaxed) {
            tracing::warn!("cannot delete entry after closed");
            return;
        }

        let sequence = self.inner.sequence.fetch_add(1, Ordering::Relaxed);
        let stats = self
            .inner
            .indexer
            .insert_tombstone(hash, sequence)
            .map(|addr| InvalidStats {
                region: addr.region,
                size: bits::align_up(PAGE, addr.len as usize),
            });

        let this = self.clone();
        self.inner.runtime.write().spawn(async move {
            this.inner.flushers[hash as usize % this.inner.flushers.len()].submit(Submission::Tombstone {
                tombstone: Tombstone { hash, sequence },
                stats,
            });
        });
    }

    fn may_contains(&self, hash: u64) -> bool {
        self.inner.indexer.get(hash).is_some()
    }

    fn destroy(&self) -> BoxFuture<'static, Result<()>> {
        let this = self.clone();
        async move {
            if !this.inner.active.load(Ordering::Relaxed) {
                return Err(anyhow::anyhow!("cannot delete entry after closed").into());
            }

            // Write an tombstone to clear tombstone log by increase the max sequence.
            let sequence = this.inner.sequence.fetch_add(1, Ordering::Relaxed);

            this.inner.flushers[0].submit(Submission::Tombstone {
                tombstone: Tombstone { hash: 0, sequence },
                stats: None,
            });
            this.wait().await;

            // Clear indices.
            //
            // This step must perform after the latest writer finished,
            // otherwise the indices of the latest batch cannot be cleared.
            this.inner.indexer.clear();

            // Clean regions.
            try_join_all((0..this.inner.region_manager.regions() as RegionId).map(|id| {
                let region = this.inner.region_manager.region(id).clone();
                async move {
                    let res = RegionCleaner::clean(&region).await;
                    region.statistics().reset();
                    res
                }
            }))
            .await?;

            Ok(())
        }
        .boxed()
    }

    #[cfg(test)]
    pub fn hold_flush(&self) {
        self.inner.flush_holder.hold();
    }

    #[cfg(test)]
    pub fn unhold_flush(&self) {
        self.inner.flush_holder.unhold();
    }
}

impl<K, V, P> Engine<K, V, P> for LargeObjectEngine<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    fn enqueue(&self, piece: Piece<K, V, P>, estimated_size: usize) {
        self.enqueue(piece, estimated_size);
    }

    fn load(&self, hash: u64) -> BoxFuture<'static, Result<Load<K, V>>> {
        // TODO(MrCroxx): refactor this.
        let future = self.load(hash);
        async move { future.await }.boxed()
    }

    fn delete(&self, hash: u64) {
        self.delete(hash);
    }

    fn may_contains(&self, hash: u64) -> bool {
        self.may_contains(hash)
    }

    fn destroy(&self) -> BoxFuture<'static, Result<()>> {
        self.destroy()
    }

    fn wait(&self) -> BoxFuture<'static, ()> {
        // TODO(MrCroxx): refactor this.
        let future = self.wait();
        async move { future.await }.boxed()
    }
}

#[cfg(test)]
mod tests {

    use std::{any::Any, fs::File, path::Path};

    use bytesize::ByteSize;
    use foyer_common::hasher::ModHasher;
    use foyer_memory::{Cache, CacheBuilder, CacheEntry, FifoConfig, TestProperties};
    use itertools::Itertools;
    use tokio::runtime::Handle;

    use super::*;
    use crate::{
        io::{
            device::{fs::FsDeviceBuilder, Device, DeviceBuilder},
            engine::{psync::PsyncIoEngineBuilder, IoEngineBuilder},
        },
        large_v2::tombstone::TombstoneLogConfigBuilder,
        picker_v2::utils::{FifoPicker, RejectAllPicker},
        serde::EntrySerializer,
        test_utils_v2::BiasedPicker,
        IopsCounter,
    };

    const KB: usize = 1024;

    fn cache_for_test() -> Cache<u64, Vec<u8>, ModHasher, TestProperties> {
        CacheBuilder::new(10)
            .with_shards(1)
            .with_eviction_config(FifoConfig::default())
            .with_hash_builder(ModHasher::default())
            .build()
    }

    fn device_for_test(dir: impl AsRef<Path>) -> Arc<dyn Device> {
        FsDeviceBuilder::new(dir)
            .with_capacity(ByteSize::kib(64).as_u64() as _)
            .with_file_size(ByteSize::kib(16).as_u64() as _)
            .build()
            .unwrap()
    }

    fn io_engine_for_test(device: Arc<dyn Device>) -> Arc<dyn IoEngine> {
        PsyncIoEngineBuilder::new(device).build().unwrap()
    }

    /// 4 files, fifo eviction, 16 KiB region, 64 KiB capacity.
    async fn engine_for_test(dir: impl AsRef<Path>) -> Arc<dyn Engine<u64, Vec<u8>, TestProperties>> {
        store_for_test_with_reinsertion_picker(dir, Arc::<RejectAllPicker>::default()).await
    }

    async fn store_for_test_with_reinsertion_picker(
        dir: impl AsRef<Path>,
        reinsertion_picker: Arc<dyn ReinsertionPicker>,
    ) -> Arc<dyn Engine<u64, Vec<u8>, TestProperties>> {
        let device = device_for_test(dir);
        let regions = 0..device.regions() as RegionId;
        let io_engine = io_engine_for_test(device);
        let metrics = Arc::new(Metrics::noop());
        let statistics = Arc::new(Statistics::new(IopsCounter::per_io()));
        let builder = LargeObjectEngineBuilder {
            io_engine,
            regions,
            compression: Compression::None,
            indexer_shards: 4,
            recover_mode: RecoverMode::Strict,
            recover_concurrency: 2,
            flushers: 1,
            reclaimers: 1,
            clean_region_threshold: 1,
            eviction_pickers: vec![Box::<FifoPicker>::default()],
            reinsertion_picker,
            tombstone_log_config: None,
            buffer_pool_size: 16 * 1024 * 1024,
            blob_index_size: 4 * 1024,
            submit_queue_size_threshold: 16 * 1024 * 1024 * 2,
            runtime: Runtime::new(None, None, Handle::current()),
            metrics,
            statistics,
            marker: PhantomData,
        };
        builder.build().await.unwrap()
    }

    async fn store_for_test_with_tombstone_log(
        dir: impl AsRef<Path>,
        path: impl AsRef<Path>,
    ) -> Arc<dyn Engine<u64, Vec<u8>, TestProperties>> {
        let device = device_for_test(dir);
        let regions = 0..device.regions() as RegionId;
        let io_engine = io_engine_for_test(device);
        let metrics = Arc::new(Metrics::noop());
        let statistics = Arc::new(Statistics::new(IopsCounter::per_io()));
        let builder = LargeObjectEngineBuilder {
            io_engine,
            regions,
            compression: Compression::None,
            indexer_shards: 4,
            recover_mode: RecoverMode::Strict,
            recover_concurrency: 2,
            flushers: 1,
            reclaimers: 1,
            clean_region_threshold: 1,
            eviction_pickers: vec![Box::<FifoPicker>::default()],
            reinsertion_picker: Arc::<RejectAllPicker>::default(),
            tombstone_log_config: Some(TombstoneLogConfigBuilder::new(path).with_flush(true).build()),
            buffer_pool_size: 16 * 1024 * 1024,
            blob_index_size: 4 * 1024,
            submit_queue_size_threshold: 16 * 1024 * 1024 * 2,
            runtime: Runtime::new(None, None, Handle::current()),
            metrics,
            statistics,
            marker: PhantomData,
        };
        builder.build().await.unwrap()
    }

    fn enqueue(
        store: &LargeObjectEngine<u64, Vec<u8>, TestProperties>,
        entry: CacheEntry<u64, Vec<u8>, ModHasher, TestProperties>,
    ) {
        let estimated_size = EntrySerializer::estimated_size(entry.key(), entry.value());
        store.enqueue(entry.piece(), estimated_size);
    }

    fn concrete(engine: Arc<dyn Any + Send + Sync>) -> Arc<LargeObjectEngine<u64, Vec<u8>, TestProperties>> {
        engine
            .downcast::<LargeObjectEngine<u64, Vec<u8>, TestProperties>>()
            .unwrap()
    }

    #[test_log::test(tokio::test)]
    async fn test_store_enqueue_lookup_recovery() {
        let dir = tempfile::tempdir().unwrap();

        let memory = cache_for_test();
        let store = engine_for_test(dir.path()).await;
        let store = concrete(store);

        // [ [e1, e2], [], [], [] ]
        store.hold_flush();
        let e1 = memory.insert(1, vec![1; 7 * KB]);
        let e2 = memory.insert(2, vec![2; 3 * KB]);
        enqueue(&store, e1.clone());
        enqueue(&store, e2);
        store.unhold_flush();
        store.wait().await;

        let r1 = store.load(memory.hash(&1)).await.unwrap().kv().unwrap();
        assert_eq!(r1, (1, vec![1; 7 * KB]));
        let r2 = store.load(memory.hash(&2)).await.unwrap().kv().unwrap();
        assert_eq!(r2, (2, vec![2; 3 * KB]));

        // [ [e1, e2], [e3, e4], [], [] ]
        store.hold_flush();
        let e3 = memory.insert(3, vec![3; 7 * KB]);
        let e4 = memory.insert(4, vec![4; 2 * KB]);
        enqueue(&store, e3);
        enqueue(&store, e4);
        store.unhold_flush();
        store.wait().await;

        let r1 = store.load(memory.hash(&1)).await.unwrap().kv().unwrap();
        assert_eq!(r1, (1, vec![1; 7 * KB]));
        let r2 = store.load(memory.hash(&2)).await.unwrap().kv().unwrap();
        assert_eq!(r2, (2, vec![2; 3 * KB]));
        let r3 = store.load(memory.hash(&3)).await.unwrap().kv().unwrap();
        assert_eq!(r3, (3, vec![3; 7 * KB]));
        let r4 = store.load(memory.hash(&4)).await.unwrap().kv().unwrap();
        assert_eq!(r4, (4, vec![4; 2 * KB]));

        // [ [e1, e2], [e3, e4], [e5], [] ]
        let e5 = memory.insert(5, vec![5; 11 * KB]);
        enqueue(&store, e5);
        store.wait().await;

        let r1 = store.load(memory.hash(&1)).await.unwrap().kv().unwrap();
        assert_eq!(r1, (1, vec![1; 7 * KB]));
        let r2 = store.load(memory.hash(&2)).await.unwrap().kv().unwrap();
        assert_eq!(r2, (2, vec![2; 3 * KB]));
        let r3 = store.load(memory.hash(&3)).await.unwrap().kv().unwrap();
        assert_eq!(r3, (3, vec![3; 7 * KB]));
        let r4 = store.load(memory.hash(&4)).await.unwrap().kv().unwrap();
        assert_eq!(r4, (4, vec![4; 2 * KB]));
        let r5 = store.load(memory.hash(&5)).await.unwrap().kv().unwrap();
        assert_eq!(r5, (5, vec![5; 11 * KB]));

        // [ [], [e3, e4], [e5], [e6, e4*] ]
        store.hold_flush();
        let e6 = memory.insert(6, vec![6; 7 * KB]);
        let e4v2 = memory.insert(4, vec![!4; 3 * KB]);
        enqueue(&store, e6);
        enqueue(&store, e4v2);
        store.unhold_flush();
        store.wait().await;

        assert!(store.load(memory.hash(&1)).await.unwrap().kv().is_none());
        assert!(store.load(memory.hash(&2)).await.unwrap().kv().is_none());
        let r3 = store.load(memory.hash(&3)).await.unwrap().kv().unwrap();
        assert_eq!(r3, (3, vec![3; 7 * KB]));
        let r4v2 = store.load(memory.hash(&4)).await.unwrap().kv().unwrap();
        assert_eq!(r4v2, (4, vec![!4; 3 * KB]));
        let r5 = store.load(memory.hash(&5)).await.unwrap().kv().unwrap();
        assert_eq!(r5, (5, vec![5; 11 * KB]));
        let r6 = store.load(memory.hash(&6)).await.unwrap().kv().unwrap();
        assert_eq!(r6, (6, vec![6; 7 * KB]));

        store.close().await.unwrap();
        enqueue(&store, e1);
        store.wait().await;

        drop(store);

        let store = engine_for_test(dir.path()).await;

        assert!(store.load(memory.hash(&1)).await.unwrap().kv().is_none());
        assert!(store.load(memory.hash(&2)).await.unwrap().kv().is_none());
        let r3 = store.load(memory.hash(&3)).await.unwrap().kv().unwrap();
        assert_eq!(r3, (3, vec![3; 7 * KB]));
        let r4v2 = store.load(memory.hash(&4)).await.unwrap().kv().unwrap();
        assert_eq!(r4v2, (4, vec![!4; 3 * KB]));
        let r5 = store.load(memory.hash(&5)).await.unwrap().kv().unwrap();
        assert_eq!(r5, (5, vec![5; 11 * KB]));
        let r6 = store.load(memory.hash(&6)).await.unwrap().kv().unwrap();
        assert_eq!(r6, (6, vec![6; 7 * KB]));
    }

    #[test_log::test(tokio::test)]
    async fn test_store_delete_recovery() {
        let dir = tempfile::tempdir().unwrap();

        let memory = cache_for_test();
        let store = store_for_test_with_tombstone_log(dir.path(), dir.path().join("test-tombstone-log")).await;
        let store = concrete(store);

        let es = (0..10).map(|i| memory.insert(i, vec![i as u8; 3 * KB])).collect_vec();

        // [[0, 1, 2], [3, 4, 5], [6, 7, 8], []]
        for e in es.iter().take(9) {
            enqueue(&store, e.clone());
        }
        store.wait().await;

        for i in 0..9 {
            assert_eq!(
                store.load(memory.hash(&i)).await.unwrap().kv(),
                Some((i, vec![i as u8; 3 * KB]))
            );
        }

        store.delete(memory.hash(&3));
        store.wait().await;
        assert_eq!(store.load(memory.hash(&3)).await.unwrap().kv(), None);

        store.close().await.unwrap();
        drop(store);

        let store = store_for_test_with_tombstone_log(dir.path(), dir.path().join("test-tombstone-log")).await;
        let store = concrete(store);
        for i in 0..9 {
            if i != 3 {
                assert_eq!(
                    store.load(memory.hash(&i)).await.unwrap().kv(),
                    Some((i, vec![i as u8; 3 * KB]))
                );
            } else {
                assert_eq!(store.load(memory.hash(&3)).await.unwrap().kv(), None);
            }
        }

        enqueue(&store, es[3].clone());
        store.wait().await;
        assert_eq!(
            store.load(memory.hash(&3)).await.unwrap().kv(),
            Some((3, vec![3; 3 * KB]))
        );

        store.close().await.unwrap();
        drop(store);

        let store = store_for_test_with_tombstone_log(dir.path(), dir.path().join("test-tombstone-log")).await;

        assert_eq!(
            store.load(memory.hash(&3)).await.unwrap().kv(),
            Some((3, vec![3; 3 * KB]))
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_store_destroy_recovery() {
        let dir = tempfile::tempdir().unwrap();

        let memory = cache_for_test();
        let store = store_for_test_with_tombstone_log(dir.path(), dir.path().join("test-tombstone-log")).await;
        let store = concrete(store);

        let es = (0..10).map(|i| memory.insert(i, vec![i as u8; 3 * KB])).collect_vec();

        // [[0, 1, 2], [3, 4, 5], [6, 7, 8], []]
        store.hold_flush();
        for e in es.iter().take(9) {
            enqueue(&store, e.clone());
        }
        store.unhold_flush();
        store.wait().await;

        for i in 0..9 {
            assert_eq!(
                store.load(memory.hash(&i)).await.unwrap().kv(),
                Some((i, vec![i as u8; 3 * KB]))
            );
        }

        store.delete(memory.hash(&3));
        store.wait().await;
        assert_eq!(store.load(memory.hash(&3)).await.unwrap().kv(), None);

        store.destroy().await.unwrap();

        store.close().await.unwrap();
        drop(store);

        let store = store_for_test_with_tombstone_log(dir.path(), dir.path().join("test-tombstone-log")).await;
        let store = concrete(store);
        for i in 0..9 {
            assert_eq!(store.load(memory.hash(&i)).await.unwrap().kv(), None);
        }

        enqueue(&store, es[3].clone());
        store.wait().await;
        assert_eq!(
            store.load(memory.hash(&3)).await.unwrap().kv(),
            Some((3, vec![3; 3 * KB]))
        );

        store.close().await.unwrap();
        drop(store);

        let store = store_for_test_with_tombstone_log(dir.path(), dir.path().join("test-tombstone-log")).await;

        assert_eq!(
            store.load(memory.hash(&3)).await.unwrap().kv(),
            Some((3, vec![3; 3 * KB]))
        );
    }

    // FIXME(MrCroxx): Move the admission test to store level.
    // #[test_log::test(tokio::test)]
    // async fn test_store_admission() {
    //     let dir = tempfile::tempdir().unwrap();

    //     let memory = cache_for_test();
    //     let store = store_for_test_with_admission_picker(&memory, dir.path(),
    // Arc::new(BiasedPicker::new([1]))).await;

    //     let e1 = memory.insert(1, vec![1; 7 * KB]);
    //     let e2 = memory.insert(2, vec![2; 7 * KB]);

    //     assert!(enqueue(&store, e1.clone(),).await.unwrap());
    //     assert!(!enqueue(&store, e2,).await.unwrap());

    //     let r1 = store.load(&1).await.unwrap().unwrap();
    //     assert_eq!(r1, (1, vec![1; 7 * KB]));
    //     assert!(store.load(&2).await.unwrap().is_none());
    // }

    #[test_log::test(tokio::test)]
    async fn test_store_reinsertion() {
        let dir = tempfile::tempdir().unwrap();

        let memory = cache_for_test();
        let store = store_for_test_with_reinsertion_picker(
            dir.path(),
            Arc::new(BiasedPicker::new(vec![1, 3, 5, 7, 9, 11, 13, 15, 17, 19])),
        )
        .await;
        let store = concrete(store);

        let es = (0..15).map(|i| memory.insert(i, vec![i as u8; 3 * KB])).collect_vec();

        // [[(0), (1), (2)], [(3), (4), (5)], [(6), (7), (8)], []]
        for e in es.iter().take(9).cloned() {
            enqueue(&store, e);
            store.wait().await;
        }

        for i in 0..9 {
            let r = store.load(memory.hash(&i)).await.unwrap().kv().unwrap();
            assert_eq!(r, (i, vec![i as u8; 3 * KB]));
        }

        // [[], [(3), (4), (5)], [(6), (7), (8)], [(9), (10), (1)]]
        enqueue(&store, es[9].clone());
        enqueue(&store, es[10].clone());
        store.wait().await;
        for reclaimer in store.inner.reclaimers.iter() {
            reclaimer.wait().await;
        }
        let mut res = vec![];
        for i in 0..11 {
            res.push(store.load(memory.hash(&i)).await.unwrap().kv());
        }
        assert_eq!(
            res,
            vec![
                None,
                Some((1, vec![1; 3 * KB])),
                None,
                Some((3, vec![3; 3 * KB])),
                Some((4, vec![4; 3 * KB])),
                Some((5, vec![5; 3 * KB])),
                Some((6, vec![6; 3 * KB])),
                Some((7, vec![7; 3 * KB])),
                Some((8, vec![8; 3 * KB])),
                Some((9, vec![9; 3 * KB])),
                Some((10, vec![10; 3 * KB])),
            ]
        );

        // [[(11), (3), (5)], [], [(6), (7), (8)], [(9), (10), (1)]]
        enqueue(&store, es[11].clone());
        store.wait().await;
        for reclaimer in store.inner.reclaimers.iter() {
            reclaimer.wait().await;
        }
        let mut res = vec![];
        for i in 0..12 {
            tracing::trace!("==========> {i}");
            res.push(store.load(memory.hash(&i)).await.unwrap().kv());
        }
        assert_eq!(
            res,
            vec![
                None,
                Some((1, vec![1; 3 * KB])),
                None,
                Some((3, vec![3; 3 * KB])),
                None,
                Some((5, vec![5; 3 * KB])),
                Some((6, vec![6; 3 * KB])),
                Some((7, vec![7; 3 * KB])),
                Some((8, vec![8; 3 * KB])),
                Some((9, vec![9; 3 * KB])),
                Some((10, vec![10; 3 * KB])),
                Some((11, vec![11; 3 * KB])),
            ]
        );

        // [[(11), (3), (5)], [(12), (13), (14)], [], [(9), (10), (1)]]
        store.delete(memory.hash(&7));
        store.wait().await;
        enqueue(&store, es[12].clone());
        store.wait().await;
        enqueue(&store, es[13].clone());
        store.wait().await;
        enqueue(&store, es[14].clone());
        store.wait().await;
        for reclaimer in store.inner.reclaimers.iter() {
            reclaimer.wait().await;
        }
        let mut res = vec![];
        for i in 0..15 {
            res.push(store.load(memory.hash(&i)).await.unwrap().kv());
        }
        assert_eq!(
            res,
            vec![
                None,
                Some((1, vec![1; 3 * KB])),
                None,
                Some((3, vec![3; 3 * KB])),
                None,
                Some((5, vec![5; 3 * KB])),
                None,
                None,
                None,
                Some((9, vec![9; 3 * KB])),
                Some((10, vec![10; 3 * KB])),
                Some((11, vec![11; 3 * KB])),
                Some((12, vec![12; 3 * KB])),
                Some((13, vec![13; 3 * KB])),
                Some((14, vec![14; 3 * KB])),
            ]
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_store_magic_checksum_mismatch() {
        let dir = tempfile::tempdir().unwrap();

        let memory = cache_for_test();
        let store = engine_for_test(dir.path()).await;
        let store = concrete(store);

        // write entry 1
        let e1 = memory.insert(1, vec![1; 7 * KB]);
        enqueue(&store, e1);
        store.wait().await;

        // check entry 1
        let r1 = store.load(memory.hash(&1)).await.unwrap().kv().unwrap();
        assert_eq!(r1, (1, vec![1; 7 * KB]));

        // corrupt entry and header
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let entry = entry.unwrap();
            if !entry.metadata().unwrap().is_file() {
                continue;
            }

            let file = File::options().write(true).open(entry.path()).unwrap();
            #[cfg(target_family = "unix")]
            {
                use std::os::unix::fs::FileExt;
                file.write_all_at(&[b'x'; 4 * 1024], 4 * 1024).unwrap();
            }
            #[cfg(target_family = "windows")]
            {
                use std::os::windows::fs::FileExt;
                file.seek_write(&[b'x'; 4 * 1024], 4 * 1024).unwrap();
            }
        }

        assert!(store.load(memory.hash(&1)).await.unwrap().kv().is_none());
    }
}
