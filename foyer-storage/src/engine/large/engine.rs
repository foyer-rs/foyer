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
use futures_core::future::BoxFuture;
use futures_util::{
    future::{join_all, try_join_all},
    FutureExt,
};
use itertools::Itertools;

use super::{
    flusher::{Flusher, InvalidStats, Submission},
    indexer::Indexer,
    recover::RecoverRunner,
};
#[cfg(test)]
use crate::engine::large::test_utils::*;
use crate::{
    compress::Compression,
    engine::{
        large::{
            eviction::{EvictionPicker, FifoPicker, InvalidRatioPicker},
            reclaimer::{Reclaimer, ReclaimerTrait, RegionCleaner},
            region::{RegionId, RegionManager},
            serde::{AtomicSequence, EntryHeader},
            tombstone::{Tombstone, TombstoneLog},
        },
        Engine, EngineBuildContext, EngineBuilder,
    },
    error::{Error, Result},
    io::{bytes::IoSliceMut, PAGE},
    keeper::PieceRef,
    picker::{utils::RejectAllPicker, ReinsertionPicker},
    runtime::Runtime,
    serde::EntryDeserializer,
    Load,
};

/// Builder for the large object disk cache engine.
pub struct LargeObjectEngineBuilder<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    region_size: usize,
    compression: Compression,
    indexer_shards: usize,
    recover_concurrency: usize,
    flushers: usize,
    reclaimers: usize,
    buffer_pool_size: usize,
    blob_index_size: usize,
    submit_queue_size_threshold: usize,
    clean_region_threshold: usize,
    eviction_pickers: Vec<Box<dyn EvictionPicker>>,
    reinsertion_picker: Arc<dyn ReinsertionPicker>,
    enable_tombstone_log: bool,
    marker: PhantomData<(K, V, P)>,
}

impl<K, V, P> Debug for LargeObjectEngineBuilder<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LargeObjectEngineBuilder")
            .field("region_size", &self.region_size)
            .field("compression", &self.compression)
            .field("indexer_shards", &self.indexer_shards)
            .field("recover_concurrency", &self.recover_concurrency)
            .field("flushers", &self.flushers)
            .field("reclaimers", &self.reclaimers)
            .field("buffer_pool_size", &self.buffer_pool_size)
            .field("blob_index_size", &self.blob_index_size)
            .field("submit_queue_size_threshold", &self.submit_queue_size_threshold)
            .field("clean_region_threshold", &self.clean_region_threshold)
            .field("eviction_pickers", &self.eviction_pickers)
            .field("reinsertion_picker", &self.reinsertion_picker)
            .field("enable_tombstone_log", &self.enable_tombstone_log)
            .finish()
    }
}

impl<K, V, P> Default for LargeObjectEngineBuilder<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V, P> LargeObjectEngineBuilder<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    /// Create a new large object disk cache engine builder with default configurations.
    pub fn new() -> Self {
        Self {
            region_size: 16 * 1024 * 1024, // 16 MiB
            compression: Compression::default(),
            indexer_shards: 64,
            recover_concurrency: 8,
            flushers: 1,
            reclaimers: 1,
            buffer_pool_size: 16 * 1024 * 1024,            // 16 MiB
            blob_index_size: 4 * 1024,                     // 4 KiB
            submit_queue_size_threshold: 16 * 1024 * 1024, // 16 MiB
            clean_region_threshold: 1,
            eviction_pickers: vec![Box::new(InvalidRatioPicker::new(0.8)), Box::<FifoPicker>::default()],
            reinsertion_picker: Arc::<RejectAllPicker>::default(),
            enable_tombstone_log: false,
            marker: PhantomData,
        }
    }

    /// Set the region size for the large object disk cache.
    ///
    /// Region is the minimal cache eviction unit for the large object disk cache,
    /// its size also limits the max cacheable entry size.
    ///
    /// The region size must be 4K-aligned. the given value is not 4K-aligned, it will be automatically aligned up.
    ///
    /// Default: `16 MiB`.
    pub fn with_region_size(mut self, region_size: usize) -> Self {
        self.region_size = bits::align_up(PAGE, region_size);
        self
    }

    /// Set the shard num of the indexer. Each shard has its own lock.
    ///
    /// Default: `64`.
    pub fn with_indexer_shards(mut self, indexer_shards: usize) -> Self {
        self.indexer_shards = indexer_shards;
        self
    }

    /// Set the recover concurrency for the disk cache store.
    ///
    /// Default: `8`.
    pub fn with_recover_concurrency(mut self, recover_concurrency: usize) -> Self {
        self.recover_concurrency = recover_concurrency;
        self
    }

    /// Set the flusher count for the disk cache store.
    ///
    /// The flusher count limits how many regions can be concurrently written.
    ///
    /// Default: `1`.
    pub fn with_flushers(mut self, flushers: usize) -> Self {
        self.flushers = flushers;
        self
    }

    /// Set the reclaimer count for the disk cache store.
    ///
    /// The reclaimer count limits how many regions can be concurrently reclaimed.
    ///
    /// Default: `1`.
    pub fn with_reclaimers(mut self, reclaimers: usize) -> Self {
        self.reclaimers = reclaimers;
        self
    }

    /// Set the total flush buffer pool size.
    ///
    /// Each flusher shares a volume at `threshold / flushers`.
    ///
    /// If the buffer of the flush queue exceeds the threshold, the further entries will be ignored.
    ///
    /// Default: 16 MiB.
    pub fn with_buffer_pool_size(mut self, buffer_pool_size: usize) -> Self {
        self.buffer_pool_size = buffer_pool_size;
        self
    }

    /// Set the blob index size for each blob.
    ///
    /// A larger blob index size can hold more blob entries, but it will also increase the io size of each blob part
    /// write.
    ///
    /// NOTE: The size will be aligned up to a multiplier of 4K.
    ///
    /// Default: 4 KiB
    pub fn with_blob_index_size(mut self, blob_index_size: usize) -> Self {
        let blob_index_size = bits::align_up(PAGE, blob_index_size);
        self.blob_index_size = blob_index_size;
        self
    }

    /// Set the submit queue size threshold.
    ///
    /// If the total entry estimated size in the submit queue exceeds the threshold, the further entries will be
    /// ignored.
    ///
    /// Default: `buffer_pool_size` * 2.
    pub fn with_submit_queue_size_threshold(mut self, submit_queue_size_threshold: usize) -> Self {
        self.submit_queue_size_threshold = submit_queue_size_threshold;
        self
    }

    /// Set the clean region threshold for the disk cache store.
    ///
    /// The reclaimers only work when the clean region count is equal to or lower than the clean region threshold.
    ///
    /// Default: the same value as the `reclaimers`.
    pub fn with_clean_region_threshold(mut self, clean_region_threshold: usize) -> Self {
        self.clean_region_threshold = clean_region_threshold;
        self
    }

    /// Set the eviction pickers for th disk cache store.
    ///
    /// The eviction picker is used to pick the region to reclaim.
    ///
    /// The eviction pickers are applied in order. If the previous eviction picker doesn't pick any region, the next one
    /// will be applied.
    ///
    /// If no eviction picker picks a region, a region will be picked randomly.
    ///
    /// Default: [ invalid ratio picker { threshold = 0.8 }, fifo picker ]
    pub fn with_eviction_pickers(mut self, eviction_pickers: Vec<Box<dyn EvictionPicker>>) -> Self {
        self.eviction_pickers = eviction_pickers;
        self
    }

    /// Set the reinsertion pickers for th disk cache store.
    ///
    /// The reinsertion picker is used to pick the entries that can be reinsertion into the disk cache store while
    /// reclaiming.
    ///
    /// Note: Only extremely important entries should be picked. If too many entries are picked, both insertion and
    /// reinsertion will be stuck.
    ///
    /// Default: [`RejectAllPicker`].
    pub fn with_reinsertion_picker(mut self, reinsertion_picker: Arc<dyn ReinsertionPicker>) -> Self {
        self.reinsertion_picker = reinsertion_picker;
        self
    }

    /// Enable the tombstone log.
    ///
    /// For updatable cache, either the tombstone log or [`crate::engine::RecoverMode::None`] must be enabled to prevent
    /// from the phantom entries after reopen.
    pub fn with_tombstone_log(mut self, enable: bool) -> Self {
        self.enable_tombstone_log = enable;
        self
    }

    /// Build the large object disk cache engine with the given configurations.
    pub async fn build(
        self: Box<Self>,
        EngineBuildContext {
            io_engine,
            metrics,
            statistics,
            runtime,
            recover_mode,
        }: EngineBuildContext,
    ) -> Result<Arc<LargeObjectEngine<K, V, P>>> {
        let device = io_engine.device().clone();
        let region_size = self.region_size;

        let mut tombstones = vec![];

        let tombstone_log = if self.enable_tombstone_log {
            let max_entries = device.capacity() / PAGE;
            let pages = max_entries / TombstoneLog::SLOTS_PER_PAGE
                + if max_entries % TombstoneLog::SLOTS_PER_PAGE > 0 {
                    1
                } else {
                    0
                };
            let partition = device.create_partition(pages * PAGE)?;
            let tombstone_log = TombstoneLog::open(partition, io_engine.clone(), &mut tombstones).await?;
            Some(tombstone_log)
        } else {
            None
        };

        let indexer = Indexer::new(self.indexer_shards);
        let submit_queue_size = Arc::<AtomicUsize>::default();

        #[expect(clippy::type_complexity)]
        let (flushers, rxs): (Vec<Flusher<K, V, P>>, Vec<flume::Receiver<Submission<K, V, P>>>) = (0..self.flushers)
            .map(|id| Flusher::<K, V, P>::new(id, submit_queue_size.clone(), metrics.clone()))
            .unzip();

        let reclaimer = Reclaimer::new(
            indexer.clone(),
            flushers.clone(),
            self.reinsertion_picker,
            self.blob_index_size,
            statistics.clone(),
            runtime.clone(),
        );
        let reclaimer: Arc<dyn ReclaimerTrait> = Arc::new(reclaimer);

        let region_manager = RegionManager::open(
            io_engine,
            region_size,
            self.eviction_pickers,
            reclaimer,
            self.reclaimers,
            self.clean_region_threshold,
            metrics.clone(),
            runtime.clone(),
        )?;
        let regions = region_manager.regions();

        if self.flushers + self.clean_region_threshold > regions / 2 {
            tracing::warn!("[lodc]: large object disk cache stable regions count is too small, flusher [{flushers}] + clean region threshold [{clean_region_threshold}] (default = reclaimers) is supposed to be much larger than the region count [{regions}]",
                    flushers = self.flushers,
                    clean_region_threshold = self.clean_region_threshold,
                );
        }

        let sequence = AtomicSequence::default();

        RecoverRunner::run(
            self.recover_concurrency,
            recover_mode,
            self.blob_index_size,
            &(0..regions as RegionId).collect_vec(),
            &sequence,
            &indexer,
            &region_manager,
            &tombstones,
            runtime.clone(),
            metrics.clone(),
        )
        .await?;

        #[cfg(test)]
        let flush_holder = FlushHolder::default();

        let io_buffer_size = self.buffer_pool_size / self.flushers;
        for (flusher, rx) in flushers.iter().zip(rxs.into_iter()) {
            flusher.run(
                rx,
                region_size,
                io_buffer_size,
                self.blob_index_size,
                self.compression,
                indexer.clone(),
                region_manager.clone(),
                tombstone_log.clone(),
                metrics.clone(),
                &runtime,
                #[cfg(test)]
                flush_holder.clone(),
            )?;
        }

        let inner = LargeObjectEngineInner {
            indexer,
            region_manager,
            flushers,
            submit_queue_size,
            submit_queue_size_threshold: self.submit_queue_size_threshold,
            sequence,
            runtime,
            active: AtomicBool::new(true),
            metrics,
            #[cfg(test)]
            flush_holder,
        };
        let inner = Arc::new(inner);
        let engine = LargeObjectEngine { inner };
        let engine = Arc::new(engine);
        Ok(engine)
    }
}

impl<K, V, P> EngineBuilder<K, V, P> for LargeObjectEngineBuilder<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    fn build(self: Box<Self>, ctx: EngineBuildContext) -> BoxFuture<'static, Result<Arc<dyn Engine<K, V, P>>>> {
        async move { self.build(ctx).await.map(|e| e as Arc<dyn Engine<K, V, P>>) }.boxed()
    }
}

impl<K, V, P> From<LargeObjectEngineBuilder<K, V, P>> for Box<dyn EngineBuilder<K, V, P>>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    fn from(builder: LargeObjectEngineBuilder<K, V, P>) -> Self {
        builder.boxed()
    }
}

/// Large object disk cache engine.
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
    indexer: Indexer,
    region_manager: RegionManager,

    flushers: Vec<Flusher<K, V, P>>,

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
        let flushers = self.inner.flushers.clone();
        let region_manager = self.inner.region_manager.clone();
        async move {
            join_all(flushers.iter().map(|flusher| flusher.wait())).await;
            region_manager.wait_reclaim().await;
        }
    }

    fn close(&self) -> BoxFuture<'static, Result<()>> {
        let this = self.clone();
        async move {
            this.inner.active.store(false, Ordering::Relaxed);
            this.wait().await;
            Ok(())
        }
        .boxed()
    }

    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::storage::engine::large::generic::enqueue")
    )]
    fn enqueue(&self, piece: PieceRef<K, V, P>, estimated_size: usize) {
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

    fn load(&self, hash: u64) -> impl Future<Output = Result<Load<K, V, P>>> + Send + 'static {
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
        let load = load.in_span(Span::enter_with_local_parent(
            "foyer::storage::engine::large::generic::load",
        ));
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
    fn enqueue(&self, piece: PieceRef<K, V, P>, estimated_size: usize) {
        self.enqueue(piece, estimated_size);
    }

    fn load(&self, hash: u64) -> BoxFuture<'static, Result<Load<K, V, P>>> {
        // TODO(MrCroxx): refactor this.
        self.load(hash).boxed()
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
        self.wait().boxed()
    }

    fn close(&self) -> BoxFuture<'static, Result<()>> {
        self.close()
    }
}

#[cfg(test)]
mod tests {

    use std::{fs::File, path::Path};

    use bytesize::ByteSize;
    use foyer_common::hasher::ModHasher;
    use foyer_memory::{Cache, CacheBuilder, CacheEntry, FifoConfig, TestProperties};
    use itertools::Itertools;
    use tokio::runtime::Handle;

    use super::*;
    use crate::{
        engine::RecoverMode,
        io::{
            self,
            device::{fs::FsDeviceBuilder, Device, DeviceBuilder},
            engine::{IoEngine, IoEngineBuilder},
            throttle::IopsCounter,
        },
        picker::utils::RejectAllPicker,
        serde::EntrySerializer,
        test_utils::BiasedPicker,
        CombinedDeviceBuilder, PsyncIoEngineBuilder, Statistics,
    };

    const KB: usize = 1024;

    fn cache_for_test() -> Cache<u64, Vec<u8>, ModHasher, TestProperties> {
        CacheBuilder::new(10)
            .with_shards(1)
            .with_eviction_config(FifoConfig::default())
            .with_hash_builder(ModHasher::default())
            .build()
    }

    fn io_engine_for_test(device: Arc<dyn Device>) -> Arc<dyn IoEngine> {
        // TODO(MrCroxx): Test with other io engines.
        // io::engine::uring::UringIoEngineBuilder::new().build(device).unwrap()
        io::engine::psync::PsyncIoEngineBuilder::new()
            .boxed()
            .build(device, Runtime::current())
            .unwrap()
    }

    /// 4 files, fifo eviction, 16 KiB region, 64 KiB capacity.
    async fn engine_for_test(dir: impl AsRef<Path>) -> Arc<LargeObjectEngine<u64, Vec<u8>, TestProperties>> {
        store_for_test_with_reinsertion_picker(dir, Arc::<RejectAllPicker>::default()).await
    }

    async fn store_for_test_with_reinsertion_picker(
        dir: impl AsRef<Path>,
        reinsertion_picker: Arc<dyn ReinsertionPicker>,
    ) -> Arc<LargeObjectEngine<u64, Vec<u8>, TestProperties>> {
        let device = FsDeviceBuilder::new(dir)
            .with_capacity(ByteSize::kib(64).as_u64() as _)
            .boxed()
            .build()
            .unwrap();
        let io_engine = io_engine_for_test(device);
        let metrics = Arc::new(Metrics::noop());
        let statistics = Arc::new(Statistics::new(IopsCounter::per_io()));
        let runtime = Runtime::new(None, None, Handle::current());
        let builder = LargeObjectEngineBuilder {
            region_size: 16 * 1024,
            compression: Compression::None,
            indexer_shards: 4,
            recover_concurrency: 2,
            flushers: 1,
            reclaimers: 1,
            clean_region_threshold: 1,
            eviction_pickers: vec![Box::<FifoPicker>::default()],
            reinsertion_picker,
            enable_tombstone_log: false,
            buffer_pool_size: 16 * 1024 * 1024,
            blob_index_size: 4 * 1024,
            submit_queue_size_threshold: 16 * 1024 * 1024 * 2,
            marker: PhantomData,
        };

        let builder = Box::new(builder);
        builder
            .build(EngineBuildContext {
                io_engine,
                metrics,
                statistics,
                runtime,
                recover_mode: RecoverMode::Strict,
            })
            .await
            .unwrap()
    }

    async fn store_for_test_with_tombstone_log(
        dir: impl AsRef<Path>,
    ) -> Arc<LargeObjectEngine<u64, Vec<u8>, TestProperties>> {
        let device = FsDeviceBuilder::new(dir)
            .with_capacity(ByteSize::kib(64).as_u64() as usize + ByteSize::kib(4).as_u64() as usize)
            .boxed()
            .build()
            .unwrap();
        let io_engine = io_engine_for_test(device);
        let metrics = Arc::new(Metrics::noop());
        let statistics = Arc::new(Statistics::new(IopsCounter::per_io()));
        let runtime = Runtime::new(None, None, Handle::current());
        let builder = LargeObjectEngineBuilder {
            region_size: 16 * 1024,
            compression: Compression::None,
            indexer_shards: 4,
            recover_concurrency: 2,
            flushers: 1,
            reclaimers: 1,
            clean_region_threshold: 1,
            eviction_pickers: vec![Box::<FifoPicker>::default()],
            reinsertion_picker: Arc::<RejectAllPicker>::default(),
            enable_tombstone_log: true,
            buffer_pool_size: 16 * 1024 * 1024,
            blob_index_size: 4 * 1024,
            submit_queue_size_threshold: 16 * 1024 * 1024 * 2,
            marker: PhantomData,
        };
        let builder = Box::new(builder);
        builder
            .build(EngineBuildContext {
                io_engine,
                metrics,
                statistics,
                runtime,
                recover_mode: RecoverMode::Strict,
            })
            .await
            .unwrap()
    }

    fn enqueue(
        store: &LargeObjectEngine<u64, Vec<u8>, TestProperties>,
        entry: CacheEntry<u64, Vec<u8>, ModHasher, TestProperties>,
    ) {
        let estimated_size = EntrySerializer::estimated_size(entry.key(), entry.value());
        store.enqueue(entry.piece().into(), estimated_size);
    }

    #[test_log::test(tokio::test)]
    async fn test_store_enqueue_lookup_recovery() {
        let dir = tempfile::tempdir().unwrap();

        let memory = cache_for_test();
        let store = engine_for_test(dir.path()).await;

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
        let store = store_for_test_with_tombstone_log(dir.path()).await;

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

        let store = store_for_test_with_tombstone_log(dir.path()).await;
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

        let store = store_for_test_with_tombstone_log(dir.path()).await;

        assert_eq!(
            store.load(memory.hash(&3)).await.unwrap().kv(),
            Some((3, vec![3; 3 * KB]))
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_store_destroy_recovery() {
        let dir = tempfile::tempdir().unwrap();

        let memory = cache_for_test();
        let store = store_for_test_with_tombstone_log(dir.path()).await;

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

        let store = store_for_test_with_tombstone_log(dir.path()).await;
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

        let store = store_for_test_with_tombstone_log(dir.path()).await;

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

    #[test_log::test(tokio::test)]
    async fn test_aggregated_device() {
        let dir = tempfile::tempdir().unwrap();

        const KB: usize = 1024;
        const MB: usize = 1024 * 1024;

        let runtime = Runtime::current();

        let d1 = FsDeviceBuilder::new(dir.path().join("dev1"))
            .with_capacity(MB)
            .boxed()
            .build()
            .unwrap();
        let d2 = FsDeviceBuilder::new(dir.path().join("dev2"))
            .with_capacity(2 * MB)
            .boxed()
            .build()
            .unwrap();
        let d3 = FsDeviceBuilder::new(dir.path().join("dev3"))
            .with_capacity(4 * MB)
            .boxed()
            .build()
            .unwrap();
        let device = CombinedDeviceBuilder::new()
            .with_device(d1)
            .with_device(d2)
            .with_device(d3)
            .boxed()
            .build()
            .unwrap();
        let io_engine = PsyncIoEngineBuilder::new()
            .boxed()
            .build(device, runtime.clone())
            .unwrap();
        let engine = LargeObjectEngineBuilder::<u64, Vec<u8>, TestProperties>::new()
            .with_region_size(64 * KB)
            .boxed()
            .build(EngineBuildContext {
                io_engine,
                metrics: Arc::new(Metrics::noop()),
                statistics: Arc::new(Statistics::new(IopsCounter::per_io())),
                runtime,
                recover_mode: RecoverMode::None,
            })
            .await
            .unwrap();
        assert_eq!(engine.inner.region_manager.regions(), (1 + 2 + 4) * MB / (64 * KB));
    }
}
