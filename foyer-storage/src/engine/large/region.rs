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
    collections::{HashSet, VecDeque},
    fmt::Debug,
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, RwLock, RwLockWriteGuard,
    },
};

use foyer_common::metrics::Metrics;
use futures_core::future::BoxFuture;
use futures_util::{
    future::{ready, Shared},
    FutureExt,
};
use itertools::Itertools;
use rand::seq::IteratorRandom;
use tokio::sync::oneshot;

use crate::{
    engine::large::{
        eviction::{EvictionInfo, EvictionPicker},
        reclaimer::ReclaimerTrait,
    },
    error::Result,
    io::{
        bytes::{IoB, IoBuf, IoBufMut},
        device::Partition,
        engine::IoEngine,
    },
    IoError, Runtime,
};

pub type RegionId = u32;

/// Region statistics.
#[derive(Debug, Default)]
pub struct RegionStatistics {
    /// Estimated invalid bytes in the region.
    /// FIXME(MrCroxx): This value is way too coarse. Need fix.
    pub invalid: AtomicUsize,
    /// Access count of the region.
    pub access: AtomicUsize,
    /// Marked as `true` if the region is about to be evicted by some eviction picker.
    pub probation: AtomicBool,
}

impl RegionStatistics {
    pub(crate) fn reset(&self) {
        self.invalid.store(0, Ordering::Relaxed);
        self.access.store(0, Ordering::Relaxed);
        self.probation.store(false, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct RegionInner {
    id: RegionId,
    partition: Arc<dyn Partition>,
    io_engine: Arc<dyn IoEngine>,
    statistics: Arc<RegionStatistics>,
}

/// A region is a logical partition of a device. It is used to manage the device's storage space.
#[derive(Debug, Clone)]
pub struct Region {
    inner: Arc<RegionInner>,
}

impl Region {
    /// Get region id.
    pub fn id(&self) -> RegionId {
        self.inner.id
    }

    /// Get Region Statistics.
    pub fn statistics(&self) -> &Arc<RegionStatistics> {
        &self.inner.statistics
    }

    /// Get region size.
    pub fn size(&self) -> usize {
        self.inner.partition.size()
    }

    pub(crate) async fn write(&self, buf: Box<dyn IoBuf>, offset: u64) -> (Box<dyn IoB>, Result<()>) {
        let (buf, res) = self
            .inner
            .io_engine
            .write(buf, self.inner.partition.as_ref(), offset)
            .await;
        (buf, res.map_err(|e| e.into()))
    }

    pub(crate) async fn read(&self, buf: Box<dyn IoBufMut>, offset: u64) -> (Box<dyn IoB>, Result<()>) {
        let (buf, res) = self
            .inner
            .io_engine
            .read(buf, self.inner.partition.as_ref(), offset)
            .await;
        (buf, res.map_err(|e| e.into()))
    }
}

#[cfg(test)]
impl Region {
    pub(crate) fn new_for_test(id: RegionId, partition: Arc<dyn Partition>, io_engine: Arc<dyn IoEngine>) -> Self {
        let inner = RegionInner {
            id,
            partition,
            io_engine,
            statistics: Arc::<RegionStatistics>::default(),
        };
        let inner = Arc::new(inner);
        Self { inner }
    }
}

pub type GetCleanRegionHandle = Shared<BoxFuture<'static, Region>>;

#[derive(Debug)]
struct State {
    clean_regions: VecDeque<RegionId>,
    evictable_regions: HashSet<RegionId>,
    writing_regions: HashSet<RegionId>,
    reclaiming_regions: HashSet<RegionId>,

    clean_region_waiters: Vec<oneshot::Sender<Region>>,

    eviction_pickers: Vec<Box<dyn EvictionPicker>>,

    reclaim_waiters: Vec<oneshot::Sender<()>>,
}

#[derive(Debug)]
struct Inner {
    regions: Vec<Region>,
    state: RwLock<State>,
    reclaimer: Arc<dyn ReclaimerTrait>,
    reclaim_concurrency: usize,
    clean_region_threshold: usize,
    metrics: Arc<Metrics>,
    runtime: Runtime,
}

#[derive(Debug, Clone)]
pub struct RegionManager {
    inner: Arc<Inner>,
}

impl RegionManager {
    #[expect(clippy::too_many_arguments)]
    pub fn open(
        io_engine: Arc<dyn IoEngine>,
        region_size: usize,
        mut eviction_pickers: Vec<Box<dyn EvictionPicker>>,
        reclaimer: Arc<dyn ReclaimerTrait>,
        reclaim_concurrency: usize,
        clean_region_threshold: usize,
        metrics: Arc<Metrics>,
        runtime: Runtime,
    ) -> Result<Self> {
        let device = io_engine.device().clone();

        let mut regions = vec![];
        while device.free() >= region_size {
            let partition = match device.create_partition(region_size) {
                Ok(partition) => partition,
                Err(IoError::NoSpace { .. }) => break,
                Err(e) => return Err(e.into()),
            };
            let id = regions.len() as RegionId;
            let region = Region {
                inner: Arc::new(RegionInner {
                    id,
                    partition,
                    io_engine: io_engine.clone(),
                    statistics: Arc::<RegionStatistics>::default(),
                }),
            };
            regions.push(region);
        }

        let rs = regions.iter().map(|r| r.id()).collect_vec();
        for pickers in eviction_pickers.iter_mut() {
            pickers.init(&rs, region_size);
        }

        metrics.storage_lodc_region_size_bytes.absolute(region_size as _);

        let state = State {
            clean_regions: VecDeque::new(),
            evictable_regions: HashSet::new(),
            writing_regions: HashSet::new(),
            reclaiming_regions: HashSet::new(),
            clean_region_waiters: Vec::new(),
            eviction_pickers,
            reclaim_waiters: Vec::new(),
        };
        let inner = Inner {
            regions,
            state: RwLock::new(state),
            reclaimer,
            reclaim_concurrency,
            clean_region_threshold,
            metrics,
            runtime,
        };
        let inner = Arc::new(inner);
        let this = Self { inner };
        Ok(this)
    }

    pub fn init(&self, clean_regions: &[RegionId]) {
        let mut state = self.inner.state.write().unwrap();
        let mut evictable_regions: HashSet<RegionId> = self.inner.regions.iter().map(|r| r.id()).collect();
        state.clean_regions = clean_regions
            .iter()
            .inspect(|id| {
                evictable_regions.remove(id);
            })
            .copied()
            .collect();

        // Temporarily take pickers to make borrow checker happy.
        let mut pickers = std::mem::take(&mut state.eviction_pickers);

        // Notify pickers.
        for region in evictable_regions {
            state.evictable_regions.insert(region);
            for picker in pickers.iter_mut() {
                picker.on_region_evictable(
                    EvictionInfo {
                        regions: &self.inner.regions,
                        evictable: &state.evictable_regions,
                        clean: state.clean_regions.len(),
                    },
                    region,
                );
            }
        }

        // Restore taken pickers after operations.

        std::mem::swap(&mut state.eviction_pickers, &mut pickers);
        assert!(pickers.is_empty());

        let metrics = &self.inner.metrics;
        metrics
            .storage_lodc_region_clean
            .absolute(state.clean_regions.len() as _);
        metrics
            .storage_lodc_region_evictable
            .absolute(state.evictable_regions.len() as _);
        metrics
            .storage_lodc_region_writing
            .absolute(state.writing_regions.len() as _);
        metrics
            .storage_lodc_region_reclaiming
            .absolute(state.reclaiming_regions.len() as _);
    }

    pub fn regions(&self) -> usize {
        self.inner.regions.len()
    }

    pub fn region(&self, id: RegionId) -> &Region {
        &self.inner.regions[id as usize]
    }

    pub fn get_clean_region(&self) -> GetCleanRegionHandle {
        let this = self.clone();
        async move {
            // Wrap state lock guard to make borrow checker happy.
            let rx = {
                let mut state = this.inner.state.write().unwrap();
                if let Some(id) = state.clean_regions.pop_front() {
                    let region = this.inner.regions[id as usize].clone();
                    state.writing_regions.insert(id);
                    this.inner.metrics.storage_lodc_region_clean.decrease(1);
                    this.inner.metrics.storage_lodc_region_writing.increase(1);
                    this.reclaim_if_needed(&mut state);
                    return region;
                } else {
                    let (tx, rx) = oneshot::channel();
                    state.clean_region_waiters.push(tx);
                    drop(state);
                    rx
                }
            };
            rx.await.unwrap()
        }
        .boxed()
        .shared()
    }

    pub fn on_writing_finish(&self, region: Region) {
        let mut state = self.inner.state.write().unwrap();
        state.writing_regions.remove(&region.id());
        self.inner.metrics.storage_lodc_region_writing.decrease(1);
        let inserted = state.evictable_regions.insert(region.id());
        self.inner.metrics.storage_lodc_region_evictable.increase(1);

        assert!(inserted);

        // Temporarily take pickers to make borrow checker happy.
        let mut pickers = std::mem::take(&mut state.eviction_pickers);

        // Notify pickers.
        for picker in pickers.iter_mut() {
            picker.on_region_evictable(
                EvictionInfo {
                    regions: &self.inner.regions,
                    evictable: &state.evictable_regions,
                    clean: state.clean_regions.len(),
                },
                region.id(),
            );
        }

        // Restore taken pickers after operations.

        std::mem::swap(&mut state.eviction_pickers, &mut pickers);
        assert!(pickers.is_empty());

        tracing::debug!(
            id = region.id(),
            "[region manager]: Region state transfers from writing to evictable."
        );

        self.reclaim_if_needed(&mut state);
    }

    fn on_reclaim_finish(&self, region: Region) {
        let mut state = self.inner.state.write().unwrap();
        state.reclaiming_regions.remove(&region.id());
        self.inner.metrics.storage_lodc_region_reclaiming.decrease(1);
        if let Some(waiter) = state.clean_region_waiters.pop() {
            self.inner.metrics.storage_lodc_region_writing.increase(1);
            let _ = waiter.send(region);
        } else {
            self.inner.metrics.storage_lodc_region_clean.increase(1);
            state.clean_regions.push_back(region.id());
        }
        self.reclaim_if_needed(&mut state);
        if state.reclaiming_regions.is_empty() {
            for tx in std::mem::take(&mut state.reclaim_waiters) {
                let _ = tx.send(());
            }
        }
    }

    fn reclaim_if_needed<'a>(&self, state: &mut RwLockWriteGuard<'a, State>) {
        if state.clean_regions.len() < self.inner.clean_region_threshold
            && state.reclaiming_regions.len() < self.inner.reclaim_concurrency
        {
            if let Some(region) = self.evict(state) {
                state.reclaiming_regions.insert(region.id());
                self.inner.metrics.storage_lodc_region_reclaiming.increase(1);
                let region = ReclaimingRegion {
                    region_manager: self.clone(),
                    region,
                };
                let future = self.inner.reclaimer.reclaim(region);
                self.inner.runtime.write().spawn(future);
            }
        }
    }

    fn evict<'a>(&self, state: &mut RwLockWriteGuard<'a, State>) -> Option<Region> {
        let mut picked = None;

        if state.evictable_regions.is_empty() {
            return None;
        }

        // Temporarily take pickers to make borrow checker happy.
        let mut pickers = std::mem::take(&mut state.eviction_pickers);

        // Pick a region to evict with pickers.
        for picker in pickers.iter_mut() {
            if let Some(region) = picker.pick(EvictionInfo {
                regions: &self.inner.regions,
                evictable: &state.evictable_regions,
                clean: state.clean_regions.len(),
            }) {
                picked = Some(region);
                break;
            }
        }

        // If no region is selected, just randomly pick one.
        let picked = picked.unwrap_or_else(|| {
            state
                .evictable_regions
                .iter()
                .choose(&mut rand::rng())
                .copied()
                .unwrap()
        });

        // Update evictable map.
        let removed = state.evictable_regions.remove(&picked);
        self.inner.metrics.storage_lodc_region_evictable.decrease(1);
        assert!(removed);

        // Notify pickers.
        for picker in pickers.iter_mut() {
            picker.on_region_evict(
                EvictionInfo {
                    regions: &self.inner.regions,
                    evictable: &state.evictable_regions,
                    clean: state.clean_regions.len(),
                },
                picked,
            );
        }

        // Restore taken pickers after operations.
        std::mem::swap(&mut state.eviction_pickers, &mut pickers);
        assert!(pickers.is_empty());

        let region = self.inner.regions[picked as usize].clone();
        tracing::debug!("[region manager]: Region {picked} is evicted.");

        Some(region)
    }

    pub fn wait_reclaim(&self) -> BoxFuture<'static, ()> {
        let mut state = self.inner.state.write().unwrap();
        if state.reclaiming_regions.is_empty() {
            return ready(()).boxed();
        }
        let (tx, rx) = oneshot::channel();
        state.reclaim_waiters.push(tx);
        async move {
            let _ = rx.await;
        }
        .boxed()
    }
}

pub struct ReclaimingRegion {
    region_manager: RegionManager,
    region: Region,
}

impl Deref for ReclaimingRegion {
    type Target = Region;

    fn deref(&self) -> &Self::Target {
        &self.region
    }
}

impl DerefMut for ReclaimingRegion {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.region
    }
}

impl Drop for ReclaimingRegion {
    fn drop(&mut self) {
        self.region_manager.on_reclaim_finish(self.region.clone());
    }
}
