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
    future::ready,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};

use flume::{Receiver, Sender};
use foyer_common::{
    code::{StorageKey, StorageValue},
    countdown::Countdown,
    metrics::Metrics,
    properties::Properties,
};
use futures_core::future::BoxFuture;
use futures_util::{future::Shared, FutureExt};
use itertools::Itertools;
use parking_lot::{Mutex, MutexGuard};
use rand::seq::IteratorRandom;
use tokio::sync::{oneshot, Semaphore};

use crate::{
    device::{Dev, DevExt, MonitoredDevice, RegionId},
    error::Result,
    io::buffer::{IoBuf, IoBufMut},
    large::{
        flusher::Flusher,
        indexer::Indexer,
        reclaimer_v2::{ReclaimerV2, ReclaimerV2Ctx},
    },
    picker::EvictionPicker,
    EvictionInfo, ReinsertionPicker, Runtime,
};

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
    device: MonitoredDevice,
    statistics: Arc<RegionStatistics>,
}

/// A region is a logical partition of a device. It is used to manage the device's storage space.
#[derive(Debug, Clone)]
pub struct Region {
    id: RegionId,
    inner: Arc<RegionInner>,
}

impl Region {
    /// Get region id.
    pub fn id(&self) -> RegionId {
        self.id
    }

    /// Get Region Statistics.
    pub fn statistics(&self) -> &Arc<RegionStatistics> {
        &self.inner.statistics
    }

    /// Get region size.
    pub fn size(&self) -> usize {
        self.inner.device.region_size()
    }

    pub(crate) async fn write<B>(&self, buf: B, offset: u64) -> (B, Result<()>)
    where
        B: IoBuf,
    {
        self.inner.device.write(buf, self.id, offset).await
    }

    pub(crate) async fn read<B>(&self, buf: B, offset: u64) -> (B, Result<()>)
    where
        B: IoBufMut,
    {
        self.inner.statistics.access.fetch_add(1, Ordering::Relaxed);
        self.inner.device.read(buf, self.id, offset).await
    }

    #[expect(unused)]
    pub(crate) async fn sync(&self) -> Result<()> {
        self.inner.device.sync(Some(self.id)).await
    }
}

#[cfg(test)]
impl Region {
    pub(crate) fn new_for_test(id: RegionId, device: MonitoredDevice) -> Self {
        let inner = RegionInner {
            device,
            statistics: Arc::<RegionStatistics>::default(),
        };
        let inner = Arc::new(inner);
        Self { id, inner }
    }
}

// #[derive(Clone)]
// pub struct RegionManager {
//     inner: Arc<RegionManagerInner>,
// }

// struct Eviction {
//     evictable: HashSet<RegionId>,
//     eviction_pickers: Vec<Box<dyn EvictionPicker>>,
// }

// struct RegionManagerInner {
//     regions: Vec<Region>,

//     eviction: Mutex<Eviction>,

//     clean_region_tx: Sender<Region>,
//     clean_region_rx: Receiver<Region>,

//     reclaim_semaphore: Arc<Semaphore>,
//     reclaim_semaphore_countdown: Arc<Countdown>,

//     metrics: Arc<Metrics>,
// }

// impl Debug for RegionManager {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         f.debug_struct("RegionManager").finish()
//     }
// }

// impl RegionManager {
//     pub fn new(
//         device: MonitoredDevice,
//         eviction_pickers: Vec<Box<dyn EvictionPicker>>,
//         reclaim_semaphore: Arc<Semaphore>,
//         metrics: Arc<Metrics>,
//     ) -> Self {
//         let regions = (0..device.regions() as RegionId)
//             .map(|id| Region {
//                 id,
//                 inner: Arc::new(RegionInner {
//                     device: device.clone(),
//                     statistics: Arc::<RegionStatistics>::default(),
//                 }),
//             })
//             .collect_vec();
//         let (clean_region_tx, clean_region_rx) = flume::unbounded();

//         metrics.storage_region_total.absolute(device.regions() as _);
//         metrics.storage_region_size_bytes.absolute(device.region_size() as _);

//         Self {
//             inner: Arc::new(RegionManagerInner {
//                 regions,
//                 eviction: Mutex::new(Eviction {
//                     evictable: HashSet::new(),
//                     eviction_pickers,
//                 }),
//                 clean_region_tx,
//                 clean_region_rx,
//                 reclaim_semaphore,
//                 reclaim_semaphore_countdown: Arc::new(Countdown::new(0)),
//                 metrics,
//             }),
//         }
//     }

//     pub fn mark_evictable(&self, rid: RegionId) {
//         let mut eviction = self.inner.eviction.lock();

//         // Update evictable map.
//         let inserted = eviction.evictable.insert(rid);
//         assert!(inserted);

//         // Temporarily take pickers to make borrow checker happy.
//         let mut pickers = std::mem::take(&mut eviction.eviction_pickers);

//         // Notify pickers.
//         for picker in pickers.iter_mut() {
//             picker.on_region_evictable(
//                 EvictionInfo {
//                     regions: &self.inner.regions,
//                     evictable: &eviction.evictable,
//                     clean: self.inner.clean_region_tx.len(),
//                 },
//                 rid,
//             );
//         }

//         // Restore taken pickers after operations.

//         std::mem::swap(&mut eviction.eviction_pickers, &mut pickers);
//         assert!(pickers.is_empty());

//         self.inner.metrics.storage_region_evictable.increase(1);

//         tracing::debug!("[region manager]: Region {rid} is marked evictable.");
//     }

//     pub fn evict(&self) -> Option<Region> {
//         let mut picked = None;

//         let mut eviction = self.inner.eviction.lock();

//         if eviction.evictable.is_empty() {
//             // No evictable region now.
//             return None;
//         }

//         // Temporarily take pickers to make borrow checker happy.
//         let mut pickers = std::mem::take(&mut eviction.eviction_pickers);

//         // Pick a region to evict with pickers.
//         for picker in pickers.iter_mut() {
//             if let Some(region) = picker.pick(EvictionInfo {
//                 regions: &self.inner.regions,
//                 evictable: &eviction.evictable,
//                 clean: self.inner.clean_region_tx.len(),
//             }) {
//                 picked = Some(region);
//                 break;
//             }
//         }

//         // If no region is selected, just randomly pick one.
//         let picked = picked.unwrap_or_else(|| eviction.evictable.iter().choose(&mut rand::rng()).copied().unwrap());

//         // Update evictable map.
//         let removed = eviction.evictable.remove(&picked);
//         assert!(removed);
//         self.inner.metrics.storage_region_evictable.decrease(1);

//         // Notify pickers.
//         for picker in pickers.iter_mut() {
//             picker.on_region_evict(
//                 EvictionInfo {
//                     regions: &self.inner.regions,
//                     evictable: &eviction.evictable,
//                     clean: self.inner.clean_region_tx.len(),
//                 },
//                 picked,
//             );
//         }

//         // Restore taken pickers after operations.
//         std::mem::swap(&mut eviction.eviction_pickers, &mut pickers);
//         assert!(pickers.is_empty());

//         let region = self.region(picked).clone();
//         tracing::debug!("[region manager]: Region {picked} is evicted.");

//         Some(region)
//     }

//     pub async fn mark_clean(&self, region: RegionId) {
//         self.inner
//             .clean_region_tx
//             .send_async(self.region(region).clone())
//             .await
//             .unwrap();
//         self.inner.metrics.storage_region_clean.increase(1);
//     }

//     pub fn get_clean_region(&self) -> GetCleanRegionHandle {
//         let clean_region_rx = self.inner.clean_region_rx.clone();
//         let reclaim_semaphore = self.inner.reclaim_semaphore.clone();
//         let reclaim_semaphore_countdown = self.inner.reclaim_semaphore_countdown.clone();
//         let metrics = self.inner.metrics.clone();

//         async move {
//             let region = clean_region_rx.recv_async().await.unwrap();
//             // The only place to increase the permit.
//             //
//             // See comments in `ReclaimRunner::handle()` 1and `RecoverRunner::run()`.
//             if reclaim_semaphore_countdown.countdown() {
//                 reclaim_semaphore.add_permits(1);
//             }
//             metrics.storage_region_clean.decrease(1);
//             region
//         }
//         .boxed()
//         .shared()
//     }

//     pub fn regions(&self) -> usize {
//         self.inner.regions.len()
//     }

//     #[expect(dead_code)]
//     pub fn evictable_regions(&self) -> usize {
//         self.inner.eviction.lock().evictable.len()
//     }

//     #[expect(dead_code)]
//     pub fn clean_regions(&self) -> usize {
//         self.inner.clean_region_rx.len()
//     }

//     pub fn region(&self, id: RegionId) -> &Region {
//         &self.inner.regions[id as usize]
//     }

//     pub fn reclaim_semaphore(&self) -> &Arc<Semaphore> {
//         &self.inner.reclaim_semaphore
//     }

//     pub fn reclaim_semaphore_countdown(&self) -> &Countdown {
//         &self.inner.reclaim_semaphore_countdown
//     }
// }

// pub type GetCleanRegionHandle = Shared<BoxFuture<'static, Region>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegionState {
    Clean,
    Writing,
    Evictable,
    Reclaiming,
}

#[derive(Debug, Default)]
struct RegionStateCounter {
    clean: usize,
    writing: usize,
    evictable: usize,
    reclaiming: usize,
}

#[derive(Debug)]
struct Mutable {
    counter: RegionStateCounter,

    evictable: HashSet<RegionId>,
    eviction_pickers: Vec<Box<dyn EvictionPicker>>,

    clean_regions: VecDeque<RegionId>,
    clean_region_waiters: VecDeque<oneshot::Sender<Region>>,
}

#[derive(Debug)]
struct Inner<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    mutable: Mutex<Mutable>,

    regions: Vec<Region>,
    clean_region_threshold: usize,

    metrics: Arc<Metrics>,

    reclaimer: ReclaimerV2<K, V, P>,
}

#[derive(Debug)]
pub struct RegionManager<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    inner: Arc<Inner<K, V, P>>,
}

impl<K, V, P> Clone for RegionManager<K, V, P>
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

impl<K, V, P> RegionManager<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    pub fn new(
        device: MonitoredDevice,
        eviction_pickers: Vec<Box<dyn EvictionPicker>>,
        clean_region_threshold: usize,
        reclaimer: ReclaimerV2<K, V, P>,
        metrics: Arc<Metrics>,
    ) -> Self {
        let regions = (0..device.regions() as RegionId)
            .map(|id| Region {
                id,
                inner: Arc::new(RegionInner {
                    device: device.clone(),
                    statistics: Arc::<RegionStatistics>::default(),
                }),
            })
            .collect_vec();

        let mutable = Mutable {
            counter: RegionStateCounter::default(),
            evictable: HashSet::new(),
            eviction_pickers,
            clean_regions: VecDeque::new(),
            clean_region_waiters: VecDeque::new(),
        };
        let mutable = Mutex::new(mutable);
        let inner = Inner {
            mutable,
            regions,
            clean_region_threshold,
            metrics,
            reclaimer,
        };
        let inner = Arc::new(inner);
        Self { inner }
    }

    pub fn region(&self, id: &RegionId) -> &Region {
        &self.inner.regions[*id as usize]
    }

    pub fn regions(&self) -> usize {
        self.inner.regions.len()
    }

    pub fn init(&self, cleans: Vec<RegionId>, evictables: Vec<RegionId>) {
        let mut mutable = self.inner.mutable.lock();

        mutable.counter.clean = cleans.len();
        mutable.clean_regions.extend(cleans);

        mutable.counter.evictable = evictables.len();
        mutable.evictable.extend(evictables);

        self.inner
            .metrics
            .storage_region_clean
            .absolute(mutable.counter.clean as _);
        self.inner
            .metrics
            .storage_region_evictable
            .absolute(mutable.counter.evictable as _);

        tracing::debug!(
            "[region manager]: Initialized with {} clean regions and {} evictable regions.",
            mutable.counter.clean,
            mutable.counter.evictable
        );
    }

    pub fn get_clean_region(&self) -> GetCleanRegionHandle {
        let mut mutable = self.inner.mutable.lock();
        if let Some(id) = mutable.clean_regions.pop_front() {
            let region = self.region(&id).clone();
            mutable.counter.clean -= 1;
            mutable.counter.writing += 1;
            self.try_start_reclaim(&mut mutable);
            ready(region).boxed().shared()
        } else {
            let (tx, rx) = oneshot::channel();
            mutable.clean_region_waiters.push_back(tx);
            async move { rx.await.unwrap() }.boxed().shared()
        }
    }

    fn evict<'a>(&self, mutable: &mut MutexGuard<'a, Mutable>) -> Option<Region> {
        let mut picked = None;

        if mutable.evictable.is_empty() {
            // No evictable region now.
            return None;
        }

        // Temporarily take pickers to make borrow checker happy.
        let mut pickers = std::mem::take(&mut mutable.eviction_pickers);

        // Pick a region to evict with pickers.
        for picker in pickers.iter_mut() {
            if let Some(region) = picker.pick(EvictionInfo {
                regions: &self.inner.regions,
                evictable: &mutable.evictable,
                clean: mutable.counter.clean,
            }) {
                picked = Some(region);
                break;
            }
        }

        // If no region is selected, just randomly pick one.
        let picked = picked.unwrap_or_else(|| mutable.evictable.iter().choose(&mut rand::rng()).copied().unwrap());

        // Update evictable map.
        let removed = mutable.evictable.remove(&picked);
        assert!(removed);
        self.inner.metrics.storage_region_evictable.decrease(1);

        // Notify pickers.
        for picker in pickers.iter_mut() {
            picker.on_region_evict(
                EvictionInfo {
                    regions: &self.inner.regions,
                    evictable: &mutable.evictable,
                    clean: mutable.counter.clean,
                },
                picked,
            );
        }

        // Restore taken pickers after operations.
        std::mem::swap(&mut mutable.eviction_pickers, &mut pickers);
        assert!(pickers.is_empty());

        let region = self.region(&picked).clone();
        tracing::debug!("[region manager]: Region {picked} is evicted.");

        Some(region)
    }

    fn try_start_reclaim<'a>(&self, mutable: &mut MutexGuard<'a, Mutable>) {
        if self.need_reclaim(&mutable) {
            self.start_reclaim(mutable);
        }
    }

    fn start_reclaim<'a>(&self, mutable: &mut MutexGuard<'a, Mutable>) {
        let region = match self.evict(mutable) {
            Some(region) => region,
            None => {
                tracing::warn!("[region manager]: No evictable region to reclaim.");
                return;
            }
        };

        mutable.counter.reclaiming += 1;
        mutable.counter.evictable -= 1;

        let notifier = RegionReclaimNotifier {
            region_manager: self.clone(),
            id: region.id(),
        };
        self.inner.reclaimer.spawn_reclaim(region, notifier);
    }

    fn finish_reclaim(&self, region: Region) {
        let mut mutable = self.inner.mutable.lock();

        mutable.counter.reclaiming -= 1;
        mutable.counter.clean += 1;

        if let Some(tx) = mutable.clean_region_waiters.pop_front() {
            let _ = tx.send(region);
        } else {
            mutable.clean_regions.push_back(region.id());
        }

        self.try_start_reclaim(&mut mutable);
    }

    fn need_reclaim<'a>(&self, mutable: &MutexGuard<'a, Mutable>) -> bool {
        // have enough reclaimers
        mutable.counter.reclaiming < self.inner.reclaimer.concurrency()
        // not enough clean regions
            && mutable.counter.clean + mutable.counter.reclaiming < self.inner.clean_region_threshold
    }

    pub fn mark_evictable(&self, id: RegionId) {
        let mut mutable = self.inner.mutable.lock();

        // Update evictable map.
        let inserted = mutable.evictable.insert(id);
        assert!(inserted);

        // Temporarily take pickers to make borrow checker happy.
        let mut pickers = std::mem::take(&mut mutable.eviction_pickers);

        // Notify pickers.
        for picker in pickers.iter_mut() {
            picker.on_region_evictable(
                EvictionInfo {
                    regions: &self.inner.regions,
                    evictable: &mutable.evictable,
                    clean: mutable.counter.clean,
                },
                id,
            );
        }

        // Restore taken pickers after operations.
        std::mem::swap(&mut mutable.eviction_pickers, &mut pickers);
        assert!(pickers.is_empty());

        mutable.counter.writing -= 1;
        mutable.counter.evictable += 1;
        tracing::debug!("[region manager]: Region {id} is marked evictable.");
    }
}

#[derive(Debug)]
pub struct RegionReclaimNotifier<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    region_manager: RegionManager<K, V, P>,
    id: RegionId,
}

impl<K, V, P> RegionReclaimNotifier<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    pub fn notify(self) {
        let region = self.region_manager.region(&self.id).clone();
        self.region_manager.finish_reclaim(region);
    }
}

pub type GetCleanRegionHandle = Shared<BoxFuture<'static, Region>>;
