// // Copyright 2025 foyer Project Authors
// //
// // Licensed under the Apache License, Version 2.0 (the "License");
// // you may not use this file except in compliance with the License.
// // You may obtain a copy of the License at
// //
// //     http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing, software
// // distributed under the License is distributed on an "AS IS" BASIS,
// // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// // See the License for the specific language governing permissions and
// // limitations under the License.

// use std::{
//     collections::HashSet,
//     fmt::Debug,
//     sync::{
//         atomic::{AtomicBool, AtomicUsize, Ordering},
//         Arc,
//     },
// };

// use flume::{Receiver, Sender};
// use foyer_common::{countdown::Countdown, metrics::Metrics};
// use futures_core::future::BoxFuture;
// use futures_util::{future::Shared, FutureExt};
// use itertools::Itertools;
// use parking_lot::Mutex;
// use rand::seq::IteratorRandom;
// use tokio::sync::Semaphore;

// use crate::{
//     device::{Dev, DevExt, MonitoredDevice, RegionId},
//     error::Result,
//     io::buffer::{IoBuf, IoBufMut},
//     picker::EvictionPicker,
//     EvictionInfo,
// };

// /// Region statistics.
// #[derive(Debug, Default)]
// pub struct RegionStatistics {
//     /// Estimated invalid bytes in the region.
//     /// FIXME(MrCroxx): This value is way too coarse. Need fix.
//     pub invalid: AtomicUsize,
//     /// Access count of the region.
//     pub access: AtomicUsize,
//     /// Marked as `true` if the region is about to be evicted by some eviction picker.
//     pub probation: AtomicBool,
// }

// impl RegionStatistics {
//     pub(crate) fn reset(&self) {
//         self.invalid.store(0, Ordering::Relaxed);
//         self.access.store(0, Ordering::Relaxed);
//         self.probation.store(false, Ordering::Relaxed);
//     }
// }

// #[derive(Debug)]
// struct RegionInner {
//     device: MonitoredDevice,
//     statistics: Arc<RegionStatistics>,
// }

// /// A region is a logical partition of a device. It is used to manage the device's storage space.
// #[derive(Debug, Clone)]
// pub struct Region {
//     id: RegionId,
//     inner: Arc<RegionInner>,
// }

// impl Region {
//     /// Get region id.
//     pub fn id(&self) -> RegionId {
//         self.id
//     }

//     /// Get Region Statistics.
//     pub fn statistics(&self) -> &Arc<RegionStatistics> {
//         &self.inner.statistics
//     }

//     /// Get region size.
//     pub fn size(&self) -> usize {
//         self.inner.device.region_size()
//     }

//     pub(crate) async fn write<B>(&self, buf: B, offset: u64) -> (B, Result<()>)
//     where
//         B: IoBuf,
//     {
//         self.inner.device.write(buf, self.id, offset).await
//     }

//     pub(crate) async fn read<B>(&self, buf: B, offset: u64) -> (B, Result<()>)
//     where
//         B: IoBufMut,
//     {
//         self.inner.statistics.access.fetch_add(1, Ordering::Relaxed);
//         self.inner.device.read(buf, self.id, offset).await
//     }

//     #[expect(unused)]
//     pub(crate) async fn sync(&self) -> Result<()> {
//         self.inner.device.sync(Some(self.id)).await
//     }
// }

// #[cfg(test)]
// impl Region {
//     pub(crate) fn new_for_test(id: RegionId, device: MonitoredDevice) -> Self {
//         let inner = RegionInner {
//             device,
//             statistics: Arc::<RegionStatistics>::default(),
//         };
//         let inner = Arc::new(inner);
//         Self { id, inner }
//     }
// }

// use std::{
//     collections::{HashSet, VecDeque},
//     future::ready,
//     sync::Arc,
// };

// use foyer_common::{
//     code::{StorageKey, StorageValue},
//     metrics::Metrics,
//     properties::Properties,
// };
// use futures_util::FutureExt;
// use parking_lot::{Mutex, MutexGuard};
// use rand::seq::IteratorRandom;
// use tokio::sync::oneshot;

// use crate::{
//     device::{MonitoredDevice, RegionId},
//     large::{
//         flusher::Flusher,
//         indexer::Indexer,
//         reclaimer_v2::{ReclaimerV2, ReclaimerV2Ctx},
//     },
//     region::GetCleanRegionHandle,
//     DevExt, EvictionInfo, EvictionPicker, Region, ReinsertionPicker, Runtime,
// };

// #[derive(Debug, Clone, Copy, PartialEq, Eq)]
// pub enum RegionState {
//     Clean,
//     Writing,
//     Evictable,
//     Reclaiming,
// }

// #[derive(Debug, Default)]
// struct RegionStateCounter {
//     clean: usize,
//     writing: usize,
//     evictable: usize,
//     reclaiming: usize,
// }

// #[derive(Debug)]
// struct Mutable {
//     counter: RegionStateCounter,

//     evictable: HashSet<RegionId>,
//     eviction_pickers: Vec<Box<dyn EvictionPicker>>,

//     clean_regions: VecDeque<RegionId>,
//     clean_region_waiters: VecDeque<oneshot::Sender<Region>>,
// }

// #[derive(Debug)]
// struct Inner<K, V, P>
// where
//     K: StorageKey,
//     V: StorageValue,
//     P: Properties,
// {
//     mutable: Mutex<Mutable>,

//     regions: Vec<Region>,
//     clean_region_threshold: usize,

//     metrics: Arc<Metrics>,

//     blob_index_size: usize,
//     reinsertion_picker: Arc<dyn ReinsertionPicker>,
//     device: MonitoredDevice,
//     flushers: Vec<Flusher<K, V, P>>,
//     runtime: Runtime,
//     indexer: Indexer,
//     reclaimers: usize,
// }

// #[derive(Debug)]
// pub struct RegionManager<K, V, P>
// where
//     K: StorageKey,
//     V: StorageValue,
//     P: Properties,
// {
//     inner: Arc<Inner<K, V, P>>,
// }

// impl<K, V, P> Clone for RegionManager<K, V, P>
// where
//     K: StorageKey,
//     V: StorageValue,
//     P: Properties,
// {
//     fn clone(&self) -> Self {
//         Self {
//             inner: self.inner.clone(),
//         }
//     }
// }

// impl<K, V, P> RegionManager<K, V, P>
// where
//     K: StorageKey,
//     V: StorageValue,
//     P: Properties,
// {
//     pub fn new(device: MonitoredDevice, eviction_pickers: Vec<Box<dyn EvictionPicker>>, metrics: Arc<Metrics>) -> Self {
//         let regions = (0..device.regions() as RegionId)
//             .map(|id| Region {
//                 id,
//                 inner: Arc::new(RegionInner {
//                     device: device.clone(),
//                     statistics: Arc::<RegionStatistics>::default(),
//                 }),
//             })
//             .collect_vec();

//         let mutable = Mutable {
//             counter: RegionStateCounter::default(),
//             evictable: todo!(),
//             eviction_pickers,
//             clean_regions: todo!(),
//             clean_region_waiters: todo!(),
//         };
//         let mutable = Mutex::new(mutable);
//         let inner = Inner {
//             mutable,
//             regions: todo!(),
//             clean_region_threshold: todo!(),
//             metrics,
//             blob_index_size: todo!(),
//             reinsertion_picker: todo!(),
//             device,
//             flushers: todo!(),
//             runtime: todo!(),
//             indexer: todo!(),
//             reclaimers: todo!(),
//         };
//         let inner = Arc::new(inner);
//         Self { inner }
//     }

//     pub fn region(&self, id: &RegionId) -> &Region {
//         &self.inner.regions[*id as usize]
//     }

//     pub fn regions(&self) -> usize {
//         self.inner.regions.len()
//     }

//     pub fn get_clean_region(&self) -> GetCleanRegionHandle {
//         let mut mutable = self.inner.mutable.lock();
//         if let Some(id) = mutable.clean_regions.pop_front() {
//             let region = self.region(&id).clone();
//             mutable.counter.clean -= 1;
//             self.try_start_reclaim(&mut mutable);
//             ready(region).boxed().shared()
//         } else {
//             let (tx, rx) = oneshot::channel();
//             mutable.clean_region_waiters.push_back(tx);
//             async move { rx.await.unwrap() }.boxed().shared()
//         }
//     }

//     fn evict<'a>(&self, mutable: &mut MutexGuard<'a, Mutable>) -> Option<Region> {
//         let mut picked = None;

//         if mutable.evictable.is_empty() {
//             // No evictable region now.
//             return None;
//         }

//         // Temporarily take pickers to make borrow checker happy.
//         let mut pickers = std::mem::take(&mut mutable.eviction_pickers);

//         // Pick a region to evict with pickers.
//         for picker in pickers.iter_mut() {
//             if let Some(region) = picker.pick(EvictionInfo {
//                 regions: &self.inner.regions,
//                 evictable: &mutable.evictable,
//                 clean: mutable.counter.clean,
//             }) {
//                 picked = Some(region);
//                 break;
//             }
//         }

//         // If no region is selected, just randomly pick one.
//         let picked = picked.unwrap_or_else(|| mutable.evictable.iter().choose(&mut rand::rng()).copied().unwrap());

//         // Update evictable map.
//         let removed = mutable.evictable.remove(&picked);
//         assert!(removed);
//         self.inner.metrics.storage_region_evictable.decrease(1);

//         // Notify pickers.
//         for picker in pickers.iter_mut() {
//             picker.on_region_evict(
//                 EvictionInfo {
//                     regions: &self.inner.regions,
//                     evictable: &mutable.evictable,
//                     clean: mutable.counter.clean,
//                 },
//                 picked,
//             );
//         }

//         // Restore taken pickers after operations.
//         std::mem::swap(&mut mutable.eviction_pickers, &mut pickers);
//         assert!(pickers.is_empty());

//         let region = self.region(&picked).clone();
//         tracing::debug!("[region manager]: Region {picked} is evicted.");

//         Some(region)
//     }

//     fn try_start_reclaim<'a>(&self, mutable: &mut MutexGuard<'a, Mutable>) {
//         if self.need_reclaim(&mutable) {
//             self.start_reclaim(mutable);
//         }
//     }

//     fn start_reclaim<'a>(&self, mutable: &mut MutexGuard<'a, Mutable>) {
//         let region = match self.evict(mutable) {
//             Some(region) => region,
//             None => {
//                 tracing::warn!("[region manager]: No evictable region to reclaim.");
//                 return;
//             }
//         };

//         mutable.counter.reclaiming += 1;
//         mutable.counter.evictable -= 1;

//         let ctx = ReclaimerV2Ctx {
//             region: region.clone(),
//             blob_index_size: self.inner.blob_index_size,
//             reinsertion_picker: self.inner.reinsertion_picker.clone(),
//             device: self.inner.device.clone(),
//             flushers: self.inner.flushers.clone(),
//             runtime: self.inner.runtime.clone(),
//             indexer: self.inner.indexer.clone(),
//             notifier: RegionReclaimNotifier {
//                 region_manager: self.clone(),
//                 region,
//             },
//         };
//         self.inner
//             .runtime
//             .write()
//             .spawn(async move { ReclaimerV2::reclaim(ctx) });
//     }

//     fn finish_reclaim(&self, region: Region) {
//         let mut mutable = self.inner.mutable.lock();

//         mutable.counter.reclaiming -= 1;
//         mutable.counter.clean += 1;

//         if let Some(tx) = mutable.clean_region_waiters.pop_front() {
//             let _ = tx.send(region);
//         } else {
//             mutable.clean_regions.push_back(region.id());
//         }

//         self.try_start_reclaim(&mut mutable);
//     }

//     fn need_reclaim<'a>(&self, mutable: &MutexGuard<'a, Mutable>) -> bool {
//         // have enough reclaimers
//         mutable.counter.reclaiming < self.inner.reclaimers
//         // not enough clean regions
//             && mutable.counter.clean + mutable.counter.reclaiming < self.inner.clean_region_threshold
//     }
// }

// #[derive(Debug)]
// pub struct RegionReclaimNotifier<K, V, P>
// where
//     K: StorageKey,
//     V: StorageValue,
//     P: Properties,
// {
//     region_manager: RegionManager<K, V, P>,
//     region: Region,
// }

// impl<K, V, P> RegionReclaimNotifier<K, V, P>
// where
//     K: StorageKey,
//     V: StorageValue,
//     P: Properties,
// {
//     pub fn notify(self) {
//         self.region_manager.finish_reclaim(self.region);
//     }
// }

// pub type GetCleanRegionHandle = Shared<BoxFuture<'static, Region>>;
