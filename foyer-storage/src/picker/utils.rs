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
    collections::{HashMap, VecDeque},
    fmt::Debug,
    num::NonZeroUsize,
    ops::Range,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use foyer_common::strict_assert;
use itertools::Itertools;

use super::{AdmissionPicker, EvictionPicker, ReinsertionPicker};
use crate::{device::RegionId, io::throttle::IoThrottler, region::RegionStats, statistics::Statistics};

/// Only admit on all chained admission pickers pick.
#[derive(Debug, Default, Clone)]
pub struct ChainedAdmissionPicker {
    pickers: Arc<Vec<Arc<dyn AdmissionPicker>>>,
}

impl AdmissionPicker for ChainedAdmissionPicker {
    fn pick(&self, stats: &Arc<Statistics>, size: u64) -> bool {
        self.pickers.iter().all(|picker| picker.pick(stats, size))
    }
}

/// A builder for [`ChainedAdmissionPicker`].
#[derive(Debug, Default)]
pub struct ChainedAdmissionPickerBuilder {
    pickers: Vec<Arc<dyn AdmissionPicker>>,
}

impl ChainedAdmissionPickerBuilder {
    /// Chain a new admission picker.
    pub fn chain(mut self, picker: Arc<dyn AdmissionPicker>) -> Self {
        self.pickers.push(picker);
        self
    }

    /// Build the chained admission picker.
    pub fn build(self) -> ChainedAdmissionPicker {
        ChainedAdmissionPicker {
            pickers: Arc::new(self.pickers),
        }
    }
}

/// A picker that always returns `true`.
#[derive(Debug, Default)]
pub struct AdmitAllPicker;

impl AdmissionPicker for AdmitAllPicker {
    fn pick(&self, _: &Arc<Statistics>, _: u64) -> bool {
        true
    }
}

impl ReinsertionPicker for AdmitAllPicker {
    fn pick(&self, _: &Arc<Statistics>, _: u64) -> bool {
        true
    }
}

/// A picker that always returns `false`.
#[derive(Debug, Default)]
pub struct RejectAllPicker;

impl AdmissionPicker for RejectAllPicker {
    fn pick(&self, _: &Arc<Statistics>, _: u64) -> bool {
        false
    }
}

impl ReinsertionPicker for RejectAllPicker {
    fn pick(&self, _: &Arc<Statistics>, _: u64) -> bool {
        false
    }
}

#[derive(Debug)]
struct IoThrottlerPickerInner {
    throttler: IoThrottler,
    bytes_last: AtomicUsize,
    ios_last: AtomicUsize,
}

/// A picker that picks based on the disk statistics and the given throttle args.
#[derive(Debug, Clone)]
pub struct IoThrottlerPicker {
    inner: Arc<IoThrottlerPickerInner>,
}

impl IoThrottlerPicker {
    /// Create a rate limit picker with the given rate limit.
    ///
    /// Note: `None` stands for unlimited.
    pub fn new(throughput: Option<NonZeroUsize>, iops: Option<NonZeroUsize>) -> Self {
        let inner = IoThrottlerPickerInner {
            throttler: IoThrottler::new(throughput, iops),
            bytes_last: AtomicUsize::default(),
            ios_last: AtomicUsize::default(),
        };

        Self { inner: Arc::new(inner) }
    }

    fn pick_inner(&self, stats: &Arc<Statistics>) -> bool {
        let picked = self.inner.throttler.probe();

        let bytes_current = stats.cache_write_bytes();
        let ios_current = stats.cache_write_ios();

        let bytes_last = self.inner.bytes_last.load(Ordering::Relaxed);
        let ios_last = self.inner.ios_last.load(Ordering::Relaxed);

        let bytes_delta = bytes_current.saturating_sub(bytes_last);
        let ios_delta = ios_current.saturating_sub(ios_last);

        self.inner.bytes_last.store(bytes_current, Ordering::Relaxed);
        self.inner.ios_last.store(ios_current, Ordering::Relaxed);

        self.inner.throttler.reduce(bytes_delta as f64, ios_delta as f64);

        picked
    }
}

impl AdmissionPicker for IoThrottlerPicker {
    fn pick(&self, stats: &Arc<Statistics>, _: u64) -> bool {
        self.pick_inner(stats)
    }
}

/// A picker that pick region to eviction with a FIFO behavior.
#[derive(Debug, Default)]
pub struct FifoPicker {
    queue: VecDeque<RegionId>,
}

impl EvictionPicker for FifoPicker {
    fn pick(&mut self, _: &HashMap<RegionId, Arc<RegionStats>>) -> Option<RegionId> {
        let res = self.queue.front().copied();
        tracing::trace!("[fifo picker]: pick {res:?}");
        res
    }

    fn on_region_evictable(&mut self, _: &HashMap<RegionId, Arc<RegionStats>>, region: RegionId) {
        tracing::trace!("[fifo picker]: {region} is evictable");
        self.queue.push_back(region);
    }

    fn on_region_evict(&mut self, _: &HashMap<RegionId, Arc<RegionStats>>, region: RegionId) {
        tracing::trace!("[fifo picker]: {region} is evicted");
        let index = self.queue.iter().position(|r| r == &region).unwrap();
        self.queue.remove(index);
    }
}

/// Evict the region with the largest invalid data ratio.
///
/// If the largest invalid data ratio is less than the threshold, no region will be picked.
#[derive(Debug)]
pub struct InvalidRatioPicker {
    threshold: f64,
    region_size: usize,
}

impl InvalidRatioPicker {
    /// Create [`InvalidRatioPicker`] with the given `threshold` (0.0 ~ 1.0).
    pub fn new(threshold: f64) -> Self {
        let ratio = threshold.clamp(0.0, 1.0);
        Self {
            threshold: ratio,
            region_size: 0,
        }
    }
}

impl EvictionPicker for InvalidRatioPicker {
    fn init(&mut self, _: Range<RegionId>, region_size: usize) {
        self.region_size = region_size;
    }

    fn pick(&mut self, evictable: &HashMap<RegionId, Arc<RegionStats>>) -> Option<RegionId> {
        strict_assert!(self.region_size > 0);

        let mut info = evictable
            .iter()
            .map(|(region, stats)| (*region, stats.invalid.load(Ordering::Relaxed)))
            .collect_vec();
        info.sort_by_key(|(_, invalid)| *invalid);

        let (region, invalid) = info.last().copied()?;
        if (invalid as f64 / self.region_size as f64) < self.threshold {
            return None;
        }
        tracing::trace!("[invalid ratio picker]: pick {region:?}");
        Some(region)
    }

    fn on_region_evictable(&mut self, _: &HashMap<RegionId, Arc<RegionStats>>, region: RegionId) {
        tracing::trace!("[invalid ratio picker]: {region} is evictable");
    }

    fn on_region_evict(&mut self, _: &HashMap<RegionId, Arc<RegionStats>>, region: RegionId) {
        tracing::trace!("[invalid ratio picker]: {region} is evicted");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fifo_picker() {
        let mut picker = FifoPicker::default();
        let m = HashMap::new();

        (0..10).for_each(|i| picker.on_region_evictable(&m, i));

        assert_eq!(picker.pick(&m), Some(0));
        picker.on_region_evict(&m, 0);
        assert_eq!(picker.pick(&m), Some(1));
        picker.on_region_evict(&m, 1);
        assert_eq!(picker.pick(&m), Some(2));
        picker.on_region_evict(&m, 2);

        picker.on_region_evict(&m, 3);
        picker.on_region_evict(&m, 5);
        picker.on_region_evict(&m, 7);
        picker.on_region_evict(&m, 9);

        assert_eq!(picker.pick(&m), Some(4));
        picker.on_region_evict(&m, 4);
        assert_eq!(picker.pick(&m), Some(6));
        picker.on_region_evict(&m, 6);
        assert_eq!(picker.pick(&m), Some(8));
        picker.on_region_evict(&m, 8);
    }

    #[test]
    fn test_invalid_ratio_picker() {
        let mut picker = InvalidRatioPicker::new(0.5);
        picker.init(0..10, 10);

        let mut m = HashMap::new();

        (0..10).for_each(|i| {
            let stats = Arc::new(RegionStats::default());
            stats.invalid.fetch_add(i as _, Ordering::Relaxed);
            m.insert(i, stats);
        });

        assert_eq!(picker.pick(&m), Some(9));

        assert_eq!(picker.pick(&m), Some(9));
        m.remove(&9);
        assert_eq!(picker.pick(&m), Some(8));
        m.remove(&8);
        assert_eq!(picker.pick(&m), Some(7));
        m.remove(&7);
        assert_eq!(picker.pick(&m), Some(6));
        m.remove(&6);
        assert_eq!(picker.pick(&m), Some(5));
        m.remove(&5);

        assert_eq!(picker.pick(&m), None);
        assert_eq!(picker.pick(&m), None);
    }
}
