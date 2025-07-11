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
    collections::VecDeque,
    fmt::Debug,
    num::NonZeroUsize,
    ops::Range,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use foyer_common::strict_assert;
use itertools::Itertools;

use super::{AdmissionPicker, EvictionInfo, EvictionPicker, Pick, ReinsertionPicker};
use crate::{device::RegionId, io::throttle::IoThrottler, statistics::Statistics};

/// Only admit on all chained admission pickers pick.
#[derive(Debug, Default, Clone)]
pub struct ChainedAdmissionPicker {
    pickers: Arc<Vec<Arc<dyn AdmissionPicker>>>,
}

impl AdmissionPicker for ChainedAdmissionPicker {
    fn pick(&self, stats: &Arc<Statistics>, hash: u64) -> Pick {
        let mut duration = Duration::ZERO;
        for picker in self.pickers.iter() {
            match picker.pick(stats, hash) {
                Pick::Admit => {}
                Pick::Reject => return Pick::Reject,
                Pick::Throttled(dur) => duration += dur,
            }
        }
        if duration.is_zero() {
            Pick::Admit
        } else {
            Pick::Throttled(duration)
        }
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
    fn pick(&self, _: &Arc<Statistics>, _: u64) -> Pick {
        Pick::Admit
    }
}

impl ReinsertionPicker for AdmitAllPicker {
    fn pick(&self, _: &Arc<Statistics>, _: u64) -> Pick {
        Pick::Admit
    }
}

/// A picker that always returns `false`.
#[derive(Debug, Default)]
pub struct RejectAllPicker;

impl AdmissionPicker for RejectAllPicker {
    fn pick(&self, _: &Arc<Statistics>, _: u64) -> Pick {
        Pick::Reject
    }
}

impl ReinsertionPicker for RejectAllPicker {
    fn pick(&self, _: &Arc<Statistics>, _: u64) -> Pick {
        Pick::Reject
    }
}

#[derive(Debug)]
struct IoThrottlerPickerInner {
    throttler: IoThrottler,
    bytes_last: AtomicUsize,
    ios_last: AtomicUsize,
    target: IoThrottlerTarget,
}

/// Target of the io throttler.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum IoThrottlerTarget {
    /// Count read io for io throttling.
    Read,
    /// Count write io for io throttling.
    Write,
    /// Count read and write io for io throttling.
    ReadWrite,
}

/// A picker that picks based on the disk statistics and the given throttle args.
///
/// NOTE: This picker is automatically applied if the device throttle is set.
///
/// Please use set the `throttle` in the device options instead use this picker directly.
/// Unless you know what you are doing. :D
#[derive(Debug, Clone)]
pub struct IoThrottlerPicker {
    inner: Arc<IoThrottlerPickerInner>,
}

impl IoThrottlerPicker {
    /// Create a rate limit picker with the given rate limit.
    ///
    /// Note: `None` stands for unlimited.
    pub fn new(target: IoThrottlerTarget, throughput: Option<NonZeroUsize>, iops: Option<NonZeroUsize>) -> Self {
        let inner = IoThrottlerPickerInner {
            throttler: IoThrottler::new(throughput, iops),
            bytes_last: AtomicUsize::default(),
            ios_last: AtomicUsize::default(),
            target,
        };
        Self { inner: Arc::new(inner) }
    }

    fn pick_inner(&self, stats: &Arc<Statistics>) -> Pick {
        let duration = self.inner.throttler.probe();

        let bytes_current = match self.inner.target {
            IoThrottlerTarget::Read => stats.disk_read_bytes(),
            IoThrottlerTarget::Write => stats.disk_write_bytes(),
            IoThrottlerTarget::ReadWrite => stats.disk_read_bytes() + stats.disk_write_bytes(),
        };
        let ios_current = match self.inner.target {
            IoThrottlerTarget::Read => stats.disk_read_ios(),
            IoThrottlerTarget::Write => stats.disk_write_ios(),
            IoThrottlerTarget::ReadWrite => stats.disk_read_ios() + stats.disk_write_ios(),
        };

        let bytes_last = self.inner.bytes_last.load(Ordering::Relaxed);
        let ios_last = self.inner.ios_last.load(Ordering::Relaxed);

        let bytes_delta = bytes_current.saturating_sub(bytes_last);
        let ios_delta = ios_current.saturating_sub(ios_last);

        self.inner.bytes_last.store(bytes_current, Ordering::Relaxed);
        self.inner.ios_last.store(ios_current, Ordering::Relaxed);

        self.inner.throttler.reduce(bytes_delta as f64, ios_delta as f64);

        if duration.is_zero() {
            Pick::Admit
        } else {
            Pick::Throttled(duration)
        }
    }
}

impl AdmissionPicker for IoThrottlerPicker {
    fn pick(&self, stats: &Arc<Statistics>, _: u64) -> Pick {
        self.pick_inner(stats)
    }
}

/// A picker that pick region to eviction with a FIFO behavior.
#[derive(Debug)]
pub struct FifoPicker {
    queue: VecDeque<RegionId>,
    regions: usize,
    probations: usize,
    probation_ratio: f64,
}

impl Default for FifoPicker {
    fn default() -> Self {
        Self::new(0.1)
    }
}

impl FifoPicker {
    /// Create a new [`FifoPicker`] with the given `probation_ratio` (0.0 ~ 1.0).
    ///
    /// The `probation_ratio` is the ratio of regions that will be marked as probation.
    /// The regions that are marked as probation will be evicted first.
    ///
    /// The default value is 0.1.
    pub fn new(probation_ratio: f64) -> Self {
        assert!(
            (0.0..=1.0).contains(&probation_ratio),
            "probation ratio {probation_ratio} must be in [0.0, 1.0]"
        );
        Self {
            queue: VecDeque::new(),
            regions: 0,
            probations: 0,
            probation_ratio,
        }
    }
}

impl FifoPicker {
    fn mark_probation(&self, info: EvictionInfo<'_>) {
        let marks = self.probations.saturating_sub(info.clean);
        self.queue.iter().take(marks).for_each(|rid| {
            if info.evictable.contains(rid) {
                info.regions[*rid as usize]
                    .statistics()
                    .probation
                    .store(true, Ordering::Relaxed);
                tracing::trace!(rid, "[fifo picker]: mark probation");
            }
        });
    }
}

impl EvictionPicker for FifoPicker {
    fn init(&mut self, regions: Range<RegionId>, _: usize) {
        self.regions = regions.len();
        let probations = (self.regions as f64 * self.probation_ratio).floor() as usize;
        self.probations = probations.clamp(0, self.regions);
    }

    fn pick(&mut self, info: EvictionInfo<'_>) -> Option<RegionId> {
        tracing::trace!(evictable = ?info.evictable, clean = info.clean, "[fifo picker]: pick");
        self.mark_probation(info);
        let candidate = self.queue.front().copied();
        tracing::trace!("[fifo picker]: pick {candidate:?}");
        candidate
    }

    fn on_region_evictable(&mut self, _: EvictionInfo<'_>, region: RegionId) {
        tracing::trace!("[fifo picker]: {region} is evictable");
        self.queue.push_back(region);
    }

    fn on_region_evict(&mut self, _: EvictionInfo<'_>, region: RegionId) {
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

    fn pick(&mut self, info: EvictionInfo<'_>) -> Option<RegionId> {
        strict_assert!(self.region_size > 0);

        let mut data = info
            .evictable
            .iter()
            .map(|rid| {
                (
                    *rid,
                    info.regions[*rid as usize].statistics().invalid.load(Ordering::Relaxed),
                )
            })
            .collect_vec();
        data.sort_by_key(|(_, invalid)| *invalid);

        let (rid, invalid) = data.last().copied()?;
        if (invalid as f64 / self.region_size as f64) < self.threshold {
            return None;
        }
        tracing::trace!("[invalid ratio picker]: pick {rid:?}");
        Some(rid)
    }

    fn on_region_evictable(&mut self, _: EvictionInfo<'_>, region: RegionId) {
        tracing::trace!("[invalid ratio picker]: {region} is evictable");
    }

    fn on_region_evict(&mut self, _: EvictionInfo<'_>, region: RegionId) {
        tracing::trace!("[invalid ratio picker]: {region} is evicted");
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::{io::engine::noop::NoopIoEngine, region_v2::Region};

    #[test_log::test]
    fn test_fifo_picker() {
        let mut picker = FifoPicker::new(0.1);

        let regions = (0..10)
            .map(|rid| Region::new_for_test(rid, Arc::<NoopIoEngine>::default()))
            .collect_vec();
        let mut evictable = HashSet::new();

        fn info<'a>(regions: &'a [Region], evictable: &'a HashSet<RegionId>, dirty: usize) -> EvictionInfo<'a> {
            EvictionInfo {
                regions,
                evictable,
                clean: regions.len() - evictable.len() - dirty,
            }
        }

        fn check(regions: &[Region], probations: impl IntoIterator<Item = RegionId>) {
            let probations = probations.into_iter().collect::<HashSet<_>>();
            for rid in 0..regions.len() as RegionId {
                if probations.contains(&rid) {
                    assert!(
                        regions[rid as usize].statistics().probation.load(Ordering::Relaxed),
                        "probations {probations:?}, assert {rid} is probation failed"
                    );
                } else {
                    assert!(
                        !regions[rid as usize].statistics().probation.load(Ordering::Relaxed),
                        "probations {probations:?}, assert {rid} is not probation failed"
                    );
                }
            }
        }

        picker.init(0..10, 0);

        // mark 0..10 evictable in order
        (0..10).for_each(|i| {
            evictable.insert(i as _);
            picker.on_region_evictable(info(&regions, &evictable, 0), i);
        });

        // evict 0, mark 0 probation.
        assert_eq!(picker.pick(info(&regions, &evictable, 0)), Some(0));
        check(&regions, [0]);
        evictable.remove(&0);
        picker.on_region_evict(info(&regions, &evictable, 1), 0);

        // evict 1, with 1 dirty region, mark 1 probation.
        assert_eq!(picker.pick(info(&regions, &evictable, 1)), Some(1));
        check(&regions, [0, 1]);
        evictable.remove(&1);
        picker.on_region_evict(info(&regions, &evictable, 2), 1);

        // evict 2 with 1 dirty region, mark no region probation.
        assert_eq!(picker.pick(info(&regions, &evictable, 1)), Some(2));
        check(&regions, [0, 1]);
        evictable.remove(&2);
        picker.on_region_evict(info(&regions, &evictable, 2), 2);

        picker.on_region_evict(info(&regions, &evictable, 3), 3);
        evictable.remove(&3);
        picker.on_region_evict(info(&regions, &evictable, 4), 5);
        evictable.remove(&5);
        picker.on_region_evict(info(&regions, &evictable, 5), 7);
        evictable.remove(&7);
        picker.on_region_evict(info(&regions, &evictable, 6), 9);
        evictable.remove(&9);

        picker.on_region_evict(info(&regions, &evictable, 7), 4);
        evictable.remove(&4);
        picker.on_region_evict(info(&regions, &evictable, 8), 6);
        evictable.remove(&6);
        picker.on_region_evict(info(&regions, &evictable, 9), 8);
        evictable.remove(&8);
    }

    #[test]
    fn test_invalid_ratio_picker() {
        let mut picker = InvalidRatioPicker::new(0.5);
        picker.init(0..10, 10);

        let regions = (0..10)
            .map(|rid| Region::new_for_test(rid, Arc::<NoopIoEngine>::default()))
            .collect_vec();
        let mut evictable = HashSet::new();

        (0..10).for_each(|i| {
            regions[i].statistics().invalid.fetch_add(i as _, Ordering::Relaxed);
            evictable.insert(i as RegionId);
        });

        fn info<'a>(regions: &'a [Region], evictable: &'a HashSet<RegionId>) -> EvictionInfo<'a> {
            EvictionInfo {
                regions,
                evictable,
                clean: regions.len() - evictable.len(),
            }
        }

        assert_eq!(picker.pick(info(&regions, &evictable)), Some(9));

        assert_eq!(picker.pick(info(&regions, &evictable)), Some(9));
        evictable.remove(&9);
        assert_eq!(picker.pick(info(&regions, &evictable)), Some(8));
        evictable.remove(&8);
        assert_eq!(picker.pick(info(&regions, &evictable)), Some(7));
        evictable.remove(&7);
        assert_eq!(picker.pick(info(&regions, &evictable)), Some(6));
        evictable.remove(&6);
        assert_eq!(picker.pick(info(&regions, &evictable)), Some(5));
        evictable.remove(&5);

        assert_eq!(picker.pick(info(&regions, &evictable)), None);
        assert_eq!(picker.pick(info(&regions, &evictable)), None);
    }
}
