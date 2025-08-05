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
    sync::atomic::Ordering,
};

use foyer_common::strict_assert;
use itertools::Itertools;

use crate::engine::large::region::{Region, RegionId};

/// Eviction related information for eviction picker to make decisions.
#[derive(Debug)]
pub struct EvictionInfo<'a> {
    /// All regions in the disk cache.
    pub regions: &'a [Region],
    /// Evictable regions.
    pub evictable: &'a HashSet<RegionId>,
    /// Clean regions counts.
    pub clean: usize,
}

/// The eviction picker for the disk cache.
pub trait EvictionPicker: Send + Sync + 'static + Debug {
    /// Init the eviction picker with information.
    #[expect(unused_variables)]
    fn init(&mut self, regions: &[RegionId], region_size: usize) {}

    /// Pick a region to evict.
    ///
    /// `pick` can return `None` if no region can be picked based on its rules, and the next picker will be used.
    ///
    /// If no picker picks a region, the disk cache will pick randomly pick one.
    fn pick(&mut self, info: EvictionInfo<'_>) -> Option<RegionId>;

    /// Notify the picker that a region is ready to pick.
    fn on_region_evictable(&mut self, info: EvictionInfo<'_>, region: RegionId);

    /// Notify the picker that a region is evicted.
    fn on_region_evict(&mut self, info: EvictionInfo<'_>, region: RegionId);
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
    fn init(&mut self, regions: &[RegionId], _: usize) {
        self.regions = regions.len();
        let probations = (self.regions as f64 * self.probation_ratio).floor() as usize;
        self.probations = probations.clamp(0, self.regions);
    }

    fn pick(&mut self, info: EvictionInfo<'_>) -> Option<RegionId> {
        tracing::trace!(evictable = ?info.evictable, clean = info.clean, queue = ?self.queue, "[fifo picker]: pick");
        self.mark_probation(info);
        let candidate = self.queue.front().copied();
        tracing::trace!("[fifo picker]: pick {candidate:?}");
        candidate
    }

    fn on_region_evictable(&mut self, _: EvictionInfo<'_>, region: RegionId) {
        tracing::trace!(queue = ?self.queue, "[fifo picker]: {region} is evictable");
        self.queue.push_back(region);
    }

    fn on_region_evict(&mut self, _: EvictionInfo<'_>, region: RegionId) {
        tracing::trace!(queue = ?self.queue, "[fifo picker]: {region} is evicted");
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
    fn init(&mut self, _: &[RegionId], region_size: usize) {
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
    use std::{collections::HashSet, sync::Arc};

    use itertools::Itertools;

    use super::*;
    use crate::{engine::large::region::Region, io::device::noop::NoopPartition, IoEngineBuilder, NoopIoEngineBuilder};

    #[test_log::test(tokio::test)]
    async fn test_fifo_picker() {
        let mut picker = FifoPicker::new(0.1);
        let mock_io_engine = NoopIoEngineBuilder.build().await.unwrap();

        let regions = (0..10)
            .map(|rid| Region::new_for_test(rid, Arc::<NoopPartition>::default(), mock_io_engine.clone()))
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

        picker.init(&(0..10).collect_vec(), 0);

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

    #[test_log::test(tokio::test)]
    async fn test_invalid_ratio_picker() {
        let mut picker = InvalidRatioPicker::new(0.5);
        picker.init(&(0..10).collect_vec(), 10);

        let mock_io_engine = NoopIoEngineBuilder.build().await.unwrap();

        let regions = (0..10)
            .map(|rid| Region::new_for_test(rid, Arc::<NoopPartition>::default(), mock_io_engine.clone()))
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
