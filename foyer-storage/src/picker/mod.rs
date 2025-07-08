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

use std::{collections::HashSet, fmt::Debug, ops::Range, sync::Arc, time::Duration};

use crate::{device::RegionId, region::Region, statistics::Statistics};

/// Pick result for admission pickers and reinsertion pickers.
#[derive(Debug, Clone, Copy)]
pub enum Pick {
    /// Admittion.
    Admit,
    /// Rejection.
    Reject,
    /// This result indicates that the disk cache is throttled caused by the current io throttle.
    /// The minimal duration to retry this submission is returned for the caller to decide whether to retry it later.
    Throttled(Duration),
}

impl Pick {
    /// Return `true` if the pick result is `Admit`.
    pub fn admitted(&self) -> bool {
        matches! {self, Self::Admit}
    }

    /// Return `true` if the pick result is `Reject`.
    pub fn rejected(&self) -> bool {
        matches! {self, Self::Reject}
    }
}

impl From<bool> for Pick {
    fn from(value: bool) -> Self {
        match value {
            true => Self::Admit,
            false => Self::Reject,
        }
    }
}

/// The admission picker for the disk cache.
pub trait AdmissionPicker: Send + Sync + 'static + Debug {
    /// Decide whether to pick an entry by hash.
    fn pick(&self, stats: &Arc<Statistics>, hash: u64) -> Pick;
}

/// The reinsertion picker for the disk cache.
pub trait ReinsertionPicker: Send + Sync + 'static + Debug {
    /// Decide whether to pick an entry by hash.
    fn pick(&self, stats: &Arc<Statistics>, hash: u64) -> Pick;
}

/// Eviction related information for eviction picker to make decisions.
#[derive(Debug)]
pub struct EvictionInfo<'a> {
    /// All regions in the disk cache.
    pub regions: &'a [Region],
    /// Evictable regions.
    pub evictable: &'a HashSet<RegionId>,
    // TODO(MrCroxx): use counters!!!!!
    /// Clean regions counts.
    pub clean: usize,
}

/// The eviction picker for the disk cache.
pub trait EvictionPicker: Send + Sync + 'static + Debug {
    /// Init the eviction picker with information.
    #[expect(unused_variables)]
    fn init(&mut self, regions: Range<RegionId>, region_size: usize) {}

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

pub mod utils;
