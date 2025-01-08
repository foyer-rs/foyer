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

//  Copyright 2024 foyer Project Authors
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

use std::{collections::HashMap, fmt::Debug, ops::Range, sync::Arc};

use crate::{device::RegionId, region::RegionStats, statistics::Statistics};

/// The admission picker for the disk cache.
pub trait AdmissionPicker: Send + Sync + 'static + Debug {
    /// The key type for the admission picker.
    type Key;

    /// Decide whether to pick a key.
    fn pick(&self, stats: &Arc<Statistics>, key: &Self::Key) -> bool;
}

/// The reinsertion picker for the disk cache.
pub trait ReinsertionPicker: Send + Sync + 'static + Debug {
    /// The key type for the reinsertion picker.
    type Key;

    /// Decide whether to pick a key.
    fn pick(&self, stats: &Arc<Statistics>, key: &Self::Key) -> bool;
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
    fn pick(&mut self, evictable: &HashMap<RegionId, Arc<RegionStats>>) -> Option<RegionId>;

    /// Notify the picker that a region is ready to pick.
    fn on_region_evictable(&mut self, evictable: &HashMap<RegionId, Arc<RegionStats>>, region: RegionId);

    /// Notify the picker that a region is evicted.
    fn on_region_evict(&mut self, evictable: &HashMap<RegionId, Arc<RegionStats>>, region: RegionId);
}

pub mod utils;
