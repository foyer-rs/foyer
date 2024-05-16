//  Copyright 2024 Foyer Project Authors
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

use crate::{device::RegionId, region::RegionStats, statistics::Statistics};
use std::{collections::HashMap, fmt::Debug, sync::Arc};

pub trait AdmissionPicker: Send + Sync + 'static + Debug {
    type Key;

    fn pick(&self, stats: &Arc<Statistics>, key: &Self::Key) -> bool;
}

pub trait ReinsertionPicker: Send + Sync + 'static + Debug {
    type Key;

    fn pick(&self, stats: &Arc<Statistics>, key: &Self::Key) -> bool;
}

pub trait EvictionPicker: Send + Sync + 'static + Debug {
    // TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
    #[allow(unused_variables)]
    fn init(&mut self, regions: usize, region_size: usize) {}

    fn pick(&mut self, evictable: &HashMap<RegionId, Arc<RegionStats>>) -> Option<RegionId>;

    fn on_region_evictable(&mut self, evictable: &HashMap<RegionId, Arc<RegionStats>>, region: RegionId);

    fn on_region_evict(&mut self, evictable: &HashMap<RegionId, Arc<RegionStats>>, region: RegionId);
}

pub mod utils;
