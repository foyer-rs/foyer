// Copyright 2026 foyer Project Authors
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

use std::sync::Arc;

use foyer_memory::CacheProperties;
use foyer_storage::{BlockEngineConfig, Device, Partition, PartitionId, Statistics};

#[derive(Debug)]
struct UnsupportedDevice;

impl Device for UnsupportedDevice {
    fn capacity(&self) -> usize {
        unimplemented!()
    }

    fn allocated(&self) -> usize {
        unimplemented!()
    }

    fn create_partition(&self, _: usize) -> foyer_storage::Result<Arc<dyn Partition>> {
        unimplemented!()
    }

    fn partitions(&self) -> usize {
        unimplemented!()
    }

    fn partition(&self, _: PartitionId) -> Arc<dyn Partition> {
        unimplemented!()
    }

    fn statistics(&self) -> &Arc<Statistics> {
        unimplemented!()
    }
}

fn main() {
    let device = Arc::new(UnsupportedDevice);
    let _: BlockEngineConfig<u64, u64, CacheProperties> = BlockEngineConfig::new(device);
}
