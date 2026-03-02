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

use crate::storage::{
    AddNamedRandomAccessSegment, AddNamedSegment, AddRandomAccessSegment, AddSegment, RandomAccessSegment,
    RemoveNamedSegment, RemoveSegment, Segment, SegmentId, Storage, volume::io::engine::IoEngine,
};

use foyer_common::error::Result;

pub trait VolumeStorageDevice: Storage {}

#[derive(Debug)]
pub struct VolumeStorageBuilder {}

#[derive(Debug)]
pub struct VolumeStorage<D>
where
    D: VolumeStorageDevice,
{
    io_engine: Arc<dyn IoEngine>,
    device: D,
}

impl<D> AddSegment for VolumeStorage<D>
where
    D: VolumeStorageDevice + AddSegment,
{
    fn add_segment(&self, size: usize) -> Arc<dyn Segment> {
        self.device.add_segment(size)
    }
}

impl<D> AddRandomAccessSegment for VolumeStorage<D>
where
    D: VolumeStorageDevice + AddRandomAccessSegment,
{
    fn add_random_access_segment(&self, size: usize) -> Arc<dyn RandomAccessSegment> {
        self.device.add_random_access_segment(size)
    }
}

impl<D> AddNamedSegment for VolumeStorage<D>
where
    D: VolumeStorageDevice + AddNamedSegment,
{
    fn add_named_segment(&self, name: &str, size: usize) -> Arc<dyn Segment> {
        self.device.add_named_segment(name, size)
    }
}

impl<D> AddNamedRandomAccessSegment for VolumeStorage<D>
where
    D: VolumeStorageDevice + AddNamedRandomAccessSegment,
{
    fn add_named_random_access_segment(&self, name: &str, size: usize) -> Arc<dyn RandomAccessSegment> {
        self.device.add_named_random_access_segment(name, size)
    }
}

impl<D> RemoveSegment for VolumeStorage<D>
where
    D: VolumeStorageDevice + RemoveSegment,
{
    fn remove_segment(&self, id: SegmentId) -> Result<()> {
        self.device.remove_segment(id)
    }
}

impl<D> RemoveNamedSegment for VolumeStorage<D>
where
    D: VolumeStorageDevice + RemoveNamedSegment,
{
    fn remove_named_segment(&self, name: &str) -> Result<()> {
        self.device.remove_named_segment(name)
    }
}
