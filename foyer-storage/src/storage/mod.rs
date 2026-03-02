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

use std::{any::Any, fmt::Debug, sync::Arc};

use crate::io::{
    bytes::{IoBuf, IoBufMut},
    engine::IoHandle,
};

use foyer_common::error::Result;

pub type SegmentId = u64;

pub trait Segment: Debug + Send + Sync + Any + 'static {
    fn id(&self) -> SegmentId;
    fn size(&self) -> usize;

    fn read(&self, data: Box<dyn IoBufMut>) -> IoHandle;
    fn write(&self, data: Box<dyn IoBuf>) -> IoHandle;
}

pub trait RandomAccessSegment: Segment {
    fn read_at(&self, offset: u64, data: Box<dyn IoBufMut>) -> IoHandle;
    fn write_at(&self, offset: u64, data: Box<dyn IoBuf>) -> IoHandle;
}

pub trait Storage: Send + Sync + Any + 'static {
    fn capacity(&self) -> usize;
    fn free(&self) -> usize;
    fn used(&self) -> usize;
}

pub trait AddSegment: Storage {
    fn add_segment(&self, size: usize) -> Arc<dyn Segment>;
}

pub trait AddRandomAccessSegment: Storage {
    fn add_random_access_segment(&self, size: usize) -> Arc<dyn RandomAccessSegment>;
}

pub trait AddNamedSegment: Storage {
    fn add_named_segment(&self, name: &str, size: usize) -> Arc<dyn Segment>;
}

pub trait AddNamedRandomAccessSegment: Storage {
    fn add_named_random_access_segment(&self, name: &str, size: usize) -> Arc<dyn RandomAccessSegment>;
}

pub trait RemoveSegment: Storage {
    fn remove_segment(&self, id: SegmentId) -> Result<()>;
}

pub trait RemoveNamedSegment: Storage {
    fn remove_named_segment(&self, name: &str) -> Result<()>;
}

mod volume;
