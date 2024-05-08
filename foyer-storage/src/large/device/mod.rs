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

pub mod allocator;
pub mod direct_fs;

use crate::error::Result;
use std::{fmt::Debug, future::Future};

use allocator::AlignedAllocator;

pub const ALIGN: usize = 4096;
pub const IO_BUFFER_ALLOCATOR: AlignedAllocator<ALIGN> = AlignedAllocator::new();

pub type RegionId = u32;

// TODO(MrCroxx): Use `trait_alias` after stable.

// pub trait IoBuf: AsRef<[u8]> + Send + Sync + 'static {}
// impl<T: AsRef<[u8]> + Send + Sync + 'static> IoBuf for T {}
// pub trait IoBufMut: AsRef<[u8]> + AsMut<[u8]> + Send + Sync + 'static {}
// impl<T: AsRef<[u8]> + AsMut<[u8]> + Send + Sync + 'static> IoBufMut for T {}

pub type IoBuffer = allocator_api2::vec::Vec<u8, &'static AlignedAllocator<ALIGN>>;

pub trait DeviceConfig: Send + Sync + 'static + Debug {
    fn verify(&self) -> Result<()>;
}

pub trait Device: Send + Sync + 'static + Sized + Clone {
    type Config: DeviceConfig;

    /// The capacity of the device, must be 4K aligned.
    fn capacity(&self) -> usize;

    /// The region size of the device, must be 4K aligned.
    fn region_size(&self) -> usize;

    #[must_use]
    fn open(config: &Self::Config) -> impl Future<Output = Result<Self>> + Send;

    #[must_use]
    fn write(&self, buf: IoBuffer, region: RegionId, offset: u64) -> impl Future<Output = Result<()>> + Send;

    #[must_use]
    fn read(&self, region: RegionId, offset: u64, len: usize) -> impl Future<Output = Result<IoBuffer>> + Send;

    #[must_use]
    fn flush(&self, region: Option<RegionId>) -> impl Future<Output = Result<()>> + Send;
}

pub trait DeviceExt: Device {
    fn align(&self) -> usize {
        ALIGN
    }

    fn regions(&self) -> usize {
        self.capacity() / self.region_size()
    }
}

impl<T> DeviceExt for T where T: Device {}
