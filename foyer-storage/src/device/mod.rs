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
pub mod direct_file;
pub mod direct_fs;
pub mod monitor;

use crate::{error::Result, DirectFileDevice, DirectFileDeviceOptions, DirectFsDevice, DirectFsDeviceOptions};
use std::{fmt::Debug, future::Future};

use allocator::AlignedAllocator;
use monitor::Monitored;

pub const ALIGN: usize = 4096;
pub const IO_BUFFER_ALLOCATOR: AlignedAllocator<ALIGN> = AlignedAllocator::new();

pub type RegionId = u32;

// TODO(MrCroxx): Use `trait_alias` after stable.

// pub trait IoBuf: AsRef<[u8]> + Send + Sync + 'static {}
// impl<T: AsRef<[u8]> + Send + Sync + 'static> IoBuf for T {}
// pub trait IoBufMut: AsRef<[u8]> + AsMut<[u8]> + Send + Sync + 'static {}
// impl<T: AsRef<[u8]> + AsMut<[u8]> + Send + Sync + 'static> IoBufMut for T {}

pub type IoBuffer = allocator_api2::vec::Vec<u8, &'static AlignedAllocator<ALIGN>>;

/// Options for the device.
pub trait DevOptions: Send + Sync + 'static + Debug + Clone {
    /// Verify the correctness of the options.
    fn verify(&self) -> Result<()>;
}

/// [`Dev`] represents 4K aligned block device.
///
/// Both i/o block and i/o buffer must be aligned to 4K.
pub trait Dev: Send + Sync + 'static + Sized + Clone + Debug {
    /// Options for the device.
    type Options: DevOptions;

    /// The capacity of the device, must be 4K aligned.
    fn capacity(&self) -> usize;

    /// The region size of the device, must be 4K aligned.
    fn region_size(&self) -> usize;

    /// Open the device with the given options.
    #[must_use]
    fn open(options: Self::Options) -> impl Future<Output = Result<Self>> + Send;

    /// Write API for the device.
    #[must_use]
    fn write(&self, buf: IoBuffer, region: RegionId, offset: u64) -> impl Future<Output = Result<()>> + Send;

    /// Read API for the device.
    #[must_use]
    fn read(&self, region: RegionId, offset: u64, len: usize) -> impl Future<Output = Result<IoBuffer>> + Send;

    /// Flush the device, make sure all modifications are persisted safely on the device.
    #[must_use]
    fn flush(&self, region: Option<RegionId>) -> impl Future<Output = Result<()>> + Send;
}

/// Device extend interfaces.
pub trait DevExt: Dev {
    /// Get the align size of the device.
    fn align(&self) -> usize {
        ALIGN
    }

    /// Get the region count of the device.
    fn regions(&self) -> usize {
        self.capacity() / self.region_size()
    }
}

impl<T> DevExt for T where T: Dev {}

#[derive(Debug, Clone)]
pub enum DeviceOptions {
    DirectFile(DirectFileDeviceOptions),
    DirectFs(DirectFsDeviceOptions),
}

impl From<DirectFileDeviceOptions> for DeviceOptions {
    fn from(value: DirectFileDeviceOptions) -> Self {
        Self::DirectFile(value)
    }
}

impl From<DirectFsDeviceOptions> for DeviceOptions {
    fn from(value: DirectFsDeviceOptions) -> Self {
        Self::DirectFs(value)
    }
}

impl DevOptions for DeviceOptions {
    fn verify(&self) -> Result<()> {
        match self {
            DeviceOptions::DirectFile(dev) => dev.verify(),
            DeviceOptions::DirectFs(dev) => dev.verify(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Device {
    DirectFile(DirectFileDevice),
    DirectFs(DirectFsDevice),
}

impl Dev for Device {
    type Options = DeviceOptions;

    fn capacity(&self) -> usize {
        match self {
            Device::DirectFile(dev) => dev.capacity(),
            Device::DirectFs(dev) => dev.capacity(),
        }
    }

    fn region_size(&self) -> usize {
        match self {
            Device::DirectFile(dev) => dev.region_size(),
            Device::DirectFs(dev) => dev.region_size(),
        }
    }

    async fn open(options: Self::Options) -> Result<Self> {
        match options {
            DeviceOptions::DirectFile(opts) => Ok(Self::DirectFile(DirectFileDevice::open(opts).await?)),
            DeviceOptions::DirectFs(opts) => Ok(Self::DirectFs(DirectFsDevice::open(opts).await?)),
        }
    }

    async fn write(&self, buf: IoBuffer, region: RegionId, offset: u64) -> Result<()> {
        match self {
            Device::DirectFile(dev) => dev.write(buf, region, offset).await,
            Device::DirectFs(dev) => dev.write(buf, region, offset).await,
        }
    }

    async fn read(&self, region: RegionId, offset: u64, len: usize) -> Result<IoBuffer> {
        match self {
            Device::DirectFile(dev) => dev.read(region, offset, len).await,
            Device::DirectFs(dev) => dev.read(region, offset, len).await,
        }
    }

    async fn flush(&self, region: Option<RegionId>) -> Result<()> {
        match self {
            Device::DirectFile(dev) => dev.flush(region).await,
            Device::DirectFs(dev) => dev.flush(region).await,
        }
    }
}

pub type MonitoredDevice = Monitored<Device>;
