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

pub mod allocator;
pub mod direct_file;
pub mod direct_fs;
pub mod monitor;

use std::{fmt::Debug, future::Future, num::NonZeroUsize};

use direct_file::DirectFileDeviceConfig;
use direct_fs::DirectFsDeviceConfig;
use monitor::Monitored;

use crate::{
    DirectFileDevice, DirectFileDeviceOptions, DirectFsDevice, DirectFsDeviceOptions, Runtime,
    error::Result,
    io::{
        PAGE,
        buffer::{IoBuf, IoBufMut},
    },
};

pub type RegionId = u32;

/// Config for the device.
pub trait DevConfig: Send + Sync + 'static + Debug {}
impl<T: Send + Sync + 'static + Debug> DevConfig for T {}

/// Device iops counter.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum IopsCounter {
    /// Count 1 iops for each read/write.
    PerIo,
    /// Count 1 iops for each read/write with the size of the i/o.
    PerIoSize(NonZeroUsize),
}

impl IopsCounter {
    /// Create a new iops counter that count 1 iops for each io.
    pub fn per_io() -> Self {
        Self::PerIo
    }

    /// Create a new iops counter that count 1 iops for every io size in bytes among ios.
    ///
    /// NOTE: `io_size` must NOT be zero.
    pub fn per_io_size(io_size: usize) -> Self {
        Self::PerIoSize(NonZeroUsize::new(io_size).expect("io size must be non-zero"))
    }

    /// Count io(s) by io size in bytes.
    pub fn count(&self, bytes: usize) -> usize {
        match self {
            IopsCounter::PerIo => 1,
            IopsCounter::PerIoSize(size) => bytes / *size + if bytes % *size != 0 { 1 } else { 0 },
        }
    }
}

/// Throttle config for the device.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Throttle {
    /// The maximum write iops for the device.
    pub write_iops: Option<NonZeroUsize>,
    /// The maximum read iops for the device.
    pub read_iops: Option<NonZeroUsize>,
    /// The maximum write throughput for the device.
    pub write_throughput: Option<NonZeroUsize>,
    /// The maximum read throughput for the device.
    pub read_throughput: Option<NonZeroUsize>,
    /// The iops counter for the device.
    pub iops_counter: IopsCounter,
}

impl Default for Throttle {
    fn default() -> Self {
        Self::new()
    }
}

impl Throttle {
    /// Create a new unlimited throttle config.
    pub fn new() -> Self {
        Self {
            write_iops: None,
            read_iops: None,
            write_throughput: None,
            read_throughput: None,
            iops_counter: IopsCounter::PerIo,
        }
    }

    /// Set the maximum write iops for the device.
    pub fn with_write_iops(mut self, iops: usize) -> Self {
        self.write_iops = NonZeroUsize::new(iops);
        self
    }

    /// Set the maximum read iops for the device.
    pub fn with_read_iops(mut self, iops: usize) -> Self {
        self.read_iops = NonZeroUsize::new(iops);
        self
    }

    /// Set the maximum write throughput for the device.
    pub fn with_write_throughput(mut self, throughput: usize) -> Self {
        self.write_throughput = NonZeroUsize::new(throughput);
        self
    }

    /// Set the maximum read throughput for the device.
    pub fn with_read_throughput(mut self, throughput: usize) -> Self {
        self.read_throughput = NonZeroUsize::new(throughput);
        self
    }

    /// Set the iops counter for the device.
    pub fn with_iops_counter(mut self, counter: IopsCounter) -> Self {
        self.iops_counter = counter;
        self
    }
}

/// [`Dev`] represents 4K aligned block device.
///
/// Both i/o block and i/o buffer must be aligned to 4K.
pub trait Dev: Send + Sync + 'static + Sized + Clone + Debug {
    /// Config for the device.
    type Config: DevConfig;

    /// The capacity of the device, must be 4K aligned.
    fn capacity(&self) -> usize;

    /// The region size of the device, must be 4K aligned.
    fn region_size(&self) -> usize;

    /// The throttle config for the device.
    fn throttle(&self) -> &Throttle;

    /// Open the device with the given config.
    #[must_use]
    fn open(config: Self::Config, runtime: Runtime) -> impl Future<Output = Result<Self>> + Send;

    /// Write API for the device.
    #[must_use]
    fn write<B>(&self, buf: B, region: RegionId, offset: u64) -> impl Future<Output = (B, Result<()>)> + Send
    where
        B: IoBuf;

    /// Read API for the device.
    #[must_use]
    fn read<B>(&self, buf: B, region: RegionId, offset: u64) -> impl Future<Output = (B, Result<()>)> + Send
    where
        B: IoBufMut;

    /// Flush the device, make sure all modifications are persisted safely on the device.
    #[must_use]
    fn flush(&self, region: Option<RegionId>) -> impl Future<Output = Result<()>> + Send;
}

/// Device extend interfaces.
pub trait DevExt: Dev {
    /// Get the align size of the device.
    fn align(&self) -> usize {
        PAGE
    }

    /// Get the region count of the device.
    fn regions(&self) -> usize {
        self.capacity() / self.region_size()
    }
}

impl<T> DevExt for T where T: Dev {}

#[derive(Debug, Clone)]
pub enum DeviceConfig {
    DirectFile(DirectFileDeviceConfig),
    DirectFs(DirectFsDeviceConfig),
}

impl From<DirectFileDeviceOptions> for DeviceConfig {
    fn from(options: DirectFileDeviceOptions) -> Self {
        Self::DirectFile(options.into())
    }
}

impl From<DirectFsDeviceOptions> for DeviceConfig {
    fn from(options: DirectFsDeviceOptions) -> Self {
        Self::DirectFs(options.into())
    }
}

#[derive(Debug, Clone)]
pub enum Device {
    DirectFile(DirectFileDevice),
    DirectFs(DirectFsDevice),
}

impl Dev for Device {
    type Config = DeviceConfig;

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

    async fn open(options: Self::Config, runtime: Runtime) -> Result<Self> {
        match options {
            DeviceConfig::DirectFile(opts) => Ok(Self::DirectFile(DirectFileDevice::open(opts, runtime).await?)),
            DeviceConfig::DirectFs(opts) => Ok(Self::DirectFs(DirectFsDevice::open(opts, runtime).await?)),
        }
    }

    fn throttle(&self) -> &Throttle {
        match self {
            Device::DirectFile(dev) => dev.throttle(),
            Device::DirectFs(dev) => dev.throttle(),
        }
    }

    async fn write<B>(&self, buf: B, region: RegionId, offset: u64) -> (B, Result<()>)
    where
        B: IoBuf,
    {
        match self {
            Device::DirectFile(dev) => dev.write(buf, region, offset).await,
            Device::DirectFs(dev) => dev.write(buf, region, offset).await,
        }
    }

    async fn read<B>(&self, buf: B, region: RegionId, offset: u64) -> (B, Result<()>)
    where
        B: IoBufMut,
    {
        match self {
            Device::DirectFile(dev) => dev.read(buf, region, offset).await,
            Device::DirectFs(dev) => dev.read(buf, region, offset).await,
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
