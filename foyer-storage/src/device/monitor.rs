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
    fmt::Debug,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};

use foyer_common::metrics::Metrics;

use super::{RegionId, Throttle};
use crate::{
    error::Result,
    io::{IoBuf, IoBufMut},
    Dev, DirectFileDevice, Runtime,
};

/// The statistics information of the device.
#[derive(Debug, Default)]
pub struct DeviceStats {
    /// The read io count of the device.
    pub read_ios: AtomicUsize,
    /// The read bytes of the device.
    pub read_bytes: AtomicUsize,

    /// The write io count of the device.
    pub write_ios: AtomicUsize,
    /// The write bytes of the device.
    pub write_bytes: AtomicUsize,

    /// The flush io count of the device.
    pub flush_ios: AtomicUsize,
}

#[derive(Clone)]
pub struct MonitoredConfig<D>
where
    D: Dev,
{
    pub config: D::Config,
    pub metrics: Arc<Metrics>,
}

impl<D> Debug for MonitoredConfig<D>
where
    D: Dev,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MonitoredOptions")
            .field("options", &self.config)
            .field("metrics", &self.metrics)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct Monitored<D>
where
    D: Dev,
{
    device: D,
    stats: Arc<DeviceStats>,
    metrics: Arc<Metrics>,
}

impl<D> Monitored<D>
where
    D: Dev,
{
    async fn open(options: MonitoredConfig<D>, runtime: Runtime) -> Result<Self> {
        let device = D::open(options.config, runtime).await?;
        Ok(Self {
            device,
            stats: Arc::default(),
            metrics: options.metrics,
        })
    }

    #[fastrace::trace(name = "foyer::storage::device::monitor::write")]
    async fn write<B>(&self, buf: B, region: RegionId, offset: u64) -> (B, Result<()>)
    where
        B: IoBuf,
    {
        let now = Instant::now();

        let bytes = buf.len();
        self.stats.write_ios.fetch_add(1, Ordering::Relaxed);
        self.stats.write_bytes.fetch_add(bytes, Ordering::Relaxed);

        let res = self.device.write(buf, region, offset).await;

        self.metrics.storage_disk_write.increase(1);
        self.metrics.storage_disk_write_bytes.increase(bytes as u64);
        self.metrics
            .storage_disk_write_duration
            .record(now.elapsed().as_secs_f64());

        res
    }

    #[fastrace::trace(name = "foyer::storage::device::monitor::read")]
    async fn read<B>(&self, buf: B, region: RegionId, offset: u64) -> (B, Result<()>)
    where
        B: IoBufMut,
    {
        let now = Instant::now();

        let bytes = buf.len();
        self.stats.read_ios.fetch_add(1, Ordering::Relaxed);
        self.stats.read_bytes.fetch_add(bytes, Ordering::Relaxed);

        let res = self.device.read(buf, region, offset).await;

        self.metrics.storage_disk_read.increase(1);
        self.metrics.storage_disk_read_bytes.increase(bytes as u64);
        self.metrics
            .storage_disk_read_duration
            .record(now.elapsed().as_secs_f64());

        res
    }

    #[fastrace::trace(name = "foyer::storage::device::monitor::flush")]
    async fn flush(&self, region: Option<RegionId>) -> Result<()> {
        let now = Instant::now();

        self.stats.flush_ios.fetch_add(1, Ordering::Relaxed);

        let res = self.device.flush(region).await;

        self.metrics.storage_disk_flush.increase(1);
        self.metrics
            .storage_disk_flush_duration
            .record(now.elapsed().as_secs_f64());

        res
    }
}

impl<D> Dev for Monitored<D>
where
    D: Dev,
{
    type Config = MonitoredConfig<D>;

    fn capacity(&self) -> usize {
        self.device.capacity()
    }

    fn region_size(&self) -> usize {
        self.device.region_size()
    }

    fn throttle(&self) -> &Throttle {
        self.device.throttle()
    }

    async fn open(config: Self::Config, runtime: Runtime) -> Result<Self> {
        Self::open(config, runtime).await
    }

    async fn write<B>(&self, buf: B, region: RegionId, offset: u64) -> (B, Result<()>)
    where
        B: IoBuf,
    {
        self.write(buf, region, offset).await
    }

    async fn read<B>(&self, buf: B, region: RegionId, offset: u64) -> (B, Result<()>)
    where
        B: IoBufMut,
    {
        self.read(buf, region, offset).await
    }

    async fn flush(&self, region: Option<RegionId>) -> Result<()> {
        self.flush(region).await
    }
}

impl Monitored<DirectFileDevice> {
    #[fastrace::trace(name = "foyer::storage::device::monitor::pwrite")]
    pub async fn pwrite<B>(&self, buf: B, offset: u64) -> (B, Result<()>)
    where
        B: IoBuf,
    {
        let now = Instant::now();

        let bytes = buf.len();
        self.stats.write_ios.fetch_add(1, Ordering::Relaxed);
        self.stats.write_bytes.fetch_add(bytes, Ordering::Relaxed);

        let res = self.device.pwrite(buf, offset).await;

        self.metrics.storage_disk_write.increase(1);
        self.metrics.storage_disk_write_bytes.increase(bytes as u64);
        self.metrics
            .storage_disk_write_duration
            .record(now.elapsed().as_secs_f64());

        res
    }

    #[fastrace::trace(name = "foyer::storage::device::monitor::pread")]
    pub async fn pread<B>(&self, buf: B, offset: u64) -> (B, Result<()>)
    where
        B: IoBufMut,
    {
        let now = Instant::now();

        let bytes = buf.len();
        self.stats.read_ios.fetch_add(1, Ordering::Relaxed);
        self.stats.read_bytes.fetch_add(bytes, Ordering::Relaxed);

        let res = self.device.pread(buf, offset).await;

        self.metrics.storage_disk_read.increase(1);
        self.metrics.storage_disk_read_bytes.increase(bytes as u64);
        self.metrics
            .storage_disk_read_duration
            .record(now.elapsed().as_secs_f64());

        res
    }
}

impl<D> Monitored<D>
where
    D: Dev,
{
    pub fn stat(&self) -> &Arc<DeviceStats> {
        &self.stats
    }

    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }
}
