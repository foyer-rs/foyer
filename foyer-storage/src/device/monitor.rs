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
//  limitations under the License.use std::marker::PhantomData;

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use foyer_common::bits;

use crate::{error::Result, Device, DeviceExt, DirectFileDevice};

use super::{IoBuffer, RegionId};

#[derive(Debug, Default)]
pub struct DeviceStats {
    pub read_ios: AtomicUsize,
    pub read_bytes: AtomicUsize,

    pub write_ios: AtomicUsize,
    pub write_bytes: AtomicUsize,

    pub flush_ios: AtomicUsize,
}

#[derive(Debug, Clone)]
pub struct Monitored<D>
where
    D: Device,
{
    device: D,
    stats: Arc<DeviceStats>,
}

impl<D> Device for Monitored<D>
where
    D: Device,
{
    type Options = D::Options;

    fn capacity(&self) -> usize {
        self.device.capacity()
    }

    fn region_size(&self) -> usize {
        self.device.region_size()
    }

    async fn open(options: Self::Options) -> Result<Self> {
        let device = D::open(options).await?;
        Ok(Self {
            device,
            stats: Arc::default(),
        })
    }

    async fn write(&self, buf: IoBuffer, region: RegionId, offset: u64) -> Result<()> {
        let bytes = bits::align_up(self.align(), buf.len());
        self.stats.write_ios.fetch_add(1, Ordering::Relaxed);
        self.stats.write_bytes.fetch_add(bytes, Ordering::Relaxed);

        self.device.write(buf, region, offset).await
    }

    async fn read(&self, region: RegionId, offset: u64, len: usize) -> Result<IoBuffer> {
        let bytes = bits::align_up(self.align(), len);
        self.stats.read_ios.fetch_add(1, Ordering::Relaxed);
        self.stats.read_bytes.fetch_add(bytes, Ordering::Relaxed);

        self.device.read(region, offset, len).await
    }

    async fn flush(&self, region: Option<RegionId>) -> Result<()> {
        self.stats.flush_ios.fetch_add(1, Ordering::Relaxed);

        self.device.flush(region).await
    }
}

impl Monitored<DirectFileDevice> {
    pub async fn pwrite(&self, buf: IoBuffer, offset: u64) -> Result<()> {
        let bytes = bits::align_up(self.align(), buf.len());
        self.stats.write_ios.fetch_add(1, Ordering::Relaxed);
        self.stats.write_bytes.fetch_add(bytes, Ordering::Relaxed);

        self.device.pwrite(buf, offset).await
    }
    pub async fn pread(&self, offset: u64, len: usize) -> Result<IoBuffer> {
        let bytes = bits::align_up(self.align(), len);
        self.stats.read_ios.fetch_add(1, Ordering::Relaxed);
        self.stats.read_bytes.fetch_add(bytes, Ordering::Relaxed);

        self.device.pread(offset, len).await
    }
}

impl<D> Monitored<D>
where
    D: Device,
{
    pub fn stat(&self) -> &Arc<DeviceStats> {
        &self.stats
    }
}
