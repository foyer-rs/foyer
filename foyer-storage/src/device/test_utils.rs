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

use super::{monitor::Monitored, Dev, Device, MonitoredDevice, RegionId, Throttle};
use crate::{error::Result, runtime::Runtime, IoBuf, IoBufMut};

#[derive(Debug, Clone, Default)]
pub struct NoopDevice(Throttle);

impl NoopDevice {
    pub fn monitored() -> MonitoredDevice {
        Monitored::new_for_test(Device::Noop(Self::default()))
    }
}

impl Dev for NoopDevice {
    type Config = ();

    fn capacity(&self) -> usize {
        0
    }

    fn region_size(&self) -> usize {
        0
    }

    fn throttle(&self) -> &Throttle {
        &self.0
    }

    async fn open(_: Self::Config, _: Runtime) -> Result<Self> {
        Ok(Self(Throttle::default()))
    }

    async fn write<B>(&self, buf: B, _: RegionId, _: u64) -> (B, Result<()>)
    where
        B: IoBuf,
    {
        (buf, Ok(()))
    }

    async fn read<B>(&self, buf: B, _: RegionId, _: u64) -> (B, Result<()>)
    where
        B: IoBufMut,
    {
        (buf, Ok(()))
    }

    async fn sync(&self, _: Option<super::RegionId>) -> Result<()> {
        Ok(())
    }
}
