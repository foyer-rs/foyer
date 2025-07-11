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

use std::{fmt::Debug, sync::Arc, time::Instant};

use foyer_common::metrics::Metrics;

use crate::{
    io::{
        bytes::{IoBuf, IoBufMut},
        device::RegionId,
        engine::{IoEngine, IoHandle},
    },
    IopsCounter, Statistics,
};

#[derive(Debug)]
struct Inner {
    io_engine: Arc<dyn IoEngine>,
    iops_counter: IopsCounter,
    stats: Arc<Statistics>,
    metrics: Arc<Metrics>,
}

#[derive(Clone)]
pub struct MonitoredIoEngine {
    inner: Arc<Inner>,
}

impl Debug for MonitoredIoEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MonitoredIoEngine")
            .field("engine", &self.inner.io_engine)
            .finish()
    }
}

impl IoEngine for MonitoredIoEngine {
    fn device(&self) -> &Arc<dyn crate::io::device::Device> {
        self.inner.io_engine.device()
    }

    fn read(&self, buf: Box<dyn IoBufMut>, region: RegionId, offset: u64) -> IoHandle {
        let now = Instant::now();
        let bytes = buf.len();

        let handle = self.inner.io_engine.read(buf, region, offset);

        self.inner.stats.record_disk_read(bytes);
        self.inner.metrics.storage_disk_read.increase(1);
        self.inner.metrics.storage_disk_read_bytes.increase(bytes as u64);
        self.inner
            .metrics
            .storage_disk_read_duration
            .record(now.elapsed().as_secs_f64());

        handle
    }

    fn write(&self, buf: Box<dyn IoBuf>, region: RegionId, offset: u64) -> IoHandle {
        let now = Instant::now();
        let bytes = buf.len();

        let handle = self.inner.io_engine.write(buf, region, offset);

        self.inner.stats.record_disk_write(bytes);
        self.inner.metrics.storage_disk_write.increase(1);
        self.inner.metrics.storage_disk_write_bytes.increase(bytes as u64);
        self.inner
            .metrics
            .storage_disk_write_duration
            .record(now.elapsed().as_secs_f64());

        handle
    }
}
