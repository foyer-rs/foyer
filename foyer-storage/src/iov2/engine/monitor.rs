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

use std::{fmt::Debug, sync::Arc, time::Instant};

use foyer_common::metrics::Metrics;

use crate::iov2::engine::{IoEngine, IoHandle, IoOp, IoUnit};

#[derive(Debug)]
struct Inner {
    io_engine: Arc<dyn IoEngine>,
    metrics: Arc<Metrics>,
}

#[derive(Clone)]
pub struct MonitoredIoEngine {
    inner: Arc<Inner>,
}

impl MonitoredIoEngine {
    pub fn new(io_engine: Arc<dyn IoEngine>, metrics: Arc<Metrics>) -> Arc<Self> {
        let inner = Inner { io_engine, metrics };
        Arc::new(Self { inner: Arc::new(inner) })
    }
}

impl Debug for MonitoredIoEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MonitoredIoEngine")
            .field("engine", &self.inner.io_engine)
            .finish()
    }
}

impl IoEngine for MonitoredIoEngine {
    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::storage::io::engine::monitor::submit")
    )]
    fn submit(&self, unit: IoUnit) -> IoHandle {
        let now = Instant::now();

        let bytes = unit.buf.len();
        let op = unit.op;

        let statistics = unit.statistics.clone();
        let metrics = self.inner.metrics.clone();

        let handle = self.inner.io_engine.submit(unit);
        handle.with_callback(move || match op {
            IoOp::Read => {
                statistics.record_disk_read(bytes);
                metrics.storage_disk_read.increase(1);
                metrics.storage_disk_read_bytes.increase(bytes as u64);
                metrics.storage_disk_read_duration.record(now.elapsed().as_secs_f64());
            }
            IoOp::Write => {
                statistics.record_disk_write(bytes);
                metrics.storage_disk_write.increase(1);
                metrics.storage_disk_write_bytes.increase(bytes as u64);
                metrics.storage_disk_write_duration.record(now.elapsed().as_secs_f64());
            }
        })
    }
}
