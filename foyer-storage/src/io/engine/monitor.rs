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

use std::{
    fmt::Debug,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use foyer_common::metrics::Metrics;
use futures_util::FutureExt;

use crate::io::{
    bytes::{IoBuf, IoBufMut},
    device::Partition,
    engine::{IoEngine, IoHandle},
};

#[derive(Debug)]
struct Inner {
    io_engine: Arc<dyn IoEngine>,
    metrics: Arc<Metrics>,
    inflight: AtomicU64,
}

const WRITE_INFLIGHT_ONE: u64 = 1 << 32;
const READ_INFLIGHT_MASK: u64 = WRITE_INFLIGHT_ONE - 1;

impl Inner {
    fn start_read(&self) -> bool {
        let previous = self.inflight.fetch_add(1, Ordering::SeqCst);
        previous >= WRITE_INFLIGHT_ONE
    }

    fn finish_read(&self) {
        self.inflight.fetch_sub(1, Ordering::SeqCst);
    }

    fn start_write(&self) -> bool {
        let previous = self.inflight.fetch_add(WRITE_INFLIGHT_ONE, Ordering::SeqCst);
        previous & READ_INFLIGHT_MASK > 0
    }

    fn finish_write(&self) {
        self.inflight.fetch_sub(WRITE_INFLIGHT_ONE, Ordering::SeqCst);
    }

    #[cfg(test)]
    fn read_inflight(&self) -> u64 {
        self.inflight.load(Ordering::SeqCst) & READ_INFLIGHT_MASK
    }

    #[cfg(test)]
    fn write_inflight(&self) -> u64 {
        self.inflight.load(Ordering::SeqCst) >> 32
    }
}

#[derive(Debug, Clone, Copy)]
enum IoDirection {
    Read,
    Write,
}

struct InflightGuard {
    inner: Arc<Inner>,
    direction: IoDirection,
}

impl InflightGuard {
    fn new(inner: Arc<Inner>, direction: IoDirection) -> Self {
        let overlaps_opposite = match direction {
            IoDirection::Read => inner.start_read(),
            IoDirection::Write => inner.start_write(),
        };

        match direction {
            IoDirection::Read => {
                inner.metrics.storage_disk_read_inflight.increase(1);
                if overlaps_opposite {
                    inner.metrics.storage_disk_read_overlap.increase(1);
                }
            }
            IoDirection::Write => {
                inner.metrics.storage_disk_write_inflight.increase(1);
                if overlaps_opposite {
                    inner.metrics.storage_disk_write_overlap.increase(1);
                }
            }
        }
        inner.metrics.storage_disk_combined_inflight.increase(1);

        Self { inner, direction }
    }
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        match self.direction {
            IoDirection::Read => {
                self.inner.finish_read();
                self.inner.metrics.storage_disk_read_inflight.decrease(1);
            }
            IoDirection::Write => {
                self.inner.finish_write();
                self.inner.metrics.storage_disk_write_inflight.decrease(1);
            }
        }
        self.inner.metrics.storage_disk_combined_inflight.decrease(1);
    }
}

#[derive(Clone)]
pub struct MonitoredIoEngine {
    inner: Arc<Inner>,
}

impl MonitoredIoEngine {
    pub fn new(io_engine: Arc<dyn IoEngine>, metrics: Arc<Metrics>) -> Arc<Self> {
        let inner = Inner {
            io_engine,
            metrics,
            inflight: AtomicU64::new(0),
        };
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
        fastrace::trace(name = "foyer::storage::io::engine::monitor::read")
    )]
    fn read(&self, buf: Box<dyn IoBufMut>, partition: &dyn Partition, offset: u64) -> IoHandle {
        let now = Instant::now();
        let bytes = buf.len();

        let statistics = partition.statistics().clone();
        let inner = self.inner.clone();
        let metrics = inner.metrics.clone();
        let handle = self.inner.io_engine.read(buf, partition, offset);
        let inflight = InflightGuard::new(inner, IoDirection::Read);

        async move {
            let result = handle.await;
            statistics.record_disk_read(bytes);
            metrics.storage_disk_read.increase(1);
            metrics.storage_disk_read_bytes.increase(bytes as u64);
            metrics.storage_disk_read_duration.record(now.elapsed().as_secs_f64());
            drop(inflight);
            result
        }
        .boxed()
        .into()
    }

    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::storage::io::engine::monitor::write")
    )]
    fn write(&self, buf: Box<dyn IoBuf>, partition: &dyn Partition, offset: u64) -> IoHandle {
        let now = Instant::now();
        let bytes = buf.len();

        let statistics = partition.statistics().clone();
        let inner = self.inner.clone();
        let metrics = inner.metrics.clone();
        let handle = self.inner.io_engine.write(buf, partition, offset);
        let inflight = InflightGuard::new(inner, IoDirection::Write);

        async move {
            let result = handle.await;
            statistics.record_disk_write(bytes);
            metrics.storage_disk_write.increase(1);
            metrics.storage_disk_write_bytes.increase(bytes as u64);
            metrics.storage_disk_write_duration.record(now.elapsed().as_secs_f64());
            drop(inflight);
            result
        }
        .boxed()
        .into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{bytes::IoSliceMut, device::noop::NoopPartition, engine::noop::NoopIoEngine};

    #[tokio::test]
    async fn test_inflight_io_lifecycle_and_overlap() {
        let engine = MonitoredIoEngine::new(Arc::new(NoopIoEngine), Arc::new(Metrics::noop()));
        let partition = NoopPartition::default();

        let read = engine.read(Box::new(IoSliceMut::new(4096)), &partition, 0);
        assert_eq!(engine.inner.read_inflight(), 1);
        assert_eq!(engine.inner.write_inflight(), 0);

        let write = engine.write(Box::new(IoSliceMut::new(4096)), &partition, 0);
        assert_eq!(engine.inner.read_inflight(), 1);
        assert_eq!(engine.inner.write_inflight(), 1);

        read.await.1.unwrap();
        assert_eq!(engine.inner.read_inflight(), 0);
        assert_eq!(engine.inner.write_inflight(), 1);

        write.await.1.unwrap();
        assert_eq!(engine.inner.read_inflight(), 0);
        assert_eq!(engine.inner.write_inflight(), 0);
    }

    #[tokio::test]
    async fn test_dropped_io_handle_releases_inflight_state() {
        let engine = MonitoredIoEngine::new(Arc::new(NoopIoEngine), Arc::new(Metrics::noop()));
        let partition = NoopPartition::default();

        let read = engine.read(Box::new(IoSliceMut::new(4096)), &partition, 0);
        assert_eq!(engine.inner.read_inflight(), 1);
        drop(read);
        assert_eq!(engine.inner.read_inflight(), 0);
    }
}
