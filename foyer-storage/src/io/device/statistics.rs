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
    sync::atomic::{AtomicIsize, AtomicUsize, Ordering},
    time::Duration,
};

#[cfg(not(miri))]
use fastant::{Atomic, Instant};
#[cfg(miri)]
use std::{sync::RwLock, time::Instant};

use crate::Throttle;

#[cfg(not(miri))]
type AtomicInstant = Atomic;
#[cfg(miri)]
type AtomicInstant = RwLock<Instant>;

#[derive(Debug)]
struct Metric {
    value: AtomicUsize,
    throttle: f64,
    quota: AtomicIsize,
    update: AtomicInstant,
}

impl Metric {
    fn new(throttle: f64) -> Self {
        Self {
            value: AtomicUsize::new(0),
            throttle,
            quota: AtomicIsize::new(0),
            #[cfg(not(miri))]
            update: Atomic::new(Instant::now()),
            #[cfg(miri)]
            update: RwLock::new(Instant::now()),
        }
    }

    fn load(&self) -> usize {
        self.value.load(Ordering::Relaxed)
    }

    fn record(&self, value: usize) {
        self.value.fetch_add(value, Ordering::Relaxed);
        // If throttle is set, update the quota.
        if self.throttle != 0.0 {
            self.quota.fetch_sub(value as _, Ordering::Relaxed);
        }
    }

    /// Get the nearest time to retry to check if there is quota.
    ///
    /// Return `Duration::ZERO` if no need to wait.
    fn throttle(&self) -> Duration {
        // If throttle is not set, no need to wait.
        if self.throttle == 0.0 {
            return Duration::ZERO;
        }

        let now = Instant::now();

        #[cfg(not(miri))]
        let update = self.update.load(Ordering::Relaxed);
        #[cfg(miri)]
        let update = *self.update.read().unwrap();

        let dur = now.duration_since(update).as_secs_f64();
        let fill = dur * self.throttle;

        let quota = f64::min(self.throttle, self.quota.load(Ordering::Relaxed) as f64 + fill);

        #[cfg(not(miri))]
        self.update.store(now, Ordering::Relaxed);
        #[cfg(miri)]
        {
            *self.update.write().unwrap() = now;
        }

        self.quota.store(quota as isize, Ordering::Relaxed);

        if quota >= 0.0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64(-quota / self.throttle)
        }
    }
}

/// The statistics of the device.
#[derive(Debug)]
pub struct Statistics {
    throttle: Throttle,

    disk_write_bytes: Metric,
    disk_read_bytes: Metric,
    disk_write_ios: Metric,
    disk_read_ios: Metric,
}

impl Statistics {
    /// Create a new statistics.
    pub fn new(throttle: Throttle) -> Self {
        let disk_write_bytes = Metric::new(throttle.write_throughput.map(|v| v.get()).unwrap_or_default() as f64);
        let disk_read_bytes = Metric::new(throttle.read_throughput.map(|v| v.get()).unwrap_or_default() as f64);
        let disk_write_ios = Metric::new(throttle.write_iops.map(|v| v.get()).unwrap_or_default() as f64);
        let disk_read_ios = Metric::new(throttle.read_iops.map(|v| v.get()).unwrap_or_default() as f64);
        Self {
            throttle,
            disk_write_bytes,
            disk_read_bytes,
            disk_write_ios,
            disk_read_ios,
        }
    }

    /// Get the disk cache written bytes.
    pub fn disk_write_bytes(&self) -> usize {
        self.disk_write_bytes.load()
    }

    /// Get the disk cache read bytes.
    pub fn disk_read_bytes(&self) -> usize {
        self.disk_read_bytes.load()
    }

    /// Get the disk cache written ios.
    pub fn disk_write_ios(&self) -> usize {
        self.disk_write_ios.load()
    }

    /// Get the disk cache read bytes.
    pub fn disk_read_ios(&self) -> usize {
        self.disk_read_ios.load()
    }

    /// Record the write IO and update the statistics.
    pub fn record_disk_write(&self, bytes: usize) {
        self.disk_write_bytes.record(bytes);
        self.disk_write_ios.record(self.throttle.iops_counter.count(bytes));
    }

    /// Record the read IO and update the statistics.
    pub fn record_disk_read(&self, bytes: usize) {
        self.disk_read_bytes.record(bytes);
        self.disk_read_ios.record(self.throttle.iops_counter.count(bytes));
    }

    /// Get the nearest time to retry to check if there is quota for read ops.
    ///
    /// Return `Duration::ZERO` if no need to wait.
    pub fn read_throttle(&self) -> Duration {
        std::cmp::max(self.disk_read_bytes.throttle(), self.disk_read_ios.throttle())
    }

    /// Get the nearest time to retry to check if there is quota for write ops.
    ///
    /// Return `Duration::ZERO` if no need to wait.
    pub fn write_throttle(&self) -> Duration {
        std::cmp::max(self.disk_write_bytes.throttle(), self.disk_write_ios.throttle())
    }

    /// Check if the read ops are throttled.
    pub fn is_read_throttled(&self) -> bool {
        self.read_throttle() > Duration::ZERO
    }

    /// Check if the write ops are throttled.
    pub fn is_write_throttled(&self) -> bool {
        self.write_throttle() > Duration::ZERO
    }

    /// Get the throttle configuration.
    pub fn throttle(&self) -> &Throttle {
        &self.throttle
    }
}
