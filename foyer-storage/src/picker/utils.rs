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
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use super::{AdmissionPicker, Pick, ReinsertionPicker};
use crate::{io::throttle::IoThrottler, statistics::Statistics};

/// Only admit on all chained admission pickers pick.
#[derive(Debug, Default, Clone)]
pub struct ChainedAdmissionPicker {
    pickers: Arc<Vec<Arc<dyn AdmissionPicker>>>,
}

impl AdmissionPicker for ChainedAdmissionPicker {
    fn pick(&self, stats: &Arc<Statistics>, hash: u64) -> Pick {
        let mut duration = Duration::ZERO;
        for picker in self.pickers.iter() {
            match picker.pick(stats, hash) {
                Pick::Admit => {}
                Pick::Reject => return Pick::Reject,
                Pick::Throttled(dur) => duration += dur,
            }
        }
        if duration.is_zero() {
            Pick::Admit
        } else {
            Pick::Throttled(duration)
        }
    }
}

/// A builder for [`ChainedAdmissionPicker`].
#[derive(Debug, Default)]
pub struct ChainedAdmissionPickerBuilder {
    pickers: Vec<Arc<dyn AdmissionPicker>>,
}

impl ChainedAdmissionPickerBuilder {
    /// Chain a new admission picker.
    pub fn chain(mut self, picker: Arc<dyn AdmissionPicker>) -> Self {
        self.pickers.push(picker);
        self
    }

    /// Build the chained admission picker.
    pub fn build(self) -> ChainedAdmissionPicker {
        ChainedAdmissionPicker {
            pickers: Arc::new(self.pickers),
        }
    }
}

/// A picker that always returns `true`.
#[derive(Debug, Default)]
pub struct AdmitAllPicker;

impl AdmissionPicker for AdmitAllPicker {
    fn pick(&self, _: &Arc<Statistics>, _: u64) -> Pick {
        Pick::Admit
    }
}

impl ReinsertionPicker for AdmitAllPicker {
    fn pick(&self, _: &Arc<Statistics>, _: u64) -> Pick {
        Pick::Admit
    }
}

/// A picker that always returns `false`.
#[derive(Debug, Default)]
pub struct RejectAllPicker;

impl AdmissionPicker for RejectAllPicker {
    fn pick(&self, _: &Arc<Statistics>, _: u64) -> Pick {
        Pick::Reject
    }
}

impl ReinsertionPicker for RejectAllPicker {
    fn pick(&self, _: &Arc<Statistics>, _: u64) -> Pick {
        Pick::Reject
    }
}

#[derive(Debug)]
struct IoThrottlerPickerInner {
    throttler: IoThrottler,
    bytes_last: AtomicUsize,
    ios_last: AtomicUsize,
    target: IoThrottlerTarget,
}

/// Target of the io throttler.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum IoThrottlerTarget {
    /// Count read io for io throttling.
    Read,
    /// Count write io for io throttling.
    Write,
    /// Count read and write io for io throttling.
    ReadWrite,
}

/// A picker that picks based on the disk statistics and the given throttle args.
///
/// NOTE: This picker is automatically applied if the device throttle is set.
///
/// Please use set the `throttle` in the device options instead use this picker directly.
/// Unless you know what you are doing. :D
#[derive(Debug, Clone)]
pub struct IoThrottlerPicker {
    inner: Arc<IoThrottlerPickerInner>,
}

impl IoThrottlerPicker {
    /// Create a rate limit picker with the given rate limit.
    ///
    /// Note: `None` stands for unlimited.
    pub fn new(target: IoThrottlerTarget, throughput: Option<NonZeroUsize>, iops: Option<NonZeroUsize>) -> Self {
        let inner = IoThrottlerPickerInner {
            throttler: IoThrottler::new(throughput, iops),
            bytes_last: AtomicUsize::default(),
            ios_last: AtomicUsize::default(),
            target,
        };
        Self { inner: Arc::new(inner) }
    }

    fn pick_inner(&self, stats: &Arc<Statistics>) -> Pick {
        let duration = self.inner.throttler.probe();

        let bytes_current = match self.inner.target {
            IoThrottlerTarget::Read => stats.disk_read_bytes(),
            IoThrottlerTarget::Write => stats.disk_write_bytes(),
            IoThrottlerTarget::ReadWrite => stats.disk_read_bytes() + stats.disk_write_bytes(),
        };
        let ios_current = match self.inner.target {
            IoThrottlerTarget::Read => stats.disk_read_ios(),
            IoThrottlerTarget::Write => stats.disk_write_ios(),
            IoThrottlerTarget::ReadWrite => stats.disk_read_ios() + stats.disk_write_ios(),
        };

        let bytes_last = self.inner.bytes_last.load(Ordering::Relaxed);
        let ios_last = self.inner.ios_last.load(Ordering::Relaxed);

        let bytes_delta = bytes_current.saturating_sub(bytes_last);
        let ios_delta = ios_current.saturating_sub(ios_last);

        self.inner.bytes_last.store(bytes_current, Ordering::Relaxed);
        self.inner.ios_last.store(ios_current, Ordering::Relaxed);

        self.inner.throttler.reduce(bytes_delta as f64, ios_delta as f64);

        if duration.is_zero() {
            Pick::Admit
        } else {
            Pick::Throttled(duration)
        }
    }
}

impl AdmissionPicker for IoThrottlerPicker {
    fn pick(&self, stats: &Arc<Statistics>, _: u64) -> Pick {
        self.pick_inner(stats)
    }
}
