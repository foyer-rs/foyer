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

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::IopsCounter;

/// The statistics of the disk cache, which is used by the pickers.
#[derive(Debug)]
pub struct Statistics {
    iops_counter: IopsCounter,

    cache_write_bytes: AtomicUsize,
    cache_read_bytes: AtomicUsize,
    cache_write_ios: AtomicUsize,
    cache_read_ios: AtomicUsize,
}

impl Statistics {
    /// Create a new statistics.
    pub fn new(iops_counter: IopsCounter) -> Self {
        Self {
            iops_counter,
            cache_write_bytes: AtomicUsize::new(0),
            cache_read_bytes: AtomicUsize::new(0),
            cache_write_ios: AtomicUsize::new(0),
            cache_read_ios: AtomicUsize::new(0),
        }
    }

    /// Get the disk cache written bytes.
    pub fn cache_write_bytes(&self) -> usize {
        self.cache_write_bytes.load(Ordering::Relaxed)
    }

    /// Get the disk cache read bytes.
    pub fn cache_read_bytes(&self) -> usize {
        self.cache_read_bytes.load(Ordering::Relaxed)
    }

    /// Get the disk cache written ios.
    pub fn cache_write_ios(&self) -> usize {
        self.cache_write_ios.load(Ordering::Relaxed)
    }

    /// Get the disk cache read bytes.
    pub fn cache_read_ios(&self) -> usize {
        self.cache_read_ios.load(Ordering::Relaxed)
    }

    /// Record the write IO and update the statistics.
    pub fn record_write_io(&self, bytes: usize) {
        self.cache_write_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.cache_write_ios
            .fetch_add(self.iops_counter.count(bytes), Ordering::Relaxed);
    }

    /// Record the read IO and update the statistics.
    pub fn record_read_io(&self, bytes: usize) {
        self.cache_read_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.cache_read_ios
            .fetch_add(self.iops_counter.count(bytes), Ordering::Relaxed);
    }
}
