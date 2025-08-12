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

//! Test utils for the `foyer-storage` crate.

use std::{
    collections::HashSet,
    fmt::Debug,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use parking_lot::Mutex;

use crate::{io::device::statistics::Statistics, StorageFilterCondition, StorageFilterResult};

/// A picker that only admits hash from the given list.
#[derive(Debug)]
pub struct Biased {
    admits: HashSet<u64>,
}

impl Biased {
    /// Create a biased picker with the given admit list.
    pub fn new(admits: impl IntoIterator<Item = u64>) -> Self {
        Self {
            admits: admits.into_iter().collect(),
        }
    }
}

impl StorageFilterCondition for Biased {
    fn filter(&self, _: &Arc<Statistics>, hash: u64, _: usize) -> StorageFilterResult {
        if self.admits.contains(&hash) {
            StorageFilterResult::Admit
        } else {
            StorageFilterResult::Reject
        }
    }
}

/// The record entry for admission and eviction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Record {
    /// Admission record entry hash.
    Admit(u64),
    /// Eviction record entry hash.
    Evict(u64),
}

#[derive(Debug, Default)]
struct RecorderInner {
    records: Mutex<Vec<Record>>,
}

impl RecorderInner {
    /// Dump the record entries of the recorder.
    fn dump(&self) -> Vec<Record> {
        self.records.lock().clone()
    }

    /// Get the hash set of the remaining hash at the moment.
    fn remains(&self) -> HashSet<u64> {
        let records = self.dump();
        let mut res = HashSet::default();
        for record in records {
            match record {
                Record::Admit(key) => {
                    res.insert(key);
                }
                Record::Evict(key) => {
                    res.remove(&key);
                }
            }
        }
        res
    }
}

/// A recorder that records the cache entry admission of a disk cache.
///
/// This is supposed to be used as a admission filter condition.
#[derive(Debug)]
pub struct AdmitRecorder {
    inner: Arc<RecorderInner>,
}

/// A recorder that records the cache entry eviction of a disk cache.
///
/// This is supposed to be used as a reinsertion filter condition.
#[derive(Debug)]
pub struct EvictRecorder {
    inner: Arc<RecorderInner>,
}

impl StorageFilterCondition for AdmitRecorder {
    fn filter(&self, _: &Arc<Statistics>, hash: u64, _: usize) -> StorageFilterResult {
        self.inner.records.lock().push(Record::Admit(hash));
        StorageFilterResult::Admit
    }
}

impl StorageFilterCondition for EvictRecorder {
    fn filter(&self, _: &Arc<Statistics>, hash: u64, _: usize) -> StorageFilterResult {
        self.inner.records.lock().push(Record::Evict(hash));
        StorageFilterResult::Reject
    }
}

/// A recorder that records the cache entry admission and eviction of a disk cache.
///
/// [`Recorder`] should be used as both the admission picker and the reinsertion picker to record.
#[derive(Debug, Default, Clone)]
pub struct Recorder {
    inner: Arc<RecorderInner>,
}

impl Recorder {
    /// Dump the record entries of the recorder.
    pub fn dump(&self) -> Vec<Record> {
        self.inner.dump()
    }

    /// Get the hash set of the remaining hash at the moment.
    pub fn remains(&self) -> HashSet<u64> {
        self.inner.remains()
    }

    /// Get the recorder for admission.
    pub fn admission(&self) -> AdmitRecorder {
        AdmitRecorder {
            inner: self.inner.clone(),
        }
    }

    /// Get the recorder for eviction.
    pub fn eviction(&self) -> EvictRecorder {
        EvictRecorder {
            inner: self.inner.clone(),
        }
    }
}

/// A switch to throttle/unthrottle all loads.
#[derive(Debug, Clone, Default)]
pub struct LoadThrottleSwitch {
    throttled: Arc<AtomicBool>,
}

impl LoadThrottleSwitch {
    /// If all loads are throttled.
    pub fn is_throttled(&self) -> bool {
        self.throttled.load(Ordering::Relaxed)
    }

    /// Throttle all loads.
    pub fn throttle(&self) {
        self.throttled.store(true, Ordering::Relaxed);
    }

    /// Unthrottle all loads.
    pub fn unthrottle(&self) {
        self.throttled.store(false, Ordering::Relaxed);
    }
}
