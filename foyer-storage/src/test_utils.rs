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

//! Test utils for the `foyer-storage` crate.

use std::{
    collections::HashSet,
    fmt::Debug,
    future::ready,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use mea::oneshot;
use parking_lot::Mutex;

use crate::{StorageFilterCondition, StorageFilterResult, io::device::statistics::Statistics};

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
/// This is supposed to be used as an admission filter condition.
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

/// A wrapper of atomic bool.
#[derive(Debug, Clone, Default)]
pub struct Switch {
    hold: Arc<AtomicBool>,
}

impl Switch {
    /// Check if the switch is on.
    pub fn is_on(&self) -> bool {
        self.hold.load(Ordering::Relaxed)
    }

    /// Turn on the switch
    pub fn on(&self) {
        self.hold.store(true, Ordering::Relaxed);
    }

    /// Turn off the switch
    pub fn off(&self) {
        self.hold.store(false, Ordering::Relaxed);
    }
}

impl StorageFilterCondition for Switch {
    fn filter(&self, _: &Arc<Statistics>, _: u64, _: usize) -> StorageFilterResult {
        if self.is_on() {
            StorageFilterResult::Admit
        } else {
            StorageFilterResult::Reject
        }
    }
}

#[derive(Debug, Default)]
struct HolderInner {
    holdees: Vec<oneshot::Sender<()>>,
    holding: bool,
}

/// A holder that can hold and unhold.
#[derive(Debug, Clone, Default)]
pub struct Holder {
    inner: Arc<Mutex<HolderInner>>,
}

impl Holder {
    /// Check if it is being held.
    pub fn is_held(&self) -> bool {
        self.inner.lock().holding
    }

    /// Hold the holder. Block all following waits.
    pub fn hold(&self) {
        self.inner.lock().holding = true;
    }

    /// Unhold the holder. Release all previous waits.
    pub fn unhold(&self) {
        let mut inner = self.inner.lock();
        inner.holding = false;
        for tx in inner.holdees.drain(..) {
            let _ = tx.send(());
        }
    }

    /// Wait if it is being held.
    pub fn wait(&self) -> BoxFuture<'static, ()> {
        let mut inner = self.inner.lock();
        if !inner.holding {
            return ready(()).boxed();
        }
        let (tx, rx) = oneshot::channel();
        inner.holdees.push(tx);
        async move {
            let _ = rx.await;
        }
        .boxed()
    }
}
