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
//  limitations under the License.

use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    marker::PhantomData,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use foyer_common::{code::StorageKey, rated_ticket::RatedTicket};

use crate::{device::RegionId, region::RegionStats, statistics::Statistics};

use super::{AdmissionPicker, EvictionPicker, ReinsertionPicker};

pub struct AdmitAllPicker<K>(PhantomData<K>)
where
    K: StorageKey;

impl<K> Debug for AdmitAllPicker<K>
where
    K: StorageKey,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("AdmitAllPicker").finish()
    }
}

impl<K> Default for AdmitAllPicker<K>
where
    K: StorageKey,
{
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<K> AdmissionPicker for AdmitAllPicker<K>
where
    K: StorageKey,
{
    type Key = K;

    fn pick(&self, _: &Arc<Statistics>, _: &Self::Key) -> bool {
        true
    }
}

impl<K> ReinsertionPicker for AdmitAllPicker<K>
where
    K: StorageKey,
{
    type Key = K;

    fn pick(&self, _: &Arc<Statistics>, _: &Self::Key) -> bool {
        true
    }
}

pub struct RejectAllPicker<K>(PhantomData<K>)
where
    K: Send + Sync + 'static;

impl<K> Debug for RejectAllPicker<K>
where
    K: Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("RejectAllPicker").finish()
    }
}

impl<K> Default for RejectAllPicker<K>
where
    K: Send + Sync + 'static,
{
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<K> AdmissionPicker for RejectAllPicker<K>
where
    K: Send + Sync + 'static,
{
    type Key = K;

    fn pick(&self, _: &Arc<Statistics>, _: &Self::Key) -> bool {
        false
    }
}

impl<K> ReinsertionPicker for RejectAllPicker<K>
where
    K: Send + Sync + 'static,
{
    type Key = K;

    fn pick(&self, _: &Arc<Statistics>, _: &Self::Key) -> bool {
        false
    }
}

pub struct RateLimitPicker<K>
where
    K: StorageKey,
{
    inner: RatedTicket,
    last: AtomicUsize,

    _marker: PhantomData<K>,
}

impl<K> Debug for RateLimitPicker<K>
where
    K: StorageKey,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateLimitPicker")
            .field("inner", &self.inner)
            .field("last", &self.last)
            .finish()
    }
}

impl<K> RateLimitPicker<K>
where
    K: StorageKey,
{
    pub fn new(rate: usize) -> Self {
        Self {
            inner: RatedTicket::new(rate as f64),
            last: AtomicUsize::default(),
            _marker: PhantomData,
        }
    }

    fn pick_inner(&self, stats: &Arc<Statistics>) -> bool {
        let res = self.inner.probe();

        let current = stats.cache_write_bytes();
        let last = self.last.load(Ordering::Relaxed);
        let delta = current.saturating_sub(last);

        if delta > 0 {
            self.last.store(current, Ordering::Relaxed);
            self.inner.reduce(delta as f64);
        }

        res
    }
}

impl<K> AdmissionPicker for RateLimitPicker<K>
where
    K: StorageKey,
{
    type Key = K;

    fn pick(&self, stats: &Arc<Statistics>, _: &Self::Key) -> bool {
        self.pick_inner(stats)
    }
}

impl<K> ReinsertionPicker for RateLimitPicker<K>
where
    K: StorageKey,
{
    type Key = K;

    fn pick(&self, stats: &Arc<Statistics>, _: &Self::Key) -> bool {
        self.pick_inner(stats)
    }
}

#[derive(Debug, Default)]
pub struct FifoPicker {
    queue: VecDeque<RegionId>,
}

impl EvictionPicker for FifoPicker {
    fn pick(&mut self, _: &HashMap<RegionId, Arc<RegionStats>>) -> Option<RegionId> {
        let res = self.queue.front().copied();
        tracing::trace!("[fifo picker]: pick {res:?}");
        res
    }

    fn on_region_evictable(&mut self, _: &HashMap<RegionId, Arc<RegionStats>>, region: RegionId) {
        tracing::trace!("[fifo picker]: {region} is evictable");
        self.queue.push_back(region);
    }

    fn on_region_evict(&mut self, _: &HashMap<RegionId, Arc<RegionStats>>, region: RegionId) {
        tracing::trace!("[fifo picker]: {region} is evicted");
        let index = self.queue.iter().position(|r| r == &region).unwrap();
        self.queue.remove(index);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fifo_picker() {
        let mut picker = FifoPicker::default();
        let m = HashMap::new();

        (0..10).for_each(|i| picker.on_region_evictable(&m, i));

        assert_eq!(picker.pick(&m), Some(0));
        picker.on_region_evict(&m, 0);
        assert_eq!(picker.pick(&m), Some(1));
        picker.on_region_evict(&m, 1);
        assert_eq!(picker.pick(&m), Some(2));
        picker.on_region_evict(&m, 2);

        picker.on_region_evict(&m, 3);
        picker.on_region_evict(&m, 5);
        picker.on_region_evict(&m, 7);
        picker.on_region_evict(&m, 9);

        assert_eq!(picker.pick(&m), Some(4));
        picker.on_region_evict(&m, 4);
        assert_eq!(picker.pick(&m), Some(6));
        picker.on_region_evict(&m, 6);
        assert_eq!(picker.pick(&m), Some(8));
        picker.on_region_evict(&m, 8);
    }
}
