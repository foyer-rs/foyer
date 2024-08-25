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
    fmt::Debug,
    ops::Deref,
    time::{Duration, Instant},
};

use metrics::{counter, gauge, histogram, Counter, Gauge, Histogram};

/// A Wrapper for [`Histogram`] to track durations.
#[derive(Clone)]
pub struct DurationHistogram(Histogram);

impl Deref for DurationHistogram {
    type Target = Histogram;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Histogram> for DurationHistogram {
    fn from(value: Histogram) -> Self {
        Self::new(value)
    }
}

impl DurationHistogram {
    /// Create a new [`DurationHistogram`].
    pub fn new(histogram: Histogram) -> Self {
        Self(histogram)
    }

    /// Get a timer that automatically record duration on drop.
    pub fn timer(&self) -> DurationHistogramTimer<'_> {
        DurationHistogramTimer {
            histogram: self,
            created: Instant::now(),
        }
    }
}

/// A timer that automatically record duration with [`Histogram`] on drop.
pub struct DurationHistogramTimer<'a> {
    histogram: &'a Histogram,
    created: Instant,
}

impl<'a> Drop for DurationHistogramTimer<'a> {
    fn drop(&mut self) {
        self.histogram.record(self.created.elapsed());
    }
}

impl<'a> DurationHistogramTimer<'a> {
    /// Get elapsed duration since created.
    pub fn elapsed(&self) -> Duration {
        self.created.elapsed()
    }
}

// TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
#[allow(missing_docs)]
#[derive(Clone)]
pub struct Metrics {
    /* in-memory cache metrics */
    pub memory_insert: Counter,
    pub memory_replace: Counter,
    pub memory_hit: Counter,
    pub memory_miss: Counter,
    pub memory_remove: Counter,
    pub memory_evict: Counter,
    pub memory_reinsert: Counter,
    pub memory_release: Counter,
    pub memory_queue: Counter,
    pub memory_fetch: Counter,

    pub memory_usage: Gauge,

    /* disk cache metrics */
    pub storage_enqueue: Counter,
    pub storage_hit: Counter,
    pub storage_miss: Counter,
    pub storage_delete: Counter,

    pub storage_enqueue_duration: DurationHistogram,
    pub storage_hit_duration: DurationHistogram,
    pub storage_miss_duration: DurationHistogram,
    pub storage_delete_duration: DurationHistogram,

    pub storage_queue_rotate: Counter,
    pub storage_queue_rotate_duration: DurationHistogram,
    pub storage_queue_drop: Counter,

    pub storage_disk_write: Counter,
    pub storage_disk_read: Counter,
    pub storage_disk_flush: Counter,

    pub storage_disk_write_bytes: Counter,
    pub storage_disk_read_bytes: Counter,

    pub storage_disk_write_duration: DurationHistogram,
    pub storage_disk_read_duration: DurationHistogram,
    pub storage_disk_flush_duration: DurationHistogram,

    pub storage_region_total: Gauge,
    pub storage_region_clean: Gauge,
    pub storage_region_evictable: Gauge,

    pub storage_region_size_bytes: Gauge,

    pub storage_entry_serialize_duration: DurationHistogram,
    pub storage_entry_deserialize_duration: DurationHistogram,

    /* hybrid cache metrics */
    pub hybrid_insert: Counter,
    pub hybrid_hit: Counter,
    pub hybrid_miss: Counter,
    pub hybrid_remove: Counter,

    pub hybrid_insert_duration: DurationHistogram,
    pub hybrid_hit_duration: DurationHistogram,
    pub hybrid_miss_duration: DurationHistogram,
    pub hybrid_remove_duration: DurationHistogram,
    pub hybrid_fetch_duration: DurationHistogram,
}

impl Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metrics").finish()
    }
}

impl Metrics {
    /// Create a new metrics with the given name.
    pub fn new(name: &str) -> Self {
        /* in-memory cache metrics */

        let memory_insert = counter!(format!("foyer_memory_op_total"), "name" => name.to_string(), "op" => "insert");
        let memory_replace = counter!(format!("foyer_memory_op_total"), "name" => name.to_string(), "op" => "replace");
        let memory_hit = counter!(format!("foyer_memory_op_total"), "name" => name.to_string(), "op" => "hit");
        let memory_miss = counter!(format!("foyer_memory_op_total"), "name" => name.to_string(), "op" => "miss");
        let memory_remove = counter!(format!("foyer_memory_op_total"), "name" => name.to_string(), "op" => "remove");
        let memory_evict = counter!(format!("foyer_memory_op_total"), "name" => name.to_string(), "op" => "evict");
        let memory_reinsert =
            counter!(format!("foyer_memory_op_total"), "name" => name.to_string(), "op" => "reinsert");
        let memory_release = counter!(format!("foyer_memory_op_total"), "name" => name.to_string(), "op" => "release");
        let memory_queue = counter!(format!("foyer_memory_op_total"), "name" => name.to_string(), "op" => "queue");
        let memory_fetch = counter!(format!("foyer_memory_op_total"), "name" => name.to_string(), "op" => "fetch");

        let memory_usage = gauge!(format!("foyer_memory_usage"), "name" => name.to_string(), "op" => "usage");

        /* disk cache metrics */

        let storage_enqueue =
            counter!(format!("foyer_storage_op_total"), "name" => name.to_string(), "op" => "enqueue");
        let storage_hit = counter!(format!("foyer_storage_op_total"), "name" => name.to_string(), "op" => "hit");
        let storage_miss = counter!(format!("foyer_storage_op_total"), "name" => name.to_string(), "op" => "miss");
        let storage_delete = counter!(format!("foyer_storage_op_total"), "name" => name.to_string(), "op" => "delete");

        let storage_enqueue_duration =
            histogram!(format!("foyer_storage_op_duration"), "name" => name.to_string(), "op" => "enqueue").into();
        let storage_hit_duration =
            histogram!(format!("foyer_storage_op_duration"), "name" => name.to_string(), "op" => "hit").into();
        let storage_miss_duration =
            histogram!(format!("foyer_storage_op_duration"), "name" => name.to_string(), "op" => "miss").into();
        let storage_delete_duration =
            histogram!(format!("foyer_storage_op_duration"), "name" => name.to_string(), "op" => "delete").into();

        let storage_queue_rotate =
            counter!(format!("foyer_storage_inner_op_total"), "name" => name.to_string(), "op" => "queue_rotate");
        let storage_queue_rotate_duration =
            histogram!(format!("foyer_storage_inner_op_duration"), "name" => name.to_string(), "op" => "queue_rotate")
                .into();
        let storage_queue_drop =
            counter!(format!("foyer_storage_inner_op_total"), "name" => name.to_string(), "op" => "queue_drop");

        let storage_disk_write =
            counter!(format!("foyer_storage_disk_io_total"), "name" => name.to_string(), "op" => "write");
        let storage_disk_read =
            counter!(format!("foyer_storage_disk_io_total"), "name" => name.to_string(), "op" => "read");
        let storage_disk_flush =
            counter!(format!("foyer_storage_disk_io_total"), "name" => name.to_string(), "op" => "flush");

        let storage_disk_write_bytes =
            counter!(format!("foyer_storage_disk_io_bytes"), "name" => name.to_string(), "op" => "write");
        let storage_disk_read_bytes =
            counter!(format!("foyer_storage_disk_io_bytes"), "name" => name.to_string(), "op" => "read");

        let storage_disk_write_duration =
            histogram!(format!("foyer_storage_disk_io_duration"), "name" => name.to_string(), "op" => "write").into();
        let storage_disk_read_duration =
            histogram!(format!("foyer_storage_disk_io_duration"), "name" => name.to_string(), "op" => "read").into();
        let storage_disk_flush_duration =
            histogram!(format!("foyer_storage_disk_io_duration"), "name" => name.to_string(), "op" => "flush").into();

        let storage_region_total =
            gauge!(format!("foyer_storage_region"), "name" => name.to_string(), "type" => "total");
        let storage_region_clean =
            gauge!(format!("foyer_storage_region"), "name" => name.to_string(), "type" => "clean");
        let storage_region_evictable =
            gauge!(format!("foyer_storage_region"), "name" => name.to_string(), "type" => "evictable");

        let storage_region_size_bytes = gauge!(format!("foyer_storage_region_size_bytes"), "name" => name.to_string());

        let storage_entry_serialize_duration =
            histogram!(format!("foyer_storage_entry_serde_duration"), "name" => name.to_string(), "op" => "serialize")
                .into();
        let storage_entry_deserialize_duration = histogram!(format!("foyer_storage_entry_serde_duration"), "name" => name.to_string(), "op" => "deserialize").into();

        /* hybrid cache metrics */

        let hybrid_insert = counter!(format!("foyer_hybrid_op_total"), "name" => name.to_string(), "op" => "insert");
        let hybrid_hit = counter!(format!("foyer_hybrid_op_total"), "name" => name.to_string(), "op" => "hit");
        let hybrid_miss = counter!(format!("foyer_hybrid_op_total"), "name" => name.to_string(), "op" => "miss");
        let hybrid_remove = counter!(format!("foyer_hybrid_op_total"), "name" => name.to_string(), "op" => "remove");

        let hybrid_insert_duration =
            histogram!(format!("foyer_hybrid_op_duration"), "name" => name.to_string(), "op" => "insert").into();
        let hybrid_hit_duration =
            histogram!(format!("foyer_hybrid_op_duration"), "name" => name.to_string(), "op" => "hit").into();
        let hybrid_miss_duration =
            histogram!(format!("foyer_hybrid_op_duration"), "name" => name.to_string(), "op" => "miss").into();
        let hybrid_remove_duration =
            histogram!(format!("foyer_hybrid_op_duration"), "name" => name.to_string(), "op" => "remove").into();
        let hybrid_fetch_duration =
            histogram!(format!("foyer_hybrid_op_duration"), "name" => name.to_string(), "op" => "fetch").into();

        Self {
            memory_insert,
            memory_replace,
            memory_hit,
            memory_miss,
            memory_remove,
            memory_evict,
            memory_reinsert,
            memory_release,
            memory_queue,
            memory_fetch,
            memory_usage,

            storage_enqueue,
            storage_hit,
            storage_miss,
            storage_delete,
            storage_enqueue_duration,
            storage_hit_duration,
            storage_miss_duration,
            storage_delete_duration,
            storage_queue_rotate,
            storage_queue_rotate_duration,
            storage_queue_drop,
            storage_disk_write,
            storage_disk_read,
            storage_disk_flush,
            storage_disk_write_bytes,
            storage_disk_read_bytes,
            storage_disk_write_duration,
            storage_disk_read_duration,
            storage_disk_flush_duration,
            storage_region_total,
            storage_region_clean,
            storage_region_evictable,
            storage_region_size_bytes,
            storage_entry_serialize_duration,
            storage_entry_deserialize_duration,

            hybrid_insert,
            hybrid_hit,
            hybrid_miss,
            hybrid_remove,
            hybrid_insert_duration,
            hybrid_hit_duration,
            hybrid_miss_duration,
            hybrid_remove_duration,
            hybrid_fetch_duration,
        }
    }
}
