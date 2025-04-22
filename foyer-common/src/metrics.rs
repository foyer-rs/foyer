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

use std::{borrow::Cow, fmt::Debug};

use mixtrics::metrics::{BoxedCounter, BoxedGauge, BoxedHistogram, BoxedRegistry, Buckets};

#[expect(missing_docs)]
pub struct Metrics {
    /* in-memory cache metrics */
    pub memory_insert: BoxedCounter,
    pub memory_replace: BoxedCounter,
    pub memory_hit: BoxedCounter,
    pub memory_miss: BoxedCounter,
    pub memory_remove: BoxedCounter,
    pub memory_evict: BoxedCounter,
    pub memory_reinsert: BoxedCounter,
    pub memory_release: BoxedCounter,
    pub memory_queue: BoxedCounter,
    pub memory_fetch: BoxedCounter,

    pub memory_usage: BoxedGauge,

    /* disk cache metrics */
    pub storage_enqueue: BoxedCounter,
    pub storage_hit: BoxedCounter,
    pub storage_miss: BoxedCounter,
    pub storage_delete: BoxedCounter,
    pub storage_error: BoxedCounter,

    pub storage_enqueue_duration: BoxedHistogram,
    pub storage_hit_duration: BoxedHistogram,
    pub storage_miss_duration: BoxedHistogram,
    pub storage_delete_duration: BoxedHistogram,

    pub storage_queue_rotate: BoxedCounter,
    pub storage_queue_rotate_duration: BoxedHistogram,
    pub storage_queue_buffer_overflow: BoxedCounter,
    pub storage_queue_channel_overflow: BoxedCounter,

    pub storage_disk_write: BoxedCounter,
    pub storage_disk_read: BoxedCounter,
    pub storage_disk_flush: BoxedCounter,

    pub storage_disk_write_bytes: BoxedCounter,
    pub storage_disk_read_bytes: BoxedCounter,

    pub storage_disk_write_duration: BoxedHistogram,
    pub storage_disk_read_duration: BoxedHistogram,
    pub storage_disk_flush_duration: BoxedHistogram,

    pub storage_region_total: BoxedGauge,
    pub storage_region_clean: BoxedGauge,
    pub storage_region_evictable: BoxedGauge,

    pub storage_region_size_bytes: BoxedGauge,

    pub storage_entry_serialize_duration: BoxedHistogram,
    pub storage_entry_deserialize_duration: BoxedHistogram,

    pub storage_lodc_indexer_conflict: BoxedCounter,
    pub storage_lodc_buffer_efficiency: BoxedHistogram,
    pub storage_lodc_recover_duration: BoxedHistogram,

    /* hybrid cache metrics */
    pub hybrid_insert: BoxedCounter,
    pub hybrid_hit: BoxedCounter,
    pub hybrid_miss: BoxedCounter,
    pub hybrid_throttled: BoxedCounter,
    pub hybrid_remove: BoxedCounter,

    pub hybrid_insert_duration: BoxedHistogram,
    pub hybrid_hit_duration: BoxedHistogram,
    pub hybrid_miss_duration: BoxedHistogram,
    pub hybrid_throttled_duration: BoxedHistogram,
    pub hybrid_remove_duration: BoxedHistogram,
    pub hybrid_fetch_duration: BoxedHistogram,
}

impl Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metrics").finish()
    }
}

impl Metrics {
    /// Create a new metric with the given name.
    pub fn new(name: impl Into<Cow<'static, str>>, registry: &BoxedRegistry) -> Self {
        let name = name.into();

        /* in-memory cache metrics */

        let foyer_memory_op_total = registry.register_counter_vec(
            "foyer_memory_op_total".into(),
            "foyer in-memory cache operations".into(),
            &["name", "op"],
        );
        let foyer_memory_usage = registry.register_gauge_vec(
            "foyer_memory_usage".into(),
            "foyer in-memory cache usage".into(),
            &["name"],
        );

        let memory_insert = foyer_memory_op_total.counter(&[name.clone(), "insert".into()]);
        let memory_replace = foyer_memory_op_total.counter(&[name.clone(), "replace".into()]);
        let memory_hit = foyer_memory_op_total.counter(&[name.clone(), "hit".into()]);
        let memory_miss = foyer_memory_op_total.counter(&[name.clone(), "miss".into()]);
        let memory_remove = foyer_memory_op_total.counter(&[name.clone(), "remove".into()]);
        let memory_evict = foyer_memory_op_total.counter(&[name.clone(), "evict".into()]);
        let memory_reinsert = foyer_memory_op_total.counter(&[name.clone(), "reinsert".into()]);
        let memory_release = foyer_memory_op_total.counter(&[name.clone(), "release".into()]);
        let memory_queue = foyer_memory_op_total.counter(&[name.clone(), "queue".into()]);
        let memory_fetch = foyer_memory_op_total.counter(&[name.clone(), "fetch".into()]);

        let memory_usage = foyer_memory_usage.gauge(&[name.clone()]);

        /* disk cache metrics */

        let foyer_storage_op_total = registry.register_counter_vec(
            "foyer_storage_op_total".into(),
            "foyer disk cache operations".into(),
            &["name", "op"],
        );
        let foyer_storage_op_duration = registry.register_histogram_vec_with_buckets(
            "foyer_storage_op_duration".into(),
            "foyer disk cache op durations".into(),
            &["name", "op"],
            // 1us ~ 4s
            Buckets::exponential(0.000_001, 2.0, 23),
        );

        let foyer_storage_inner_op_total = registry.register_counter_vec(
            "foyer_storage_inner_op_total".into(),
            "foyer disk cache inner operations".into(),
            &["name", "op"],
        );
        let foyer_storage_inner_op_duration = registry.register_histogram_vec_with_buckets(
            "foyer_storage_inner_op_duration".into(),
            "foyer disk cache inner op durations".into(),
            &["name", "op"],
            // 1us ~ 16s
            Buckets::exponential(0.000_001, 2.0, 25),
        );

        let foyer_storage_disk_io_total = registry.register_counter_vec(
            "foyer_storage_disk_io_total".into(),
            "foyer disk cache disk operations".into(),
            &["name", "op"],
        );
        let foyer_storage_disk_io_bytes = registry.register_counter_vec(
            "foyer_storage_disk_io_bytes".into(),
            "foyer disk cache disk io bytes".into(),
            &["name", "op"],
        );
        let foyer_storage_disk_io_duration = registry.register_histogram_vec_with_buckets(
            "foyer_storage_disk_io_duration".into(),
            "foyer disk cache disk io duration".into(),
            &["name", "op"],
            // 1us ~ 4s
            Buckets::exponential(0.000_001, 2.0, 23),
        );

        let foyer_storage_region = registry.register_gauge_vec(
            "foyer_storage_region".into(),
            "foyer disk cache regions".into(),
            &["name", "type"],
        );
        let foyer_storage_region_size_bytes = registry.register_gauge_vec(
            "foyer_storage_region_size_bytes".into(),
            "foyer disk cache region sizes".into(),
            &["name"],
        );

        let foyer_storage_entry_serde_duration = registry.register_histogram_vec_with_buckets(
            "foyer_storage_entry_serde_duration".into(),
            "foyer disk cache entry serde durations".into(),
            &["name", "op"],
            // 10ns ~ 40ms
            Buckets::exponential(0.000_000_01, 2.0, 23),
        );

        let foyer_storage_lodc_op_total = registry.register_counter_vec(
            "foyer_storage_lodc_op_total".into(),
            "foyer large object disk cache operations".into(),
            &["name", "op"],
        );

        let foyer_storage_lodc_buffer_efficiency = registry.register_histogram_vec_with_buckets(
            "foyer_storage_lodc_buffer_efficiency".into(),
            "foyer large object disk cache buffer efficiency".into(),
            &["name"],
            // 0% ~ 100%
            Buckets::linear(0.1, 0.1, 10),
        );

        let foyer_storage_lodc_recover_duration = registry.register_histogram_vec_with_buckets(
            "foyer_storage_lodc_recover_duration".into(),
            "foyer large object disk cache recover duration".into(),
            &["name"],
            // 1ms ~ 1000s
            Buckets::exponential(0.001, 2.0, 21),
        );

        let storage_enqueue = foyer_storage_op_total.counter(&[name.clone(), "enqueue".into()]);
        let storage_hit = foyer_storage_op_total.counter(&[name.clone(), "hit".into()]);
        let storage_miss = foyer_storage_op_total.counter(&[name.clone(), "miss".into()]);
        let storage_delete = foyer_storage_op_total.counter(&[name.clone(), "delete".into()]);
        let storage_error = foyer_storage_op_total.counter(&[name.clone(), "error".into()]);

        let storage_enqueue_duration = foyer_storage_op_duration.histogram(&[name.clone(), "enqueue".into()]);
        let storage_hit_duration = foyer_storage_op_duration.histogram(&[name.clone(), "hit".into()]);
        let storage_miss_duration = foyer_storage_op_duration.histogram(&[name.clone(), "miss".into()]);
        let storage_delete_duration = foyer_storage_op_duration.histogram(&[name.clone(), "delete".into()]);

        let storage_queue_rotate = foyer_storage_inner_op_total.counter(&[name.clone(), "queue_rotate".into()]);
        let storage_queue_buffer_overflow =
            foyer_storage_inner_op_total.counter(&[name.clone(), "buffer_overflow".into()]);
        let storage_queue_channel_overflow =
            foyer_storage_inner_op_total.counter(&[name.clone(), "channel_overflow".into()]);

        let storage_queue_rotate_duration =
            foyer_storage_inner_op_duration.histogram(&[name.clone(), "queue_rotate".into()]);

        let storage_disk_write = foyer_storage_disk_io_total.counter(&[name.clone(), "write".into()]);
        let storage_disk_read = foyer_storage_disk_io_total.counter(&[name.clone(), "read".into()]);
        let storage_disk_flush = foyer_storage_disk_io_total.counter(&[name.clone(), "flush".into()]);

        let storage_disk_write_bytes = foyer_storage_disk_io_bytes.counter(&[name.clone(), "write".into()]);
        let storage_disk_read_bytes = foyer_storage_disk_io_bytes.counter(&[name.clone(), "read".into()]);

        let storage_disk_write_duration = foyer_storage_disk_io_duration.histogram(&[name.clone(), "write".into()]);
        let storage_disk_read_duration = foyer_storage_disk_io_duration.histogram(&[name.clone(), "read".into()]);
        let storage_disk_flush_duration = foyer_storage_disk_io_duration.histogram(&[name.clone(), "flush".into()]);

        let storage_region_total = foyer_storage_region.gauge(&[name.clone(), "total".into()]);
        let storage_region_clean = foyer_storage_region.gauge(&[name.clone(), "clean".into()]);
        let storage_region_evictable = foyer_storage_region.gauge(&[name.clone(), "evictable".into()]);

        let storage_region_size_bytes = foyer_storage_region_size_bytes.gauge(&[name.clone()]);

        let storage_entry_serialize_duration =
            foyer_storage_entry_serde_duration.histogram(&[name.clone(), "serialize".into()]);
        let storage_entry_deserialize_duration =
            foyer_storage_entry_serde_duration.histogram(&[name.clone(), "deserialize".into()]);

        let storage_lodc_indexer_conflict =
            foyer_storage_lodc_op_total.counter(&[name.clone(), "indexer_conflict".into()]);
        let storage_lodc_buffer_efficiency = foyer_storage_lodc_buffer_efficiency.histogram(&[name.clone()]);
        let storage_lodc_recover_duration = foyer_storage_lodc_recover_duration.histogram(&[name.clone()]);

        /* hybrid cache metrics */

        let foyer_hybrid_op_total = registry.register_counter_vec(
            "foyer_hybrid_op_total".into(),
            "foyer hybrid cache operations".into(),
            &["name", "op"],
        );
        let foyer_hybrid_op_duration = registry.register_histogram_vec(
            "foyer_hybrid_op_duration".into(),
            "foyer hybrid cache operation durations".into(),
            &["name", "op"],
        );

        let hybrid_insert = foyer_hybrid_op_total.counter(&[name.clone(), "insert".into()]);
        let hybrid_hit = foyer_hybrid_op_total.counter(&[name.clone(), "hit".into()]);
        let hybrid_miss = foyer_hybrid_op_total.counter(&[name.clone(), "miss".into()]);
        let hybrid_throttled = foyer_hybrid_op_total.counter(&[name.clone(), "throttled".into()]);
        let hybrid_remove = foyer_hybrid_op_total.counter(&[name.clone(), "remove".into()]);

        let hybrid_insert_duration = foyer_hybrid_op_duration.histogram(&[name.clone(), "insert".into()]);
        let hybrid_hit_duration = foyer_hybrid_op_duration.histogram(&[name.clone(), "hit".into()]);
        let hybrid_miss_duration = foyer_hybrid_op_duration.histogram(&[name.clone(), "miss".into()]);
        let hybrid_throttled_duration = foyer_hybrid_op_duration.histogram(&[name.clone(), "throttled".into()]);
        let hybrid_remove_duration = foyer_hybrid_op_duration.histogram(&[name.clone(), "remove".into()]);
        let hybrid_fetch_duration = foyer_hybrid_op_duration.histogram(&[name.clone(), "fetch".into()]);

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
            storage_error,
            storage_enqueue_duration,
            storage_hit_duration,
            storage_miss_duration,
            storage_delete_duration,
            storage_queue_rotate,
            storage_queue_rotate_duration,
            storage_queue_buffer_overflow,
            storage_queue_channel_overflow,
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
            storage_lodc_indexer_conflict,
            storage_lodc_buffer_efficiency,
            storage_lodc_recover_duration,

            hybrid_insert,
            hybrid_hit,
            hybrid_miss,
            hybrid_throttled,
            hybrid_throttled_duration,
            hybrid_remove,
            hybrid_insert_duration,
            hybrid_hit_duration,
            hybrid_miss_duration,
            hybrid_remove_duration,
            hybrid_fetch_duration,
        }
    }

    /// Build noop metrics.
    ///
    /// Note: `noop` is only supposed to be called by other foyer components.
    #[doc(hidden)]
    pub fn noop() -> Self {
        let registry: BoxedRegistry = Box::new(mixtrics::registry::noop::NoopMetricsRegistry);
        Self::new("test", &registry)
    }
}

#[cfg(test)]
mod tests {
    use mixtrics::metrics::BoxedRegistry;

    use super::Metrics;

    fn test_fn(registry: &BoxedRegistry) {
        Metrics::new("test", registry);
    }

    mixtrics::test! { test_fn }
}
