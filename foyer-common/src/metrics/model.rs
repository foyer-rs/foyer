//  Copyright 2024 foyer Project Authors
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

use super::{BoxedCounter, BoxedGauge, BoxedHistogram, GaugeVecOps, HistogramVecOps, RegistryOps};
use crate::metrics::CounterVecOps;

trait Boxer {
    fn boxed(self) -> Box<Self>
    where
        Self: Sized,
    {
        Box::new(self)
    }
}
impl<T> Boxer for T {}

// FIXME: https://github.com/rust-lang/rust-analyzer/issues/17685
// #[expect(missing_docs)]
/// ... ...
#[derive(Debug)]
pub struct Metrics {
    /* in-memory cache metrics */
    /// ... ...
    pub memory_insert: BoxedCounter,
    /// ... ...
    pub memory_replace: BoxedCounter,
    /// ... ...
    pub memory_hit: BoxedCounter,
    /// ... ...
    pub memory_miss: BoxedCounter,
    /// ... ...
    pub memory_remove: BoxedCounter,
    /// ... ...
    pub memory_evict: BoxedCounter,
    /// ... ...
    pub memory_reinsert: BoxedCounter,
    /// ... ...
    pub memory_release: BoxedCounter,
    /// ... ...
    pub memory_queue: BoxedCounter,
    /// ... ...
    pub memory_fetch: BoxedCounter,

    /// ... ...
    pub memory_usage: BoxedGauge,

    /* disk cache metrics */
    /// ... ...
    pub storage_enqueue: BoxedCounter,
    /// ... ...
    pub storage_hit: BoxedCounter,
    /// ... ...
    pub storage_miss: BoxedCounter,
    /// ... ...
    pub storage_delete: BoxedCounter,

    /// ... ...
    pub storage_enqueue_duration: BoxedHistogram,
    /// ... ...
    pub storage_hit_duration: BoxedHistogram,
    /// ... ...
    pub storage_miss_duration: BoxedHistogram,
    /// ... ...
    pub storage_delete_duration: BoxedHistogram,

    /// ... ...
    pub storage_queue_rotate: BoxedCounter,
    /// ... ...
    pub storage_queue_rotate_duration: BoxedHistogram,
    /// ... ...
    pub storage_queue_drop: BoxedCounter,

    /// ... ...
    pub storage_disk_write: BoxedCounter,
    /// ... ...
    pub storage_disk_read: BoxedCounter,
    /// ... ...
    pub storage_disk_flush: BoxedCounter,

    /// ... ...
    pub storage_disk_write_bytes: BoxedCounter,
    /// ... ...
    pub storage_disk_read_bytes: BoxedCounter,

    /// ... ...
    pub storage_disk_write_duration: BoxedHistogram,
    /// ... ...
    pub storage_disk_read_duration: BoxedHistogram,
    /// ... ...
    pub storage_disk_flush_duration: BoxedHistogram,

    /// ... ...
    pub storage_region_total: BoxedGauge,
    /// ... ...
    pub storage_region_clean: BoxedGauge,
    /// ... ...
    pub storage_region_evictable: BoxedGauge,

    /// ... ...
    pub storage_region_size_bytes: BoxedGauge,

    /// ... ...
    pub storage_entry_serialize_duration: BoxedHistogram,
    /// ... ...
    pub storage_entry_deserialize_duration: BoxedHistogram,

    /* hybrid cache metrics */
    /// ... ...
    pub hybrid_insert: BoxedCounter,
    /// ... ...
    pub hybrid_hit: BoxedCounter,
    /// ... ...
    pub hybrid_miss: BoxedCounter,
    /// ... ...
    pub hybrid_remove: BoxedCounter,

    /// ... ...
    pub hybrid_insert_duration: BoxedHistogram,
    /// ... ...
    pub hybrid_hit_duration: BoxedHistogram,
    /// ... ...
    pub hybrid_miss_duration: BoxedHistogram,
    /// ... ...
    pub hybrid_remove_duration: BoxedHistogram,
    /// ... ...
    pub hybrid_fetch_duration: BoxedHistogram,
}

impl Metrics {
    /// Create a new metric with the given name.
    pub fn new<R>(name: &'static str, registry: &R) -> Self
    where
        R: RegistryOps,
    {
        /* in-memory cache metrics */

        let foyer_memory_op_total = registry.register_counter_vec(
            "foyer_memory_op_total",
            "foyer in-memory cache operations",
            &["name", "op"],
        );
        let foyer_memory_usage =
            registry.register_gauge_vec("foyer_memory_usage", "foyer in-memory cache usage", &["name"]);

        let memory_insert = foyer_memory_op_total.counter(&[name, "insert"]).boxed();
        let memory_replace = foyer_memory_op_total.counter(&[name, "replace"]).boxed();
        let memory_hit = foyer_memory_op_total.counter(&[name, "hit"]).boxed();
        let memory_miss = foyer_memory_op_total.counter(&[name, "miss"]).boxed();
        let memory_remove = foyer_memory_op_total.counter(&[name, "remove"]).boxed();
        let memory_evict = foyer_memory_op_total.counter(&[name, "evict"]).boxed();
        let memory_reinsert = foyer_memory_op_total.counter(&[name, "reinsert"]).boxed();
        let memory_release = foyer_memory_op_total.counter(&[name, "release"]).boxed();
        let memory_queue = foyer_memory_op_total.counter(&[name, "queue"]).boxed();
        let memory_fetch = foyer_memory_op_total.counter(&[name, "fetch"]).boxed();

        let memory_usage = foyer_memory_usage.gauge(&[name]).boxed();

        /* disk cache metrics */

        let foyer_storage_op_total =
            registry.register_counter_vec("foyer_storage_op_total", "foyer disk cache operations", &["name", "op"]);
        let foyer_storage_op_duration = registry.register_histogram_vec(
            "foyer_storage_op_duration",
            "foyer disk cache op durations",
            &["name", "op"],
        );

        let foyer_storage_inner_op_total = registry.register_counter_vec(
            "foyer_storage_inner_op_total",
            "foyer disk cache inner operations",
            &["name", "op"],
        );
        let foyer_storage_inner_op_duration = registry.register_histogram_vec(
            "foyer_storage_inner_op_duration",
            "foyer disk cache inner op durations",
            &["name", "op"],
        );

        let foyer_storage_disk_io_total = registry.register_counter_vec(
            "foyer_storage_disk_io_total",
            "foyer disk cache disk operations",
            &["name", "op"],
        );
        let foyer_storage_disk_io_bytes = registry.register_counter_vec(
            "foyer_storage_disk_io_bytes",
            "foyer disk cache disk io bytes",
            &["name", "op"],
        );
        let foyer_storage_disk_io_duration = registry.register_histogram_vec(
            "foyer_storage_disk_io_duration",
            "foyer disk cache disk io duration",
            &["name", "op"],
        );

        let foyer_storage_region =
            registry.register_gauge_vec("foyer_storage_region", "foyer disk cache regions", &["name", "type"]);
        let foyer_storage_region_size_bytes = registry.register_gauge_vec(
            "foyer_storage_region_size_bytes",
            "foyer disk cache region sizes",
            &["name"],
        );

        let foyer_storage_entry_serde_duration = registry.register_histogram_vec(
            "foyer_storage_entry_serde_duration",
            "foyer disk cache entry serde durations",
            &["name", "op"],
        );

        let storage_enqueue = foyer_storage_op_total.counter(&[name, "enqueue"]).boxed();
        let storage_hit = foyer_storage_op_total.counter(&[name, "hit"]).boxed();
        let storage_miss = foyer_storage_op_total.counter(&[name, "miss"]).boxed();
        let storage_delete = foyer_storage_op_total.counter(&[name, "delete"]).boxed();

        let storage_enqueue_duration = foyer_storage_op_duration.histogram(&[name, "enqueue"]).boxed();
        let storage_hit_duration = foyer_storage_op_duration.histogram(&[name, "hit"]).boxed();
        let storage_miss_duration = foyer_storage_op_duration.histogram(&[name, "miss"]).boxed();
        let storage_delete_duration = foyer_storage_op_duration.histogram(&[name, "delete"]).boxed();

        let storage_queue_rotate = foyer_storage_inner_op_total.counter(&[name, "queue_rotate"]).boxed();
        let storage_queue_drop = foyer_storage_inner_op_total.counter(&[name, "queue_drop"]).boxed();

        let storage_queue_rotate_duration = foyer_storage_inner_op_duration
            .histogram(&[name, "queue_rotate"])
            .boxed();

        let storage_disk_write = foyer_storage_disk_io_total.counter(&[name, "write"]).boxed();
        let storage_disk_read = foyer_storage_disk_io_total.counter(&[name, "read"]).boxed();
        let storage_disk_flush = foyer_storage_disk_io_total.counter(&[name, "flush"]).boxed();

        let storage_disk_write_bytes = foyer_storage_disk_io_bytes.counter(&[name, "write"]).boxed();
        let storage_disk_read_bytes = foyer_storage_disk_io_bytes.counter(&[name, "read"]).boxed();

        let storage_disk_write_duration = foyer_storage_disk_io_duration.histogram(&[name, "write"]).boxed();
        let storage_disk_read_duration = foyer_storage_disk_io_duration.histogram(&[name, "read"]).boxed();
        let storage_disk_flush_duration = foyer_storage_disk_io_duration.histogram(&[name, "flush"]).boxed();

        let storage_region_total = foyer_storage_region.gauge(&[name, "total"]).boxed();
        let storage_region_clean = foyer_storage_region.gauge(&[name, "clean"]).boxed();
        let storage_region_evictable = foyer_storage_region.gauge(&[name, "evictable"]).boxed();

        let storage_region_size_bytes = foyer_storage_region_size_bytes.gauge(&[name]).boxed();

        let storage_entry_serialize_duration = foyer_storage_entry_serde_duration
            .histogram(&[name, "serialize"])
            .boxed();
        let storage_entry_deserialize_duration = foyer_storage_entry_serde_duration
            .histogram(&[name, "deserialize"])
            .boxed();

        /* hybrid cache metrics */

        let foyer_hybrid_op_total = registry.register_counter_vec(
            "foyer_hybrid_op_total",
            "foyer hybrid cache operations",
            &["name", "op"],
        );
        let foyer_hybrid_op_duration = registry.register_histogram_vec(
            "foyer_hybrid_op_duration",
            "foyer hybrid cache operation durations",
            &["name", "op"],
        );

        let hybrid_insert = foyer_hybrid_op_total.counter(&[name, "insert"]).boxed();
        let hybrid_hit = foyer_hybrid_op_total.counter(&[name, "hit"]).boxed();
        let hybrid_miss = foyer_hybrid_op_total.counter(&[name, "miss"]).boxed();
        let hybrid_remove = foyer_hybrid_op_total.counter(&[name, "remove"]).boxed();

        let hybrid_insert_duration = foyer_hybrid_op_duration.histogram(&[name, "insert"]).boxed();
        let hybrid_hit_duration = foyer_hybrid_op_duration.histogram(&[name, "hit"]).boxed();
        let hybrid_miss_duration = foyer_hybrid_op_duration.histogram(&[name, "miss"]).boxed();
        let hybrid_remove_duration = foyer_hybrid_op_duration.histogram(&[name, "remove"]).boxed();
        let hybrid_fetch_duration = foyer_hybrid_op_duration.histogram(&[name, "fetch"]).boxed();

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

    /// Build noop metrics.
    ///
    /// Note: `noop` is only supposed to be called by other foyer components.
    #[doc(hidden)]
    pub fn noop() -> Self {
        use super::registry::noop::NoopMetricsRegistry;

        Self::new("test", &NoopMetricsRegistry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::registry::noop::NoopMetricsRegistry;

    fn case(registry: &impl RegistryOps) {
        let _ = Metrics::new("test", registry);
    }

    #[test]
    fn test_metrics_noop() {
        case(&NoopMetricsRegistry);
    }

    #[cfg(feature = "prometheus")]
    #[test]
    fn test_metrics_prometheus() {
        use crate::metrics::registry::prometheus::PrometheusMetricsRegistry;

        case(&PrometheusMetricsRegistry::new(prometheus::Registry::new()));
    }

    #[cfg(feature = "opentelemetry")]
    #[test]
    fn test_metrics_opentelemetry() {
        use crate::metrics::registry::opentelemetry::OpenTelemetryMetricsRegistry;

        case(&OpenTelemetryMetricsRegistry::new(opentelemetry::global::meter("test")));
    }
}
