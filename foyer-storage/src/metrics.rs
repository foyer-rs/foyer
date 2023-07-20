//  Copyright 2023 MrCroxx
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

use prometheus::{
    register_histogram_vec_with_registry, register_int_counter_vec_with_registry,
    register_int_gauge_with_registry, Histogram, HistogramOpts, IntCounter, IntGauge, Opts,
    Registry,
};

#[derive(Debug)]
pub struct Metrics {
    pub latency_insert_inserted: Histogram,
    pub latency_insert_dropped: Histogram,
    pub latency_lookup_hit: Histogram,
    pub latency_lookup_miss: Histogram,
    pub latency_remove: Histogram,

    pub bytes_insert: IntCounter,
    pub bytes_lookup: IntCounter,
    pub bytes_flush: IntCounter,
    pub bytes_reclaim: IntCounter,
    pub bytes_reinsert: IntCounter,

    pub size: IntGauge,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        Self::with_registry_namespace(Registry::default(), "")
    }

    pub fn with_namespace(namespace: impl ToString) -> Self {
        Self::with_registry_namespace(Registry::default(), namespace)
    }

    pub fn with_registry(registry: Registry) -> Self {
        Self::with_registry_namespace(registry, "")
    }

    pub fn with_registry_namespace(registry: Registry, namespace: impl ToString) -> Self {
        let latency = {
            let opts = HistogramOpts::new("foyer_storage_latency", "foyer storage latency")
                .namespace(namespace.to_string())
                .buckets(vec![
                    0.0001, 0.001, 0.005, 0.01, 0.02, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0,
                ]);
            register_histogram_vec_with_registry!(opts, &["op", "extra"], registry).unwrap()
        };
        let bytes = {
            let opts = Opts::new("foyer_storage_bytes", "foyer storage bytes")
                .namespace(namespace.to_string());
            register_int_counter_vec_with_registry!(opts, &["op", "extra"], registry).unwrap()
        };

        let latency_insert_inserted = latency.with_label_values(&["insert", "inserted"]);
        let latency_insert_dropped = latency.with_label_values(&["insert", "dropped"]);
        let latency_lookup_hit = latency.with_label_values(&["lookup", "hit"]);
        let latency_lookup_miss = latency.with_label_values(&["lookup", "miss"]);
        let latency_remove = latency.with_label_values(&["remove", ""]);

        let bytes_insert = bytes.with_label_values(&["insert", ""]);
        let bytes_lookup = bytes.with_label_values(&["lookup", ""]);
        let bytes_flush = bytes.with_label_values(&["flush", ""]);
        let bytes_reclaim = bytes.with_label_values(&["reclaim", ""]);
        let bytes_reinsert = bytes.with_label_values(&["reinsert", ""]);

        let size = {
            let opts = Opts::new("foyer_storage_size", "foyer storage size")
                .namespace(namespace.to_string());
            register_int_gauge_with_registry!(opts, registry).unwrap()
        };

        Self {
            latency_insert_inserted,
            latency_insert_dropped,
            latency_lookup_hit,
            latency_lookup_miss,
            latency_remove,

            bytes_insert,
            bytes_lookup,
            bytes_flush,
            bytes_reclaim,
            bytes_reinsert,

            size,
        }
    }
}
