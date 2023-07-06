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
    register_int_counter_with_registry, register_int_gauge_with_registry, Histogram, HistogramVec,
    IntCounter, IntCounterVec, IntGauge, Registry,
};

#[derive(Debug)]
pub struct Metrics {
    pub latency_insert: Histogram,
    pub latency_lookup_hit: Histogram,
    pub latency_lookup_miss: Histogram,
    pub latency_remove: Histogram,

    pub bytes_insert: IntCounter,
    pub bytes_lookup: IntCounter,
    pub bytes_remove: IntCounter,

    pub size: IntGauge,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new(Registry::default())
    }
}

impl Metrics {
    pub fn new(registry: Registry) -> Self {
        let latency = register_histogram_vec_with_registry!(
            "foyer_storage_latency",
            "foyer storage latency",
            &["op", "extra"],
            vec![0.0001, 0.001, 0.005, 0.01, 0.02, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0],
            registry
        )
        .unwrap();
        let bytes = register_int_counter_vec_with_registry!(
            "foyer_storage_bytes",
            "foyer storage bytes",
            &["op", "extra"],
            registry
        )
        .unwrap();

        let latency_insert = latency.with_label_values(&["insert", ""]);
        let latency_lookup_hit = latency.with_label_values(&["lookup", "hit"]);
        let latency_lookup_miss = latency.with_label_values(&["lookup", "miss"]);
        let latency_remove = latency.with_label_values(&["remove", ""]);

        let bytes_insert = bytes.with_label_values(&["insert"]);
        let bytes_lookup = bytes.with_label_values(&["lookup"]);
        let bytes_remove = bytes.with_label_values(&["remove"]);

        let size =
            register_int_gauge_with_registry!("foyer_storage_size", "foyer storage size", registry)
                .unwrap();

        Self {
            latency_insert,
            latency_lookup_hit,
            latency_lookup_miss,
            latency_remove,

            bytes_insert,
            bytes_lookup,
            bytes_remove,

            size,
        }
    }
}
