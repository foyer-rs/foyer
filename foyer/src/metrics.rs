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
    register_counter_vec_with_registry, register_gauge_with_registry,
    register_histogram_vec_with_registry, register_int_counter_with_registry, Counter, CounterVec,
    Gauge, Histogram, HistogramVec, IntCounter, Registry,
};

pub struct Metrics {
    pub latency_insert: Histogram,
    pub latency_get: Histogram,
    pub latency_remove: Histogram,

    pub latency_store: Histogram,
    pub latency_load: Histogram,
    pub latency_delete: Histogram,

    pub bytes_store: Counter,
    pub bytes_load: Counter,
    pub bytes_delete: Counter,

    pub cache_data_size: Gauge,

    pub miss: IntCounter,

    _latency: HistogramVec,
    _bytes: CounterVec,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new(Registry::new())
    }
}

impl Metrics {
    pub fn new(registry: Registry) -> Self {
        let latency = register_histogram_vec_with_registry!(
            "foyer_latency",
            "foyer latency",
            &["op"],
            vec![
                0.0001, 0.001, 0.005, 0.01, 0.02, 0.03, 0.04, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75,
                1.0
            ],
            registry
        )
        .unwrap();

        let bytes =
            register_counter_vec_with_registry!("foyer_bytes", "foyer bytes", &["op"], registry)
                .unwrap();

        let latency_insert = latency.with_label_values(&["insert"]);
        let latency_get = latency.with_label_values(&["get"]);
        let latency_remove = latency.with_label_values(&["remove"]);

        let latency_store = latency.with_label_values(&["store"]);
        let latency_load = latency.with_label_values(&["load"]);
        let latency_delete = latency.with_label_values(&["delete"]);

        let bytes_store = bytes.with_label_values(&["store"]);
        let bytes_load = bytes.with_label_values(&["load"]);
        let bytes_delete = bytes.with_label_values(&["delete"]);

        let miss =
            register_int_counter_with_registry!("foyer_cache_miss", "foyer cache miss", registry)
                .unwrap();
        let cache_data_size = register_gauge_with_registry!(
            "foyer_cache_data_size",
            "foyer cache data size",
            registry
        )
        .unwrap();

        Self {
            latency_insert,
            latency_get,
            latency_remove,

            latency_store,
            latency_load,
            latency_delete,

            bytes_store,
            bytes_load,
            bytes_delete,

            cache_data_size,

            miss,

            _latency: latency,
            _bytes: bytes,
        }
    }
}
