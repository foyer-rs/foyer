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
    register_counter_vec_with_registry, register_histogram_vec_with_registry,
    register_int_counter_with_registry, CounterVec, HistogramVec, IntCounter, Registry,
};

pub struct Metrics {
    pub miss: IntCounter,

    pub latency: HistogramVec,
    pub bytes: CounterVec,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new(Registry::new())
    }
}

impl Metrics {
    pub fn new(registry: Registry) -> Self {
        let miss =
            register_int_counter_with_registry!("foyer_cache_miss", "file cache miss", registry)
                .unwrap();

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

        Self {
            miss,
            latency,
            bytes,
        }
    }
}
