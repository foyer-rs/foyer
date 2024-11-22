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

use prometheus::{
    register_histogram_vec_with_registry, register_int_counter_vec_with_registry, register_int_gauge_vec_with_registry,
    Histogram, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Registry,
};

use super::{CounterOps, CounterVecOps, GaugeOps, GaugeVecOps, HistogramOps, HistogramVecOps, RegistryOps};

impl CounterOps for IntCounter {
    fn increase(&self, val: u64) {
        self.inc_by(val);
    }
}

impl CounterVecOps for IntCounterVec {
    fn counter(&self, labels: &[&str]) -> impl CounterOps {
        self.with_label_values(labels)
    }
}

impl GaugeOps for IntGauge {
    fn increase(&self, val: u64) {
        self.add(val as _);
    }

    fn decrease(&self, val: u64) {
        self.sub(val as _);
    }

    fn absolute(&self, val: u64) {
        self.set(val as _);
    }
}

impl GaugeVecOps for IntGaugeVec {
    fn gauge(&self, labels: &[&str]) -> impl GaugeOps {
        self.with_label_values(labels)
    }
}

impl HistogramOps for Histogram {
    fn record(&self, val: f64) {
        self.observe(val);
    }
}

impl HistogramVecOps for HistogramVec {
    fn histogram(&self, labels: &[&str]) -> impl HistogramOps {
        self.with_label_values(labels)
    }
}

/// Prometheus metrics registry.
pub struct Prometheus {
    registry: Registry,
}

impl Prometheus {
    /// Create an Prometheus metrics registry.
    pub fn new(registry: Registry) -> Self {
        Self { registry }
    }
}

impl RegistryOps for Prometheus {
    fn register_counter_vec(
        &self,
        name: &'static str,
        desc: &'static str,
        label_names: &'static [&'static str],
    ) -> impl CounterVecOps {
        register_int_counter_vec_with_registry! {
            name, desc, label_names, self.registry
        }
        .unwrap()
    }

    fn register_gauge_vec(
        &self,
        name: &'static str,
        desc: &'static str,
        label_names: &'static [&'static str],
    ) -> impl GaugeVecOps {
        register_int_gauge_vec_with_registry! {
            name, desc, label_names, self.registry
        }
        .unwrap()
    }

    fn register_histogram_vec(
        &self,
        name: &'static str,
        desc: &'static str,
        label_names: &'static [&'static str],
    ) -> impl HistogramVecOps {
        register_histogram_vec_with_registry! {
            name, desc, label_names, self.registry
        }
        .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let registry = Registry::new();
        let p8s = Prometheus::new(registry);

        let cv = p8s.register_counter_vec("test_counter_1", "test counter 1", &["label1", "label2"]);
        let c = cv.counter(&["l1", "l2"]);
        c.increase(42);

        let gv = p8s.register_gauge_vec("test_gauge_1", "test gauge 1", &["label1", "label2"]);
        let g = gv.gauge(&["l1", "l2"]);
        g.increase(514);
        g.decrease(114);
        g.absolute(114514);

        let hv = p8s.register_histogram_vec("test_histogram_1", "test histogram 1", &["label1", "label2"]);
        let h = hv.histogram(&["l1", "l2"]);
        h.record(3.1415926);
    }
}
