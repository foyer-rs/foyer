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

use std::sync::Arc;

use itertools::Itertools;
use parking_lot::Mutex;
use prometheus_client_0_22::{
    encoding::{EncodeLabel, EncodeLabelSet, LabelSetEncoder},
    metrics::{
        counter::Counter as PcCounter, family::Family, gauge::Gauge as PcGauge, histogram::Histogram as PcHistogram,
    },
    registry::Registry,
};

use crate::metrics::{CounterOps, CounterVecOps, GaugeOps, GaugeVecOps, HistogramOps, HistogramVecOps, RegistryOps};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Labels {
    pairs: Vec<(&'static str, &'static str)>,
}

impl EncodeLabelSet for Labels {
    fn encode(&self, mut encoder: LabelSetEncoder) -> Result<(), std::fmt::Error> {
        for pair in self.pairs.iter() {
            pair.encode(encoder.encode_label())?;
        }
        Ok(())
    }
}

#[derive(Debug)]
struct Counter {
    counter: Family<Labels, PcCounter>,
    labels: Labels,
}

impl CounterOps for Counter {
    fn increase(&self, val: u64) {
        self.counter.get_or_create(&self.labels).inc_by(val);
    }
}

#[derive(Debug)]
struct CounterVec {
    counter: Family<Labels, PcCounter>,
    label_names: &'static [&'static str],
}

impl CounterVecOps for CounterVec {
    fn counter(&self, labels: &[&'static str]) -> impl CounterOps {
        Counter {
            counter: self.counter.clone(),
            labels: Labels {
                pairs: self
                    .label_names
                    .iter()
                    .zip_eq(labels.iter())
                    .map(|(name, label)| (*name, *label))
                    .collect(),
            },
        }
    }
}

#[derive(Debug)]
struct Gauge {
    gauge: Family<Labels, PcGauge>,
    labels: Labels,
}

impl GaugeOps for Gauge {
    fn increase(&self, val: u64) {
        self.gauge.get_or_create(&self.labels).inc_by(val as _);
    }

    fn decrease(&self, val: u64) {
        self.gauge.get_or_create(&self.labels).dec_by(val as _);
    }

    fn absolute(&self, val: u64) {
        self.gauge.get_or_create(&self.labels).set(val as _);
    }
}

#[derive(Debug)]
struct GaugeVec {
    gauge: Family<Labels, PcGauge>,
    label_names: &'static [&'static str],
}

impl GaugeVecOps for GaugeVec {
    fn gauge(&self, labels: &[&'static str]) -> impl GaugeOps {
        Gauge {
            gauge: self.gauge.clone(),
            labels: Labels {
                pairs: self
                    .label_names
                    .iter()
                    .zip_eq(labels.iter())
                    .map(|(name, label)| (*name, *label))
                    .collect(),
            },
        }
    }
}

#[derive(Debug)]
struct Histogram {
    histogram: Family<Labels, PcHistogram>,
    labels: Labels,
}

impl HistogramOps for Histogram {
    fn record(&self, val: f64) {
        self.histogram.get_or_create(&self.labels).observe(val);
    }
}

#[derive(Debug)]
struct HistogramVec {
    histogram: Family<Labels, PcHistogram>,
    label_names: &'static [&'static str],
}

impl HistogramVecOps for HistogramVec {
    fn histogram(&self, labels: &[&'static str]) -> impl HistogramOps {
        Histogram {
            histogram: self.histogram.clone(),
            labels: Labels {
                pairs: self
                    .label_names
                    .iter()
                    .zip_eq(labels.iter())
                    .map(|(name, label)| (*name, *label))
                    .collect(),
            },
        }
    }
}

#[derive(Debug, Clone)]
/// Prometheus metric registry with lib `prometheus-client`.
pub struct PrometheusClientMetricsRegistry {
    registry: Arc<Mutex<Registry>>,
}

impl PrometheusClientMetricsRegistry {
    /// Create an Prometheus metrics registry.
    pub fn new(registry: Arc<Mutex<Registry>>) -> Self {
        Self { registry }
    }
}

impl RegistryOps for PrometheusClientMetricsRegistry {
    fn register_counter_vec(
        &self,
        name: &'static str,
        desc: &'static str,
        label_names: &'static [&'static str],
    ) -> impl CounterVecOps {
        let counter = Family::<Labels, PcCounter>::default();
        self.registry.lock().register(name, desc, counter.clone());
        CounterVec { counter, label_names }
    }

    fn register_gauge_vec(
        &self,
        name: &'static str,
        desc: &'static str,
        label_names: &'static [&'static str],
    ) -> impl GaugeVecOps {
        let gauge = Family::<Labels, PcGauge>::default();
        self.registry.lock().register(name, desc, gauge.clone());
        GaugeVec { gauge, label_names }
    }

    fn register_histogram_vec(
        &self,
        name: &'static str,
        desc: &'static str,
        label_names: &'static [&'static str],
    ) -> impl HistogramVecOps {
        let histogram = Family::<Labels, PcHistogram>::new_with_constructor(|| {
            PcHistogram::new([0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0].into_iter())
        });
        self.registry.lock().register(name, desc, histogram.clone());
        HistogramVec { histogram, label_names }
    }
}

#[cfg(test)]
mod tests {
    use prometheus_client_0_22::encoding::text::encode;

    use super::*;

    #[test]
    fn test() {
        let registry = Arc::new(Mutex::new(Registry::default()));
        let pc = PrometheusClientMetricsRegistry::new(registry.clone());

        let cv = pc.register_counter_vec("test_counter_1", "test counter 1", &["label1", "label2"]);
        let c = cv.counter(&["l1", "l2"]);
        c.increase(42);

        let gv = pc.register_gauge_vec("test_gauge_1", "test gauge 1", &["label1", "label2"]);
        let g = gv.gauge(&["l1", "l2"]);
        g.increase(514);
        g.decrease(114);
        g.absolute(114514);

        let hv = pc.register_histogram_vec("test_histogram_1", "test histogram 1", &["label1", "label2"]);
        let h = hv.histogram(&["l1", "l2"]);
        h.record(114.514);

        let mut text = String::new();
        encode(&mut text, &registry.lock()).unwrap();
        println!("{text}");
    }
}
