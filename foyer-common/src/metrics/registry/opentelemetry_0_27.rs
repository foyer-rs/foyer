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

use std::{
    borrow::Cow,
    sync::atomic::{AtomicU64, Ordering},
};

use itertools::Itertools;
use opentelemetry::{
    metrics::{Counter as OtCounter, Gauge as OtGauge, Histogram as OtHistogram, Meter},
    KeyValue,
};
use opentelemetry_0_27 as opentelemetry;

use crate::metrics::{
    BoxedCounter, BoxedCounterVec, BoxedGauge, BoxedGaugeVec, BoxedHistogram, BoxedHistogramVec, Boxer, CounterOps,
    CounterVecOps, GaugeOps, GaugeVecOps, HistogramOps, HistogramVecOps, RegistryOps,
};

/// OpenTelemetry counter metric.
#[derive(Debug)]
pub struct Counter {
    counter: OtCounter<u64>,
    labels: Vec<KeyValue>,
}

impl CounterOps for Counter {
    fn increase(&self, val: u64) {
        self.counter.add(val, &self.labels);
    }
}

/// OpenTelemetry gauge metric.
#[derive(Debug)]
pub struct Gauge {
    val: AtomicU64,
    gauge: OtGauge<u64>,
    labels: Vec<KeyValue>,
}

impl GaugeOps for Gauge {
    fn increase(&self, val: u64) {
        let v = self.val.fetch_add(val, Ordering::Relaxed) + val;
        self.gauge.record(v, &self.labels);
    }

    fn decrease(&self, val: u64) {
        let v = self.val.fetch_sub(val, Ordering::Relaxed) - val;
        self.gauge.record(v, &self.labels);
    }

    fn absolute(&self, val: u64) {
        self.val.store(val, Ordering::Relaxed);
        self.gauge.record(val, &self.labels);
    }
}

/// OpenTelemetry histogram metric.
#[derive(Debug)]
pub struct Histogram {
    histogram: OtHistogram<f64>,
    labels: Vec<KeyValue>,
}

impl HistogramOps for Histogram {
    fn record(&self, val: f64) {
        self.histogram.record(val, &self.labels);
    }
}

/// OpenTelemetry metric vector.
#[derive(Debug)]
pub struct MetricVec {
    meter: Meter,
    name: Cow<'static, str>,
    desc: Cow<'static, str>,
    label_names: &'static [&'static str],
}

impl CounterVecOps for MetricVec {
    fn counter(&self, labels: &[Cow<'static, str>]) -> BoxedCounter {
        let counter = self
            .meter
            .u64_counter(self.name.clone())
            .with_description(self.desc.clone())
            .build();
        let labels = self
            .label_names
            .iter()
            .zip_eq(labels.iter())
            .map(|(name, label)| KeyValue::new(name.to_string(), label.clone()))
            .collect();
        Counter { counter, labels }.boxed()
    }
}

impl GaugeVecOps for MetricVec {
    fn gauge(&self, labels: &[Cow<'static, str>]) -> BoxedGauge {
        let gauge = self
            .meter
            .u64_gauge(self.name.clone())
            .with_description(self.desc.clone())
            .build();
        let labels = self
            .label_names
            .iter()
            .zip_eq(labels.iter())
            .map(|(name, label)| KeyValue::new(name.to_string(), label.clone()))
            .collect();
        let val = AtomicU64::new(0);
        Gauge { val, gauge, labels }.boxed()
    }
}

impl HistogramVecOps for MetricVec {
    fn histogram(&self, labels: &[Cow<'static, str>]) -> BoxedHistogram {
        let histogram = self
            .meter
            .f64_histogram(self.name.clone())
            .with_description(self.desc.clone())
            .build();
        let labels = self
            .label_names
            .iter()
            .zip_eq(labels.iter())
            .map(|(name, label)| KeyValue::new(name.to_string(), label.clone()))
            .collect();
        Histogram { histogram, labels }.boxed()
    }
}

/// OpenTelemetry metrics registry.
#[derive(Debug)]
pub struct OpenTelemetryMetricsRegistry {
    meter: Meter,
}

impl OpenTelemetryMetricsRegistry {
    /// Create an OpenTelemetry metrics registry.
    pub fn new(meter: Meter) -> Self {
        Self { meter }
    }
}

impl RegistryOps for OpenTelemetryMetricsRegistry {
    fn register_counter_vec(
        &self,
        name: Cow<'static, str>,
        desc: Cow<'static, str>,
        label_names: &'static [&'static str],
    ) -> BoxedCounterVec {
        MetricVec {
            meter: self.meter.clone(),
            name,
            desc,
            label_names,
        }
        .boxed()
    }

    fn register_gauge_vec(
        &self,
        name: Cow<'static, str>,
        desc: Cow<'static, str>,
        label_names: &'static [&'static str],
    ) -> BoxedGaugeVec {
        MetricVec {
            meter: self.meter.clone(),
            name,
            desc,
            label_names,
        }
        .boxed()
    }

    fn register_histogram_vec(
        &self,
        name: Cow<'static, str>,
        desc: Cow<'static, str>,
        label_names: &'static [&'static str],
    ) -> BoxedHistogramVec {
        MetricVec {
            meter: self.meter.clone(),
            name,
            desc,
            label_names,
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let meter = opentelemetry::global::meter("test");
        let ot = OpenTelemetryMetricsRegistry::new(meter);

        let cv = ot.register_counter_vec("test_counter_1".into(), "test counter 1".into(), &["label1", "label2"]);
        let c = cv.counter(&["l1".into(), "l2".into()]);
        c.increase(42);

        let gv = ot.register_gauge_vec("test_gauge_1".into(), "test gauge 1".into(), &["label1", "label2"]);
        let g = gv.gauge(&["l1".into(), "l2".into()]);
        g.increase(514);
        g.decrease(114);
        g.absolute(114514);

        let hv = ot.register_histogram_vec(
            "test_histogram_1".into(),
            "test histogram 1".into(),
            &["label1", "label2"],
        );
        let h = hv.histogram(&["l1".into(), "l2".into()]);
        h.record(114.514);
    }
}
