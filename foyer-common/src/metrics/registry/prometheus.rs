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
    collections::HashMap,
    hash::{Hash, Hasher},
    sync::{Arc, LazyLock},
};

use itertools::Itertools;
use parking_lot::Mutex;
use prometheus::{
    register_histogram_vec_with_registry, register_int_counter_vec_with_registry, register_int_gauge_vec_with_registry,
    Histogram, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Registry,
};

use crate::{
    metrics::{
        BoxedCounter, BoxedCounterVec, BoxedGauge, BoxedGaugeVec, BoxedHistogram, BoxedHistogramVec, Boxer, CounterOps,
        CounterVecOps, GaugeOps, GaugeVecOps, HistogramOps, HistogramVecOps, RegistryOps,
    },
    scope::Scope,
};

static METRICS: LazyLock<Mutex<HashMap<PrometheusMetricsRegistry, HashMap<Metadata, MetricVec>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn get_or_register_counter_vec(registry: &PrometheusMetricsRegistry, metadata: Metadata) -> IntCounterVec {
    let vec = METRICS.lock().with(|mut metrics| {
        metrics
            .get_mut(registry)
            .expect("registry must be registered when creating")
            .entry(metadata.clone())
            .or_insert_with(|| {
                MetricVec::Counter(
                    register_int_counter_vec_with_registry! {
                        metadata.name, metadata.desc, metadata.label_names, registry.registry
                    }
                    .unwrap(),
                )
            })
            .clone()
    });
    match vec {
        MetricVec::Counter(v) => v,
        _ => unreachable!(),
    }
}

fn get_or_register_gauge_vec(registry: &PrometheusMetricsRegistry, metadata: Metadata) -> IntGaugeVec {
    let vec = METRICS.lock().with(|mut metrics| {
        metrics
            .get_mut(registry)
            .expect("registry must be registered when creating")
            .entry(metadata.clone())
            .or_insert_with(|| {
                MetricVec::Gauge(
                    register_int_gauge_vec_with_registry! {
                        metadata.name, metadata.desc, metadata.label_names, registry.registry
                    }
                    .unwrap(),
                )
            })
            .clone()
    });
    match vec {
        MetricVec::Gauge(v) => v,
        _ => unreachable!(),
    }
}

fn get_or_register_histogram_vec(registry: &PrometheusMetricsRegistry, metadata: Metadata) -> HistogramVec {
    let vec = METRICS.lock().with(|mut metrics| {
        metrics
            .get_mut(registry)
            .expect("registry must be registered when creating")
            .entry(metadata.clone())
            .or_insert_with(|| {
                MetricVec::Histogram(
                    register_histogram_vec_with_registry! {
                        metadata.name, metadata.desc, metadata.label_names, registry.registry
                    }
                    .unwrap(),
                )
            })
            .clone()
    });
    match vec {
        MetricVec::Histogram(v) => v,
        _ => unreachable!(),
    }
}

#[derive(Debug, Clone)]
enum MetricVec {
    Counter(IntCounterVec),
    Gauge(IntGaugeVec),
    Histogram(HistogramVec),
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct Metadata {
    name: Cow<'static, str>,
    desc: Cow<'static, str>,
    label_names: &'static [&'static str],
}

impl CounterOps for IntCounter {
    fn increase(&self, val: u64) {
        self.inc_by(val);
    }
}

impl CounterVecOps for IntCounterVec {
    fn counter(&self, labels: &[Cow<'static, str>]) -> BoxedCounter {
        let labels = labels.iter().map(Cow::as_ref).collect_vec();
        self.with_label_values(&labels).boxed()
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
    fn gauge(&self, labels: &[Cow<'static, str>]) -> BoxedGauge {
        let labels = labels.iter().map(Cow::as_ref).collect_vec();
        self.with_label_values(&labels).boxed()
    }
}

impl HistogramOps for Histogram {
    fn record(&self, val: f64) {
        self.observe(val);
    }
}

impl HistogramVecOps for HistogramVec {
    fn histogram(&self, labels: &[Cow<'static, str>]) -> BoxedHistogram {
        let labels = labels.iter().map(Cow::as_ref).collect_vec();
        self.with_label_values(&labels).boxed()
    }
}

/// Prometheus metric registry with lib `prometheus`.
///
/// The [`PrometheusMetricsRegistry`] can be cloned and used by multiple foyer instances, without worrying about
/// duplicately registering.
#[derive(Debug, Clone)]
pub struct PrometheusMetricsRegistry {
    registry: Arc<Registry>,
}

impl PartialEq for PrometheusMetricsRegistry {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.registry, &other.registry)
    }
}

impl Eq for PrometheusMetricsRegistry {}

impl Hash for PrometheusMetricsRegistry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.registry).hash(state);
    }
}

impl PrometheusMetricsRegistry {
    /// Create an Prometheus metrics registry.
    pub fn new(registry: Registry) -> Self {
        let registry = Arc::new(registry);
        let this = Self { registry };
        METRICS.lock().insert(this.clone(), HashMap::new());
        this
    }
}

impl RegistryOps for PrometheusMetricsRegistry {
    fn register_counter_vec(
        &self,
        name: Cow<'static, str>,
        desc: Cow<'static, str>,
        label_names: &'static [&'static str],
    ) -> BoxedCounterVec {
        get_or_register_counter_vec(
            self,
            Metadata {
                name,
                desc,
                label_names,
            },
        )
        .boxed()
    }

    fn register_gauge_vec(
        &self,
        name: Cow<'static, str>,
        desc: Cow<'static, str>,
        label_names: &'static [&'static str],
    ) -> BoxedGaugeVec {
        get_or_register_gauge_vec(
            self,
            Metadata {
                name,
                desc,
                label_names,
            },
        )
        .boxed()
    }

    fn register_histogram_vec(
        &self,
        name: Cow<'static, str>,
        desc: Cow<'static, str>,
        label_names: &'static [&'static str],
    ) -> BoxedHistogramVec {
        get_or_register_histogram_vec(
            self,
            Metadata {
                name,
                desc,
                label_names,
            },
        )
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn case(registry: &PrometheusMetricsRegistry) {
        let cv = registry.register_counter_vec("test_counter_1".into(), "test counter 1".into(), &["label1", "label2"]);
        let c = cv.counter(&["l1".into(), "l2".into()]);
        c.increase(42);

        let gv = registry.register_gauge_vec("test_gauge_1".into(), "test gauge 1".into(), &["label1", "label2"]);
        let g = gv.gauge(&["l1".into(), "l2".into()]);
        g.increase(514);
        g.decrease(114);
        g.absolute(114514);

        let hv = registry.register_histogram_vec(
            "test_histogram_1".into(),
            "test histogram 1".into(),
            &["label1", "label2"],
        );
        let h = hv.histogram(&["l1".into(), "l2".into()]);
        h.record(114.514);
    }

    #[test]
    fn test_prometheus_metrics_registry() {
        let registry = Registry::new();
        let p8s = PrometheusMetricsRegistry::new(registry);
        case(&p8s);
    }

    #[should_panic]
    #[test]
    fn test_duplicated_prometheus_metrics_registry_wrongly() {
        let registry = Registry::new();
        let p8s1 = PrometheusMetricsRegistry::new(registry.clone());
        let p8s2 = PrometheusMetricsRegistry::new(registry);
        case(&p8s1);
        case(&p8s2);
    }

    #[test]
    fn test_duplicated_prometheus_metrics_registry() {
        let registry = Registry::new();
        let p8s1 = PrometheusMetricsRegistry::new(registry);
        let p8s2 = p8s1.clone();
        case(&p8s1);
        case(&p8s2);
    }
}
