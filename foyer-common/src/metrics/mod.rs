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

use std::fmt::Debug;

/// Counter metric operations.
pub trait CounterOps: Send + Sync + 'static + Debug {
    /// Increase record by `val`.
    fn increase(&self, val: u64);
}

/// Gauge metric operations.
pub trait GaugeOps: Send + Sync + 'static + Debug {
    /// Increase record by `val`.
    fn increase(&self, val: u64);
    /// Decrease record by `val`.
    fn decrease(&self, val: u64);
    /// Set the record as a absolute value `val`.
    fn absolute(&self, val: u64);
}

/// Histogram metric operations.
pub trait HistogramOps: Send + Sync + 'static + Debug {
    /// Record a value.
    fn record(&self, val: f64);
}

/// A vector of counters.
pub trait CounterVecOps: Send + Sync + 'static + Debug {
    /// Get a counter within the vector of counters.
    fn counter(&self, labels: &[&'static str]) -> impl CounterOps;
}

/// A vector of gauges.
pub trait GaugeVecOps: Send + Sync + 'static + Debug {
    /// Get a gauge within the vector of gauges.
    fn gauge(&self, labels: &[&'static str]) -> impl GaugeOps;
}

/// A vector of histograms.
pub trait HistogramVecOps: Send + Sync + 'static + Debug {
    /// Get a histogram within the vector of histograms.
    fn histogram(&self, labels: &[&'static str]) -> impl HistogramOps;
}

/// Metrics registry.
pub trait RegistryOps: Send + Sync + 'static + Debug {
    /// Register a vector of counters to the registry.
    fn register_counter_vec(
        &self,
        name: &'static str,
        desc: &'static str,
        label_names: &'static [&'static str],
    ) -> impl CounterVecOps;

    /// Register a vector of gauges to the registry.
    fn register_gauge_vec(
        &self,
        name: &'static str,
        desc: &'static str,
        label_names: &'static [&'static str],
    ) -> impl GaugeVecOps;

    /// Register a vector of histograms to the registry.
    fn register_histogram_vec(
        &self,
        name: &'static str,
        desc: &'static str,
        label_names: &'static [&'static str],
    ) -> impl HistogramVecOps;
}

/// Boxed generic counter.
pub type BoxedCounter = Box<dyn CounterOps>;
/// Boxed generic gauge.
pub type BoxedGauge = Box<dyn GaugeOps>;
/// Boxed generic histogram.
pub type BoxedHistogram = Box<dyn HistogramOps>;

/// Shared metrics model.
pub mod model;
/// Provisioned metrics registries.
pub mod registry;
