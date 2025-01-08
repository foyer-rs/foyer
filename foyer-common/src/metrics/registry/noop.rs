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

use std::borrow::Cow;

use crate::metrics::{CounterOps, CounterVecOps, GaugeOps, GaugeVecOps, HistogramOps, HistogramVecOps, RegistryOps};

/// Noop metrics placeholder.
#[derive(Debug)]
pub struct NoopMetricsRegistry;

impl CounterOps for NoopMetricsRegistry {
    fn increase(&self, _: u64) {}
}

impl CounterVecOps for NoopMetricsRegistry {
    fn counter(&self, _: &[Cow<'static, str>]) -> impl CounterOps {
        NoopMetricsRegistry
    }
}

impl GaugeOps for NoopMetricsRegistry {
    fn increase(&self, _: u64) {}

    fn decrease(&self, _: u64) {}

    fn absolute(&self, _: u64) {}
}

impl GaugeVecOps for NoopMetricsRegistry {
    fn gauge(&self, _: &[Cow<'static, str>]) -> impl GaugeOps {
        NoopMetricsRegistry
    }
}

impl HistogramOps for NoopMetricsRegistry {
    fn record(&self, _: f64) {}
}

impl HistogramVecOps for NoopMetricsRegistry {
    fn histogram(&self, _: &[Cow<'static, str>]) -> impl HistogramOps {
        NoopMetricsRegistry
    }
}

impl RegistryOps for NoopMetricsRegistry {
    fn register_counter_vec(
        &self,
        _: impl Into<Cow<'static, str>>,
        _: impl Into<Cow<'static, str>>,
        _: &'static [&'static str],
    ) -> impl CounterVecOps {
        NoopMetricsRegistry
    }

    fn register_gauge_vec(
        &self,
        _: impl Into<Cow<'static, str>>,
        _: impl Into<Cow<'static, str>>,
        _: &'static [&'static str],
    ) -> impl GaugeVecOps {
        NoopMetricsRegistry
    }

    fn register_histogram_vec(
        &self,
        _: impl Into<Cow<'static, str>>,
        _: impl Into<Cow<'static, str>>,
        _: &'static [&'static str],
    ) -> impl HistogramVecOps {
        NoopMetricsRegistry
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let noop = NoopMetricsRegistry;

        let cv = noop.register_counter_vec("test_counter_1", "test counter 1", &["label1", "label2"]);
        let c = cv.counter(&["l1".into(), "l2".into()]);
        c.increase(42);

        let gv = noop.register_gauge_vec("test_gauge_1", "test gauge 1", &["label1", "label2"]);
        let g = gv.gauge(&["l1".into(), "l2".into()]);
        g.increase(514);
        g.decrease(114);
        g.absolute(114514);

        let hv = noop.register_histogram_vec("test_histogram_1", "test histogram 1", &["label1", "label2"]);
        let h = hv.histogram(&["l1".into(), "l2".into()]);
        h.record(114.514);
    }
}
