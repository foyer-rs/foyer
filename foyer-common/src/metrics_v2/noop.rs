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

use super::{CounterOps, CounterVecOps, GaugeOps, GaugeVecOps, HistogramOps, HistogramVecOps, RegistryOps};

/// Noop metrics placeholder.
#[derive(Debug)]
pub struct Noop;

impl CounterOps for Noop {
    fn increase(&self, _: u64) {}
}

impl CounterVecOps for Noop {
    fn counter(&self, _: &[&str]) -> impl CounterOps {
        Noop
    }
}

impl GaugeOps for Noop {
    fn increase(&self, _: u64) {}

    fn decrease(&self, _: u64) {}

    fn absolute(&self, _: u64) {}
}

impl GaugeVecOps for Noop {
    fn gauge(&self, _: &[&str]) -> impl GaugeOps {
        Noop
    }
}

impl HistogramOps for Noop {
    fn record(&self, _: f64) {}
}

impl HistogramVecOps for Noop {
    fn histogram(&self, _: &[&str]) -> impl HistogramOps {
        Noop
    }
}

impl RegistryOps for Noop {
    fn register_counter_vec(&self, _: &'static str, _: &'static str, _: &'static [&'static str]) -> impl CounterVecOps {
        Noop
    }

    fn register_gauge_vec(&self, _: &'static str, _: &'static str, _: &'static [&'static str]) -> impl GaugeVecOps {
        Noop
    }

    fn register_histogram_vec(
        &self,
        _: &'static str,
        _: &'static str,
        _: &'static [&'static str],
    ) -> impl HistogramVecOps {
        Noop
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let noop = Noop;

        let cv = noop.register_counter_vec("test_counter_1", "test counter 1", &["label1", "label2"]);
        let c = cv.counter(&["l1", "l2"]);
        c.increase(42);

        let gv = noop.register_gauge_vec("test_gauge_1", "test gauge 1", &["label1", "label2"]);
        let g = gv.gauge(&["l1", "l2"]);
        g.increase(514);
        g.decrease(114);
        g.absolute(114514);

        let hv = noop.register_histogram_vec("test_histogram_1", "test histogram 1", &["label1", "label2"]);
        let h = hv.histogram(&["l1", "l2"]);
        h.record(3.1415926);
    }
}
