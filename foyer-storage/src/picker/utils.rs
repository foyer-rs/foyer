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

use std::{fmt::Debug, sync::Arc, time::Duration};

use super::{AdmissionPicker, Pick, ReinsertionPicker};
use crate::io::device::statistics::Statistics;

/// Only admit on all chained admission pickers pick.
#[derive(Debug, Default, Clone)]
pub struct ChainedAdmissionPicker {
    pickers: Arc<Vec<Arc<dyn AdmissionPicker>>>,
}

impl AdmissionPicker for ChainedAdmissionPicker {
    fn pick(&self, stats: &Arc<Statistics>, hash: u64) -> Pick {
        let mut duration = Duration::ZERO;
        for picker in self.pickers.iter() {
            match picker.pick(stats, hash) {
                Pick::Admit => {}
                Pick::Reject => return Pick::Reject,
                Pick::Throttled(dur) => duration += dur,
            }
        }
        if duration.is_zero() {
            Pick::Admit
        } else {
            Pick::Throttled(duration)
        }
    }
}

/// A builder for [`ChainedAdmissionPicker`].
#[derive(Debug, Default)]
pub struct ChainedAdmissionPickerBuilder {
    pickers: Vec<Arc<dyn AdmissionPicker>>,
}

impl ChainedAdmissionPickerBuilder {
    /// Chain a new admission picker.
    pub fn chain(mut self, picker: Arc<dyn AdmissionPicker>) -> Self {
        self.pickers.push(picker);
        self
    }

    /// Build the chained admission picker.
    pub fn build(self) -> ChainedAdmissionPicker {
        ChainedAdmissionPicker {
            pickers: Arc::new(self.pickers),
        }
    }
}

/// A picker that always returns `true`.
#[derive(Debug, Default)]
pub struct AdmitAllPicker;

impl AdmissionPicker for AdmitAllPicker {
    fn pick(&self, _: &Arc<Statistics>, _: u64) -> Pick {
        Pick::Admit
    }
}

impl ReinsertionPicker for AdmitAllPicker {
    fn pick(&self, _: &Arc<Statistics>, _: u64) -> Pick {
        Pick::Admit
    }
}

/// A picker that always returns `false`.
#[derive(Debug, Default)]
pub struct RejectAllPicker;

impl AdmissionPicker for RejectAllPicker {
    fn pick(&self, _: &Arc<Statistics>, _: u64) -> Pick {
        Pick::Reject
    }
}

impl ReinsertionPicker for RejectAllPicker {
    fn pick(&self, _: &Arc<Statistics>, _: u64) -> Pick {
        Pick::Reject
    }
}

#[derive(Debug, Default)]
pub struct IoThrottlerPicker;

impl AdmissionPicker for IoThrottlerPicker {
    fn pick(&self, stats: &Arc<Statistics>, _: u64) -> Pick {
        let duration = stats.write_throttle();
        if duration.is_zero() {
            Pick::Admit
        } else {
            Pick::Throttled(duration)
        }
    }
}
