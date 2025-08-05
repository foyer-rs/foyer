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

use crate::io::device::statistics::Statistics;

/// Filter result for admission pickers and reinsertion pickers.
#[derive(Debug, Clone, Copy)]
pub enum FilterResult {
    /// Admittion.
    Admit,
    /// Rejection.
    Reject,
    /// This result indicates that the disk cache is throttled caused by the current io throttle.
    /// The minimal duration to retry this submission is returned for the caller to decide whether to retry it later.
    Throttled(Duration),
}

impl FilterResult {
    /// Convert the filter result to a boolean value.
    pub fn is_admitted(&self) -> bool {
        matches!(self, FilterResult::Admit)
    }

    /// Convert the filter result to a boolean value.
    pub fn is_rejected(&self) -> bool {
        matches!(self, FilterResult::Reject)
    }
}

/// Condition for [`Filter`].
pub trait FilterCondition: Send + Sync + Debug + 'static {
    /// Decide whether to pick an entry by hash.
    fn filter(&self, stats: &Arc<Statistics>, hash: u64, estimated_size: usize) -> FilterResult;
}

/// [`Filter`] filters entries based on multiple conditions for admission and reinsertion.
///
/// [`Filter`] admits all entries if no conditions are set.
#[derive(Debug, Default)]
pub struct Filter {
    conditions: Vec<Box<dyn FilterCondition>>,
}

impl Filter {
    /// Create a new empty filter.
    pub fn new() -> Self {
        Self::default()
    }

    /// Push a new condition to the filter.
    pub fn with_condition<C: FilterCondition>(mut self, condition: C) -> Self {
        self.conditions.push(Box::new(condition));
        self
    }

    /// Check if the entry can be admitted by the filter conditions.
    pub fn filter(&self, stats: &Arc<Statistics>, hash: u64, estimated_size: usize) -> FilterResult {
        let mut duration = Duration::ZERO;
        for condition in &self.conditions {
            match condition.filter(stats, hash, estimated_size) {
                FilterResult::Admit => {}
                FilterResult::Reject => return FilterResult::Reject,
                FilterResult::Throttled(dur) => duration += dur,
            }
        }
        if duration.is_zero() {
            FilterResult::Admit
        } else {
            FilterResult::Throttled(duration)
        }
    }
}

pub mod conditions {

    pub use super::*;

    /// Admit all entries.
    #[derive(Debug, Default)]
    pub struct AdmitAll;

    impl FilterCondition for AdmitAll {
        fn filter(&self, _: &Arc<Statistics>, _: u64, _: usize) -> FilterResult {
            FilterResult::Admit
        }
    }

    /// Reject all entries.
    #[derive(Debug, Default)]
    pub struct RejectAll;

    impl FilterCondition for RejectAll {
        fn filter(&self, _: &Arc<Statistics>, _: u64, _: usize) -> FilterResult {
            FilterResult::Reject
        }
    }

    #[derive(Debug, Default)]
    pub struct IoThrottle;

    impl FilterCondition for IoThrottle {
        fn filter(&self, stats: &Arc<Statistics>, _: u64, _: usize) -> FilterResult {
            let duration = stats.write_throttle();
            if duration.is_zero() {
                FilterResult::Admit
            } else {
                FilterResult::Throttled(duration)
            }
        }
    }
}
