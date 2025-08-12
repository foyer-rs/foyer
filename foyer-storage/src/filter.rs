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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageFilterResult {
    /// Admittion.
    Admit,
    /// Rejection.
    Reject,
    /// This result indicates that the disk cache is throttled caused by the current io throttle.
    /// The minimal duration to retry this submission is returned for the caller to decide whether to retry it later.
    Throttled(Duration),
}

impl StorageFilterResult {
    /// Convert the filter result to a boolean value.
    pub fn is_admitted(&self) -> bool {
        matches!(self, StorageFilterResult::Admit)
    }

    /// Convert the filter result to a boolean value.
    pub fn is_rejected(&self) -> bool {
        matches!(self, StorageFilterResult::Reject)
    }
}

/// Condition for [`StorageFilter`].
pub trait StorageFilterCondition: Send + Sync + Debug + 'static {
    /// Decide whether to pick an entry by hash.
    fn filter(&self, stats: &Arc<Statistics>, hash: u64, estimated_size: usize) -> StorageFilterResult;
}

/// [`StorageFilter`] filters entries based on multiple conditions for admission and reinsertion.
///
/// [`StorageFilter`] admits all entries if no conditions are set.
#[derive(Debug, Default)]
pub struct StorageFilter {
    conditions: Vec<Box<dyn StorageFilterCondition>>,
}

impl StorageFilter {
    /// Create a new empty filter.
    pub fn new() -> Self {
        Self::default()
    }

    /// Push a new condition to the filter.
    pub fn with_condition<C: StorageFilterCondition>(mut self, condition: C) -> Self {
        self.conditions.push(Box::new(condition));
        self
    }

    /// Check if the entry can be admitted by the filter conditions.
    pub fn filter(&self, stats: &Arc<Statistics>, hash: u64, estimated_size: usize) -> StorageFilterResult {
        let mut duration = Duration::ZERO;
        for condition in &self.conditions {
            match condition.filter(stats, hash, estimated_size) {
                StorageFilterResult::Admit => {}
                StorageFilterResult::Reject => return StorageFilterResult::Reject,
                StorageFilterResult::Throttled(dur) => duration += dur,
            }
        }
        if duration.is_zero() {
            StorageFilterResult::Admit
        } else {
            StorageFilterResult::Throttled(duration)
        }
    }
}

pub mod conditions {

    use std::ops::{Bound, Range, RangeBounds};

    pub use super::*;

    /// Admit all entries.
    #[derive(Debug, Default)]
    pub struct AdmitAll;

    impl StorageFilterCondition for AdmitAll {
        fn filter(&self, _: &Arc<Statistics>, _: u64, _: usize) -> StorageFilterResult {
            StorageFilterResult::Admit
        }
    }

    /// Reject all entries.
    #[derive(Debug, Default)]
    pub struct RejectAll;

    impl StorageFilterCondition for RejectAll {
        fn filter(&self, _: &Arc<Statistics>, _: u64, _: usize) -> StorageFilterResult {
            StorageFilterResult::Reject
        }
    }

    #[derive(Debug, Default)]
    pub struct IoThrottle;

    impl StorageFilterCondition for IoThrottle {
        fn filter(&self, stats: &Arc<Statistics>, _: u64, _: usize) -> StorageFilterResult {
            let duration = stats.write_throttle();
            if duration.is_zero() {
                StorageFilterResult::Admit
            } else {
                StorageFilterResult::Throttled(duration)
            }
        }
    }

    /// A condition that checks if the estimated size is within a specified range.
    #[derive(Debug)]
    pub struct EstimatedSize {
        range: Range<usize>,
    }

    impl EstimatedSize {
        /// Create a new `EstimatedSize` condition with a specified range.
        pub fn new<R: RangeBounds<usize>>(range: R) -> Self {
            let start = match range.start_bound() {
                Bound::Included(v) => *v,
                Bound::Excluded(v) => *v + 1,
                Bound::Unbounded => 0,
            };
            let end = match range.end_bound() {
                Bound::Included(v) => *v + 1,
                Bound::Excluded(v) => *v,
                Bound::Unbounded => usize::MAX,
            };
            Self { range: start..end }
        }
    }

    impl StorageFilterCondition for EstimatedSize {
        fn filter(&self, _: &Arc<Statistics>, _: u64, estimated_size: usize) -> StorageFilterResult {
            if self.range.contains(&estimated_size) {
                StorageFilterResult::Admit
            } else {
                StorageFilterResult::Reject
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::conditions::*;
    use crate::Throttle;

    #[test]
    fn test_estimated_size_condition() {
        let condition = EstimatedSize::new(10..20);
        let statistics = Arc::new(Statistics::new(Throttle::default()));
        assert_eq!(condition.filter(&statistics, 0, 15), StorageFilterResult::Admit);
        assert_eq!(condition.filter(&statistics, 0, 5), StorageFilterResult::Reject);
        assert_eq!(condition.filter(&statistics, 0, 20), StorageFilterResult::Reject);
    }
}
