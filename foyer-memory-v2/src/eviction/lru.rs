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

use std::ptr::NonNull;

use foyer_common::{
    assert::OptionExt,
    code::{Key, Value},
    strict_assert, strict_assert_eq,
};
use foyer_intrusive_v2::{
    adapter::Link,
    dlist::{Dlist, DlistLink},
    intrusive_adapter,
};
use serde::{Deserialize, Serialize};

use crate::Record;

use super::{Eviction, Operator};

/// Lru eviction algorithm config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LruConfig {
    /// The ratio of the high priority pool occupied.
    ///
    /// [`Lru`] guarantees that the high priority weight are always as larger as
    /// but no larger that the capacity * high priority pool ratio.
    ///
    /// # Panic
    ///
    /// Panics if the value is not in [0, 1.0].
    pub high_priority_pool_ratio: f64,
}

impl Default for LruConfig {
    fn default() -> Self {
        Self {
            high_priority_pool_ratio: 0.9,
        }
    }
}

/// Lru eviction algorithm hint.
#[derive(Debug, Clone)]
pub enum LruHint {
    HighPriority,
    LowPriority,
}

impl Default for LruHint {
    fn default() -> Self {
        Self::HighPriority
    }
}

/// Lru eviction algorithm state.
#[derive(Debug, Default)]
pub struct LruState {
    link: DlistLink,
    in_high_priority_pool: bool,
}

intrusive_adapter! { Adapter<K, V> = Record<Lru<K, V>> { state.link => DlistLink } where K: Key, V: Value }

pub struct Lru<K, V>
where
    K: Key,
    V: Value,
{
    high_priority_list: Dlist<Adapter<K, V>>,
    list: Dlist<Adapter<K, V>>,

    high_priority_weight: usize,
    high_priority_weight_capacity: usize,
}

impl<K, V> Lru<K, V>
where
    K: Key,
    V: Value,
{
    fn may_overflow_high_priority_pool(&mut self) {
        while self.high_priority_weight > self.high_priority_weight_capacity {
            strict_assert!(!self.high_priority_list.is_empty());

            // overflow last entry in high priority pool to low priority pool
            let mut ptr = unsafe { self.high_priority_list.pop_front().strict_unwrap_unchecked() };
            let record = unsafe { ptr.as_mut() };
            strict_assert!(record.state.in_high_priority_pool);
            record.state.in_high_priority_pool = false;
            self.high_priority_weight -= record.weight();
            self.list.push_back(ptr);
        }
    }
}

impl<K, V> Eviction for Lru<K, V>
where
    K: Key,
    V: Value,
{
    type Config = LruConfig;
    type Key = K;
    type Value = V;
    type Hint = LruHint;
    type State = LruState;

    fn new(capacity: usize, config: &Self::Config) -> Self
    where
        Self: Sized,
    {
        assert!(
            (0.0..=1.0).contains(&config.high_priority_pool_ratio),
            "high_priority_pool_ratio_percentage must be in 0.0..=1.0, given: {}",
            config.high_priority_pool_ratio
        );

        let high_priority_weight_capacity = (capacity as f64 * config.high_priority_pool_ratio) as usize;

        Self {
            high_priority_list: Dlist::new(),
            list: Dlist::new(),
            high_priority_weight: 0,
            high_priority_weight_capacity,
        }
    }

    fn update(&mut self, capacity: usize, config: &Self::Config) {
        if !(0.0..=1.0).contains(&config.high_priority_pool_ratio) {
            tracing::error!(
                "[fifo]: high_priority_pool_ratio_percentage must be in 0.0..=1.0, given: {}, new configuration ignored",
                config.high_priority_pool_ratio
            );
            return;
        }

        let high_priority_weight_capacity = (capacity as f64 * config.high_priority_pool_ratio) as usize;
        self.high_priority_weight_capacity = high_priority_weight_capacity;

        self.may_overflow_high_priority_pool();
    }

    fn push(&mut self, mut ptr: NonNull<Record<Self>>) {
        let record = unsafe { ptr.as_mut() };

        strict_assert!(!record.state.link.is_linked());

        match record.hint() {
            LruHint::HighPriority => {
                record.state.in_high_priority_pool = true;
                self.high_priority_weight += record.weight();
                self.high_priority_list.push_back(ptr);

                self.may_overflow_high_priority_pool();
            }
            LruHint::LowPriority => {
                record.state.in_high_priority_pool = false;
                self.list.push_back(ptr);
            }
        }

        record.set_in_eviction(true);
    }

    fn pop(&mut self) -> Option<NonNull<Record<Self>>> {
        let mut ptr = self.list.pop_front().or_else(|| self.high_priority_list.pop_front())?;

        let record = unsafe { ptr.as_mut() };
        strict_assert!(!record.state.link.is_linked());

        if record.state.in_high_priority_pool {
            self.high_priority_weight -= record.weight();
            record.state.in_high_priority_pool = false;
        }

        record.set_in_eviction(false);

        Some(ptr)
    }

    fn remove(&mut self, mut ptr: NonNull<Record<Self>>) {
        let record = unsafe { ptr.as_mut() };
        strict_assert!(record.state.link.is_linked());

        if record.state.in_high_priority_pool {
            self.high_priority_weight -= record.weight();
            self.high_priority_list.remove(ptr);
            record.state.in_high_priority_pool = false;
        } else {
            self.list.remove(ptr);
        }

        strict_assert!(!record.state.link.is_linked());

        record.set_in_eviction(false);
    }

    fn clear(&mut self) {
        while let Some(mut ptr) = self.list.pop_front() {
            unsafe { ptr.as_mut() }.set_in_eviction(false);
        }
        while let Some(mut ptr) = self.high_priority_list.pop_front() {
            let record = unsafe { ptr.as_mut() };
            record.set_in_eviction(false);
            record.state.in_high_priority_pool = false;
            self.high_priority_weight -= record.weight();
        }

        strict_assert_eq!(self.high_priority_weight, 0);
    }

    fn len(&self) -> usize {
        self.high_priority_list.len() + self.list.len()
    }

    fn acquire_operator() -> super::Operator {
        Operator::Immutable
    }

    fn acquire_immutable(&self, _ptr: NonNull<Record<Self>>) {}

    fn acquire_mutable(&mut self, _ptr: NonNull<Record<Self>>) {
        unreachable!()
    }

    fn release(&mut self, mut ptr: NonNull<Record<Self>>) {
        let record = unsafe { ptr.as_mut() };

        if record.is_in_eviction() {
            strict_assert!(record.state.link.is_linked());
            self.remove(ptr);
            self.push(ptr);
        } else {
            strict_assert!(!record.state.link.is_linked());
            self.push(ptr);
        }

        strict_assert!(record.is_in_eviction());
    }
}
