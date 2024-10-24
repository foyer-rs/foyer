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

use cmsketch::CMSketchU16;
use foyer_common::{
    assert::OptionExt,
    code::{Key, Value},
    strict_assert, strict_assert_eq, strict_assert_ne,
};
use foyer_intrusive_v2::{
    adapter::Link,
    dlist::{Dlist, DlistLink},
    intrusive_adapter,
};
use serde::{Deserialize, Serialize};

use crate::Record;

use super::{Eviction, Operator};

/// w-TinyLFU eviction algorithm config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LfuConfig {
    /// `window` capacity ratio of the total cache capacity.
    ///
    /// Must be in (0, 1).
    ///
    /// Must guarantee `window_capacity_ratio + protected_capacity_ratio < 1`.
    pub window_capacity_ratio: f64,
    /// `protected` capacity ratio of the total cache capacity.
    ///
    /// Must be in (0, 1).
    ///
    /// Must guarantee `window_capacity_ratio + protected_capacity_ratio < 1`.
    pub protected_capacity_ratio: f64,

    /// Error of the count-min sketch.
    ///
    /// See [`CMSketchU16::new`].
    pub cmsketch_eps: f64,

    /// Confidence of the count-min sketch.
    ///
    /// See [`CMSketchU16::new`].
    pub cmsketch_confidence: f64,
}

impl Default for LfuConfig {
    fn default() -> Self {
        Self {
            window_capacity_ratio: 0.1,
            protected_capacity_ratio: 0.8,
            cmsketch_eps: 0.001,
            cmsketch_confidence: 0.9,
        }
    }
}

/// w-TinyLFU eviction algorithm hint.
#[derive(Debug, Clone, Default)]
pub struct LfuHint;

#[derive(Debug, PartialEq, Eq)]
enum Queue {
    None,
    Window,
    Probation,
    Protected,
}

impl Default for Queue {
    fn default() -> Self {
        Self::None
    }
}

/// w-TinyLFU eviction algorithm hint.
#[derive(Debug, Default)]
pub struct LfuState {
    link: DlistLink,
    queue: Queue,
}

intrusive_adapter! { Adapter<K, V> = Record<Lfu<K, V>> { state.link => DlistLink } where K: Key, V: Value }

/// This implementation is inspired by [Caffeine](https://github.com/ben-manes/caffeine) under Apache License 2.0
///
/// A new and hot entry is kept in `window`.
///
/// When `window` is full, entries from it will overflow to `probation`.
///
/// When a entry in `probation` is accessed, it will be promoted to `protected`.
///
/// When `protected` is full, entries from it will overflow to `probation`.
///
/// When evicting, the entry with a lower frequency from `window` or `probation` will be evicted first, then from
/// `protected`.
pub struct Lfu<K, V>
where
    K: Key,
    V: Value,
{
    window: Dlist<Adapter<K, V>>,
    probation: Dlist<Adapter<K, V>>,
    protected: Dlist<Adapter<K, V>>,

    window_weight: usize,
    probation_weight: usize,
    protected_weight: usize,

    window_weight_capacity: usize,
    protected_weight_capacity: usize,

    // TODO(MrCroxx): use a count-min-sketch impl with atomic u16
    frequencies: CMSketchU16,

    step: usize,
    decay: usize,
}

impl<K, V> Lfu<K, V>
where
    K: Key,
    V: Value,
{
    fn increase_queue_weight(&mut self, record: &Record<Lfu<K, V>>) {
        let weight = record.weight();
        match record.state.queue {
            Queue::None => unreachable!(),
            Queue::Window => self.window_weight += weight,
            Queue::Probation => self.probation_weight += weight,
            Queue::Protected => self.protected_weight += weight,
        }
    }

    fn decrease_queue_weight(&mut self, record: &Record<Lfu<K, V>>) {
        let weight = record.weight();
        match record.state.queue {
            Queue::None => unreachable!(),
            Queue::Window => self.window_weight -= weight,
            Queue::Probation => self.probation_weight -= weight,
            Queue::Protected => self.protected_weight -= weight,
        }
    }

    fn update_frequencies(&mut self, hash: u64) {
        self.frequencies.inc(hash);
        self.step += 1;
        if self.step >= self.decay {
            self.step >>= 1;
            self.frequencies.halve();
        }
    }
}

impl<K, V> Eviction for Lfu<K, V>
where
    K: Key,
    V: Value,
{
    type Config = LfuConfig;
    type Key = K;
    type Value = V;
    type Hint = LfuHint;
    type State = LfuState;

    fn new(capacity: usize, config: &Self::Config) -> Self
    where
        Self: Sized,
    {
        assert!(
            config.window_capacity_ratio > 0.0 && config.window_capacity_ratio < 1.0,
            "window_capacity_ratio must be in (0, 1), given: {}",
            config.window_capacity_ratio
        );

        assert!(
            config.protected_capacity_ratio > 0.0 && config.protected_capacity_ratio < 1.0,
            "protected_capacity_ratio must be in (0, 1), given: {}",
            config.protected_capacity_ratio
        );

        assert!(
            config.window_capacity_ratio + config.protected_capacity_ratio < 1.0,
            "must guarantee: window_capacity_ratio + protected_capacity_ratio < 1, given: {}",
            config.window_capacity_ratio + config.protected_capacity_ratio
        );

        let window_weight_capacity = (capacity as f64 * config.window_capacity_ratio) as usize;
        let protected_weight_capacity = (capacity as f64 * config.protected_capacity_ratio) as usize;
        let frequencies = CMSketchU16::new(config.cmsketch_eps, config.cmsketch_confidence);
        let decay = frequencies.width();

        Self {
            window: Dlist::new(),
            probation: Dlist::new(),
            protected: Dlist::new(),
            window_weight: 0,
            probation_weight: 0,
            protected_weight: 0,
            window_weight_capacity,
            protected_weight_capacity,
            frequencies,
            step: 0,
            decay,
        }
    }

    fn update(&mut self, capacity: usize, config: &Self::Config) {
        if config.window_capacity_ratio <= 0.0 || config.window_capacity_ratio >= 1.0 {
            tracing::error!(
                "window_capacity_ratio must be in (0, 1), given: {}, new config ignored",
                config.window_capacity_ratio
            );
        }

        if config.protected_capacity_ratio <= 0.0 || config.protected_capacity_ratio >= 1.0 {
            tracing::error!(
                "protected_capacity_ratio must be in (0, 1), given: {}, new config ignored",
                config.protected_capacity_ratio
            );
        }

        if config.window_capacity_ratio + config.protected_capacity_ratio >= 1.0 {
            tracing::error!(
                "must guarantee: window_capacity_ratio + protected_capacity_ratio < 1, given: {}, new config ignored",
                config.window_capacity_ratio + config.protected_capacity_ratio
            )
        }

        // TODO(MrCroxx): Raise a warn log the cmsketch args updates is not supported yet if it is modified.

        let window_weight_capacity = (capacity as f64 * config.window_capacity_ratio) as usize;
        let protected_weight_capacity = (capacity as f64 * config.protected_capacity_ratio) as usize;

        self.window_weight_capacity = window_weight_capacity;
        self.protected_weight_capacity = protected_weight_capacity;
    }

    fn push(&mut self, mut ptr: NonNull<Record<Self>>) {
        let record = unsafe { ptr.as_mut() };

        strict_assert!(!record.state.link.is_linked());
        strict_assert!(!record.is_in_eviction());
        strict_assert_eq!(record.state.queue, Queue::None);

        self.window.push_back(ptr);
        record.set_in_eviction(true);
        record.state.queue = Queue::Window;

        self.increase_queue_weight(record);
        self.update_frequencies(record.hash());

        // If `window` weight exceeds the capacity, overflow entry from `window` to `probation`.
        while self.window_weight > self.window_weight_capacity {
            strict_assert!(!self.window.is_empty());
            let mut p = unsafe { self.window.pop_front().strict_unwrap_unchecked() };
            let r = unsafe { p.as_mut() };
            self.decrease_queue_weight(r);
            r.state.queue = Queue::Probation;
            self.increase_queue_weight(r);
            self.probation.push_back(p);
        }
    }

    fn pop(&mut self) -> Option<NonNull<Record<Self>>> {
        // Compare the frequency of the front element of `window` and `probation` queue, and evict the lower one.
        // If both `window` and `probation` are empty, try evict from `protected`.
        let mut ptr = match (self.window.front(), self.probation.front()) {
            (None, None) => None,
            (None, Some(_)) => self.probation.pop_front(),
            (Some(_), None) => self.window.pop_front(),
            (Some(window), Some(probation)) => {
                if self.frequencies.estimate(window.hash()) < self.frequencies.estimate(probation.hash()) {
                    self.window.pop_front()

                    // TODO(MrCroxx): Rotate probation to prevent a high frequency but cold head holds back promotion
                    // too long like CacheLib does?
                } else {
                    self.probation.pop_front()
                }
            }
        }
        .or_else(|| self.protected.pop_front())?;

        let record = unsafe { ptr.as_mut() };

        strict_assert!(!record.state.link.is_linked());
        strict_assert!(record.is_in_eviction());
        strict_assert_ne!(record.state.queue, Queue::None);

        self.decrease_queue_weight(record);
        record.state.queue = Queue::None;
        record.set_in_eviction(false);

        Some(ptr)
    }

    fn remove(&mut self, mut ptr: NonNull<Record<Self>>) {
        let record = unsafe { ptr.as_mut() };

        strict_assert!(record.state.link.is_linked());
        strict_assert!(record.is_in_eviction());
        strict_assert_ne!(record.state.queue, Queue::None);

        match record.state.queue {
            Queue::None => unreachable!(),
            Queue::Window => self.window.remove(ptr),
            Queue::Probation => self.probation.remove(ptr),
            Queue::Protected => self.protected.remove(ptr),
        };

        strict_assert!(!record.state.link.is_linked());

        self.decrease_queue_weight(record);
        record.state.queue = Queue::None;
        record.set_in_eviction(false);
    }

    fn clear(&mut self) {
        while let Some(ptr) = self.pop() {
            strict_assert!(!unsafe { ptr.as_ref() }.is_in_eviction());
            strict_assert!(!unsafe { ptr.as_ref() }.state.link.is_linked());
            strict_assert_eq!(unsafe { ptr.as_ref() }.state.queue, Queue::None);
        }
    }

    fn len(&self) -> usize {
        self.window.len() + self.probation.len() + self.protected.len()
    }

    fn acquire_operator() -> super::Operator {
        // TODO(MrCroxx): use a count-min-sketch with atomic u16 impl.
        Operator::Mutable
    }

    fn acquire_immutable(&self, _ptr: NonNull<Record<Self>>) {
        unreachable!()
    }

    fn acquire_mutable(&mut self, ptr: NonNull<Record<Self>>) {
        self.update_frequencies(unsafe { ptr.as_ref() }.hash());
    }

    fn release(&mut self, mut ptr: NonNull<Record<Self>>) {
        let record = unsafe { ptr.as_mut() };

        match record.state.queue {
            Queue::None => {
                strict_assert!(!record.state.link.is_linked());
                strict_assert!(!record.is_in_eviction());
                self.push(ptr);
                strict_assert!(record.state.link.is_linked());
                strict_assert!(record.is_in_eviction());
            }
            Queue::Window => {
                // Move to MRU position of `window`.
                strict_assert!(record.state.link.is_linked());
                strict_assert!(record.is_in_eviction());
                self.window.remove(ptr);
                self.window.push_back(ptr);
            }
            Queue::Probation => {
                // Promote to MRU position of `protected`.
                strict_assert!(record.state.link.is_linked());
                strict_assert!(record.is_in_eviction());
                self.probation.remove(ptr);
                self.decrease_queue_weight(record);
                record.state.queue = Queue::Protected;
                self.increase_queue_weight(record);
                self.protected.push_back(ptr);

                // If `protected` weight exceeds the capacity, overflow entry from `protected` to `probation`.
                while self.protected_weight > self.protected_weight_capacity {
                    strict_assert!(!self.protected.is_empty());
                    let mut p = unsafe { self.protected.pop_front().strict_unwrap_unchecked() };
                    let r = unsafe { p.as_mut() };
                    self.decrease_queue_weight(r);
                    r.state.queue = Queue::Probation;
                    self.increase_queue_weight(r);
                    self.probation.push_back(p);
                }
            }
            Queue::Protected => {
                // Move to MRU position of `protected`.
                strict_assert!(record.state.link.is_linked());
                strict_assert!(record.is_in_eviction());
                self.protected.remove(ptr);
                self.protected.push_back(ptr);
            }
        }
    }
}
