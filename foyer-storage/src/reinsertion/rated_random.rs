//  Copyright 2023 MrCroxx
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

use std::{fmt::Debug, sync::Arc, time::Duration};

use foyer_common::{
    code::{Key, Value},
    rated_random::RatedRandom,
};

use crate::metrics::Metrics;

use super::ReinsertionPolicy;

#[derive(Debug)]
pub struct RatedRandomReinsertionPolicy<K, V>
where
    K: Key,
    V: Value,
{
    inner: RatedRandom<K, V>,
}

impl<K, V> RatedRandomReinsertionPolicy<K, V>
where
    K: Key,
    V: Value,
{
    pub fn new(rate: usize, update_interval: Duration) -> Self {
        Self {
            inner: RatedRandom::new(rate, update_interval),
        }
    }
}

impl<K, V> ReinsertionPolicy for RatedRandomReinsertionPolicy<K, V>
where
    K: Key,
    V: Value,
{
    type Key = K;

    type Value = V;

    fn judge(&self, key: &Self::Key, weight: usize, _metrics: &Arc<Metrics>) -> bool {
        self.inner.judge(key, weight)
    }

    fn on_insert(&self, key: &Self::Key, weight: usize, _metrics: &Arc<Metrics>, judge: bool) {
        self.inner.on_insert(key, weight, judge)
    }

    fn on_drop(&self, key: &Self::Key, weight: usize, _metrics: &Arc<Metrics>, judge: bool) {
        self.inner.on_drop(key, weight, judge)
    }
}
