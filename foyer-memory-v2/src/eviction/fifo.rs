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

use foyer_common::code::{Key, Value};
use foyer_intrusive_v2::{
    dlist::{Dlist, DlistLink},
    intrusive_adapter,
};
use serde::{Deserialize, Serialize};

use crate::Record;

use super::{Eviction, Operator};

/// Fifo eviction algorithm config.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FifoConfig;

/// Fifo eviction algorithm hint.
#[derive(Debug, Clone, Default)]
pub struct FifoHint;

/// Fifo eviction algorithm state.
#[derive(Debug, Default)]
pub struct FifoState {
    link: DlistLink,
}

intrusive_adapter! { Adapter<K, V> = Record<Fifo<K, V>> { state.link => DlistLink } where K: Key, V: Value }

pub struct Fifo<K, V>
where
    K: Key,
    V: Value,
{
    queue: Dlist<Adapter<K, V>>,
}

impl<K, V> Eviction for Fifo<K, V>
where
    K: Key,
    V: Value,
{
    type Config = FifoConfig;
    type Key = K;
    type Value = V;
    type Hint = FifoHint;
    type State = FifoState;

    fn new(_capacity: usize, _config: &Self::Config) -> Self
    where
        Self: Sized,
    {
        Self { queue: Dlist::new() }
    }

    fn update(&mut self, _: usize, _: &Self::Config) {}

    fn push(&mut self, ptr: NonNull<Record<Self>>) {
        self.queue.push_back(ptr);
        unsafe { ptr.as_ref().set_in_eviction(true) };
    }

    fn pop(&mut self) -> Option<NonNull<Record<Self>>> {
        self.queue
            .pop_front()
            .inspect(|ptr| unsafe { ptr.as_ref().set_in_eviction(false) })
    }

    fn remove(&mut self, ptr: NonNull<Record<Self>>) {
        let p = self.queue.remove(ptr);
        assert_eq!(p, ptr);
        unsafe { ptr.as_ref().set_in_eviction(false) };
    }

    fn clear(&mut self) {
        while let Some(_) = self.pop() {}
    }

    fn len(&self) -> usize {
        self.queue.len()
    }

    fn acquire_operator() -> super::Operator {
        Operator::Immutable
    }

    fn acquire_immutable(&self, _ptr: NonNull<Record<Self>>) {}

    fn acquire_mutable(&mut self, _ptr: NonNull<Record<Self>>) {
        unreachable!()
    }

    fn release(&mut self, _ptr: NonNull<Record<Self>>) {}
}
