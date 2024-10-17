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

use serde::{de::DeserializeOwned, Serialize};
use slab::Slab;

use crate::record::{Record, RecordToken};

pub trait Hint: Send + Sync + 'static + Default {}
impl<T> Hint for T where T: Send + Sync + 'static + Default {}

pub trait State: Send + Sync + 'static + Default {}
impl<T> State for T where T: Send + Sync + 'static + Default {}

pub trait Config: Send + Sync + 'static + Clone + Serialize + DeserializeOwned + Default {}
impl<T> Config for T where T: Send + Sync + 'static + Clone + Serialize + DeserializeOwned + Default {}

pub enum Operator {
    Mutable,
    Immutable,
}

pub trait Eviction<K, V>: Send + Sync + 'static {
    type Hint: Hint;
    type State: State;
    type Config: Config;

    fn new(capacity: usize, config: &Self::Config) -> Self;

    fn push(&mut self, token: RecordToken<K, V, Self::Hint, Self::State>);

    fn pop(&mut self) -> Option<RecordToken<K, V, Self::Hint, Self::State>>;

    fn remove(&mut self, token: RecordToken<K, V, Self::Hint, Self::State>);

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool;

    fn acquire_op() -> Operator;

    fn acquire(&self, token: RecordToken<K, V, Self::Hint, Self::State>);

    fn acquire_mut(&mut self, token: RecordToken<K, V, Self::Hint, Self::State>);

    fn release_op() -> Operator;

    fn release(&self, token: RecordToken<K, V, Self::Hint, Self::State>);

    fn release_mut(&self, token: RecordToken<K, V, Self::Hint, Self::State>);
}

mod fifo;
