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

use std::marker::PhantomData;

use foyer_common::code::{Key, Value};
use foyer_intrusive::{
    dlist::{Dlist, DlistLink},
    intrusive_adapter,
};
use serde::{Deserialize, Serialize};
use slab::Slab;

use crate::record::RecordToken;

use super::Eviction;

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct FifoConfig;

#[derive(Debug, Default)]
pub struct FifoState {
    link: DlistLink,
}

#[derive(Debug, Default)]
pub struct FifoHint;

intrusive_adapter! { FifoDlistAdapter = FifoState { link: DlistLink } }

#[derive(Debug)]
pub struct Fifo<K, V>
where
    K: Key,
    V: Value,
{
    queue: Dlist<FifoDlistAdapter>,
    _marker: PhantomData<(K, V)>,
}

unsafe impl<K, V> Send for Fifo<K, V>
where
    K: Key,
    V: Value,
{
}
unsafe impl<K, V> Sync for Fifo<K, V>
where
    K: Key,
    V: Value,
{
}

impl<K, V> Eviction<K, V> for Fifo<K, V>
where
    K: Key,
    V: Value,
{
    type Hint = FifoHint;
    type State = FifoState;
    type Config = FifoConfig;

    fn new(_capacity: usize, _config: &Self::Config) -> Self {
        Self {
            queue: Dlist::new(),
            _marker: PhantomData,
        }
    }

    fn push(&mut self, token: RecordToken<K, V, Self::Hint, Self::State>) {
        todo!()
    }

    fn pop(&mut self) -> Option<RecordToken<K, V, Self::Hint, Self::State>> {
        todo!()
    }

    fn remove(&mut self, token: RecordToken<K, V, Self::Hint, Self::State>) {
        todo!()
    }

    fn len(&self) -> usize {
        todo!()
    }

    fn is_empty(&self) -> bool {
        todo!()
    }

    fn acquire_op() -> super::Operator {
        todo!()
    }

    fn acquire(&self, token: RecordToken<K, V, Self::Hint, Self::State>) {
        todo!()
    }

    fn acquire_mut(&mut self, token: RecordToken<K, V, Self::Hint, Self::State>) {
        todo!()
    }

    fn release_op() -> super::Operator {
        todo!()
    }

    fn release(&self, token: RecordToken<K, V, Self::Hint, Self::State>) {
        todo!()
    }

    fn release_mut(&self, token: RecordToken<K, V, Self::Hint, Self::State>) {
        todo!()
    }
}
