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

use std::marker::PhantomData;

use foyer_common::code::{Key, Value};

use std::fmt::Debug;

#[allow(unused_variables)]
pub trait AdmissionPolicy: Send + Sync + 'static + Debug {
    type Key: Key;
    type Value: Value;

    fn judge(&self, key: &Self::Key, value: &Self::Value) -> bool;

    fn admit(&self, key: &Self::Key, value: &Self::Value) {}
}

#[derive(Debug)]
pub struct AdmitAll<K: Key, V: Value>(PhantomData<(K, V)>);

impl<K: Key, V: Value> Default for AdmitAll<K, V> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<K: Key, V: Value> AdmissionPolicy for AdmitAll<K, V> {
    type Key = K;

    type Value = V;

    fn judge(&self, _key: &Self::Key, _value: &Self::Value) -> bool {
        true
    }
}

pub mod rated_random;
