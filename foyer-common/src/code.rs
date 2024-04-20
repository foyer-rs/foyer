//  Copyright 2024 Foyer Project Authors
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

use std::hash::Hash;

use serde::{de::DeserializeOwned, Serialize};

pub trait Key: Send + Sync + 'static + Hash + Eq + PartialEq {}
pub trait Value: Send + Sync + 'static {}

impl<T: Send + Sync + 'static + std::hash::Hash + Eq> Key for T {}
impl<T: Send + Sync + 'static> Value for T {}

// TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
pub trait StorageKey: Key + Serialize + DeserializeOwned {}
impl<T> StorageKey for T where T: Key + Serialize + DeserializeOwned {}

// TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
pub trait StorageValue: Value + 'static + Clone + Serialize + DeserializeOwned {}
impl<T> StorageValue for T where T: Value + Clone + Serialize + DeserializeOwned {}
