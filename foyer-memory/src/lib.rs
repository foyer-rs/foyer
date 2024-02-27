//  Copyright 2024 MrCroxx
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

#![feature(let_chains)]
#![feature(lint_reasons)]

pub mod cache;
pub mod eviction;
pub mod handle;
pub mod indexer;

use std::hash::Hash;

pub trait Key: Send + Sync + 'static + Hash + Eq + Ord {}
pub trait Value: Send + Sync + 'static {}

impl<T: Send + Sync + 'static + Hash + Eq + Ord> Key for T {}
impl<T: Send + Sync + 'static> Value for T {}
