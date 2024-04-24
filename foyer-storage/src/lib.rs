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

#![cfg_attr(feature = "nightly", feature(allocator_api))]

mod admission;
mod buffer;
mod catalog;
mod compress;
mod device;
mod error;
mod flusher;
mod generic;
mod judge;
mod lazy;
mod metrics;
mod none;
mod reclaimer;
mod region;
mod region_manager;
mod reinsertion;
mod runtime;
mod storage;
mod store;

mod prelude;
pub use prelude::*;

pub mod test_utils;
