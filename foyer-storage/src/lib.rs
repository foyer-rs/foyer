// Copyright 2025 foyer Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A disk cache engine that serves as the disk cache backend of `foyer`.

#![cfg_attr(feature = "nightly", feature(allocator_api))]
#![cfg_attr(feature = "nightly", feature(write_all_vectored))]

mod compress;
mod engine;
mod error;
mod io;
mod picker;
mod serde;
mod statistics;
mod store;

mod prelude;
pub use prelude::*;

#[cfg(any(test, feature = "test_utils"))]
pub mod test_utils;
