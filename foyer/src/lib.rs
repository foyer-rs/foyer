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
#![warn(missing_docs)]
#![warn(clippy::allow_attributes)]

//! A hybrid cache library that supports plug-and-play cache algorithms, in-memory cache and disk cache.
//!
//! ![Crates.io Version](https://img.shields.io/crates/v/foyer?style=for-the-badge&logo=docs.rs&labelColor=555555)
//! ![docs.rs](https://img.shields.io/docsrs/foyer?style=for-the-badge&logo=docs.rs&labelColor=555555)
//! ![Website](https://img.shields.io/website?url=https%3A%2F%2Ffoyer.rs&up_message=foyer.rs&style=for-the-badge&logo=rust&labelColor=555555)

use foyer_common as common;
use foyer_memory as memory;
use foyer_storage as storage;

mod hybrid;

mod prelude;
pub use prelude::*;
