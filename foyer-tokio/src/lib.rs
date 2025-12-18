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

//! This crate re-exports `tokio`-like crates to switch between normal usage and deterministic testing.

/// Re-export `madsim-tokio`.
#[cfg(feature = "runtime-madsim-tokio")]
pub use runtime_madsim_tokio::*;
/// Re-export `tokio`.
#[cfg(feature = "runtime-tokio")]
pub use runtime_tokio::*;

#[cfg(not(any(feature = "runtime-tokio", feature = "runtime-madsim-tokio")))]
compile_error!("");

#[cfg(all(feature = "runtime-tokio", feature = "runtime-madsim-tokio"))]
compile_error!("");