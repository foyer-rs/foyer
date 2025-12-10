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

#[cfg(feature = "runtime-madsim-tokio")]
pub use madsim_tokio::*;
#[cfg(feature = "runtime-tokio")]
pub use tokio::*;
#[cfg(all(feature = "runtime-madsim-tokio", feature = "runtime-tokio"))]
compile_error!("Features `runtime-madsim-tokio` and `runtime-tokio` are mutually exclusive.");
