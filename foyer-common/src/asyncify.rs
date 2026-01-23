// Copyright 2026 foyer Project Authors
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

use crate::runtime::SingletonHandle;

/// Convert the block call to async call with given runtime handle.
#[cfg(not(madsim))]
pub async fn asyncify_with_runtime<F, T>(runtime: &SingletonHandle, f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    runtime.spawn_blocking(f).await.unwrap()
}

#[cfg(madsim)]
/// Convert the block call to async call with given runtime.
///
/// madsim compatible mode.
pub async fn asyncify_with_runtime<F, T>(_: &SingletonHandle, f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    f()
}
