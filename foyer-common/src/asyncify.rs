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

use tokio::runtime::Handle;

/// Convert the block call to async call.
#[cfg(not(madsim))]
pub async fn asyncify<F, T>(f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f).await.unwrap()
}

#[cfg(madsim)]
pub async fn asyncify<F, T>(f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    f()
}

/// Convert the block call to async call.
#[cfg(not(madsim))]
pub async fn asyncify_with_runtime<F, T>(runtime: &Handle, f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    runtime.spawn_blocking(f).await.unwrap()
}

#[cfg(madsim)]
pub async fn asyncify_with_runtime<F, T>(_: &Handle, f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    f()
}
