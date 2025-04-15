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

use std::sync::Arc;

use foyer_common::runtime::{BackgroundShutdownRuntime, SingletonHandle};
use tokio::runtime::Handle;

/// The identify of the runtime in foyer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", serde::Serialize, serde::Deserialize)]
pub enum RuntimeIdentify {
    /// Runtime for user tasks.
    User,
    /// Runtime for read tasks.
    Read,
    /// Runtime for write tasks.
    Write,
}

#[cfg(all(not(madsim), tokio_unstable, tokio_taskdump))]
#[derive(Debug)]
pub struct RuntimeDump {
    pub identifies: Vec<RuntimeIdentify>,
    pub dump: tokio::runtime::dump::Dump,
}

#[derive(Debug)]
struct RuntimeInner {
    _read_runtime: Option<Arc<BackgroundShutdownRuntime>>,
    _write_runtime: Option<Arc<BackgroundShutdownRuntime>>,

    read_runtime_handle: SingletonHandle,
    write_runtime_handle: SingletonHandle,
    user_runtime_handle: SingletonHandle,
}

/// [`Runtime`] holds the runtime reference and non-cloneable handles to prevent handle usage after runtime shutdown.
#[derive(Debug, Clone)]
pub struct Runtime {
    inner: Arc<RuntimeInner>,
}

impl Runtime {
    /// Create a new runtime with runtimes if given.
    pub fn new(
        read_runtime: Option<Arc<BackgroundShutdownRuntime>>,
        write_runtime: Option<Arc<BackgroundShutdownRuntime>>,
        user_runtime_handle: Handle,
    ) -> Self {
        let read_runtime_handle = read_runtime
            .as_ref()
            .map(|rt| rt.handle().clone())
            .unwrap_or(user_runtime_handle.clone());
        let write_runtime_handle = write_runtime
            .as_ref()
            .map(|rt| rt.handle().clone())
            .unwrap_or(user_runtime_handle.clone());
        Self {
            inner: Arc::new(RuntimeInner {
                _read_runtime: read_runtime,
                _write_runtime: write_runtime,
                read_runtime_handle: read_runtime_handle.into(),
                write_runtime_handle: write_runtime_handle.into(),
                user_runtime_handle: user_runtime_handle.into(),
            }),
        }
    }

    /// Create a new runtime with current runtime env only.
    pub fn current() -> Self {
        Self {
            inner: Arc::new(RuntimeInner {
                _read_runtime: None,
                _write_runtime: None,
                read_runtime_handle: Handle::current().into(),
                write_runtime_handle: Handle::current().into(),
                user_runtime_handle: Handle::current().into(),
            }),
        }
    }

    /// Get the non-cloneable read runtime handle.
    pub fn read(&self) -> &SingletonHandle {
        &self.inner.read_runtime_handle
    }

    /// Get the non-cloneable write runtime handle.
    pub fn write(&self) -> &SingletonHandle {
        &self.inner.write_runtime_handle
    }

    /// Get the non-cloneable user runtime handle.
    pub fn user(&self) -> &SingletonHandle {
        &self.inner.user_runtime_handle
    }

    /// Captures the snapshots of all of the runtime’s state.
    ///
    /// FYI: https://docs.rs/tokio/latest/tokio/runtime/struct.Handle.html#method.dump
    #[cfg(all(not(madsim), tokio_unstable, tokio_taskdump))]
    pub async fn dump(&self) -> std::collections::HashMap<tokio::runtime::Id, RuntimeDump> {
        let mut dumps = std::collections::HashMap::new();

        match dumps.entry(self.inner.read_runtime_handle.id()) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(RuntimeDump {
                    identifies: vec![RuntimeIdentify::Read],
                    dump: self.inner.read_runtime_handle.dump().await,
                });
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                entry.get_mut().identifies.push(RuntimeIdentify::Read);
            }
        }
        match dumps.entry(self.inner.write_runtime_handle.id()) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(RuntimeDump {
                    identifies: vec![RuntimeIdentify::Write],
                    dump: self.inner.write_runtime_handle.dump().await,
                });
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                entry.get_mut().identifies.push(RuntimeIdentify::Write);
            }
        }
        match dumps.entry(self.inner.user_runtime_handle.id()) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(RuntimeDump {
                    identifies: vec![RuntimeIdentify::User],
                    dump: self.inner.user_runtime_handle.dump().await,
                });
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                entry.get_mut().identifies.push(RuntimeIdentify::User);
            }
        }
        dumps
    }
}
