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

use std::{
    fmt::Debug,
    mem::ManuallyDrop,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use tokio::{
    runtime::{Handle, Runtime},
    task::JoinHandle,
};

use crate::error::{Error, ErrorKind, Result};

/// A wrapper around [`Runtime`] that shuts down the runtime in the background when dropped.
///
/// This is necessary because directly dropping a nested runtime is not allowed in a parent runtime.
pub struct BackgroundShutdownRuntime(ManuallyDrop<Runtime>);

impl Debug for BackgroundShutdownRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("BackgroundShutdownRuntime").finish()
    }
}

impl Drop for BackgroundShutdownRuntime {
    fn drop(&mut self) {
        // Safety: The runtime is only dropped once here.
        let runtime = unsafe { ManuallyDrop::take(&mut self.0) };

        #[cfg(madsim)]
        drop(runtime);
        #[cfg(not(madsim))]
        runtime.shutdown_background();
    }
}

impl Deref for BackgroundShutdownRuntime {
    type Target = Runtime;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for BackgroundShutdownRuntime {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<Runtime> for BackgroundShutdownRuntime {
    fn from(runtime: Runtime) -> Self {
        Self(ManuallyDrop::new(runtime))
    }
}

/// A wrapper for [`JoinHandle`].
#[derive(Debug)]
pub struct SpawnHandle<T> {
    inner: JoinHandle<T>,
}

impl<T> std::future::Future for SpawnHandle<T> {
    type Output = Result<T>;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        match std::pin::Pin::new(&mut self.inner).poll(cx) {
            std::task::Poll::Ready(res) => match res {
                Ok(v) => std::task::Poll::Ready(Ok(v)),
                Err(e) => std::task::Poll::Ready(Err(Error::new(ErrorKind::Join, "tokio join error").with_source(e))),
            },
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

/// A wrapper around a dedicated tokio runtime or handle to spawn tasks.
#[derive(Debug, Clone)]
pub enum Spawner {
    /// A dedicated runtime to spawn tasks.
    Runtime(Arc<BackgroundShutdownRuntime>),
    /// A handle to spawn tasks.
    Handle(Handle),
}

impl From<Runtime> for Spawner {
    fn from(runtime: Runtime) -> Self {
        Self::Runtime(Arc::new(runtime.into()))
    }
}

impl From<Handle> for Spawner {
    fn from(handle: Handle) -> Self {
        Self::Handle(handle)
    }
}

impl Spawner {
    /// Wrapper for [`Runtime::spawn`] or [`Handle::spawn`].
    pub fn spawn<F>(&self, future: F) -> SpawnHandle<<F as std::future::Future>::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let inner = match self {
            Spawner::Runtime(rt) => rt.spawn(future),
            Spawner::Handle(h) => h.spawn(future),
        };
        SpawnHandle { inner }
    }

    /// Wrapper for [`Runtime::spawn_blocking`] or [`Handle::spawn_blocking`].
    pub fn spawn_blocking<F, R>(&self, func: F) -> SpawnHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let inner = match self {
            Spawner::Runtime(rt) => rt.spawn_blocking(func),
            Spawner::Handle(h) => h.spawn_blocking(func),
        };
        SpawnHandle { inner }
    }

    /// Get the current spawner.
    pub fn current() -> Self {
        Spawner::Handle(Handle::current())
    }
}
