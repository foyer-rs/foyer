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
    future::Future,
    mem::ManuallyDrop,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use futures_util::FutureExt;
use tokio::{
    runtime::{Handle, Runtime},
    task::JoinHandle,
};

/// A wrapper around [`Runtime`] that shuts down the runtime in the background when dropped.
///
/// This is necessary because directly dropping a nested runtime is not allowed in a parent runtime.
///
/// FYI: https://docs.rs/tokio/latest/tokio/runtime/struct.Runtime.html#method.shutdown_background
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

/// A non-cloneable runtime handle.
#[derive(Debug)]
pub struct SingletonHandle(Handle);

impl From<Handle> for SingletonHandle {
    fn from(handle: Handle) -> Self {
        Self(handle)
    }
}

impl SingletonHandle {
    /// Spawns a future onto the Tokio runtime.
    ///
    /// This spawns the given future onto the runtime's executor, usually a
    /// thread pool. The thread pool is then responsible for polling the future
    /// until it completes.
    ///
    /// The provided future will start running in the background immediately
    /// when `spawn` is called, even if you don't await the returned
    /// `JoinHandle`.
    ///
    /// See [module level][mod] documentation for more details.
    ///
    /// [mod]: index.html
    ///
    /// # Examples
    ///
    /// ```
    /// use Runtime;
    ///
    /// # fn dox() {
    /// // Create the runtime
    /// let rt = Runtime::new().unwrap();
    /// // Get a handle from this runtime
    /// let handle = rt.handle();
    ///
    /// // Spawn a future onto the runtime using the handle
    /// handle.spawn(async {
    ///     println!("now running on a worker thread");
    /// });
    /// # }
    /// ```
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.0.spawn(future)
    }

    /// Runs the provided function on an executor dedicated to blocking
    /// operations.
    ///
    /// # Examples
    ///
    /// ```
    /// use Runtime;
    ///
    /// # fn dox() {
    /// // Create the runtime
    /// let rt = Runtime::new().unwrap();
    /// // Get a handle from this runtime
    /// let handle = rt.handle();
    ///
    /// // Spawn a blocking function onto the runtime using the handle
    /// handle.spawn_blocking(|| {
    ///     println!("now running on a worker thread");
    /// });
    /// # }
    pub fn spawn_blocking<F, R>(&self, func: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.0.spawn_blocking(func)
    }
}

/// A join handle for Tokio executor.
pub struct TokioJoinHandle<T> {
    inner: JoinHandle<T>,
}

impl<T> Debug for TokioJoinHandle<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokioJoinHandle").finish()
    }
}

impl<T> From<JoinHandle<T>> for TokioJoinHandle<T> {
    fn from(inner: JoinHandle<T>) -> Self {
        Self { inner }
    }
}

impl<T> Future for TokioJoinHandle<T>
where
    T: Send + 'static,
{
    type Output = std::result::Result<T, Box<dyn std::error::Error + Send + Sync + 'static>>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        this.inner.poll_unpin(cx).map_err(|e| e.into())
    }
}

impl<T> super::JoinHandle<T> for TokioJoinHandle<T> where T: Send + 'static {}

/// An executor implementation for Tokio runtime.
///
/// This executor owns a `Runtime` and spawns tasks onto it.
#[derive(Debug, Clone)]
pub struct TokioRuntimeExecutor {
    inner: Arc<BackgroundShutdownRuntime>,
}

impl TokioRuntimeExecutor {
    /// Creates a new `TokioRuntimeExecutor` from a `Runtime`.
    pub fn new(inner: Runtime) -> Self {
        Self {
            inner: Arc::new(inner.into()),
        }
    }
}

impl From<Runtime> for TokioRuntimeExecutor {
    fn from(inner: Runtime) -> Self {
        Self::new(inner)
    }
}

impl super::Executor for TokioRuntimeExecutor {
    type JoinHandle<T>
        = TokioJoinHandle<T>
    where
        T: Send + 'static;

    fn spawn<F>(&self, future: F) -> Self::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.inner.spawn(future).into()
    }

    fn spawn_blocking<F, R>(&self, func: F) -> Self::JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.inner.spawn_blocking(func).into()
    }
}

/// An executor implementation for Tokio runtime.
///
/// This executor uses a `Handle` to spawn tasks onto an existing runtime.
#[derive(Debug, Clone)]
pub struct TokioHandleExecutor {
    inner: Handle,
}

impl TokioHandleExecutor {
    /// Creates a new `TokioHandleExecutor` from a `Handle`.
    pub fn new(inner: Handle) -> Self {
        Self { inner }
    }

    /// Creates a new `TokioHandleExecutor` from the current runtime handle.
    pub fn current() -> Self {
        Self::new(Handle::current())
    }
}

impl From<Handle> for TokioHandleExecutor {
    fn from(inner: Handle) -> Self {
        Self::new(inner)
    }
}

impl super::Executor for TokioHandleExecutor {
    type JoinHandle<T>
        = TokioJoinHandle<T>
    where
        T: Send + 'static;

    fn spawn<F>(&self, future: F) -> Self::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.inner.spawn(future).into()
    }

    fn spawn_blocking<F, R>(&self, func: F) -> Self::JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.inner.spawn_blocking(func).into()
    }
}
