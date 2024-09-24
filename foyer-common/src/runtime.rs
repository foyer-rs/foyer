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

use std::{
    fmt::Debug,
    future::Future,
    mem::ManuallyDrop,
    ops::{Deref, DerefMut},
};

use tokio::{
    runtime::{Handle, Runtime},
    task::JoinHandle,
};

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

/// A non-clonable runtime handle.
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
    /// use tokio::runtime::Runtime;
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
    /// use tokio::runtime::Runtime;
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
