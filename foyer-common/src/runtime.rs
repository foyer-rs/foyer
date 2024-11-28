//  Copyright 2024 foyer Project Authors
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

    /// Runs a future to completion on this `Handle`'s associated `Runtime`.
    ///
    /// This runs the given future on the current thread, blocking until it is
    /// complete, and yielding its resolved result. Any tasks or timers which
    /// the future spawns internally will be executed on the runtime.
    ///
    /// When this is used on a `current_thread` runtime, only the
    /// [`Runtime::block_on`] method can drive the IO and timer drivers, but the
    /// `Handle::block_on` method cannot drive them. This means that, when using
    /// this method on a `current_thread` runtime, anything that relies on IO or
    /// timers will not work unless there is another thread currently calling
    /// [`Runtime::block_on`] on the same runtime.
    ///
    /// # If the runtime has been shut down
    ///
    /// If the `Handle`'s associated `Runtime` has been shut down (through
    /// [`Runtime::shutdown_background`], [`Runtime::shutdown_timeout`], or by
    /// dropping it) and `Handle::block_on` is used it might return an error or
    /// panic. Specifically IO resources will return an error and timers will
    /// panic. Runtime independent futures will run as normal.
    ///
    /// # Panics
    ///
    /// This function panics if the provided future panics, if called within an
    /// asynchronous execution context, or if a timer future is executed on a
    /// runtime that has been shut down.
    ///
    /// # Examples
    ///
    /// ```
    /// use tokio::runtime::Runtime;
    ///
    /// // Create the runtime
    /// let rt  = Runtime::new().unwrap();
    ///
    /// // Get a handle from this runtime
    /// let handle = rt.handle();
    ///
    /// // Execute the future, blocking the current thread until completion
    /// handle.block_on(async {
    ///     println!("hello");
    /// });
    /// ```
    ///
    /// Or using `Handle::current`:
    ///
    /// ```
    /// use tokio::runtime::Handle;
    ///
    /// #[tokio::main]
    /// async fn main () {
    ///     let handle = Handle::current();
    ///     std::thread::spawn(move || {
    ///         // Using Handle::block_on to run async code in the new thread.
    ///         handle.block_on(async {
    ///             println!("hello");
    ///         });
    ///     });
    /// }
    /// ```
    ///
    /// [`JoinError`]: struct@crate::task::JoinError
    /// [`JoinHandle`]: struct@crate::task::JoinHandle
    /// [`Runtime::block_on`]: fn@crate::runtime::Runtime::block_on
    /// [`Runtime::shutdown_background`]: fn@crate::runtime::Runtime::shutdown_background
    /// [`Runtime::shutdown_timeout`]: fn@crate::runtime::Runtime::shutdown_timeout
    /// [`spawn_blocking`]: crate::task::spawn_blocking
    /// [`tokio::fs`]: crate::fs
    /// [`tokio::net`]: crate::net
    /// [`tokio::time`]: crate::time
    #[cfg(not(madsim))]
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.0.block_on(future)
    }

    #[cfg(madsim)]
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        unimplemented!("`block_on()` is not supported with madsim")
    }
}
