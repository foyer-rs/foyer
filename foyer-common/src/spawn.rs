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

/// A trait for spawning tasks.
#[doc(hidden)]
pub trait Spawn: Send + Sync + 'static + Clone {
    /// See [`Handle`].
    type Handle<T>: Handle<T>
    where
        T: Send + 'static;

    /// Spawns a future onto the [`Spawn`].
    ///
    /// This spawns the given future onto the runtime's executor, usually a
    /// thread pool. The thread pool is then responsible for polling the future
    /// until it completes.
    ///
    /// The provided future will start running in the background immediately
    /// when `spawn` is called, even if you don't await the returned
    /// `Handle`.
    fn spawn<F>(&self, future: F) -> Self::Handle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static;

    /// Runs the provided function on an executor dedicated to blocking operations.
    fn spawn_blocking<F, R>(&self, func: F) -> Self::Handle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static;

    /// Get the current spawner.
    fn current() -> Self;
}

/// An owned permission to join on a task (await its termination).
///
/// This can be thought of as the equivalent of [`std::thread::JoinHandle`]
/// for an asynchronous task rather than a thread. Note that the background task
/// associated with this `Handle` started running immediately when you
/// called spawn, even if you have not yet awaited the `Handle`.
///
/// A `Handle` *detaches* the associated task when it is dropped, which
/// means that there is no longer any handle to the task, and no way to `join`
/// on it.
///
/// This `struct` is created by the [`spawn`] and [`spawn_blocking`]
/// functions.
///
/// # Cancel safety
///
/// The `&mut Handle<T>` type is cancel safe. If it is used as the event
/// in a `tokio::select!` statement and some other branch completes first,
/// then it is guaranteed that the output of the task is not lost.
///
/// If a `Handle` is dropped, then the task continues running in the
/// background and its return value is lost.
#[doc(hidden)]
pub trait Handle<T>:
    Send
    + Sync
    + std::panic::UnwindSafe
    + std::panic::RefUnwindSafe
    + Unpin
    + std::future::Future<Output = crate::error::Result<T>>
    + 'static
{
}

#[cfg(any(feature = "spawner-tokio", feature = "spawner-madsim-tokio"))]
pub use tokio_like::{select, SpawnHandle, Spawner};

/// [`tokio`]-like based spawner.
#[cfg(any(feature = "spawner-tokio", feature = "spawner-madsim-tokio"))]
pub mod tokio_like {
    use std::{
        fmt::Debug,
        mem::ManuallyDrop,
        ops::{Deref, DerefMut},
        sync::Arc,
    };

    #[cfg(feature = "spawner-madsim-tokio")]
    pub use madsim_tokio::select;
    #[cfg(feature = "spawner-madsim-tokio")]
    use madsim_tokio::{
        runtime::{Handle, Runtime},
        task::JoinHandle,
    };
    #[cfg(feature = "spawner-tokio")]
    pub use tokio::select;
    #[cfg(feature = "spawner-tokio")]
    use tokio::{
        runtime::{Handle, Runtime},
        task::JoinHandle,
    };

    use crate::{
        error::{Error, ErrorKind, Result},
        spawn::Spawn,
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
                    Err(e) => {
                        std::task::Poll::Ready(Err(Error::new(ErrorKind::Join, "tokio join error").with_source(e)))
                    }
                },
                std::task::Poll::Pending => std::task::Poll::Pending,
            }
        }
    }

    impl<T> super::Handle<T> for SpawnHandle<T> where T: Send + 'static {}

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

    impl Spawn for Spawner {
        type Handle<T>
            = SpawnHandle<T>
        where
            T: Send + 'static;

        fn spawn<F>(&self, future: F) -> SpawnHandle<<F as std::future::Future>::Output>
        where
            F: std::future::Future + Send + 'static,
            F::Output: Send + 'static,
        {
            self.spawn(future)
        }

        fn spawn_blocking<F, R>(&self, func: F) -> SpawnHandle<R>
        where
            F: FnOnce() -> R + Send + 'static,
            R: Send + 'static,
        {
            self.spawn_blocking(func)
        }

        fn current() -> Self {
            Self::current()
        }
    }
}
