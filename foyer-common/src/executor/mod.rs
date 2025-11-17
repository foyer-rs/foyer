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

/// `tokio` executor implementation.
#[cfg(feature = "executor-tokio")]
pub mod tokio;

use std::{
    fmt::Debug,
    future::Future,
    marker::PhantomData,
    panic::{RefUnwindSafe, UnwindSafe},
};

#[cfg(feature = "executor-tokio")]
use futures_util::FutureExt;
use pin_project::pin_project;

/// A phantom join handle that does nothing.
#[doc(hidden)]
pub struct PhantomJoinHandle<T>(PhantomData<T>);

unsafe impl<T> Send for PhantomJoinHandle<T> {}
unsafe impl<T> Sync for PhantomJoinHandle<T> {}
impl<T> UnwindSafe for PhantomJoinHandle<T> {}
impl<T> RefUnwindSafe for PhantomJoinHandle<T> {}

/// A deatched join handle returned by an executor spawn.
pub trait JoinHandle<T>:
    Debug
    + Future<Output = std::result::Result<T, Box<dyn std::error::Error + Send + Sync + 'static>>>
    + Send
    + Sync
    + UnwindSafe
    + RefUnwindSafe
    + 'static
{
}

/// An executor that can spawn asynchronous tasks or blocking functions.
pub trait Executor: Debug + Send + Sync + 'static + Clone {
    /// The join handle type returned by this executor.
    type JoinHandle<T>: JoinHandle<T>
    where
        T: Send + 'static;

    /// Spawn a future onto the executor.
    fn spawn<F>(&self, future: F) -> Self::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static;

    /// Spawn a blocking function onto the executor.
    fn spawn_blocking<F, R>(&self, func: F) -> Self::JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static;
}

/// An enum wrapper for different join handle implementations.
#[pin_project(project = JoinHandleEnumProj)]
pub enum JoinHandleEnum<T> {
    /// Tokio join handle.
    #[cfg(feature = "executor-tokio")]
    Tokio(crate::executor::tokio::TokioJoinHandle<T>),
    #[doc(hidden)]
    Phantom(PhantomJoinHandle<T>),
}

impl<T> Debug for JoinHandleEnum<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(feature = "executor-tokio")]
            Self::Tokio(handle) => f.debug_tuple("Tokio").field(handle).finish(),
            JoinHandleEnum::Phantom(_) => f.debug_tuple("Phantom").finish(),
        }
    }
}

impl<T> Future for JoinHandleEnum<T>
where
    T: Send + 'static,
{
    type Output = std::result::Result<T, Box<dyn std::error::Error + Send + Sync + 'static>>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        let this = self.project();
        match this {
            #[cfg(feature = "executor-tokio")]
            JoinHandleEnumProj::Tokio(handle) => handle.poll_unpin(cx),
            JoinHandleEnumProj::Phantom(_) => panic!("polling a phantom join handle, cx: {:?}", cx),
        }
    }
}

impl<T> JoinHandle<T> for JoinHandleEnum<T> where T: Send + 'static {}

#[cfg(feature = "executor-tokio")]
impl<T> From<crate::executor::tokio::TokioJoinHandle<T>> for JoinHandleEnum<T> {
    fn from(inner: crate::executor::tokio::TokioJoinHandle<T>) -> Self {
        Self::Tokio(inner)
    }
}

/// An enum wrapper for different executor implementations.
#[derive(Debug, Clone)]
pub enum ExecutorEnum {
    /// Tokio runtime executor.
    #[cfg(feature = "executor-tokio")]
    TokioRuntime(crate::executor::tokio::TokioRuntimeExecutor),
    /// Tokio handle executor.
    #[cfg(feature = "executor-tokio")]
    TokioHandle(crate::executor::tokio::TokioHandleExecutor),
    #[doc(hidden)]
    Phantom,
}

impl Executor for ExecutorEnum {
    type JoinHandle<T>
        = JoinHandleEnum<T>
    where
        T: Send + 'static;

    fn spawn<F>(&self, future: F) -> Self::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        match self {
            #[cfg(feature = "executor-tokio")]
            ExecutorEnum::TokioRuntime(executor) => JoinHandleEnum::Tokio(executor.spawn(future)),
            #[cfg(feature = "executor-tokio")]
            ExecutorEnum::TokioHandle(executor) => JoinHandleEnum::Tokio(executor.spawn(future)),
            ExecutorEnum::Phantom => {
                drop(future);
                panic!("spawning a future on a phantom executor")
            }
        }
    }

    fn spawn_blocking<F, R>(&self, func: F) -> Self::JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        match self {
            #[cfg(feature = "executor-tokio")]
            ExecutorEnum::TokioRuntime(executor) => JoinHandleEnum::Tokio(executor.spawn_blocking(func)),
            #[cfg(feature = "executor-tokio")]
            ExecutorEnum::TokioHandle(executor) => JoinHandleEnum::Tokio(executor.spawn_blocking(func)),
            ExecutorEnum::Phantom => {
                drop(func);
                panic!("spawning a future on a phantom executor");
            }
        }
    }
}

#[cfg(feature = "executor-tokio")]
impl From<crate::executor::tokio::TokioRuntimeExecutor> for ExecutorEnum {
    fn from(inner: crate::executor::tokio::TokioRuntimeExecutor) -> Self {
        Self::TokioRuntime(inner)
    }
}

#[cfg(feature = "executor-tokio")]
impl From<crate::executor::tokio::TokioHandleExecutor> for ExecutorEnum {
    fn from(inner: crate::executor::tokio::TokioHandleExecutor) -> Self {
        Self::TokioHandle(inner)
    }
}
