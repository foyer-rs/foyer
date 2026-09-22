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

pub mod monitor;
pub mod noop;
pub mod psync;

#[cfg(target_os = "linux")]
pub mod uring;

use crate::{Statistics, iov2::bytes::IoB};

#[cfg(feature = "tracing")]
use fastrace::{future::InSpan, prelude::*};
use foyer_common::{error::Result, spawn::Spawner};
use futures_core::future::BoxFuture;
use pin_project::pin_project;

use std::{
    any::Any,
    fmt::Debug,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, ready},
};

/// Raw os file resource.
///
/// Use `fd` with unix and wasm, use `handle` with windows.
#[cfg(any(target_family = "unix", target_family = "wasm"))]
#[derive(Debug)]
pub struct RawFile(pub std::os::fd::RawFd);

/// Raw os file resource.
///
/// Use `fd` with unix and wasm, use `handle` with windows.
#[cfg(target_family = "windows")]
#[derive(Debug)]
pub struct RawFile(pub std::os::windows::io::RawHandle);

unsafe impl Send for RawFile {}
unsafe impl Sync for RawFile {}

#[derive(Debug)]
pub enum IoTarget {
    File { raw: RawFile, offset: u64 },
    Object { name: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IoOp {
    Read,
    Write,
}

#[derive(Debug)]
pub struct IoUnit {
    pub target: IoTarget,
    pub op: IoOp,
    pub buf: Box<dyn IoB>,

    pub statistics: Arc<Statistics>,
}

pub trait IoEngine: Send + Sync + 'static + Debug + Any {
    fn submit(&self, unit: IoUnit) -> IoHandle;
}

#[cfg(not(feature = "tracing"))]
type IoHandleInner = BoxFuture<'static, (Box<dyn IoB>, Result<()>)>;
#[cfg(feature = "tracing")]
type IoHandleInner = InSpan<BoxFuture<'static, (Box<dyn IoB>, Result<()>)>>;

/// A detached I/O handle that can be polled for completion.
#[pin_project]
pub struct IoHandle {
    #[pin]
    inner: IoHandleInner,
    callback: Option<Box<dyn FnOnce() + Send + 'static>>,
}

impl Debug for IoHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IoHandle").finish()
    }
}

#[cfg(not(feature = "tracing"))]
impl From<BoxFuture<'static, (Box<dyn IoB>, Result<()>)>> for IoHandle {
    fn from(inner: BoxFuture<'static, (Box<dyn IoB>, Result<()>)>) -> Self {
        Self { inner, callback: None }
    }
}

#[cfg(feature = "tracing")]
impl From<BoxFuture<'static, (Box<dyn IoB>, Result<()>)>> for IoHandle {
    fn from(inner: BoxFuture<'static, (Box<dyn IoB>, Result<()>)>) -> Self {
        let inner = inner.in_span(Span::enter_with_local_parent("foyer::storage::io::io_handle"));
        Self { inner, callback: None }
    }
}

impl IoHandle {
    pub(crate) fn with_callback<F>(mut self, callback: F) -> Self
    where
        F: FnOnce() + Send + 'static,
    {
        assert!(self.callback.is_none(), "io handle callback can only be set once");
        self.callback = Some(Box::new(callback));
        self
    }
}

impl Future for IoHandle {
    type Output = (Box<dyn IoB>, Result<()>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let res = ready!(this.inner.poll(cx));
        if let Some(callback) = this.callback.take() {
            callback();
        }
        Poll::Ready(res)
    }
}

/// Context for building the disk cache io engine.
pub struct IoEngineBuildContext {
    /// The runtime for the disk cache engine.
    pub spawner: Spawner,
}

/// I/O engine config trait.
pub trait IoEngineConfig: Send + Sync + 'static + Debug {
    /// Build an I/O engine from the given configuration.
    fn build(self: Box<Self>, ctx: IoEngineBuildContext) -> BoxFuture<'static, Result<Arc<dyn IoEngine>>>;

    /// Box the config.
    fn boxed(self) -> Box<Self>
    where
        Self: Sized,
    {
        Box::new(self)
    }
}
