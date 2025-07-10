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
    any::Any,
    fmt::Debug,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
};

use futures_core::future::BoxFuture;
use pin_project::pin_project;

use crate::io::{
    bytes::{IoB, IoBuf, IoBufMut, IoSlice, IoSliceMut},
    device::RegionId,
    error::{IoError, IoResult},
};

#[pin_project]
pub struct IoHandle {
    #[pin]
    inner: BoxFuture<'static, IoResult<Box<dyn IoB>>>,
}

impl Debug for IoHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IoHandle").finish()
    }
}

impl From<BoxFuture<'static, IoResult<Box<dyn IoB>>>> for IoHandle {
    fn from(inner: BoxFuture<'static, IoResult<Box<dyn IoB>>>) -> Self {
        Self { inner }
    }
}

impl Future for IoHandle {
    type Output = IoResult<Box<dyn IoB>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let res = ready!(this.inner.poll(cx));
        match res {
            Ok(r) => Poll::Ready(Ok(r)),
            Err(e) => Poll::Ready(Err(IoError::other(e))),
        }
    }
}

impl IoHandle {
    pub fn into_slice_handle(self) -> SliceIoHandle {
        SliceIoHandle { inner: self }
    }

    pub fn into_slice_mut_handle(self) -> SliceMutIoHandle {
        SliceMutIoHandle { inner: self }
    }
}

#[pin_project]
pub struct SliceIoHandle {
    #[pin]
    inner: IoHandle,
}

impl Debug for SliceIoHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SliceIoHandle").finish()
    }
}

impl Future for SliceIoHandle {
    type Output = IoResult<Box<IoSlice>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let res = ready!(this.inner.poll(cx));
        match res {
            Err(e) => Poll::Ready(Err(e)),
            Ok(iob) => {
                let iob: Box<dyn Any> = iob;
                let slice = iob
                    .downcast::<IoSlice>()
                    .expect("Cannot downcast to IoSlice, type mismatch.");
                Poll::Ready(Ok(slice))
            }
        }
    }
}

#[pin_project]
pub struct SliceMutIoHandle {
    #[pin]
    inner: IoHandle,
}

impl Debug for SliceMutIoHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SliceIoHandle").finish()
    }
}

impl Future for SliceMutIoHandle {
    type Output = IoResult<Box<IoSliceMut>>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let res = ready!(this.inner.poll(cx));
        match res {
            Err(e) => Poll::Ready(Err(e)),
            Ok(iob) => {
                let iob: Box<dyn Any> = iob;
                let slice = iob
                    .downcast::<IoSliceMut>()
                    .expect("Cannot downcast to IoSlice, type mismatch.");
                Poll::Ready(Ok(slice))
            }
        }
    }
}

pub trait IoEngineBuilder: Send + Sync + 'static {
    /// Build an I/O engine from the given configuration.
    fn build(self) -> IoResult<Arc<dyn IoEngine>>;
}

pub trait IoEngine: Send + Sync + 'static {
    fn read(&self, buf: Box<dyn IoBufMut>, region: RegionId, offset: u64) -> IoHandle;
    fn write(&self, buf: Box<dyn IoBuf>, region: RegionId, offset: u64) -> IoHandle;
}

pub mod psync;
pub mod uring;

#[cfg(test)]
mod tests {
    use std::path::Path;

    use rand::{rng, Fill};
    use tempfile::tempdir;

    use super::*;
    use crate::io::{
        device::{file::FileDeviceBuilder, Device, DeviceBuilder},
        engine::{psync::PsyncIoEngineBuilder, uring::UringIoEngineBuilder},
    };

    const KIB: usize = 1024;
    const MIB: usize = 1024 * 1024;

    fn build_test_file_device(path: impl AsRef<Path>) -> IoResult<Arc<dyn Device>> {
        FileDeviceBuilder::new(&path)
            .with_capacity(16 * MIB)
            .with_region_size(1 * MIB)
            .build()
    }

    fn build_psync_io_engine(device: Arc<dyn Device>) -> IoResult<Arc<dyn IoEngine>> {
        PsyncIoEngineBuilder::new(device).build()
    }

    fn build_uring_io_engine(device: Arc<dyn Device>) -> IoResult<Arc<dyn IoEngine>> {
        UringIoEngineBuilder::new(device.clone())
            .with_threads(4)
            .with_io_depth(64)
            .build()
    }

    async fn test_read_write(engine: Arc<dyn IoEngine>) {
        let mut b1 = Box::new(IoSliceMut::new(16 * KIB));
        Fill::fill(&mut b1[..], &mut rng());

        let b1 = engine.write(b1, 0, 0).into_slice_mut_handle().await.unwrap();

        let b2 = Box::new(IoSliceMut::new(16 * KIB));
        let b2 = engine.read(b2, 0, 0).into_slice_mut_handle().await.unwrap();
        assert_eq!(b1, b2);
    }

    #[test_log::test(tokio::test)]
    async fn test_io_engine() {
        let dir = tempdir().unwrap();

        let path = dir.path().join("test_file_1");
        let device = build_test_file_device(&path).unwrap();
        let engine = build_uring_io_engine(device.clone()).unwrap();
        test_read_write(engine).await;

        let path = dir.path().join("test_file_1");
        let device = build_test_file_device(&path).unwrap();
        let engine = build_psync_io_engine(device.clone()).unwrap();
        test_read_write(engine).await;
    }
}
