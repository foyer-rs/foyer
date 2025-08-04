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

pub mod monitor;
pub mod noop;
pub mod psync;

#[cfg(target_os = "linux")]
pub mod uring;

use std::{
    fmt::Debug,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures_core::future::BoxFuture;
use pin_project::pin_project;

use crate::io::{
    bytes::{IoB, IoBuf, IoBufMut},
    device::Partition,
    error::IoResult,
};

/// A detached I/O handle that can be polled for completion.
#[pin_project]
pub struct IoHandle {
    #[pin]
    inner: BoxFuture<'static, (Box<dyn IoB>, IoResult<()>)>,
}

impl Debug for IoHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IoHandle").finish()
    }
}

impl From<BoxFuture<'static, (Box<dyn IoB>, IoResult<()>)>> for IoHandle {
    fn from(inner: BoxFuture<'static, (Box<dyn IoB>, IoResult<()>)>) -> Self {
        Self { inner }
    }
}

impl Future for IoHandle {
    type Output = (Box<dyn IoB>, IoResult<()>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        this.inner.poll(cx)
    }
}

/// I/O engine builder trait.
pub trait IoEngineBuilder: Send + Sync + 'static + Debug {
    /// Build an I/O engine from the given configuration.
    fn build(self) -> BoxFuture<'static, IoResult<Arc<dyn IoEngine>>>;
}

/// I/O engine builder trait.
pub trait IoEngine: Send + Sync + 'static + Debug {
    /// Read data into the buffer from the specified partition and offset.
    fn read(&self, buf: Box<dyn IoBufMut>, partition: &dyn Partition, offset: u64) -> IoHandle;
    /// Write data from the buffer to the specified region and offset.
    fn write(&self, buf: Box<dyn IoBuf>, partition: &dyn Partition, offset: u64) -> IoHandle;
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use rand::{rng, Fill};
    use tempfile::tempdir;

    use super::*;
    #[cfg(target_os = "linux")]
    use crate::io::engine::uring::UringIoEngineBuilder;
    use crate::io::{
        bytes::IoSliceMut,
        device::{file::FileDeviceBuilder, Device, DeviceBuilder},
        engine::psync::PsyncIoEngineBuilder,
    };

    const KIB: usize = 1024;
    const MIB: usize = 1024 * 1024;

    fn build_test_file_device(path: impl AsRef<Path>) -> IoResult<Arc<dyn Device>> {
        let device = FileDeviceBuilder::new(&path).with_capacity(16 * MIB).build()?;
        for _ in 0..16 {
            device.create_partition(MIB)?;
        }
        Ok(device)
    }

    async fn test_read_write(engine: Arc<dyn IoEngine>, device: &dyn Device) {
        let mut b1 = Box::new(IoSliceMut::new(16 * KIB));
        Fill::fill(&mut b1[..], &mut rng());

        let (b1, res) = engine.write(b1, device.partition(0).as_ref(), 0).await;
        res.unwrap();
        let b1 = b1.try_into_io_slice_mut().unwrap();

        let b2 = Box::new(IoSliceMut::new(16 * KIB));
        let (b2, res) = engine.read(b2, device.partition(0).as_ref(), 0).await;
        res.unwrap();
        let b2 = b2.try_into_io_slice_mut().unwrap();
        assert_eq!(b1, b2);
    }

    #[test_log::test(tokio::test)]
    async fn test_io_engine() {
        let dir = tempdir().unwrap();

        #[cfg(target_os = "linux")]
        {
            let path = dir.path().join("test_file_1");
            let device = build_test_file_device(&path).unwrap();
            let engine = UringIoEngineBuilder::new()
                .with_threads(4)
                .with_io_depth(64)
                .build()
                .await
                .unwrap();
            test_read_write(engine, device.as_ref()).await;
        }

        let path = dir.path().join("test_file_1");
        let device = build_test_file_device(&path).unwrap();
        let engine = PsyncIoEngineBuilder::new().build().await.unwrap();
        test_read_write(engine, device.as_ref()).await;
    }
}
