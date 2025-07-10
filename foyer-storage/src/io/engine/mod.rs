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
    fs::{create_dir_all, File, OpenOptions},
    future::Future,
    mem::ManuallyDrop,
    os::{
        fd::{AsRawFd, FromRawFd, RawFd},
        unix::fs::FileExt,
    },
    path::{Path, PathBuf},
    pin::Pin,
    sync::{mpsc, Arc},
    task::{ready, Context, Poll},
};

use fs4::free_space;
use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use io_uring::{opcode, types::Fd, IoUring};
use pin_project::pin_project;
use tokio::sync::oneshot;

use crate::{
    io::{
        bytes::{IoB, IoBuf, IoBufMut, IoSlice, IoSliceMut},
        PAGE,
    },
    Throttle,
};

#[derive(Debug, thiserror::Error)]
pub enum IoError {
    #[error("I/O operation failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("Other error: {0}")]
    Other(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl IoError {
    pub fn from_raw_os_error(raw: i32) -> Self {
        Self::Io(std::io::Error::from_raw_os_error(raw))
    }

    pub fn other<E>(e: E) -> Self
    where
        E: Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    {
        Self::Other(e.into())
    }
}

pub type IoResult<T> = std::result::Result<T, IoError>;

pub type RegionId = u64;

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

pub trait DeviceBuilder: Send + Sync + 'static {
    /// Build a device from the given configuration.
    fn build(self) -> IoResult<Arc<dyn Device>>;
}

pub trait Device: Send + Sync + 'static {
    /// The capacity of the device, must be 4K aligned.
    fn capacity(&self) -> usize;
    /// The region size of the device, must be 4K aligned.
    fn region_size(&self) -> usize;
    /// The throttle config for the device.
    fn throttle(&self) -> &Throttle;
    /// Translate a region and offset to a raw file descriptor and offset.
    fn translate(&self, region: RegionId, offset: u64) -> (RawFd, u64);
}

#[derive(Debug)]
pub struct FileDeviceBuilder {
    path: PathBuf,
    capacity: Option<usize>,
    region_size: Option<usize>,
    throttle: Throttle,
}

impl FileDeviceBuilder {
    const DEFAULT_REGION_SIZE: usize = 64 * 1024 * 1024;

    /// Use the given file path as the file device path.
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().into(),
            capacity: None,
            region_size: None,
            throttle: Throttle::default(),
        }
    }

    /// Set the capacity of the file device.
    ///
    /// The given capacity may be modified on build for alignment.
    ///
    /// The file device uses 80% of the current free disk space by default.
    pub fn with_capacity(mut self, capacity: usize) -> Self {
        self.capacity = Some(capacity);
        self
    }

    pub fn with_region_size(mut self, region_size: usize) -> Self {
        self.region_size = Some(region_size);
        self
    }

    pub fn with_throttle(mut self, throttle: Throttle) -> Self {
        self.throttle = throttle;
        self
    }
}

impl DeviceBuilder for FileDeviceBuilder {
    fn build(self) -> IoResult<Arc<dyn Device>> {
        // Normalize configurations.

        let align_v = |value: usize, align: usize| value - value % align;

        let capacity = self.capacity.unwrap_or({
            // Create an empty directory before to get free space.
            let dir = self.path.parent().expect("path must point to a file").to_path_buf();
            create_dir_all(&dir).unwrap();
            free_space(&dir).unwrap() as usize / 10 * 8
        });
        let capacity = align_v(capacity, PAGE);

        let region_size = self.region_size.unwrap_or(Self::DEFAULT_REGION_SIZE).min(capacity / 16);
        let region_size = align_v(region_size, PAGE);

        let capacity = align_v(capacity, region_size);

        // Verify configurations.

        if region_size == 0 || region_size % PAGE != 0 {
            return Err(IoError::other(anyhow::anyhow!(
                "region size ({region_size}) must be a multiplier of PAGE ({PAGE})",
            )));
        }

        if capacity == 0 || capacity % region_size != 0 {
            return Err(IoError::other(anyhow::anyhow!(
                "capacity ({capacity}) must be a multiplier of region size ({region_size})",
            )));
        }

        // Build device.

        let mut opts = OpenOptions::new();
        opts.create(true).write(true).read(true);
        let file = opts.open(&self.path)?;
        let throttle = self.throttle;

        let inner = FileDeviceInner {
            file,
            capacity,
            region_size,
            throttle,
        };
        let inner = Arc::new(inner);
        let device = FileDevice { inner };
        let device: Arc<dyn Device> = Arc::new(device);
        Ok(device)
    }
}

#[derive(Debug)]
struct FileDeviceInner {
    file: File,
    capacity: usize,
    region_size: usize,
    throttle: Throttle,
}

#[derive(Debug, Clone)]
pub struct FileDevice {
    inner: Arc<FileDeviceInner>,
}

impl Device for FileDevice {
    fn capacity(&self) -> usize {
        self.inner.capacity
    }

    fn region_size(&self) -> usize {
        self.inner.region_size
    }

    fn throttle(&self) -> &Throttle {
        &self.inner.throttle
    }

    fn translate(&self, region: RegionId, offset: u64) -> (RawFd, u64) {
        let fd = self.inner.file.as_raw_fd();
        let offset = region * self.inner.region_size as u64 + offset;
        (fd, offset)
    }
}

pub struct PsyncIoEngineBuilder {
    device: Arc<dyn Device>,
}

impl PsyncIoEngineBuilder {
    pub fn new(device: Arc<dyn Device>) -> Self {
        Self { device }
    }
}

impl IoEngineBuilder for PsyncIoEngineBuilder {
    fn build(self) -> IoResult<Arc<dyn IoEngine>> {
        let engine = PsyncIoEngine { device: self.device };
        let engine = Arc::new(engine);
        Ok(engine)
    }
}

pub struct PsyncIoEngine {
    device: Arc<dyn Device>,
}

impl IoEngine for PsyncIoEngine {
    fn read(&self, buf: Box<dyn IoBufMut>, region: RegionId, offset: u64) -> IoHandle {
        let device = self.device.clone();
        async move {
            let res = tokio::task::spawn_blocking(move || {
                let (fd, offset) = device.translate(region, offset);
                let file = ManuallyDrop::new(unsafe { File::from_raw_fd(fd) });
                let (ptr, len) = buf.as_raw_parts();
                let slice = unsafe { std::slice::from_raw_parts_mut(ptr, len) };
                file.read_exact_at(slice, offset).map_err(IoError::from)?;
                let buf: Box<dyn IoB> = buf;
                Ok(buf)
            })
            .await
            .map_err(IoError::other)?;
            res
        }
        .boxed()
        .into()
    }

    fn write(&self, buf: Box<dyn IoBuf>, region: RegionId, offset: u64) -> IoHandle {
        let device = self.device.clone();
        async move {
            let res = tokio::task::spawn_blocking(move || {
                let (fd, offset) = device.translate(region, offset);
                let file = ManuallyDrop::new(unsafe { File::from_raw_fd(fd) });
                let (ptr, len) = buf.as_raw_parts();
                let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
                file.write_all_at(slice, offset).map_err(IoError::from)?;
                let buf: Box<dyn IoB> = buf;
                Ok(buf)
            })
            .await
            .map_err(IoError::other)?;
            res
        }
        .boxed()
        .into()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UringIoType {
    Read,
    Write,
}

struct UringIoRequest {
    tx: oneshot::Sender<IoResult<Box<dyn IoB>>>,
    io_type: UringIoType,
    buf: Box<dyn IoB>,
    region: RegionId,
    offset: u64,
}

struct UringIoCtx {
    tx: oneshot::Sender<IoResult<Box<dyn IoB>>>,
    io_type: UringIoType,
    buf: Box<dyn IoB>,
    region: RegionId,
    offset: u64,
}

struct UringIoEngineShard {
    rx: mpsc::Receiver<UringIoRequest>,
    device: Arc<dyn Device>,
    uring: IoUring,
    io_depth: usize,
    inflight: usize,
}

impl UringIoEngineShard {
    fn run(mut self) {
        loop {
            'recv: while self.inflight < self.io_depth {
                let request = match self.rx.try_recv() {
                    Ok(request) => request,
                    Err(mpsc::TryRecvError::Empty) => break 'recv,
                    Err(mpsc::TryRecvError::Disconnected) => return,
                };

                self.inflight += 1;

                let (ptr, len) = request.buf.as_raw_parts();
                let ctx = Box::new(UringIoCtx {
                    tx: request.tx,
                    io_type: request.io_type,
                    buf: request.buf,
                    region: request.region,
                    offset: request.offset,
                });
                let data = Box::into_raw(ctx) as u64;

                let (fd, offset) = self.device.translate(request.region, request.offset);
                let fd = Fd(fd);
                let sqe = match request.io_type {
                    UringIoType::Read => opcode::Read::new(fd, ptr, len as _).offset(offset).build(),
                    UringIoType::Write => opcode::Write::new(fd, ptr, len as _).offset(offset).build(),
                }
                .user_data(data);
                unsafe { self.uring.submission().push(&sqe).unwrap() }
            }

            if self.inflight > 0 {
                self.uring.submit_and_wait(1).unwrap();
            }

            for cqe in self.uring.completion() {
                let data = cqe.user_data();
                let ctx = unsafe { Box::from_raw(data as *mut UringIoCtx) };

                let res = cqe.result();
                if res < 0 {
                    let err = IoError::from_raw_os_error(res);
                    let _ = ctx.tx.send(Err(err));
                } else {
                    let _ = ctx.tx.send(Ok(ctx.buf));
                }
                self.inflight -= 1;
            }
        }
    }
}

pub struct UringIoEngine {
    txs: Vec<mpsc::SyncSender<UringIoRequest>>,
}

// impl IoEngine for UringIoEngine {
//     // fn submit(&self, request: IoRequest) -> IoHandle {
//     //     let (tx, rx) = oneshot::channel();
//     //     let shard = &self.txs[request.region as usize % self.txs.len()];
//     //     let _ = shard.send((request, tx));
//     //     rx
//     // }
// }

impl UringIoEngine {
    fn read(&self, buf: Box<dyn IoBufMut>, region: RegionId, offset: u64) -> IoHandle {
        let (tx, rx) = oneshot::channel();
        let shard = &self.txs[region as usize % self.txs.len()];
        let _ = shard.send(UringIoRequest {
            tx,
            io_type: UringIoType::Read,
            buf,
            region,
            offset,
        });
        async move {
            let recv = rx.await.map_err(IoError::other)?;
            recv
        }
        .boxed()
        .into()
    }

    fn write(&self, buf: Box<dyn IoBuf>, region: RegionId, offset: u64) -> IoHandle {
        let (tx, rx) = oneshot::channel();
        let shard = &self.txs[region as usize % self.txs.len()];
        let _ = shard.send(UringIoRequest {
            tx,
            io_type: UringIoType::Write,
            buf,
            region,
            offset,
        });
        async move {
            let recv = rx.await.map_err(IoError::other)?;
            recv
        }
        .boxed()
        .into()
    }
}

pub struct UringIoEngineBuilder {
    device: Arc<dyn Device>,
    threads: usize,
    io_depth: usize,
}

impl UringIoEngineBuilder {
    pub fn new(device: Arc<dyn Device>) -> Self {
        Self {
            device,
            threads: 1,
            io_depth: 64,
        }
    }

    /// Set the number of threads to use for the I/O engine.
    pub fn with_threads(mut self, threads: usize) -> Self {
        self.threads = threads;
        self
    }

    /// Set the I/O depth for each thread.
    pub fn with_io_depth(mut self, io_depth: usize) -> Self {
        self.io_depth = io_depth;
        self
    }
}

impl IoEngineBuilder for UringIoEngineBuilder {
    fn build(self) -> IoResult<Arc<dyn IoEngine>> {
        if self.threads == 0 {
            return Err(IoError::other(anyhow::anyhow!("shards must be greater than 0")));
        }

        let (txs, rxs): (Vec<mpsc::SyncSender<_>>, Vec<mpsc::Receiver<_>>) = (0..self.threads)
            .map(|_| {
                let (tx, rx) = mpsc::sync_channel(1024);
                (tx, rx)
            })
            .unzip();

        for rx in rxs {
            let uring = IoUring::new(self.io_depth as _)?;
            let shard = UringIoEngineShard {
                rx,
                device: self.device.clone(),
                uring,
                io_depth: self.io_depth,
                inflight: 0,
            };

            std::thread::spawn(move || {
                shard.run();
            });
        }

        let engine = UringIoEngine { txs };
        let engine = Arc::new(engine);
        Ok(engine)
    }
}

impl IoEngine for UringIoEngine {
    fn read(&self, buf: Box<dyn IoBufMut>, region: RegionId, offset: u64) -> IoHandle {
        self.read(buf, region, offset)
    }

    fn write(&self, buf: Box<dyn IoBuf>, region: RegionId, offset: u64) -> IoHandle {
        self.write(buf, region, offset)
    }
}

#[cfg(test)]
mod tests {
    use rand::{rng, Fill};
    use tempfile::tempdir;

    use super::*;

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
