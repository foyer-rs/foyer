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
    fs::{create_dir_all, File, OpenOptions},
    mem::ManuallyDrop,
    os::{
        fd::{AsRawFd, FromRawFd, RawFd},
        unix::fs::FileExt,
    },
    path::{Path, PathBuf},
    sync::{mpsc, Arc},
};

use fs4::free_space;
use io_uring::{opcode, squeue::Entry as Sqe, types::Fd, IoUring};
use tokio::sync::oneshot;

use crate::{io::PAGE, Throttle};

#[derive(Debug, Clone, Copy)]
pub struct BufferPtr {
    pub ptr: *mut u8,
    pub len: usize,
}

unsafe impl Send for BufferPtr {}
unsafe impl Sync for BufferPtr {}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoType {
    Read,
    Write,
}

pub struct IoRequest {
    pub io_type: IoType,
    pub buf: BufferPtr,
    pub region: RegionId,
    pub offset: u64,
}

pub struct IoResponse {
    pub io_type: IoType,
    pub buf: BufferPtr,
    pub region: RegionId,
    pub offset: u64,
}

// FIXME(MrCroxx): To make io task cancellation safe, we need RAII resource management.

pub type IoNotifier = oneshot::Sender<IoResult<IoResponse>>;
pub type IoHandle = oneshot::Receiver<IoResult<IoResponse>>;

pub trait IoEngineBuilder: Send + Sync + 'static {
    /// Build an I/O engine from the given configuration.
    fn build(self) -> IoResult<Arc<dyn IoEngine>>;
}

pub trait IoEngine: Send + Sync + 'static {
    fn submit(&self, requests: IoRequest) -> IoHandle;
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

// pub trait DeviceExt: Device {
//     #[must_use]
//     fn write_bytes(
//         &self,
//         buf: IoBytes,
//         region: RegionId,
//         offset: u64,
//     ) -> Box<dyn Future<Output = IoResult<IoBytes>> + Send + 'static> {
//         todo!()
//     }

//     #[must_use]
//     fn write_bytes_mut(
//         &self,
//         buf: IoBytesMut,
//         region: RegionId,
//         offset: u64,
//     ) -> Box<dyn Future<Output = IoResult<IoBytesMut>> + Send + 'static> {
//         todo!()
//     }

//     #[must_use]
//     fn read_bytes_mut(
//         &self,
//         buf: IoBytesMut,
//         region: RegionId,
//         offset: u64,
//     ) -> Box<dyn Future<Output = IoResult<IoBytesMut>> + Send + 'static> {
//         todo!()
//     }
// }

// impl<T> DeviceExt for T where T: Device + ?Sized {}

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
    fn submit(&self, request: IoRequest) -> IoHandle {
        let (tx, rx) = oneshot::channel();
        let device = self.device.clone();
        tokio::task::spawn_blocking(move || {
            let (fd, offset) = device.translate(request.region, request.offset);
            let file = ManuallyDrop::new(unsafe { File::from_raw_fd(fd) });
            let res = match request.io_type {
                IoType::Read => {
                    let buf = unsafe { std::slice::from_raw_parts_mut(request.buf.ptr, request.buf.len) };
                    file.read_exact_at(buf, offset)
                }
                IoType::Write => {
                    let buf = unsafe { std::slice::from_raw_parts(request.buf.ptr, request.buf.len) };
                    file.write_all_at(buf, offset)
                }
            };
            let res = res
                .map(|_| IoResponse {
                    io_type: request.io_type,
                    buf: request.buf,
                    region: request.region,
                    offset: request.offset,
                })
                .map_err(IoError::from);
            let _ = tx.send(res);
        });

        rx
    }
}

struct UringIoCtx {
    notifier: IoNotifier,
    io_type: IoType,
    buf: BufferPtr,
    region: RegionId,
    offset: u64,
}

struct UringIoEngineShard {
    rx: mpsc::Receiver<(IoRequest, IoNotifier)>,
    device: Arc<dyn Device>,
    uring: IoUring,
    io_depth: usize,
    inflight: usize,
}

impl UringIoEngineShard {
    fn prepare(&mut self, request: IoRequest) -> Sqe {
        let (fd, offset) = self.device.translate(request.region, request.offset);
        let fd = Fd(fd);
        let ptr = request.buf.ptr;
        let len = request.buf.len as _;
        match request.io_type {
            IoType::Read => opcode::Read::new(fd, ptr, len).offset(offset).build(),
            IoType::Write => opcode::Write::new(fd, ptr, len).offset(offset).build(),
        }
    }

    fn notify(&mut self) {}

    fn run(mut self) {
        loop {
            'recv: while self.inflight < self.io_depth {
                let (request, notifier) = match self.rx.try_recv() {
                    Ok((request, notifier)) => (request, notifier),
                    Err(mpsc::TryRecvError::Empty) => break 'recv,
                    Err(mpsc::TryRecvError::Disconnected) => return,
                };
                self.inflight += 1;
                let ctx = Box::new(UringIoCtx {
                    notifier,
                    io_type: request.io_type,
                    buf: request.buf,
                    region: request.region,
                    offset: request.offset,
                });
                let data = Box::into_raw(ctx) as u64;
                let sqe = self.prepare(request).user_data(data);
                unsafe { self.uring.submission().push(&sqe).unwrap() }
            }

            if self.inflight > 0 {
                self.uring.submit_and_wait(1).unwrap();
            }

            for cqe in self.uring.completion() {
                let data = cqe.user_data();
                let ctx = unsafe { Box::from_raw(data as *mut UringIoCtx) };
                let response = IoResponse {
                    io_type: ctx.io_type,
                    buf: ctx.buf,
                    region: ctx.region,
                    offset: ctx.offset,
                };
                let res = cqe.result();
                if res < 0 {
                    let err = IoError::from_raw_os_error(res);
                    let _ = ctx.notifier.send(Err(err));
                } else {
                    let _ = ctx.notifier.send(Ok(response));
                }
                self.inflight -= 1;
            }
        }
    }
}

pub struct UringIoEngine {
    txs: Vec<mpsc::SyncSender<(IoRequest, IoNotifier)>>,
}

impl IoEngine for UringIoEngine {
    fn submit(&self, request: IoRequest) -> IoHandle {
        let (tx, rx) = oneshot::channel();
        let shard = &self.txs[request.region as usize % self.txs.len()];
        let _ = shard.send((request, tx));
        rx
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

        let (txs, rxs): (
            Vec<mpsc::SyncSender<(IoRequest, IoNotifier)>>,
            Vec<mpsc::Receiver<(IoRequest, IoNotifier)>>,
        ) = (0..self.threads)
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

#[cfg(test)]
mod tests {
    use rand::{rng, Fill};
    use tempfile::tempdir;

    use super::*;
    use crate::io::bytes::Raw;

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
        let mut b1 = Raw::new(16 * KIB);
        Fill::fill(&mut b1[..], &mut rng());
        let (p1, l1) = b1.into_raw_parts();

        let recv = engine
            .submit(IoRequest {
                io_type: IoType::Write,
                buf: BufferPtr { ptr: p1, len: l1 },
                region: 0,
                offset: 0,
            })
            .await
            .unwrap();
        let _ = recv.unwrap();
        let b1 = unsafe { Raw::from_raw_parts(p1, l1) };
        let b2 = Raw::new(16 * KIB);
        let (p2, l2) = b2.into_raw_parts();

        let recv = engine
            .submit(IoRequest {
                io_type: IoType::Read,
                buf: BufferPtr { ptr: p2, len: l2 },
                region: 0,
                offset: 0,
            })
            .await
            .unwrap();
        let _ = recv.unwrap();
        let b2 = unsafe { Raw::from_raw_parts(p2, l2) };
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
