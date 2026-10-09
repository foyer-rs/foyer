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

use std::{
    fmt::Debug,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread::Thread,
};

use asyncband::oneshot;
use core_affinity::CoreId;
#[cfg(feature = "tracing")]
use fastrace::prelude::*;
use foyer_common::error::{Error, ErrorKind, Result};
use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use io_uring::{IoUring, opcode, types::Fd};

use crate::{
    RawFile,
    io::{
        bytes::{IoB, IoBuf, IoBufMut},
        device::Partition,
        engine::{IoEngine, IoEngineBuildContext, IoEngineConfig, IoHandle},
    },
};

/// Config for io_uring based I/O engine.
#[derive(Debug)]
pub struct UringIoEngineConfig {
    threads: usize,
    cpus: Vec<u32>,
    io_depth: usize,
    sqpoll: bool,
    sqpoll_cpus: Vec<u32>,
    sqpoll_idle: u32,
    iopoll: bool,
    weight: f64,

    #[cfg(any(test, feature = "test_utils"))]
    write_io_latency: Option<std::ops::Range<std::time::Duration>>,
    #[cfg(any(test, feature = "test_utils"))]
    read_io_latency: Option<std::ops::Range<std::time::Duration>>,
}

impl Default for UringIoEngineConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl UringIoEngineConfig {
    /// Create a new io_uring based I/O engine config with default configurations.
    pub fn new() -> Self {
        Self {
            threads: 1,
            cpus: vec![],
            io_depth: 64,
            sqpoll: false,
            sqpoll_cpus: vec![],
            sqpoll_idle: 10,
            iopoll: false,
            weight: 1.0,
            #[cfg(any(test, feature = "test_utils"))]
            write_io_latency: None,
            #[cfg(any(test, feature = "test_utils"))]
            read_io_latency: None,
        }
    }

    /// Set the number of threads to use for the I/O engine.
    pub fn with_threads(mut self, threads: usize) -> Self {
        self.threads = threads;
        self
    }

    /// Bind the engine threads to specific CPUs.
    ///
    /// The length of `cpus` must be equal to the threads.
    pub fn with_cpus(mut self, cpus: Vec<u32>) -> Self {
        self.cpus = cpus;
        self
    }

    /// Set the I/O depth for each thread.
    pub fn with_io_depth(mut self, io_depth: usize) -> Self {
        self.io_depth = io_depth;
        self
    }

    /// Enable or disable I/O polling.
    ///
    /// FYI:
    ///
    /// - [io_uring_setup(2)](https://man7.org/linux/man-pages/man2/io_uring_setup.2.html)
    /// - [crate - io-uring](https://docs.rs/io-uring/latest/io_uring/struct.Builder.html#method.setup_iopoll)
    ///
    /// Related syscall flag: `IORING_SETUP_IOPOLL`.
    ///
    /// NOTE:
    ///
    /// - If this feature is enabled, the underlying device MUST be opened with the `O_DIRECT` flag.
    /// - If this feature is enabled, the underlying device MUST support io polling.
    ///
    /// Default: `false`.
    pub fn with_iopoll(mut self, iopoll: bool) -> Self {
        self.iopoll = iopoll;
        self
    }

    /// Set the weight of read/write priorities.
    ///
    /// The engine will try to keep the read/write iodepth ratio as close to the specified weight as possible.
    pub fn with_weight(mut self, weight: f64) -> Self {
        self.weight = weight;
        self
    }

    /// Enable or disable SQ polling.
    ///
    /// FYI:
    ///
    /// - [io_uring_setup(2)](https://man7.org/linux/man-pages/man2/io_uring_setup.2.html)
    /// - [crate - io-uring](https://docs.rs/io-uring/latest/io_uring/struct.Builder.html#method.setup_sqpoll)
    ///
    /// Related syscall flag: `IORING_SETUP_IOPOLL`.
    ///
    /// NOTE: If this feature is enabled, the underlying device must be opened with the `O_DIRECT` flag.
    ///
    /// Default: `false`.
    pub fn with_sqpoll(mut self, sqpoll: bool) -> Self {
        self.sqpoll = sqpoll;
        self
    }

    /// Bind the kernel’s SQ poll thread to the specified cpu.
    ///
    /// This flag is only meaningful when [`Self::with_sqpoll`] is enabled.
    ///
    /// The length of `cpus` must be equal to the number of threads.
    pub fn with_sqpoll_cpus(mut self, cpus: Vec<u32>) -> Self {
        self.sqpoll_cpus = cpus;
        self
    }

    /// After idle milliseconds, the kernel thread will go to sleep and you will have to wake it up again with a system
    /// call.
    ///
    /// This flag is only meaningful when [`Self::with_sqpoll`] is enabled.
    pub fn with_sqpoll_idle(mut self, idle: u32) -> Self {
        self.sqpoll_idle = idle;
        self
    }

    /// Set the simulated additional write I/O latency for testing purposes.
    #[cfg(any(test, feature = "test_utils"))]
    pub fn with_write_io_latency(mut self, latency: std::ops::Range<std::time::Duration>) -> Self {
        self.write_io_latency = Some(latency);
        self
    }

    /// Set the simulated additional read I/O latency for testing purposes.
    #[cfg(any(test, feature = "test_utils"))]
    pub fn with_read_io_latency(mut self, latency: std::ops::Range<std::time::Duration>) -> Self {
        self.read_io_latency = Some(latency);
        self
    }
}

impl IoEngineConfig for UringIoEngineConfig {
    fn build(self: Box<Self>, _: IoEngineBuildContext) -> BoxFuture<'static, Result<Arc<dyn IoEngine>>> {
        async move {
            if self.threads == 0 {
                return Err(Error::new(ErrorKind::Config, "shards must be greater than 0")
                    .with_context("threads", self.threads));
            }

            let (read_txs, read_rxs): (Vec<mpsc::SyncSender<_>>, Vec<mpsc::Receiver<_>>) = (0..self.threads)
                .map(|_| {
                    let (tx, rx) = mpsc::sync_channel(4096);
                    (tx, rx)
                })
                .unzip();

            let (write_txs, write_rxs): (Vec<mpsc::SyncSender<_>>, Vec<mpsc::Receiver<_>>) = (0..self.threads)
                .map(|_| {
                    let (tx, rx) = mpsc::sync_channel(4096);
                    (tx, rx)
                })
                .unzip();

            let mut states = Vec::with_capacity(self.threads);
            for (i, (read_rx, write_rx)) in read_rxs.into_iter().zip(write_rxs).enumerate() {
                let mut builder = IoUring::builder();
                if self.iopoll {
                    builder.setup_iopoll();
                }
                if self.sqpoll {
                    builder.setup_sqpoll(self.sqpoll_idle);
                    if !self.sqpoll_cpus.is_empty() {
                        let cpu = self.sqpoll_cpus[i];
                        builder.setup_sqpoll_cpu(cpu);
                    }
                }
                let cpu = if self.cpus.is_empty() { None } else { Some(self.cpus[i]) };
                let uring = builder.build(self.io_depth as _).map_err(Error::io_error)?;
                let shard = UringIoEngineShard {
                    read_rx,
                    write_rx,
                    uring,
                    io_depth: self.io_depth,
                    weight: self.weight,
                    read_inflight: 0,
                    write_inflight: 0,
                    state: Default::default(),
                };
                states.push(shard.state.clone());

                std::thread::Builder::new()
                    .name(format!("foyer-uring-{i}"))
                    .spawn(move || {
                        if let Some(cpu) = cpu {
                            core_affinity::set_for_current(CoreId { id: cpu as _ });
                        }
                        shard.state.thread.set(std::thread::current()).unwrap();
                        shard.run();
                    })
                    .map_err(Error::io_error)?;
            }

            let engine = UringIoEngine {
                read_txs,
                write_txs,
                states,
            };
            let engine = Arc::new(engine);
            Ok(engine as Arc<dyn IoEngine>)
        }
        .boxed()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UringIoType {
    Read,
    Write,
}

struct RawBuf {
    ptr: *mut u8,
    len: usize,
}

unsafe impl Send for RawBuf {}
unsafe impl Sync for RawBuf {}

struct RawFileAddress {
    file: RawFile,
    offset: u64,
}

struct UringIoCtx {
    tx: oneshot::Sender<Result<()>>,
    io_type: UringIoType,
    rbuf: RawBuf,
    addr: RawFileAddress,
    #[cfg(feature = "tracing")]
    span: fastrace::Span,
}

#[derive(Default)]
struct UringIoEngineShardState {
    parked: AtomicBool,
    thread: OnceLock<Thread>,
}

impl UringIoEngineShardState {
    fn unpark(&self) {
        if self.parked.swap(false, Ordering::AcqRel) {
            self.thread.get().unwrap().unpark();
        }
    }
}

struct UringIoEngineShard {
    read_rx: mpsc::Receiver<UringIoCtx>,
    write_rx: mpsc::Receiver<UringIoCtx>,
    weight: f64,
    uring: IoUring,
    io_depth: usize,
    read_inflight: usize,
    write_inflight: usize,
    state: Arc<UringIoEngineShardState>,
}

impl UringIoEngineShard {
    fn try_recv(&self) -> std::result::Result<Option<UringIoCtx>, ()> {
        if (self.read_inflight as f64) < self.write_inflight as f64 * self.weight {
            match self.read_rx.try_recv() {
                Err(mpsc::TryRecvError::Disconnected) => Err(()),
                Ok(ctx) => Ok(Some(ctx)),
                Err(mpsc::TryRecvError::Empty) => match self.write_rx.try_recv() {
                    Err(mpsc::TryRecvError::Disconnected) => Err(()),
                    Ok(ctx) => Ok(Some(ctx)),
                    Err(mpsc::TryRecvError::Empty) => Ok(None),
                },
            }
        } else {
            match self.write_rx.try_recv() {
                Err(mpsc::TryRecvError::Disconnected) => Err(()),
                Ok(ctx) => Ok(Some(ctx)),
                Err(mpsc::TryRecvError::Empty) => match self.read_rx.try_recv() {
                    Err(mpsc::TryRecvError::Disconnected) => Err(()),
                    Ok(ctx) => Ok(Some(ctx)),
                    Err(mpsc::TryRecvError::Empty) => Ok(None),
                },
            }
        }
    }

    fn run(mut self) {
        let mut pending = None;
        loop {
            'prepare: loop {
                if self.read_inflight + self.write_inflight >= self.io_depth {
                    break 'prepare;
                }

                let ctx = match pending.take() {
                    Some(ctx) => Some(ctx),
                    None => match self.try_recv() {
                        Err(()) => return,
                        Ok(ctx) => ctx,
                    },
                };

                let ctx = match ctx {
                    Some(ctx) => ctx,
                    None => break 'prepare,
                };

                let ctx = Box::new(ctx);

                let fd = Fd(ctx.addr.file.0);
                let sqe = match ctx.io_type {
                    UringIoType::Read => {
                        self.read_inflight += 1;
                        opcode::Read::new(fd, ctx.rbuf.ptr, ctx.rbuf.len as _)
                            .offset(ctx.addr.offset)
                            .build()
                    }
                    UringIoType::Write => {
                        self.write_inflight += 1;
                        opcode::Write::new(fd, ctx.rbuf.ptr, ctx.rbuf.len as _)
                            .offset(ctx.addr.offset)
                            .build()
                    }
                };
                let data = Box::into_raw(ctx) as u64;
                let sqe = sqe.user_data(data);
                unsafe { self.uring.submission().push(&sqe).unwrap() }
            }

            if self.read_inflight + self.write_inflight > 0 {
                self.uring.submit().unwrap();
            }

            for cqe in self.uring.completion() {
                let data = cqe.user_data();
                let ctx = unsafe { Box::from_raw(data as *mut UringIoCtx) };

                match ctx.io_type {
                    UringIoType::Read => self.read_inflight -= 1,
                    UringIoType::Write => self.write_inflight -= 1,
                }

                let res = cqe.result();
                if res < 0 {
                    let err = Error::raw_os_io_error(res);
                    let _ = ctx.tx.send(Err(err));
                } else {
                    let _ = ctx.tx.send(Ok(()));
                }

                #[cfg(feature = "tracing")]
                drop(ctx.span);
            }

            if self.read_inflight + self.write_inflight == 0 {
                self.state.parked.store(true, Ordering::Release);
                match self.try_recv() {
                    Err(()) => return,
                    Ok(Some(ctx)) => pending = Some(ctx),
                    Ok(None) => std::thread::park(),
                }
                self.state.parked.store(false, Ordering::Release);
            }
        }
    }
}

/// The io_uring based I/O engine.
pub struct UringIoEngine {
    read_txs: Vec<mpsc::SyncSender<UringIoCtx>>,
    write_txs: Vec<mpsc::SyncSender<UringIoCtx>>,
    states: Vec<Arc<UringIoEngineShardState>>,
}

impl Drop for UringIoEngine {
    fn drop(&mut self) {
        self.read_txs.clear();
        self.write_txs.clear();
        for state in &self.states {
            state.unpark();
        }
    }
}

impl Debug for UringIoEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UringIoEngine").finish()
    }
}

impl UringIoEngine {
    fn submit(&self, partition: &dyn Partition, ctx: UringIoCtx) {
        let shard = partition.id() as usize % self.read_txs.len();
        let tx = match ctx.io_type {
            UringIoType::Read => &self.read_txs[shard],
            UringIoType::Write => &self.write_txs[shard],
        };
        let _ = tx.send(ctx);
        self.states[shard].unpark();
    }

    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::storage::io::engine::uring::read")
    )]
    fn read(&self, buf: Box<dyn IoBufMut>, partition: &dyn Partition, offset: u64) -> IoHandle {
        let (tx, rx) = oneshot::channel();
        let (ptr, len) = buf.as_raw_parts();
        let rbuf = RawBuf { ptr, len };
        let (file, offset) = partition.translate(offset);
        let addr = RawFileAddress { file, offset };
        #[cfg(feature = "tracing")]
        let span = Span::enter_with_local_parent("foyer::storage::io::engine::uring::read::io");
        let ctx = UringIoCtx {
            tx,
            io_type: UringIoType::Read,
            rbuf,
            addr,
            #[cfg(feature = "tracing")]
            span,
        };
        self.submit(partition, ctx);
        async move {
            let res = match rx.await {
                Ok(res) => res,
                Err(e) => Err(Error::new(ErrorKind::ChannelClosed, "io completion channel closed").with_source(e)),
            };
            let buf: Box<dyn IoB> = buf.into_iob();
            (buf, res)
        }
        .boxed()
        .into()
    }

    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::storage::io::engine::uring::write")
    )]
    fn write(&self, buf: Box<dyn IoBuf>, partition: &dyn Partition, offset: u64) -> IoHandle {
        let (tx, rx) = oneshot::channel();
        let (ptr, len) = buf.as_raw_parts();
        let rbuf = RawBuf { ptr, len };
        let (file, offset) = partition.translate(offset);
        let addr = RawFileAddress { file, offset };
        #[cfg(feature = "tracing")]
        let span = Span::enter_with_local_parent("foyer::storage::io::engine::uring::write::io");
        let ctx = UringIoCtx {
            tx,
            io_type: UringIoType::Write,
            rbuf,
            addr,
            #[cfg(feature = "tracing")]
            span,
        };
        self.submit(partition, ctx);
        async move {
            let res = match rx.await {
                Ok(res) => res,
                Err(e) => Err(Error::new(ErrorKind::ChannelClosed, "io completion channel closed").with_source(e)),
            };
            let buf: Box<dyn IoB> = buf.into_iob();
            (buf, res)
        }
        .boxed()
        .into()
    }
}

impl IoEngine for UringIoEngine {
    fn read(&self, buf: Box<dyn IoBufMut>, partition: &dyn Partition, offset: u64) -> IoHandle {
        self.read(buf, partition, offset)
    }

    fn write(&self, buf: Box<dyn IoBuf>, partition: &dyn Partition, offset: u64) -> IoHandle {
        self.write(buf, partition, offset)
    }
}

#[cfg(all(test, target_os = "linux", not(madsim)))]
mod tests {
    use std::{
        sync::{Arc, atomic::Ordering, mpsc},
        thread,
        time::Duration,
    };

    use tempfile::tempdir;

    use super::*;
    use crate::io::{
        bytes::Raw,
        device::{DeviceBuilder, file::FileDeviceBuilder},
    };

    #[test_log::test(tokio::test)]
    async fn idle_shard_wakes_for_io() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("uring_idle_wake");
        let device = FileDeviceBuilder::new(&path).with_capacity(4096).build().unwrap();
        let partition = device.create_partition(4096).unwrap();

        let (read_tx, read_rx) = mpsc::sync_channel(4096);
        let (write_tx, write_rx) = mpsc::sync_channel(4096);
        let state = Arc::new(UringIoEngineShardState::default());
        let shard = UringIoEngineShard {
            read_rx,
            write_rx,
            weight: 1.0,
            uring: IoUring::builder().build(1).unwrap(),
            io_depth: 1,
            read_inflight: 0,
            write_inflight: 0,
            state: state.clone(),
        };
        let worker_state = state.clone();
        let (done_tx, done_rx) = mpsc::channel();
        let worker = thread::spawn(move || {
            worker_state.thread.set(thread::current()).unwrap();
            shard.run();
            done_tx.send(()).unwrap();
        });
        let wait_until_parked = || {
            let mut parked = false;
            for _ in 0..1000 {
                if state.parked.load(Ordering::Acquire) {
                    parked = true;
                    break;
                }
                thread::sleep(Duration::from_millis(1));
            }
            assert!(parked, "uring shard did not park while idle");
        };
        wait_until_parked();

        let mut write_buf = Raw::new(4096);
        write_buf.fill(0x5a);
        let engine = UringIoEngine {
            read_txs: vec![read_tx],
            write_txs: vec![write_tx],
            states: vec![state.clone()],
        };
        let (_, result) = engine.write(Box::new(write_buf), partition.as_ref(), 0).await;
        result.unwrap();

        wait_until_parked();
        let read_buf = Raw::new(4096);
        let (read_buf, result) = engine.read(Box::new(read_buf), partition.as_ref(), 0).await;
        result.unwrap();
        assert!(read_buf.iter().all(|byte| *byte == 0x5a));

        wait_until_parked();
        drop(engine);
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("uring shard did not exit after engine drop");
        worker.join().unwrap();
    }

    fn shard(
        read_rx: mpsc::Receiver<UringIoCtx>,
        write_rx: mpsc::Receiver<UringIoCtx>,
        read_inflight: usize,
        write_inflight: usize,
    ) -> UringIoEngineShard {
        UringIoEngineShard {
            read_rx,
            write_rx,
            weight: 1.0,
            uring: IoUring::builder().build(1).unwrap(),
            io_depth: 1,
            read_inflight,
            write_inflight,
            state: Arc::new(UringIoEngineShardState::default()),
        }
    }

    #[test]
    fn try_recv_reports_disconnected_channels() {
        // Read first: the read channel is disconnected.
        let (read_tx, read_rx) = mpsc::sync_channel(1);
        let (_write_tx, write_rx) = mpsc::sync_channel(1);
        drop(read_tx);
        assert!(shard(read_rx, write_rx, 0, 1).try_recv().is_err());

        // Read first: the read channel is empty and the write channel is disconnected.
        let (_read_tx, read_rx) = mpsc::sync_channel(1);
        let (write_tx, write_rx) = mpsc::sync_channel(1);
        drop(write_tx);
        assert!(shard(read_rx, write_rx, 0, 1).try_recv().is_err());

        // Write first: the write channel is empty and the read channel is disconnected.
        let (read_tx, read_rx) = mpsc::sync_channel(1);
        let (_write_tx, write_rx) = mpsc::sync_channel(1);
        drop(read_tx);
        assert!(shard(read_rx, write_rx, 1, 0).try_recv().is_err());
    }
}
