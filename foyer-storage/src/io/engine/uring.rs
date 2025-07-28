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
    fmt::Debug,
    sync::{mpsc, Arc},
    time::Duration,
};

use core_affinity::CoreId;
use futures_util::FutureExt;
use io_uring::{opcode, types::Fd, IoUring};
use tokio::sync::oneshot;

use crate::{
    io::{
        bytes::{IoB, IoBuf, IoBufMut},
        device::{Device, Partition},
        engine::{IoEngine, IoEngineBuilder, IoHandle},
        error::{IoError, IoResult},
    },
    RawFile, Runtime,
};

use fastant::Instant;

/// Builder for io_uring based I/O engine.
#[derive(Debug)]
pub struct UringIoEngineBuilder {
    threads: usize,
    cpus: Vec<u32>,
    io_depth: usize,
    sqpoll: bool,
    sqpoll_cpus: Vec<u32>,
    sqpoll_idle: u32,
    iopoll: bool,
    weight: f64,
    tail: Duration,
}

impl Default for UringIoEngineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl UringIoEngineBuilder {
    /// Create a new io_uring based I/O engine builder with default configurations.
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
            tail: Duration::from_secs(1),
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
    /// - If this feature is enabeld, the underlying device MUST be opened with the `O_DIRECT` flag.
    /// - If this feature is enabeld, the underlying device MUST support io polling.
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
    /// NOTE: If this feature is enabeld, the underlying device must be opened with the `O_DIRECT` flag.
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

    /// Set the tail tracing duration.
    ///
    /// Only available with `DEBUG` log level.
    pub fn with_tail_tracing(mut self, tail: Duration) -> Self {
        self.tail = tail;
        self
    }
}

impl IoEngineBuilder for UringIoEngineBuilder {
    fn build(self: Box<Self>, device: Arc<dyn Device>, _: Runtime) -> IoResult<Arc<dyn IoEngine>> {
        if self.threads == 0 {
            return Err(IoError::other(anyhow::anyhow!("shards must be greater than 0")));
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

        for (i, (read_rx, write_rx)) in read_rxs.into_iter().zip(write_rxs.into_iter()).enumerate() {
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
            let uring = builder.build(self.io_depth as _)?;
            let shard = UringIoEngineShard {
                read_rx,
                write_rx,
                _device: device.clone(),
                uring,
                io_depth: self.io_depth,
                weight: self.weight,
                read_inflight: 0,
                write_inflight: 0,
                tail: self.tail,
            };

            std::thread::Builder::new()
                .name(format!("foyer-uring-{i}"))
                .spawn(move || {
                    if let Some(cpu) = cpu {
                        core_affinity::set_for_current(CoreId { id: cpu as _ });
                    }
                    shard.run();
                })?;
        }

        let engine = UringIoEngine {
            device,
            read_txs,
            write_txs,
        };
        let engine = Arc::new(engine);
        Ok(engine)
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

// TODO(MrCroxx): Add a feature to opt-in io tracing.
///
#[derive(Debug)]
pub struct Trace {
    pub base: Instant,
    pub prepare: Instant,
    pub queue: Instant,
    pub submit: Instant,
    pub io: Instant,
    pub notify: Instant,
}

impl Trace {
    pub fn now() -> Self {
        Self {
            base: Instant::now(),
            prepare: Instant::now(),
            queue: Instant::now(),
            submit: Instant::now(),
            io: Instant::now(),
            notify: Instant::now(),
        }
    }

    pub fn prepare(&self) -> Duration {
        self.prepare.duration_since(self.base)
    }

    pub fn queue(&self) -> Duration {
        self.queue.duration_since(self.prepare)
    }

    pub fn submit(&self) -> Duration {
        self.submit.duration_since(self.queue)
    }

    pub fn io(&self) -> Duration {
        self.io.duration_since(self.submit)
    }

    pub fn notify(&self) -> Duration {
        self.notify.duration_since(self.io)
    }

    pub fn total(&self) -> Duration {
        self.notify.duration_since(self.base)
    }
}

struct UringIoCtx {
    tx: oneshot::Sender<IoResult<()>>,
    io_type: UringIoType,
    rbuf: RawBuf,
    addr: RawFileAddress,
    trace: Trace,
}

struct UringIoEngineShard {
    read_rx: mpsc::Receiver<UringIoCtx>,
    write_rx: mpsc::Receiver<UringIoCtx>,
    weight: f64,
    _device: Arc<dyn Device>,
    uring: IoUring,
    io_depth: usize,
    read_inflight: usize,
    write_inflight: usize,
    tail: Duration,
}

impl UringIoEngineShard {
    fn run(mut self) {
        loop {
            let mut pctxs = vec![];

            'prepare: loop {
                if self.read_inflight + self.write_inflight >= self.io_depth {
                    break 'prepare;
                }

                let ctx = if (self.read_inflight as f64) < self.write_inflight as f64 * self.weight {
                    match self.read_rx.try_recv() {
                        Err(mpsc::TryRecvError::Disconnected) => return,
                        Ok(ctx) => Some(ctx),
                        Err(mpsc::TryRecvError::Empty) => match self.write_rx.try_recv() {
                            Err(mpsc::TryRecvError::Disconnected) => return,
                            Ok(ctx) => Some(ctx),
                            Err(mpsc::TryRecvError::Empty) => None,
                        },
                    }
                } else {
                    match self.write_rx.try_recv() {
                        Err(mpsc::TryRecvError::Disconnected) => return,
                        Ok(ctx) => Some(ctx),
                        Err(mpsc::TryRecvError::Empty) => match self.read_rx.try_recv() {
                            Err(mpsc::TryRecvError::Disconnected) => return,
                            Ok(ctx) => Some(ctx),
                            Err(mpsc::TryRecvError::Empty) => None,
                        },
                    }
                };

                let mut ctx = match ctx {
                    Some(ctx) => ctx,
                    None => break 'prepare,
                };

                ctx.trace.queue = Instant::now();

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
                let pctx = Box::into_raw(ctx);
                pctxs.push(pctx);
                let data = pctx as u64;
                let sqe = sqe.user_data(data);
                unsafe { self.uring.submission().push(&sqe).unwrap() }
            }

            if self.read_inflight + self.write_inflight > 0 {
                self.uring.submit().unwrap();
            }

            for pctx in pctxs {
                let ctx = unsafe { &mut *pctx };
                ctx.trace.submit = Instant::now();
            }

            let rio = self.read_inflight;
            let wio = self.write_inflight;

            for cqe in self.uring.completion() {
                let data = cqe.user_data();
                let mut ctx = unsafe { Box::from_raw(data as *mut UringIoCtx) };

                ctx.trace.io = Instant::now();

                match ctx.io_type {
                    UringIoType::Read => self.read_inflight -= 1,
                    UringIoType::Write => self.write_inflight -= 1,
                }

                let res = cqe.result();
                if res < 0 {
                    let err = IoError::from_raw_os_error(res);
                    let _ = ctx.tx.send(Err(err));
                } else {
                    let _ = ctx.tx.send(Ok(()));
                }

                ctx.trace.notify = Instant::now();

                if ctx.io_type == UringIoType::Read && ctx.trace.total() >= self.tail {
                    tracing::debug!(
                        total = ?ctx.trace.total(),
                        prepare = ?ctx.trace.prepare(),
                        queue = ?ctx.trace.queue(),
                        submit = ?ctx.trace.submit(),
                        io = ?ctx.trace.io(),
                        notify = ?ctx.trace.notify(),
                        rio,
                        wio,
                        "[io_uring io engine]: tail tracing"
                    );
                }
            }
        }
    }
}

pub struct UringIoEngine {
    device: Arc<dyn Device>,
    read_txs: Vec<mpsc::SyncSender<UringIoCtx>>,
    write_txs: Vec<mpsc::SyncSender<UringIoCtx>>,
}

impl Debug for UringIoEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UringIoEngine").finish()
    }
}

impl UringIoEngine {
    fn read(&self, buf: Box<dyn IoBufMut>, partition: &dyn Partition, offset: u64) -> IoHandle {
        let mut trace = Trace::now();
        let (tx, rx) = oneshot::channel();
        let shard = &self.read_txs[partition.id() as usize % self.read_txs.len()];
        let (ptr, len) = buf.as_raw_parts();
        let rbuf = RawBuf { ptr, len };
        let (file, offset) = partition.translate(offset);
        let addr = RawFileAddress { file, offset };
        trace.prepare = Instant::now();
        let _ = shard.send(UringIoCtx {
            tx,
            io_type: UringIoType::Read,
            rbuf,
            addr,
            trace,
        });
        async move {
            let res = match rx.await {
                Ok(res) => res,
                Err(e) => Err(IoError::other(e)),
            };
            let buf: Box<dyn IoB> = buf.into_iob();
            (buf, res)
        }
        .boxed()
        .into()
    }

    fn write(&self, buf: Box<dyn IoBuf>, partition: &dyn Partition, offset: u64) -> IoHandle {
        let mut trace = Trace::now();

        let (tx, rx) = oneshot::channel();
        let shard = &self.write_txs[partition.id() as usize % self.write_txs.len()];
        let (ptr, len) = buf.as_raw_parts();
        let rbuf = RawBuf { ptr, len };
        let (file, offset) = partition.translate(offset);
        let addr = RawFileAddress { file, offset };
        trace.prepare = Instant::now();
        let _ = shard.send(UringIoCtx {
            tx,
            io_type: UringIoType::Write,
            rbuf,
            addr,
            trace,
        });
        async move {
            let res = match rx.await {
                Ok(res) => res,
                Err(e) => Err(IoError::other(e)),
            };
            let buf: Box<dyn IoB> = buf.into_iob();
            (buf, res)
        }
        .boxed()
        .into()
    }
}

impl IoEngine for UringIoEngine {
    fn device(&self) -> &Arc<dyn Device> {
        &self.device
    }

    fn read(&self, buf: Box<dyn IoBufMut>, partition: &dyn Partition, offset: u64) -> IoHandle {
        self.read(buf, partition, offset)
    }

    fn write(&self, buf: Box<dyn IoBuf>, partition: &dyn Partition, offset: u64) -> IoHandle {
        self.write(buf, partition, offset)
    }
}
