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
};

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
    RawFile,
};

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
    tx: oneshot::Sender<IoResult<()>>,
    io_type: UringIoType,
    rbuf: RawBuf,
    addr: RawFileAddress,
}

struct UringIoEngineShard {
    read_rx: mpsc::Receiver<UringIoCtx>,
    write_rx: mpsc::Receiver<UringIoCtx>,
    weight: f64,
    _device: Arc<dyn Device>,
    uring: IoUring,
    io_depth: usize,
    io_poll: bool,
    read_inflight: usize,
    write_inflight: usize,
}

impl UringIoEngineShard {
    fn run(mut self) {
        loop {
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
                if self.io_poll {
                    self.uring.submit().unwrap();
                } else {
                    self.uring.submit_and_wait(1).unwrap();
                }
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
                    let err = IoError::from_raw_os_error(res);
                    let _ = ctx.tx.send(Err(err));
                } else {
                    let _ = ctx.tx.send(Ok(()));
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
        let (tx, rx) = oneshot::channel();
        let shard = &self.read_txs[partition.id() as usize % self.read_txs.len()];
        let (ptr, len) = buf.as_raw_parts();
        let rbuf = RawBuf { ptr, len };
        let (file, offset) = partition.translate(offset);
        let addr = RawFileAddress { file, offset };
        let _ = shard.send(UringIoCtx {
            tx,
            io_type: UringIoType::Read,
            rbuf,
            addr,
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
        let (tx, rx) = oneshot::channel();
        let shard = &self.write_txs[partition.id() as usize % self.write_txs.len()];
        let (ptr, len) = buf.as_raw_parts();
        let rbuf = RawBuf { ptr, len };
        let (file, offset) = partition.translate(offset);
        let addr = RawFileAddress { file, offset };
        let _ = shard.send(UringIoCtx {
            tx,
            io_type: UringIoType::Write,
            rbuf,
            addr,
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

/// Builder for io_uring based I/O engine.
#[derive(Debug)]
pub struct UringIoEngineBuilder {
    threads: usize,
    io_depth: usize,
    io_poll: bool,
    weight: f64,
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
            io_depth: 64,
            io_poll: false,
            weight: 1.0,
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

    /// Enable or disable I/O polling.
    ///
    /// FYI: [io_uring_setup(2)](https://man7.org/linux/man-pages/man2/io_uring_setup.2.html)
    ///
    /// Related syscall flag: `IORING_SETUP_IOPOLL`.
    ///
    /// NOTE: If this feature is enabeld, the underlying device must be opened with the `O_DIRECT` flag.
    ///
    /// Default: `false`.
    pub fn with_iopoll(mut self, iopoll: bool) -> Self {
        self.io_poll = iopoll;
        self
    }

    /// Set the weight of read/write priorities.
    ///
    /// The engine will try to keep the read/write iodepth ratio as close to the specified weight as possible.
    pub fn with_weight(mut self, weight: f64) -> Self {
        self.weight = weight;
        self
    }
}

impl IoEngineBuilder for UringIoEngineBuilder {
    fn build(self: Box<Self>, device: Arc<dyn Device>) -> IoResult<Arc<dyn IoEngine>> {
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

        for (read_rx, write_rx) in read_rxs.into_iter().zip(write_rxs.into_iter()) {
            let mut builder = IoUring::builder();
            if self.io_poll {
                builder.setup_iopoll();
            }
            let uring = builder.build(self.io_depth as _)?;
            let shard = UringIoEngineShard {
                read_rx,
                write_rx,
                _device: device.clone(),
                uring,
                io_depth: self.io_depth,
                io_poll: self.io_poll,
                weight: self.weight,
                read_inflight: 0,
                write_inflight: 0,
            };

            std::thread::spawn(move || {
                shard.run();
            });
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
