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
    RawFile, Runtime,
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

#[cfg(target_family = "windows")]
unsafe impl Send for RawFileAddress {}
#[cfg(target_family = "windows")]
unsafe impl Sync for RawFileAddress {}

struct UringIoCtx {
    tx: oneshot::Sender<IoResult<()>>,
    io_type: UringIoType,
    rbuf: RawBuf,
    addr: RawFileAddress,
}

struct UringIoEngineShard {
    rx: mpsc::Receiver<UringIoCtx>,
    _device: Arc<dyn Device>,
    uring: IoUring,
    io_depth: usize,
    inflight: usize,
}

impl UringIoEngineShard {
    fn run(mut self) {
        loop {
            'recv: while self.inflight < self.io_depth {
                let ctx = match self.rx.try_recv() {
                    Ok(ctx) => ctx,
                    Err(mpsc::TryRecvError::Empty) => break 'recv,
                    Err(mpsc::TryRecvError::Disconnected) => return,
                };

                self.inflight += 1;
                let ctx = Box::new(ctx);

                let fd = Fd(ctx.addr.file);
                let sqe = match ctx.io_type {
                    UringIoType::Read => opcode::Read::new(fd, ctx.rbuf.ptr, ctx.rbuf.len as _)
                        .offset(ctx.addr.offset)
                        .build(),
                    UringIoType::Write => opcode::Write::new(fd, ctx.rbuf.ptr, ctx.rbuf.len as _)
                        .offset(ctx.addr.offset)
                        .build(),
                };
                let data = Box::into_raw(ctx) as u64;
                let sqe = sqe.user_data(data);
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
                    let _ = ctx.tx.send(Ok(()));
                }
                self.inflight -= 1;
            }
        }
    }
}

pub struct UringIoEngine {
    device: Arc<dyn Device>,
    txs: Vec<mpsc::SyncSender<UringIoCtx>>,
}

impl Debug for UringIoEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UringIoEngine").finish()
    }
}

impl UringIoEngine {
    fn read(&self, buf: Box<dyn IoBufMut>, partition: &dyn Partition, offset: u64) -> IoHandle {
        let (tx, rx) = oneshot::channel();
        let shard = &self.txs[partition.id() as usize % self.txs.len()];
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
        let shard = &self.txs[partition.id() as usize % self.txs.len()];
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
    fn build(self: Box<Self>, device: Arc<dyn Device>, _: Runtime) -> IoResult<Arc<dyn IoEngine>> {
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
                _device: device.clone(),
                uring,
                io_depth: self.io_depth,
                inflight: 0,
            };

            std::thread::spawn(move || {
                shard.run();
            });
        }

        let engine = UringIoEngine { device, txs };
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
