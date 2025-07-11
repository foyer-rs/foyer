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

use crate::io::{
    bytes::{IoB, IoBuf, IoBufMut},
    device::{Device, RegionId},
    engine::{IoEngine, IoEngineBuilder, IoHandle},
    error::{IoError, IoResult},
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

struct UringIoRequest {
    tx: oneshot::Sender<IoResult<()>>,
    io_type: UringIoType,
    buf: RawBuf,
    region: RegionId,
    offset: u64,
}

struct UringIoCtx {
    tx: oneshot::Sender<IoResult<()>>,
    io_type: UringIoType,
    buf: RawBuf,
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
                let (ptr, len) = (request.buf.ptr, request.buf.len);
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
                    let _ = ctx.tx.send(Ok(()));
                }
                self.inflight -= 1;
            }
        }
    }
}

pub struct UringIoEngine {
    device: Arc<dyn Device>,
    txs: Vec<mpsc::SyncSender<UringIoRequest>>,
}

impl Debug for UringIoEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UringIoEngine").finish()
    }
}

impl UringIoEngine {
    fn read(&self, buf: Box<dyn IoBufMut>, region: RegionId, offset: u64) -> IoHandle {
        let (tx, rx) = oneshot::channel();
        let shard = &self.txs[region as usize % self.txs.len()];
        let (ptr, len) = buf.as_raw_parts();
        let _ = shard.send(UringIoRequest {
            tx,
            io_type: UringIoType::Read,
            buf: RawBuf { ptr, len },
            region,
            offset,
        });
        async move {
            let res = match rx.await {
                Ok(res) => res,
                Err(e) => Err(IoError::other(e)),
            };
            let buf: Box<dyn IoB> = buf;
            (buf, res)
        }
        .boxed()
        .into()
    }

    fn write(&self, buf: Box<dyn IoBuf>, region: RegionId, offset: u64) -> IoHandle {
        let (tx, rx) = oneshot::channel();
        let shard = &self.txs[region as usize % self.txs.len()];
        let (ptr, len) = buf.as_raw_parts();
        let _ = shard.send(UringIoRequest {
            tx,
            io_type: UringIoType::Write,
            buf: RawBuf { ptr, len },
            region,
            offset,
        });
        async move {
            let res = match rx.await {
                Ok(res) => res,
                Err(e) => Err(IoError::other(e)),
            };
            let buf: Box<dyn IoB> = buf;
            (buf, res)
        }
        .boxed()
        .into()
    }
}

#[derive(Debug)]
pub struct UringIoEngineBuilder {
    threads: usize,
    io_depth: usize,
}

impl UringIoEngineBuilder {
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
    fn build(self: Box<Self>, device: Arc<dyn Device>) -> IoResult<Arc<dyn IoEngine>> {
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
                device: device.clone(),
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

    fn read(&self, buf: Box<dyn IoBufMut>, region: RegionId, offset: u64) -> IoHandle {
        self.read(buf, region, offset)
    }

    fn write(&self, buf: Box<dyn IoBuf>, region: RegionId, offset: u64) -> IoHandle {
        self.write(buf, region, offset)
    }
}
