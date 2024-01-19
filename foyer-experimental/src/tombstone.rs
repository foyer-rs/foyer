//  Copyright 2023 MrCroxx
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::JoinHandle,
    time::Instant,
};

use bytes::{Buf, BufMut};
use crossbeam::channel;
use parking_lot::{Condvar, Mutex};
use tokio::sync::oneshot;

use crate::{
    asyncify,
    buf::{BufExt, BufMutExt},
    error::Result,
};

pub trait HashValue: Send + Sync + 'static + Eq + std::fmt::Debug {
    fn size() -> usize;
    fn write(&self, buf: impl BufMut);
    fn read(buf: impl Buf) -> Self;
}

macro_rules! for_all_primitives {
    ($macro:ident) => {
        $macro! {
            u8, u16, u32, u64, usize,
            i8, i16, i32, i64, isize,
        }
    };
}

macro_rules! impl_hash_value {
    ($( $type:ty, )*) => {
        paste::paste! {
            $(
                impl HashValue for $type {
                    fn size() -> usize {
                        std::mem::size_of::<$type>()
                    }

                    fn write(&self, mut buf: impl BufMut) {
                        buf.[<put_ $type>](*self);
                    }

                    fn read(mut buf: impl Buf) -> Self {
                        buf.[<get_ $type>]()
                    }
                }
            )*
        }
    };
}

for_all_primitives! { impl_hash_value }

#[derive(Debug)]
pub struct Tombstone<H: HashValue> {
    hash: H,
    pos: u32,
}

impl<H: HashValue> Tombstone<H> {
    pub fn size() -> usize {
        H::size() + 8
    }

    pub fn new(hash: H, pos: u32) -> Self {
        Self { hash, pos }
    }

    pub fn write(&self, mut buf: impl BufMut) {
        self.hash.write(&mut buf);
        buf.put_u32(self.pos);
    }

    pub fn read(mut buf: impl Buf) -> Self {
        let hash = H::read(&mut buf);
        let pos = buf.get_u32();
        Self { hash, pos }
    }
}

#[derive(Debug)]
pub struct TombstoneLogConfig {
    pub id: usize,
    pub dir: PathBuf,
}

#[derive(Debug)]
struct InflightTombstone<H: HashValue> {
    tombstone: Tombstone<H>,
    tx: oneshot::Sender<Result<()>>,
}

#[derive(Debug)]
struct TombstoneLogInner<H: HashValue> {
    inflights: Mutex<Vec<InflightTombstone<H>>>,
    condvar: Condvar,
}

#[derive(Debug)]
pub struct TombstoneLog<H: HashValue> {
    inner: Arc<TombstoneLogInner<H>>,
    stopped: Arc<AtomicBool>,
    handles: Arc<Mutex<Vec<JoinHandle<Result<()>>>>>,
}

impl<H: HashValue> Clone for TombstoneLog<H> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            stopped: self.stopped.clone(),
            handles: self.handles.clone(),
        }
    }
}

impl<H: HashValue> TombstoneLog<H> {
    pub async fn open(config: TombstoneLogConfig) -> Result<Self> {
        let mut path = config.dir;

        path.push(format!("tombstone-{:08X}", config.id));

        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(path)?;

        let inner = Arc::new(TombstoneLogInner {
            inflights: Mutex::new(vec![]),
            condvar: Condvar::new(),
        });

        let (task_tx, task_rx) = channel::unbounded();
        let stopped = Arc::new(AtomicBool::new(false));

        let flusher = TombstoneLogFlusher {
            file,
            inner: inner.clone(),
            task_tx,
            stopped: stopped.clone(),
        };

        let notifier = TombstoneLogFlushNotifier { task_rx };

        let handles = vec![
            std::thread::spawn(move || flusher.run()),
            std::thread::spawn(move || notifier.run()),
        ];
        let handles = Arc::new(Mutex::new(handles));

        Ok(Self {
            inner,
            stopped,
            handles,
        })
    }

    pub async fn close(&self) -> Result<()> {
        let handles = std::mem::take(&mut *self.handles.lock());
        if !handles.is_empty() {
            self.stopped.store(true, Ordering::Release);
            self.inner.condvar.notify_one();
            for handle in handles {
                asyncify(move || handle.join().unwrap()).await?;
            }
        }
        Ok(())
    }

    pub async fn append(&self, tombstone: Tombstone<H>) -> Result<()> {
        let rx = {
            let mut inflights = self.inner.inflights.lock();
            let (tx, rx) = oneshot::channel();
            inflights.push(InflightTombstone { tombstone, tx });
            rx
        };
        self.inner.condvar.notify_one();
        rx.await.unwrap()
    }
}

#[derive(Debug)]
struct TombstoneLogFlusher<H: HashValue> {
    file: File,
    inner: Arc<TombstoneLogInner<H>>,
    task_tx: channel::Sender<FlushNotifyTask>,
    stopped: Arc<AtomicBool>,
}

impl<H: HashValue> TombstoneLogFlusher<H> {
    fn run(mut self) -> Result<()> {
        loop {
            let inflights = {
                let mut inflights = self.inner.inflights.lock();
                if self.stopped.load(Ordering::Relaxed) {
                    return Ok(());
                }
                if inflights.is_empty() {
                    self.inner.condvar.wait_while(&mut inflights, |inflights| {
                        inflights.is_empty() && !self.stopped.load(Ordering::Relaxed)
                    });
                }
                if self.stopped.load(Ordering::Relaxed) {
                    return Ok(());
                }
                std::mem::take(&mut *inflights)
            };

            let now = Instant::now();

            let mut buffer = Vec::with_capacity(Tombstone::<H>::size() * inflights.len());
            let mut txs = Vec::with_capacity(inflights.len());
            let ts = inflights.len();

            let write_buffer_start = Instant::now();
            for inflight in inflights {
                inflight.tombstone.write(&mut buffer);
                txs.push(inflight.tx);
            }
            let write_buffer_duration = write_buffer_start.elapsed();

            let write_all_start = Instant::now();
            match self.file.write_all(&buffer) {
                Ok(()) => {}
                Err(e) => {
                    self.task_tx
                        .send(FlushNotifyTask {
                            txs,
                            io_result: Err(e),
                        })
                        .unwrap();
                    continue;
                }
            }
            let write_all_duration = write_all_start.elapsed();

            let fdatasync_start = Instant::now();
            match self.file.sync_data() {
                Ok(()) => {}
                Err(e) => {
                    self.task_tx
                        .send(FlushNotifyTask {
                            txs,
                            io_result: Err(e),
                        })
                        .unwrap();
                    continue;
                }
            }
            let fdatasync_duration = fdatasync_start.elapsed();

            let dispatch_start = Instant::now();
            self.task_tx
                .send(FlushNotifyTask {
                    txs,
                    io_result: Ok(()),
                })
                .unwrap();
            let dispatch_duration = dispatch_start.elapsed();

            let duration = now.elapsed();

            if duration.as_micros() >= 500 {
                println!(
                    "slow: {:?}, write buffer: {:?}, write all: {:?}, fdatasync: {:?}, dispatch: {:?}, ts: {:?}, buffer size: {:.3}KB",
                    duration,
                    write_buffer_duration,
                    write_all_duration,
                    fdatasync_duration,
                    dispatch_duration,
                    ts,
                    buffer.len() as f64 / 4096.0,
                );
            }
        }
    }
}

#[derive(Debug)]
struct FlushNotifyTask {
    txs: Vec<oneshot::Sender<Result<()>>>,
    io_result: std::io::Result<()>,
}

#[derive(Debug)]
struct TombstoneLogFlushNotifier {
    task_rx: channel::Receiver<FlushNotifyTask>,
}

impl TombstoneLogFlushNotifier {
    fn run(self) -> Result<()> {
        while let Ok(task) = self.task_rx.recv() {
            for tx in task.txs {
                let res = match &task.io_result {
                    Ok(()) => Ok(()),
                    Err(e) => Err(std::io::Error::from(e.kind()).into()),
                };
                tx.send(res).unwrap();
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {}
