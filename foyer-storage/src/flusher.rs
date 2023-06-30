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

use std::sync::Arc;

use foyer_common::queue::AsyncQueue;
use foyer_intrusive::{core::adapter::Link, eviction::EvictionPolicy};
use itertools::Itertools;
use tokio::sync::{
    mpsc::{channel, error::TrySendError, Receiver, Sender},
    Mutex,
};

use crate::{
    device::{BufferAllocator, Device},
    error::{Error, Result},
    region::RegionId,
    region_manager::{RegionEpItemAdapter, RegionManager},
    slice::Slice,
};

#[derive(Debug)]
pub struct FlushTask {
    pub region_id: RegionId,
}

struct FlusherInner {
    sequence: usize,

    task_txs: Vec<Sender<FlushTask>>,
}

pub struct Flusher {
    runners: usize,

    inner: Mutex<FlusherInner>,
}

impl Flusher {
    pub fn new(runners: usize) -> Self {
        let inner = FlusherInner {
            sequence: 0,
            task_txs: Vec::with_capacity(runners),
        };
        Self {
            runners,
            inner: Mutex::new(inner),
        }
    }

    pub async fn run<A, D, E, EL>(
        &self,
        buffers: Arc<AsyncQueue<Vec<u8, A>>>,
        region_manager: Arc<RegionManager<A, D, E, EL>>,
    ) where
        A: BufferAllocator,
        D: Device<IoBufferAllocator = A>,
        E: EvictionPolicy<RegionEpItemAdapter<EL>, Link = EL>,
        EL: Link,
    {
        let mut inner = self.inner.lock().await;

        #[allow(clippy::type_complexity)]
        let (mut txs, rxs): (Vec<Sender<FlushTask>>, Vec<Receiver<FlushTask>>) =
            (0..self.runners).map(|_| channel(1)).unzip();
        inner.task_txs.append(&mut txs);

        let runners = rxs
            .into_iter()
            .map(|rx| Runner {
                task_rx: rx,
                buffers: buffers.clone(),
                region_manager: region_manager.clone(),
            })
            .collect_vec();

        for runner in runners {
            tokio::spawn(async move {
                runner.run().await.unwrap();
            });
        }
    }

    pub fn runners(&self) -> usize {
        self.runners
    }

    pub async fn submit(&self, task: FlushTask) -> Result<()> {
        let mut inner = self.inner.lock().await;
        let submittee = inner.sequence % inner.task_txs.len();
        inner.sequence += 1;
        inner.task_txs[submittee]
            .send(task)
            .await
            .map_err(Error::other)
    }

    pub async fn try_submit(&self, task: FlushTask) -> Result<()> {
        let mut inner = self.inner.lock().await;
        let submittee = inner.sequence % inner.task_txs.len();
        match inner.task_txs[submittee].try_send(task) {
            Ok(()) => {
                inner.sequence += 1;
                Ok(())
            }
            Err(TrySendError::Full(_)) => Err(Error::ChannelFull),
            Err(e) => Err(Error::Other(e.to_string())),
        }
    }
}

struct Runner<A, D, E, EL>
where
    A: BufferAllocator,
    D: Device<IoBufferAllocator = A>,
    E: EvictionPolicy<RegionEpItemAdapter<EL>, Link = EL>,
    EL: Link,
{
    task_rx: Receiver<FlushTask>,
    buffers: Arc<AsyncQueue<Vec<u8, A>>>,

    region_manager: Arc<RegionManager<A, D, E, EL>>,
}

impl<A, D, E, EL> Runner<A, D, E, EL>
where
    A: BufferAllocator,
    D: Device<IoBufferAllocator = A>,
    E: EvictionPolicy<RegionEpItemAdapter<EL>, Link = EL>,
    EL: Link,
{
    async fn run(mut self) -> Result<()> {
        loop {
            if let Some(task) = self.task_rx.recv().await {
                // TODO(MrCroxx): seal buffer

                tracing::info!("[flusher] receive flush task, region: {}", task.region_id);

                let region = self.region_manager.region(&task.region_id);

                {
                    // step 1: write buffer back to device
                    let slice = region.load(.., 0).await?.unwrap();

                    // wait all physical readers (from previous version) and writers done
                    let guard = region.exclusive(false, true, false).await;

                    tracing::info!("[flusher] write region {} back to device", task.region_id);

                    let mut offset = 0;
                    let len = region.device().io_size();
                    while offset < region.device().region_size() {
                        let start = offset;
                        let end = std::cmp::min(offset + len, region.device().region_size());

                        tracing::trace!("write region {} [{}..{}]", region.id(), start, end);

                        let s = unsafe { Slice::new(&slice.as_ref()[start..end]) };
                        region
                            .device()
                            .write(s, region.id(), offset as u64, len)
                            .await?;
                        offset += len;
                    }

                    drop(guard);

                    tracing::debug!("[flusher] drop exclusive guard");
                }

                let buffer = {
                    // step 2: detach buffer
                    let mut guard = region.exclusive(false, false, true).await;

                    let buffer = guard.detach_buffer();

                    tracing::trace!(
                            "[flusher] region {}, writers: {}, buffered readers: {}, physical readers: {}",
                            region.id(),
                            guard.writers(),
                            guard.buffered_readers(),
                            guard.physical_readers()
                        );

                    drop(guard);
                    buffer
                };

                // step 3: release buffer
                self.buffers.release(buffer);

                self.region_manager.post_flush(&region.id()).await;

                tracing::info!("[flusher] finish flush task, region: {}", task.region_id);
            } else {
                return Ok(());
            }
        }
    }
}
