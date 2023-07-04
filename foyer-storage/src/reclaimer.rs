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

use crate::{
    admission::AdmissionPolicy,
    device::{BufferAllocator, Device},
    error::{Error, Result},
    indices::Indices,
    region::RegionId,
    region_manager::{RegionEpItemAdapter, RegionManager},
    reinsertion::ReinsertionPolicy,
    store::Store,
};
use bytes::BufMut;
use foyer_common::{
    code::{Key, Value},
    queue::AsyncQueue,
};
use foyer_intrusive::{core::adapter::Link, eviction::EvictionPolicy};
use itertools::Itertools;
use tokio::{
    sync::{broadcast, mpsc, Mutex},
    task::JoinHandle,
};

#[derive(Debug)]
pub struct ReclaimTask {
    pub region_id: RegionId,
}

struct ReclaimerInner {
    sequence: usize,

    task_txs: Vec<mpsc::Sender<ReclaimTask>>,
}

pub struct Reclaimer {
    runners: usize,

    inner: Mutex<ReclaimerInner>,
}

impl Reclaimer {
    pub fn new(runners: usize) -> Self {
        let inner = ReclaimerInner {
            sequence: 0,
            task_txs: Vec::with_capacity(runners),
        };

        Self {
            runners,
            inner: Mutex::new(inner),
        }
    }

    pub async fn run<K, V, A, D, EP, AP, RP, EL>(
        &self,
        store: Arc<Store<K, V, A, D, EP, AP, RP, EL>>,
        region_manager: Arc<RegionManager<A, D, EP, EL>>,
        clean_regions: Arc<AsyncQueue<RegionId>>,
        reinsertion: RP,
        indices: Arc<Indices<K>>,
        stop_rxs: Vec<broadcast::Receiver<()>>,
    ) -> Vec<JoinHandle<()>>
    where
        K: Key,
        V: Value,
        A: BufferAllocator,
        D: Device<IoBufferAllocator = A>,
        EP: EvictionPolicy<RegionEpItemAdapter<EL>, Link = EL>,
        AP: AdmissionPolicy<Key = K, Value = V>,
        RP: ReinsertionPolicy<Key = K, Value = V>,
        EL: Link,
    {
        let mut inner = self.inner.lock().await;

        #[allow(clippy::type_complexity)]
        let (mut txs, rxs): (
            Vec<mpsc::Sender<ReclaimTask>>,
            Vec<mpsc::Receiver<ReclaimTask>>,
        ) = (0..self.runners).map(|_| mpsc::channel(1)).unzip();
        inner.task_txs.append(&mut txs);

        let runners = rxs
            .into_iter()
            .zip_eq(stop_rxs.into_iter())
            .map(|(task_rx, stop_rx)| Runner {
                task_rx,
                _store: store.clone(),
                region_manager: region_manager.clone(),
                clean_regions: clean_regions.clone(),
                _reinsertion: reinsertion.clone(),
                indices: indices.clone(),
                stop_rx,
            })
            .collect_vec();

        let mut handles = vec![];
        for runner in runners {
            let handle = tokio::spawn(async move {
                runner.run().await.unwrap();
            });
            handles.push(handle);
        }
        handles
    }

    pub fn runners(&self) -> usize {
        self.runners
    }

    pub async fn submit(&self, task: ReclaimTask) -> Result<()> {
        let mut inner = self.inner.lock().await;
        let submittee = inner.sequence % inner.task_txs.len();
        inner.sequence += 1;
        inner.task_txs[submittee]
            .send(task)
            .await
            .map_err(Error::other)
    }
}

struct Runner<K, V, A, D, EP, AP, RP, EL>
where
    K: Key,
    V: Value,
    A: BufferAllocator,
    D: Device<IoBufferAllocator = A>,
    EP: EvictionPolicy<RegionEpItemAdapter<EL>, Link = EL>,
    AP: AdmissionPolicy<Key = K, Value = V>,
    RP: ReinsertionPolicy<Key = K, Value = V>,
    EL: Link,
{
    task_rx: mpsc::Receiver<ReclaimTask>,

    _store: Arc<Store<K, V, A, D, EP, AP, RP, EL>>,
    region_manager: Arc<RegionManager<A, D, EP, EL>>,
    clean_regions: Arc<AsyncQueue<RegionId>>,
    _reinsertion: RP,
    indices: Arc<Indices<K>>,

    stop_rx: broadcast::Receiver<()>,
}

impl<K, V, A, D, EP, AP, RP, EL> Runner<K, V, A, D, EP, AP, RP, EL>
where
    K: Key,
    V: Value,
    A: BufferAllocator,
    D: Device<IoBufferAllocator = A>,
    EP: EvictionPolicy<RegionEpItemAdapter<EL>, Link = EL>,
    AP: AdmissionPolicy<Key = K, Value = V>,
    RP: ReinsertionPolicy<Key = K, Value = V>,
    EL: Link,
{
    async fn run(mut self) -> Result<()> {
        loop {
            tokio::select! {
                Some(task) = self.task_rx.recv() => {
                    self.handle(task).await?;
                }
                _ = self.stop_rx.recv() => {
                    tracing::info!("[reclaimer] exit");
                    return Ok(())
                }
            }
        }
    }

    async fn handle(&self, task: ReclaimTask) -> Result<()> {
        tracing::info!(
            "[reclaimer] receive reclaim task, region: {}",
            task.region_id
        );

        let region = self.region_manager.region(&task.region_id);

        // keep region totally exclusive while reclamation
        let guard = region.exclusive(false, false, false).await;

        tracing::trace!(
            "[reclaimer] region {}, writers: {}, buffered readers: {}, physical readers: {}",
            region.id(),
            guard.writers(),
            guard.buffered_readers(),
            guard.physical_readers()
        );

        // step 1: drop indices
        let _indices = self.indices.take_region(&task.region_id);

        // step 2: do reinsertion
        // TODO(MrCroxx): do reinsertion

        // step 3: set region last block zero
        let align = region.device().align();
        let region_size = region.device().region_size();
        let mut buf = region.device().io_buffer(align, align);
        (&mut buf[..]).put_slice(&vec![0; align]);
        region
            .device()
            .write(buf, task.region_id, (region_size - align) as u64, align)
            .await?;

        // step 4: send clean region
        self.clean_regions.release(task.region_id);

        drop(guard);

        tracing::info!(
            "[reclaimer] finish reclaim task, region: {}",
            task.region_id
        );

        Ok(())
    }
}
