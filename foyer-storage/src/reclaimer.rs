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

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use crate::{
    admission::AdmissionPolicy,
    device::{BufferAllocator, Device},
    error::Result,
    indices::Indices,
    region::RegionId,
    region_manager::{RegionEpItemAdapter, RegionManager},
    reinsertion::ReinsertionPolicy,
    store::Store,
};
use foyer_common::{Key, Value};
use foyer_intrusive::{core::adapter::Link, eviction::EvictionPolicy};
use foyer_utils::queue::AsyncQueue;
use itertools::Itertools;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

#[derive(Debug)]
pub struct ReclaimTask {
    pub region_id: RegionId,
}

pub struct Reclaimer {
    sequence: AtomicUsize,

    task_txs: Vec<UnboundedSender<ReclaimTask>>,
}

impl Reclaimer {
    pub fn new(runners: usize) -> Self {
        Self {
            sequence: AtomicUsize::new(0),
            task_txs: Vec::with_capacity(runners),
        }
    }

    pub fn run<K, V, A, D, EP, AP, RP, EL>(
        &mut self,
        store: Arc<Store<K, V, A, D, EP, AP, RP, EL>>,
        region_manager: Arc<RegionManager<A, D, EP, EL>>,
        clean_regions: Arc<AsyncQueue<RegionId>>,
        reinsertion: RP,
        indices: Arc<Indices<K>>,
    ) where
        K: Key,
        V: Value,
        A: BufferAllocator,
        D: Device<IoBufferAllocator = A>,
        EP: EvictionPolicy<RegionEpItemAdapter<EL>, Link = EL>,
        AP: AdmissionPolicy<Key = K, Value = V>,
        RP: ReinsertionPolicy<Key = K, Value = V>,
        EL: Link,
    {
        let runners = self.task_txs.capacity();

        #[allow(clippy::type_complexity)]
        let (mut txs, rxs): (
            Vec<UnboundedSender<ReclaimTask>>,
            Vec<UnboundedReceiver<ReclaimTask>>,
        ) = (0..runners).map(|_| unbounded_channel()).unzip();
        self.task_txs.append(&mut txs);

        let runners = rxs
            .into_iter()
            .map(|rx| Runner {
                task_rx: rx,
                _store: store.clone(),
                region_manager: region_manager.clone(),
                clean_regions: clean_regions.clone(),
                _reinsertion: reinsertion.clone(),
                indices: indices.clone(),
            })
            .collect_vec();

        for runner in runners {
            tokio::spawn(async move {
                runner.run().await.unwrap();
            });
        }
    }

    pub fn submit(&self, task: ReclaimTask) {
        let submittee = self.sequence.fetch_add(1, Ordering::Relaxed) % self.task_txs.len();
        self.task_txs[submittee].send(task).unwrap();
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
    task_rx: UnboundedReceiver<ReclaimTask>,

    _store: Arc<Store<K, V, A, D, EP, AP, RP, EL>>,
    region_manager: Arc<RegionManager<A, D, EP, EL>>,
    clean_regions: Arc<AsyncQueue<RegionId>>,
    _reinsertion: RP,
    indices: Arc<Indices<K>>,
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
            if let Some(task) = self.task_rx.recv().await {
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

                // step 3: send clean region
                self.clean_regions.release(task.region_id);

                drop(guard);

                tracing::info!(
                    "[reclaimer] finish reclaim task, region: {}",
                    task.region_id
                );
            } else {
                return Ok(());
            }
        }
    }
}
