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
    device::Device,
    error::{Error, Result},
    event::EventListener,
    indices::Indices,
    judge::Judges,
    metrics::Metrics,
    region::RegionId,
    region_manager::{RegionEpItemAdapter, RegionManager},
    reinsertion::ReinsertionPolicy,
    store::{RegionEntryIter, Store},
};
use bytes::BufMut;
use foyer_common::{
    code::{Key, Value},
    queue::AsyncQueue,
    rate::RateLimiter,
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

    #[allow(clippy::too_many_arguments)]
    pub async fn run<K, V, D, EP, EL>(
        &self,
        store: Arc<Store<K, V, D, EP, EL>>,
        region_manager: Arc<RegionManager<D, EP, EL>>,
        clean_regions: Arc<AsyncQueue<RegionId>>,
        reinsertions: Vec<Arc<dyn ReinsertionPolicy<Key = K, Value = V>>>,
        indices: Arc<Indices<K>>,
        rate_limiter: Option<Arc<RateLimiter>>,
        event_listeners: Vec<Arc<dyn EventListener<K = K, V = V>>>,
        stop_rxs: Vec<broadcast::Receiver<()>>,
        metrics: Arc<Metrics>,
    ) -> Vec<JoinHandle<()>>
    where
        K: Key,
        V: Value,
        D: Device,
        EP: EvictionPolicy<Adapter = RegionEpItemAdapter<EL>>,
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
                store: store.clone(),
                region_manager: region_manager.clone(),
                clean_regions: clean_regions.clone(),
                reinsertions: reinsertions.clone(),
                indices: indices.clone(),
                rate_limiter: rate_limiter.clone(),
                stop_rx,
                metrics: metrics.clone(),
                event_listeners: event_listeners.clone(),
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

struct Runner<K, V, D, EP, EL>
where
    K: Key,
    V: Value,
    D: Device,
    EP: EvictionPolicy<Adapter = RegionEpItemAdapter<EL>>,
    EL: Link,
{
    task_rx: mpsc::Receiver<ReclaimTask>,

    store: Arc<Store<K, V, D, EP, EL>>,
    region_manager: Arc<RegionManager<D, EP, EL>>,
    clean_regions: Arc<AsyncQueue<RegionId>>,
    reinsertions: Vec<Arc<dyn ReinsertionPolicy<Key = K, Value = V>>>,
    indices: Arc<Indices<K>>,

    rate_limiter: Option<Arc<RateLimiter>>,

    event_listeners: Vec<Arc<dyn EventListener<K = K, V = V>>>,

    stop_rx: broadcast::Receiver<()>,

    metrics: Arc<Metrics>,
}

impl<K, V, D, EP, EL> Runner<K, V, D, EP, EL>
where
    K: Key,
    V: Value,
    D: Device,
    EP: EvictionPolicy<Adapter = RegionEpItemAdapter<EL>>,
    EL: Link,
{
    async fn run(mut self) -> Result<()> {
        loop {
            tokio::select! {
                biased;
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

        // step 1: drop indices
        let indices = self.indices.take_region(&task.region_id);
        for index in indices.iter() {
            for listener in self.event_listeners.iter() {
                listener.on_evict(&index.key).await?;
            }
        }

        // after drop indices and acquire exclusive lock, no writers or readers are supposed to access the region
        {
            let guard = region.exclusive(false, false, false).await;
            tracing::trace!(
                "[reclaimer] region {}, writers: {}, buffered readers: {}, physical readers: {}",
                region.id(),
                guard.writers(),
                guard.buffered_readers(),
                guard.physical_readers()
            );
            drop(guard);
        }

        // step 2: do reinsertion
        let reinsert = || {
            let region = region.clone();
            let metrics = self.metrics.clone();
            let rate = self.rate_limiter.clone();
            let reinsertions = self.reinsertions.clone();

            tracing::info!("[reclaimer] begin reinsertion, region: {}", task.region_id);

            async move {
                let mut iter = match RegionEntryIter::<K, V, D>::open(region).await {
                    Ok(Some(iter)) => iter,
                    Ok(None) => return Ok(()),
                    Err(e) => return Err(e),
                };

                while let Some((key, value)) = iter.next_kv().await? {
                    let weight = key.serialized_len() + value.serialized_len();

                    let mut judges = Judges::new(reinsertions.len());
                    for (index, reinsertion) in reinsertions.iter().enumerate() {
                        let judge = reinsertion.judge(&key, weight, &metrics);
                        judges.set(index, judge);
                    }
                    if !judges.judge() {
                        for (index, reinsertion) in reinsertions.iter().enumerate() {
                            let judge = judges.get(index);
                            reinsertion.on_drop(&key, weight, &metrics, judge);
                        }
                        continue;
                    }

                    if let Some(rate) = rate.as_ref() && let Some(wait) = rate.consume(weight as f64) {
                        tokio::time::sleep(wait).await;
                    }

                    if self.store.insert(key.clone(), value).await? {
                        for (index, reinsertion) in reinsertions.iter().enumerate() {
                            let judge = judges.get(index);
                            reinsertion.on_insert(&key, weight, &metrics, judge);
                        }
                    } else {
                        for (index, reinsertion) in reinsertions.iter().enumerate() {
                            let judge = judges.get(index);
                            reinsertion.on_drop(&key, weight, &metrics, judge);
                        }
                    }

                    metrics.bytes_reinsert.inc_by(weight as u64);
                }

                tracing::info!("[reclaimer] finish reinsertion, region: {}", task.region_id);

                Ok(())
            }
        };

        if  !self.reinsertions.is_empty() && let Err(e) = reinsert().await {
            tracing::warn!("reinsert region {:?} error: {:?}", region, e);
        }

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

        tracing::info!(
            "[reclaimer] finish reclaim task, region: {}",
            task.region_id
        );

        self.metrics
            .bytes_reclaim
            .inc_by(region.device().region_size() as u64);
        self.metrics.size.sub(region.device().region_size() as i64);

        Ok(())
    }
}
