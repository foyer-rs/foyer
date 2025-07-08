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

use std::sync::Arc;

use foyer_common::{
    bits,
    code::{StorageKey, StorageValue},
    properties::Properties,
};
use futures_util::future::join_all;
use itertools::Itertools;
use tokio::task::JoinHandle;

use crate::{
    device::MonitoredDevice,
    io::PAGE,
    large::{
        flusher::{Flusher, Submission},
        indexer::Indexer,
        reclaimer::{RegionCleaner, Reinsertion},
        scanner::RegionScanner,
    },
    region::RegionReclaimNotifier,
    IoBuffer, Region, ReinsertionPicker, Runtime,
};

#[derive(Debug)]
struct Inner<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    region: Region,
    blob_index_size: usize,
    reinsertion_picker: Arc<dyn ReinsertionPicker>,
    device: MonitoredDevice,
    flushers: Vec<Flusher<K, V, P>>,
    runtime: Runtime,
    indexer: Indexer,
    concurrency: usize,
}

#[derive(Debug)]
pub struct ReclaimerV2<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    inner: Arc<Inner<K, V, P>>,
}

impl<K, V, P> Clone for ReclaimerV2<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

pub struct ReclaimerV2Ctx<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    region: Region,
    blob_index_size: usize,
    reinsertion_picker: Arc<dyn ReinsertionPicker>,
    device: MonitoredDevice,
    flushers: Vec<Flusher<K, V, P>>,
    runtime: Runtime,
    indexer: Indexer,
    notifier: RegionReclaimNotifier<K, V, P>,
}

impl<K, V, P> ReclaimerV2<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    pub fn new(
        region: Region,
        blob_index_size: usize,
        reinsertion_picker: Arc<dyn ReinsertionPicker>,
        device: MonitoredDevice,
        flushers: Vec<Flusher<K, V, P>>,
        runtime: Runtime,
        indexer: Indexer,
        concurrency: usize,
    ) -> Self {
        let inner = Inner {
            region,
            blob_index_size,
            reinsertion_picker,
            device,
            flushers,
            runtime,
            indexer,
            concurrency,
        };
        let inner = Arc::new(inner);
        Self { inner }
    }

    pub fn concurrency(&self) -> usize {
        self.inner.concurrency
    }

    pub fn spawn_reclaim(&self, region: Region, notifier: RegionReclaimNotifier<K, V, P>) -> JoinHandle<()> {
        let this = self.clone();
        self.inner.runtime.write().spawn(async move {
            this.reclaim(region, notifier).await;
        })
    }

    pub async fn reclaim(&self, region: Region, notifier: RegionReclaimNotifier<K, V, P>) {
        let ctx = ReclaimerV2Ctx {
            region,
            blob_index_size: self.inner.blob_index_size,
            reinsertion_picker: self.inner.reinsertion_picker.clone(),
            device: self.inner.device.clone(),
            flushers: self.inner.flushers.clone(),
            runtime: self.inner.runtime.clone(),
            indexer: self.inner.indexer.clone(),
            notifier,
        };

        let region = ctx.region;
        let id = region.id();
        tracing::debug!("[reclaimer]: Start reclaiming region {id}.");

        let mut scanner = RegionScanner::new(region.clone(), ctx.blob_index_size);
        let mut picked_count = 0;
        let mut unpicked = vec![];

        // The loop will ends when:
        //
        // 1. no subsequent entries
        // 2. on error
        //
        // If the loop ends on error, the subsequent indices cannot be removed while reclaiming.
        // They will be removed when a query find a mismatch entry.
        'reinsert: loop {
            let infos = match scanner.next().await {
                Ok(None) => break 'reinsert,
                Err(e) => {
                    tracing::warn!(
                        "[reclaimer]: Error raised when reclaiming region {id}, skip the subsequent entries, err: {e}",
                        id = region.id()
                    );
                    break 'reinsert;
                }
                Ok(Some(infos)) => infos,
            };
            for info in infos {
                if ctx
                    .reinsertion_picker
                    .pick(ctx.device.statistics(), info.hash)
                    .admitted()
                {
                    let buf = IoBuffer::new(bits::align_up(PAGE, info.addr.len as _));
                    let (buf, res) = region.read(buf, info.addr.offset as _).await;
                    if let Err(e) = res {
                        tracing::warn!(
                        "[reclaimer]: error raised when reclaiming region {id}, skip the subsequent entries, err: {e}",
                        id = region.id()
                    );
                        break 'reinsert;
                    }

                    let slice = buf.into_owned_slice().slice(..info.addr.len as usize);
                    let flusher = ctx.flushers[info.hash as usize % ctx.flushers.len()].clone();
                    flusher.submit(Submission::Reinsertion {
                        reinsertion: Reinsertion {
                            hash: info.hash,
                            sequence: info.addr.sequence,
                            slice,
                        },
                    });
                    picked_count += 1;
                } else {
                    unpicked.push((info.hash, info.addr.sequence));
                }
            }
        }

        let unpicked_count = unpicked.len();

        let waits = ctx.flushers.iter().map(|flusher| flusher.wait()).collect_vec();
        ctx.runtime.write().spawn(async move {
            join_all(waits).await;
        });
        ctx.indexer.remove_batch(unpicked);

        if let Err(e) = RegionCleaner::clean(&region).await {
            tracing::warn!("reclaimer]: mark region {id} clean error: {e}", id = region.id());
        }

        tracing::debug!(
            "[reclaimer]: Finish reclaiming region {id}, picked: {picked_count}, unpicked: {unpicked_count}."
        );

        region.statistics().reset();

        ctx.notifier.notify();
    }
}
