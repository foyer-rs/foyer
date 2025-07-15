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

use std::{fmt::Debug, sync::Arc};

use foyer_common::{
    bits,
    code::{StorageKey, StorageValue},
    properties::Properties,
};
use futures_core::future::BoxFuture;
use futures_util::{future::join_all, FutureExt};
use itertools::Itertools;

use crate::{
    engine::large::{
        flusher::{Flusher, Submission},
        indexer::Indexer,
        region::{ReclaimingRegion, Region},
        scanner::RegionScanner,
        serde::Sequence,
    },
    error::Result,
    io::{
        bytes::{IoSlice, IoSliceMut},
        PAGE,
    },
    picker::ReinsertionPicker,
    runtime::Runtime,
    Statistics,
};

pub trait ReclaimerTrait: Send + Sync + 'static + Debug {
    fn reclaim(&self, region: ReclaimingRegion) -> BoxFuture<'static, ()>;
}

pub struct Reclaimer<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    indexer: Indexer,
    flushers: Vec<Flusher<K, V, P>>,
    reinsertion_picker: Arc<dyn ReinsertionPicker>,
    blob_index_size: usize,
    statistics: Arc<Statistics>,
    runtime: Runtime,
}

impl<K, V, P> Debug for Reclaimer<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reclaimer").finish()
    }
}

impl<K, V, P> Reclaimer<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    pub fn new(
        indexer: Indexer,
        flushers: Vec<Flusher<K, V, P>>,
        reinsertion_picker: Arc<dyn ReinsertionPicker>,
        blob_index_size: usize,
        statistics: Arc<Statistics>,
        runtime: Runtime,
    ) -> Self {
        Self {
            indexer,
            flushers,
            reinsertion_picker,
            blob_index_size,
            statistics,
            runtime,
        }
    }
}

impl<K, V, P> ReclaimerTrait for Reclaimer<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    fn reclaim(&self, region: ReclaimingRegion) -> BoxFuture<'static, ()> {
        let reinsertion_picker = self.reinsertion_picker.clone();
        let statistics = self.statistics.clone();
        let blob_index_size = self.blob_index_size;
        let flushers = self.flushers.clone();
        let runtime = self.runtime.clone();
        let indexer = self.indexer.clone();
        async move {
            let id = region.id();

            tracing::debug!(id, "[reclaimer]: Start reclaiming region.");

            let mut scanner = RegionScanner::new(region.clone(), blob_index_size);
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
                    if reinsertion_picker.pick(&statistics, info.hash).admitted() {
                        let buf = IoSliceMut::new(bits::align_up(PAGE, info.addr.len as _));
                        let (buf, res) = region.read(Box::new(buf), info.addr.offset as _).await;
                        if let Err(e) = res {
                            tracing::warn!(
                                    "[reclaimer]: error raised when reclaiming region {id}, skip the subsequent entries, err: {e}",
                                    id = region.id()
                                );
                            break 'reinsert;
                        }
                        let buf = buf.try_into_io_slice_mut().unwrap().into_io_slice();
                        let slice = buf.slice(..bits::align_up(PAGE, info.addr.len as usize));
                        let flusher = flushers[picked_count % flushers.len()].clone();
                        flusher.submit(Submission::Reinsertion {
                            reinsertion: Reinsertion {
                                hash: info.hash,
                                len: info.addr.len as usize,
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

            let waits = flushers.iter().map(|flusher| flusher.wait()).collect_vec();
            runtime.write().spawn(async move {
                join_all(waits).await;
            });
            indexer.remove_batch(unpicked);

            if let Err(e) = RegionCleaner::clean(&region).await {
                tracing::warn!("reclaimer]: mark region {id} clean error: {e}", id = region.id());
            }

            tracing::debug!(
                "[reclaimer]: Finish reclaiming region {id}, picked: {picked_count}, unpicked: {unpicked_count}."
            );

            region.statistics().reset();

            drop(region);
        }.boxed()
    }
}

#[derive(Debug)]
pub struct RegionCleaner;

impl RegionCleaner {
    pub async fn clean(region: &Region) -> Result<()> {
        let mut page = IoSliceMut::new(PAGE);
        page.fill(0);
        let (_, res) = region.write(Box::new(page), 0).await;
        res?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct Reinsertion {
    pub hash: u64,
    pub len: usize,
    pub sequence: Sequence,
    pub slice: IoSlice,
}
