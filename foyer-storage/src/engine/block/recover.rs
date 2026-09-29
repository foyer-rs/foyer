// Copyright 2026 foyer Project Authors
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
    collections::HashMap,
    fmt::Debug,
    mem::size_of,
    sync::{Arc, atomic::Ordering},
    time::Instant,
};

use foyer_common::{
    error::{Error, ErrorKind, Result},
    metrics::Metrics,
    spawn::Spawner,
};
use futures_util::{StreamExt, TryStreamExt, stream};

use super::indexer::Indexer;
use crate::engine::{
    RecoverMode,
    block::{
        indexer::HashedEntryAddress,
        manager::{Block, BlockId, BlockManager},
        scanner::{BlockScanner, EntryInfo},
        serde::{AtomicSequence, Sequence},
        tombstone::Tombstone,
    },
};

// Bound the extra flat and per-shard buffers used while installing recovered entries. Scan results stay intact until
// all block errors have been aggregated, preserving the existing all-scans-before-indexing behavior.
const RECOVERY_INDEX_BATCH_BYTES: usize = 32 * 1024 * 1024;

#[derive(Debug)]
pub struct RecoverRunner;

impl RecoverRunner {
    #[expect(clippy::too_many_arguments)]
    pub async fn run(
        recover_concurrency: usize,
        recover_mode: RecoverMode,
        blob_index_size: usize,
        blocks: Vec<BlockId>,
        sequence: &AtomicSequence,
        indexer: &Indexer,
        block_manager: &BlockManager,
        tombstones: &[Tombstone],
        spawner: Spawner,
        metrics: Arc<Metrics>,
    ) -> Result<()> {
        let now = Instant::now();

        if recover_mode == RecoverMode::None {
            let latest_sequence = tombstones
                .iter()
                .map(|tombstone| tombstone.sequence)
                .max()
                .unwrap_or_default();
            sequence.store(latest_sequence + 1, Ordering::Release);
            block_manager.init(&blocks);

            let elapsed = now.elapsed();
            tracing::info!(
                "Recovers 0 blocks with data, {c} clean blocks, 0 scanned entries, 0 live entries with max sequence as {s}..",
                c = blocks.len(),
                s = latest_sequence,
            );
            tracing::info!("[recover] finish in {:?}", elapsed);
            metrics
                .storage_block_engine_recover_duration
                .record(elapsed.as_secs_f64());
            return Ok(());
        }

        // Recover blocks concurrently.
        let mode = recover_mode;
        let mut total = stream::iter(blocks.into_iter().enumerate().map(|(order, id)| {
            let block = block_manager.block(id).clone();
            spawner.spawn(async move { (order, BlockRecoverRunner::run(mode, block, blob_index_size).await) })
        }))
        .buffer_unordered(recover_concurrency.max(1))
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
        // Preserve the original input order for error aggregation and equal-sequence replacement.
        total.sort_unstable_by_key(|(order, _)| *order);

        // Return error is there is.
        let (total, errs): (Vec<_>, Vec<_>) = total.into_iter().map(|(_, result)| result).partition(|res| res.is_ok());
        if !errs.is_empty() {
            let mut e = Error::new(ErrorKind::Recover, "failed to recover blocks");
            for err in errs.into_iter().map(|r| r.unwrap_err()) {
                e = e.with_context("reason", err.to_string());
            }
            return Err(e);
        }

        // Install recovered entries into the indexer. The indexer deduplicates by sequence, so recovery does not need
        // an extra global dedup table.
        let mut latest_sequence = 0;
        let mut tombstone_sequences = HashMap::<u64, Sequence>::with_capacity(tombstones.len());
        for tombstone in tombstones {
            latest_sequence = latest_sequence.max(tombstone.sequence);
            tombstone_sequences
                .entry(tombstone.hash)
                .and_modify(|sequence| *sequence = (*sequence).max(tombstone.sequence))
                .or_insert(tombstone.sequence);
        }

        let recovered_blocks = total.len();
        let mut clean_blocks = Vec::with_capacity(total.len());
        let total_entries: usize = total.iter().map(|result| result.as_ref().unwrap().len()).sum();
        let batch_entries = (RECOVERY_INDEX_BATCH_BYTES / size_of::<HashedEntryAddress>()).max(1);
        let mut indices = Vec::with_capacity(total_entries.min(batch_entries));
        let filter_tombstones = !tombstone_sequences.is_empty();

        for (block, infos) in total.into_iter().map(|result| result.unwrap()).enumerate() {
            let block = block as BlockId;
            if infos.is_empty() {
                clean_blocks.push(block);
            }

            for EntryInfo { hash, addr } in infos {
                latest_sequence = latest_sequence.max(addr.sequence);
                if filter_tombstones
                    && tombstone_sequences
                        .get(&hash)
                        .is_some_and(|sequence| addr.sequence <= *sequence)
                {
                    continue;
                }

                indices.push(HashedEntryAddress { hash, address: addr });
                if indices.len() >= batch_entries {
                    indexer
                        .insert_recovery_batch(&mut indices, recover_concurrency, spawner.clone())
                        .await
                        .unwrap();
                }
            }
        }

        if !indices.is_empty() {
            indexer
                .insert_recovery_batch(&mut indices, recover_concurrency, spawner.clone())
                .await
                .unwrap();
        }
        let live_entries = indexer.compact_and_count();

        // Log recovery.
        tracing::info!(
            "Recovers {e} blocks with data, {c} clean blocks, {t} scanned entries, {l} live entries with max sequence as {s}..",
            e = recovered_blocks - clean_blocks.len(),
            c = clean_blocks.len(),
            t = total_entries,
            l = live_entries,
            s = latest_sequence,
        );

        // Update components.
        sequence.store(latest_sequence + 1, Ordering::Release);
        block_manager.init(&clean_blocks);

        let elapsed = now.elapsed();
        tracing::info!("[recover] finish in {:?}", elapsed);

        metrics
            .storage_block_engine_recover_duration
            .record(elapsed.as_secs_f64());

        Ok(())
    }
}

#[derive(Debug)]
struct BlockRecoverRunner;

impl BlockRecoverRunner {
    async fn run(mode: RecoverMode, block: Block, blob_index_size: usize) -> Result<Vec<EntryInfo>> {
        let mut recovered = vec![];

        let id = block.id();
        let mut iter = BlockScanner::new(block, blob_index_size);
        'recover: loop {
            let r = iter.next().await;
            let infos = match r {
                Ok(Some(infos)) => infos,
                Ok(None) => break,
                Err(e) => {
                    if mode == RecoverMode::Strict {
                        return Err(e);
                    } else {
                        tracing::warn!("error raised when recovering block {id}, skip further recovery for {id}.");
                        break;
                    }
                }
            };

            for info in infos {
                if info.addr.sequence < recovered.last().map(|last: &EntryInfo| last.addr.sequence).unwrap_or(0) {
                    break 'recover;
                }
                recovered.push(info);
            }
        }

        Ok(recovered)
    }
}
