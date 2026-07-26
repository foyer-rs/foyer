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
    collections::{HashMap, hash_map::Entry},
    fmt::Debug,
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
        let total = total.into_iter().map(|(_, result)| result).collect::<Vec<_>>();

        // Return error is there is.
        let (total, errs): (Vec<_>, Vec<_>) = total.into_iter().partition(|res| res.is_ok());
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
            match tombstone_sequences.entry(tombstone.hash) {
                Entry::Occupied(mut entry) => {
                    *entry.get_mut() = (*entry.get()).max(tombstone.sequence);
                }
                Entry::Vacant(entry) => {
                    entry.insert(tombstone.sequence);
                }
            }
        }

        let mut clean_blocks = Vec::with_capacity(total.len());
        let mut evictable_blocks = 0;
        let total_entries = total.iter().map(|result| result.as_ref().unwrap().0.len()).sum();
        let mut indices = Vec::with_capacity(total_entries);

        for (block, (infos, block_latest_sequence)) in total.into_iter().map(|result| result.unwrap()).enumerate() {
            let block = block as BlockId;
            if infos.is_empty() {
                clean_blocks.push(block);
            } else {
                evictable_blocks += 1;
            }

            latest_sequence = latest_sequence.max(block_latest_sequence);
            if tombstone_sequences.is_empty() {
                indices.extend(
                    infos
                        .into_iter()
                        .map(|EntryInfo { hash, addr }| HashedEntryAddress { hash, address: addr }),
                );
            } else {
                indices.extend(infos.into_iter().filter_map(|EntryInfo { hash, addr }| {
                    tombstone_sequences
                        .get(&hash)
                        .is_none_or(|sequence| addr.sequence > *sequence)
                        .then_some(HashedEntryAddress { hash, address: addr })
                }));
            }
        }

        indexer
            .insert_recovery_batch(indices, recover_concurrency, spawner.clone())
            .await
            .unwrap();
        let live_entries = indexer.compact_and_count();

        // Log recovery.
        tracing::info!(
            "Recovers {e} blocks with data, {c} clean blocks, {t} scanned entries, {l} live entries with max sequence as {s}..",
            e = evictable_blocks,
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
    async fn run(mode: RecoverMode, block: Block, blob_index_size: usize) -> Result<(Vec<EntryInfo>, Sequence)> {
        if mode == RecoverMode::None {
            return Ok((vec![], 0));
        }

        let mut recovered = vec![];
        let mut latest_sequence = 0;

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
                latest_sequence = latest_sequence.max(info.addr.sequence);
                recovered.push(info);
            }
        }

        Ok((recovered, latest_sequence))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use foyer_common::{metrics::Metrics, spawn::Spawner};
    use tempfile::tempdir;

    use super::BlockRecoverRunner;
    use crate::{
        Compression,
        engine::{
            RecoverMode,
            block::{
                buffer::{Buffer, SplitCtx, Splitter},
                manager::Block,
            },
        },
        io::{
            bytes::IoSliceMut,
            device::{DeviceBuilder, fs::FsDeviceBuilder},
            engine::{IoEngineBuildContext, IoEngineConfig, psync::PsyncIoEngineConfig},
        },
    };

    const KB: usize = 1024;

    #[test_log::test(tokio::test)]
    async fn test_block_recovery_stops_at_out_of_order_sequences() {
        const BLOCK_SIZE: usize = 64 * KB;
        const BLOB_INDEX_SIZE: usize = 4 * KB;

        let dir = tempdir().unwrap();
        let device = FsDeviceBuilder::new(dir.path())
            .with_capacity(BLOCK_SIZE)
            .build()
            .unwrap();
        let partition = device.create_partition(BLOCK_SIZE).unwrap();
        let io_engine = PsyncIoEngineConfig::new()
            .boxed()
            .build(IoEngineBuildContext {
                spawner: Spawner::current(),
            })
            .await
            .unwrap();

        let mut buffer = Buffer::new(
            IoSliceMut::new(BLOCK_SIZE),
            BLOCK_SIZE - BLOB_INDEX_SIZE,
            Arc::new(Metrics::noop()),
        );
        for (key, sequence) in [2, 0, 1].into_iter().enumerate() {
            assert!(buffer.push(
                &(key as u64),
                &vec![key as u8; 3 * KB],
                key as u64,
                Compression::None,
                sequence,
            ));
        }

        let (bytes, infos) = buffer.finish();
        let mut split = SplitCtx::new(BLOCK_SIZE, BLOB_INDEX_SIZE);
        let batch = Splitter::split(&mut split, bytes.into_io_slice(), infos);
        assert_eq!(batch.blocks.len(), 1);
        assert_eq!(batch.blocks[0].blob_parts.len(), 1);
        let part = &batch.blocks[0].blob_parts[0];

        let (_, result) = io_engine
            .write(
                Box::new(part.data.clone()),
                partition.as_ref(),
                part.blob_block_offset as u64 + part.part_blob_offset as u64,
            )
            .await;
        result.unwrap();
        let (_, result) = io_engine
            .write(
                Box::new(part.index.clone()),
                partition.as_ref(),
                part.blob_block_offset as u64,
            )
            .await;
        result.unwrap();

        let block = Block::new_for_test(0, partition, io_engine);
        let (recovered, latest_sequence) = BlockRecoverRunner::run(RecoverMode::Strict, block, BLOB_INDEX_SIZE)
            .await
            .unwrap();
        let sequences = recovered.into_iter().map(|info| info.addr.sequence).collect::<Vec<_>>();
        assert_eq!(sequences, vec![2]);
        assert_eq!(latest_sequence, 2);
    }
}
