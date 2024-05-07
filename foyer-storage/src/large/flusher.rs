//  Copyright 2024 Foyer Project Authors
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

use std::{fmt::Debug, hash::BuildHasher};

use crate::catalog::Sequence;
use crate::device::allocator::WritableVecA;
use crate::error::{Error, Result};
use crate::serde::EntrySerializer;
use crate::Compression;

use foyer_common::async_batch_pipeline::AsyncBatchPipeline;
use foyer_common::bits;
use foyer_common::code::{StorageKey, StorageValue};
use foyer_memory::{CacheEntry, DefaultCacheEventListener};
use futures::future::try_join_all;

use tokio::sync::oneshot;

use super::device::{Device, DeviceExt, IoBuffer, RegionId, IO_BUFFER_ALLOCATOR};
use super::generic::GenericStoreConfig;
use super::indexer::{EntryAddress, Indexer};
use super::region::{GetCleanRegionHandle, RegionManager};

struct BatchState<K, V, S, D>
where
    K: StorageKey,
    V: StorageValue,
    S: BuildHasher + Send + Sync + 'static + Debug,
    D: Device,
{
    groups: Vec<WriteGroup<K, V, S, D>>,
}

impl<K, V, S, D> Debug for BatchState<K, V, S, D>
where
    K: StorageKey,
    V: StorageValue,
    S: BuildHasher + Send + Sync + 'static + Debug,
    D: Device,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchState").field("groups", &self.groups).finish()
    }
}

impl<K, V, S, D> Default for BatchState<K, V, S, D>
where
    K: StorageKey,
    V: StorageValue,
    S: BuildHasher + Send + Sync + 'static + Debug,
    D: Device,
{
    fn default() -> Self {
        Self { groups: vec![] }
    }
}

struct WriteGroup<K, V, S, D>
where
    K: StorageKey,
    V: StorageValue,
    S: BuildHasher + Send + Sync + 'static,
    D: Device,
{
    writer: RegionHandle<D>,

    buffer: IoBuffer,
    indices: Vec<(u64, EntryAddress)>,
    txs: Vec<oneshot::Sender<Result<()>>>,
    // hold the entries to avoid memory cache lookup miss?
    entries: Vec<CacheEntry<K, V, DefaultCacheEventListener<K, V>, S>>,
}

impl<K, V, S, D> Debug for WriteGroup<K, V, S, D>
where
    K: StorageKey,
    V: StorageValue,
    S: BuildHasher + Send + Sync + 'static,
    D: Device,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteGroup")
            .field("writer", &self.writer)
            .field("buffer_len", &self.buffer.len())
            .field("indices", &self.indices)
            .finish()
    }
}

struct RegionHandle<D>
where
    D: Device,
{
    handle: GetCleanRegionHandle<D>,
    offset: u64,
    size: usize,
    is_full: bool,
}

impl<D> Debug for RegionHandle<D>
where
    D: Device,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegionHandle")
            .field("offset", &self.offset)
            .field("size", &self.size)
            .field("is_full", &self.is_full)
            .finish()
    }
}

impl<D> Clone for RegionHandle<D>
where
    D: Device,
{
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            offset: self.offset,
            size: self.size,
            is_full: self.is_full,
        }
    }
}

#[derive(Debug)]
pub struct Flusher<K, V, S, D>
where
    K: StorageKey,
    V: StorageValue,
    S: BuildHasher + Send + Sync + 'static + Debug,
    D: Device,
{
    batch: AsyncBatchPipeline<BatchState<K, V, S, D>, Result<()>>,
    indexer: Indexer,
    device: D,

    region_manager: RegionManager<D>,

    compression: Compression,
    flush: bool,
}

impl<K, V, S, D> Flusher<K, V, S, D>
where
    K: StorageKey,
    V: StorageValue,
    S: BuildHasher + Send + Sync + 'static + Debug,
    D: Device,
{
    pub async fn open(
        config: &GenericStoreConfig<S, D>,
        device: D,
        indexer: Indexer,
        region_manager: RegionManager<D>,
    ) -> Result<Self> {
        let batch = AsyncBatchPipeline::new(BatchState::default());

        Ok(Self {
            batch,
            indexer,
            device,
            region_manager,
            compression: config.compression,
            flush: config.flush,
        })
    }

    pub fn submit(
        &self,
        entry: CacheEntry<K, V, DefaultCacheEventListener<K, V>, S>,
        sequence: Sequence,
    ) -> oneshot::Receiver<Result<()>> {
        tracing::trace!("[flusher]: submit entry with sequence: {sequence}");

        let (tx, rx) = oneshot::channel();
        let append = |state: &mut BatchState<K, V, S, D>| {
            let hash = entry.hash();

            // Attempt to get a clean region for the new group.
            if state.groups.is_empty() {
                let handle = self.region_manager.get_clean_region();
                state.groups.push(WriteGroup {
                    writer: RegionHandle {
                        handle,
                        offset: 0,
                        size: 0,
                        is_full: false,
                    },
                    buffer: IoBuffer::with_capacity_in(self.device.region_size(), &IO_BUFFER_ALLOCATOR),
                    indices: vec![],
                    txs: vec![],
                    entries: vec![],
                })
            }

            // Attempt to pick the latest group to write.
            let group = state.groups.last_mut().unwrap();

            // Write to the latest group and check aligns.
            bits::debug_assert_aligned(self.device.align(), group.buffer.len());
            let mut boffset = group.buffer.len();

            if let Err(e) = EntrySerializer::serialize(
                entry.key(),
                entry.value(),
                entry.hash(),
                &sequence,
                &self.compression,
                WritableVecA(&mut group.buffer),
            ) {
                let _ = tx.send(Err(e));
                return;
            }

            let len = group.buffer.len() - boffset;
            let aligned = bits::align_up(self.device.align(), len);
            group.buffer.reserve(aligned - len);
            unsafe { group.buffer.set_len(boffset + aligned) };

            bits::debug_assert_aligned(self.device.align(), group.buffer.len());

            // Split the latest group if it exceeds the current region.
            if group.writer.size + group.buffer.len() > self.device.region_size() {
                tracing::trace!("[flusher]: split group at size: {size}, buf len: {buf_len}, total (if not split): {total}, exceeds region size: {region_size}", 
                    size = group.writer.size,
                    buf_len = group.buffer.len(),
                    total = group.writer.size + group.buffer.len() ,
                    region_size = self.device.region_size(),
                );

                group.writer.is_full = true;

                let buffer = group.buffer.split_off(boffset);

                bits::debug_assert_aligned(self.device.align(), group.buffer.len());
                bits::debug_assert_aligned(self.device.align(), buffer.len());

                let handle = self.region_manager.get_clean_region();
                state.groups.push(WriteGroup {
                    writer: RegionHandle {
                        handle,
                        offset: 0,
                        size: 0,
                        is_full: false,
                    },
                    buffer,
                    indices: vec![],
                    txs: vec![],
                    entries: vec![],
                });
                boffset = 0;
            }

            // Re-reference the latest group in case it may be out-dated.
            let group = state.groups.last_mut().unwrap();
            group.indices.push((
                hash,
                EntryAddress {
                    region: RegionId::MAX,
                    offset: group.writer.size as u32 + boffset as u32,
                    len: len as _,
                },
            ));
            group.txs.push(tx);
            group.entries.push(entry);
            group.writer.size += aligned;
        };

        if let Some(token) = self.batch.accumulate(append) {
            tracing::trace!("[flusher]: entry with sequence: {sequence} becomes leader");

            let indexer = self.indexer.clone();
            let flush = self.flush;
            let region_manager = self.region_manager.clone();
            token.pipeline(
                |state| {
                    tracing::trace!("[flusher]: create new state based on old state: {state:?}");

                    let mut s = BatchState::default();
                    if let Some(group) = state.groups.last() {
                        let mut writer = group.writer.clone();
                        debug_assert_eq!(
                            writer.offset as usize + group.buffer.len(),
                            writer.size,
                            "offset ({offset}) + buffer len ({buf_len}) = total ({total}) != size ({size})",
                            offset = writer.offset,
                            buf_len = group.buffer.len(),
                            total = writer.offset as usize + group.buffer.len(),
                            size = writer.size,
                        );
                        writer.offset = writer.size as u64;
                        s.groups.push(WriteGroup {
                            writer,
                            buffer: IoBuffer::with_capacity_in(self.device.region_size(), &IO_BUFFER_ALLOCATOR),
                            indices: vec![],
                            txs: vec![],
                            entries: vec![],
                        });
                    }
                    s
                },
                |res| match res {
                    Ok(_) => {}
                    Err(e) => tracing::error!("batch pipeline error: {e}"),
                },
                move |state| async move {
                    // Write regions concurrently.
                    let futures = state.groups.into_iter().map(|group| {
                        let indexer = indexer.clone();
                        let region_manager = region_manager.clone();
                        async move {
                            // Wait for region is clean.
                            let region = group.writer.handle.await;

                            tracing::trace!(
                                "[flusher]: write region: {id}, at offset: {offset}, buffer len: {buf_len}",
                                id = region.id(),
                                offset = group.writer.size,
                                buf_len = group.buffer.len(),
                            );

                            // Write buffet to device.
                            if !group.buffer.is_empty() {
                                region
                                    .device()
                                    .write(group.buffer, region.id(), group.writer.offset)
                                    .await?;
                                if flush {
                                    region.device().flush(Some(region.id())).await?;
                                }

                                let mut indices = group.indices;
                                for (_, addr) in indices.iter_mut() {
                                    addr.region = region.id();
                                }
                                indexer.insert_batch(indices);

                                for tx in group.txs {
                                    let _ = tx.send(Ok(()));
                                }
                            }

                            if group.writer.is_full {
                                region_manager.mark_evictable(region.id());
                            }

                            tracing::trace!("[flusher]: write region {id} finish.", id = region.id());

                            Ok::<_, Error>(())
                        }
                    });
                    try_join_all(futures).await?;

                    Ok(())
                },
            );
        }

        rx
    }

    /// Wait for the current batch to finish.
    pub async fn wait(&self) -> Result<()> {
        if let Some(handle) = self.batch.wait() {
            match handle.await.unwrap() {
                Ok(_) => {}
                Err(e) => tracing::error!("batch pipeline error: {e}"),
            }
        }
        Ok(())
    }
}
