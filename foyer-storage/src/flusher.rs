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

use crate::{
    buffer::{BufferError, FlushBuffer, PositionedEntry},
    catalog::{Catalog, Index, Item, Sequence},
    compress::Compression,
    device::Device,
    error::{Error, Result},
    metrics::Metrics,
    region_manager::{RegionEpItemAdapter, RegionManager},
    ring::RingBufferView,
};
use foyer_common::code::Key;
use foyer_intrusive::{core::adapter::Link, eviction::EvictionPolicy};
use std::{any::Any, fmt::Debug, sync::Arc};
use tokio::sync::{broadcast, mpsc};
use tracing::Instrument;

pub struct Entry {
    /// # Safety
    ///
    /// `key` must be `Arc<K> where K = Flusher<K>`.
    ///
    /// Use `dyn Any` here to avoid contagious generic type.
    pub key: Arc<dyn Any + Send + Sync>,
    pub sequence: Sequence,
    pub compression: Compression,

    /// Hold a view of referenced buffer, for lookup and prevent from releasing.
    pub view: RingBufferView,
}

impl Debug for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Entry")
            .field("sequence", &self.sequence)
            .field("view", &self.view)
            .finish()
    }
}

impl Clone for Entry {
    fn clone(&self) -> Self {
        Self {
            key: Arc::clone(&self.key),
            view: self.view.clone(),
            sequence: self.sequence,
            compression: self.compression,
        }
    }
}

#[derive(Debug)]
pub struct Flusher<K, D, EP, EL>
where
    K: Key,
    D: Device,
    EP: EvictionPolicy<Adapter = RegionEpItemAdapter<EL>>,
    EL: Link,
{
    region_manager: Arc<RegionManager<D, EP, EL>>,

    catalog: Arc<Catalog<K>>,

    buffer: FlushBuffer<D>,

    entry_rx: mpsc::UnboundedReceiver<Entry>,

    metrics: Arc<Metrics>,

    stop_rx: broadcast::Receiver<()>,
}

impl<K, D, EP, EL> Flusher<K, D, EP, EL>
where
    K: Key,
    D: Device,
    EP: EvictionPolicy<Adapter = RegionEpItemAdapter<EL>>,
    EL: Link,
{
    pub fn new(
        default_buffer_capacity: usize,
        region_manager: Arc<RegionManager<D, EP, EL>>,
        catalog: Arc<Catalog<K>>,
        device: D,
        entry_rx: mpsc::UnboundedReceiver<Entry>,
        metrics: Arc<Metrics>,
        stop_rx: broadcast::Receiver<()>,
    ) -> Self {
        let buffer = FlushBuffer::new(device.clone(), default_buffer_capacity);
        Self {
            region_manager,
            catalog,
            buffer,
            entry_rx,
            metrics,
            stop_rx,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        loop {
            tokio::select! {
                biased;
                entry = self.entry_rx.recv() => {
                    let Some(entry) = entry else {
                        self.buffer.flush().await?;
                        tracing::info!("[flusher] exit");
                        return Ok(());
                    };
                    self.handle(entry).await?;
                }
                _ = self.stop_rx.recv() => {
                    self.buffer.flush().await?;
                    tracing::info!("[flusher] exit");
                    return Ok(())
                }
            }
        }
    }

    async fn handle(&mut self, entry: Entry) -> Result<()> {
        let timer = self.metrics.inner_op_duration_flusher_handle.start_timer();

        let old_region = self.buffer.region();

        let entry = match self.buffer.write::<K>(entry).await {
            Err(BufferError::NotEnough { entry }) => entry,

            Ok(entries) => return self.update_catalog(entries).await,
            Err(e) => return Err(Error::from(e)),
        };

        // current region is full, rotate flush buffer region and retry

        // 1. get a clean region
        let acquire_clean_region_timer = self
            .metrics
            .inner_op_duration_acquire_clean_region
            .start_timer();
        let new_region = self
            .region_manager
            .clean_regions()
            .acquire()
            .instrument(tracing::debug_span!("acquire_clean_region"))
            .await;
        drop(acquire_clean_region_timer);

        // 2. rotate flush buffer
        let entries = self.buffer.rotate(new_region).await?;
        self.update_catalog(entries).await?;
        if let Some(old_region) = old_region {
            self.region_manager.eviction_push(old_region);
        }

        self.metrics.total_bytes.add(
            self.region_manager
                .region(&new_region)
                .device()
                .region_size() as u64,
        );

        // 3. retry write
        let entries = self.buffer.write::<K>(entry).await?;
        self.update_catalog(entries).await?;

        drop(timer);
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn update_catalog(&self, entries: Vec<PositionedEntry>) -> Result<()> {
        // record fully flushed bytes by the way
        let mut bytes = 0;

        let timer = self.metrics.inner_op_duration_update_catalog.start_timer();
        for PositionedEntry {
            entry: Entry { key, sequence, .. },
            region,
            offset,
            len,
        } in entries
        {
            bytes += len;
            let key = key.downcast::<K>().unwrap();
            let index = Index::Region {
                view: self
                    .region_manager
                    .region(&region)
                    .view(offset as u32, len as u32),
            };
            let item = Item::new(sequence, index);
            self.catalog.insert(key, item);
        }
        drop(timer);

        self.metrics.op_bytes_flush.inc_by(bytes as u64);

        Ok(())
    }
}
