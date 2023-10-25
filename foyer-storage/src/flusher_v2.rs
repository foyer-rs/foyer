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

use foyer_common::{
    code::{Key, Value},
    rate::RateLimiter,
};
use foyer_intrusive::{core::adapter::Link, eviction::EvictionPolicy};
use std::{any::Any, sync::Arc};
use tokio::sync::{broadcast, mpsc};

use crate::{
    buffer::{BufferError, FlushBuffer, PositionedEntry},
    catalog::{Catalog, Index, Item, Sequence},
    device::Device,
    error::{Error, Result},
    judge::Judges,
    metrics::Metrics,
    region_manager::{RegionEpItemAdapter, RegionManager},
    reinsertion::ReinsertionPolicy,
    ring::View,
};

#[derive(Debug)]
pub struct Entry {
    /// # Safety
    ///
    /// `key` must be `Arc<K> where K = Flusher<K>`.
    ///
    /// Use `dyn Any` here to avoid contagious generic type.
    pub key: Arc<dyn Any + Send + Sync>,
    pub key_len: usize,
    pub value_len: usize,
    pub sequence: Sequence,
    pub view: View,
}

impl Clone for Entry {
    fn clone(&self) -> Self {
        Self {
            key: Arc::clone(&self.key),
            view: self.view.clone(),
            key_len: self.key_len,
            value_len: self.value_len,
            sequence: self.sequence,
        }
    }
}

#[derive(Debug)]
pub struct Flusher<K, V, D, EP, EL>
where
    K: Key,
    V: Value,
    D: Device,
    EP: EvictionPolicy<Adapter = RegionEpItemAdapter<EL>>,
    EL: Link,
{
    region_manager: Arc<RegionManager<D, EP, EL>>,

    catalog: Arc<Catalog<K>>,

    buffer: FlushBuffer<D>,

    entry_rx: mpsc::UnboundedReceiver<Entry>,

    _rate_limiter: Option<Arc<RateLimiter>>,

    reinsertions: Vec<Arc<dyn ReinsertionPolicy<Key = K, Value = V>>>,

    metrics: Arc<Metrics>,

    stop_rx: broadcast::Receiver<()>,
}

impl<K, V, D, EP, EL> Flusher<K, V, D, EP, EL>
where
    K: Key,
    V: Value,
    D: Device,
    EP: EvictionPolicy<Adapter = RegionEpItemAdapter<EL>>,
    EL: Link,
{
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        region_manager: Arc<RegionManager<D, EP, EL>>,
        catalog: Arc<Catalog<K>>,
        device: D,
        entry_rx: mpsc::UnboundedReceiver<Entry>,
        rate_limiter: Option<Arc<RateLimiter>>,
        reinsertions: Vec<Arc<dyn ReinsertionPolicy<Key = K, Value = V>>>,
        metrics: Arc<Metrics>,
        stop_rx: broadcast::Receiver<()>,
    ) -> Self {
        let buffer = FlushBuffer::new(device.clone());
        Self {
            region_manager,
            catalog,
            buffer,
            entry_rx,
            _rate_limiter: rate_limiter,
            reinsertions,
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
        let old_region = self.buffer.region();

        let entry = match self.buffer.write(entry).await {
            Err(BufferError::NotEnough { entry }) => entry,

            Ok(entries) => return self.update_catalog(entries).await,
            Err(e) => return Err(Error::from(e)),
        };

        // current region is full, rotate flush buffer region and retry

        // 1. evict a region (since regions >= flushers * 2, there must be evictable regions)
        let new_region = self.region_manager.eviction_pop().unwrap();
        let region = self.region_manager.region(&new_region);

        // 2. drop catalog of evicted region
        let items = self.catalog.take_region(&new_region);

        // 3. make sure there is no readers or writers on this region.
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

        // TODO(MrCroxx): do reinsertion backgroud !!!!!!!!!!
        // TODO(MrCroxx): do reinsertion backgroud !!!!!!!!!!
        // TODO(MrCroxx): do reinsertion backgroud !!!!!!!!!!
        if !self.reinsertions.is_empty() {
            for (key, Item { index, .. }) in items {
                let Index::Region {
                    key_len, value_len, ..
                } = index
                else {
                    unreachable!()
                };

                let weight = key_len as usize + value_len as usize;
                let mut judges = Judges::new(self.reinsertions.len());
                for (index, reinsertion) in self.reinsertions.iter().enumerate() {
                    let judge = reinsertion.judge(&key, weight, &self.metrics);
                    judges.set(index, judge);
                }
                if !judges.judge() {
                    for (index, reinsertion) in self.reinsertions.iter().enumerate() {
                        let judge = judges.get(index);
                        reinsertion.on_drop(&key, weight, &self.metrics, judge);
                    }
                    continue;
                }

                for (index, reinsertion) in self.reinsertions.iter().enumerate() {
                    let judge = judges.get(index);
                    reinsertion.on_insert(&key, weight, &self.metrics, judge);
                }
            }
        }

        // 4. rotate flush buffer
        let mut entries = self.buffer.rotate(new_region).await?;

        // 5. make old region evictable
        if let Some(old_region) = old_region {
            self.region_manager.eviction_push(old_region);
        }

        // 6. retry write
        entries.append(&mut self.buffer.write(entry).await?);

        self.update_catalog(entries).await
    }

    async fn update_catalog(&self, entries: Vec<PositionedEntry>) -> Result<()> {
        for PositionedEntry {
            entry:
                Entry {
                    key,
                    view,
                    key_len,
                    value_len,
                    sequence,
                },
            region,
            offset,
        } in entries
        {
            let key = key.downcast::<K>().unwrap();
            let index = Index::Region {
                region,
                version: 0,
                offset: offset as u32,
                len: view.aligned() as u32,
                key_len: key_len as u32,
                value_len: value_len as u32,
            };
            let item = Item { sequence, index };
            self.catalog.insert(key, item);
        }
        Ok(())
    }
}
