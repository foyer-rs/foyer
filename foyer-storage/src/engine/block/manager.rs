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
    collections::{HashSet, VecDeque},
    fmt::Debug,
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, RwLock, RwLockWriteGuard,
    },
};

use foyer_common::{
    error::{ErrorKind, Result},
    metrics::Metrics,
};
use futures_core::future::BoxFuture;
use futures_util::{
    future::{ready, Shared},
    FutureExt,
};
use itertools::Itertools;
use rand::seq::IteratorRandom;
use tokio::sync::oneshot;

use crate::{
    engine::block::{
        eviction::{EvictionInfo, EvictionPicker},
        reclaimer::ReclaimerTrait,
    },
    io::{
        bytes::{IoB, IoBuf, IoBufMut},
        device::Partition,
        engine::IoEngine,
    },
    Device, Runtime,
};

pub type BlockId = u32;

/// Block statistics.
#[derive(Debug, Default)]
pub struct BlockStatistics {
    /// Estimated invalid bytes in the block.
    /// FIXME(MrCroxx): This value is way too coarse. Need fix.
    pub invalid: AtomicUsize,
    /// Access count of the block.
    pub access: AtomicUsize,
    /// Marked as `true` if the block is about to be evicted by some eviction picker.
    pub probation: AtomicBool,
}

impl BlockStatistics {
    pub(crate) fn reset(&self) {
        self.invalid.store(0, Ordering::Relaxed);
        self.access.store(0, Ordering::Relaxed);
        self.probation.store(false, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct BlockInner {
    id: BlockId,
    partition: Arc<dyn Partition>,
    io_engine: Arc<dyn IoEngine>,
    statistics: Arc<BlockStatistics>,
}

/// A block is a logical partition of a device. It is used to manage the device's storage space.
#[derive(Debug, Clone)]
pub struct Block {
    inner: Arc<BlockInner>,
}

impl Block {
    /// Get block id.
    pub fn id(&self) -> BlockId {
        self.inner.id
    }

    /// Get block Statistics.
    pub fn statistics(&self) -> &Arc<BlockStatistics> {
        &self.inner.statistics
    }

    /// Get block size.
    pub fn size(&self) -> usize {
        self.inner.partition.size()
    }

    pub(crate) async fn write(&self, buf: Box<dyn IoBuf>, offset: u64) -> (Box<dyn IoB>, Result<()>) {
        let (buf, res) = self
            .inner
            .io_engine
            .write(buf, self.inner.partition.as_ref(), offset)
            .await;
        (buf, res)
    }

    pub(crate) async fn read(&self, buf: Box<dyn IoBufMut>, offset: u64) -> (Box<dyn IoB>, Result<()>) {
        let (buf, res) = self
            .inner
            .io_engine
            .read(buf, self.inner.partition.as_ref(), offset)
            .await;
        (buf, res)
    }

    pub(crate) fn partition(&self) -> &Arc<dyn Partition> {
        &self.inner.partition
    }
}

#[cfg(test)]
impl Block {
    pub(crate) fn new_for_test(id: BlockId, partition: Arc<dyn Partition>, io_engine: Arc<dyn IoEngine>) -> Self {
        let inner = BlockInner {
            id,
            partition,
            io_engine,
            statistics: Arc::<BlockStatistics>::default(),
        };
        let inner = Arc::new(inner);
        Self { inner }
    }
}

pub type GetCleanBlockHandle = Shared<BoxFuture<'static, Block>>;

#[derive(Debug)]
struct State {
    clean_blocks: VecDeque<BlockId>,
    evictable_blocks: HashSet<BlockId>,
    writing_blocks: HashSet<BlockId>,
    reclaiming_blocks: HashSet<BlockId>,

    clean_block_waiters: Vec<oneshot::Sender<Block>>,

    eviction_pickers: Vec<Box<dyn EvictionPicker>>,

    reclaim_waiters: Vec<oneshot::Sender<()>>,
}

#[derive(Debug)]
struct Inner {
    blocks: Vec<Block>,
    state: RwLock<State>,
    reclaimer: Arc<dyn ReclaimerTrait>,
    reclaim_concurrency: usize,
    clean_block_threshold: usize,
    metrics: Arc<Metrics>,
    runtime: Runtime,
}

#[derive(Debug, Clone)]
pub struct BlockManager {
    inner: Arc<Inner>,
}

impl BlockManager {
    #[expect(clippy::too_many_arguments)]
    pub fn open(
        device: Arc<dyn Device>,
        io_engine: Arc<dyn IoEngine>,
        block_size: usize,
        mut eviction_pickers: Vec<Box<dyn EvictionPicker>>,
        reclaimer: Arc<dyn ReclaimerTrait>,
        reclaim_concurrency: usize,
        clean_block_threshold: usize,
        metrics: Arc<Metrics>,
        runtime: Runtime,
    ) -> Result<Self> {
        let mut blocks = vec![];

        while device.free() >= block_size {
            let partition = match device.create_partition(block_size) {
                Ok(partition) => partition,
                Err(e) if e.kind() == ErrorKind::NoSpace => break,
                Err(e) => return Err(e),
            };
            let id = blocks.len() as BlockId;
            let block = Block {
                inner: Arc::new(BlockInner {
                    id,
                    partition,
                    io_engine: io_engine.clone(),
                    statistics: Arc::<BlockStatistics>::default(),
                }),
            };
            blocks.push(block);
        }

        let rs = blocks.iter().map(|r| r.id()).collect_vec();
        for pickers in eviction_pickers.iter_mut() {
            pickers.init(&rs, block_size);
        }

        metrics.storage_block_engine_block_size_bytes.absolute(block_size as _);

        let state = State {
            clean_blocks: VecDeque::new(),
            evictable_blocks: HashSet::new(),
            writing_blocks: HashSet::new(),
            reclaiming_blocks: HashSet::new(),
            clean_block_waiters: Vec::new(),
            eviction_pickers,
            reclaim_waiters: Vec::new(),
        };
        let inner = Inner {
            blocks,
            state: RwLock::new(state),
            reclaimer,
            reclaim_concurrency,
            clean_block_threshold,
            metrics,
            runtime,
        };
        let inner = Arc::new(inner);
        let this = Self { inner };
        Ok(this)
    }

    pub fn init(&self, clean_blocks: &[BlockId]) {
        let mut state = self.inner.state.write().unwrap();
        let mut evictable_blocks: HashSet<BlockId> = self.inner.blocks.iter().map(|r| r.id()).collect();
        state.clean_blocks = clean_blocks
            .iter()
            .inspect(|id| {
                evictable_blocks.remove(id);
            })
            .copied()
            .collect();

        // Temporarily take pickers to make borrow checker happy.
        let mut pickers = std::mem::take(&mut state.eviction_pickers);

        // Notify pickers.
        for block in evictable_blocks {
            state.evictable_blocks.insert(block);
            for picker in pickers.iter_mut() {
                picker.on_block_evictable(
                    EvictionInfo {
                        blocks: &self.inner.blocks,
                        evictable: &state.evictable_blocks,
                        clean: state.clean_blocks.len(),
                    },
                    block,
                );
            }
        }

        // Restore taken pickers after operations.

        std::mem::swap(&mut state.eviction_pickers, &mut pickers);
        assert!(pickers.is_empty());

        let metrics = &self.inner.metrics;
        metrics
            .storage_block_engine_block_clean
            .absolute(state.clean_blocks.len() as _);
        metrics
            .storage_block_engine_block_evictable
            .absolute(state.evictable_blocks.len() as _);
        metrics
            .storage_block_engine_block_writing
            .absolute(state.writing_blocks.len() as _);
        metrics
            .storage_block_engine_block_reclaiming
            .absolute(state.reclaiming_blocks.len() as _);
    }

    pub fn blocks(&self) -> usize {
        self.inner.blocks.len()
    }

    pub fn block(&self, id: BlockId) -> &Block {
        &self.inner.blocks[id as usize]
    }

    pub fn get_clean_block(&self) -> GetCleanBlockHandle {
        let this = self.clone();
        async move {
            // Wrap state lock guard to make borrow checker happy.
            let rx = {
                let mut state = this.inner.state.write().unwrap();
                if let Some(id) = state.clean_blocks.pop_front() {
                    let block = this.inner.blocks[id as usize].clone();
                    state.writing_blocks.insert(id);
                    this.inner.metrics.storage_block_engine_block_clean.decrease(1);
                    this.inner.metrics.storage_block_engine_block_writing.increase(1);
                    this.reclaim_if_needed(&mut state);
                    return block;
                } else {
                    let (tx, rx) = oneshot::channel();
                    state.clean_block_waiters.push(tx);
                    drop(state);
                    rx
                }
            };
            rx.await.unwrap()
        }
        .boxed()
        .shared()
    }

    pub fn on_writing_finish(&self, block: Block) {
        let mut state = self.inner.state.write().unwrap();
        state.writing_blocks.remove(&block.id());
        self.inner.metrics.storage_block_engine_block_writing.decrease(1);
        let inserted = state.evictable_blocks.insert(block.id());
        self.inner.metrics.storage_block_engine_block_evictable.increase(1);

        assert!(inserted);

        // Temporarily take pickers to make borrow checker happy.
        let mut pickers = std::mem::take(&mut state.eviction_pickers);

        // Notify pickers.
        for picker in pickers.iter_mut() {
            picker.on_block_evictable(
                EvictionInfo {
                    blocks: &self.inner.blocks,
                    evictable: &state.evictable_blocks,
                    clean: state.clean_blocks.len(),
                },
                block.id(),
            );
        }

        // Restore taken pickers after operations.

        std::mem::swap(&mut state.eviction_pickers, &mut pickers);
        assert!(pickers.is_empty());

        tracing::debug!(
            id = block.id(),
            "[block manager]: Block state transfers from writing to evictable."
        );

        self.reclaim_if_needed(&mut state);
    }

    fn on_reclaim_finish(&self, block: Block) {
        let mut state = self.inner.state.write().unwrap();
        state.reclaiming_blocks.remove(&block.id());
        self.inner.metrics.storage_block_engine_block_reclaiming.decrease(1);
        if let Some(waiter) = state.clean_block_waiters.pop() {
            self.inner.metrics.storage_block_engine_block_writing.increase(1);
            let _ = waiter.send(block);
        } else {
            self.inner.metrics.storage_block_engine_block_clean.increase(1);
            state.clean_blocks.push_back(block.id());
        }
        self.reclaim_if_needed(&mut state);
        if state.reclaiming_blocks.is_empty() {
            for tx in std::mem::take(&mut state.reclaim_waiters) {
                let _ = tx.send(());
            }
        }
    }

    fn reclaim_if_needed<'a>(&self, state: &mut RwLockWriteGuard<'a, State>) {
        if state.clean_blocks.len() < self.inner.clean_block_threshold
            && state.reclaiming_blocks.len() < self.inner.reclaim_concurrency
        {
            if let Some(block) = self.evict(state) {
                state.reclaiming_blocks.insert(block.id());
                self.inner.metrics.storage_block_engine_block_reclaiming.increase(1);
                let block = ReclaimingBlock {
                    block_manager: self.clone(),
                    block,
                };
                let future = self.inner.reclaimer.reclaim(block);
                self.inner.runtime.write().spawn(future);
            }
        }
    }

    fn evict<'a>(&self, state: &mut RwLockWriteGuard<'a, State>) -> Option<Block> {
        let mut picked = None;

        if state.evictable_blocks.is_empty() {
            return None;
        }

        // Temporarily take pickers to make borrow checker happy.
        let mut pickers = std::mem::take(&mut state.eviction_pickers);

        // Pick a block to evict with pickers.
        for picker in pickers.iter_mut() {
            if let Some(block) = picker.pick(EvictionInfo {
                blocks: &self.inner.blocks,
                evictable: &state.evictable_blocks,
                clean: state.clean_blocks.len(),
            }) {
                picked = Some(block);
                break;
            }
        }

        // If no block is selected, just randomly pick one.
        let picked = picked.unwrap_or_else(|| state.evictable_blocks.iter().choose(&mut rand::rng()).copied().unwrap());

        // Update evictable map.
        let removed = state.evictable_blocks.remove(&picked);
        self.inner.metrics.storage_block_engine_block_evictable.decrease(1);
        assert!(removed);

        // Notify pickers.
        for picker in pickers.iter_mut() {
            picker.on_block_evict(
                EvictionInfo {
                    blocks: &self.inner.blocks,
                    evictable: &state.evictable_blocks,
                    clean: state.clean_blocks.len(),
                },
                picked,
            );
        }

        // Restore taken pickers after operations.
        std::mem::swap(&mut state.eviction_pickers, &mut pickers);
        assert!(pickers.is_empty());

        let block = self.inner.blocks[picked as usize].clone();
        tracing::debug!("[block manager]: Block {picked} is evicted.");

        Some(block)
    }

    pub fn wait_reclaim(&self) -> BoxFuture<'static, ()> {
        let mut state = self.inner.state.write().unwrap();
        if state.reclaiming_blocks.is_empty() {
            return ready(()).boxed();
        }
        let (tx, rx) = oneshot::channel();
        state.reclaim_waiters.push(tx);
        async move {
            let _ = rx.await;
        }
        .boxed()
    }
}

pub struct ReclaimingBlock {
    block_manager: BlockManager,
    block: Block,
}

impl Deref for ReclaimingBlock {
    type Target = Block;

    fn deref(&self) -> &Self::Target {
        &self.block
    }
}

impl DerefMut for ReclaimingBlock {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.block
    }
}

impl Drop for ReclaimingBlock {
    fn drop(&mut self) {
        self.block_manager.on_reclaim_finish(self.block.clone());
    }
}
