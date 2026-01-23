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
    sync::atomic::Ordering,
};

use foyer_common::strict_assert;
use itertools::Itertools;

use crate::engine::block::manager::{Block, BlockId};

/// Eviction related information for eviction picker to make decisions.
#[derive(Debug)]
pub struct EvictionInfo<'a> {
    /// All blocks in the disk cache.
    pub blocks: &'a [Block],
    /// Evictable blocks.
    pub evictable: &'a HashSet<BlockId>,
    /// Clean blocks counts.
    pub clean: usize,
}

/// The eviction picker for the disk cache.
pub trait EvictionPicker: Send + Sync + 'static + Debug {
    /// Init the eviction picker with information.
    #[expect(unused_variables)]
    fn init(&mut self, blocks: &[BlockId], block_size: usize) {}

    /// Pick a block to evict.
    ///
    /// `pick` can return `None` if no block can be picked based on its rules, and the next picker will be used.
    ///
    /// If no picker picks a block, the disk cache will pick randomly pick one.
    fn pick(&mut self, info: EvictionInfo<'_>) -> Option<BlockId>;

    /// Notify the picker that a block is ready to pick.
    fn on_block_evictable(&mut self, info: EvictionInfo<'_>, block: BlockId);

    /// Notify the picker that a block is evicted.
    fn on_block_evict(&mut self, info: EvictionInfo<'_>, block: BlockId);
}

/// A picker that pick block to eviction with a FIFO behavior.
#[derive(Debug)]
pub struct FifoPicker {
    queue: VecDeque<BlockId>,
    blocks: usize,
    probations: usize,
    probation_ratio: f64,
}

impl Default for FifoPicker {
    fn default() -> Self {
        Self::new(0.1)
    }
}

impl FifoPicker {
    /// Create a new [`FifoPicker`] with the given `probation_ratio` (0.0 ~ 1.0).
    ///
    /// The `probation_ratio` is the ratio of blocks that will be marked as probation.
    /// The blocks that are marked as probation will be evicted first.
    ///
    /// The default value is 0.1.
    pub fn new(probation_ratio: f64) -> Self {
        assert!(
            (0.0..=1.0).contains(&probation_ratio),
            "probation ratio {probation_ratio} must be in [0.0, 1.0]"
        );
        Self {
            queue: VecDeque::new(),
            blocks: 0,
            probations: 0,
            probation_ratio,
        }
    }
}

impl FifoPicker {
    fn mark_probation(&self, info: EvictionInfo<'_>) {
        let marks = self.probations.saturating_sub(info.clean);
        self.queue.iter().take(marks).for_each(|rid| {
            if info.evictable.contains(rid) {
                info.blocks[*rid as usize]
                    .statistics()
                    .probation
                    .store(true, Ordering::Relaxed);
                tracing::trace!(rid, "[fifo picker]: mark probation");
            }
        });
    }
}

impl EvictionPicker for FifoPicker {
    fn init(&mut self, blocks: &[BlockId], _: usize) {
        self.blocks = blocks.len();
        let probations = (self.blocks as f64 * self.probation_ratio).floor() as usize;
        self.probations = probations.clamp(0, self.blocks);
    }

    fn pick(&mut self, info: EvictionInfo<'_>) -> Option<BlockId> {
        tracing::trace!(evictable = ?info.evictable, clean = info.clean, queue = ?self.queue, "[fifo picker]: pick");
        self.mark_probation(info);
        let candidate = self.queue.front().copied();
        tracing::trace!("[fifo picker]: pick {candidate:?}");
        candidate
    }

    fn on_block_evictable(&mut self, _: EvictionInfo<'_>, block: BlockId) {
        tracing::trace!(queue = ?self.queue, "[fifo picker]: {block} is evictable");
        self.queue.push_back(block);
    }

    fn on_block_evict(&mut self, _: EvictionInfo<'_>, block: BlockId) {
        tracing::trace!(queue = ?self.queue, "[fifo picker]: {block} is evicted");
        let index = self.queue.iter().position(|r| r == &block).unwrap();
        self.queue.remove(index);
    }
}

/// Evict the block with the largest invalid data ratio.
///
/// If the largest invalid data ratio is less than the threshold, no reblockgion will be picked.
#[derive(Debug)]
pub struct InvalidRatioPicker {
    threshold: f64,
    block_size: usize,
}

impl InvalidRatioPicker {
    /// Create [`InvalidRatioPicker`] with the given `threshold` (0.0 ~ 1.0).
    pub fn new(threshold: f64) -> Self {
        let ratio = threshold.clamp(0.0, 1.0);
        Self {
            threshold: ratio,
            block_size: 0,
        }
    }
}

impl EvictionPicker for InvalidRatioPicker {
    fn init(&mut self, _: &[BlockId], block_size: usize) {
        self.block_size = block_size;
    }

    fn pick(&mut self, info: EvictionInfo<'_>) -> Option<BlockId> {
        strict_assert!(self.block_size > 0);

        let mut data = info
            .evictable
            .iter()
            .map(|rid| {
                (
                    *rid,
                    info.blocks[*rid as usize].statistics().invalid.load(Ordering::Relaxed),
                )
            })
            .collect_vec();
        data.sort_by_key(|(_, invalid)| *invalid);

        let (rid, invalid) = data.last().copied()?;
        if (invalid as f64 / self.block_size as f64) < self.threshold {
            return None;
        }
        tracing::trace!("[invalid ratio picker]: pick {rid:?}");
        Some(rid)
    }

    fn on_block_evictable(&mut self, _: EvictionInfo<'_>, block: BlockId) {
        tracing::trace!("[invalid ratio picker]: {block} is evictable");
    }

    fn on_block_evict(&mut self, _: EvictionInfo<'_>, block: BlockId) {
        tracing::trace!("[invalid ratio picker]: {block} is evicted");
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

    use itertools::Itertools;

    use super::*;
    use crate::{engine::block::manager::Block, io::device::noop::NoopPartition, IoEngineBuilder, NoopIoEngineBuilder};

    #[test_log::test(tokio::test)]
    async fn test_fifo_picker() {
        let mut picker = FifoPicker::new(0.1);
        let mock_io_engine = NoopIoEngineBuilder.build().await.unwrap();

        let blocks = (0..10)
            .map(|rid| Block::new_for_test(rid, Arc::<NoopPartition>::default(), mock_io_engine.clone()))
            .collect_vec();
        let mut evictable = HashSet::new();

        fn info<'a>(blocks: &'a [Block], evictable: &'a HashSet<BlockId>, dirty: usize) -> EvictionInfo<'a> {
            EvictionInfo {
                blocks,
                evictable,
                clean: blocks.len() - evictable.len() - dirty,
            }
        }

        fn check(block: &[Block], probations: impl IntoIterator<Item = BlockId>) {
            let probations = probations.into_iter().collect::<HashSet<_>>();
            for rid in 0..block.len() as BlockId {
                if probations.contains(&rid) {
                    assert!(
                        block[rid as usize].statistics().probation.load(Ordering::Relaxed),
                        "probations {probations:?}, assert {rid} is probation failed"
                    );
                } else {
                    assert!(
                        !block[rid as usize].statistics().probation.load(Ordering::Relaxed),
                        "probations {probations:?}, assert {rid} is not probation failed"
                    );
                }
            }
        }

        picker.init(&(0..10).collect_vec(), 0);

        // mark 0..10 evictable in order
        (0..10).for_each(|i| {
            evictable.insert(i as _);
            picker.on_block_evictable(info(&blocks, &evictable, 0), i);
        });

        // evict 0, mark 0 probation.
        assert_eq!(picker.pick(info(&blocks, &evictable, 0)), Some(0));
        check(&blocks, [0]);
        evictable.remove(&0);
        picker.on_block_evict(info(&blocks, &evictable, 1), 0);

        // evict 1, with 1 dirty block, mark 1 probation.
        assert_eq!(picker.pick(info(&blocks, &evictable, 1)), Some(1));
        check(&blocks, [0, 1]);
        evictable.remove(&1);
        picker.on_block_evict(info(&blocks, &evictable, 2), 1);

        // evict 2 with 1 dirty block, mark no block probation.
        assert_eq!(picker.pick(info(&blocks, &evictable, 1)), Some(2));
        check(&blocks, [0, 1]);
        evictable.remove(&2);
        picker.on_block_evict(info(&blocks, &evictable, 2), 2);

        picker.on_block_evict(info(&blocks, &evictable, 3), 3);
        evictable.remove(&3);
        picker.on_block_evict(info(&blocks, &evictable, 4), 5);
        evictable.remove(&5);
        picker.on_block_evict(info(&blocks, &evictable, 5), 7);
        evictable.remove(&7);
        picker.on_block_evict(info(&blocks, &evictable, 6), 9);
        evictable.remove(&9);

        picker.on_block_evict(info(&blocks, &evictable, 7), 4);
        evictable.remove(&4);
        picker.on_block_evict(info(&blocks, &evictable, 8), 6);
        evictable.remove(&6);
        picker.on_block_evict(info(&blocks, &evictable, 9), 8);
        evictable.remove(&8);
    }

    #[test_log::test(tokio::test)]
    async fn test_invalid_ratio_picker() {
        let mut picker = InvalidRatioPicker::new(0.5);
        picker.init(&(0..10).collect_vec(), 10);

        let mock_io_engine = NoopIoEngineBuilder.build().await.unwrap();

        let blocks = (0..10)
            .map(|rid| Block::new_for_test(rid, Arc::<NoopPartition>::default(), mock_io_engine.clone()))
            .collect_vec();
        let mut evictable = HashSet::new();

        (0..10).for_each(|i| {
            blocks[i].statistics().invalid.fetch_add(i as _, Ordering::Relaxed);
            evictable.insert(i as BlockId);
        });

        fn info<'a>(blocks: &'a [Block], evictable: &'a HashSet<BlockId>) -> EvictionInfo<'a> {
            EvictionInfo {
                blocks,
                evictable,
                clean: blocks.len() - evictable.len(),
            }
        }

        assert_eq!(picker.pick(info(&blocks, &evictable)), Some(9));

        assert_eq!(picker.pick(info(&blocks, &evictable)), Some(9));
        evictable.remove(&9);
        assert_eq!(picker.pick(info(&blocks, &evictable)), Some(8));
        evictable.remove(&8);
        assert_eq!(picker.pick(info(&blocks, &evictable)), Some(7));
        evictable.remove(&7);
        assert_eq!(picker.pick(info(&blocks, &evictable)), Some(6));
        evictable.remove(&6);
        assert_eq!(picker.pick(info(&blocks, &evictable)), Some(5));
        evictable.remove(&5);

        assert_eq!(picker.pick(info(&blocks, &evictable)), None);
        assert_eq!(picker.pick(info(&blocks, &evictable)), None);
    }
}
