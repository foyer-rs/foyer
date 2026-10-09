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
    sync::Arc,
};

use foyer_common::{error::Result, hasher::ModHasher, spawn::Spawner};
use futures_util::{StreamExt, TryStreamExt, stream};
use itertools::Itertools;
use parking_lot::RwLock;

use crate::engine::block::{manager::BlockId, serde::Sequence};

#[derive(Debug, Clone)]
pub enum Index {
    Address(EntryAddress),
    Tombstone(Sequence),
}

impl Index {
    fn sequence(&self) -> Sequence {
        match self {
            Index::Address(addr) => addr.sequence,
            Index::Tombstone(seq) => *seq,
        }
    }
}

#[derive(Debug, Clone)]
pub struct HashedEntryAddress {
    pub hash: u64,
    pub address: EntryAddress,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryAddress {
    pub block: BlockId,
    pub offset: u32,
    pub len: u32,

    pub sequence: Sequence,
}

type IndexerShard = HashMap<u64, Index, ModHasher>;

/// [`Indexer`] records key hash to entry address on fs.
#[derive(Debug, Clone)]
pub struct Indexer {
    shards: Arc<Vec<RwLock<IndexerShard>>>,
    shard_mask: usize,
}

impl Indexer {
    pub fn new(shards: usize) -> Self {
        let shard_mask = if shards.is_power_of_two() {
            shards - 1
        } else {
            usize::MAX
        };
        let shards = (0..shards)
            .map(|_| RwLock::new(HashMap::with_hasher(ModHasher::default())))
            .collect_vec();
        Self {
            shards: Arc::new(shards),
            shard_mask,
        }
    }

    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::storage::block::indexer::insert_tombstone")
    )]
    pub fn insert_tombstone(&self, hash: u64, sequence: Sequence) -> Option<EntryAddress> {
        let shard = self.shard(hash);
        let mut shard = self.shards[shard].write();
        self.insert_inner(&mut shard, hash, Index::Tombstone(sequence))
    }

    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::storage::block::indexer::insert_batch")
    )]
    pub fn insert_batch(&self, mut batch: Vec<HashedEntryAddress>) -> Vec<HashedEntryAddress> {
        let shards = self.partition_batch(&mut batch);
        drop(batch);
        shards
            .into_iter()
            .enumerate()
            .filter(|(_, batch)| !batch.is_empty())
            .flat_map(|(shard, batch)| self.insert_shard_batch(shard, batch))
            .collect()
    }

    pub(super) async fn insert_recovery_batch(
        &self,
        batch: &mut Vec<HashedEntryAddress>,
        concurrency: usize,
        spawner: Spawner,
    ) -> Result<()> {
        let this = self.clone();
        stream::iter(
            self.partition_batch(batch)
                .into_iter()
                .enumerate()
                .filter(|(_, batch)| !batch.is_empty())
                .map(move |(shard, batch)| {
                    let this = this.clone();
                    spawner.spawn(async move { this.insert_shard_batch_discard(shard, batch) })
                }),
        )
        .buffer_unordered(concurrency.max(1))
        .try_collect::<Vec<_>>()
        .await
        .map(drop)
    }

    fn partition_batch(&self, batch: &mut Vec<HashedEntryAddress>) -> Vec<Vec<HashedEntryAddress>> {
        let mut counts = vec![0; self.shards.len()];
        for haddr in batch.iter() {
            counts[self.shard(haddr.hash)] += 1;
        }

        let mut shards = counts.iter().map(|count| Vec::with_capacity(*count)).collect_vec();
        for haddr in batch.drain(..) {
            let shard = self.shard(haddr.hash);
            shards[shard].push(haddr);
        }
        shards
    }

    fn insert_shard_batch(&self, shard: usize, batch: Vec<HashedEntryAddress>) -> Vec<HashedEntryAddress> {
        let mut olds = vec![];
        self.insert_shard_batch_with(shard, batch, |hash, address| {
            olds.push(HashedEntryAddress { hash, address });
        });
        olds
    }

    fn insert_shard_batch_discard(&self, shard: usize, batch: Vec<HashedEntryAddress>) {
        self.insert_shard_batch_with(shard, batch, |_, _| {});
    }

    fn insert_shard_batch_with(
        &self,
        shard: usize,
        batch: Vec<HashedEntryAddress>,
        mut on_old: impl FnMut(u64, EntryAddress),
    ) {
        let mut shard = self.shards[shard].write();
        let available = shard.capacity().saturating_sub(shard.len());
        if available < batch.len() {
            shard.reserve(batch.len());
        }
        for haddr in batch {
            if let Some(old) = self.insert_inner(&mut shard, haddr.hash, Index::Address(haddr.address)) {
                on_old(haddr.hash, old);
            }
        }
    }

    #[cfg_attr(feature = "tracing", fastrace::trace(name = "foyer::storage::block::indexer::get"))]
    pub fn get(&self, hash: u64) -> Option<EntryAddress> {
        let shard = self.shard(hash);
        match self.shards[shard].read().get(&hash) {
            Some(index) => match index {
                Index::Address(addr) => Some(addr.clone()),
                Index::Tombstone(_) => None,
            },
            None => None,
        }
    }

    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::storage::block::indexer::remove")
    )]
    pub fn remove(&self, hash: u64) -> Option<EntryAddress> {
        let shard = self.shard(hash);
        match self.shards[shard].write().entry(hash) {
            Entry::Occupied(o) => match o.get() {
                Index::Address(_) => self.extract_address(o.remove()),
                Index::Tombstone(_) => None,
            },
            Entry::Vacant(_) => None,
        }
    }

    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::storage::block::indexer::remove_batch")
    )]
    pub fn remove_batch<I>(&self, batch: I) -> Vec<EntryAddress>
    where
        I: IntoIterator<Item = (u64, Sequence)>,
    {
        let shards = batch.into_iter().into_group_map_by(|(hash, _)| self.shard(*hash));

        let mut olds = vec![];
        for (s, hashes) in shards {
            let mut shard = self.shards[s].write();
            for (hash, sequence) in hashes {
                match shard.entry(hash) {
                    Entry::Occupied(o) => {
                        if sequence >= o.get().sequence()
                            && let Some(addr) = self.extract_address(o.remove())
                        {
                            olds.push(addr);
                        }
                    }
                    Entry::Vacant(_) => {}
                }
            }
        }
        olds
    }

    #[cfg_attr(feature = "tracing", fastrace::trace(name = "foyer::storage::block::indexer::clear"))]
    pub fn clear(&self) {
        self.shards.iter().for_each(|shard| shard.write().clear());
    }

    pub(super) fn compact_and_count(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| {
                let mut shard = shard.write();
                if shard.capacity() > shard.len().saturating_mul(2) {
                    shard.shrink_to_fit();
                }
                shard.len()
            })
            .sum()
    }

    #[inline(always)]
    fn shard(&self, hash: u64) -> usize {
        // Select the shard from mixed high bits. `IndexerShard` uses the original hash with `ModHasher`, so reusing
        // its low bits here would make every key in a shard start in the same group of hash table buckets.
        const GOLDEN_RATIO: u64 = 0x9e37_79b9_7f4a_7c15;
        let mixed = (hash.wrapping_mul(GOLDEN_RATIO) >> 32) as usize;
        if self.shard_mask != usize::MAX {
            mixed & self.shard_mask
        } else {
            mixed % self.shards.len()
        }
    }

    fn insert_inner(&self, shard: &mut IndexerShard, hash: u64, index: Index) -> Option<EntryAddress> {
        match shard.entry(hash) {
            Entry::Occupied(mut o) => {
                // `>` for updates.
                // '=' for reinsertions.
                if index.sequence() >= o.get().sequence() {
                    self.extract_address(o.insert(index))
                } else {
                    self.extract_address(index)
                }
            }
            Entry::Vacant(v) => {
                v.insert(index);
                None
            }
        }
    }

    fn extract_address(&self, index: Index) -> Option<EntryAddress> {
        match index {
            Index::Address(addr) => Some(addr),
            Index::Tombstone(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn haddr(hash: u64, sequence: Sequence) -> HashedEntryAddress {
        HashedEntryAddress {
            hash,
            address: EntryAddress {
                block: 0,
                offset: 0,
                len: 1,
                sequence,
            },
        }
    }

    #[test]
    fn test_shard_selection_does_not_reuse_hash_low_bits() {
        let indexer = Indexer::new(64);
        let mut low_bits = [0u64; 64];
        for hash in 0..4096 {
            low_bits[indexer.shard(hash)] |= 1 << (hash & 63);
        }
        assert!(low_bits.iter().all(|bits| bits.count_ones() > 16));
    }

    #[test]
    fn test_compact_duplicate_heavy_batch() {
        let indexer = Indexer::new(4);
        indexer.insert_batch((0..10_000).map(|sequence| haddr(sequence % 10, sequence)).collect());

        let capacity_before = indexer
            .shards
            .iter()
            .map(|shard| shard.read().capacity())
            .sum::<usize>();
        assert_eq!(indexer.compact_and_count(), 10);
        let capacity_after = indexer
            .shards
            .iter()
            .map(|shard| shard.read().capacity())
            .sum::<usize>();

        assert!(capacity_after < capacity_before);
        for hash in 0..10 {
            assert_eq!(indexer.get(hash).unwrap().sequence, 9_990 + hash);
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_recovery_batches_match_insert_batch() {
        let expected = Indexer::new(7);
        let actual = Indexer::new(7);
        let mut batch = (0..10_000)
            .map(|i| {
                let mut entry = haddr((i * 31 % 257) as u64, (i * 17 % 101) as Sequence);
                entry.address.block = i as BlockId;
                entry
            })
            .collect_vec();
        batch[126] = haddr(u64::MAX, 42);
        batch[126].address.block = 1;
        batch[127] = haddr(u64::MAX, 42);
        batch[127].address.block = 2;

        expected.insert_batch(batch.clone());
        let mut entries = batch.into_iter();
        let mut chunk = Vec::with_capacity(127);
        loop {
            chunk.extend(entries.by_ref().take(127));
            if chunk.is_empty() {
                break;
            }
            actual
                .insert_recovery_batch(&mut chunk, 8, Spawner::current())
                .await
                .unwrap();
        }

        for hash in 0..257 {
            assert_eq!(actual.get(hash), expected.get(hash));
        }
        assert_eq!(actual.get(u64::MAX), expected.get(u64::MAX));
    }
}
