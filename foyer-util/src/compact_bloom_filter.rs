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

use std::{cell::UnsafeCell, sync::Arc};

use bitvec::prelude::*;
use foyer_common::strict_assert;
use itertools::Itertools;

/// Reduce two 64-bit hashes into one.
///
/// Ported from CacheLib, which uses the `Hash128to64` function from Google's city hash.
#[inline(always)]
fn combine_hashes(upper: u64, lower: u64) -> u64 {
    const MUL: u64 = 0x9ddfea08eb382d69;

    let mut a = (lower ^ upper).wrapping_mul(MUL);
    a ^= a >> 47;
    let mut b = (upper ^ a).wrapping_mul(MUL);
    b ^= b >> 47;
    b = b.wrapping_mul(MUL);
    b
}

#[inline(always)]
fn twang_mix64(val: u64) -> u64 {
    let mut val = (!val).wrapping_add(val << 21); // val *= (1 << 21); val -= 1
    val = val ^ (val >> 24);
    val = val.wrapping_add(val << 3).wrapping_add(val << 8); // val *= 1 + (1 << 3) + (1 << 8)
    val = val ^ (val >> 14);
    val = val.wrapping_add(val << 2).wrapping_add(val << 4); // va; *= 1 + (1 << 2) + (1 << 4)
    val = val ^ (val >> 28);
    val = val.wrapping_add(val << 31); // val *= 1 + (1 << 31)
    val
}

/// [`CompactBloomFilter`] is composed of a series of contiguous bloom filters with each size is smaller than cache
/// line.
pub struct CompactBloomFilter {
    /// Bloom filter data.
    data: BitVec,
    /// Seeds for each hash function.
    seeds: Box<[u64]>,
    /// Count of hash functions apply to each input hash.
    hashes: usize,
    /// Bit count of each hash function result.
    bits: usize,
    /// Count of bloom filters
    filters: usize,
}

impl CompactBloomFilter {
    /// Create a new compact bloom filter.
    pub fn new(filters: usize, hashes: usize, bits: usize) -> Self {
        let data = bitvec![0; filters * hashes * bits];
        let seeds = (0..hashes)
            .map(|i| twang_mix64(i as _))
            .collect_vec()
            .into_boxed_slice();
        Self {
            data,
            seeds,
            hashes,
            bits,
            filters,
        }
    }

    /// Insert the given hash `hash` into the `idx` filter.
    pub fn insert(&mut self, idx: usize, hash: u64) {
        strict_assert!(idx < self.filters);
        for (i, seed) in self.seeds.iter().enumerate() {
            let bit =
                (idx * self.hashes * self.bits) + i * self.bits + (combine_hashes(hash, *seed) as usize % self.bits);
            self.data.set(bit, true);
        }
    }

    /// Lookup for if the `idx` filter may contains the given key `hash`.
    pub fn lookup(&self, idx: usize, hash: u64) -> bool {
        strict_assert!(idx < self.filters);
        for (i, seed) in self.seeds.iter().enumerate() {
            let bit =
                (idx * self.hashes * self.bits) + i * self.bits + (combine_hashes(hash, *seed) as usize % self.bits);
            if unsafe { !*self.data.get_unchecked(bit) } {
                return false;
            }
        }
        true
    }

    /// Clear the `idx` filter.
    pub fn clear(&mut self, idx: usize) {
        strict_assert!(idx < self.filters);
        let start = idx * self.hashes * self.bits;
        let end = (idx + 1) * self.hashes * self.bits;
        self.data.as_mut_bitslice()[start..end].fill(false);
    }

    /// Reset the all filters.
    pub fn reset(&mut self) {
        self.data.fill(false);
    }

    /// Create a new compact bloom filter and return the shards.
    ///
    /// See [`CompactBloomFilterShard`].
    pub fn shards(filters: usize, hashes: usize, bits: usize) -> Vec<CompactBloomFilterShard> {
        #[expect(clippy::arc_with_non_send_sync)]
        let filter = Arc::new(UnsafeCell::new(Self::new(filters, hashes, bits)));
        (0..filters)
            .map(|idx| CompactBloomFilterShard {
                inner: filter.clone(),
                idx,
            })
            .collect_vec()
    }
}

/// A shard of the compact bloom filter.
///
/// [`CompactBloomFilterShard`] takes the partial ownership of the compact bloom filter.
///
/// Operations from different shards don't affect each other.
#[derive(Debug)]
pub struct CompactBloomFilterShard {
    inner: Arc<UnsafeCell<CompactBloomFilter>>,
    idx: usize,
}

impl CompactBloomFilterShard {
    /// Insert the given hash `hash` the filter.
    pub fn insert(&mut self, hash: u64) {
        let inner = unsafe { &mut *self.inner.get() };
        inner.insert(self.idx, hash);
    }

    /// Lookup for if the filter may contains the given key `hash`.
    pub fn lookup(&self, hash: u64) -> bool {
        let inner = unsafe { &mut *self.inner.get() };
        inner.lookup(self.idx, hash)
    }

    /// Clear the filter.
    pub fn clear(&mut self) {
        let inner = unsafe { &mut *self.inner.get() };
        inner.clear(self.idx)
    }
}

unsafe impl Send for CompactBloomFilterShard {}
unsafe impl Sync for CompactBloomFilterShard {}

#[cfg(test)]
mod tests {
    use super::*;

    fn is_send_sync_static<T: Send + Sync + 'static>() {}

    #[test]
    fn ensure_send_sync_static() {
        is_send_sync_static::<CompactBloomFilter>();
        is_send_sync_static::<CompactBloomFilterShard>();
    }

    #[test]
    fn test_compact_bloom_filter() {
        let mut shards = CompactBloomFilter::shards(10, 4, 8);
        shards[0].insert(42);
        shards[9].insert(42);
        for (i, shard) in shards.iter().enumerate() {
            let res = shard.lookup(42);
            if i == 0 || i == 9 {
                assert!(res);
            } else {
                assert!(!res);
            }
        }
        shards[0].clear();
        shards[9].clear();
        for shard in shards.iter() {
            assert!(!shard.lookup(42));
        }
    }
}
