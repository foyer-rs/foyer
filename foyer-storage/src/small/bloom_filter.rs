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

/// A u64 bloom filter with 4 hash hashers.
#[derive(Debug, Default)]
pub struct BloomFilterU64 {
    value: u64,
}

impl From<u64> for BloomFilterU64 {
    fn from(value: u64) -> Self {
        Self { value }
    }
}

impl BloomFilterU64 {
    const HASHERS: usize = 4;

    pub fn insert(&mut self, hash: u64) {
        for i in 0..Self::HASHERS {
            let seed = twang_mix64(i as _);
            let hash = combine_hashes(hash, seed);
            let hash = (hash & u16::MAX as u64) << (i * u16::BITS as usize);
            self.value |= hash;
        }
    }

    pub fn lookup(&self, hash: u64) -> bool {
        for i in 0..Self::HASHERS {
            let seed = twang_mix64(i as _);
            let hash = combine_hashes(hash, seed);
            let hash = (hash & u16::MAX as u64) << (i * u16::BITS as usize);
            if self.value & hash == 0 {
                return false;
            }
        }
        true
    }

    pub fn clear(&mut self) {
        self.value = 0
    }

    pub fn value(&self) -> u64 {
        self.value
    }
}

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
