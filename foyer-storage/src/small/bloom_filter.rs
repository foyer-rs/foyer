//  Copyright 2024 foyer Project Authors
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

#![cfg_attr(not(test), expect(dead_code))]

use paste::paste;

macro_rules! bloom_filter {
    ($( {$type:ty, $suffix:ident}, )*) => {
        paste! {
            $(
                /// A [<$type>] bloom filter with N hash hashers.
                #[derive(Debug, Clone, PartialEq, Eq)]
                pub struct [<BloomFilter $suffix>]<const N: usize> {
                    data: [$type; N],
                }

                impl<const N: usize> Default for [<BloomFilter $suffix>]<N> {
                    fn default() -> Self {
                        Self::new()
                    }
                }

                impl<const N: usize> [<BloomFilter $suffix>]<N> {
                    const BYTES: usize = $type::BITS as usize / u8::BITS as usize * N;

                    pub fn new() -> Self {
                        Self {
                            data: [0; N],
                        }
                    }

                    pub fn read(raw: &[u8]) -> Self {
                        let mut data = [0; N];
                        data.copy_from_slice(unsafe { std::slice::from_raw_parts(raw.as_ptr() as *const $type, N) });
                        Self { data }
                    }

                    pub fn write(&self, raw: &mut [u8]) {
                        raw[..Self::BYTES].copy_from_slice(unsafe { std::slice::from_raw_parts(self.data.as_ptr() as *const u8, Self::BYTES) })
                    }

                    pub fn insert(&mut self, hash: u64) {
                        tracing::trace!("[bloom filter]: insert hash {hash}");
                        for i in 0..N {
                            let seed = twang_mix64(i as _);
                            let hash = combine_hashes(hash, seed);
                            let bit = hash as usize % $type::BITS as usize;
                            self.data[i] |= 1 << bit;
                        }
                    }

                    pub fn lookup(&self, hash: u64) -> bool {
                        for i in 0..N {
                            let seed = twang_mix64(i as _);
                            let hash = combine_hashes(hash, seed) as $type;
                            let bit = hash as usize % $type::BITS as usize;
                            if self.data[i] & (1 << bit) == 0 {
                                return false;
                            }
                        }
                        true
                    }

                    pub fn clear(&mut self) {
                        tracing::trace!("[bloom filter]: clear");
                        self.data = [0; N];
                    }
                }
            )*
        }
    };
}

macro_rules! for_all_uint_types {
    ($macro:ident) => {
        $macro! {
            {u8, U8},
            {u16, U16},
            {u32, U32},
            {u64, U64},
            {usize, Usize},
        }
    };
}

for_all_uint_types! { bloom_filter }

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

macro_rules! test_bloom_filter {
    ($( {$type:ty, $suffix:ident}, )*) => {
        #[cfg(test)]
        mod tests {
            use super::*;

            const N: usize = 4;

            paste! {
                $(
                    #[test]
                    fn [<test_bloom_filter_ $suffix:lower _insert_lookup_clear>]() {
                        let mut bf = [<BloomFilter $suffix>]::<N>::new();

                        bf.insert(42);
                        assert!(bf.lookup(42));
                        assert!(!bf.lookup(114514));
                        bf.clear();
                        assert!(!bf.lookup(42));
                    }

                    #[test]
                    fn [<test_bloom_filter_ $suffix:lower _multiple_inserts>]() {
                        let mut bf = [<BloomFilter $suffix>]::<N>::new();
                        bf.insert(1);
                        bf.insert(2);
                        bf.insert(3);
                        assert!(bf.lookup(1));
                        assert!(bf.lookup(2));
                        assert!(bf.lookup(3));
                        assert!(!bf.lookup(4));
                    }

                    #[test]
                    fn [<test_bloom_filter_ $suffix:lower _false_positives>]() {
                        const INSERTS: usize = [<BloomFilter $suffix>]::<N>::BYTES;
                        const LOOKUPS: usize = [<BloomFilter $suffix>]::<N>::BYTES * 100;
                        const THRESHOLD: f64 = 0.1;
                        let mut bf = [<BloomFilter $suffix>]::<N>::new();
                        // Insert a bunch of values
                        for i in 0..INSERTS {
                            bf.insert(i as _);
                            println!("{i}: {:X?}", bf.data);
                        }
                        // Check for false positives
                        let mut false_positives = 0;
                        for i in INSERTS..INSERTS + LOOKUPS {
                            if bf.lookup(i as _) {
                                false_positives += 1;
                            }
                        }
                        let ratio = false_positives as f64 / LOOKUPS as f64;
                        println!("ratio: {ratio}");
                        assert!(
                            ratio < THRESHOLD,
                            "false positive ratio {ratio} > threshold {THRESHOLD}, inserts: {INSERTS}, lookups: {LOOKUPS}"
                        );
                    }

                    #[test]
                    fn [<test_bloom_filter_ $suffix:lower _read_write>]() {
                        let mut buf = [0; [<BloomFilter $suffix>]::<N>::BYTES];
                        let mut bf = [<BloomFilter $suffix>]::<N>::new();
                        bf.insert(42);
                        bf.write(&mut buf);
                        let bf2 = [<BloomFilter $suffix>]::<N>::read(&buf);
                        assert_eq!(bf, bf2);
                    }
                )*
            }

        }

    };
}

for_all_uint_types! { test_bloom_filter }
