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

//  Copyright (c) Meta Platforms, Inc. and affiliates.
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

//! A probabilistic counting data structure that never undercounts items before
//! it hits counter's capacity. It is a table structure with the depth being the
//! number of hashes and the width being the number of unique items. When a key
//! is inserted, each row's hash function is used to generate the index for that
//! row. Then the element's count at that index is incremented. As a result, one
//! key being inserted will increment different indicies in each row. Querying the
//! count returns the minimum values of these elements since some hashes might collide.
//!
//!
//! Users are supposed to synchronize concurrent accesses to the data structure.
//!
//! E.g. insert(1)
//! hash1(1) = 2 -> increment row 1, index 2
//! hash2(1) = 5 -> increment row 2, index 5
//! hash3(1) = 3 -> increment row 3, index 3
//! etc.

use crate::utils::hash::{combine_hashes, twang_mix64};
use paste::paste;

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

macro_rules! cm_sketch {
    ($( {$type:ty, $suffix:ident}, )*) => {
        paste! {
            $(
                pub struct [<CMSketch $suffix>] {
                    width: usize,
                    depth: usize,
                    saturated: usize,

                    /// size = width * depth
                    table: Vec<$type>
                }

                impl [<CMSketch $suffix>] {

                    /// `error`: Tolerable error in count given as a fraction of the total number of inserts. Must be between 0 and 1.
                    /// `probability`: The certainty that the count is with in the error threshold.
                    /// `max_width`: Maximum number of the elements per row in the table. 0 represents there is no limitations.
                    /// `max_depth`: Maximum num of rows. 0 represents there is no limitations.
                    pub fn new(error: f64, probability: f64, max_width: usize, max_depth: usize) -> Self {
                        let width = Self::calculate_width(error, max_width);
                        let depth = Self::calculate_depth(probability, max_depth);

                        Self::new_with_size(width, depth)
                    }

                    pub fn new_with_size(width: usize, depth: usize) -> Self {
                        assert!(width > 0, "Width must be greater than 0, width: {}.", width);
                        assert!(depth > 0, "Depth must be greater than 0, depth: {}.", depth);

                        Self {
                            width,
                            depth,
                            saturated: 0,

                            table: vec![0; width * depth],
                        }
                    }

                    pub fn add(&mut self, key: u64) {
                        for i in 0..self.depth {
                            let index = self.index(i, key);
                            if self.table[index] < self.max_count() {
                                self.table[index] += 1;
                                if self.table[index] == self.max_count() {
                                    self.saturated += 1;
                                }
                            }
                            // println!("(ins) table[{}, {}, {}] = {}", i, index, key, self.table[index]);
                        }
                    }

                    pub fn count(&self, key: u64) -> $type {
                        (0..self.depth)
                            .into_iter()
                            .map(|i| self.table[self.index(i, key)])
                            // .map(|i| {
                            //     let index = self.index(i, key);
                            //     let res = self.table[index];
                            //     println!("(cnt) table[{}, {}, {}] = {}", i, index, key, res);
                            //     res
                            // })
                            .min()
                            .unwrap()
                    }

                    pub fn remove(&mut self, key: u64) {
                        let count = self.count(key);
                        for i in 0..self.depth {
                            let index = self.index(i, key);
                            self.table[index] -= count;
                        }
                    }

                    pub fn clear(&mut self) {
                        self.saturated = 0;
                        self.table.iter_mut().for_each(|c| *c = 0);
                    }

                    pub fn decay(&mut self, decay: f64) {
                        self.table.iter_mut().for_each(|c| *c = (*c as f64 * decay) as $type);
                    }

                    pub fn width(&self) -> usize {
                        self.width
                    }

                    pub fn depth(&self) -> usize {
                        self.depth
                    }

                    pub fn max_count(&self) -> $type {
                        $type::MAX
                    }

                    pub fn saturated(&self) -> usize {
                        self.saturated
                    }

                    /// `max_width == 0` represents there is no limitations.
                    fn calculate_width(error: f64, max_width: usize) -> usize {
                        assert!(error > 0.0 && error < 1.0, "Error should be greater than 0 and less than 1, error: {}.", error);

                        // From "Approximating Data with the Count-Min Data Structure" (Cormode & Muthukrishnan)
                        let width = (2.0 / error).ceil() as usize;
                        if max_width > 0 {
                            std::cmp::min(width, max_width)
                        }else {
                            width
                        }
                    }

                    /// `max_depth == 0` represents there is no limitations.
                    fn calculate_depth(probability: f64, max_depth: usize) -> usize {
                        assert!(probability > 0.0 && probability < 1.0, "Probability should be greater than 0 and less than 1, probability: {}.", probability);

                        // From "Approximating Data with the Count-Min Data Structure" (Cormode & Muthukrishnan)
                        let depth  = (1.0 - probability).log2().abs().ceil() as usize;
                        let depth = std::cmp::max(1, depth);
                        if max_depth > 0 {
                            std::cmp::min(depth, max_depth)
                        } else {
                            depth
                        }
                    }

                    fn index(&self, hash_index: usize, key: u64) -> usize {
                        hash_index as usize * self.width
                            + (combine_hashes(twang_mix64(hash_index as u64), key) as usize % self.width)
                    }
                }
            )*
        }
    };
}

for_all_uint_types! {cm_sketch}

#[cfg(test)]
mod tests {
    use super::*;
    use rand_mt::Mt64;

    macro_rules! test_cm_sketch {
        ($( {$type:ty, $suffix:ident}, )*) => {
            paste! {
                $(
                    #[test]
                    fn [<test_add_ $type>]() {
                        let mut cms = [<CMSketch $suffix>]::new_with_size(100, 3);
                        let mut rng = Mt64::new_unseeded();
                        let mut keys = vec![];

                        for i in 0..10 {
                            keys.push(rng.next_u64());
                            for _ in 0..i {
                                cms.add(keys[i]);
                            }
                        }

                        for i in 0..10 {
                            assert!(
                                cms.count(keys[i]) >= std::cmp::min(i as $type, cms.max_count()),
                                "assert {} >= {} failed",
                                cms.count(keys[i]), std::cmp::min(i as $type, cms.max_count())
                            );
                        }
                    }

                    #[test]
                    fn [<test_remove_ $type>]() {
                        let mut cms = [<CMSketch $suffix>]::new_with_size(100, 3);
                        let mut rng = Mt64::new_unseeded();
                        let mut keys = vec![];

                        for i in 0..10 {
                            keys.push(rng.next_u64());
                            for _ in 0..i {
                                cms.add(keys[i]);
                            }
                        }

                        for i in 0..10 {
                            cms.remove(keys[i]);
                            assert_eq!(cms.count(keys[i]), 0);
                        }
                    }

                    #[test]
                    fn [<test_clear_ $type>]() {
                        let mut cms = [<CMSketch $suffix>]::new_with_size(100, 3);
                        let mut rng = Mt64::new_unseeded();
                        let mut keys = vec![];

                        for i in 0..10 {
                            keys.push(rng.next_u64());
                            for _ in 0..i {
                                cms.add(keys[i]);
                            }
                        }

                        cms.clear();
                        for i in 0..10 {
                            assert_eq!(cms.count(keys[i]), 0);
                        }
                    }

                    #[test]
                    fn [<test_collisions_ $type>]() {
                        let mut cms = [<CMSketch $suffix>]::new_with_size(40, 5);
                        let mut rng = Mt64::new_unseeded();
                        let mut keys = vec![];
                        let mut sum = 0;

                        // Try inserting more keys than cms table width
                        for i in 0..55 {
                            keys.push(rng.next_u64());
                            for _ in 0..i {
                                cms.add(keys[i]);
                            }
                            sum += i;
                        }

                        let error = sum as f64 * 0.05;
                        for i in 0..10 {
                            assert!(cms.count(keys[i]) >= i as $type);
                            assert!(i as f64 + error >= cms.count(keys[i]) as f64);
                        }
                    }

                    #[test]
                    fn [<test_probability_constructor_ $type>]() {
                        let cms = [<CMSketch $suffix>]::new(0.01, 0.95, 0, 0);
                        assert_eq!(cms.width(), 200);
                        assert_eq!(cms.depth(), 5);
                    }


                    #[test]
                    #[should_panic]
                    fn [<test_invalid_args_ $type>]() {
                        [<CMSketch $suffix>]::new(0f64, 0f64, 0, 0);
                    }

                    #[test]
                    fn [<test_decay_ $type>]() {
                        let mut cms = [<CMSketch $suffix>]::new_with_size(100, 3);
                        let mut rng = Mt64::new_unseeded();
                        let mut keys = vec![];

                        for i in 0..1000 {
                            keys.push(rng.next_u64());
                            for _ in 0..i {
                                cms.add(keys[i]);
                            }
                        }

                        for i in 0..1000 {
                            assert!(
                                cms.count(keys[i]) >= std::cmp::min(i as $type, cms.max_count()),
                                "assert {} >= {} failed",
                                cms.count(keys[i]), std::cmp::min(i as $type, cms.max_count())
                            );
                        }

                        const FACTOR: f64 = 0.5;
                        cms.decay(FACTOR);

                        for i in 0..1000 {
                            assert!(cms.count(keys[i]) >= (std::cmp::min(i as $type, cms.max_count()) as f64 * FACTOR).floor() as $type);
                        }
                    }

                    #[test]
                    fn [<test_overflow_ $type>]() {
                        let mut cms = [<CMSketch $suffix>]::new_with_size(10, 3);
                        let mut rng = Mt64::new_unseeded();
                        let key = rng.next_u64();
                        let max = cms.max_count();

                        // Skip test if max is too large.
                        if (max as usize) < (u32::MAX as usize) {
                            for _ in 0..max {
                                cms.add(key);
                            }

                            assert_eq!(cms.count(key), max);
                            assert_eq!(cms.saturated(), 3);
                        }
                    }
                )*
            }
        };
    }

    for_all_uint_types! {test_cm_sketch}
}
