// Copyright 2025 foyer Project Authors
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

#![expect(missing_docs)]

use std::time::Instant;

use criterion::Bencher;
use foyer_common::code::StorageValue;

#[cfg(feature = "serde")]
mod serde;
#[cfg(feature = "serde")]
criterion::criterion_group!(benches, serde::bench_encode, serde::bench_decode);
#[cfg(feature = "serde")]
criterion::criterion_main!(benches);

#[cfg(not(feature = "serde"))]
mod no_serde;
#[cfg(not(feature = "serde"))]
criterion::criterion_group!(benches, no_serde::bench_encode, no_serde::bench_decode);
#[cfg(not(feature = "serde"))]
criterion::criterion_main!(benches);

const K: usize = 1 << 10;
const M: usize = 1 << 20;

fn encode<V: StorageValue>(b: &mut Bencher, v: V, size: usize) {
    b.iter_custom(|iters| {
        let mut buf = vec![0; size * 2];
        let start = Instant::now();
        for _ in 0..iters {
            v.encode(&mut &mut buf[..]).unwrap();
        }
        start.elapsed()
    });
}

fn decode<V: StorageValue>(b: &mut Bencher, v: V, size: usize) {
    b.iter_custom(|iters| {
        let mut buf = vec![0; size * 2];
        v.encode(&mut &mut buf[..]).unwrap();
        let start = Instant::now();
        for _ in 0..iters {
            V::decode(&mut &buf[..]).unwrap();
        }
        start.elapsed()
    });
}
