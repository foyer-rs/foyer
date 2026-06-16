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

#![expect(missing_docs)]

use std::time::Instant;

use criterion::{Bencher, Criterion, criterion_group, criterion_main};

#[cfg(feature = "borsh")]
mod borsh;
mod manual;
#[cfg(feature = "serde")]
mod postcard;

/// A representative cache entry: an id, a label, and a payload.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Entry {
    pub id: u64,
    pub label: String,
    pub payload: Vec<u8>,
}

impl Entry {
    pub fn create(payload_size: usize, label_len: usize) -> Self {
        let mut payload = vec![0; payload_size];
        rand::fill(&mut payload[..]);
        let label = "x".repeat(label_len);
        Self {
            id: rand::random(),
            label,
            payload,
        }
    }
}

const K: usize = 1 << 10;
const M: usize = 1 << 20;

/// Sizes to benchmark: (label, payload_size, label_len).
const SIZES: &[(&str, usize, usize)] = &[("64KiB", 64 * K, 8), ("4MiB", 4 * M, 8), ("64MiB", 64 * M, 8)];

/// Helper: measure encode throughput for a given value.
fn bench_encode<V>(b: &mut Bencher, encode: fn(&V, &mut Vec<u8>), v: V, cap: usize)
where
    V: Clone,
{
    b.iter_custom(|iters| {
        let mut buf = Vec::with_capacity(cap);
        let start = Instant::now();
        for _ in 0..iters {
            buf.clear();
            encode(&v, &mut buf);
        }
        start.elapsed()
    });
}

/// Helper: measure decode throughput for a given encoded buffer.
fn bench_decode<V: Clone>(b: &mut Bencher, encode: fn(&V, &mut Vec<u8>), decode: fn(&[u8]) -> V, v: V, cap: usize) {
    let mut buf = Vec::with_capacity(cap);
    encode(&v, &mut buf);
    b.iter_custom(|iters| {
        let start = Instant::now();
        for _ in 0..iters {
            std::hint::black_box(decode(&buf));
        }
        start.elapsed()
    });
}

/// Helper: get the encoded size for a given value.
fn encoded_size<V>(encode: fn(&V, &mut Vec<u8>), v: &V) -> usize {
    let mut buf = Vec::new();
    encode(v, &mut buf);
    buf.len()
}

/// Run encode + decode benchmarks for a given backend, and print encoded sizes.
pub fn run_encode_decode_bench<V: Clone + 'static>(
    c: &mut Criterion,
    group: &str,
    encode: fn(&V, &mut Vec<u8>),
    decode: fn(&[u8]) -> V,
    create: fn(usize, usize) -> V,
) {
    {
        let mut grp = c.benchmark_group(format!("{group}/encode"));
        for (label, size, label_len) in SIZES {
            let entry = create(*size, *label_len);
            let cap = size + label_len + 4096;
            grp.bench_function(*label, |b| {
                bench_encode(b, encode, entry.clone(), cap);
            });
        }
        grp.finish();
    }

    {
        let mut grp = c.benchmark_group(format!("{group}/decode"));
        for (label, size, label_len) in SIZES {
            let entry = create(*size, *label_len);
            let cap = size + label_len + 4096;
            grp.bench_function(*label, |b| {
                bench_decode(b, encode, decode, entry.clone(), cap);
            });
        }
        grp.finish();
    }

    // Print encoded sizes (appears in benchmark output).
    println!("--- {group} encoded sizes ---");
    for (label, size, label_len) in SIZES {
        let entry = create(*size, *label_len);
        let enc_size = encoded_size(encode, &entry);
        let raw_size = std::mem::size_of::<u64>() + *label_len + *size;
        println!(
            "[{group}] {label}: encoded={enc_size}B raw={raw_size}B ratio={:.2}%",
            (enc_size as f64 / raw_size as f64) * 100.0
        );
    }
}

fn bench_all(c: &mut Criterion) {
    #[cfg(feature = "borsh")]
    borsh::bench(c);
    #[cfg(feature = "serde")]
    postcard::bench(c);
    manual::bench(c);
}

criterion_group!(benches, bench_all);
criterion_main!(benches);
