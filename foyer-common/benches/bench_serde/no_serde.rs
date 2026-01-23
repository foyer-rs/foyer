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

use bytes::Bytes;
use criterion::Criterion;

use super::{decode, encode, K, M};

#[derive(Debug)]
pub struct VecU8Value;

impl VecU8Value {
    pub fn create(size: usize) -> Vec<u8> {
        let mut v = vec![0; size];
        rand::fill(&mut v[..]);
        v
    }
}

#[derive(Debug)]
pub struct BytesValue;

impl BytesValue {
    pub fn create(size: usize) -> Bytes {
        let mut v = vec![0; size];
        rand::fill(&mut v[..]);
        Bytes::from(v)
    }
}

pub fn bench_encode(c: &mut Criterion) {
    for (s, size) in [("64K", 64 * K), ("4M", 4 * M)] {
        c.bench_function(&format!("Vec<u8> value encode - {s}"), |b| {
            encode(b, VecU8Value::create(size), size);
        });
        c.bench_function(&format!("Bytes value encode - {s}"), |b| {
            encode(b, BytesValue::create(size), size);
        });
    }
}

pub fn bench_decode(c: &mut Criterion) {
    for (s, size) in [("64K", 64 * K), ("4M", 4 * M)] {
        c.bench_function(&format!("Vec<u8> value decode - {s}"), |b| {
            decode(b, VecU8Value::create(size), size);
        });
        c.bench_function(&format!("Bytes value decode - {s}"), |b| {
            decode(b, BytesValue::create(size), size);
        });
    }
}
