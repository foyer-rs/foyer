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

use std::time::Instant;

use bytes::{Bytes, BytesMut};
use criterion::{Bencher, Criterion};
use foyer_common::code::{Code, StorageValue};

const K: usize = 1 << 10;
const M: usize = 1 << 20;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct VecU8ValueSerde(Vec<u8>);

impl VecU8ValueSerde {
    pub fn new(size: usize) -> Self {
        let mut v = vec![0; size];
        rand::fill(&mut v[..]);
        Self(v)
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct VecU8ValueSerdeBytes(#[serde(with = "serde_bytes")] Vec<u8>);

impl VecU8ValueSerdeBytes {
    pub fn new(size: usize) -> Self {
        let mut v = vec![0; size];
        rand::fill(&mut v[..]);
        Self(v)
    }
}

#[derive(Debug)]
pub struct VecU8Value(Vec<u8>);

impl VecU8Value {
    pub fn new(size: usize) -> Self {
        let mut v = vec![0; size];
        rand::fill(&mut v[..]);
        Self(v)
    }
}

impl Code for VecU8Value {
    fn encode(&self, writer: &mut impl std::io::Write) -> foyer_common::code::CodeResult<()> {
        writer.write_all(&self.0.len().to_le_bytes())?;
        writer.write_all(&self.0)?;
        Ok(())
    }

    #[expect(clippy::uninit_vec)]
    fn decode(reader: &mut impl std::io::Read) -> std::result::Result<Self, foyer_common::code::CodeError>
    where
        Self: Sized,
    {
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf)?;
        let len = u64::from_le_bytes(buf) as usize;
        let mut v = Vec::with_capacity(len);
        unsafe { v.set_len(len) };
        reader.read_exact(&mut v)?;
        Ok(Self(v))
    }

    fn estimated_size(&self) -> usize {
        self.0.len()
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct BytesValueSerde(Bytes);

impl BytesValueSerde {
    pub fn new(size: usize) -> Self {
        let mut v = vec![0; size];
        rand::fill(&mut v[..]);
        Self(Bytes::from(v))
    }
}

#[derive(Debug)]
pub struct BytesValue(Bytes);

impl BytesValue {
    pub fn new(size: usize) -> Self {
        let mut v = vec![0; size];
        rand::fill(&mut v[..]);
        Self(Bytes::from(v))
    }
}

impl Code for BytesValue {
    fn encode(&self, writer: &mut impl std::io::Write) -> foyer_common::code::CodeResult<()> {
        writer.write_all(&self.0.len().to_le_bytes())?;
        writer.write_all(&self.0)?;
        Ok(())
    }

    fn decode(reader: &mut impl std::io::Read) -> std::result::Result<Self, foyer_common::code::CodeError>
    where
        Self: Sized,
    {
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf)?;
        let len = u64::from_le_bytes(buf) as usize;
        let mut v = BytesMut::with_capacity(len);
        unsafe { v.set_len(len) };
        reader.read_exact(&mut v)?;
        Ok(Self(v.freeze()))
    }

    fn estimated_size(&self) -> usize {
        self.0.len()
    }
}

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

pub fn bench_encode(c: &mut Criterion) {
    for (s, size) in [("64K", 64 * K), ("4M", 4 * M)] {
        c.bench_function(&format!("Vec<u8> value encode - {s}"), |b| {
            encode(b, VecU8Value::new(size), size);
        });
        c.bench_function(&format!("Vec<u8> value serde encode - {s}"), |b| {
            encode(b, VecU8ValueSerde::new(size), size);
        });
        c.bench_function(&format!("Vec<u8> value serde_bytes encode - {s}"), |b| {
            encode(b, VecU8ValueSerdeBytes::new(size), size);
        });
        c.bench_function(&format!("Bytes value encode - {s}"), |b| {
            encode(b, BytesValue::new(size), size);
        });
        c.bench_function(&format!("Bytes value serde encode - {s}"), |b| {
            encode(b, BytesValueSerde::new(size), size);
        });
    }
}

pub fn bench_decode(c: &mut Criterion) {
    for (s, size) in [("64K", 64 * K), ("4M", 4 * M)] {
        c.bench_function(&format!("Vec<u8> value decode - {s}"), |b| {
            decode(b, VecU8Value::new(size), size);
        });
        c.bench_function(&format!("Vec<u8> value serde decode - {s}"), |b| {
            decode(b, VecU8ValueSerde::new(size), size);
        });
        c.bench_function(&format!("Vec<u8> value serde_bytes decode - {s}"), |b| {
            decode(b, VecU8ValueSerdeBytes::new(size), size);
        });
        c.bench_function(&format!("Bytes value decode - {s}"), |b| {
            decode(b, BytesValue::new(size), size);
        });
        c.bench_function(&format!("Bytes value serde decode - {s}"), |b| {
            decode(b, BytesValueSerde::new(size), size);
        });
    }
}
