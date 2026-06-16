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

use criterion::Criterion;

use crate::{Entry, run_encode_decode_bench};

/// Manual little-endian encoding: u64 (8 bytes LE) + label_len (u64 LE) + label bytes + payload_len (u64 LE) + payload
/// bytes.
fn manual_encode(entry: &Entry, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&entry.id.to_le_bytes());
    buf.extend_from_slice(&(entry.label.len() as u64).to_le_bytes());
    buf.extend_from_slice(entry.label.as_bytes());
    buf.extend_from_slice(&(entry.payload.len() as u64).to_le_bytes());
    buf.extend_from_slice(&entry.payload);
}

fn manual_decode(bytes: &[u8]) -> Entry {
    let (id_bytes, rest) = bytes.split_at(8);
    let id = u64::from_le_bytes(id_bytes.try_into().unwrap());

    let (label_len_bytes, rest) = rest.split_at(8);
    let label_len = u64::from_le_bytes(label_len_bytes.try_into().unwrap()) as usize;

    let (label_bytes, rest) = rest.split_at(label_len);
    let label = String::from_utf8(label_bytes.to_vec()).unwrap();

    let (payload_len_bytes, rest) = rest.split_at(8);
    let payload_len = u64::from_le_bytes(payload_len_bytes.try_into().unwrap()) as usize;

    let (payload_bytes, _) = rest.split_at(payload_len);
    let payload = payload_bytes.to_vec();

    Entry { id, label, payload }
}

pub fn bench(c: &mut Criterion) {
    run_encode_decode_bench(c, "manual", manual_encode, manual_decode, Entry::create);
}
