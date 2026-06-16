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

use borsh::{BorshDeserialize, BorshSerialize};
use criterion::Criterion;

use crate::{run_encode_decode_bench, Entry};

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
struct BorshEntry {
    id: u64,
    label: String,
    payload: Vec<u8>,
}

impl From<Entry> for BorshEntry {
    fn from(e: Entry) -> Self {
        Self {
            id: e.id,
            label: e.label,
            payload: e.payload,
        }
    }
}

impl From<BorshEntry> for Entry {
    fn from(e: BorshEntry) -> Self {
        Self {
            id: e.id,
            label: e.label,
            payload: e.payload,
        }
    }
}

fn borsh_encode(entry: &Entry, buf: &mut Vec<u8>) {
    let be: BorshEntry = entry.clone().into();
    borsh::to_writer(buf, &be).unwrap();
}

fn borsh_decode(bytes: &[u8]) -> Entry {
    let be: BorshEntry = borsh::from_reader(&mut &bytes[..]).unwrap();
    be.into()
}

pub fn bench(c: &mut Criterion) {
    run_encode_decode_bench(
        c,
        "borsh",
        borsh_encode,
        borsh_decode,
        |payload_size, label_len| Entry::create(payload_size, label_len),
    );
}
