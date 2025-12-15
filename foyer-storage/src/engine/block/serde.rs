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

use std::sync::atomic::AtomicU64;

use bytes::{Buf, BufMut};
use foyer_common::error::{Error, ErrorKind, Result};

use crate::compress::Compression;

const ENTRY_MAGIC: u32 = 0x97_03_27_00;
const ENTRY_MAGIC_MASK: u32 = 0xFF_FF_FF_00;

pub type Sequence = u64;
pub type AtomicSequence = AtomicU64;

#[derive(Debug, PartialEq, Eq)]
pub struct EntryHeader {
    pub key_len: u32,
    pub value_len: u32,
    pub hash: u64,
    pub sequence: Sequence,
    pub checksum: u64,
    pub compression: Compression,
}

impl EntryHeader {
    pub const fn serialized_len() -> usize {
        4 + 4 + 8 + 8 + 8 + 4 /* magic & compression */
    }

    pub fn write(&self, mut buf: impl BufMut) {
        buf.put_u32(self.key_len);
        buf.put_u32(self.value_len);
        buf.put_u64(self.hash);
        buf.put_u64(self.sequence);
        buf.put_u64(self.checksum);

        let v = ENTRY_MAGIC | self.compression.to_u8() as u32;
        buf.put_u32(v);
    }

    pub fn read(mut buf: impl Buf) -> Result<Self> {
        let key_len = buf.get_u32();
        let value_len = buf.get_u32();
        let hash = buf.get_u64();
        let sequence = buf.get_u64();
        let checksum = buf.get_u64();

        let v = buf.get_u32();

        tracing::trace!("read entry header, key len: {key_len}, value_len: {value_len}, hash: {hash}, sequence: {sequence}, checksum: {checksum}, extra: {v}");

        let magic = v & ENTRY_MAGIC_MASK;
        if magic != ENTRY_MAGIC {
            return Err(Error::new(ErrorKind::MagicMismatch, "entry header magic mismatch")
                .with_context("expected", ENTRY_MAGIC)
                .with_context("get", magic));
        }
        let compression = Compression::try_from(v as u8)?;

        Ok(Self {
            key_len,
            value_len,
            hash,
            sequence,
            checksum,
            compression,
        })
    }
}
