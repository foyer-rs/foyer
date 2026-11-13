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

//! Versioned, checksummed buckets with FIFO records and no per-entry padding.

use bytes::{Buf, BufMut};
use foyer_common::{
    code::{StorageKey, StorageValue},
    error::{Error, ErrorKind, Result},
};

use crate::{
    Compression,
    io::bytes::IoSliceMut,
    serde::{Checksummer, EntrySerializer, KvInfo, TrackedWriter},
};

const MAGIC: u64 = 0x464f_5945_5253_0001;
pub const HEADER: usize = 40;
pub const RECORD: usize = 20;

#[derive(Debug)]
pub struct Entry {
    pub hash: u64,
    pub key_len: usize,
    pub value_len: usize,
    pub compression: Compression,
    pub data: Vec<u8>,
}

impl Entry {
    pub fn encode<K: StorageKey, V: StorageValue>(
        hash: u64,
        key: &K,
        value: &V,
        compression: Compression,
        limit: usize,
    ) -> Result<Self> {
        let mut data = vec![0; limit - RECORD];
        let info = if compression == Compression::Lz4 {
            // Code implementations (notably serde) may write one byte at a time.
            // Finish one buffered frame instead of flushing a block per write.
            let mut writer = TrackedWriter::new(&mut data[..]);
            let mut encoder = lz4::EncoderBuilder::new()
                .checksum(lz4::ContentChecksum::NoChecksum)
                .auto_flush(false)
                .build(&mut writer)
                .map_err(Error::io_error)?;
            value.encode(&mut encoder)?;
            encoder.finish().1.map_err(Error::io_error)?;
            let value_len = writer.written();
            let mut writer = TrackedWriter::new(&mut data[value_len..]);
            key.encode(&mut writer)?;
            KvInfo {
                key_len: writer.written(),
                value_len,
            }
        } else {
            EntrySerializer::serialize(key, value, compression, &mut data[..])?
        };
        data.truncate(info.key_len + info.value_len);
        Ok(Self {
            hash,
            key_len: info.key_len,
            value_len: info.value_len,
            compression,
            data,
        })
    }

    pub fn len(&self) -> usize {
        RECORD + self.data.len()
    }
}

pub fn bloom(hash: u64) -> u64 {
    // Mix before selecting bits so bucket selection does not fix the bloom bits.
    let hash = twox_hash::XxHash64::oneshot(1, &hash.to_le_bytes());
    (1 << (hash & 63)) | (1 << ((hash >> 21) & 63)) | (1 << ((hash >> 42) & 63))
}

pub fn filter(entries: &[Entry]) -> u64 {
    entries.iter().fold(0, |bits, entry| bits | bloom(entry.hash))
}

fn corrupt() -> Error {
    Error::new(ErrorKind::Parse, "invalid small-object bucket")
}

pub fn decode(buf: &[u8], id: usize, buckets: usize) -> Result<Vec<Entry>> {
    if buf.len() < HEADER {
        return Err(corrupt());
    }
    if buf.iter().all(|b| *b == 0) {
        return Ok(vec![]);
    }
    let mut header = &buf[..HEADER];
    let checksum = header.get_u64();
    if checksum != Checksummer::checksum64(&buf[8..]) {
        return Err(Error::new(
            ErrorKind::ChecksumMismatch,
            "small-object bucket checksum mismatch",
        ));
    }
    if header.get_u64() != MAGIC || header.get_u64() != id as u64 || header.get_u64() != buckets as u64 {
        return Err(corrupt());
    }
    let used = header.get_u32() as usize;
    let count = header.get_u32() as usize;
    if used < HEADER || used > buf.len() || count > (used - HEADER) / RECORD {
        return Err(corrupt());
    }
    let mut data = &buf[HEADER..used];
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        if data.len() < RECORD {
            return Err(corrupt());
        }
        let hash = data.get_u64();
        let key_len = data.get_u32() as usize;
        let value_len = data.get_u32() as usize;
        let compression = data.get_u32();
        if compression > u8::MAX as u32 {
            return Err(corrupt());
        }
        let compression = Compression::try_from(compression as u8)?;
        let len = key_len.checked_add(value_len).ok_or_else(corrupt)?;
        if len > data.len() || hash % buckets as u64 != id as u64 {
            return Err(corrupt());
        }
        entries.push(Entry {
            hash,
            key_len,
            value_len,
            compression,
            data: data[..len].to_vec(),
        });
        data.advance(len);
    }
    if !data.is_empty() {
        return Err(corrupt());
    }
    Ok(entries)
}

pub fn encode(entries: &[Entry], size: usize, id: usize, buckets: usize) -> IoSliceMut {
    let mut buf = IoSliceMut::new(size);
    buf.fill(0);
    let used = HEADER + entries.iter().map(Entry::len).sum::<usize>();
    assert!(used <= size);
    let mut out = &mut buf[8..];
    out.put_u64(MAGIC);
    out.put_u64(id as u64);
    out.put_u64(buckets as u64);
    out.put_u32(used as u32);
    out.put_u32(entries.len() as u32);
    for entry in entries {
        out.put_u64(entry.hash);
        out.put_u32(entry.key_len as u32);
        out.put_u32(entry.value_len as u32);
        out.put_u32(entry.compression.to_u8() as u32);
        out.put_slice(&entry.data);
    }
    let checksum = Checksummer::checksum64(&buf[8..]);
    (&mut buf[..8]).put_u64(checksum);
    buf
}

pub fn insert(entries: &mut Vec<Entry>, entry: Entry, size: usize) {
    entries.retain(|old| old.hash != entry.hash);
    let mut used = HEADER + entries.iter().map(Entry::len).sum::<usize>() + entry.len();
    let mut victims = 0;
    while used > size {
        used -= entries[victims].len();
        victims += 1;
    }
    entries.drain(..victims);
    entries.push(entry);
}
