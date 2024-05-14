//  Copyright 2024 Foyer Project Authors
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

use std::{fmt::Debug, hash::Hasher};
use twox_hash::XxHash64;

use allocator_api2::alloc::Allocator;
use bytes::{Buf, BufMut};
use foyer_common::code::{StorageKey, StorageValue};

use crate::{
    compress::Compression,
    device::allocator::WritableVecA,
    error::{Error, Result},
    Sequence,
};

pub fn checksum(buf: &[u8]) -> u64 {
    let mut hasher = XxHash64::with_seed(0);
    hasher.write(buf);
    hasher.finish()
}

const ENTRY_MAGIC: u32 = 0x97_03_27_00;
const ENTRY_MAGIC_MASK: u32 = 0xFF_FF_FF_00;

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

    pub fn entry_len(&self) -> usize {
        Self::serialized_len() + self.key_len as usize + self.value_len as usize
    }

    pub fn write(&self, mut buf: &mut [u8]) {
        buf.put_u32(self.key_len);
        buf.put_u32(self.value_len);
        buf.put_u64(self.hash);
        buf.put_u64(self.sequence);
        buf.put_u64(self.checksum);

        let v = ENTRY_MAGIC | self.compression.to_u8() as u32;
        buf.put_u32(v);
    }

    pub fn read(mut buf: &[u8]) -> Result<Self> {
        let key_len = buf.get_u32();
        let value_len = buf.get_u32();
        let hash = buf.get_u64();
        let sequence = buf.get_u64();
        let checksum = buf.get_u64();

        let v = buf.get_u32();
        let compression = Compression::try_from(v as u8)?;

        let this = Self {
            key_len,
            value_len,
            hash,
            sequence,
            compression,
            checksum,
        };

        tracing::trace!("{this:#?}");

        let magic = v & ENTRY_MAGIC_MASK;
        if magic != ENTRY_MAGIC {
            return Err(Error::MagicMismatch {
                expected: ENTRY_MAGIC,
                get: magic,
            });
        }

        Ok(this)
    }
}

#[derive(Debug)]
pub struct EntrySerializer;

impl EntrySerializer {
    pub fn serialize<'a, K, V, A>(
        key: &'a K,
        value: &'a V,
        hash: u64,
        sequence: &'a Sequence,
        compression: &'a Compression,
        mut buffer: WritableVecA<'a, u8, A>,
    ) -> Result<()>
    where
        K: StorageKey,
        V: StorageValue,
        A: Allocator,
    {
        let mut cursor = buffer.len();

        // reserve space for header, header will be filled after the serialized len is known
        cursor += EntryHeader::serialized_len();
        buffer.reserve(EntryHeader::serialized_len());
        unsafe { buffer.set_len(cursor) };

        // serialize value
        match compression {
            Compression::None => {
                bincode::serialize_into(&mut buffer, &value).map_err(Error::from)?;
            }
            Compression::Zstd => {
                let encoder = zstd::Encoder::new(&mut buffer, 0).map_err(Error::from)?.auto_finish();
                bincode::serialize_into(encoder, &value).map_err(Error::from)?;
            }

            Compression::Lz4 => {
                let encoder = lz4::EncoderBuilder::new()
                    .checksum(lz4::ContentChecksum::NoChecksum)
                    .auto_flush(true)
                    .build(&mut buffer)
                    .map_err(Error::from)?;
                bincode::serialize_into(encoder, &value).map_err(Error::from)?;
            }
        }

        let value_len = buffer.len() - cursor;
        cursor = buffer.len();

        // serialize key
        bincode::serialize_into(WritableVecA(&mut buffer), &key).map_err(Error::from)?;
        let key_len = buffer.len() - cursor;
        cursor = buffer.len();

        // calculate checksum
        cursor -= value_len + key_len;
        let checksum = checksum(&buffer[cursor..cursor + value_len + key_len]);

        // serialize entry header
        cursor -= EntryHeader::serialized_len();
        let header = EntryHeader {
            key_len: key_len as u32,
            value_len: value_len as u32,
            hash,
            sequence: *sequence,
            compression: *compression,
            checksum,
        };
        header.write(&mut buffer[cursor..cursor + EntryHeader::serialized_len()]);

        Ok(())
    }
}

#[derive(Debug)]
pub struct EntryDeserializer;

impl EntryDeserializer {
    pub fn deserialize<K, V>(buffer: &[u8]) -> Result<(EntryHeader, K, V)>
    where
        K: StorageKey,
        V: StorageValue,
    {
        // deserialize entry header
        let header = EntryHeader::read(buffer).map_err(Error::from)?;

        // deserialize value
        let mut offset = EntryHeader::serialized_len();
        let buf = &buffer[offset..offset + header.value_len as usize];
        offset += header.value_len as usize;
        let value = Self::deserialize_value(buf, header.compression)?;

        // deserialize key
        let buf = &buffer[offset..offset + header.key_len as usize];
        let key = Self::deserialize_key(buf)?;
        offset += header.key_len as usize;

        let checksum = checksum(&buffer[EntryHeader::serialized_len()..offset]);
        if checksum != header.checksum {
            return Err(Error::ChecksumMismatch {
                expected: header.checksum,
                get: checksum,
            });
        }

        Ok((header, key, value))
    }

    pub fn deserialize_header(buf: &[u8]) -> Option<EntryHeader> {
        if buf.len() < EntryHeader::serialized_len() {
            return None;
        }
        EntryHeader::read(buf).ok()
    }

    pub fn deserialize_key<K>(buf: &[u8]) -> Result<K>
    where
        K: StorageKey,
    {
        bincode::deserialize_from(buf).map_err(Error::from)
    }

    pub fn deserialize_value<V>(buf: &[u8], compression: Compression) -> Result<V>
    where
        V: StorageValue,
    {
        match compression {
            Compression::None => bincode::deserialize_from(buf).map_err(Error::from),
            Compression::Zstd => {
                let decoder = zstd::Decoder::new(buf).map_err(Error::from)?;
                bincode::deserialize_from(decoder).map_err(Error::from)
            }
            Compression::Lz4 => {
                let decoder = lz4::Decoder::new(buf).map_err(Error::from)?;
                bincode::deserialize_from(decoder).map_err(Error::from)
            }
        }
    }
}
