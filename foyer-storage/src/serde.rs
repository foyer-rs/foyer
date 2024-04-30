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

use anyhow::anyhow;
use std::{fmt::Debug, hash::Hasher};
use twox_hash::XxHash64;

use allocator_api2::alloc::Allocator;
use bytes::{Buf, BufMut};
use foyer_common::code::{StorageKey, StorageValue};

use crate::{
    catalog::Sequence,
    device::{allocator::WritableVecA, DeviceError},
    Compression,
};

#[derive(thiserror::Error, Debug)]
pub enum SerdeError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("device error: {0}")]
    Device(#[from] DeviceError),
    #[error("bincode error: {0}")]
    Bincode(#[from] bincode::Error),
    #[error("other error: {0}")]
    Other(#[from] anyhow::Error),
}

pub type SerdeResult<T> = core::result::Result<T, SerdeError>;

pub fn checksum(buf: &[u8]) -> u64 {
    let mut hasher = XxHash64::with_seed(0);
    hasher.write(buf);
    hasher.finish()
}

const ENTRY_MAGIC: u32 = 0x97_03_27_00;
const ENTRY_MAGIC_MASK: u32 = 0xFF_FF_FF_00;

#[derive(Debug)]
pub struct EntryHeader {
    pub key_len: u32,
    pub value_len: u32,
    pub sequence: Sequence,
    pub checksum: u64,
    pub compression: Compression,
}

impl EntryHeader {
    pub const fn serialized_len() -> usize {
        4 + 4 + 8 + 8 + 4 /* magic & compression */
    }

    pub fn write(&self, mut buf: &mut [u8]) {
        buf.put_u32(self.key_len);
        buf.put_u32(self.value_len);
        buf.put_u64(self.sequence);
        buf.put_u64(self.checksum);

        let v = ENTRY_MAGIC | self.compression.to_u8() as u32;
        buf.put_u32(v);
    }

    pub fn read(mut buf: &[u8]) -> SerdeResult<Self> {
        let key_len = buf.get_u32();
        let value_len = buf.get_u32();
        let sequence = buf.get_u64();
        let checksum = buf.get_u64();

        let v = buf.get_u32();
        let magic = v & ENTRY_MAGIC_MASK;
        if magic != ENTRY_MAGIC {
            return Err(anyhow!("magic mismatch, expected: {}, got: {}", ENTRY_MAGIC, magic).into());
        }
        let compression = Compression::try_from(v as u8)?;

        Ok(Self {
            key_len,
            value_len,
            sequence,
            compression,
            checksum,
        })
    }
}

#[derive(Debug)]
pub struct EntrySerializer;

impl EntrySerializer {
    pub fn serialize<'a, K, V, A>(
        key: &'a K,
        value: &'a V,
        sequence: &'a Sequence,
        compression: &'a Compression,
        mut buffer: WritableVecA<'a, u8, A>,
    ) -> SerdeResult<()>
    where
        K: StorageKey,
        V: StorageValue,
        A: Allocator,
    {
        let mut cursor = buffer.len();

        // reserve space for header, header will be filled after the serialized len is known
        cursor += EntryHeader::serialized_len();
        unsafe { buffer.set_len(cursor) };

        // serialize value
        match compression {
            Compression::None => {
                bincode::serialize_into(&mut buffer, &value).map_err(SerdeError::from)?;
            }
            Compression::Zstd => {
                let encoder = zstd::Encoder::new(&mut buffer, 0)
                    .map_err(SerdeError::from)?
                    .auto_finish();
                bincode::serialize_into(encoder, &value).map_err(SerdeError::from)?;
            }

            Compression::Lz4 => {
                let encoder = lz4::EncoderBuilder::new()
                    .checksum(lz4::ContentChecksum::NoChecksum)
                    .auto_flush(true)
                    .build(&mut buffer)
                    .map_err(SerdeError::from)?;
                bincode::serialize_into(encoder, &value).map_err(SerdeError::from)?;
            }
        }

        let value_len = buffer.len() - cursor;
        cursor = buffer.len();

        // serialize key
        bincode::serialize_into(WritableVecA(&mut buffer), &key).map_err(SerdeError::from)?;
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
    pub fn deserialize<K, V>(buffer: &[u8]) -> SerdeResult<(EntryHeader, K, V)>
    where
        K: StorageKey,
        V: StorageValue,
    {
        // deserialize entry header
        let header = EntryHeader::read(buffer).map_err(SerdeError::from)?;

        // deserialize value
        let mut offset = EntryHeader::serialized_len();
        let compressed = &buffer[offset..offset + header.value_len as usize];
        offset += header.value_len as usize;
        let value = match header.compression {
            Compression::None => bincode::deserialize_from(compressed).map_err(SerdeError::from)?,
            Compression::Zstd => {
                let decoder = zstd::Decoder::new(compressed).map_err(SerdeError::from)?;
                bincode::deserialize_from(decoder).map_err(SerdeError::from)?
            }
            Compression::Lz4 => {
                let decoder = lz4::Decoder::new(compressed).map_err(SerdeError::from)?;
                bincode::deserialize_from(decoder).map_err(SerdeError::from)?
            }
        };

        // deserialize key
        let compressed = &buffer[offset..offset + header.key_len as usize];
        let key = bincode::deserialize_from(compressed).map_err(SerdeError::from)?;
        offset += header.key_len as usize;

        let checksum = checksum(&buffer[EntryHeader::serialized_len()..offset]);
        if checksum != header.checksum {
            return Err(anyhow!("magic mismatch, expected: {}, got: {}", header.checksum, checksum).into());
        }

        Ok((header, key, value))
    }
}
