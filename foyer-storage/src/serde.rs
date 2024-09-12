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

use std::{fmt::Debug, hash::Hasher, time::Instant};

use foyer_common::{
    bits,
    code::{StorageKey, StorageValue},
    metrics::Metrics,
};
use twox_hash::XxHash64;

use crate::{
    compress::Compression,
    device::ALIGN,
    error::{Error, Result},
    IoBytesMut,
};

#[derive(Debug)]
pub struct Checksummer;

impl Checksummer {
    pub fn checksum(buf: &[u8]) -> u64 {
        let mut hasher = XxHash64::with_seed(0);
        hasher.write(buf);
        hasher.finish()
    }
}

#[derive(Debug)]
pub struct KvInfo {
    pub key_len: usize,
    pub value_len: usize,
}

#[derive(Debug)]
pub struct EntrySerializer;

impl EntrySerializer {
    #[fastrace::trace(name = "foyer::storage::serde::serialize")]
    pub fn serialize<'a, K, V>(
        key: &'a K,
        value: &'a V,
        compression: &'a Compression,
        mut buffer: &'a mut IoBytesMut,
        metrics: &Metrics,
    ) -> Result<KvInfo>
    where
        K: StorageKey,
        V: StorageValue,
    {
        let now = Instant::now();

        let mut cursor = buffer.len();

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
        bincode::serialize_into(&mut buffer, &key).map_err(Error::from)?;
        let key_len = buffer.len() - cursor;

        metrics.storage_entry_serialize_duration.record(now.elapsed());

        Ok(KvInfo { key_len, value_len })
    }

    pub fn size_hint<'a, K, V>(key: &'a K, value: &'a V) -> usize
    where
        K: StorageKey,
        V: StorageValue,
    {
        let hint = match (bincode::serialized_size(key), bincode::serialized_size(value)) {
            (Ok(k), Ok(v)) => (k + v) as usize,
            _ => 0,
        };
        bits::align_up(ALIGN, hint)
    }
}

#[derive(Debug)]
pub struct EntryDeserializer;

impl EntryDeserializer {
    #[fastrace::trace(name = "foyer::storage::serde::deserialize")]
    pub fn deserialize<K, V>(
        buffer: &[u8],
        ken_len: usize,
        value_len: usize,
        compression: Compression,
        checksum: Option<u64>,
        metrics: &Metrics,
    ) -> Result<(K, V)>
    where
        K: StorageKey,
        V: StorageValue,
    {
        let now = Instant::now();

        // deserialize value
        let buf = &buffer[..value_len];
        let value = Self::deserialize_value(buf, compression)?;

        // deserialize key
        let buf = &buffer[value_len..value_len + ken_len];
        let key = Self::deserialize_key(buf)?;

        // calculate checksum if needed
        if let Some(expected) = checksum {
            let get = Checksummer::checksum(&buffer[..value_len + ken_len]);
            if expected != get {
                return Err(Error::ChecksumMismatch { expected, get });
            }
        }

        metrics.storage_entry_deserialize_duration.record(now.elapsed());

        Ok((key, value))
    }

    #[fastrace::trace(name = "foyer::storage::serde::deserialize_key")]
    pub fn deserialize_key<K>(buf: &[u8]) -> Result<K>
    where
        K: StorageKey,
    {
        bincode::deserialize_from(buf).map_err(Error::from)
    }

    #[fastrace::trace(name = "foyer::storage::serde::deserialize_value")]
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::metrics_for_test;

    #[test]
    fn test_serde_size_hint() {
        let key = 42u64;
        let value = vec![b'x'; 114514];
        let hint = EntrySerializer::size_hint(&key, &value);
        let mut buf = IoBytesMut::new();
        EntrySerializer::serialize(&key, &value, &Compression::None, &mut buf, metrics_for_test()).unwrap();
        assert!(hint >= buf.len());
        assert!(hint.abs_diff(buf.len()) < ALIGN);
    }
}
