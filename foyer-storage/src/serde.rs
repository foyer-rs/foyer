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

use std::{fmt::Debug, hash::Hasher, io::Write, time::Instant};

use foyer_common::{
    code::{StorageKey, StorageValue},
    metrics::Metrics,
};
use twox_hash::XxHash64;

use crate::{
    compress::Compression,
    error::{Error, Result},
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
pub struct TrackedWriter<W> {
    inner: W,
    written: usize,
}

impl<W> TrackedWriter<W> {
    pub fn new(inner: W) -> Self {
        Self { inner, written: 0 }
    }

    pub fn written(&self) -> usize {
        self.written
    }

    pub fn recount(&mut self) {
        self.written = 0;
    }
}

impl<W> Write for TrackedWriter<W>
where
    W: Write,
{
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.write(buf).inspect(|len| self.written += len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }

    fn write_vectored(&mut self, bufs: &[std::io::IoSlice<'_>]) -> std::io::Result<usize> {
        self.inner.write_vectored(bufs).inspect(|len| self.written += len)
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.inner.write_all(buf).inspect(|_| self.written += buf.len())
    }

    #[cfg(feature = "nightly")]
    fn write_all_vectored(&mut self, bufs: &mut [std::io::IoSlice<'_>]) -> std::io::Result<()> {
        self.inner
            .write_all_vectored(bufs)
            .inspect(|_| self.count += bufs.iter().map(|slice| slice.len()).sum())
    }
}

#[derive(Debug)]
pub struct EntrySerializer;

impl EntrySerializer {
    #[fastrace::trace(name = "foyer::storage::serde::serialize")]
    pub fn serialize<'a, K, V, W>(
        key: &'a K,
        value: &'a V,
        compression: &'a Compression,
        writer: W,
        metrics: &Metrics,
    ) -> Result<KvInfo>
    where
        K: StorageKey,
        V: StorageValue,
        W: Write,
    {
        let now = Instant::now();

        let mut writer = TrackedWriter::new(writer);

        // serialize value
        match compression {
            Compression::None => {
                bincode::serialize_into(&mut writer, &value).map_err(Error::from)?;
            }
            Compression::Zstd => {
                // Do not use `auto_finish()` here, for we will lost `ZeroWrite` error.
                let mut encoder = zstd::Encoder::new(&mut writer, 0).map_err(Error::from)?;
                bincode::serialize_into(&mut encoder, &value).map_err(Error::from)?;
                encoder.finish().map_err(Error::from)?;
            }
            Compression::Lz4 => {
                let encoder = lz4::EncoderBuilder::new()
                    .checksum(lz4::ContentChecksum::NoChecksum)
                    .auto_flush(true)
                    .build(&mut writer)
                    .map_err(Error::from)?;
                bincode::serialize_into(encoder, &value).map_err(Error::from)?;
            }
        }

        let value_len = writer.written();
        writer.recount();

        // serialize key
        bincode::serialize_into(&mut writer, &key).map_err(Error::from)?;
        let key_len = writer.written();

        metrics.storage_entry_serialize_duration.record(now.elapsed());

        Ok(KvInfo { key_len, value_len })
    }

    pub fn estimated_size<'a, K, V>(key: &'a K, value: &'a V) -> usize
    where
        K: StorageKey,
        V: StorageValue,
    {
        // `serialized_size` should always return `Ok(..)` without a hard size limit.
        (bincode::serialized_size(key).unwrap() + bincode::serialized_size(value).unwrap()) as usize
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
