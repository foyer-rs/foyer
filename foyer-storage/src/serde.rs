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

use std::{fmt::Debug, io::Write};

use foyer_common::{
    code::{StorageKey, StorageValue},
    error::{Error, ErrorKind, Result},
};
use twox_hash::{XxHash32, XxHash64};

use crate::compress::Compression;

#[derive(Debug)]
pub struct Checksummer;

impl Checksummer {
    pub fn checksum64(buf: &[u8]) -> u64 {
        XxHash64::oneshot(0, buf)
    }

    #[expect(unused)]
    pub fn checksum32(buf: &[u8]) -> u32 {
        XxHash32::oneshot(0, buf)
    }
}

#[derive(Debug)]
pub struct KvInfo {
    pub key_len: usize,
    pub value_len: usize,
}

/// A writer wrapper that count how many bytes has been written.
#[derive(Debug)]
pub struct TrackedWriter<W> {
    inner: W,
    written: usize,
}

impl<W> TrackedWriter<W> {
    pub fn new(inner: W) -> Self {
        Self { inner, written: 0 }
    }

    pub fn written(self) -> usize {
        self.written
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
            .inspect(|_| self.written += bufs.iter().map(|slice| slice.len()).sum::<usize>())
    }
}

#[derive(Debug)]
pub struct EntrySerializer;

impl EntrySerializer {
    #[cfg_attr(feature = "tracing", fastrace::trace(name = "foyer::storage::serde::serialize"))]
    pub fn serialize<K, V, W>(key: &K, value: &V, compression: Compression, mut writer: W) -> Result<KvInfo>
    where
        K: StorageKey,
        V: StorageValue,
        W: Write,
    {
        // serialize value
        let value_len = Self::serialize_value(value, &mut writer, compression)?;

        // serialize key
        let key_len = Self::serialize_key(key, &mut writer)?;

        Ok(KvInfo { key_len, value_len })
    }

    fn serialize_key<K, W>(key: &K, writer: W) -> Result<usize>
    where
        K: StorageKey,
        W: Write,
    {
        let mut writer = TrackedWriter::new(writer);
        key.encode(&mut writer)?;
        Ok(writer.written())
    }

    fn serialize_value<V, W>(value: &V, writer: W, compression: Compression) -> Result<usize>
    where
        V: StorageValue,
        W: Write,
    {
        let mut writer = TrackedWriter::new(writer);
        match compression {
            Compression::None => {
                value.encode(&mut writer)?;
            }
            Compression::Zstd => {
                // Do not use `auto_finish()` here, for we will lost `ZeroWrite` error.
                let mut encoder = zstd::Encoder::new(&mut writer, 0).map_err(Error::io_error)?;
                value.encode(&mut encoder)?;
                encoder.finish().map_err(Error::io_error)?;
            }
            Compression::Lz4 => {
                let mut encoder = lz4::EncoderBuilder::new()
                    .checksum(lz4::ContentChecksum::NoChecksum)
                    .auto_flush(true)
                    .build(&mut writer)
                    .map_err(Error::io_error)?;
                value.encode(&mut encoder)?;
            }
        }
        Ok(writer.written())
    }

    pub fn estimated_size<'a, K, V>(key: &'a K, value: &'a V) -> usize
    where
        K: StorageKey,
        V: StorageValue,
    {
        key.estimated_size() + value.estimated_size()
    }
}

#[derive(Debug)]
pub struct EntryDeserializer;

impl EntryDeserializer {
    #[cfg_attr(feature = "tracing", fastrace::trace(name = "foyer::storage::serde::deserialize"))]
    pub fn deserialize<K, V>(
        buffer: &[u8],
        ken_len: usize,
        value_len: usize,
        compression: Compression,
        checksum: Option<u64>,
    ) -> Result<(K, V)>
    where
        K: StorageKey,
        V: StorageValue,
    {
        if buffer.len() < value_len + ken_len {
            return Err(Error::new(ErrorKind::OutOfRange, "fail to deserialize entry")
                .with_context("valid", format!("{:?}", 0..buffer.len()))
                .with_context("get", format!("{:?}", 0..value_len + ken_len)));
        }

        // calculate checksum if needed
        if let Some(expected) = checksum {
            let get = Checksummer::checksum64(&buffer[..value_len + ken_len]);
            if expected != get {
                return Err(Error::new(ErrorKind::ChecksumMismatch, "fail to deserialize entry")
                    .with_context("expected", expected)
                    .with_context("get", get));
            }
        }

        // deserialize value
        let buf = &buffer[..value_len];
        let value = Self::deserialize_value(buf, compression)?;

        // deserialize key
        let buf = &buffer[value_len..value_len + ken_len];
        let key = Self::deserialize_key(buf)?;

        Ok((key, value))
    }

    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::storage::serde::deserialize_key")
    )]
    fn deserialize_key<K>(buf: &[u8]) -> Result<K>
    where
        K: StorageKey,
    {
        K::decode(&mut &buf[..])
    }

    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::storage::serde::deserialize_value")
    )]
    fn deserialize_value<V>(buf: &[u8], compression: Compression) -> Result<V>
    where
        V: StorageValue,
    {
        match compression {
            Compression::None => V::decode(&mut &buf[..]),
            Compression::Zstd => {
                let mut decoder = zstd::Decoder::new(buf).map_err(Error::io_error)?;
                V::decode(&mut decoder)
            }
            Compression::Lz4 => {
                let mut decoder = lz4::Decoder::new(buf).map_err(Error::io_error)?;
                V::decode(&mut decoder)
            }
        }
    }
}
