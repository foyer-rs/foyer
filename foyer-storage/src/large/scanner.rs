//  Copyright 2024 foyer Project Authors
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

use std::sync::Arc;

use foyer_common::{
    bits,
    code::{StorageKey, StorageValue},
    metrics::model::Metrics,
    strict_assert,
};

use super::indexer::EntryAddress;
use crate::{
    device::bytes::IoBytes,
    error::{Error, Result},
    large::serde::{EntryHeader, Sequence},
    region::Region,
    serde::EntryDeserializer,
};

#[derive(Debug)]
pub struct EntryInfo {
    pub hash: u64,
    pub sequence: Sequence,
    pub addr: EntryAddress,
}

#[derive(Debug)]
struct CachedRegionReader {
    region: Region,
    offset: u64,
    buffer: IoBytes,
}

impl CachedRegionReader {
    const IO_SIZE_HINT: usize = 16 * 1024;

    fn new(region: Region) -> Self {
        Self {
            region,
            offset: 0,
            buffer: IoBytes::new(),
        }
    }

    async fn read(&mut self, offset: u64, len: usize) -> Result<&[u8]> {
        if offset as usize + len >= self.region.size() {
            return Err(Error::OutOfRange {
                valid: 0..self.region.size(),
                get: offset as usize..offset as usize + len,
            });
        }

        if offset >= self.offset && offset as usize + len <= self.offset as usize + self.buffer.len() {
            let start = (offset - self.offset) as usize;
            let end = start + len;
            return Ok(&self.buffer[start..end]);
        }

        // Move forward.
        self.offset = bits::align_down(self.region.align() as u64, offset);
        let end = bits::align_up(
            self.region.align(),
            std::cmp::max(offset as usize + len, offset as usize + Self::IO_SIZE_HINT),
        );
        let end = std::cmp::min(self.region.size(), end);

        let read_len = end - self.offset as usize;
        assert!(bits::is_aligned(self.region.align(), read_len));
        assert!(read_len >= len);

        let buffer = self.region.read(self.offset, read_len).await?.freeze();
        self.buffer = buffer;

        let start = (offset - self.offset) as usize;
        let end = start + len;
        Ok(&self.buffer[start..end])
    }
}

#[derive(Debug)]
pub struct RegionScanner {
    region: Region,
    offset: u64,
    cache: CachedRegionReader,
    metrics: Arc<Metrics>,
}

impl RegionScanner {
    pub fn new(region: Region, metrics: Arc<Metrics>) -> Self {
        let cache = CachedRegionReader::new(region.clone());
        Self {
            region,
            offset: 0,
            cache,
            metrics,
        }
    }

    async fn current(&mut self) -> Result<Option<EntryHeader>> {
        strict_assert!(bits::is_aligned(self.region.align() as u64, self.offset));

        if self.offset as usize >= self.region.size() {
            // reach region EOF
            return Ok(None);
        }

        // load entry header buf
        let buf = self.cache.read(self.offset, EntryHeader::serialized_len()).await?;

        if buf.len() < EntryHeader::serialized_len() {
            return Ok(None);
        }

        let res = EntryHeader::read(buf).ok();

        Ok(res)
    }

    async fn step(&mut self, header: &EntryHeader) {
        let aligned = bits::align_up(self.region.align(), header.entry_len());
        self.offset += aligned as u64;
    }

    fn info(&self, header: &EntryHeader) -> EntryInfo {
        EntryInfo {
            hash: header.hash,
            sequence: header.sequence,
            addr: EntryAddress {
                region: self.region.id(),
                offset: self.offset as _,
                len: header.entry_len() as _,
                sequence: header.sequence,
            },
        }
    }

    pub async fn next(&mut self) -> Result<Option<EntryInfo>> {
        let header = match self.current().await {
            Ok(Some(header)) => header,
            Ok(None) => return Ok(None),
            Err(e) => return Err(e),
        };

        let info = self.info(&header);

        self.step(&header).await;

        Ok(Some(info))
    }

    pub async fn next_key<K>(&mut self) -> Result<Option<(EntryInfo, K)>>
    where
        K: StorageKey,
    {
        let header = match self.current().await {
            Ok(Some(header)) => header,
            Ok(None) => return Ok(None),
            Err(e) => return Err(e),
        };

        let info = self.info(&header);

        let offset = info.addr.offset as u64 + EntryHeader::serialized_len() as u64 + header.value_len as u64;
        let len = header.key_len as usize;
        let buf = self.cache.read(offset, len).await?;
        let key = EntryDeserializer::deserialize_key(buf)?;

        self.step(&header).await;

        Ok(Some((info, key)))
    }

    #[expect(dead_code)]
    pub async fn next_value<V>(&mut self) -> Result<Option<(EntryInfo, V)>>
    where
        V: StorageValue,
    {
        let header = match self.current().await {
            Ok(Some(header)) => header,
            Ok(None) => return Ok(None),
            Err(e) => return Err(e),
        };

        let info = self.info(&header);

        let offset = info.addr.offset as u64 + EntryHeader::serialized_len() as u64;
        let len = header.value_len as usize;
        let buf = self.cache.read(offset, len).await?;
        let value = EntryDeserializer::deserialize_value(buf, header.compression)?;

        self.step(&header).await;

        Ok(Some((info, value)))
    }

    #[expect(dead_code)]
    pub async fn next_kv<K, V>(&mut self) -> Result<Option<(EntryInfo, K, V)>>
    where
        K: StorageKey,
        V: StorageValue,
    {
        let header = match self.current().await {
            Ok(Some(header)) => header,
            Ok(None) => return Ok(None),
            Err(e) => return Err(e),
        };

        let info = self.info(&header);

        let buf = self.cache.read(info.addr.offset as _, info.addr.len as _).await?;

        let (key, value) = EntryDeserializer::deserialize(
            &buf[EntryHeader::serialized_len()..],
            header.key_len as _,
            header.value_len as _,
            header.compression,
            Some(header.checksum),
            &self.metrics,
        )?;

        self.step(&header).await;

        Ok(Some((info, key, value)))
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use bytesize::ByteSize;

    use super::*;
    use crate::{
        device::{
            monitor::{Monitored, MonitoredConfig},
            Dev, MonitoredDevice,
        },
        region::RegionStats,
        DirectFsDeviceOptions, Runtime,
    };

    async fn device_for_test(dir: impl AsRef<Path>) -> MonitoredDevice {
        let runtime = Runtime::current();
        Monitored::open(
            MonitoredConfig {
                config: DirectFsDeviceOptions::new(dir)
                    .with_capacity(ByteSize::kib(64).as_u64() as _)
                    .with_file_size(ByteSize::kib(16).as_u64() as _)
                    .into(),
                metrics: Arc::new(Metrics::noop()),
            },
            runtime,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn test_cached_region_reader_overflow() {
        let dir = tempfile::tempdir().unwrap();

        let device = device_for_test(dir.path()).await;
        let region = Region::new_for_test(0, device, Arc::<RegionStats>::default());

        let mut cached = CachedRegionReader::new(region.clone());

        cached.read(0, region.size() / 2).await.unwrap();
        let res = cached.read(region.size() as u64 / 2, region.size()).await;
        assert!(
            matches!(res, Err(Error::OutOfRange { valid, get }) if valid == (0..region.size()) && get == (
                region.size() / 2..region.size() / 2 + region.size()
            ))
        );
    }
}
