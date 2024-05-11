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

use foyer_common::{
    bits,
    code::{StorageKey, StorageValue},
};

use crate::{
    catalog::Sequence,
    error::Result,
    serde::{EntryDeserializer, EntryHeader},
};

use super::{
    device::{Device, DeviceExt, IoBuffer, RegionId, IO_BUFFER_ALLOCATOR},
    indexer::EntryAddress,
};

#[derive(Debug)]
pub struct EntryInfo {
    pub hash: u64,
    pub sequence: Sequence,
    pub addr: EntryAddress,
}

#[derive(Debug)]
struct CachedDeviceReader<D> {
    region: RegionId,
    offset: u64,
    buffer: IoBuffer,
    device: D,
}

impl<D> CachedDeviceReader<D> {
    const IO_SIZE_HINT: usize = 16 * 1024;

    fn new(region: RegionId, device: D) -> Self {
        Self {
            region,
            offset: 0,
            buffer: IoBuffer::new_in(&IO_BUFFER_ALLOCATOR),
            device,
        }
    }

    async fn read(&mut self, offset: u64, len: usize) -> Result<&[u8]>
    where
        D: Device,
    {
        if offset >= self.offset && offset as usize + len <= self.offset as usize + self.buffer.len() {
            let start = (offset - self.offset) as usize;
            let end = start + len;
            return Ok(&self.buffer[start..end]);
        }
        self.offset = bits::align_down(self.device.align() as u64, offset);
        let end = bits::align_up(
            self.device.align(),
            std::cmp::max(offset as usize + len, offset as usize + Self::IO_SIZE_HINT),
        );
        let end = std::cmp::min(self.device.region_size(), end);
        let read_len = end - self.offset as usize;
        debug_assert!(bits::is_aligned(self.device.align(), read_len));
        debug_assert!(read_len >= len);

        let buffer = self.device.read(self.region, self.offset, read_len).await?;
        self.buffer = buffer;

        let start = (offset - self.offset) as usize;
        let end = start + len;
        Ok(&self.buffer[start..end])
    }
}

#[derive(Debug)]
pub struct RegionScanner<D> {
    region: RegionId,
    offset: u64,
    cache: CachedDeviceReader<D>,
}

impl<D> RegionScanner<D> {
    // TODO(MrCroxx): Receives `Region` directly.
    pub fn new(region: RegionId, device: D) -> Self {
        Self {
            region,
            offset: 0,
            cache: CachedDeviceReader::new(region, device),
        }
    }

    async fn current(&mut self) -> Result<Option<EntryHeader>>
    where
        D: Device,
    {
        debug_assert!(bits::is_aligned(self.cache.device.align() as u64, self.offset));

        if self.offset as usize >= self.cache.device.region_size() {
            // reach region EOF
            return Ok(None);
        }

        // load entry header buf
        let buf = self.cache.read(self.offset, EntryHeader::serialized_len()).await?;

        Ok(EntryDeserializer::deserialize_header(buf))
    }

    async fn step(&mut self, header: &EntryHeader)
    where
        D: Device,
    {
        let aligned = bits::align_up(self.cache.device.align(), header.entry_len());
        self.offset += aligned as u64;
    }

    fn info(&self, header: &EntryHeader) -> EntryInfo {
        EntryInfo {
            hash: header.hash,
            sequence: header.sequence,
            addr: EntryAddress {
                region: self.region,
                offset: self.offset as _,
                len: header.entry_len() as _,
                sequence: header.sequence,
            },
        }
    }

    pub async fn next(&mut self) -> Result<Option<EntryInfo>>
    where
        D: Device,
    {
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
        D: Device,
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

    pub async fn next_value<V>(&mut self) -> Result<Option<(EntryInfo, V)>>
    where
        V: StorageValue,
        D: Device,
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

    pub async fn next_kv<K, V>(&mut self) -> Result<Option<(EntryInfo, K, V)>>
    where
        K: StorageKey,
        V: StorageValue,
        D: Device,
    {
        let header = match self.current().await {
            Ok(Some(header)) => header,
            Ok(None) => return Ok(None),
            Err(e) => return Err(e),
        };

        let info = self.info(&header);

        let buf = self.cache.read(info.addr.offset as _, info.addr.len as _).await?;
        let (h, key, value) = EntryDeserializer::deserialize(buf)?;
        debug_assert_eq!(header, h);

        self.step(&header).await;

        Ok(Some((info, key, value)))
    }
}
