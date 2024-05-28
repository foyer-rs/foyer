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
    strict_assert, strict_assert_eq,
};

use crate::{
    device::{Device, IoBuffer, IO_BUFFER_ALLOCATOR},
    error::Result,
    region::Region,
    serde::{EntryDeserializer, EntryHeader},
    Sequence,
};

use super::indexer::EntryAddress;

#[derive(Debug)]
pub struct EntryInfo {
    pub hash: u64,
    pub sequence: Sequence,
    pub addr: EntryAddress,
}

#[derive(Debug)]
struct CachedDeviceReader<D>
where
    D: Device,
{
    region: Region<D>,
    offset: u64,
    buffer: IoBuffer,
}

impl<D> CachedDeviceReader<D>
where
    D: Device,
{
    const IO_SIZE_HINT: usize = 16 * 1024;

    fn new(region: Region<D>) -> Self {
        Self {
            region,
            offset: 0,
            buffer: IoBuffer::new_in(&IO_BUFFER_ALLOCATOR),
        }
    }

    async fn read(&mut self, offset: u64, len: usize) -> Result<&[u8]> {
        if offset >= self.offset && offset as usize + len <= self.offset as usize + self.buffer.len() {
            let start = (offset - self.offset) as usize;
            let end = start + len;
            return Ok(&self.buffer[start..end]);
        }
        self.offset = bits::align_down(self.region.align() as u64, offset);
        let end = bits::align_up(
            self.region.align(),
            std::cmp::max(offset as usize + len, offset as usize + Self::IO_SIZE_HINT),
        );
        let end = std::cmp::min(self.region.size(), end);
        let read_len = end - self.offset as usize;
        strict_assert!(bits::is_aligned(self.region.align(), read_len));
        strict_assert!(read_len >= len);

        let buffer = self.region.read(self.offset, read_len).await?;
        self.buffer = buffer;

        let start = (offset - self.offset) as usize;
        let end = start + len;
        Ok(&self.buffer[start..end])
    }
}

#[derive(Debug)]
pub struct RegionScanner<D>
where
    D: Device,
{
    region: Region<D>,
    offset: u64,
    cache: CachedDeviceReader<D>,
}

impl<D> RegionScanner<D>
where
    D: Device,
{
    pub fn new(region: Region<D>) -> Self {
        let cache = CachedDeviceReader::new(region.clone());
        Self {
            region,
            offset: 0,
            cache,
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

        Ok(EntryDeserializer::deserialize_header(buf))
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

    // TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
    #[allow(dead_code)]
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

    // TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
    #[allow(dead_code)]
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
        let (h, key, value) = EntryDeserializer::deserialize(buf)?;
        strict_assert_eq!(header, h);

        self.step(&header).await;

        Ok(Some((info, key, value)))
    }
}
