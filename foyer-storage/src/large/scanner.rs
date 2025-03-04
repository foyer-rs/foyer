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

use itertools::Itertools;

use super::indexer::EntryAddress;
use crate::{
    error::Result,
    io::{IoBuffer, PAGE},
    large::batch::BlobReader,
    region::Region,
};

#[derive(Debug)]
pub struct EntryInfo {
    pub hash: u64,
    pub addr: EntryAddress,
}

#[derive(Debug)]
pub struct RegionScanner {
    region: Region,
    offset: u64,
}

impl RegionScanner {
    pub fn new(region: Region) -> Self {
        Self { region, offset: 0 }
    }

    pub async fn next(&mut self) -> Result<Option<Vec<EntryInfo>>> {
        if self.offset as usize + PAGE > self.region.size() {
            return Ok(None);
        }

        let page = IoBuffer::new(PAGE);
        let (page, res) = self.region.read(page, self.offset).await;
        res?;
        let blob = match BlobReader::read(page) {
            Ok(blob) => blob,
            Err(e) => {
                tracing::trace!(
                    ?e,
                    region_id = self.region.id(),
                    "[region blob scanner]: error to parse a blob, skip the remaining region"
                );
                return Ok(None);
            }
        };

        let infos = blob
            .entry_indices
            .into_iter()
            .map(|entry_index| EntryInfo {
                hash: entry_index.hash,
                addr: EntryAddress {
                    region: self.region.id(),
                    offset: self.offset as u32 + entry_index.offset,
                    len: entry_index.len,
                    sequence: entry_index.sequence,
                },
            })
            .inspect(|info| tracing::trace!(?info, "[scanner] extract entry info"))
            .collect_vec();

        self.offset += blob.size as u64;

        Ok(Some(infos))
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Arc};

    use bytesize::ByteSize;
    use foyer_common::metrics::Metrics;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        device::{
            monitor::{Monitored, MonitoredConfig},
            Dev, MonitoredDevice,
        },
        large::batch::{BatchWriter, EntryIndex, EntryWriter, Op},
        region::RegionStats,
        Compression, DirectFsDeviceOptions, Runtime,
    };

    const KB: usize = 1024;

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

    #[test_log::test(tokio::test)]
    async fn test_region_scanner() {
        let tempdir = tempdir().unwrap();
        let dev = device_for_test(tempdir.path()).await;

        let buf = IoBuffer::new(16 * KB);

        // 4K (meta)
        let mut writer = BatchWriter::new(buf, 16 * KB, 16 * KB, Arc::new(Metrics::noop()));
        // 4K
        let op = writer.push(&1, &vec![1u8; 3 * KB], 1, Compression::None, 1);
        assert_eq!(op, Op::Noop);
        // 8K
        let op = writer.push(&3, &vec![3u8; 7 * KB], 3, Compression::None, 3);
        assert_eq!(op, Op::Noop);
        let (buf, batch) = writer.finish();

        let (_, res) = dev.write(buf, 0, 0).await;
        res.unwrap();

        let mut w_entries = vec![];
        let mut blob_offset = 0;
        for window in batch.windows {
            for blob in window.blobs {
                for e in blob.entry_indices {
                    let ei = EntryIndex {
                        hash: e.hash,
                        sequence: e.sequence,
                        offset: blob_offset as u32 + e.offset,
                        len: e.len,
                    };
                    w_entries.push(ei);
                }
                blob_offset += blob.size;
            }
        }

        let mut r_entries = vec![];
        let mut scanner = RegionScanner::new(Region::new_for_test(0, dev, Arc::<RegionStats>::default()));

        while let Some(es) = scanner.next().await.unwrap() {
            for e in es {
                let ei = EntryIndex {
                    hash: e.hash,
                    sequence: e.addr.sequence,
                    offset: e.addr.offset,
                    len: e.addr.len,
                };
                r_entries.push(ei);
            }
        }
    }
}
