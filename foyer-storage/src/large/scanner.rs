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
use crate::{error::Result, io::buffer::IoBuffer, large::buffer::BlobIndexReader, region::Region};

#[derive(Debug, PartialEq, Eq)]
pub struct EntryInfo {
    pub hash: u64,
    pub addr: EntryAddress,
}

#[derive(Debug)]
pub struct RegionScanner {
    region: Region,
    offset: u64,

    blob_index_size: usize,
}

impl RegionScanner {
    pub fn new(region: Region, blob_index_size: usize) -> Self {
        Self {
            region,
            offset: 0,
            blob_index_size,
        }
    }

    pub async fn next(&mut self) -> Result<Option<Vec<EntryInfo>>> {
        if self.offset as usize + self.blob_index_size > self.region.size() {
            return Ok(None);
        }

        let io_buffer = IoBuffer::new(self.blob_index_size);
        let (io_buffer, res) = self.region.read(io_buffer, self.offset).await;
        res?;
        let indices = match BlobIndexReader::read(&io_buffer) {
            Some(indices) => indices,
            None => {
                tracing::trace!(
                    region_id = self.region.id(),
                    "[region blob scanner]: cannot parse a blob, skip the remaining region"
                );
                return Ok(None);
            }
        };

        let step = indices
            .last()
            .map(|index| index.offset as u64 + index.aligned() as u64)
            .unwrap_or(self.region.size() as u64);

        tracing::trace!(
            region = self.region.id(),
            offset = self.offset,
            step,
            "[scanner] calculate next position"
        );

        let infos = indices
            .into_iter()
            .map(|index| EntryInfo {
                hash: index.hash,
                addr: EntryAddress {
                    region: self.region.id(),
                    offset: self.offset as u32 + index.offset,
                    len: index.len,
                    sequence: index.sequence,
                },
            })
            .inspect(|info| tracing::trace!(?info, "[scanner] extract entry info"))
            .collect_vec();

        self.offset += step;

        Ok(Some(infos))
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Arc};

    use foyer_common::metrics::Metrics;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        Compression, DirectFsDeviceOptions, Runtime,
        device::{
            Dev, MonitoredDevice, RegionId,
            monitor::{Monitored, MonitoredConfig},
        },
        io::PAGE,
        large::{
            buffer::{BlobEntryIndex, BlobIndex, BlobPart, Buffer, SplitCtx, Splitter},
            serde::Sequence,
        },
        region::RegionStats,
    };

    const KB: usize = 1024;

    async fn device_for_test(dir: impl AsRef<Path>, file_size: usize, files: usize) -> MonitoredDevice {
        let runtime = Runtime::current();
        Monitored::open(
            MonitoredConfig {
                config: DirectFsDeviceOptions::new(dir)
                    .with_capacity(file_size * files)
                    .with_file_size(file_size)
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
        const BLOB_INDEX_SIZE: usize = 4 * KB;
        const BLOB_INDEX_CAPACITY: usize =
            (BLOB_INDEX_SIZE - BlobIndex::INDEX_OFFSET) / BlobEntryIndex::serialized_len();
        const REGION_SIZE: usize = (BLOB_INDEX_CAPACITY * PAGE + BLOB_INDEX_SIZE) * 2;
        const BATCH_SIZE: usize = REGION_SIZE * 3;
        const MAX_ENTRY_SIZE: usize = REGION_SIZE - BLOB_INDEX_SIZE;

        let tempdir = tempdir().unwrap();
        let dev = device_for_test(tempdir.path(), REGION_SIZE, 4).await;

        let mut ctx = SplitCtx::new(REGION_SIZE, BLOB_INDEX_SIZE);
        let mut buffer = Buffer::new(IoBuffer::new(BATCH_SIZE), MAX_ENTRY_SIZE, Arc::new(Metrics::noop()));

        for i in 0..BLOB_INDEX_CAPACITY * 5 {
            buffer.push(
                &(i as u64),
                &vec![i as u8; 3 * 1024],
                i as u64,
                Compression::None,
                i as Sequence,
            );
        }
        let (io_buffer, infos) = buffer.finish();
        let shared_io_slice = io_buffer.into_shared_io_slice();
        let batch = Splitter::split(&mut ctx, shared_io_slice, infos.clone());

        assert_eq!(batch.regions.len(), 3);
        assert_eq!(batch.regions[0].blob_parts.len(), 2);
        assert_eq!(batch.regions[1].blob_parts.len(), 2);
        assert_eq!(batch.regions[2].blob_parts.len(), 1);

        async fn flush<'a>(dev: &'a MonitoredDevice, region: &'a RegionId, part: &'a BlobPart) {
            let (_, res) = dev
                .write(
                    part.data.clone(),
                    *region,
                    part.blob_region_offset as u64 + part.part_blob_offset as u64,
                )
                .await;
            res.unwrap();
            let (_, res) = dev
                .write(part.index.clone(), *region, part.blob_region_offset as u64)
                .await;
            res.unwrap();
        }

        flush(&dev, &0, &batch.regions[0].blob_parts[0]).await;
        flush(&dev, &0, &batch.regions[0].blob_parts[1]).await;
        flush(&dev, &1, &batch.regions[1].blob_parts[0]).await;
        flush(&dev, &1, &batch.regions[1].blob_parts[1]).await;
        flush(&dev, &2, &batch.regions[2].blob_parts[0]).await;

        fn cal(region: RegionId, part: &BlobPart) -> Vec<EntryInfo> {
            part.indices
                .iter()
                .map(|index| EntryInfo {
                    hash: index.hash,
                    addr: EntryAddress {
                        region,
                        offset: part.blob_region_offset as u32 + index.offset,
                        len: index.len,
                        sequence: index.sequence,
                    },
                })
                .collect_vec()
        }

        let mut r0 = cal(0, &batch.regions[0].blob_parts[0]);
        r0.append(&mut cal(0, &batch.regions[0].blob_parts[1]));
        let mut r1 = cal(1, &batch.regions[1].blob_parts[0]);
        r1.append(&mut cal(1, &batch.regions[1].blob_parts[1]));
        let r2 = cal(2, &batch.regions[2].blob_parts[0]);

        async fn extract(dev: &MonitoredDevice, region: RegionId) -> Vec<EntryInfo> {
            let mut infos = vec![];
            let mut scanner = RegionScanner::new(
                Region::new_for_test(region, dev.clone(), Arc::<RegionStats>::default()),
                BLOB_INDEX_SIZE,
            );
            while let Some(mut es) = scanner.next().await.unwrap() {
                infos.append(&mut es);
            }
            infos
        }

        let rr0 = extract(&dev, 0).await;
        let rr1 = extract(&dev, 1).await;
        let rr2 = extract(&dev, 2).await;

        assert_eq!(r0, rr0);
        assert_eq!(r1, rr1);
        assert_eq!(r2, rr2);
    }
}
