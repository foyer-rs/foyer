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
    engine::large::{buffer::BlobIndexReader, region::Region},
    error::Result,
    io::bytes::IoSliceMut,
};

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

        let bytes = Box::new(IoSliceMut::new(self.blob_index_size));
        let (bytes, res) = self.region.read(bytes, self.offset).await;
        res?;
        let indices = match BlobIndexReader::read(&bytes) {
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
        engine::large::{
            buffer::{BlobEntryIndex, BlobIndex, BlobPart, Buffer, SplitCtx, Splitter},
            region::RegionId,
            serde::Sequence,
        },
        io::{
            device::{fs::FsDeviceBuilder, Device, DeviceBuilder, Partition},
            engine::{psync::PsyncIoEngineBuilder, IoEngine, IoEngineBuilder},
            PAGE,
        },
        Compression, Runtime,
    };

    const KB: usize = 1024;

    async fn device_for_test(dir: impl AsRef<Path>, file_size: usize, files: usize) -> Arc<dyn Device> {
        FsDeviceBuilder::new(dir)
            .with_capacity(file_size * files)
            .boxed()
            .build()
            .unwrap()
    }

    async fn io_engine_for_test() -> Arc<dyn IoEngine> {
        PsyncIoEngineBuilder::new().boxed().build(Runtime::current()).unwrap()
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
        let device = device_for_test(tempdir.path(), REGION_SIZE, 4).await;
        let partitions = (0..4)
            .map(|_| device.create_partition(REGION_SIZE).unwrap())
            .collect_vec();
        let engine = io_engine_for_test().await;

        let mut ctx = SplitCtx::new(REGION_SIZE, BLOB_INDEX_SIZE);
        let mut buffer = Buffer::new(IoSliceMut::new(BATCH_SIZE), MAX_ENTRY_SIZE, Arc::new(Metrics::noop()));

        for i in 0..BLOB_INDEX_CAPACITY * 5 {
            buffer.push(
                &(i as u64),
                &vec![i as u8; 3 * 1024],
                i as u64,
                Compression::None,
                i as Sequence,
            );
        }
        let (bytes, infos) = buffer.finish();
        let bytes = bytes.into_io_slice();
        let batch = Splitter::split(&mut ctx, bytes, infos.clone());

        assert_eq!(batch.regions.len(), 3);
        assert_eq!(batch.regions[0].blob_parts.len(), 2);
        assert_eq!(batch.regions[1].blob_parts.len(), 2);
        assert_eq!(batch.regions[2].blob_parts.len(), 1);

        async fn flush<'a>(engine: &'a Arc<dyn IoEngine>, partition: &'a dyn Partition, part: &'a BlobPart) {
            let (_, res) = engine
                .write(
                    Box::new(part.data.clone()),
                    partition,
                    part.blob_region_offset as u64 + part.part_blob_offset as u64,
                )
                .await;
            res.unwrap();
            let (_, res) = engine
                .write(Box::new(part.index.clone()), partition, part.blob_region_offset as u64)
                .await;
            res.unwrap();
        }

        flush(&engine, partitions[0].as_ref(), &batch.regions[0].blob_parts[0]).await;
        flush(&engine, partitions[0].as_ref(), &batch.regions[0].blob_parts[1]).await;
        flush(&engine, partitions[1].as_ref(), &batch.regions[1].blob_parts[0]).await;
        flush(&engine, partitions[1].as_ref(), &batch.regions[1].blob_parts[1]).await;
        flush(&engine, partitions[2].as_ref(), &batch.regions[2].blob_parts[0]).await;

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

        async fn extract(engine: Arc<dyn IoEngine>, partition: Arc<dyn Partition>) -> Vec<EntryInfo> {
            let mut infos = vec![];
            let mut scanner = RegionScanner::new(
                Region::new_for_test(partition.id() as _, partition, engine),
                BLOB_INDEX_SIZE,
            );
            while let Some(mut es) = scanner.next().await.unwrap() {
                infos.append(&mut es);
            }
            infos
        }

        let rr0 = extract(engine.clone(), partitions[0].clone()).await;
        let rr1 = extract(engine.clone(), partitions[1].clone()).await;
        let rr2 = extract(engine.clone(), partitions[2].clone()).await;

        assert_eq!(r0, rr0);
        assert_eq!(r1, rr1);
        assert_eq!(r2, rr2);
    }
}
