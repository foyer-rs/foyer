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

//! Fuzzy test for foyer storage engines.

#![expect(clippy::identity_op)]

use std::{path::Path, sync::Arc};

use foyer_common::{hasher::ModHasher, metrics::Metrics};
use foyer_memory::{Cache, CacheBuilder, FifoConfig, TestProperties};
use foyer_storage::{test_utils::Recorder, Compression, FsDeviceBuilder, LargeObjectEngineBuilder, StoreBuilder};

const KB: usize = 1024;
const MB: usize = 1024 * 1024;

const INSERTS: usize = 100;
const LOOPS: usize = 10;

async fn test_store(
    memory: Cache<u64, Vec<u8>, ModHasher, TestProperties>,
    builder: impl Fn(
        &Cache<u64, Vec<u8>, ModHasher, TestProperties>,
    ) -> StoreBuilder<u64, Vec<u8>, ModHasher, TestProperties>,
    recorder: Arc<Recorder>,
) {
    let store = builder(&memory).build().await.unwrap();

    let mut index = 0;

    for _ in 0..INSERTS as u64 {
        index += 1;
        let e = memory.insert(index, vec![index as u8; KB]);
        store.enqueue(e.piece(), false);
        store.wait().await;
    }

    store.close().await.unwrap();

    let remains = recorder.remains();

    for i in 0..INSERTS as u64 * (LOOPS + 1) as u64 {
        let value = store.load(&i).await.unwrap().kv().map(|(_, v)| v);
        if remains.contains(&i) {
            assert_eq!(value, Some(vec![i as u8; 1 * KB]));
        } else {
            assert!(value.is_none());
        }
    }

    drop(store);

    for l in 0..LOOPS {
        let store = builder(&memory).build().await.unwrap();

        let remains = recorder.remains();

        for i in 0..INSERTS as u64 * (LOOPS + 1) as u64 {
            let value = store.load(&i).await.unwrap().kv().map(|(_, v)| v);
            if remains.contains(&i) {
                assert_eq!(value, Some(vec![i as u8; 1 * KB]), "value mismatch, loop: {l}, i: {i}");
            } else {
                assert!(value.is_none(), "phantom value, loop: {l}, i: {i}");
            }
        }

        for _ in 0..INSERTS as u64 {
            index += 1;
            let e = memory.insert(index, vec![index as u8; KB]);
            store.enqueue(e.piece(), false);
            store.wait().await;
        }

        store.close().await.unwrap();

        let remains = recorder.remains();

        for i in 0..INSERTS as u64 * (LOOPS + 1) as u64 {
            let value = store.load(&i).await.unwrap().kv().map(|(_, v)| v);
            if remains.contains(&i) {
                assert_eq!(value, Some(vec![i as u8; 1 * KB]));
            } else {
                assert!(value.is_none());
            }
        }

        drop(store);
    }
}

fn basic(
    memory: &Cache<u64, Vec<u8>, ModHasher, TestProperties>,
    path: impl AsRef<Path>,
    recorder: &Arc<Recorder>,
) -> StoreBuilder<u64, Vec<u8>, ModHasher, TestProperties> {
    // TODO(MrCroxx): Test mixed engine here.
    StoreBuilder::new("test", memory.clone(), Arc::new(Metrics::noop()))
        .with_device_builder(FsDeviceBuilder::new(path).with_capacity(4 * MB))
        .with_engine_builder(
            LargeObjectEngineBuilder::new()
                .with_region_size(MB)
                .with_recover_concurrency(2)
                .with_indexer_shards(4)
                .with_reinsertion_picker(recorder.clone()),
        )
        .with_admission_picker(recorder.clone())
}

#[test_log::test(tokio::test)]
async fn test_direct_fs_store() {
    let tempdir = tempfile::tempdir().unwrap();
    let recorder = Arc::new(Recorder::default());
    let memory = CacheBuilder::new(1)
        .with_eviction_config(FifoConfig::default())
        .with_hash_builder(ModHasher::default())
        .build();
    let r = recorder.clone();
    let builder = |memory: &Cache<u64, Vec<u8>, ModHasher, TestProperties>| basic(memory, tempdir.path(), &r);
    test_store(memory, builder, recorder).await;
}

#[test_log::test(tokio::test)]
async fn test_direct_fs_store_zstd() {
    let tempdir = tempfile::tempdir().unwrap();
    let recorder = Arc::new(Recorder::default());
    let memory = CacheBuilder::new(1)
        .with_eviction_config(FifoConfig::default())
        .with_hash_builder(ModHasher::default())
        .build();
    let r = recorder.clone();
    let builder = |memory: &Cache<u64, Vec<u8>, ModHasher, TestProperties>| {
        basic(memory, tempdir.path(), &r).with_compression(Compression::Zstd)
    };
    test_store(memory, builder, recorder).await;
}

#[test_log::test(tokio::test)]
async fn test_direct_fs_store_lz4() {
    let tempdir = tempfile::tempdir().unwrap();
    let recorder = Arc::new(Recorder::default());
    let memory = CacheBuilder::new(1)
        .with_eviction_config(FifoConfig::default())
        .with_hash_builder(ModHasher::default())
        .build();
    let r = recorder.clone();
    let builder = |memory: &Cache<u64, Vec<u8>, ModHasher, TestProperties>| {
        basic(memory, tempdir.path(), &r).with_compression(Compression::Lz4)
    };
    test_store(memory, builder, recorder).await;
}
