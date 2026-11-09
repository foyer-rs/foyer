// Copyright 2026 foyer Project Authors
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

//! Recovery contracts exercised through the public disk-only API and real files.
#![forbid(unsafe_code)]

use std::{
    fs::{File, OpenOptions},
    hash::BuildHasher,
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    sync::Arc,
    time::Duration,
};

use foyer::{
    BlockEngineConfig, DefaultHasher, DeviceBuilder, FileDeviceBuilder, HybridCache, HybridCacheBuilder, RecoverMode,
    Statistics, StorageFilter, StorageFilterCondition, StorageFilterResult,
};

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;

#[derive(Debug)]
struct KeepOne(Option<u64>);

impl StorageFilterCondition for KeepOne {
    fn filter(&self, _: &Arc<Statistics>, hash: u64, _: usize) -> StorageFilterResult {
        if self.0 == Some(hash) {
            StorageFilterResult::Admit
        } else {
            StorageFilterResult::Reject
        }
    }
}

async fn open(directory: &Path, keep: Option<u64>) -> HybridCache<u64, Vec<u8>> {
    HybridCacheBuilder::new()
        .memory(1)
        .storage()
        .with_recover_mode(RecoverMode::Strict)
        .with_engine_config(
            BlockEngineConfig::new(
                FileDeviceBuilder::new(directory.join("cache"))
                    .with_capacity(16 * MIB)
                    .build()
                    .unwrap(),
            )
            .with_block_size(MIB)
            .with_buffer_pool_size(512 * KIB)
            .with_submit_queue_size_threshold(MIB)
            .with_flushers(1)
            .with_reclaimers(1)
            .with_recover_concurrency(1)
            .with_indexer_shards(4)
            .with_reinsertion_filter(
                StorageFilter::new().with_condition(KeepOne(keep.map(|key| DefaultHasher::default().hash_one(key)))),
            ),
        )
        .build()
        .await
        .unwrap()
}

#[tokio::test]
async fn reinserted_entries_survive_reopen() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let directory = tempfile::tempdir().unwrap();
        let cache = open(directory.path(), Some(0)).await;
        for key in 0..256_u64 {
            drop(cache.writer(key).storage().insert(vec![(key % 251) as u8; 128 * KIB]));
            cache.storage().wait().await;
        }
        cache.close().await.unwrap();
        assert!(cache.memory().get(&0).is_none());
        let mut resident = Vec::new();
        for key in 0..256_u64 {
            if cache.storage().load(&key).await.unwrap().kv().is_some() {
                resident.push(key);
            }
        }
        assert!(resident.contains(&0), "the reinserted entry must exist before restart");
        drop(cache);

        let cache = open(directory.path(), None).await;
        let mut missing = Vec::new();
        for key in resident {
            match cache.storage().load(&key).await.unwrap().kv() {
                Some((stored, payload)) => {
                    assert_eq!(stored, key);
                    assert_eq!(payload.len(), 128 * KIB);
                    assert!(payload.iter().all(|byte| *byte == (key % 251) as u8));
                }
                None => missing.push(key),
            }
        }
        cache.close().await.unwrap();
        assert!(
            missing.is_empty(),
            "completed entries lost during recovery: {missing:?}"
        );
    })
    .await
    .expect("reinsertion and recovery must complete");
}

#[tokio::test]
async fn recovery_rejects_a_previous_region_tail() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let old = tempfile::tempdir().unwrap();
        let cache = open(old.path(), None).await;
        // Small entries produce multiple blob indexes in the same region.
        for key in 0..220_u64 {
            drop(cache.writer(key).storage().insert(vec![b'o'; 64]));
            cache.storage().wait().await;
        }
        cache.close().await.unwrap();
        assert!(cache.storage().load(&200).await.unwrap().kv().is_some());
        drop(cache);

        let new = tempfile::tempdir().unwrap();
        let cache = open(new.path(), None).await;
        // Reused regions have newer logical operations, even if the new data
        // includes reinsertions with old logical sequences.
        for key in 10_000..10_512_u64 {
            cache.storage().delete(&key);
        }
        cache.storage().wait().await;
        for key in 1000..1220_u64 {
            drop(cache.writer(key).storage().insert(vec![b'n'; 64]));
            cache.storage().wait().await;
        }
        cache.close().await.unwrap();
        drop(cache);

        // A crash during reuse can leave a valid new prefix and an intact old
        // tail. Copy real generated pages rather than forging index records.
        let mut prefix = vec![0; 256 * KIB];
        File::open(new.path().join("cache"))
            .unwrap()
            .read_exact(&mut prefix)
            .unwrap();
        let mut file = OpenOptions::new().write(true).open(old.path().join("cache")).unwrap();
        file.write_all(&prefix).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let cache = open(old.path(), None).await;
        assert_eq!(
            cache.storage().load(&1000).await.unwrap().kv(),
            Some((1000, vec![b'n'; 64]))
        );
        assert!(
            cache.storage().load(&200).await.unwrap().kv().is_none(),
            "old tail must not be resurrected"
        );
        cache.close().await.unwrap();
    })
    .await
    .expect("reuse recovery must complete");
}

#[tokio::test]
async fn recovered_full_cache_resumes_writing() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let directory = tempfile::tempdir().unwrap();
        let cache = open(directory.path(), None).await;
        drop(cache.writer(1).storage().insert(b"existing".to_vec()));
        cache.storage().wait().await;
        cache.close().await.unwrap();
        drop(cache);

        // A stop after consuming the final clean region can leave every region
        // populated. Reproduce that disk state with a real completed region;
        // recovery must deduplicate it and start reclamation for the first write.
        let path = directory.path().join("cache");
        let mut region = vec![0; MIB];
        File::open(&path).unwrap().read_exact(&mut region).unwrap();
        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        for region_id in 1..16_u64 {
            file.seek(SeekFrom::Start(region_id * MIB as u64)).unwrap();
            file.write_all(&region).unwrap();
        }
        file.sync_all().unwrap();
        drop(file);

        let cache = open(directory.path(), None).await;
        assert_eq!(
            cache.storage().load(&1).await.unwrap().kv(),
            Some((1, b"existing".to_vec()))
        );
        for key in 2..18_u64 {
            drop(cache.writer(key).storage().insert(vec![key as u8; 128 * KIB]));
            tokio::time::timeout(Duration::from_secs(5), cache.storage().wait())
                .await
                .expect("a recovered cache with no clean regions must make write progress");
            let (stored, value) = cache.storage().load(&key).await.unwrap().kv().unwrap();
            assert_eq!(stored, key);
            assert_eq!(value.len(), 128 * KIB);
            assert!(value.iter().all(|byte| *byte == key as u8));
        }
        cache.close().await.unwrap();
    })
    .await
    .expect("full-cache recovery must complete");
}
