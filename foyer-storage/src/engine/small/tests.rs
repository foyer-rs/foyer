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

use std::{any::Any, path::Path, sync::atomic::AtomicBool};

use foyer_common::hasher::ModHasher;
use foyer_memory::{Cache, CacheBuilder, TestProperties};

use super::*;
use crate::{
    DeviceBuilder, FsDeviceBuilder, IoEngineConfig, IoHandle, PsyncIoEngineConfig,
    io::bytes::{IoBuf, IoBufMut},
    serde::EntrySerializer,
};

type TestEngine = dyn Engine<u64, Vec<u8>, TestProperties>;
type Memory = Cache<u64, Vec<u8>, ModHasher, TestProperties>;

fn memory() -> Memory {
    CacheBuilder::new(1)
        .with_shards(1)
        .with_hash_builder(ModHasher::default())
        .build()
}

fn config(path: &Path, buckets: usize, size: usize) -> SmallEngineConfig {
    let builder = FsDeviceBuilder::new(path).with_capacity(buckets * size);
    #[cfg(target_os = "linux")]
    let builder = builder.with_direct(true);
    SmallEngineConfig::new(builder.build().unwrap())
        .with_bucket_size(size)
        .with_shards(2)
}

fn context(mode: RecoverMode) -> EngineBuildContext {
    EngineBuildContext {
        io_engine_config: PsyncIoEngineConfig::new().boxed(),
        metrics: Arc::new(Metrics::noop()),
        spawner: Spawner::current(),
        recover_mode: mode,
    }
}

async fn open(config: SmallEngineConfig, mode: RecoverMode) -> Arc<TestEngine> {
    config.open(context(mode)).await.unwrap()
}

fn concrete(engine: &Arc<TestEngine>) -> &SmallEngine<u64, Vec<u8>, TestProperties> {
    (engine.as_ref() as &dyn Any).downcast_ref().unwrap()
}

fn put(engine: &Arc<TestEngine>, memory: &Memory, key: u64, value: Vec<u8>) {
    let entry = memory.insert(key, value);
    engine.enqueue(
        entry.piece().into(),
        EntrySerializer::estimated_size(entry.key(), entry.value()),
    );
}

async fn value(engine: &Arc<TestEngine>, key: u64) -> Option<Vec<u8>> {
    engine.load(key).await.unwrap().kv().map(|(_, value)| value)
}

#[test_log::test(tokio::test)]
async fn packed_fifo_update_and_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let memory = memory();
    let engine = open(config(dir.path(), 1, PAGE), RecoverMode::Strict).await;
    // Three 1 KiB values share one physical page, including keys and headers.
    for key in 0..3 {
        put(&engine, &memory, key, vec![key as u8; 1024]);
    }
    engine.wait().await;
    let entries = concrete(&engine).shared.read(0).await.unwrap();
    assert_eq!(entries.len(), 3);
    assert!(bucket::HEADER + entries.iter().map(bucket::Entry::len).sum::<usize>() <= PAGE);
    assert_eq!(engine.device().unwrap().partitions(), 1);
    for key in 0..3 {
        assert_eq!(value(&engine, key).await, Some(vec![key as u8; 1024]));
    }
    // Updating moves the key to the FIFO tail; inserting then evicts key 1.
    put(&engine, &memory, 0, vec![9; 1024]);
    put(&engine, &memory, 3, vec![3; 1024]);
    engine.wait().await;
    assert_eq!(value(&engine, 1).await, None);
    assert_eq!(value(&engine, 0).await, Some(vec![9; 1024]));
    engine.delete(2);
    assert!(!engine.may_contains(2));
    engine.close().await.unwrap();
    drop(engine);
    let engine = open(config(dir.path(), 1, PAGE), RecoverMode::Strict).await;
    assert_eq!(value(&engine, 1).await, None);
    assert_eq!(value(&engine, 2).await, None);
    assert_eq!(value(&engine, 0).await, Some(vec![9; 1024]));
    assert_eq!(value(&engine, 3).await, Some(vec![3; 1024]));
    // A definite bloom miss performs no device read.
    let missing = (100..1000).find(|key| !engine.may_contains(*key)).unwrap();
    let reads = engine.statistics().disk_read_ios();
    assert!(engine.load(missing).await.unwrap().is_miss());
    assert_eq!(reads, engine.statistics().disk_read_ios());
    engine.close().await.unwrap();
}

#[test_log::test(tokio::test)]
async fn compression_large_bucket_and_oversized_replacement() {
    for compression in [Compression::None, Compression::Lz4, Compression::Zstd] {
        let dir = tempfile::tempdir().unwrap();
        let memory = memory();
        let engine = open(
            config(dir.path(), 2, 4 * PAGE).with_compression(compression),
            RecoverMode::Strict,
        )
        .await;
        put(&engine, &memory, 1, vec![5; 8192]);
        engine.close().await.unwrap();
        assert_eq!(value(&engine, 1).await, Some(vec![5; 8192]));
        drop(engine);
        let engine = open(config(dir.path(), 2, 4 * PAGE), RecoverMode::Strict).await;
        assert_eq!(value(&engine, 1).await, Some(vec![5; 8192]));
        // The current uncompressed writer rejects this but must remove the old value.
        put(&engine, &memory, 1, vec![6; 4 * PAGE]);
        engine.close().await.unwrap();
        assert_eq!(value(&engine, 1).await, None);
        drop(engine);
        let engine = open(config(dir.path(), 2, 4 * PAGE), RecoverMode::Strict).await;
        assert_eq!(value(&engine, 1).await, None);
        engine.close().await.unwrap();
    }
}

#[test_log::test(tokio::test)]
async fn overflow_delete_and_destroy_do_not_resurrect() {
    let dir = tempfile::tempdir().unwrap();
    let memory = memory();
    let engine = open(config(dir.path(), 2, PAGE).with_queue_capacity(2), RecoverMode::Strict).await;
    put(&engine, &memory, 0, vec![1; 100]);
    engine.wait().await;
    // No yield: two queued writes fill the shard, then deletion overflows it.
    put(&engine, &memory, 2, vec![2; 100]);
    put(&engine, &memory, 4, vec![4; 100]);
    engine.delete(0);
    engine.close().await.unwrap();
    drop(engine);
    let engine = open(config(dir.path(), 2, PAGE), RecoverMode::Strict).await;
    for key in [0, 2, 4] {
        assert_eq!(value(&engine, key).await, None);
    }
    for key in 0..4 {
        put(&engine, &memory, key, vec![key as u8; 100]);
    }
    engine.destroy().await.unwrap();
    for key in 0..4 {
        assert_eq!(value(&engine, key).await, None);
    }
    put(&engine, &memory, 7, vec![7; 100]);
    engine.close().await.unwrap();
    drop(engine);
    let engine = open(config(dir.path(), 2, PAGE), RecoverMode::Strict).await;
    assert_eq!(value(&engine, 7).await, Some(vec![7; 100]));
    for key in 0..4 {
        assert_eq!(value(&engine, key).await, None);
    }
    engine.close().await.unwrap();
}

#[test_log::test(tokio::test)]
async fn byte_limit_and_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let memory = memory();
    let engine = open(config(dir.path(), 1, PAGE).with_queue_bytes(128), RecoverMode::Strict).await;
    put(&engine, &memory, 1, vec![1; 10]);
    engine.wait().await;
    put(&engine, &memory, 1, vec![2; 256]);
    engine.close().await.unwrap();
    assert_eq!(value(&engine, 1).await, None);
    put(&engine, &memory, 1, vec![3; 10]);
    engine.close().await.unwrap();
    assert_eq!(value(&engine, 1).await, None);
    assert_eq!(engine.destroy().await.unwrap_err().kind(), ErrorKind::Closed);
}

#[test_log::test(tokio::test)]
async fn corrupt_bucket_recovery_modes_and_geometry() {
    let dir = tempfile::tempdir().unwrap();
    let memory = memory();
    let engine = open(config(dir.path(), 2, PAGE), RecoverMode::Strict).await;
    put(&engine, &memory, 0, vec![0; 100]);
    put(&engine, &memory, 1, vec![1; 100]);
    engine.close().await.unwrap();
    let shared = &concrete(&engine).shared;
    let mut corrupt = IoSliceMut::new(PAGE);
    corrupt.fill(0);
    corrupt[PAGE - 1] = 1;
    let (_, result) = shared.io.write(Box::new(corrupt), shared.partition.as_ref(), 0).await;
    result.unwrap();
    drop(engine);
    assert!(
        config(dir.path(), 2, PAGE)
            .open::<u64, Vec<u8>, TestProperties>(context(RecoverMode::Strict))
            .await
            .is_err()
    );
    let engine = open(config(dir.path(), 2, PAGE), RecoverMode::Quiet).await;
    assert_eq!(value(&engine, 0).await, None);
    assert_eq!(value(&engine, 1).await, Some(vec![1; 100]));
    engine.close().await.unwrap();
    drop(engine);
    let engine = open(config(dir.path(), 2, PAGE), RecoverMode::None).await;
    assert_eq!(value(&engine, 1).await, None);
    engine.close().await.unwrap();
    drop(engine);
    // Changing geometry cannot reinterpret valid records as a different layout.
    assert!(
        config(dir.path(), 1, 2 * PAGE)
            .open::<u64, Vec<u8>, TestProperties>(context(RecoverMode::Strict))
            .await
            .is_err()
    );
}

#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn concurrent_same_bucket_mutations() {
    let dir = tempfile::tempdir().unwrap();
    let memory = memory();
    let engine = open(config(dir.path(), 4, PAGE), RecoverMode::Strict).await;
    let mut handles = vec![];
    for key in 0..20 {
        let engine = engine.clone();
        let memory = memory.clone();
        handles.push(tokio::spawn(async move {
            for version in 0..5 {
                put(&engine, &memory, key, vec![version; 32]);
                tokio::task::yield_now().await;
            }
            if key % 2 == 0 {
                engine.delete(key);
            }
        }));
    }
    for handle in handles {
        handle.await.unwrap();
    }
    // Repeated close calls must both wait for workers to finish.
    let (a, b) = tokio::join!(engine.close(), engine.close());
    a.unwrap();
    b.unwrap();
    drop(engine);
    let engine = open(config(dir.path(), 4, PAGE), RecoverMode::Strict).await;
    for key in 0..20 {
        assert_eq!(
            value(&engine, key).await,
            if key % 2 == 0 { None } else { Some(vec![4; 32]) }
        );
    }
    engine.close().await.unwrap();
}

#[test]
fn bucket_format_validates_lengths_and_checksum() {
    let entry = bucket::Entry::encode(0, &0u64, &vec![1u8; 100], Compression::None, PAGE - bucket::HEADER).unwrap();
    let mut bytes = bucket::encode(&[entry], PAGE, 0, 1);
    assert_eq!(bucket::decode(&bytes, 0, 1).unwrap().len(), 1);
    assert!(bucket::decode(&bytes[..16], 0, 1).is_err());
    assert!(bucket::decode(&bytes, 1, 2).is_err());
    bytes[70] ^= 1;
    assert_eq!(
        bucket::decode(&bytes, 0, 1).unwrap_err().kind(),
        ErrorKind::ChecksumMismatch
    );
    // Checksum-valid malformed record lengths must return an error, not panic.
    bytes[bucket::HEADER + 8..bucket::HEADER + 12].copy_from_slice(&u32::MAX.to_be_bytes());
    let checksum = crate::serde::Checksummer::checksum64(&bytes[8..]);
    bytes[..8].copy_from_slice(&checksum.to_be_bytes());
    assert!(bucket::decode(&bytes, 0, 1).is_err());
}

#[test_log::test(tokio::test)]
async fn invalid_config_rejected() {
    let dir = tempfile::tempdir().unwrap();
    for cfg in [
        config(dir.path(), 1, PAGE).with_bucket_size(0),
        config(dir.path(), 1, PAGE).with_bucket_size(512),
        config(dir.path(), 1, PAGE).with_shards(0),
        config(dir.path(), 1, PAGE).with_queue_capacity(0),
        config(dir.path(), 1, PAGE).with_queue_bytes(0),
    ] {
        assert_eq!(
            cfg.open::<u64, Vec<u8>, TestProperties>(context(RecoverMode::Strict))
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Config
        );
    }
}

#[derive(Debug)]
struct FailingIoConfig(Arc<AtomicBool>);
#[derive(Debug)]
struct FailingIo {
    inner: Arc<dyn IoEngine>,
    fail: Arc<AtomicBool>,
}
impl IoEngineConfig for FailingIoConfig {
    fn build(self: Box<Self>, ctx: IoEngineBuildContext) -> BoxFuture<'static, Result<Arc<dyn IoEngine>>> {
        async move {
            let inner = PsyncIoEngineConfig::new().boxed().build(ctx).await?;
            Ok(Arc::new(FailingIo { inner, fail: self.0 }) as Arc<dyn IoEngine>)
        }
        .boxed()
    }
}
impl IoEngine for FailingIo {
    fn read(&self, buf: Box<dyn IoBufMut>, partition: &dyn Partition, offset: u64) -> IoHandle {
        self.inner.read(buf, partition, offset)
    }
    fn write(&self, buf: Box<dyn IoBuf>, partition: &dyn Partition, offset: u64) -> IoHandle {
        if self.fail.load(Ordering::Relaxed) {
            async move { (buf.into_iob(), Err(Error::new(ErrorKind::Io, "injected write failure"))) }
                .boxed()
                .into()
        } else {
            self.inner.write(buf, partition, offset)
        }
    }
}

#[test_log::test(tokio::test)]
async fn background_failure_survives_repeated_close() {
    let dir = tempfile::tempdir().unwrap();
    let memory = memory();
    let fail = Arc::new(AtomicBool::new(false));
    let mut ctx = context(RecoverMode::Strict);
    ctx.io_engine_config = Box::new(FailingIoConfig(fail.clone()));
    let engine: Arc<TestEngine> = config(dir.path(), 1, PAGE).open(ctx).await.unwrap();
    put(&engine, &memory, 0, vec![1; 100]);
    engine.wait().await;
    fail.store(true, Ordering::Relaxed);
    engine.delete(0);
    engine.wait().await;
    assert_eq!(value(&engine, 0).await, None);
    fail.store(false, Ordering::Relaxed);
    put(&engine, &memory, 1, vec![2; 100]);
    engine.wait().await;
    assert_eq!(value(&engine, 0).await, None);
    assert_eq!(value(&engine, 1).await, Some(vec![2; 100]));
    assert!(engine.close().await.is_err());
    assert!(engine.close().await.is_err());
}

#[test_log::test(tokio::test)]
async fn canceled_destroy_still_clears_an_inflight_insert() {
    let dir = tempfile::tempdir().unwrap();
    let memory = memory();
    let engine = open(config(dir.path(), 4, PAGE), RecoverMode::Strict).await;
    let shard = concrete(&engine).shards[0].clone();
    let hold = shard.io.lock().await;
    put(&engine, &memory, 0, vec![1; 100]);
    tokio::task::yield_now().await;
    assert!(shard.state.lock().busy);
    let destroying = tokio::spawn(engine.destroy());
    tokio::task::yield_now().await;
    assert!(shard.state.lock().clear_next.is_some());
    destroying.abort();
    drop(hold);
    engine.close().await.unwrap();
    drop(engine);
    let engine = open(config(dir.path(), 4, PAGE), RecoverMode::Strict).await;
    assert_eq!(value(&engine, 0).await, None);
    engine.close().await.unwrap();
}

#[test_log::test(tokio::test)]
async fn store_checks_full_keys_on_hash_collision() {
    let dir = tempfile::tempdir().unwrap();
    let memory: Cache<u128, String, ModHasher> = CacheBuilder::new(10).with_hash_builder(ModHasher::default()).build();
    let first = memory.insert(1, "first".to_owned());
    let second = memory.insert(2 + u64::MAX as u128, "second".to_owned());
    assert_eq!(first.hash(), second.hash());
    let store = crate::StoreBuilder::new("small", memory, Arc::new(Metrics::noop()))
        .with_engine_config(config(dir.path(), 2, PAGE))
        .build()
        .await
        .unwrap();
    store.enqueue(first.piece(), true);
    store.enqueue(second.piece(), true);
    store.wait().await;
    assert!(store.load(first.key()).await.unwrap().is_miss());
    assert_eq!(store.load(second.key()).await.unwrap().kv().unwrap().1, "second");
    store.close().await.unwrap();
}

#[cfg(all(target_os = "linux", not(madsim)))]
#[test_log::test(tokio::test)]
async fn uring_bucket_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let memory = memory();
    let mut ctx = context(RecoverMode::Strict);
    ctx.io_engine_config = crate::UringIoEngineConfig::new()
        .with_threads(1)
        .with_io_depth(16)
        .boxed();
    let engine: Arc<TestEngine> = config(dir.path(), 4, PAGE).open(ctx).await.unwrap();
    for key in 0..10 {
        put(&engine, &memory, key, vec![key as u8; 128]);
    }
    engine.wait().await;
    for key in 0..10 {
        assert_eq!(value(&engine, key).await, Some(vec![key as u8; 128]));
    }
    engine.close().await.unwrap();
}
