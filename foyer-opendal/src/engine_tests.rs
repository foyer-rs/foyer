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

//! Crate-local engine tests. HeldEngine is a test double, not a public API.

#[cfg(feature = "test-redis")]
use std::time::Instant;
use std::{
    future::Future,
    hash::BuildHasher,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::Result;
use foyer::{
    BlockEngineConfig, DefaultHasher, Device, DeviceBuilder, Engine, EngineBuildContext, EngineConfig, ErrorKind,
    FifoConfig, FsDeviceBuilder, HybridCache, HybridCacheBuilder, HybridCachePolicy, HybridCacheProperties, IoEngine,
    IoEngineBuildContext, IoEngineConfig, Load, PieceRef, PsyncIoEngineConfig, RecoverMode, Statistics,
    StorageFilterResult, Throttle,
};
use futures_util::future::BoxFuture;
use opendal_core::{EntryMode, Operator};

use crate::{OpenDalEngineConfig, codec, object_path};

type Cache = HybridCache<String, Vec<u8>>;
type Backend = dyn Engine<String, Vec<u8>, HybridCacheProperties>;
type Piece = PieceRef<String, Vec<u8>, HybridCacheProperties>;

/// One 2s I/O plus drain/scheduling margin for a single admitted command.
const ONE_COMMAND: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Fs,
    #[cfg(feature = "test-redis")]
    Redis,
}

struct Fixture {
    op: Operator,
    namespace: String,
    root: Option<tempfile::TempDir>,
}

fn kinds() -> Vec<Kind> {
    vec![
        Kind::Fs,
        #[cfg(feature = "test-redis")]
        Kind::Redis,
    ]
}

fn serialize_kind(kind: Kind) -> Option<std::sync::MutexGuard<'static, ()>> {
    match kind {
        Kind::Fs => None,
        #[cfg(feature = "test-redis")]
        Kind::Redis => {
            static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
            Some(LOCK.lock().unwrap_or_else(|e| e.into_inner()))
        }
    }
}

fn next_namespace(name: &str) -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    format!("t/{}/{name}", N.fetch_add(1, Ordering::Relaxed))
}

fn key_hash(key: &str) -> u64 {
    DefaultHasher::default().hash_one(key.to_string())
}

fn object_for(ns: &str, key: &str, sequence: u64) -> String {
    object_path(ns, key_hash(key), sequence)
}

fn version_key(object: &str, version: &str, offset: u64, length: u64) -> String {
    format!("[\"source-a\",{object:?},{version:?},{offset},{length}]")
}

async fn bounded<T>(bound: Duration, fut: impl Future<Output = T>) -> Result<T> {
    tokio::time::timeout(bound, fut)
        .await
        .map_err(|_| anyhow::anyhow!("operation exceeded {bound:?}"))
}

fn fixture(kind: Kind, name: &str) -> Result<Fixture> {
    let namespace = next_namespace(name);
    match kind {
        Kind::Fs => {
            let root = tempfile::tempdir()?;
            let path = root
                .path()
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("non-utf8 tempfile"))?;
            let op = Operator::new(opendal_service_fs::Fs::default().root(path))?;
            Ok(Fixture {
                op,
                namespace,
                root: Some(root),
            })
        }
        #[cfg(feature = "test-redis")]
        Kind::Redis => {
            let op = Operator::new(opendal_service_redis::Redis::default().endpoint(&redis_url()))?;
            Ok(Fixture {
                op,
                namespace,
                root: None,
            })
        }
    }
}

fn engine_config(kind: Kind, name: &str, capacity: usize, queue: usize) -> Result<(OpenDalEngineConfig, Fixture)> {
    let fx = fixture(kind, name)?;
    let config = OpenDalEngineConfig::new(fx.op.clone(), fx.namespace.clone(), capacity, queue);
    Ok((config, fx))
}

async fn build(config: impl Into<Box<dyn EngineConfig<String, Vec<u8>, HybridCacheProperties>>>) -> Result<Cache> {
    let calls = Arc::new(AtomicU64::new(0));
    let cache = HybridCacheBuilder::new()
        .with_policy(HybridCachePolicy::WriteOnInsertion)
        .memory(1 << 20)
        .with_shards(1)
        .with_weighter(|k: &String, v: &Vec<u8>| k.len() + v.len())
        .storage()
        .with_recover_mode(RecoverMode::None)
        .with_io_engine_config(ProbeIoConfig {
            calls: calls.clone(),
            fail: true,
        })
        .with_engine_config(config)
        .build()
        .await?;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(cache.storage().device().is_none());
    Ok(cache)
}

#[derive(Debug)]
struct ProbeIoConfig {
    calls: Arc<AtomicU64>,
    fail: bool,
}

impl From<ProbeIoConfig> for Box<dyn IoEngineConfig> {
    fn from(value: ProbeIoConfig) -> Self {
        Box::new(value)
    }
}

impl IoEngineConfig for ProbeIoConfig {
    fn build(self: Box<Self>, ctx: IoEngineBuildContext) -> BoxFuture<'static, foyer::Result<Arc<dyn IoEngine>>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail {
            Box::pin(async {
                Err(foyer::Error::new(
                    foyer::ErrorKind::Config,
                    "unexpected block IO construction",
                ))
            })
        } else {
            Box::new(PsyncIoEngineConfig::new()).build(ctx)
        }
    }
}

#[derive(Debug, Default)]
struct Forwarder {
    engine: Option<Arc<Backend>>,
    entries: Vec<(Piece, usize)>,
}

#[derive(Debug)]
struct HeldConfig {
    config: OpenDalEngineConfig,
    forwarder: Arc<Mutex<Forwarder>>,
}

impl From<HeldConfig> for Box<dyn EngineConfig<String, Vec<u8>, HybridCacheProperties>> {
    fn from(value: HeldConfig) -> Self {
        Box::new(value)
    }
}

impl EngineConfig<String, Vec<u8>, HybridCacheProperties> for HeldConfig {
    fn build(self: Box<Self>, ctx: EngineBuildContext) -> BoxFuture<'static, foyer::Result<Arc<Backend>>> {
        Box::pin(async move {
            let engine = Box::new(self.config).build(ctx).await?;
            self.forwarder.lock().unwrap().engine = Some(engine.clone());
            Ok(Arc::new(HeldEngine {
                engine,
                forwarder: self.forwarder,
            }) as Arc<Backend>)
        })
    }
}

#[derive(Debug)]
struct HeldEngine {
    engine: Arc<Backend>,
    forwarder: Arc<Mutex<Forwarder>>,
}

impl Engine<String, Vec<u8>, HybridCacheProperties> for HeldEngine {
    fn device(&self) -> Option<&Arc<dyn Device>> {
        self.engine.device()
    }

    fn statistics(&self) -> &Arc<Statistics> {
        self.engine.statistics()
    }

    fn filter(&self, h: u64, s: usize) -> StorageFilterResult {
        self.engine.filter(h, s)
    }

    fn enqueue(&self, p: Piece, s: usize) {
        self.forwarder.lock().unwrap().entries.push((p, s));
    }

    fn load(&self, h: u64) -> BoxFuture<'static, foyer::Result<Load<String, Vec<u8>, HybridCacheProperties>>> {
        self.engine.load(h)
    }

    fn delete(&self, h: u64) {
        self.engine.delete(h)
    }

    fn may_contains(&self, hash: u64) -> bool {
        self.engine.may_contains(hash)
    }

    fn wait(&self) -> BoxFuture<'static, ()> {
        self.engine.wait()
    }

    fn close(&self) -> BoxFuture<'static, foyer::Result<()>> {
        self.engine.close()
    }

    fn destroy(&self) -> BoxFuture<'static, foyer::Result<()>> {
        self.engine.destroy()
    }
}

async fn held(
    kind: Kind,
    name: &str,
    capacity: usize,
    queue: usize,
) -> Result<(Cache, Arc<Mutex<Forwarder>>, Fixture)> {
    let (config, fx) = engine_config(kind, name, capacity, queue)?;
    let forwarder = Arc::new(Mutex::new(Forwarder::default()));
    let cache = build(HeldConfig {
        config,
        forwarder: forwarder.clone(),
    })
    .await?;
    Ok((cache, forwarder, fx))
}

fn flush(f: &Arc<Mutex<Forwarder>>, reverse: bool) -> (Arc<Backend>, u64) {
    let mut state = f.lock().unwrap();
    let engine = state.engine.clone().unwrap();
    let mut entries = std::mem::take(&mut state.entries);
    let hash = entries[0].0.hash();
    if reverse {
        entries.reverse();
    }
    for (p, s) in entries {
        engine.enqueue(p, s);
    }
    (engine, hash)
}

async fn object_files(fx: &Fixture) -> Result<Vec<String>> {
    Ok(fx
        .op
        .list_with(&format!("{}/", fx.namespace))
        .recursive(true)
        .await?
        .into_iter()
        .filter(|entry| entry.metadata().mode() == EntryMode::FILE)
        .map(|entry| entry.path().to_string())
        .collect())
}

#[cfg(feature = "test-redis")]
fn redis_url() -> String {
    std::env::var("FOYER_OPENDAL_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into())
}

#[cfg(feature = "test-redis")]
fn redis_cmd(args: &[&str]) -> Result<String> {
    use std::{
        io::{Read, Write},
        net::TcpStream,
    };

    let url = redis_url();
    let addr = url.strip_prefix("redis://").unwrap_or(&url);
    let addr = addr.split('/').next().unwrap();
    let mut stream = TcpStream::connect(addr)?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    let mut buf = format!("*{}\r\n", args.len());
    for arg in args {
        buf.push_str(&format!("${}\r\n{arg}\r\n", arg.len()));
    }
    stream.write_all(buf.as_bytes())?;
    let mut resp = vec![0; 256];
    let n = stream.read(&mut resp)?;
    let text = String::from_utf8_lossy(&resp[..n]).into_owned();
    anyhow::ensure!(
        text.contains("+OK") || text.starts_with('+'),
        "redis command failed: {text}"
    );
    Ok(text)
}

#[cfg(feature = "test-redis")]
struct RedisPause;

#[cfg(feature = "test-redis")]
impl Drop for RedisPause {
    fn drop(&mut self) {
        let _ = redis_cmd(&["CLIENT", "UNPAUSE"]);
    }
}

async fn block_writes(kind: Kind, fx: &Fixture) -> Result<Option<WriteBlock>> {
    match kind {
        Kind::Fs => {
            fx.op.write(&fx.namespace, "block directory creation").await?;
            Ok(Some(WriteBlock::Fs))
        }
        #[cfg(feature = "test-redis")]
        Kind::Redis => {
            redis_cmd(&["CLIENT", "PAUSE", "3000", "ALL"])?;
            Ok(Some(WriteBlock::Redis(RedisPause)))
        }
    }
}

async fn unblock_writes(kind: Kind, fx: &Fixture, block: Option<WriteBlock>) -> Result<()> {
    drop(block);
    match kind {
        Kind::Fs => {
            fx.op.delete(&fx.namespace).await?;
            Ok(())
        }
        #[cfg(feature = "test-redis")]
        Kind::Redis => {
            redis_cmd(&["CLIENT", "UNPAUSE"])?;
            Ok(())
        }
    }
}

enum WriteBlock {
    Fs,
    #[cfg(feature = "test-redis")]
    Redis(RedisPause),
}

impl Drop for WriteBlock {
    fn drop(&mut self) {}
}

// A current-thread runtime cannot run the worker between synchronous enqueues
// and the keeper assertion, making pending and reversed arrival deterministic.
#[tokio::test]
async fn reversed_arrival_keeps_pending_readable_and_writes_once() -> Result<()> {
    for kind in kinds() {
        let _guard = serialize_kind(kind);
        let (cache, f, fx) = held(kind, "reverse", 1 << 20, 8192).await?;
        for _ in 0..32 {
            drop(cache.insert("object/v1".into(), vec![7; 4096]));
        }
        let (engine, hash) = flush(&f, true);
        cache.memory().clear();
        assert!(cache.storage().may_contains(&"object/v1".to_string()));
        assert_eq!(
            cache.get(&"object/v1".to_string()).await?.unwrap().value(),
            &vec![7; 4096]
        );
        engine.wait().await;
        assert!(fx.op.exists(&object_path(&fx.namespace, hash, 1)).await?);
        assert_eq!(cache.statistics().disk_write_ios(), 1);
        assert!(cache.statistics().disk_write_bytes() > 4096);
        for _ in 0..32 {
            drop(cache.insert("object/v1".into(), vec![7; 4096]));
        }
        flush(&f, false);
        engine.wait().await;
        assert!(fx.op.exists(&object_path(&fx.namespace, hash, 1)).await?);
        assert!(!fx.op.exists(&object_path(&fx.namespace, hash, 2)).await?);
        cache.memory().clear();
        assert_eq!(
            cache.get(&"object/v1".to_string()).await?.unwrap().value(),
            &vec![7; 4096]
        );
        cache.close().await?;
    }
    Ok(())
}

#[tokio::test]
async fn rejected_admission_retries_after_drain() -> Result<()> {
    for kind in kinds() {
        let _guard = serialize_kind(kind);
        let (cache, f, _fx) = held(kind, "admission", 1 << 20, 8192).await?;
        drop(cache.insert("a/v1".into(), vec![1; 4096]));
        drop(cache.insert("b/v1".into(), vec![2; 4096]));
        let (engine, _) = flush(&f, false);
        cache.memory().clear();
        let admitted = cache.storage().may_contains(&"b/v1".to_string());
        engine.wait().await;
        if !admitted {
            drop(cache.insert("b/v1".into(), vec![2; 4096]));
            flush(&f, false);
            engine.wait().await;
        }
        cache.memory().clear();
        assert_eq!(cache.get(&"b/v1".to_string()).await?.unwrap().value(), &vec![2; 4096]);
        cache.close().await?;
    }
    Ok(())
}

#[tokio::test]
async fn missing_and_corrupt_entries_can_be_repaired() -> Result<()> {
    for kind in kinds() {
        let _guard = serialize_kind(kind);
        for corrupt in [false, true] {
            let name = if corrupt { "corrupt" } else { "missing" };
            let (cache, f, fx) = held(kind, name, 1 << 20, 1 << 20).await?;
            drop(cache.insert("object/v1".into(), vec![7; 4096]));
            let (engine, hash) = flush(&f, false);
            engine.wait().await;
            if corrupt {
                fx.op.write(&object_path(&fx.namespace, hash, 1), "corrupt").await?;
            } else {
                fx.op.delete(&object_path(&fx.namespace, hash, 1)).await?;
            }
            cache.memory().clear();
            let result = cache.get(&"object/v1".to_string()).await;
            if corrupt {
                assert!(result.is_err());
            } else {
                assert!(result?.is_none());
            }
            let fetches = Arc::new(AtomicU64::new(0));
            drop(
                cache
                    .get_or_fetch(&"object/v1".to_string(), {
                        let fetches = fetches.clone();
                        || async move {
                            fetches.fetch_add(1, Ordering::SeqCst);
                            Ok::<_, anyhow::Error>(vec![7; 4096])
                        }
                    })
                    .await?,
            );
            assert_eq!(fetches.load(Ordering::SeqCst), 1);
            flush(&f, false);
            engine.wait().await;
            cache.memory().clear();
            assert_eq!(
                cache.get(&"object/v1".to_string()).await?.unwrap().value(),
                &vec![7; 4096]
            );
            cache.close().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn fifo_eviction_allows_young_entry_refill() -> Result<()> {
    for kind in kinds() {
        let _guard = serialize_kind(kind);
        let (config, _fx) = engine_config(kind, "eviction", 6000, 1 << 20)?;
        let cache = build(config).await?;
        drop(cache.insert("a/v1".into(), vec![1; 4096]));
        cache.storage().wait().await;
        cache.memory().clear();
        drop(cache.get(&"a/v1".to_string()).await?.unwrap());
        drop(cache.insert("b/v1".into(), vec![2; 4096]));
        cache.storage().wait().await;
        drop(
            cache
                .get_or_fetch(&"a/v1".to_string(), || async { Ok::<_, anyhow::Error>(vec![1; 4096]) })
                .await?,
        );
        cache.storage().wait().await;
        cache.memory().clear();
        assert_eq!(cache.get(&"a/v1".to_string()).await?.unwrap().value(), &vec![1; 4096]);
        assert!(cache.get(&"b/v1".to_string()).await?.is_none());
        cache.close().await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_fetches_and_versions() -> Result<()> {
    for kind in kinds() {
        let _guard = serialize_kind(kind);
        let (config, _fx) = engine_config(kind, "fetch", 1 << 20, 1 << 20)?;
        let cache = build(config).await?;
        let calls = Arc::new(AtomicU64::new(0));
        let barrier = Arc::new(tokio::sync::Barrier::new(32));
        let mut tasks = Vec::new();
        for _ in 0..32 {
            let (cache, calls, barrier) = (cache.clone(), calls.clone(), barrier.clone());
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                cache
                    .get_or_fetch(&"object/v1".to_string(), || async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok::<_, anyhow::Error>(vec![1; 4096])
                    })
                    .await
                    .map(|e| e.value().clone())
            }));
        }
        for task in tasks {
            assert_eq!(task.await??, vec![1; 4096]);
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        drop(cache.insert("object/v2".into(), vec![2; 4096]));
        cache.storage().wait().await;
        cache.memory().clear();
        for version in [1, 2] {
            assert_eq!(
                cache.get(&format!("object/v{version}")).await?.unwrap().value(),
                &vec![version; 4096]
            );
        }
        cache.close().await?;
    }
    Ok(())
}

#[tokio::test]
async fn memory_only_does_not_construct_block_io() -> Result<()> {
    let calls = Arc::new(AtomicU64::new(0));
    let cache: Cache = HybridCacheBuilder::new()
        .memory(1 << 20)
        .storage()
        .with_io_engine_config(ProbeIoConfig {
            calls: calls.clone(),
            fail: true,
        })
        .build()
        .await?;
    assert!(cache.storage().device().is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(cache.statistics().disk_read_ios(), 0);
    assert_eq!(cache.statistics().disk_write_ios(), 0);
    drop(cache.insert("a/v1".into(), vec![1; 4096]));
    assert!(cache.get(&"a/v1".to_string()).await?.is_some());
    cache.close().await?;
    Ok(())
}

#[tokio::test]
async fn block_engine_builds_config_and_preserves_device_statistics() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let device = FsDeviceBuilder::new(directory.path())
        .with_capacity(64 << 20)
        .with_throttle(Throttle::default().with_write_throughput(1 << 30))
        .build()?;
    let calls = Arc::new(AtomicU64::new(0));
    let cache: Cache = HybridCacheBuilder::new()
        .with_policy(HybridCachePolicy::WriteOnInsertion)
        .memory(1 << 20)
        .storage()
        .with_io_engine_config(ProbeIoConfig {
            calls: calls.clone(),
            fail: false,
        })
        .with_engine_config(BlockEngineConfig::new(device.clone()).with_block_size(1 << 20))
        .build()
        .await?;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(Arc::ptr_eq(cache.storage().device().unwrap(), &device));
    assert!(Arc::ptr_eq(cache.statistics(), device.statistics()));
    assert_eq!(cache.storage().throttle().write_throughput.unwrap().get(), 1 << 30);
    drop(cache.insert("a/v1".into(), vec![7; 4096]));
    cache.storage().wait().await;
    cache.memory().clear();
    assert_eq!(cache.get(&"a/v1".to_string()).await?.unwrap().value(), &vec![7; 4096]);
    assert!(cache.statistics().disk_write_ios() > 0);
    assert!(cache.statistics().disk_read_ios() > 0);
    cache.close().await?;
    let failed: foyer::Result<Cache> = HybridCacheBuilder::new()
        .memory(1 << 20)
        .storage()
        .with_io_engine_config(ProbeIoConfig {
            calls: calls.clone(),
            fail: true,
        })
        .with_engine_config(BlockEngineConfig::new(device))
        .build()
        .await;
    assert!(failed.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn opendal_flush_on_close_uses_engine_statistics_without_a_device() -> Result<()> {
    for kind in kinds() {
        let _guard = serialize_kind(kind);
        let (config, _fx) = engine_config(kind, "close-flush", 1 << 20, 1 << 20)?;
        let calls = Arc::new(AtomicU64::new(0));
        let cache: Cache = HybridCacheBuilder::new()
            .with_policy(HybridCachePolicy::WriteOnEviction)
            .with_flush_on_close(true)
            .memory(1 << 20)
            .storage()
            .with_recover_mode(RecoverMode::None)
            .with_io_engine_config(ProbeIoConfig {
                calls: calls.clone(),
                fail: true,
            })
            .with_engine_config(config)
            .build()
            .await?;
        assert!(cache.storage().device().is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        drop(cache.insert("object/v1".into(), vec![7; 4096]));
        assert_eq!(cache.statistics().disk_write_ios(), 0);
        cache.close().await?;
        assert_eq!(cache.statistics().disk_write_ios(), 1);
        assert!(cache.statistics().disk_write_bytes() > 4096);
        cache.memory().clear();
        assert_eq!(
            cache.get(&"object/v1".to_string()).await?.unwrap().value(),
            &vec![7; 4096]
        );
        assert_eq!(cache.statistics().disk_read_ios(), 1);
    }
    Ok(())
}

#[tokio::test]
async fn public_path_covers_memory_secondary_source_and_eviction_policy() -> Result<()> {
    for kind in kinds() {
        let _guard = serialize_kind(kind);
        let (config, _fx) = engine_config(kind, "public-path", 1 << 20, 1 << 20)?;
        let cache = build(config).await?;
        let fetches = Arc::new(AtomicU64::new(0));
        let key = "dataset-a/object-1/version-7/bytes-0-4096".to_string();
        for _ in 0..2 {
            let fetches = fetches.clone();
            assert_eq!(
                cache
                    .get_or_fetch(&key, || async move {
                        fetches.fetch_add(1, Ordering::SeqCst);
                        Ok::<_, anyhow::Error>(vec![7; 4096])
                    })
                    .await?
                    .value(),
                &vec![7; 4096]
            );
        }
        assert_eq!(fetches.load(Ordering::SeqCst), 1);
        cache.storage().wait().await;
        assert_eq!(cache.statistics().disk_write_ios(), 1);
        cache.memory().clear();
        assert_eq!(cache.get(&key).await?.unwrap().value(), &vec![7; 4096]);
        assert_eq!(cache.statistics().disk_read_ios(), 1);
        cache.close().await?;

        let (config, _fx) = engine_config(kind, "public-evict", 1 << 20, 1 << 20)?;
        let cache = HybridCacheBuilder::new()
            .with_policy(HybridCachePolicy::WriteOnEviction)
            .memory(8192)
            .with_shards(1)
            .with_eviction_config(FifoConfig::default())
            .with_weighter(|k: &String, v: &Vec<u8>| k.len() + v.len())
            .storage()
            .with_recover_mode(RecoverMode::None)
            .with_io_engine_config(ProbeIoConfig {
                calls: Arc::new(AtomicU64::new(0)),
                fail: true,
            })
            .with_engine_config(config)
            .build()
            .await?;
        let original = "original/v1".to_string();
        drop(cache.insert(original.clone(), vec![9; 4096]));
        for i in 0..16 {
            drop(cache.insert(format!("pressure-{i}/v1"), vec![i as u8; 4096]));
        }
        assert!(cache.memory().get(&original).is_none());
        cache.storage().wait().await;
        assert_eq!(cache.get(&original).await?.unwrap().value(), &vec![9; 4096]);
        assert!(cache.statistics().disk_read_ios() > 0);
        cache.close().await?;
    }
    Ok(())
}

#[tokio::test]
async fn oversized_and_full_admission_do_not_fail_source_fetch() -> Result<()> {
    for kind in kinds() {
        let _guard = serialize_kind(kind);
        let (config, _fx) = engine_config(kind, "oversized", 1024, 1 << 20)?;
        let cache = build(config).await?;
        let fetches = Arc::new(AtomicU64::new(0));
        let got = cache
            .get_or_fetch(&"big/v1".to_string(), {
                let fetches = fetches.clone();
                || async move {
                    fetches.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, anyhow::Error>(vec![1; 4096])
                }
            })
            .await?;
        assert_eq!(got.value(), &vec![1; 4096]);
        assert_eq!(fetches.load(Ordering::SeqCst), 1);
        cache.storage().wait().await;
        assert_eq!(cache.statistics().disk_write_ios(), 0);
        cache.memory().clear();
        assert!(cache.get(&"big/v1".to_string()).await?.is_none());
        cache.close().await?;

        let (cache, f, _fx) = held(kind, "full-source", 1 << 20, 8192).await?;
        drop(cache.insert("a/v1".into(), vec![1; 4096]));
        drop(cache.insert("b/v1".into(), vec![2; 4096]));
        let (engine, _) = flush(&f, false);
        cache.memory().clear();
        let admitted = cache.storage().may_contains(&"b/v1".to_string());
        let fetches = Arc::new(AtomicU64::new(0));
        assert_eq!(
            cache
                .get_or_fetch(&"b/v1".to_string(), {
                    let fetches = fetches.clone();
                    || async move {
                        fetches.fetch_add(1, Ordering::SeqCst);
                        Ok::<_, anyhow::Error>(vec![2; 4096])
                    }
                })
                .await?
                .value(),
            &vec![2; 4096]
        );
        if admitted {
            assert_eq!(fetches.load(Ordering::SeqCst), 0);
        } else {
            assert_eq!(fetches.load(Ordering::SeqCst), 1);
        }
        engine.wait().await;
        if !admitted {
            flush(&f, false);
            engine.wait().await;
        }
        cache.memory().clear();
        assert_eq!(cache.get(&"b/v1".to_string()).await?.unwrap().value(), &vec![2; 4096]);
        cache.close().await?;
    }
    Ok(())
}

#[tokio::test]
async fn failed_write_releases_and_retry_or_close_stays_bounded() -> Result<()> {
    for kind in kinds() {
        let _guard = serialize_kind(kind);
        let (config, fx) = engine_config(kind, "write-fail-obs", 1 << 20, 1 << 20)?;
        let block = block_writes(kind, &fx).await?;
        let cache = build(config).await?;
        drop(cache.insert("object/v1".into(), vec![7; 4096]));
        cache.storage().wait().await;
        assert!(!cache.storage().may_contains(&"object/v1".to_string()));
        assert_eq!(cache.statistics().disk_write_ios(), 0);
        let closed = bounded(ONE_COMMAND, cache.close()).await?;
        assert!(closed.is_err());
        drop(block);
        if kind == Kind::Fs {
            fx.op.delete(&fx.namespace).await?;
        }

        let (config, fx) = engine_config(kind, "write-fail-retry", 1 << 20, 1 << 20)?;
        let block = block_writes(kind, &fx).await?;
        let cache = build(config).await?;
        drop(cache.insert("object/v1".into(), vec![7; 4096]));
        cache.storage().wait().await;
        assert!(!cache.storage().may_contains(&"object/v1".to_string()));
        unblock_writes(kind, &fx, block).await?;
        drop(
            cache
                .get_or_fetch(&"object/v1".to_string(), || async {
                    Ok::<_, anyhow::Error>(vec![7; 4096])
                })
                .await?,
        );
        cache.storage().wait().await;
        cache.memory().clear();
        assert_eq!(
            cache.get(&"object/v1".to_string()).await?.unwrap().value(),
            &vec![7; 4096]
        );
        assert_eq!(cache.statistics().disk_write_ios(), 1);
        let _ = bounded(ONE_COMMAND, cache.close()).await?;
    }
    Ok(())
}

#[tokio::test]
async fn close_drains_admitted_work_then_rejects_new_writes() -> Result<()> {
    for kind in kinds() {
        let _guard = serialize_kind(kind);
        let (config, fx) = engine_config(kind, "close-drain", 1 << 20, 1 << 20)?;
        let cache = build(config).await?;
        drop(cache.insert("object/v1".into(), vec![7; 4096]));
        bounded(ONE_COMMAND, cache.close()).await??;
        let writes = cache.statistics().disk_write_ios();
        assert_eq!(writes, 1);
        if fx.root.is_some() {
            assert_eq!(object_files(&fx).await?.len(), 1);
        }
        drop(cache.insert("late/v1".into(), vec![8; 4096]));
        cache.storage().wait().await;
        assert_eq!(cache.statistics().disk_write_ios(), writes);
        cache.memory().clear();
        assert!(cache.get(&"late/v1".to_string()).await?.is_none());
        assert_eq!(
            cache.get(&"object/v1".to_string()).await?.unwrap().value(),
            &vec![7; 4096]
        );
    }
    Ok(())
}

#[tokio::test]
async fn malformed_objects_never_return_another_key() -> Result<()> {
    for kind in kinds() {
        let _guard = serialize_kind(kind);
        let (cache, f, fx) = held(kind, "malformed", 1 << 20, 1 << 20).await?;
        let valid = codec::encode("object/v1", &[7; 32], 1 << 20)?;
        let mut bad_crc = valid.clone();
        bad_crc[16] ^= 0xff;
        let payloads = [
            codec::encode("other/v1", &[9; 32], 1 << 20)?,
            codec::claimed_huge_value("object/v1"),
            b"FOYODL01".to_vec(),
            b"FODL0001".to_vec(),
            bad_crc,
        ];
        for (sequence, payload) in (1u64..).zip(payloads) {
            drop(cache.insert("object/v1".into(), vec![7; 32]));
            let (engine, hash) = flush(&f, false);
            engine.wait().await;
            fx.op
                .write(&object_path(&fx.namespace, hash, sequence), payload)
                .await?;
            cache.memory().clear();
            if let Ok(Some(entry)) = cache.get(&"object/v1".to_string()).await {
                panic!("malformed object must not return {:?}", entry.value());
            }
        }
        cache.close().await?;
    }
    Ok(())
}

#[tokio::test]
async fn hash_collision_does_not_return_another_key() -> Result<()> {
    #[derive(Debug, Default)]
    struct CollisionHasher;
    impl std::hash::Hasher for CollisionHasher {
        fn finish(&self) -> u64 {
            42
        }
        fn write(&mut self, _: &[u8]) {}
    }
    for kind in kinds() {
        let _guard = serialize_kind(kind);
        let (config, _fx) = engine_config(kind, "collision", 1 << 20, 1 << 20)?;
        let cache = HybridCacheBuilder::new()
            .with_policy(HybridCachePolicy::WriteOnInsertion)
            .memory(1 << 20)
            .with_shards(1)
            .with_hash_builder(std::hash::BuildHasherDefault::<CollisionHasher>::default())
            .with_weighter(|k: &String, v: &Vec<u8>| k.len() + v.len())
            .storage()
            .with_recover_mode(RecoverMode::None)
            .with_io_engine_config(ProbeIoConfig {
                calls: Arc::new(AtomicU64::new(0)),
                fail: true,
            })
            .with_engine_config(config)
            .build()
            .await?;
        drop(cache.insert("a/v1".into(), vec![1; 4096]));
        cache.storage().wait().await;
        drop(cache.insert("b/v1".into(), vec![2; 4096]));
        cache.storage().wait().await;
        cache.memory().clear();
        let a = cache.get(&"a/v1".to_string()).await?;
        assert!(a.is_none(), "colliding lookup must miss, not return the other key");
        assert_eq!(cache.get(&"b/v1".to_string()).await?.unwrap().value(), &vec![2; 4096]);
        cache.close().await?;
    }
    Ok(())
}

#[tokio::test]
async fn namespace_isolation_restart_and_unsupported_recovery() -> Result<()> {
    for kind in kinds() {
        let _guard = serialize_kind(kind);
        let (config, fx) = engine_config(kind, "owned", 6000, 1 << 20)?;
        let run = fx
            .namespace
            .rsplit_once('/')
            .map(|(prefix, _)| prefix.to_string())
            .unwrap();
        let alien = format!("{run}/alien/keep");
        fx.op.write(&alien, "stay").await?;
        let cache = build(config).await?;
        drop(cache.insert("a/v1".into(), vec![1; 4096]));
        cache.storage().wait().await;
        drop(cache.insert("b/v1".into(), vec![2; 4096]));
        cache.storage().wait().await;
        cache.close().await?;
        assert_eq!(fx.op.read(&alien).await?.to_vec(), b"stay");

        let (config, _fx) = engine_config(kind, "restart", 1 << 20, 1 << 20)?;
        let restarted = build(config).await?;
        assert!(restarted.get(&"a/v1".to_string()).await?.is_none());
        assert!(restarted.get(&"b/v1".to_string()).await?.is_none());
        restarted.close().await?;
        if fx.root.is_some() {
            assert!(
                !object_files(&fx).await?.is_empty(),
                "close does not promise physical reclamation"
            );
        }
        assert_eq!(fx.op.read(&alien).await?.to_vec(), b"stay");

        let (config, _fx) = engine_config(kind, "recover-quiet", 1 << 20, 1 << 20)?;
        let failed: foyer::Result<Cache> = HybridCacheBuilder::new()
            .memory(1 << 20)
            .storage()
            .with_recover_mode(RecoverMode::Quiet)
            .with_io_engine_config(ProbeIoConfig {
                calls: Arc::new(AtomicU64::new(0)),
                fail: true,
            })
            .with_engine_config(config)
            .build()
            .await;
        let err = failed.expect_err("Quiet recovery is unsupported");
        assert_eq!(err.kind(), ErrorKind::Config);

        let (_, fx) = engine_config(kind, "bad-ns", 1 << 20, 1 << 20)?;
        let failed: foyer::Result<Cache> = HybridCacheBuilder::new()
            .memory(1 << 20)
            .storage()
            .with_recover_mode(RecoverMode::None)
            .with_io_engine_config(ProbeIoConfig {
                calls: Arc::new(AtomicU64::new(0)),
                fail: true,
            })
            .with_engine_config(OpenDalEngineConfig::new(fx.op, "".into(), 1 << 20, 1 << 20))
            .build()
            .await;
        assert_eq!(
            failed.expect_err("empty namespace is rejected").kind(),
            ErrorKind::Config
        );
    }
    Ok(())
}

#[cfg(feature = "test-redis")]
#[tokio::test]
async fn slow_backend_get_errors_without_get_or_fetch_fallback() -> Result<()> {
    let _guard = serialize_kind(Kind::Redis);
    let (config, _fx) = engine_config(Kind::Redis, "slow-read", 1 << 20, 1 << 20)?;
    let cache = build(config).await?;
    drop(cache.insert("object/v1".into(), vec![8; 4096]));
    cache.storage().wait().await;
    cache.memory().clear();
    redis_cmd(&["CLIENT", "PAUSE", "3000", "ALL"])?;
    let _unpause = RedisPause;
    let start = Instant::now();
    let get_err = bounded(ONE_COMMAND, cache.get(&"object/v1".to_string())).await?;
    assert!(get_err.is_err(), "backend timeout must surface on get");
    assert!(start.elapsed() >= Duration::from_millis(1900));
    drop(_unpause);
    redis_cmd(&["CLIENT", "UNPAUSE"])?;
    bounded(ONE_COMMAND, cache.close()).await??;
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn failed_cleanup_is_observable_and_does_not_touch_foreign_objects() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let (config, fx) = engine_config(Kind::Fs, "cleanup-fail", 6000, 1 << 20)?;
    let run = fx
        .namespace
        .rsplit_once('/')
        .map(|(prefix, _)| prefix.to_string())
        .unwrap();
    let alien = format!("{run}/alien-cleanup/keep");
    fx.op.write(&alien, "stay").await?;
    let cache = build(config).await?;
    drop(cache.insert("a/v1".into(), vec![1; 4096]));
    cache.storage().wait().await;
    let victim = object_files(&fx)
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("expected persisted object"))?;
    let root = fx.root.as_ref().unwrap().path();
    let parent = root.join(&victim).parent().unwrap().to_path_buf();
    std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o555))?;
    drop(cache.insert("b/v1".into(), vec![2; 4096]));
    cache.storage().wait().await;
    let closed = bounded(ONE_COMMAND, cache.close()).await;
    let _ = std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o755));
    assert!(closed?.is_err(), "failed delete must surface on close");
    assert_eq!(fx.op.read(&alien).await?.to_vec(), b"stay");
    Ok(())
}

#[tokio::test]
async fn max_object_size_defaults_to_capacity_and_can_be_narrowed() -> Result<()> {
    for kind in kinds() {
        let _guard = serialize_kind(kind);
        let (_, fx) = engine_config(kind, "max-invalid", 1024, 1024)?;
        for bytes in [0, 2048] {
            let failed: foyer::Result<Cache> = HybridCacheBuilder::new()
                .memory(1 << 20)
                .storage()
                .with_recover_mode(RecoverMode::None)
                .with_io_engine_config(ProbeIoConfig {
                    calls: Arc::new(AtomicU64::new(0)),
                    fail: true,
                })
                .with_engine_config(
                    OpenDalEngineConfig::new(fx.op.clone(), format!("{}/max-{bytes}", fx.namespace), 1024, 1024)
                        .with_max_object_size(bytes),
                )
                .build()
                .await;
            assert_eq!(failed.expect_err("invalid max object size").kind(), ErrorKind::Config);
        }

        let (config, _fx) = engine_config(kind, "max-narrow", 1 << 20, 1 << 20)?;
        let cache = build(config.with_max_object_size(256)).await?;
        let fetches = Arc::new(AtomicU64::new(0));
        assert_eq!(
            cache
                .get_or_fetch(&"big/v1".to_string(), {
                    let fetches = fetches.clone();
                    || async move {
                        fetches.fetch_add(1, Ordering::SeqCst);
                        Ok::<_, anyhow::Error>(vec![1; 4096])
                    }
                })
                .await?
                .value(),
            &vec![1; 4096]
        );
        assert_eq!(fetches.load(Ordering::SeqCst), 1);
        cache.storage().wait().await;
        assert_eq!(cache.statistics().disk_write_ios(), 0);
        cache.memory().clear();
        assert!(cache.get(&"big/v1".to_string()).await?.is_none());
        cache.close().await?;
    }
    Ok(())
}

#[tokio::test]
async fn exact_encoded_max_admits_and_max_plus_one_rejects() -> Result<()> {
    for kind in kinds() {
        let _guard = serialize_kind(kind);
        let (config, _fx) = engine_config(kind, "encoded-max", 1 << 20, 1 << 20)?;
        let cache = build(config.with_max_object_size(21)).await?;
        drop(cache.insert("a".into(), vec![]));
        cache.storage().wait().await;
        assert_eq!(cache.statistics().disk_write_ios(), 1);
        cache.memory().clear();
        assert_eq!(cache.get(&"a".to_string()).await?.unwrap().value(), &Vec::<u8>::new());

        let fetches = Arc::new(AtomicU64::new(0));
        assert_eq!(
            cache
                .get_or_fetch(&"ab".to_string(), {
                    let fetches = fetches.clone();
                    || async move {
                        fetches.fetch_add(1, Ordering::SeqCst);
                        Ok::<_, anyhow::Error>(vec![])
                    }
                })
                .await?
                .value(),
            &Vec::<u8>::new()
        );
        assert_eq!(fetches.load(Ordering::SeqCst), 1);
        cache.storage().wait().await;
        assert_eq!(cache.statistics().disk_write_ios(), 1);
        cache.memory().clear();
        assert!(cache.get(&"ab".to_string()).await?.is_none());
        assert_eq!(cache.get(&"a".to_string()).await?.unwrap().value(), &Vec::<u8>::new());
        cache.close().await?;

        let (config, _fx) = engine_config(kind, "encoded-empty", 1 << 20, 1 << 20)?;
        let cache = build(config.with_max_object_size(20)).await?;
        drop(cache.insert("".into(), vec![]));
        cache.storage().wait().await;
        assert_eq!(cache.statistics().disk_write_ios(), 1);
        cache.memory().clear();
        assert_eq!(cache.get(&"".to_string()).await?.unwrap().value(), &Vec::<u8>::new());
        cache.close().await?;
    }
    Ok(())
}

#[tokio::test]
async fn failed_tiny_write_releases_min_queue_charge() -> Result<()> {
    for kind in kinds() {
        let _guard = serialize_kind(kind);
        let (config, fx) = engine_config(kind, "tiny-fail", 1 << 20, 64)?;
        let block = block_writes(kind, &fx).await?;
        let cache = build(config.with_max_object_size(21)).await?;
        drop(cache.insert("a".into(), vec![]));
        cache.storage().wait().await;
        assert_eq!(cache.statistics().disk_write_ios(), 0);
        unblock_writes(kind, &fx, block).await?;
        drop(cache.insert("b".into(), vec![]));
        cache.storage().wait().await;
        cache.memory().clear();
        assert!(cache.get(&"a".to_string()).await?.is_none());
        assert_eq!(cache.get(&"b".to_string()).await?.unwrap().value(), &Vec::<u8>::new());
        assert_eq!(cache.statistics().disk_write_ios(), 1);
        let _ = bounded(ONE_COMMAND, cache.close()).await?;
    }
    Ok(())
}

#[tokio::test]
async fn background_errors_are_bounded_on_close() -> Result<()> {
    let (config, fx) = engine_config(Kind::Fs, "error-bound", 1 << 20, 1 << 20)?;
    fx.op.write(&fx.namespace, "block directory creation").await?;
    let cache = build(config).await?;
    for i in 0..10 {
        drop(cache.insert(format!("k{i}/v1"), vec![1; 32]));
    }
    cache.storage().wait().await;
    let err = bounded(ONE_COMMAND, cache.close())
        .await?
        .expect_err("write failures must be observable");
    let message = err.to_string();
    assert!(
        message.contains("older errors dropped"),
        "close should report dropped samples beyond eight: {message}"
    );
    Ok(())
}

#[tokio::test]
async fn late_old_source_fetch_does_not_replace_new_version() -> Result<()> {
    for kind in kinds() {
        let _guard = serialize_kind(kind);
        let (config, _fx) = engine_config(kind, "late-source", 1 << 20, 1 << 20)?;
        let cache = build(config).await?;
        let old_key = version_key("object", "v1", 0, 4096);
        let new_key = version_key("object", "v2", 0, 4096);
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let old = cache.get_or_fetch(&old_key, || async move {
            started_tx.send(()).unwrap();
            release_rx.await?;
            Ok::<_, anyhow::Error>(vec![1; 4096])
        });
        started_rx.await?;
        let current = tokio::time::timeout(
            Duration::from_secs(5),
            cache.get_or_fetch(&new_key, || async { Ok::<_, anyhow::Error>(vec![2; 4096]) }),
        )
        .await??;
        assert_eq!(current.value(), &vec![2; 4096]);
        release_tx.send(()).unwrap();
        assert_eq!(old.await?.value(), &vec![1; 4096]);
        cache.storage().wait().await;
        cache.memory().clear();
        assert_eq!(cache.get(&new_key).await?.unwrap().value(), &vec![2; 4096]);
        assert_eq!(cache.get(&old_key).await?.unwrap().value(), &vec![1; 4096]);
        assert_eq!(cache.statistics().disk_read_ios(), 2);
        cache.close().await?;
    }
    Ok(())
}

#[tokio::test]
async fn queued_versions_are_isolated_in_keeper_and_storage() -> Result<()> {
    for kind in kinds() {
        let _guard = serialize_kind(kind);
        let (cache, f, _fx) = held(kind, "queued-versions", 1 << 20, 1 << 20).await?;
        let old_key = version_key("object", "v1", 0, 4096);
        let new_key = version_key("object", "v2", 0, 4096);
        drop(cache.insert(old_key.clone(), vec![1; 4096]));
        drop(cache.insert(new_key.clone(), vec![2; 4096]));
        let (engine, _) = flush(&f, false);
        cache.memory().clear();
        assert_eq!(cache.get(&new_key).await?.unwrap().value(), &vec![2; 4096]);
        assert_eq!(cache.get(&old_key).await?.unwrap().value(), &vec![1; 4096]);
        assert_eq!(cache.statistics().disk_write_ios(), 0);
        engine.wait().await;
        cache.memory().clear();
        assert_eq!(cache.get(&new_key).await?.unwrap().value(), &vec![2; 4096]);
        assert_eq!(cache.get(&old_key).await?.unwrap().value(), &vec![1; 4096]);
        assert_eq!(cache.statistics().disk_read_ios(), 2);
        cache.close().await?;
    }
    Ok(())
}

#[tokio::test]
async fn delayed_old_piece_can_repopulate_only_its_old_key() -> Result<()> {
    for kind in kinds() {
        let _guard = serialize_kind(kind);
        let (config, _fx) = engine_config(kind, "late-piece", 1 << 20, 1 << 20)?;
        let cache = build(config).await?;
        let old_key = version_key("object", "v1", 0, 4096);
        let new_key = version_key("object", "v2", 0, 4096);
        let old = cache.insert(old_key.clone(), vec![1; 4096]);
        let delayed = old.piece();
        cache.storage().wait().await;
        cache.memory().remove(&old_key);
        drop(cache.insert(new_key.clone(), vec![2; 4096]));
        cache.storage().wait().await;
        cache.storage().enqueue(delayed, true);
        drop(old);
        cache.storage().wait().await;
        cache.memory().clear();
        assert_eq!(cache.get(&new_key).await?.unwrap().value(), &vec![2; 4096]);
        assert_eq!(cache.get(&old_key).await?.unwrap().value(), &vec![1; 4096]);
        cache.close().await?;
    }
    Ok(())
}

#[tokio::test]
async fn capacity_reclaims_old_object_without_invalidating_new_content() -> Result<()> {
    for kind in kinds() {
        let _guard = serialize_kind(kind);
        let (config, fx) = engine_config(kind, "capacity", 6000, 1 << 20)?;
        let cache = build(config).await?;
        let old_key = version_key("object", "v1", 0, 4096);
        let new_key = version_key("object", "v2", 0, 4096);
        drop(cache.insert(old_key.clone(), vec![1; 4096]));
        cache.storage().wait().await;
        let old_path = object_for(&fx.namespace, &old_key, 1);
        assert!(fx.op.exists(&old_path).await?);
        drop(cache.insert(new_key.clone(), vec![2; 4096]));
        cache.storage().wait().await;
        assert!(!fx.op.exists(&old_path).await?);
        cache.memory().clear();
        assert!(cache.get(&old_key).await?.is_none());
        assert_eq!(cache.get(&new_key).await?.unwrap().value(), &vec![2; 4096]);
        cache.close().await?;
    }
    Ok(())
}

#[tokio::test]
async fn object_version_and_range_all_contribute_to_identity() -> Result<()> {
    for kind in kinds() {
        let _guard = serialize_kind(kind);
        let (config, _fx) = engine_config(kind, "identity", 1 << 20, 1 << 20)?;
        let cache = build(config).await?;
        let cases = [
            (version_key("a:b", "c", 0, 4), b"abcd".to_vec()),
            (version_key("a", "b:c", 0, 4), b"efgh".to_vec()),
            (version_key("a:b", "c", 4, 4), b"ijkl".to_vec()),
            (version_key("a:b", "d", 0, 4), b"mnop".to_vec()),
        ];
        for (key, value) in &cases {
            drop(cache.insert(key.clone(), value.clone()));
        }
        cache.storage().wait().await;
        cache.memory().clear();
        for (key, expected) in cases {
            assert_eq!(cache.get(&key).await?.unwrap().value(), &expected);
        }
        assert_eq!(cache.statistics().disk_read_ios(), 4);
        cache.close().await?;
    }
    Ok(())
}

#[tokio::test]
async fn varied_payloads_round_trip() -> Result<()> {
    for kind in kinds() {
        let _guard = serialize_kind(kind);
        let (config, _fx) = engine_config(kind, "sizes", 32 << 20, 32 << 20)?;
        let cache = HybridCacheBuilder::new()
            .with_policy(HybridCachePolicy::WriteOnInsertion)
            .memory(4096)
            .with_shards(1)
            .with_weighter(|k: &String, v: &Vec<u8>| k.len() + v.len())
            .storage()
            .with_recover_mode(RecoverMode::None)
            .with_io_engine_config(ProbeIoConfig {
                calls: Arc::new(AtomicU64::new(0)),
                fail: true,
            })
            .with_engine_config(config)
            .build()
            .await?;
        for size in [0, 1, 4095, 4096, 65536, 4 << 20] {
            let key = format!("bytes-{size}");
            let bytes: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
            cache.insert(key.clone(), bytes.clone());
            cache.storage().wait().await;
            cache.memory().clear();
            assert_eq!(cache.get(&key).await?.unwrap().value(), &bytes);
        }
        cache.close().await?;
    }
    Ok(())
}

#[tokio::test]
async fn clear_drains_pending_objects() -> Result<()> {
    for kind in kinds() {
        let _guard = serialize_kind(kind);
        let (config, fx) = engine_config(kind, "clear", 1 << 20, 1 << 20)?;
        let cache = build(config).await?;
        cache.insert("old".to_string(), vec![1; 4096]);
        cache.storage().wait().await;
        let old_path = object_for(&fx.namespace, "old", 1);
        cache.insert("pending".to_string(), vec![2; 4096]);
        cache.clear().await?;
        assert!(cache.get(&"old".to_string()).await?.is_none());
        assert!(cache.get(&"pending".to_string()).await?.is_none());
        assert!(!fx.op.exists(&old_path).await?);
        cache.close().await?;
    }
    Ok(())
}

#[tokio::test]
async fn backend_eviction_becomes_source_miss() -> Result<()> {
    for kind in kinds() {
        let _guard = serialize_kind(kind);
        let (config, fx) = engine_config(kind, "backend-eviction", 1 << 20, 1 << 20)?;
        let cache = build(config).await?;
        let key = "key".to_string();
        cache.insert(key.clone(), vec![1; 4096]);
        cache.storage().wait().await;
        fx.op.delete(&object_for(&fx.namespace, &key, 1)).await?;
        cache.memory().clear();
        let count = Arc::new(AtomicU64::new(0));
        let calls = count.clone();
        assert_eq!(
            cache
                .get_or_fetch(&key, || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, anyhow::Error>(vec![1; 4096])
                })
                .await?
                .value(),
            &vec![1; 4096]
        );
        assert_eq!(count.load(Ordering::SeqCst), 1);
        cache.storage().wait().await;
        cache.close().await?;
    }
    Ok(())
}

#[cfg(feature = "test-redis")]
#[tokio::test]
async fn redis_in_flight_write_retains_one_registration() -> Result<()> {
    let _guard = serialize_kind(Kind::Redis);
    let (config, fx) = engine_config(Kind::Redis, "in-flight", 1 << 20, 8192)?;
    let cache = build(config).await?;
    // Pause writes only. CLIENT PAUSE ALL would also queue EXISTS/GET, so the
    // in-flight SET can complete before those probes run.
    redis_cmd(&["CLIENT", "PAUSE", "1800", "WRITE"])?;
    let _unpause = RedisPause;
    drop(cache.insert("object/v1".into(), vec![7; 4096]));
    tokio::task::yield_now().await;
    assert!(cache.storage().may_contains(&"object/v1".to_string()));
    for _ in 0..32 {
        drop(
            cache
                .get_or_fetch(&"object/v1".to_string(), || async {
                    Err::<Vec<u8>, _>(anyhow::anyhow!("in-flight cache value must remain available"))
                })
                .await?,
        );
    }
    cache.memory().clear();
    assert_eq!(
        cache.get(&"object/v1".to_string()).await?.unwrap().value(),
        &vec![7; 4096]
    );
    assert_eq!(cache.statistics().disk_read_ios(), 0);
    assert_eq!(cache.statistics().disk_write_ios(), 0);
    drop(_unpause);
    redis_cmd(&["CLIENT", "UNPAUSE"])?;
    cache.storage().wait().await;
    assert_eq!(cache.statistics().disk_write_ios(), 1);
    assert!(fx.op.exists(&object_for(&fx.namespace, "object/v1", 1)).await?);
    cache.close().await?;
    Ok(())
}
