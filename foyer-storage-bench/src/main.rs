//  Copyright 2023 MrCroxx
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

#![feature(let_chains)]
#![feature(lint_reasons)]
#![feature(async_fn_in_trait)]

mod analyze;
mod export;
mod rate;
mod utils;

use std::{
    fs::create_dir_all,
    ops::Range,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use analyze::{analyze, monitor, Metrics};
use clap::Parser;

use foyer_common::code::{Key, Value};
use foyer_intrusive::eviction::lfu::LfuConfig;
use foyer_storage::{
    admission::{
        rated_random::RatedRandomAdmissionPolicy, rated_ticket::RatedTicketAdmissionPolicy,
        AdmissionPolicy,
    },
    device::fs::FsDeviceConfig,
    error::Result,
    reinsertion::{
        rated_random::RatedRandomReinsertionPolicy, rated_ticket::RatedTicketReinsertionPolicy,
        ReinsertionPolicy,
    },
    runtime::{RuntimeConfig, RuntimeStore, RuntimeStoreConfig, RuntimeStoreWriter},
    storage::{Storage, StorageExt, StorageWriter},
    store::{LfuFsStoreConfig, Store, StoreConfig, StoreWriter},
};
use futures::future::join_all;
use itertools::Itertools;
use rand::{
    rngs::{OsRng, StdRng},
    Rng, SeedableRng,
};

use export::MetricsExporter;
use rate::RateLimiter;
use tokio::sync::broadcast;
use utils::{detect_fs_type, dev_stat_path, file_stat_path, iostat, FsType};

#[derive(Parser, Debug, Clone)]
pub struct Args {
    /// dir for cache data
    #[arg(short, long)]
    dir: String,

    /// (MiB)
    #[arg(long, default_value_t = 1024)]
    capacity: usize,

    /// (s)
    #[arg(short, long, default_value_t = 60)]
    time: u64,

    /// (s)
    #[arg(long, default_value_t = 2)]
    report_interval: u64,

    /// Some filesystem (e.g. btrfs) can span across multiple block devices and it's hard to decide
    /// which device to moitor. Use this argument to specify which block device to monitor.
    #[arg(long, default_value = "")]
    iostat_dev: String,

    /// (MiB)
    #[arg(long, default_value_t = 0.0)]
    w_rate: f64,

    /// (MiB)
    #[arg(long, default_value_t = 0.0)]
    r_rate: f64,

    #[arg(long, default_value_t = 64 * 1024)]
    entry_size_min: usize,

    #[arg(long, default_value_t = 64 * 1024)]
    entry_size_max: usize,

    #[arg(long, default_value_t = 10000)]
    lookup_range: u64,

    /// (MiB)
    #[arg(long, default_value_t = 64)]
    region_size: usize,

    /// (MiB)
    #[arg(long, default_value_t = 1024)]
    buffer_pool_size: usize,

    /// Must be power of 2.
    ///
    /// `ring buffer capacity = alitn * ring buffer blocks`
    ///
    /// `defaut capacity = 1 GiB`
    #[arg(long, default_value_t = 256 * 1024)]
    ring_buffer_blocks: usize,

    #[arg(long, default_value_t = 4)]
    flushers: usize,

    #[arg(long, default_value_t = 4)]
    reclaimers: usize,

    #[arg(long, default_value_t = 4096)]
    align: usize,

    #[arg(long, default_value_t = 16 * 1024)]
    io_size: usize,

    #[arg(long, default_value_t = 16)]
    writers: usize,

    #[arg(long, default_value_t = 16)]
    readers: usize,

    #[arg(long, default_value_t = 16)]
    recover_concurrency: usize,

    /// enable rated random admission policy if `random_insert_rate_limit` > 0
    /// (MiB/s)
    #[arg(long, default_value_t = 0)]
    random_insert_rate_limit: usize,

    /// enable rated random reinsertion policy if `random_reinsert_rate_limit` > 0
    /// (MiB/s)
    #[arg(long, default_value_t = 0)]
    random_reinsert_rate_limit: usize,

    /// enable rated ticket admission policy if `ticket_insert_rate_limit` > 0
    /// (MiB/s)
    #[arg(long, default_value_t = 0)]
    ticket_insert_rate_limit: usize,

    /// enable rated ticket reinsetion policy if `ticket_reinsert_rate_limitgit a` > 0
    /// (MiB/s)
    #[arg(long, default_value_t = 0)]
    ticket_reinsert_rate_limit: usize,

    /// (MiB/s)
    #[arg(long, default_value_t = 0)]
    flush_rate_limit: usize,

    /// (MiB/s)
    #[arg(long, default_value_t = 0)]
    reclaim_rate_limit: usize,

    /// (ms)
    #[arg(long, default_value_t = 10)]
    allocation_timeout: usize,

    /// `0` means equal to reclaimer count
    #[arg(long, default_value_t = 0)]
    clean_region_threshold: usize,

    /// the count of allocators is `2 ^ allocator bits`
    ///
    /// Note: The count of allocators should be greater than buffer count.
    ///       (buffer count = buffer pool size / device region size)
    #[arg(long, default_value_t = 0)]
    allocator_bits: usize,

    /// Catalog indices sharding bits.
    #[arg(long, default_value_t = 6)]
    catalog_bits: usize,

    /// weigher to enable metrics exporter
    #[arg(long, default_value_t = false)]
    metrics: bool,

    /// use separate runtime
    #[arg(long, default_value_t = false)]
    runtime: bool,
}

#[derive(Debug)]
pub enum BenchStoreConfig<K, V>
where
    K: Key,
    V: Value,
{
    StoreConfig { config: StoreConfig<K, V> },
    RuntimeStoreConfig { config: RuntimeStoreConfig<K, V> },
}

impl<K, V> Clone for BenchStoreConfig<K, V>
where
    K: Key,
    V: Value,
{
    fn clone(&self) -> Self {
        match self {
            Self::StoreConfig { config } => Self::StoreConfig {
                config: config.clone(),
            },
            Self::RuntimeStoreConfig { config } => Self::RuntimeStoreConfig {
                config: config.clone(),
            },
        }
    }
}

#[derive(Debug)]
pub enum BenchStoreWriter<K, V>
where
    K: Key,
    V: Value,
{
    StoreWriter { writer: StoreWriter<K, V> },
    RuntimeStoreWriter { writer: RuntimeStoreWriter<K, V> },
}

impl<K, V> From<StoreWriter<K, V>> for BenchStoreWriter<K, V>
where
    K: Key,
    V: Value,
{
    fn from(writer: StoreWriter<K, V>) -> Self {
        Self::StoreWriter { writer }
    }
}

impl<K, V> From<RuntimeStoreWriter<K, V>> for BenchStoreWriter<K, V>
where
    K: Key,
    V: Value,
{
    fn from(writer: RuntimeStoreWriter<K, V>) -> Self {
        Self::RuntimeStoreWriter { writer }
    }
}

impl<K, V> StorageWriter for BenchStoreWriter<K, V>
where
    K: Key,
    V: Value,
{
    type Key = K;
    type Value = V;

    fn key(&self) -> &Self::Key {
        match self {
            BenchStoreWriter::StoreWriter { writer } => writer.key(),
            BenchStoreWriter::RuntimeStoreWriter { writer } => writer.key(),
        }
    }

    fn weight(&self) -> usize {
        match self {
            BenchStoreWriter::StoreWriter { writer } => writer.weight(),
            BenchStoreWriter::RuntimeStoreWriter { writer } => writer.weight(),
        }
    }

    fn judge(&mut self) -> bool {
        match self {
            BenchStoreWriter::StoreWriter { writer } => writer.judge(),
            BenchStoreWriter::RuntimeStoreWriter { writer } => writer.judge(),
        }
    }

    fn force(&mut self) {
        match self {
            BenchStoreWriter::StoreWriter { writer } => writer.force(),
            BenchStoreWriter::RuntimeStoreWriter { writer } => writer.force(),
        }
    }

    async fn finish(self, value: Self::Value) -> Result<bool> {
        match self {
            BenchStoreWriter::StoreWriter { writer } => writer.finish(value).await,
            BenchStoreWriter::RuntimeStoreWriter { writer } => writer.finish(value).await,
        }
    }
}

#[derive(Debug)]
pub enum BenchStore<K = u64, V = Vec<u8>>
where
    K: Key,
    V: Value,
{
    Store { store: Store<K, V> },
    RuntimeStore { store: RuntimeStore<K, V> },
}

impl<K, V> Clone for BenchStore<K, V>
where
    K: Key,
    V: Value,
{
    fn clone(&self) -> Self {
        match self {
            Self::Store { store } => Self::Store {
                store: store.clone(),
            },
            Self::RuntimeStore { store } => Self::RuntimeStore {
                store: store.clone(),
            },
        }
    }
}

impl<K, V> Storage for BenchStore<K, V>
where
    K: Key,
    V: Value,
{
    type Key = K;
    type Value = V;
    type Config = BenchStoreConfig<K, V>;
    type Writer = BenchStoreWriter<K, V>;

    async fn open(config: Self::Config) -> Result<Self> {
        match config {
            BenchStoreConfig::StoreConfig { config } => {
                Store::open(config).await.map(|store| Self::Store { store })
            }
            BenchStoreConfig::RuntimeStoreConfig { config } => RuntimeStore::open(config)
                .await
                .map(|store| Self::RuntimeStore { store }),
        }
    }

    fn is_ready(&self) -> bool {
        match self {
            BenchStore::Store { store } => store.is_ready(),
            BenchStore::RuntimeStore { store } => store.is_ready(),
        }
    }

    async fn close(&self) -> Result<()> {
        match self {
            BenchStore::Store { store } => store.close().await,
            BenchStore::RuntimeStore { store } => store.close().await,
        }
    }

    fn writer(&self, key: Self::Key, weight: usize) -> Self::Writer {
        match self {
            BenchStore::Store { store } => store.writer(key, weight).into(),
            BenchStore::RuntimeStore { store } => store.writer(key, weight).into(),
        }
    }

    fn exists(&self, key: &Self::Key) -> Result<bool> {
        match self {
            BenchStore::Store { store } => store.exists(key),
            BenchStore::RuntimeStore { store } => store.exists(key),
        }
    }

    async fn lookup(&self, key: &Self::Key) -> Result<Option<Self::Value>> {
        match self {
            BenchStore::Store { store } => store.lookup(key).await,
            BenchStore::RuntimeStore { store } => store.lookup(key).await,
        }
    }

    fn remove(&self, key: &Self::Key) -> Result<bool> {
        match self {
            BenchStore::Store { store } => store.remove(key),
            BenchStore::RuntimeStore { store } => store.remove(key),
        }
    }

    fn clear(&self) -> Result<()> {
        match self {
            BenchStore::Store { store } => store.clear(),
            BenchStore::RuntimeStore { store } => store.clear(),
        }
    }
}

fn is_send_sync_static<T: Send + Sync + 'static>() {}

#[cfg(feature = "tokio-console")]
fn init_logger() {
    console_subscriber::init();
}

#[cfg(feature = "trace")]
fn init_logger() {
    use opentelemetry::sdk::{
        trace::{BatchConfig, Config},
        Resource,
    };
    use opentelemetry_semantic_conventions::resource::SERVICE_NAME;
    use tracing::Level;
    use tracing_subscriber::{filter::Targets, prelude::*};

    let trace_config =
        Config::default().with_resource(Resource::new(vec![opentelemetry::KeyValue::new(
            SERVICE_NAME,
            "foyer-storage-bench",
        )]));
    let batch_config = BatchConfig::default()
        .with_max_queue_size(1048576)
        .with_max_export_batch_size(4096)
        .with_max_concurrent_exports(4);

    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(opentelemetry_otlp::new_exporter().tonic())
        .with_trace_config(trace_config)
        .with_batch_config(batch_config)
        .install_batch(opentelemetry::runtime::Tokio)
        .unwrap();
    let opentelemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    tracing_subscriber::registry()
        .with(
            Targets::new()
                .with_target("foyer_storage", Level::DEBUG)
                .with_target("foyer_common", Level::DEBUG)
                .with_target("foyer_intrusive", Level::DEBUG)
                .with_target("foyer_storage_bench", Level::DEBUG),
        )
        .with(opentelemetry_layer)
        .init();
}

#[cfg(not(any(feature = "tokio-console", feature = "trace")))]
fn init_logger() {
    use tracing_subscriber::{prelude::*, EnvFilter};

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                // .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
                .with_line_number(true),
        )
        .with(EnvFilter::from_default_env())
        .init();
}

#[tokio::main]
async fn main() {
    is_send_sync_static::<BenchStore>();

    init_logger();

    #[cfg(feature = "deadlock")]
    {
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_secs(1));
            let deadlocks = parking_lot::deadlock::check_deadlock();
            if deadlocks.is_empty() {
                continue;
            }

            println!("{} deadlocks detected", deadlocks.len());
            for (i, threads) in deadlocks.iter().enumerate() {
                println!("Deadlock #{}", i);
                for t in threads {
                    println!("Thread Id {:#?}", t.thread_id());
                    println!("{:#?}", t.backtrace());
                }
            }
            panic!()
        });
    }

    let args = Args::parse();

    if args.metrics {
        MetricsExporter::init("0.0.0.0:19970".parse().unwrap());
    }

    println!("{:#?}", args);

    create_dir_all(&args.dir).unwrap();

    let iostat_path = match detect_fs_type(&args.dir) {
        FsType::Tmpfs => panic!("tmpfs is not supported with benches"),
        FsType::Btrfs => {
            if args.iostat_dev.is_empty() {
                panic!("cannot decide which block device to monitor for btrfs, please specify device name with \'--iostat-dev\'")
            } else {
                dev_stat_path(&args.iostat_dev)
            }
        }
        _ => file_stat_path(&args.dir),
    };

    let metrics = Metrics::default();

    let iostat_start = iostat(&iostat_path);
    let metrics_dump_start = metrics.dump();
    let time = Instant::now();

    let eviction_config = LfuConfig {
        window_to_cache_size_ratio: 1,
        tiny_lru_capacity_ratio: 0.01,
    };

    let device_config = FsDeviceConfig {
        dir: PathBuf::from(&args.dir),
        capacity: args.capacity * 1024 * 1024,
        file_capacity: args.region_size * 1024 * 1024,
        align: args.align,
        io_size: args.io_size,
    };

    let mut admissions: Vec<Arc<dyn AdmissionPolicy<Key = u64, Value = Vec<u8>>>> = vec![];
    let mut reinsertions: Vec<Arc<dyn ReinsertionPolicy<Key = u64, Value = Vec<u8>>>> = vec![];
    if args.random_insert_rate_limit > 0 {
        let rr = RatedRandomAdmissionPolicy::new(
            args.random_insert_rate_limit * 1024 * 1024,
            Duration::from_millis(100),
        );
        admissions.push(Arc::new(rr));
    }
    if args.random_reinsert_rate_limit > 0 {
        let rr = RatedRandomReinsertionPolicy::new(
            args.random_reinsert_rate_limit * 1024 * 1024,
            Duration::from_millis(100),
        );
        reinsertions.push(Arc::new(rr));
    }
    if args.ticket_insert_rate_limit > 0 {
        let rt = RatedTicketAdmissionPolicy::new(args.ticket_insert_rate_limit * 1024 * 1024);
        admissions.push(Arc::new(rt));
    }
    if args.ticket_reinsert_rate_limit > 0 {
        let rt = RatedTicketReinsertionPolicy::new(args.ticket_reinsert_rate_limit * 1024 * 1024);
        reinsertions.push(Arc::new(rt));
    }

    let clean_region_threshold = if args.clean_region_threshold == 0 {
        args.reclaimers
    } else {
        args.clean_region_threshold
    };

    let config = LfuFsStoreConfig {
        name: "".to_string(),
        eviction_config,
        device_config,
        allocator_bits: args.allocator_bits,
        ring_buffer_blocks: args.ring_buffer_blocks,
        catalog_bits: args.catalog_bits,
        admissions,
        reinsertions,
        buffer_pool_size: args.buffer_pool_size * 1024 * 1024,
        flushers: args.flushers,
        flush_rate_limit: args.flush_rate_limit * 1024 * 1024,
        reclaimers: args.reclaimers,
        reclaim_rate_limit: args.reclaim_rate_limit * 1024 * 1024,
        recover_concurrency: args.recover_concurrency,
        allocation_timeout: Duration::from_millis(args.allocation_timeout as u64),
        clean_region_threshold,
    };

    let config = if args.runtime {
        BenchStoreConfig::RuntimeStoreConfig {
            config: RuntimeStoreConfig {
                store: config.into(),
                runtime: RuntimeConfig {
                    worker_threads: None,
                    thread_name: Some("foyer".to_string()),
                },
            },
        }
    } else {
        BenchStoreConfig::StoreConfig {
            config: config.into(),
        }
    };

    println!("{config:#?}");

    let store = BenchStore::open(config).await.unwrap();

    let (stop_tx, _) = broadcast::channel(4096);

    let handle_monitor = tokio::spawn({
        let iostat_path = iostat_path.clone();
        let metrics = metrics.clone();

        monitor(
            iostat_path,
            Duration::from_secs(args.report_interval),
            metrics,
            stop_tx.subscribe(),
        )
    });

    let handle_bench = tokio::spawn(bench(
        args.clone(),
        store.clone(),
        metrics.clone(),
        stop_tx.clone(),
    ));

    let handle_signal = tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        tracing::warn!("foyer-storage-bench is cancelled with CTRL-C");
        stop_tx.send(()).unwrap();
    });

    handle_bench.await.unwrap();

    let iostat_end = iostat(&iostat_path);
    let metrics_dump_end = metrics.dump();
    let analysis = analyze(
        time.elapsed(),
        &iostat_start,
        &iostat_end,
        &metrics_dump_start,
        &metrics_dump_end,
    );

    store.close().await.unwrap();

    handle_monitor.abort();
    handle_signal.abort();

    println!("\nTotal:\n{}", analysis);
}

async fn bench(
    args: Args,
    store: impl Storage<Key = u64, Value = Vec<u8>>,
    metrics: Metrics,
    stop_tx: broadcast::Sender<()>,
) {
    let w_rate = if args.w_rate == 0.0 {
        None
    } else {
        Some(args.w_rate * 1024.0 * 1024.0)
    };
    let r_rate = if args.r_rate == 0.0 {
        None
    } else {
        Some(args.r_rate * 1024.0 * 1024.0)
    };

    let index = Arc::new(AtomicU64::new(0));

    let w_handles = (0..args.writers)
        .map(|_| {
            tokio::spawn(write(
                args.entry_size_min..args.entry_size_max + 1,
                w_rate,
                index.clone(),
                store.clone(),
                args.time,
                metrics.clone(),
                stop_tx.subscribe(),
            ))
        })
        .collect_vec();
    let r_handles = (0..args.readers)
        .map(|_| {
            tokio::spawn(read(
                r_rate,
                index.clone(),
                store.clone(),
                args.time,
                metrics.clone(),
                stop_tx.subscribe(),
                args.lookup_range,
            ))
        })
        .collect_vec();

    join_all(w_handles).await;
    join_all(r_handles).await;
}

async fn write(
    entry_size_range: Range<usize>,
    rate: Option<f64>,
    index: Arc<AtomicU64>,
    store: impl Storage<Key = u64, Value = Vec<u8>>,
    time: u64,
    metrics: Metrics,
    mut stop: broadcast::Receiver<()>,
) {
    let start = Instant::now();

    let mut limiter = rate.map(RateLimiter::new);

    loop {
        match stop.try_recv() {
            Err(broadcast::error::TryRecvError::Empty) => {}
            _ => return,
        }
        if start.elapsed().as_secs() >= time {
            return;
        }

        let idx = index.fetch_add(1, Ordering::Relaxed);
        // TODO(MrCroxx): Use random content?
        let entry_size = OsRng.gen_range(entry_size_range.clone());
        let data = vec![idx as u8; entry_size];
        if let Some(limiter) = &mut limiter  && let Some(wait) = limiter.consume(entry_size as f64) {
            tokio::time::sleep(wait).await;
        }

        let time = Instant::now();
        let inserted = store.insert(idx, data).await.unwrap();
        let lat = time.elapsed().as_micros() as u64;
        if let Err(e) = metrics.insert_lats.write().record(lat) {
            tracing::error!("metrics error: {:?}, value: {}", e, lat);
        }

        if inserted {
            metrics.insert_ios.fetch_add(1, Ordering::Relaxed);
            metrics
                .insert_bytes
                .fetch_add(entry_size, Ordering::Relaxed);
        }
    }
}

async fn read(
    rate: Option<f64>,
    index: Arc<AtomicU64>,
    store: impl Storage<Key = u64, Value = Vec<u8>>,
    time: u64,
    metrics: Metrics,
    mut stop: broadcast::Receiver<()>,
    look_up_range: u64,
) {
    let start = Instant::now();

    let mut limiter = rate.map(RateLimiter::new);

    let mut rng = StdRng::seed_from_u64(0);

    loop {
        match stop.try_recv() {
            Err(broadcast::error::TryRecvError::Empty) => {}
            _ => return,
        }
        if start.elapsed().as_secs() >= time {
            return;
        }

        let idx_max = index.load(Ordering::Relaxed);
        let idx = rng.gen_range(std::cmp::max(idx_max, look_up_range) - look_up_range..=idx_max);

        let time = Instant::now();
        let res = store.lookup(&idx).await.unwrap();
        let lat = time.elapsed().as_micros() as u64;

        if let Some(buf) = res {
            let entry_size = buf.len();
            assert_eq!(vec![idx as u8; entry_size], buf);
            if let Err(e) = metrics.get_hit_lats.write().record(lat) {
                tracing::error!("metrics error: {:?}, value: {}", e, lat);
            }
            metrics.get_bytes.fetch_add(entry_size, Ordering::Relaxed);

            if let Some(limiter) = &mut limiter  && let Some(wait) = limiter.consume(entry_size as f64) {
                tokio::time::sleep(wait).await;
            }
        } else {
            if let Err(e) = metrics.get_miss_lats.write().record(lat) {
                tracing::error!("metrics error: {:?}, value: {}", e, lat);
            }
            metrics.get_miss_ios.fetch_add(1, Ordering::Relaxed);
        }
        metrics.get_ios.fetch_add(1, Ordering::Relaxed);

        tokio::task::consume_budget().await;
    }
}
