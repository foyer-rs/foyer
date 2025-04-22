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

//! foyer benchmark tools.

#![warn(clippy::allow_attributes)]

mod analyze;
mod exporter;
mod rate;
mod text;

use std::{
    collections::BTreeMap,
    fs::create_dir_all,
    net::SocketAddr,
    ops::{Deref, Range},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use analyze::{Metrics, analyze, monitor};
use bytesize::ByteSize;
use clap::{ArgGroup, Parser, builder::PossibleValuesParser};
use exporter::PrometheusExporter;
use foyer::{
    Code, CodeError, Compression, DirectFileDeviceOptions, DirectFsDeviceOptions, Engine, FifoConfig, FifoPicker,
    HybridCache, HybridCacheBuilder, HybridCachePolicy, InvalidRatioPicker, LargeEngineOptions, LfuConfig, LruConfig,
    RecoverMode, RuntimeOptions, S3FifoConfig, SmallEngineOptions, Throttle, TokioRuntimeOptions, TracingOptions,
};
use futures_util::future::join_all;
use itertools::Itertools;
use mixtrics::registry::prometheus::PrometheusMetricsRegistry;
use prometheus::Registry;
use rand::{Rng, SeedableRng, distr::Distribution, rngs::StdRng};
use rate::RateLimiter;
use text::text;
use tokio::sync::{broadcast, oneshot};

use crate::analyze::IoStat;

#[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about)]
#[command(group = ArgGroup::new("exclusive").required(true).args(&["file", "dir", "no_disk"]))]
struct Args {
    /// Run with in-memory cache compatible mode.
    ///
    /// One of `no_disk`, `file`, `dir` must be set.
    #[arg(long)]
    no_disk: bool,

    /// File for disk cache data. Use `DirectFile` as device.
    ///
    /// One of `no_disk`, `file`, `dir` must be set.
    #[arg(short, long)]
    file: Option<String>,

    /// Directory for disk cache data. Use `DirectFs` as device.
    ///
    /// One of `no_disk`, `file`, `dir` must be set.
    #[arg(short, long)]
    dir: Option<String>,

    /// In-memory cache capacity.
    #[arg(long, default_value_t = ByteSize::gib(1))]
    mem: ByteSize,

    /// Disk cache capacity.
    #[arg(long, default_value_t = ByteSize::gib(1))]
    disk: ByteSize,

    /// (s)
    #[arg(short, long, default_value_t = 60)]
    time: u64,

    /// (s)
    #[arg(long, default_value_t = 2)]
    report_interval: u64,

    /// Write rate limit per writer.
    #[arg(long, default_value_t = ByteSize::b(0))]
    w_rate: ByteSize,

    /// Read rate limit per reader.
    #[arg(long, default_value_t = ByteSize::b(0))]
    r_rate: ByteSize,

    /// Min entry size.
    #[arg(long, default_value_t = ByteSize::kib(64))]
    entry_size_min: ByteSize,

    /// Max entry size.
    #[arg(long, default_value_t = ByteSize::kib(64))]
    entry_size_max: ByteSize,

    /// Reader lookup key range.
    #[arg(long, default_value_t = 10000)]
    get_range: u64,

    /// Disk cache region size.
    #[arg(long, default_value_t = ByteSize::mib(64))]
    region_size: ByteSize,

    /// Flusher count.
    #[arg(long, default_value_t = 4)]
    flushers: usize,

    /// Reclaimer count.
    #[arg(long, default_value_t = 4)]
    reclaimers: usize,

    /// Writer count.
    #[arg(long, default_value_t = 16)]
    writers: usize,

    /// Reader count.
    #[arg(long, default_value_t = 16)]
    readers: usize,

    #[arg(long, value_enum, default_value_t = RecoverMode::None)]
    recover_mode: RecoverMode,

    /// Recover concurrency.
    #[arg(long, default_value_t = 16)]
    recover_concurrency: usize,

    /// Disk write iops throttle.
    #[arg(long, default_value_t = 0)]
    disk_write_iops: usize,

    /// Disk read iops throttle.
    #[arg(long, default_value_t = 0)]
    disk_read_iops: usize,

    /// Disk write throughput throttle.
    #[arg(long, default_value_t = ByteSize::b(0))]
    disk_write_throughput: ByteSize,

    /// Disk read throughput throttle.
    #[arg(long, default_value_t = ByteSize::b(0))]
    disk_read_throughput: ByteSize,

    /// `0` means use default.
    #[arg(long, default_value_t = 0)]
    clean_region_threshold: usize,

    /// Shards of both in-memory cache and disk cache indexer.
    #[arg(long, default_value_t = 64)]
    shards: usize,

    /// weigher to enable metrics exporter
    #[arg(long, default_value_t = false)]
    metrics: bool,

    /// Benchmark user runtime worker threads.
    #[arg(long, default_value_t = 0)]
    user_runtime_worker_threads: usize,

    /// Dedicated runtime type.
    #[arg(long, value_parser = PossibleValuesParser::new(["disabled", "unified", "separated"]), default_value = "disabled")]
    runtime: String,

    /// Dedicated runtime worker threads.
    ///
    /// Only valid when using unified dedicated runtime.
    #[arg(long, default_value_t = 0)]
    runtime_worker_threads: usize,

    /// Max threads for blocking io.
    ///
    /// Only valid when using unified dedicated runtime.
    #[arg(long, default_value_t = 0)]
    runtime_max_blocking_threads: usize,

    /// Dedicated runtime for writes worker threads.
    ///
    /// Only valid when using separated dedicated runtime.
    #[arg(long, default_value_t = 0)]
    write_runtime_worker_threads: usize,

    /// Dedicated runtime for writes Max threads for blocking io.
    ///
    /// Only valid when using separated dedicated runtime.
    #[arg(long, default_value_t = 0)]
    write_runtime_max_blocking_threads: usize,

    /// Dedicated runtime for reads worker threads.
    ///
    /// Only valid when using separated dedicated runtime.
    #[arg(long, default_value_t = 0)]
    read_runtime_worker_threads: usize,

    /// Dedicated runtime for writes max threads for blocking io.
    ///
    /// Only valid when using separated dedicated runtime.
    #[arg(long, default_value_t = 0)]
    read_runtime_max_blocking_threads: usize,

    /// compression algorithm
    #[arg(long, value_enum, default_value_t = Compression::None)]
    compression: Compression,

    // TODO(MrCroxx): use mixed engine by default.
    /// Disk cache engine.
    #[arg(long, default_value_t = Engine::Large)]
    engine: Engine,

    /// Time-series operation distribution.
    ///
    /// Available values: "none", "uniform", "zipf".
    ///
    /// If "uniform" or "zipf" is used, operations will be performed in async mode.
    #[arg(long, default_value = "none")]
    distribution: String,

    /// For `--distribution zipf` only.
    #[arg(long, default_value_t = 100)]
    distribution_zipf_n: usize,

    /// For `--distribution zipf` only.
    #[arg(long, default_value_t = 0.5)]
    distribution_zipf_s: f64,

    // (s)
    #[arg(long, default_value_t = 2)]
    warm_up: u64,

    #[arg(long, default_value_t = false)]
    flush: bool,

    #[arg(long, default_value_t = 0.8)]
    invalid_ratio: f64,

    #[arg(long, value_parser = PossibleValuesParser::new(["lru", "lfu", "fifo", "s3fifo"]), default_value = "lru")]
    eviction: String,

    #[arg(long, value_parser = PossibleValuesParser::new(["eviction", "insertion"]), default_value = "eviction")]
    policy: String,

    #[arg(long, default_value_t = ByteSize::mib(16))]
    buffer_pool_size: ByteSize,

    #[arg(long, default_value_t = ByteSize::kib(4))]
    blob_index_size: ByteSize,

    #[arg(long, default_value_t = ByteSize::kib(16))]
    set_size: ByteSize,

    #[arg(long, default_value_t = 64)]
    set_cache_capacity: usize,

    /// Record insert trace threshold. Only effective with "tracing" feature.
    #[arg(long, default_value = "1s")]
    trace_insert: humantime::Duration,
    /// Record get trace threshold. Only effective with "tracing" feature.
    #[arg(long, default_value = "1s")]
    trace_get: humantime::Duration,
    /// Record obtain trace threshold. Only effective with "tracing" feature.
    #[arg(long, default_value = "1s")]
    trace_obtain: humantime::Duration,
    /// Record remove trace threshold. Only effective with "tracing" feature.
    #[arg(long, default_value = "1s")]
    trace_remove: humantime::Duration,
    /// Record fetch trace threshold. Only effective with "tracing" feature.
    #[arg(long, default_value = "1s")]
    trace_fetch: humantime::Duration,

    #[arg(long, default_value_t = false)]
    flush_on_close: bool,
}

#[derive(Debug)]
enum TimeSeriesDistribution {
    None,
    Uniform { interval: Duration },
    Zipf { n: usize, s: f64, interval: Duration },
}

impl TimeSeriesDistribution {
    fn new(args: &Args) -> Self {
        match args.distribution.as_str() {
            "none" => TimeSeriesDistribution::None,
            "uniform" => {
                // interval = 1 / freq = 1 / (rate / size) = size / rate
                let interval =
                    ((args.entry_size_min + args.entry_size_max).as_u64() >> 1) as f64 / (args.w_rate.as_u64() as f64);
                let interval = Duration::from_secs_f64(interval);
                TimeSeriesDistribution::Uniform { interval }
            }
            "zipf" => {
                // interval = 1 / freq = 1 / (rate / size) = size / rate
                let interval =
                    ((args.entry_size_min + args.entry_size_max).as_u64() >> 1) as f64 / (args.w_rate.as_u64() as f64);
                let interval = Duration::from_secs_f64(interval);
                display_zipf_sample(args.distribution_zipf_n, args.distribution_zipf_s);
                TimeSeriesDistribution::Zipf {
                    n: args.distribution_zipf_n,
                    s: args.distribution_zipf_s,
                    interval,
                }
            }
            other => panic!("unsupported distribution: {}", other),
        }
    }
}

struct Context {
    w_rate: Option<f64>,
    r_rate: Option<f64>,
    counts: Vec<AtomicU64>,
    entry_size_range: Range<usize>,
    get_range: u64,
    time: Duration,
    warm_up: Duration,
    distribution: TimeSeriesDistribution,
    metrics: Metrics,
}

#[derive(Debug, Clone)]
struct Value {
    inner: Arc<Vec<u8>>,
}

impl Deref for Value {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.inner.as_slice()
    }
}

impl Code for Value {
    fn encode(&self, writer: &mut impl std::io::Write) -> std::result::Result<(), foyer::CodeError> {
        self.len().encode(writer)?;
        writer.write_all(self).map_err(CodeError::from)
    }

    #[expect(clippy::uninit_vec)]
    fn decode(reader: &mut impl std::io::Read) -> std::result::Result<Self, foyer::CodeError>
    where
        Self: Sized,
    {
        let len = usize::decode(reader)?;
        let mut v = Vec::with_capacity(len);
        unsafe {
            v.set_len(len);
        }
        reader.read_exact(&mut v).map_err(CodeError::from)?;
        let this = Self { inner: Arc::new(v) };
        Ok(this)
    }

    fn estimated_size(&self) -> usize {
        std::mem::size_of::<usize>() + self.len()
    }
}

#[cfg(feature = "tokio-console")]
fn setup() {
    console_subscriber::init();
}

#[cfg(feature = "tracing")]
fn setup() {
    use fastrace::collector::Config;
    let reporter = fastrace_jaeger::JaegerReporter::new("127.0.0.1:6831".parse().unwrap(), "foyer-bench").unwrap();
    fastrace::set_reporter(reporter, Config::default().report_interval(Duration::from_millis(1)));
}

#[cfg(not(any(feature = "tokio-console", feature = "tracing")))]
fn setup() {
    use tracing_subscriber::{EnvFilter, prelude::*};

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                // .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
                .with_line_number(true),
        )
        .with(EnvFilter::from_default_env())
        .init();
}

#[cfg(not(any(feature = "tracing")))]
fn teardown() {}

#[cfg(feature = "tracing")]
fn teardown() {
    fastrace::flush();
}

fn main() {
    let args = Args::parse();
    println!("{:#?}", args);

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    if args.user_runtime_worker_threads != 0 {
        builder.worker_threads(args.user_runtime_worker_threads);
    }
    builder.thread_name("foyer-bench");
    let runtime = builder.enable_all().build().unwrap();
    runtime.block_on(benchmark(args));
}

async fn benchmark(args: Args) {
    setup();

    #[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
    {
        tracing::info!("[foyer bench]: jemalloc is enabled.");
    }

    #[cfg(feature = "deadlock")]
    {
        std::thread::spawn(move || {
            loop {
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
            }
        });
    }

    assert!(args.get_range > 0, "\"--get-range\" value must be greater than 0");

    let tracing_options = TracingOptions::new()
        .with_record_hybrid_insert_threshold(args.trace_insert.into())
        .with_record_hybrid_get_threshold(args.trace_get.into())
        .with_record_hybrid_obtain_threshold(args.trace_obtain.into())
        .with_record_hybrid_remove_threshold(args.trace_remove.into())
        .with_record_hybrid_fetch_threshold(args.trace_fetch.into());

    let policy = match args.policy.as_str() {
        "eviction" => HybridCachePolicy::WriteOnEviction,
        "insertion" => HybridCachePolicy::WriteOnInsertion,
        _ => panic!("unsupported policy: {}", args.policy),
    };

    let builder = HybridCacheBuilder::new()
        .with_tracing_options(tracing_options)
        .with_policy(policy)
        .with_flush_on_close(args.flush_on_close);

    let builder = if args.metrics {
        let registry = Registry::new();
        let addr: SocketAddr = "0.0.0.0:19970".parse().unwrap();
        PrometheusExporter::new(registry.clone(), addr).run();
        builder
            .with_metrics_registry(Box::new(PrometheusMetricsRegistry::new(registry)))
            .memory(args.mem.as_u64() as _)
            .with_shards(args.shards)
    } else {
        builder.memory(args.mem.as_u64() as _).with_shards(args.shards)
    };

    let builder = match args.eviction.as_str() {
        "lru" => builder.with_eviction_config(LruConfig::default()),
        "lfu" => builder.with_eviction_config(LfuConfig::default()),
        "fifo" => builder.with_eviction_config(FifoConfig::default()),
        "s3fifo" => builder.with_eviction_config(S3FifoConfig::default()),
        _ => panic!("unsupported eviction algorithm: {}", args.eviction),
    };

    if let Some(dir) = args.dir.as_ref() {
        create_dir_all(dir).unwrap();
    }

    let mut builder = builder
        .with_weighter(|_: &u64, value: &Value| u64::BITS as usize / 8 + value.len())
        .storage(args.engine);

    let throttle = Throttle::default()
        .with_read_iops(args.disk_read_iops)
        .with_write_iops(args.disk_write_iops)
        .with_read_throughput(args.disk_read_throughput.as_u64() as _)
        .with_write_throughput(args.disk_write_throughput.as_u64() as _);

    builder = match (args.file.as_ref(), args.dir.as_ref()) {
        (Some(file), None) => builder.with_device_options(
            DirectFileDeviceOptions::new(file)
                .with_capacity(args.disk.as_u64() as _)
                .with_region_size(args.region_size.as_u64() as _)
                .with_throttle(throttle),
        ),
        (None, Some(dir)) => builder.with_device_options(
            DirectFsDeviceOptions::new(dir)
                .with_capacity(args.disk.as_u64() as _)
                .with_file_size(args.region_size.as_u64() as _)
                .with_throttle(throttle),
        ),
        (None, None) => builder,
        _ => unreachable!(),
    };

    builder = builder
        .with_flush(args.flush)
        .with_recover_mode(args.recover_mode)
        .with_compression(args.compression)
        .with_runtime_options(match args.runtime.as_str() {
            "disabled" => RuntimeOptions::Disabled,
            "unified" => RuntimeOptions::Unified(TokioRuntimeOptions {
                worker_threads: args.runtime_worker_threads,
                max_blocking_threads: args.runtime_max_blocking_threads,
            }),
            "separated" => RuntimeOptions::Separated {
                read_runtime_options: TokioRuntimeOptions {
                    worker_threads: args.read_runtime_worker_threads,
                    max_blocking_threads: args.read_runtime_max_blocking_threads,
                },
                write_runtime_options: TokioRuntimeOptions {
                    worker_threads: args.write_runtime_worker_threads,
                    max_blocking_threads: args.write_runtime_max_blocking_threads,
                },
            },
            _ => unreachable!(),
        });

    let mut large = LargeEngineOptions::new()
        .with_indexer_shards(args.shards)
        .with_recover_concurrency(args.recover_concurrency)
        .with_flushers(args.flushers)
        .with_reclaimers(args.reclaimers)
        .with_eviction_pickers(vec![
            Box::new(InvalidRatioPicker::new(args.invalid_ratio)),
            Box::<FifoPicker>::default(),
        ])
        .with_buffer_pool_size(args.buffer_pool_size.as_u64() as _)
        .with_blob_index_size(args.blob_index_size.as_u64() as _);

    let small = SmallEngineOptions::new()
        .with_flushers(args.flushers)
        .with_set_size(args.set_size.as_u64() as _)
        .with_set_cache_capacity(args.set_cache_capacity);

    if args.clean_region_threshold > 0 {
        large = large.with_clean_region_threshold(args.clean_region_threshold);
    }

    let hybrid = builder
        .with_large_object_disk_cache_options(large)
        .with_small_object_disk_cache_options(small)
        .build()
        .await
        .unwrap();

    #[cfg(feature = "tracing")]
    hybrid.enable_tracing();

    let iostat_start = IoStat::snapshot(hybrid.statistics());
    let metrics = Metrics::default();

    let metrics_dump_start = metrics.dump();

    let (stop_tx, _) = broadcast::channel(4096);

    let handle_monitor = tokio::spawn({
        let metrics = metrics.clone();
        let stats = hybrid.statistics().clone();
        monitor(
            stats,
            Duration::from_secs(args.report_interval),
            Duration::from_secs(args.time),
            Duration::from_secs(args.warm_up),
            metrics,
            stop_tx.subscribe(),
        )
    });

    let time = Instant::now();

    let handle_bench = tokio::spawn(bench(args.clone(), hybrid.clone(), metrics.clone(), stop_tx.clone()));

    let handle_signal = tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        tracing::warn!("foyer-bench is cancelled with CTRL-C");
        stop_tx.send(()).unwrap();
    });

    handle_bench.await.unwrap();

    let iostat_end = IoStat::snapshot(hybrid.statistics());
    let metrics_dump_end = metrics.dump();
    let analysis = analyze(
        time.elapsed(),
        &iostat_start,
        &iostat_end,
        &metrics_dump_start,
        &metrics_dump_end,
    );

    let close = {
        let now = Instant::now();
        hybrid.close().await.unwrap();
        now.elapsed()
    };

    handle_monitor.abort();
    handle_signal.abort();

    println!("\nTotal:\n{}", analysis);
    println!("Close takes: {close:?}");

    teardown();
}

async fn bench(args: Args, hybrid: HybridCache<u64, Value>, metrics: Metrics, stop_tx: broadcast::Sender<()>) {
    let w_rate = if args.w_rate.as_u64() == 0 {
        None
    } else {
        Some(args.w_rate.as_u64() as _)
    };
    let r_rate = if args.r_rate.as_u64() == 0 {
        None
    } else {
        Some(args.r_rate.as_u64() as _)
    };

    let counts = (0..args.writers).map(|_| AtomicU64::default()).collect_vec();

    let distribution = TimeSeriesDistribution::new(&args);

    let context = Arc::new(Context {
        w_rate,
        r_rate,
        get_range: args.get_range,
        counts,
        entry_size_range: args.entry_size_min.as_u64() as usize..args.entry_size_max.as_u64() as usize + 1,
        time: Duration::from_secs(args.time),
        warm_up: Duration::from_secs(args.warm_up),
        distribution,
        metrics: metrics.clone(),
    });

    let w_handles = (0..args.writers)
        .map(|id| tokio::spawn(write(id as u64, hybrid.clone(), context.clone(), stop_tx.subscribe())))
        .collect_vec();
    let r_handles = (0..args.readers)
        .map(|_| tokio::spawn(read(hybrid.clone(), context.clone(), stop_tx.subscribe())))
        .collect_vec();

    join_all(w_handles).await;
    join_all(r_handles).await;
}

async fn write(id: u64, hybrid: HybridCache<u64, Value>, context: Arc<Context>, mut stop: broadcast::Receiver<()>) {
    let start = Instant::now();

    let mut limiter = context.w_rate.map(RateLimiter::new);
    let step = context.counts.len() as u64;

    const K: usize = 100;
    const G: usize = 10;

    let zipf_intervals = match context.distribution {
        TimeSeriesDistribution::Zipf { n, s, interval } => {
            let histogram = gen_zipf_histogram(n, s, G, n * K);

            let loop_interval = Duration::from_secs_f64(interval.as_secs_f64() * K as f64);
            let group_cnt = K / G;
            let group_interval = interval.as_secs_f64() * group_cnt as f64;

            if id == 0 {
                println!("loop interval: {loop_interval:?}, zipf intervals: ");
            }

            let mut sum = 0;
            let intervals = histogram
                .values()
                .copied()
                .map(|ratio| {
                    let cnt = ratio * K as f64;
                    sum += cnt as usize;
                    let interval = Duration::from_secs_f64(group_interval / cnt);
                    if id == 0 {
                        println!("    [{cnt:3.0} ==> {interval:010.3?}]");
                    }
                    (sum, interval)
                })
                .collect_vec();

            Some(intervals)
        }
        _ => None,
    };

    let mut osrng = StdRng::from_os_rng();
    let mut c = 0;

    loop {
        let l = Instant::now();

        match stop.try_recv() {
            Err(broadcast::error::TryRecvError::Empty) => {}
            _ => return,
        }
        if start.elapsed() >= context.time + context.warm_up {
            return;
        }

        let idx = id + step * c;
        let entry_size = osrng.random_range(context.entry_size_range.clone());
        let data = Value {
            inner: Arc::new(text(idx as usize, entry_size)),
        };

        // TODO(MrCroxx): Use `let_chains` here after it is stable.
        if let Some(limiter) = &mut limiter {
            if let Some(wait) = limiter.consume(entry_size as f64) {
                tokio::time::sleep(wait).await;
            }
        }

        let time = Instant::now();
        let ctx = context.clone();
        let entry_size = u64::BITS as usize / 8 + data.len();
        let update = || {
            let record = start.elapsed() > ctx.warm_up;
            let lat = time.elapsed().as_micros() as u64;
            ctx.counts[id as usize].fetch_add(1, Ordering::Relaxed);
            if record {
                if let Err(e) = ctx.metrics.insert_lats.write().record(lat) {
                    tracing::error!("metrics error: {:?}, value: {}", e, lat);
                }
                ctx.metrics.insert_ios.fetch_add(1, Ordering::Relaxed);
                ctx.metrics.insert_bytes.fetch_add(entry_size, Ordering::Relaxed);
            }
        };

        let elapsed = l.elapsed();

        match &context.distribution {
            TimeSeriesDistribution::None => {
                hybrid.insert(idx, data);
                update();
            }
            TimeSeriesDistribution::Uniform { interval } => {
                hybrid.insert(idx, data);
                update();
                tokio::time::sleep(interval.saturating_sub(elapsed)).await;
            }
            TimeSeriesDistribution::Zipf { .. } => {
                hybrid.insert(idx, data);
                update();
                let intervals = zipf_intervals.as_ref().unwrap();
                let group = match intervals.binary_search_by_key(&(c as usize % K), |(sum, _)| *sum) {
                    Ok(i) => i,
                    Err(i) => i.min(G - 1),
                };
                tokio::time::sleep(intervals[group].1.saturating_sub(elapsed)).await;
            }
        }

        c += 1;
    }
}

async fn read(hybrid: HybridCache<u64, Value>, context: Arc<Context>, mut stop: broadcast::Receiver<()>) {
    let start = Instant::now();

    let mut limiter = context.r_rate.map(RateLimiter::new);
    let step = context.counts.len() as u64;

    let mut rng = StdRng::seed_from_u64(0);
    let mut osrng = StdRng::from_os_rng();

    loop {
        match stop.try_recv() {
            Err(broadcast::error::TryRecvError::Empty) => {}
            _ => return,
        }
        if start.elapsed() >= context.time + context.warm_up {
            return;
        }

        let w = rng.random_range(0..step); // pick a writer to read form
        let c_w = context.counts[w as usize].load(Ordering::Relaxed);
        if c_w == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
            continue;
        }
        let c = rng.random_range(c_w.saturating_sub(context.get_range / context.counts.len() as u64)..c_w);
        let idx = w + c * step;

        let (miss_tx, mut miss_rx) = oneshot::channel();

        let time = Instant::now();

        let fetch = hybrid.fetch(idx, || {
            let context = context.clone();
            let entry_size = osrng.random_range(context.entry_size_range.clone());
            async move {
                let _ = miss_tx.send(time.elapsed());
                Ok(Value {
                    inner: Arc::new(text(idx as usize, entry_size)),
                })
            }
        });

        let entry = fetch.await.unwrap();
        let lat = time.elapsed().as_micros() as u64;

        let (hit, miss_lat) = if let Ok(elapsed) = miss_rx.try_recv() {
            (false, elapsed.as_micros() as u64)
        } else {
            (true, 0)
        };
        let record = start.elapsed() > context.warm_up;

        if hit {
            let entry_size = entry.len();
            assert_eq!(&text(idx as usize, entry_size), entry.value().inner.as_ref());

            // TODO(MrCroxx): Use `let_chains` here after it is stable.
            if let Some(limiter) = &mut limiter {
                if let Some(wait) = limiter.consume(entry_size as f64) {
                    tokio::time::sleep(wait).await;
                }
            }

            if record {
                if let Err(e) = context.metrics.get_hit_lats.write().record(lat) {
                    tracing::error!("metrics error: {:?}, value: {}", e, lat);
                }
                context.metrics.get_bytes.fetch_add(entry_size, Ordering::Relaxed);
            }
        } else if record {
            if let Err(e) = context.metrics.get_miss_lats.write().record(miss_lat) {
                tracing::error!("metrics error: {:?}, value: {}", e, miss_lat);
            }
            context.metrics.get_miss_ios.fetch_add(1, Ordering::Relaxed);
        }

        if record {
            context.metrics.get_ios.fetch_add(1, Ordering::Relaxed);
        }

        tokio::task::consume_budget().await;
    }
}

fn gen_zipf_histogram(n: usize, s: f64, groups: usize, samples: usize) -> BTreeMap<usize, f64> {
    let step = n / groups;

    let mut rng = rand::rng();
    let zipf = rand_distr::Zipf::new(n as f64, s).unwrap();
    let mut data: BTreeMap<usize, usize> = BTreeMap::default();
    for _ in 0..samples {
        let v = zipf.sample(&mut rng);
        let v = v.round() as usize;
        let g = std::cmp::min(v / step, groups);
        *data.entry(g).or_default() += 1;
    }
    let mut histogram: BTreeMap<usize, f64> = BTreeMap::default();
    for group in 0..groups {
        histogram.insert(
            group,
            data.get(&group).copied().unwrap_or_default() as f64 / samples as f64,
        );
    }
    histogram
}

fn display_zipf_sample(n: usize, s: f64) {
    const W: usize = 100;
    const H: usize = 10;

    let samples = n * 1000;

    let histogram = gen_zipf_histogram(n, s, H, samples);

    let max = histogram.values().copied().fold(0.0, f64::max);

    println!("zipf's diagram [N = {n}][s = {s}][samples = {}]", n * 1000);

    for (g, ratio) in histogram {
        let shares = (ratio / max * W as f64) as usize;
        let bar: String = if shares != 0 {
            "=".repeat(shares)
        } else {
            ".".to_string()
        };
        println!(
            "{:3} : {:6} : {:6.3}% : {}",
            g,
            (samples as f64 * ratio) as usize,
            ratio * 100.0,
            bar
        );
    }
}
