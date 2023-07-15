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

mod analyze;
mod rate;
mod utils;

use std::{
    fs::create_dir_all,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use analyze::{analyze, monitor, Metrics};
use clap::Parser;

use foyer_intrusive::eviction::lfu::LfuConfig;
use foyer_storage::{
    admission::{rated_random::RatedRandom, AdmissionPolicy},
    device::fs::FsDeviceConfig,
    store::{PrometheusConfig, StoreConfig},
    LfuFsStore,
};
use futures::future::join_all;
use itertools::Itertools;
use rand::{rngs::StdRng, Rng, SeedableRng};

use rate::RateLimiter;
use tokio::sync::oneshot;
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

    /// must be power of 2
    #[arg(long, default_value_t = 16)]
    pools: usize,

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
    entry_size: usize,

    #[arg(long, default_value_t = 10000)]
    lookup_range: u64,

    /// (MiB)
    #[arg(long, default_value_t = 64)]
    region_size: usize,

    /// (MiB)
    #[arg(long, default_value_t = 1024)]
    buffer_pool_size: usize,

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

    /// enable rated random admission policy if `rated_random` > 0
    /// (MiB/s)
    #[arg(long, default_value_t = 0)]
    rated_random: usize,

    /// (MiB/s)
    #[arg(long, default_value_t = 0)]
    flush_rate_limit: usize,

    /// (MiB/s)
    #[arg(long, default_value_t = 0)]
    reclaim_rate_limit: usize,
}

impl Args {
    fn verify(&self) {
        assert!(self.pools.is_power_of_two());
    }
}

type TStore = LfuFsStore<u64, Vec<u8>>;

fn is_send_sync_static<T: Send + Sync + 'static>() {}

#[tokio::main]
async fn main() {
    is_send_sync_static::<TStore>();

    #[cfg(feature = "tokio-console")]
    console_subscriber::init();

    #[cfg(not(feature = "tokio-console"))]
    {
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
    args.verify();

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
    if args.rated_random > 0 {
        let rr = RatedRandom::new(args.rated_random * 1024 * 1024, Duration::from_millis(100));
        admissions.push(Arc::new(rr));
    }

    let config = StoreConfig {
        eviction_config,
        device_config,
        admissions,
        reinsertions: vec![],
        buffer_pool_size: args.buffer_pool_size * 1024 * 1024,
        flushers: args.flushers,
        flush_rate_limit: args.flush_rate_limit * 1024 * 1024,
        reclaimers: args.reclaimers,
        reclaim_rate_limit: args.reclaim_rate_limit * 1024 * 1024,
        recover_concurrency: args.recover_concurrency,
        event_listeners: vec![],
        prometheus_config: PrometheusConfig::default(),
    };

    println!("{:#?}", config);

    let store = TStore::open(config).await.unwrap();

    let (iostat_stop_tx, iostat_stop_rx) = oneshot::channel();
    let (bench_stop_tx, bench_stop_rx) = oneshot::channel();

    let handle_monitor = tokio::spawn({
        let iostat_path = iostat_path.clone();
        let metrics = metrics.clone();

        monitor(
            iostat_path,
            Duration::from_secs(args.report_interval),
            metrics,
            iostat_stop_rx,
        )
    });
    let handle_signal = tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        bench_stop_tx.send(()).unwrap();
        iostat_stop_tx.send(()).unwrap();
    });
    let handle_bench = tokio::spawn(bench(
        args.clone(),
        store.clone(),
        metrics.clone(),
        bench_stop_rx,
    ));

    handle_bench.await.unwrap();
    handle_monitor.abort();
    handle_signal.abort();

    let iostat_end = iostat(&iostat_path);
    let metrics_dump_end = metrics.dump();
    let analysis = analyze(
        time.elapsed(),
        &iostat_start,
        &iostat_end,
        &metrics_dump_start,
        &metrics_dump_end,
    );
    println!("\nTotal:\n{}", analysis);

    store.shutdown_runners().await.unwrap();
}

async fn bench(args: Args, store: Arc<TStore>, metrics: Metrics, stop: oneshot::Receiver<()>) {
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

    let (w_stop_txs, w_stop_rxs): (Vec<oneshot::Sender<()>>, Vec<oneshot::Receiver<()>>) =
        (0..args.writers).map(|_| oneshot::channel()).unzip();
    let (r_stop_txs, r_stop_rxs): (Vec<oneshot::Sender<()>>, Vec<oneshot::Receiver<()>>) =
        (0..args.readers).map(|_| oneshot::channel()).unzip();

    let w_handles = w_stop_rxs
        .into_iter()
        .map(|w_stop_rx| {
            tokio::spawn(write(
                args.entry_size,
                w_rate,
                index.clone(),
                store.clone(),
                args.time,
                metrics.clone(),
                w_stop_rx,
            ))
        })
        .collect_vec();
    let r_handles = r_stop_rxs
        .into_iter()
        .map(|r_stop_rx| {
            tokio::spawn(read(
                args.entry_size,
                r_rate,
                index.clone(),
                store.clone(),
                args.time,
                metrics.clone(),
                r_stop_rx,
                args.lookup_range,
            ))
        })
        .collect_vec();

    tokio::spawn(async move {
        if let Ok(()) = stop.await {
            for w_stop_tx in w_stop_txs {
                let _ = w_stop_tx.send(());
            }
            for r_stop_tx in r_stop_txs {
                let _ = r_stop_tx.send(());
            }
        }
    });

    join_all(w_handles).await;
    join_all(r_handles).await;
}

#[allow(clippy::too_many_arguments)]
async fn write(
    entry_size: usize,
    rate: Option<f64>,
    index: Arc<AtomicU64>,
    store: Arc<TStore>,
    time: u64,
    metrics: Metrics,
    mut stop: oneshot::Receiver<()>,
) {
    let start = Instant::now();

    let mut limiter = rate.map(RateLimiter::new);

    loop {
        match stop.try_recv() {
            Err(oneshot::error::TryRecvError::Empty) => {}
            _ => return,
        }
        if start.elapsed().as_secs() >= time {
            return;
        }

        let idx = index.fetch_add(1, Ordering::Relaxed);
        // TODO(MrCroxx): Use random content?
        let data = vec![idx as u8; entry_size];
        if let Some(limiter) = &mut limiter  && let Some(wait) = limiter.consume(entry_size as f64) {
            tokio::time::sleep(wait).await;
        }

        let time = Instant::now();
        store.insert(idx, data).await.unwrap();
        let lat = time.elapsed().as_micros() as u64;
        if let Err(e) = metrics.insert_lats.write().record(lat) {
            tracing::error!("metrics error: {:?}, value: {}", e, lat);
        }

        metrics.insert_ios.fetch_add(1, Ordering::Relaxed);
        metrics
            .insert_bytes
            .fetch_add(entry_size, Ordering::Relaxed);
    }
}

#[allow(clippy::too_many_arguments)]
async fn read(
    entry_size: usize,
    rate: Option<f64>,
    index: Arc<AtomicU64>,
    store: Arc<TStore>,
    time: u64,
    metrics: Metrics,
    mut stop: oneshot::Receiver<()>,
    look_up_range: u64,
) {
    let start = Instant::now();

    let mut limiter = rate.map(RateLimiter::new);

    let mut rng = StdRng::seed_from_u64(0);

    loop {
        match stop.try_recv() {
            Err(oneshot::error::TryRecvError::Empty) => {}
            _ => return,
        }
        if start.elapsed().as_secs() >= time {
            return;
        }

        let idx_max = index.load(Ordering::Relaxed);
        let idx = rng.gen_range(std::cmp::max(idx_max, look_up_range) - look_up_range..=idx_max);

        if let Some(limiter) = &mut limiter  && let Some(wait) = limiter.consume(entry_size as f64) {
            tokio::time::sleep(wait).await;
        }

        let time = Instant::now();
        let res = store.lookup(&idx).await.unwrap();
        let lat = time.elapsed().as_micros() as u64;

        if res.is_some() {
            assert_eq!(vec![idx as u8; entry_size], res.unwrap());
            if let Err(e) = metrics.get_hit_lats.write().record(lat) {
                tracing::error!("metrics error: {:?}, value: {}", e, lat);
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
