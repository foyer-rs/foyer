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

use std::fs::create_dir_all;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use analyze::{analyze, monitor, Metrics};
use clap::Parser;

use foyer::{
    ReadOnlyFileStoreConfig, TinyLfuConfig, TinyLfuReadOnlyFileStoreCache,
    TinyLfuReadOnlyFileStoreCacheConfig,
};
use futures::future::join_all;
use itertools::Itertools;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

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

    #[arg(long, default_value_t = 16)]
    concurrency: usize,

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

    #[arg(long, default_value_t = 0.0)]
    w_rate: f64,

    #[arg(long, default_value_t = 0.0)]
    r_rate: f64,

    #[arg(long, default_value_t = 64 * 1024)]
    entry_size: usize,

    #[arg(long, default_value_t = 10000)]
    look_up_range: u64,

    /// read only file store: max cache file size (MiB)
    #[arg(long, default_value_t = 16)]
    rofs_max_file_size: usize,

    /// read only file store: ratio of garbage to trigger reclaim (0 ~ 1)
    #[arg(long, default_value_t = 0.0)]
    rofs_trigger_reclaim_garbage_ratio: f64,

    /// read only file store: ratio of size to trigger reclaim (0 ~ 1)
    #[arg(long, default_value_t = 0.8)]
    rofs_trigger_reclaim_capacity_ratio: f64,

    /// read only file store: ratio of size to trigger randomly drop (0 ~ 1)
    #[arg(long, default_value_t = 0.0)]
    rofs_trigger_random_drop_ratio: f64,

    /// read only file store: ratio of randomly dropped entries (0 ~ 1)
    #[arg(long, default_value_t = 0.0)]
    rofs_random_drop_ratio: f64,
}

impl Args {
    fn verify(&self) {
        assert!(self.pools.is_power_of_two());
    }
}
type TContainer = TinyLfuReadOnlyFileStoreCache<u64, Vec<u8>>;

fn is_send_sync_static<T: Send + Sync + 'static>() {}

#[tokio::main]
async fn main() {
    is_send_sync_static::<TContainer>();

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

    let store_config = ReadOnlyFileStoreConfig {
        dir: PathBuf::from(&args.dir),
        capacity: args.capacity * 1024 * 1024 / args.pools,
        max_file_size: args.rofs_max_file_size * 1024 * 1024,
        trigger_reclaim_garbage_ratio: args.rofs_trigger_reclaim_garbage_ratio,
        trigger_reclaim_capacity_ratio: args.rofs_trigger_reclaim_capacity_ratio,
        trigger_random_drop_ratio: args.rofs_trigger_random_drop_ratio,
        random_drop_ratio: args.rofs_random_drop_ratio,
    };

    let policy_config = TinyLfuConfig {
        window_to_cache_size_ratio: 1,
        tiny_lru_capacity_ratio: 0.01,
    };

    let config = TinyLfuReadOnlyFileStoreCacheConfig {
        capacity: args.capacity * 1024 * 1024,
        pool_count_bits: (args.pools as f64).log2() as usize,
        policy_config,
        store_config,
    };

    println!("{:#?}", config);

    let container = TinyLfuReadOnlyFileStoreCache::open(config).await.unwrap();
    let container = Arc::new(container);

    let (iostat_stop_tx, iostat_stop_rx) = oneshot::channel();
    let (bench_stop_txs, bench_stop_rxs): (Vec<oneshot::Sender<()>>, Vec<oneshot::Receiver<()>>) =
        (0..args.concurrency).map(|_| oneshot::channel()).unzip();

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
        for bench_stop_tx in bench_stop_txs {
            bench_stop_tx.send(()).unwrap();
        }
        iostat_stop_tx.send(()).unwrap();
    });
    let handles = (bench_stop_rxs)
        .into_iter()
        .map(|stop| {
            tokio::spawn(bench(
                args.clone(),
                container.clone(),
                metrics.clone(),
                stop,
            ))
        })
        .collect_vec();

    for handle in handles {
        handle.await.unwrap();
    }
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
}

async fn bench(
    args: Args,
    container: Arc<TContainer>,
    metrics: Metrics,
    stop: oneshot::Receiver<()>,
) {
    let w_rate = if args.w_rate == 0.0 {
        None
    } else {
        Some(args.w_rate)
    };
    let r_rate = if args.r_rate == 0.0 {
        None
    } else {
        Some(args.r_rate)
    };

    let index = Arc::new(AtomicU64::new(0));

    let (w_stop_tx, w_stop_rx) = oneshot::channel();
    let (r_stop_tx, r_stop_rx) = oneshot::channel();

    let handle_w = tokio::spawn(write(
        args.entry_size,
        w_rate,
        index.clone(),
        container.clone(),
        args.time,
        metrics.clone(),
        w_stop_rx,
    ));
    let handle_r = tokio::spawn(read(
        args.entry_size,
        r_rate,
        index.clone(),
        container.clone(),
        args.time,
        metrics.clone(),
        r_stop_rx,
        args.look_up_range,
    ));
    tokio::spawn(async move {
        if let Ok(()) = stop.await {
            let _ = w_stop_tx.send(());
            let _ = r_stop_tx.send(());
        }
    });

    join_all([handle_r, handle_w]).await;
}

#[allow(clippy::too_many_arguments)]
async fn write(
    entry_size: usize,
    rate: Option<f64>,
    index: Arc<AtomicU64>,
    container: Arc<TContainer>,
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
        let data = vec![b'x'; entry_size];
        if let Some(limiter) = &mut limiter  && let Some(wait) = limiter.consume(entry_size as f64) {
            tokio::time::sleep(wait).await;
        }

        let time = Instant::now();
        container.insert(idx, data).await.unwrap();
        metrics
            .insert_lats
            .write()
            .record(time.elapsed().as_micros() as u64)
            .expect("record out of range");
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
    container: Arc<TContainer>,
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
        let hit = container.get(&idx).await.unwrap().is_some();
        let lat = time.elapsed().as_micros() as u64;
        if hit {
            metrics
                .get_hit_lats
                .write()
                .record(lat)
                .expect("record out of range");
        } else {
            metrics
                .get_miss_lats
                .write()
                .record(lat)
                .expect("record out of range");
            metrics.get_miss_ios.fetch_add(1, Ordering::Relaxed);
        }
        metrics.get_ios.fetch_add(1, Ordering::Relaxed);

        tokio::task::consume_budget().await;
    }
}
