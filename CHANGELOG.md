---
title: Changelog
description: Changelog for foyer.
authors: mrcroxx
date: 2023-05-12T11:02:09+08:00
---

# Changelog

<!-- truncate -->

## 2025-08-16

### Releases

| crate | version |
| - | - |
| foyer | 0.18.1 |
| foyer-common | 0.18.1 |
| foyer-memory | 0.18.1 |
| foyer-storage | 0.18.1 |
| foyer-bench | 0.18.1 |

### Changes

- Fix: Compare checksum before large object disk cache entry serialization.

## 2025-08-13

### Releases

| crate | version |
| - | - |
| foyer | 0.19.0 |
| foyer-common | 0.19.0 |
| foyer-memory | 0.19.0 |
| foyer-storage | 0.19.0 |
| foyer-bench | 0.19.0 |

### Changes

- Refactor: Introduce new disk cache engine, io engine, io device abstraction.
- Feature: Introduce io_uirng based disk cache io engine.
- Feature: Support combine multiple devices for disk cache engine.
- Feature: Make disk cache write queue queryable.
- Feature: Support filter for in-memory cache.
- Fix: Remove `fastrace/enable` when feature `tracing` is enabled. 
- Refactor: Rename large object disk cache engine to block engine.
- Refactor: Decouple block engine block size with device region size, remove device region size abstraction.
- Refactor: Refactor disk cache builder to provide more user-friendly and developer-friendly APIs.
- Refactor: Temporarily **REMOVE** small object disk cache engine and mixed disk cache engine. Plan to add them back in the refactored form in the next version.

## 2025-07-13

### Releases

| crate | version |
| - | - |
| foyer | 0.18.0 |
| foyer-common | 0.18.0 |
| foyer-memory | 0.18.0 |
| foyer-storage | 0.18.0 |
| foyer-bench | 0.18.0 |

### Changes

- Introduce SIEVE algorithm for in-memory cache eviction policy.
- Auto close hybrid cache on the last reference drops.
- Expose internal foyer error types.
- Fix `delete()` implementation for large object disk cache to prevent from phantom entries.
- Remove unnecessary and confusing `with_flush()` API.
- Use "cargo xtask"-style for developing, yank `make` system.
- Bump MSRV to v1.82.0

## 2025-06-27

### Releases

| crate | version |
| - | - |
| foyer | 0.17.4 |
| foyer-common | 0.17.4 |
| foyer-memory | 0.17.4 |
| foyer-storage | 0.17.4 |
| foyer-bench | 0.17.4 |

### Changes

- Use `BuildHasherDefault<XxHash64>` as `DefaultHasher` to make hash results consistent across runs.

## 2025-05-22

### Releases

| crate | version |
| - | - |
| foyer | 0.17.3 |
| foyer-common | 0.17.3 |
| foyer-memory | 0.17.3 |
| foyer-storage | 0.17.3 |
| foyer-bench | 0.17.3 |

### Changes

- Use `DefaultHasher` (alias for `std::hash::BuilderHasherDefault<ahash::AHaser>`) as the default hasher to guarantee hash algorithm stable across runs.

## 2025-05-14

### Releases

| crate | version |
| - | - |
| foyer | 0.17.2 |
| foyer-common | 0.17.2 |
| foyer-memory | 0.17.2 |
| foyer-storage | 0.17.2 |
| foyer-bench | 0.17.2 |

### Changes

- Fix disk cache return wrong entry on key hash collision.

## 2025-05-09

### Releases

| crate | version |
| - | - |
| foyer | 0.17.1 |
| foyer-common | 0.17.1 |
| foyer-memory | 0.17.1 |
| foyer-storage | 0.17.1 |
| foyer-bench | 0.17.1 |

### Changes

- Refine load error handling to prevent from panics on disk data corruption.
- Derive `Partial, Eq` for `Throttle` and `IopsCounter`.

## 2025-04-28

### Releases

| crate | version |
| - | - |
| foyer | 0.17.0 |
| foyer-common | 0.17.0 |
| foyer-memory | 0.17.0 |
| foyer-storage | 0.17.0 |
| foyer-bench | 0.17.0 |

### Changes

- Refine entry-level properties.
  - Packing `ephemeral`, `hint`, `location` into `CacheProperties` and `HybridCacheProperties`, use `insert_with_properties()` and `fetch_with_properties()` for setting them.
  - Migration from `foyer < v0.17.0`: Repleace `.insert_with_hint(..)`, `.insert_ephemeral(..)`, etc with `insert_with_properties(..)` and `fetch_with_properties(..)`.
- Refine disk cache insertion with entries populated from the disk cache.
  - Use an age-based FIFO policy to manager large object disk cache.
  - Track the source of in-memory cache entries.
  - When an in-memory cache entry is being inserted into the disk cache, the disk cache will determine if to skip it according to its resource and age. Entries populated from the disk cache and are not going to evicted soon will be skipped.
- Support flush entries in the in-memory cache to the disk cache on closing the hybrid cache.
  - Migration from `foyer < v0.17.0`: Enable by `.with_flush_on_close(true)` with the hybrid cache builder.
- Refine io throttling.
  - Support advanced io throttling by read/write IOPS/throughput, and support count IOs by IO count or by IO count with IO size.
  - Migration from `foyer < v0.17.0`: Please set io throttling for device, instead of using `RateLimiterAmissionPicker`.
- Support request deduplication and one-flight optimization with `fetch(..)` interface for the disk cache.
  - Migration from `foyer < v0.17.0`: Use `.fetch_with_properties(..)` API and set the `location` of `HybridCacheProperties` to `Location::OnDisk`.
- Miscs:
  - Add `builder()` API for `Cache` and `HybridCache` to create builders.
  - Opt-out tracing dependency by default, enable it by enabling `tracing` feature.
  - Bump `mixtrics` to `v0.1`.

Latest `foyer-bench` usage:

```
bench tool for foyer - Hybrid cache for Rust

Usage: foyer-bench [OPTIONS] <--file <FILE>|--dir <DIR>|--no-disk>

Options:
      --no-disk
          Run with in-memory cache compatible mode
  -f, --file <FILE>
          File for disk cache data. Use `DirectFile` as device
  -d, --dir <DIR>
          Directory for disk cache data. Use `DirectFs` as device
      --mem <MEM>
          In-memory cache capacity [default: "1.0 GiB"]
      --disk <DISK>
          Disk cache capacity [default: "1.0 GiB"]
  -t, --time <TIME>
          (s) [default: 60]
      --report-interval <REPORT_INTERVAL>
          (s) [default: 2]
      --w-rate <W_RATE>
          Write rate limit per writer [default: "0 B"]
      --r-rate <R_RATE>
          Read rate limit per reader [default: "0 B"]
      --entry-size-min <ENTRY_SIZE_MIN>
          Min entry size [default: "64.0 KiB"]
      --entry-size-max <ENTRY_SIZE_MAX>
          Max entry size [default: "64.0 KiB"]
      --get-range <GET_RANGE>
          Reader lookup key range [default: 10000]
      --region-size <REGION_SIZE>
          Disk cache region size [default: "64.0 MiB"]
      --flushers <FLUSHERS>
          Flusher count [default: 4]
      --reclaimers <RECLAIMERS>
          Reclaimer count [default: 4]
      --writers <WRITERS>
          Writer count [default: 16]
      --readers <READERS>
          Reader count [default: 16]
      --recover-mode <RECOVER_MODE>
          [default: none] [possible values: none, quiet, strict]
      --recover-concurrency <RECOVER_CONCURRENCY>
          Recover concurrency [default: 16]
      --disk-write-iops <DISK_WRITE_IOPS>
          Disk write iops throttle [default: 0]
      --disk-read-iops <DISK_READ_IOPS>
          Disk read iops throttle [default: 0]
      --disk-write-throughput <DISK_WRITE_THROUGHPUT>
          Disk write throughput throttle [default: "0 B"]
      --disk-read-throughput <DISK_READ_THROUGHPUT>
          Disk read throughput throttle [default: "0 B"]
      --clean-region-threshold <CLEAN_REGION_THRESHOLD>
          `0` means use default [default: 0]
      --shards <SHARDS>
          Shards of both in-memory cache and disk cache indexer [default: 64]
      --metrics
          weigher to enable metrics exporter
      --user-runtime-worker-threads <USER_RUNTIME_WORKER_THREADS>
          Benchmark user runtime worker threads [default: 0]
      --runtime <RUNTIME>
          Dedicated runtime type [default: disabled] [possible values: disabled, unified, separated]
      --runtime-worker-threads <RUNTIME_WORKER_THREADS>
          Dedicated runtime worker threads [default: 0]
      --runtime-max-blocking-threads <RUNTIME_MAX_BLOCKING_THREADS>
          Max threads for blocking io [default: 0]
      --write-runtime-worker-threads <WRITE_RUNTIME_WORKER_THREADS>
          Dedicated runtime for writes worker threads [default: 0]
      --write-runtime-max-blocking-threads <WRITE_RUNTIME_MAX_BLOCKING_THREADS>
          Dedicated runtime for writes Max threads for blocking io [default: 0]
      --read-runtime-worker-threads <READ_RUNTIME_WORKER_THREADS>
          Dedicated runtime for reads worker threads [default: 0]
      --read-runtime-max-blocking-threads <READ_RUNTIME_MAX_BLOCKING_THREADS>
          Dedicated runtime for writes max threads for blocking io [default: 0]
      --compression <COMPRESSION>
          compression algorithm [default: none] [possible values: none, zstd, lz4]
      --engine <ENGINE>
          Disk cache engine [default: large]
      --distribution <DISTRIBUTION>
          Time-series operation distribution [default: none]
      --distribution-zipf-n <DISTRIBUTION_ZIPF_N>
          For `--distribution zipf` only [default: 100]
      --distribution-zipf-s <DISTRIBUTION_ZIPF_S>
          For `--distribution zipf` only [default: 0.5]
      --warm-up <WARM_UP>
          [default: 2]
      --flush
          
      --invalid-ratio <INVALID_RATIO>
          [default: 0.8]
      --eviction <EVICTION>
          [default: lru] [possible values: lru, lfu, fifo, s3fifo]
      --policy <POLICY>
          [default: eviction] [possible values: eviction, insertion]
      --buffer-pool-size <BUFFER_POOL_SIZE>
          [default: "16.0 MiB"]
      --blob-index-size <BLOB_INDEX_SIZE>
          [default: "4.0 KiB"]
      --set-size <SET_SIZE>
          [default: "16.0 KiB"]
      --set-cache-capacity <SET_CACHE_CAPACITY>
          [default: 64]
      --trace-insert <TRACE_INSERT>
          Record insert trace threshold. Only effective with "tracing" feature [default: 1s]
      --trace-get <TRACE_GET>
          Record get trace threshold. Only effective with "tracing" feature [default: 1s]
      --trace-obtain <TRACE_OBTAIN>
          Record obtain trace threshold. Only effective with "tracing" feature [default: 1s]
      --trace-remove <TRACE_REMOVE>
          Record remove trace threshold. Only effective with "tracing" feature [default: 1s]
      --trace-fetch <TRACE_FETCH>
          Record fetch trace threshold. Only effective with "tracing" feature [default: 1s]
      --flush-on-close
          
      --lodc-fifo-probation-ratio <LODC_FIFO_PROBATION_RATIO>
          [default: 0.1]
  -h, --help
          Print help (see more with '--help')
  -V, --version
          Print version
```

## 2025-04-22

### Releases

| crate | version |
| - | - |
| foyer | 0.15.4 |
| foyer-common | 0.15.4 |
| foyer-memory | 0.15.4 |
| foyer-storage | 0.15.4 |
| foyer-bench | 0.15.4 |

### Changes

- Add metrics for large object disk cache indexer conflicts.

## 2025-04-11

### Releases

| crate | version |
| - | - |
| foyer | 0.16.1 |
| foyer-common | 0.16.1 |
| foyer-memory | 0.16.1 |
| foyer-storage | 0.16.1 |
| foyer-bench | 0.16.1 |

### Changes

- Fix panic when building large object disk cache engine if `buffer_pool_size / flushers` is not 4K-aligned.
- Make CI run on release branches.

## 2025-04-07

### Releases

| crate | version |
| - | - |
| foyer | 0.16.0 |
| foyer-common | 0.16.0 |
| foyer-memory | 0.16.0 |
| foyer-storage | 0.16.0 |
| foyer-bench | 0.16.0 |

### Changes

- Refine the cache key/value serialization and deserializaion.
  - Support customized serialization and deserialization.
  - Support serialization and deserialization with `serde` and `bincode`. (optional)
- Make `serde` and `bincode` optional, and disabled by default. You can enable it with `serde` feature.
- Reduce verbose warning logs.

## 2025-03-28

### Releases

| crate | version |
| - | - |
| foyer | 0.15.3 |
| foyer-common | 0.15.3 |
| foyer-memory | 0.15.3 |
| foyer-storage | 0.15.3 |
| foyer-bench | 0.15.3 |

### Changes

- Fix lodc flush buffer split condition calculation error.

## 2025-03-26

### Releases

| crate | version |
| - | - |
| foyer | 0.15.2 |
| foyer-common | 0.15.2 |
| foyer-memory | 0.15.2 |
| foyer-storage | 0.15.2 |
| foyer-bench | 0.15.2 |

### Changes

- Mitigate low-priority errors. Refine error handling.

## 2025-03-25

### Releases

| crate | version |
| - | - |
| foyer | 0.15.1 |
| foyer-common | 0.15.1 |
| foyer-memory | 0.15.1 |
| foyer-storage | 0.15.1 |
| foyer-bench | 0.15.1 |

### Changes

- Support either write disk cache on in-memory cache evction or on insertion with `HybridCacheBuilder::with_policy()`.

## 2025-03-24

### Releases

| crate | version |
| - | - |
| foyer | 0.15.0 |
| foyer-common | 0.15.0 |
| foyer-memory | 0.15.0 |
| foyer-storage | 0.15.0 |
| foyer-bench | 0.15.0 |

### Changes

- Support blob file format to speed up disk cache recovery and reduce reclaim overhead.
- Make histogram metrics more precise.
- Refine redundant codes.
- Refine dependencies.

## 2025-02-08

### Releases

| crate | version |
| - | - |
| foyer | 0.14.1 |
| foyer-common | 0.14.1 |
| foyer-memory | 0.14.1 |
| foyer-storage | 0.14.1 |
| foyer-bench | 0.14.1 |

### Changes

- Use `tokio-rs/tokio` by default as the runtime, use `madsim-tokio` when `madsim` configuration is enabled.
- Optimize dependencies.

## 2025-01-14

### Releases

| crate | version |
| - | - |
| foyer | 0.14.0 |
| foyer-common | 0.14.0 |
| foyer-memory | 0.14.0 |
| foyer-storage | 0.14.0 |
| foyer-bench | 0.14.0 |

### Changes

- Insert disk cache on in-memory cache eviction, instead of double write on insertion.
- Extrace multiple metrics backend support to crate [`mixtrics`](https://crates.io/crates/mixtrics).

## 2024-12-04

### Releases

| crate | version |
| - | - |
| foyer | 0.13.1 |
| foyer-common | 0.13.1 |
| foyer-memory | 0.13.1 |
| foyer-storage | 0.13.1 |
| foyer-bench | 0.13.1 |

### Changes

- Refactor cache builders. `name` field requires `Cow<'static, str>` instead of `&'static str` for convenience.

## 2024-12-02

### Releases

| crate | version |
| - | - |
| foyer | 0.13.0 |
| foyer-common | 0.13.0 |
| foyer-memory | 0.13.0 |
| foyer-storage | 0.13.0 |
| foyer-bench | 0.13.0 |

### Changes

- Refine in-memory cache framework:
  - Allow "get"/"release"/"entry drop" to acquire read lock or lock-free if the algorithm allows.
  - Make most `Eviction` APIs safe, only acquire unsafe Rust while accessing algorithm managed per-entry state with `UnsafeCell`.
  - Replace the "reinsertion" design with `release` with real "release last ref" design.
  - Rename some APIs.
- Refine metrics framework:
  - Replace `metrics` with customized metrics framework to support various metrics backend.
  - Implement built-in metrics exporter that adapts to crate `prometheus`/`prometheus-client`/`opentelemetry`.

## 2024-10-11

### Releases

| crate | version |
| - | - |
| foyer | 0.12.2 |
| foyer-common | 0.12.2 |
| foyer-intrusive | 0.12.2 |
| foyer-memory | 0.12.2 |
| foyer-storage | 0.12.2 |
| foyer-bench | 0.12.2 |

### Changes

- Revert "Scale shards to 1 when there is not enough capacity". It would be useful sometimes. Just raise the warning.

## 2024-10-10

### Releases

| crate | version |
| - | - |
| foyer | 0.12.1 |
| foyer-common | 0.12.1 |
| foyer-intrusive | 0.12.1 |
| foyer-memory | 0.12.1 |
| foyer-storage | 0.12.1 |
| foyer-bench | 0.12.1 |

### Changes

- Downgrade hashbrown to 0.14 to fix build on nightly for projects using hashbrown < 0.15.
- Fix build with madsim.
- Refine small object disk cache.
- Scale shards to 1 when there is not enough capacity.

## 2024-10-09

### Releases

| crate | version |
| - | - |
| foyer | 0.12.0 |
| foyer-common | 0.12.0 |
| foyer-intrusive | 0.12.0 |
| foyer-memory | 0.12.0 |
| foyer-storage | 0.12.0 |
| foyer-bench | 0.12.0 |

### Changes

- Align the versions of all components to the same. 📣
- Introduce small object disk cache. 🎉
- Introduce mixed/large/small storage engine.
- Refactor builders for the hybrid cache.
- Introduce submit queue size threshold to prevent from channel piling up.
- Support `jeprof` for foyer-bench.
- Rename feature "mtrace" to "tracing".

## 2024-09-25

### Releases

| crate | version |
| - | - |
| foyer | 0.11.5 |
| foyer-common | 0.9.5 |
| foyer-intrusive | 0.9.5 |
| foyer-memory | 0.7.5 |
| foyer-storage | 0.10.5 |
| foyer-bench | 0.3.5 |

### Changes

- Fix panic on dropping the hybrid cache. #736

## 2024-09-24

### Releases

| crate | version |
| - | - |
| foyer | 0.11.4 |
| foyer-common | 0.9.4 |
| foyer-intrusive | 0.9.4 |
| foyer-memory | 0.7.4 |
| foyer-storage | 0.10.4 |
| foyer-bench | 0.3.4 |

### Changes

- Revert pre-serialization design. The insert latency and memory usage would be better for most cases.
- Rename `with_buffer_threshold` to `with_buffer_pool_size`. The old method is kept but marked as deprecated.
- Raise a warn when using `DirectFileDevice` on within a file system.

## 2024-09-20

### Releases

| crate | version |
| - | - |
| foyer | 0.11.3 |
| foyer-common | 0.9.3 |
| foyer-intrusive | 0.9.3 |
| foyer-memory | 0.7.3 |
| foyer-storage | 0.10.3 |
| foyer-bench | 0.3.3 |

### Changes

- Fix panicked by io buffer pool alignment issue.

## 2024-09-12

### Releases

| crate | version |
| - | - |
| foyer | 0.11.2 |
| foyer-common | 0.9.2 |
| foyer-intrusive | 0.9.2 |
| foyer-memory | 0.7.2 |
| foyer-storage | 0.10.2 |
| foyer-bench | 0.3.2 |

### Changes

- Support windows (for `foyer` only).
- Bump rust toolchain to `1.82.0`.
- Expose in-memory cache builder and cache entry.
- Reduce page fault and vec growth overhead.
- Use bytes size for `foyer-bench`.
- Fix install deps script.

## 2024-08-31

### Releases

| crate | version |
| - | - |
| foyer | 0.11.1 |
| foyer-common | 0.9.1 |
| foyer-intrusive | 0.9.1 |
| foyer-memory | 0.7.1 |
| foyer-storage | 0.10.1 |
| foyer-bench | 0.3.1 |

### Changes

- Add metrics for serde.
- Refine `fetch` runtime usage.
- Fix unhandled low-layer errors. #674 #677 #679
- Implement `Default` for `TokioRuntimeConfig`.
- Fix typos and format code with unstable features.

## 2024-08-21

### Releases

| crate | version |
| - | - |
| foyer | 0.11.0 |
| foyer-common | 0.9.0 |
| foyer-intrusive | 0.9.0 |
| foyer-memory | 0.7.0 |
| foyer-storage | 0.10.0 |
| foyer-bench | 0.3.0 |

### Changes

- Support disk cache on raw block device.
- Support fine-grained storage engine runtime configuration.
- Enhance performance via reducing page fault.
- Refine storage engine framework for future features.
- Expose `Weighter` trait.
- Support `serde` for more configurations.
- Update `foyer-bench` with more fine-grained configurations.
- Fix panics with `None` recover mode.

## 2024-08-15

### Releases

| crate | version |
| - | - |
| foyer | 0.10.4 |
| foyer-storage | 0.9.3 |
| foyer-bench | 0.2.3 |

### Changes

- Support serde for recover mode configuration.

## 2024-08-14

### Releases

| crate | version |
| - | - |
| foyer | 0.10.2 |
| foyer-storage | 0.9.2 |
| foyer-bench | 0.2.2 |

### Changes

- Fix panic with "none" recovery mode.

## 2024-07-08

### Releases

| crate | version |
| - | - |
| foyer | 0.10.1 |
| foyer-common | 0.8.1 |
| foyer-intrusive | 0.8.1 |
| foyer-memory | 0.6.1 |
| foyer-storage | 0.9.1 |
| foyer-bench | 0.2.1 |

### Changes

- Refine write model, make flush buffer threshold configurable to mitigate memory usage spike and OOM.

## 2024-07-02

### Releases

| crate | version |
| - | - |
| foyer | 0.10.0 |
| foyer-common | 0.8.0 |
| foyer-intrusive | 0.8.0 |
| foyer-memory | 0.6.0 |
| foyer-storage | 0.9.0 |
| foyer-bench | 0.2.0 |

### Changes

- Introduce tail-based tracing framework with [minitrace](https://github.com/tikv/minitrace-rust). [Tail-based Tracing Example](https://github.com/foyer-rs/foyer/tree/main/examples/tail_based_tracing.rs).
- Fix `fetch()` disk cache refill on in-memory cache miss.
- Publish *foyer* logo! 

<img src="https://raw.githubusercontent.com/foyer-rs/foyer/main/etc/logo/slogan.min.svg" style="width: 200px"/>

## 2024-06-14

### Releases

| crate | version |
| - | - |
| foyer | 0.9.4 |
| foyer-storage | 0.8.5 |
| foyer-bench | 0.1.4 |

### Changes

- Fix phantom entries after foyer storage recovery. [#560](https://github.com/foyer-rs/foyer/pull/560)
- Fix hybrid cache hit metrics with `fetch()` interface. [#563](https://github.com/foyer-rs/foyer/pull/563)

## 2024-06-05

### Releases

| crate | version |
| - | - |
| foyer | 0.9.3 |
| foyer-common | 0.7.3 |
| foyer-intrusive | 0.7.2 |
| foyer-memory | 0.5.2 |
| foyer-storage | 0.8.4 |
| foyer-bench | 0.1.3 |

### Changes

- Hybrid cache `fetch()` use the dedicated runtime by default if enabled.
- Separate `fetch()` and `fetch_with_runtime()` interface for in-memory cache.

## 2024-06-04

### Releases

| crate | version |
| - | - |
| foyer-storage | 0.8.3 |

### Changes

- Fix "invalid argument (code: 22)" on target aarch64.

## 2024-06-03

### Releases

| crate | version |
| - | - |
| foyer | 0.9.2 |
| foyer-common | 0.7.2 |
| foyer-intrusive | 0.7.1 |
| foyer-memory | 0.5.1 |
| foyer-storage | 0.8.2 |
| foyer-bench | 0.1.2 |

### Changes

- Support customized cache event listener.

## 2024-05-31

### Releases

| crate | version |
| - | - |
| foyer | 0.9.1 |
| foyer-common | 0.7.1 |
| foyer-intrusive | 0.7.0 |
| foyer-memory | 0.5.0 |
| foyer-storage | 0.8.1 |
| foyer-bench | 0.1.1 |

### Changes

- Fix "attempt to subtract with overflow" panic after cloning cache entry. [#543](https://github.com/foyer-rs/foyer/issues/543).
- Fix "assertion failed: base.is_in_indexer()" panic after replacing deposit entry. [#547](https://github.com/foyer-rs/foyer/issues/547).
- Fix setting name for in-memory cache.
- Add region related metrics.
- Introduce `strict_assertions` and `sanity` feature for debugging.
- `foyer-bench` support setting eviction algorithm.
- Upgrade `metrics` to `0.23`.
- Remove `pop()` related interface from the in-memory cache.
- Refine intrusive data structure implementation.

## 2024-05-27

### Releases

| crate | version |
| - | - |
| foyer | 0.9.0 |
| foyer-common | 0.7.0 |
| foyer-intrusive | 0.6.0 |
| foyer-memory | 0.4.0 |
| foyer-storage | 0.8.0 |
| foyer-bench | 0.1.0 |

### Changes

- Refine the storage engine to reduce the overhead and boost the performance.
  - Hybrid cache memory overhead reduced by ~50%.
  - Hybrid cache operation latency reduced by ~80%.
- Replace `prometheus` with `metrics` to support more flexible metrics collection.
- Introduce `foyer-bench` to replace the original `foyer-storage-bench` for better benchmarking.
- Fulfill rust docs for public APIs.
- Introduce tombstone log for updatable persistent cache.
- Reduce unnecessary dependencies.
- More details: [foyer - Development Roadmap](https://github.com/orgs/foyer-rs/projects/2).

## 2024-04-28

### Releases

| crate | version |
| - | - |
| foyer | 0.8.9 |
| foyer-common | 0.6.4 |
| foyer-memory | 0.3.6 |
| foyer-storage | 0.7.6 |
| foyer-storage-bench | 0.7.5 |

### Changes

- feat: Add config to control the recover mode.
- feat: Add config to enable/disable direct i/o. (Enabled by default for large entries optimization.)

## 2024-04-28

### Releases

| crate | version |
| - | - |
| foyer | 0.8.8 |
| foyer-memory | 0.3.5 |
| foyer-storage | 0.7.5 |
| foyer-storage-bench | 0.7.4 |

### Changes

- feat: Impl `Debug` for `HybridCache`.
- feat: Impl `serde`, `Default` for eviction configs.
- refactor: Add internal trait `EvictionConfig` to bound eviction algorithm configs.

## 2024-04-27

### Releases

| crate | version |
| - | - |
| foyer | 0.8.7 |

### Changes

- Make `HybridCache` cloneable.

## 2024-04-27

### Releases

| crate | version |
| - | - |
| foyer-memory | 0.3.4 |

### Changes

- Fix S3FIFO ghost queue.

## 2024-04-26

### Releases

| crate | version |
| - | - |
| foyer-storage | 0.7.4 |

### Changes

- Fix `FsDeviceBuilder` on a non-exist directory without capacity given.

## 2024-04-26

### Releases

| crate | version |
| - | - |
| foyer | 0.8.6 |
| foyer-common | 0.6.3 |
| foyer-intrusive | 0.5.3 |
| foyer-memory | 0.3.3 |
| foyer-storage | 0.7.3 |
| foyer-storage-bench | 0.7.3 |

### Changes

- Remove unused dependencies.
- Remove hakari workspace hack.

## 2024-04-26

### Releases

| crate | version |
| - | - |
| foyer | 0.8.5 |

### Changes

- Expose `EntryState`, `HybridEntry`.
- Expose `StorageWriter`, `Metrics`, `get_metrics_registry`, `set_metrics_registry`.
- Expose `RangeBoundsExt`, `BufExt`, `BufMutExt`.
- Re-export `ahash::RandomState`.
- Loose `entry()` args trait bounds.

## 2024-04-25

### Releases

| crate | version |
| - | - |
| foyer | 0.8.4 |

### Changes

- Expose `HybridCacheEntry`.

## 2024-04-25

### Releases

| crate | version |
| - | - |
| foyer | 0.8.3 |

### Changes

- Expose `Key`, `Value`, `StorageKey`, `StorageValue` traits.

## 2024-04-24

### Releases

| crate | version |
| - | - |
| foyer | 0.8.2 |
| foyer-common | 0.6.2 |
| foyer-intrusive | 0.5.2 |
| foyer-memory | 0.3.2 |
| foyer-storage | 0.7.2 |
| foyer-storage-bench | 0.7.2 |
| foyer-workspace-hack | 0.5.2 |

### Changes

- Add `nightly` feature to make it compatible with night toolchain.

## 2024-04-24

### Releases

| crate | version |
| - | - |
| foyer | 0.8.1 |
| foyer-common | 0.6.1 |
| foyer-intrusive | 0.5.1 |
| foyer-memory | 0.3.1 |
| foyer-storage | 0.7.1 |
| foyer-storage-bench | 0.7.1 |
| foyer-workspace-hack | 0.5.1 |

### Changes

- Add `with_flush` to enable flush for each io.
- Loose MSRV to 1.76 .
- Flush the device on store close.

## 2024-04-23

### Releases

| crate | version |
| - | - |
| foyer | 0.8.0 |
| foyer-common | 0.6.0 |
| foyer-intrusive | 0.5.0 |
| foyer-memory | 0.3.0 |
| foyer-storage | 0.7.0 |
| foyer-storage-bench | 0.7.0 |
| foyer-workspace-hack | 0.5.0 |

### Changes

- Combine in-memory cache and disk cache into `HybridCache`.
- Refine APIs, make them more user-friendly.
- Refine `Key`, `Value`, `StorageKey`, `StorageValue` traits.
- Support `serde` for storage key and value serialization and deserialization.
- Loose trait bounds for key and value.
- Add configurable ghost queue for S3FIFO.
- Fix S3FIFO eviction bugs.
- Add more examples.

## 2024-04-11

### Releases

| crate | version |
| - | - |
| foyer | 0.7.0 |
| foyer-common | 0.5.0 |
| foyer-intrusive | 0.4.0 |
| foyer-memory | 0.2.0 |
| foyer-storage | 0.6.0 |
| foyer-storage-bench | 0.6.0 |
| foyer-workspace-hack | 0.4.0 |

### Changes

- Make `foyer` compatible with rust stable toolchain (MSRV = 1.77.2). 🎉

## 2024-04-09

### Releases

| crate | version |
| - | - |
| foyer-storage | 0.5.1 |
| foyer-memory | 0.1.4 |

### Changes

- fix: Fix panics on `state()` for s3fifo entry.
- fix: Enable `offset_of` feature for `foyer-storage`.

## 2024-04-08

### Releases

| crate | version |
| - | - |
| foyer-intrusive | 0.3.1 |
| foyer-memory | 0.1.3 |

### Changes

- feat: Introduce s3fifo to `foyer-memory`.
- fix: Fix doctest for `foyer-intrusive`.

## 2024-03-21

### Releases

| crate | version |
| - | - |
| foyer-memory | 0.1.2 |

### Changes

- fix: `foyer-memory` export `DefaultCacheEventListener`.

## 2024-03-14

### Releases

| crate | version |
| - | - |
| foyer-memory | 0.1.1 |

### Changes

- Make eviction config cloneable.

## 2024-03-13

### Releases

| crate | version |
| - | - |
| foyer-storage-bench | 0.5.1 |

### Changes

- Fix `foyer-storage-bench` build with `trace` feature.

## 2024-03-12

### Releases

| crate | version |
| - | - |
| foyer | 0.6.0 |
| foyer-common | 0.4.0 |
| foyer-intrusive | 0.3.0 |
| foyer-memory | 0.1.0 |
| foyer-storage | 0.5.0 |
| foyer-storage-bench | 0.5.0 |
| foyer-workspace-hack | 0.3.0 |

### Changes

- Release foyer in-memory cache as crate `foyer-memory`.
- Bump other components with changes.

## 2023-12-28

### Releases

| crate | version |
| - | - |
| foyer | 0.5.0 |
| foyer-common | 0.3.0 |
| foyer-intrusive | 0.2.0 |
| foyer-storage | 0.4.0 |
| foyer-storage-bench | 0.4.0 |
| foyer-workspace-hack | 0.2.0 |

### Changes

- Bump rust-toolchain to "nightly-2023-12-26".
- Introduce time-series distribution args to bench tool. [#253](https://github.com/foyer-rs/foyer/pull/253)
- Fix duplicated insert drop metrics.

## 2023-12-22

### Releases

| crate | version |
| - | - |
| foyer | 0.4.0 |
| foyer-storage | 0.3.0 |
| foyer-storage-bench | 0.3.0 |
| foyer-workspace-hack | 0.1.1 |

### Changes

- Remove config `flusher_buffer_capacity`.
- Fix benchmark tool cache miss ratio.

## 2023-12-20

### Releases

| crate | version |
| - | - |
| foyer-storage | 0.2.2 |

### Changes

- Fix metrics for writer dropping.
- Add interface `insert_async_with_callback` and `insert_if_not_exists_async_with_callback` for callers to get the insert result.

## 2023-12-18

### Releases

| crate | version |
| - | - |
| foyer-storage | 0.2.1 |

### Changes

- Introduce the entry size histogram, update metrics.

## 2023-12-18

### Releases

| crate | version |
| - | - |
| foyer | 0.3.0 |
| foyer-common | 0.2.0 |
| foyer-storage | 0.2.0 |
| foyer-storage-bench | 0.2.0 |

### Changes

- Introduce the associated type `Cursor` for trait `Key` and `Value` to reduce unnecessary buffer copy if possible.
- Remove the ring buffer and continuum tracker for they are no longer needed.
- Update the configuration of the storage engine and the benchmark tool.

## 2023-11-29

### Releases

| crate | version |
| - | - |
| foyer | 0.2.0 |
| foyer-common | 0.1.0 |
| foyer-intrusive | 0.1.0 |
| foyer-storage | 0.1.0 |
| foyer-storage-bench | 0.1.0 |
| foyer-workspace-hack | 0.1.0 |

### Changes

The first version that can be used as file cache.

The write model and the design of storage engine has been switched from CacheLib navy version to the ring buffer version (which was highly inspired by MySQL 8.0 link_buf).

Introduces `Store`, `RuntimeStore`, `LazyStore` to simplify usage. In most cases, `RuntimeStore` is preferred to use a dedicated tokio runtime to serve **foyer** to avoid the influence to the user's runtime. If lazy-load is needed, use `RuntimeLazyStore` instead.

The implementation of **foyer** is separated into multiple crates. But importing `foyer` is enough for it re-exports the crates that **foyer**'s user needs.

Brief description about the subcrates:

- foyer-common: Provide basic data structures and algorithms.
- foyer-intrusive: Provide intrusive containers for implementing eviction lists and collections. Intrusive data structures provide the ability to implement low-cost multi-index data structures, which will be used for the memory cache in future.
- foyer-storage: Provide the file cache storage engine and wrappers to simplify the code.
- foyer-storage-bench: Runnable benchmark tool for the file cache storage engine.
- foyer-workspace-hack: Generated by [hakari](https://crates.io/crates/hakari) to prevent building each crate from triggering building from scratch.

## 2023-05-12

### Releases

| crate | version |
| - | - |
| foyer | 0.1.0 |

### Changes

Initial version with just basic interfaces.
