<p align="center">
    <img src="https://raw.githubusercontent.com/foyer-rs/foyer/main/etc/logo/slogan.min.svg" />
</p>

<p align="center">
    <a href="https://foyer-rs.github.io/foyer">
        <img src="https://img.shields.io/website?url=https%3A%2F%2Ffoyer-rs.github.io/foyer&up_message=foyer-rs.github.io/foyer&down_message=website&style=for-the-badge&logo=htmx" alt="docs.rs" />
    </a>
    <a href="https://crates.io/crates/foyer">
        <img src="https://img.shields.io/crates/v/foyer?style=for-the-badge&logo=crates.io&labelColor=555555" alt="crates.io" />
    </a>
    <a href="https://docs.rs/foyer">
        <img src="https://img.shields.io/docsrs/foyer?style=for-the-badge&logo=rust&label=docs.rs&labelColor=555555" alt="docs.rs" />
    </a>
</p>

<p align="center">
    <b>Tutorial & Document:</b>
    <a href="https://foyer-rs.github.io/foyer"><b>https://foyer-rs.github.io/foyer</b></a>
</p>

# foyer

![GitHub License](https://img.shields.io/github/license/foyer-rs/foyer)
![Crates.io MSRV](https://img.shields.io/crates/msrv/foyer)
[![CI](https://github.com/foyer-rs/foyer/actions/workflows/ci.yml/badge.svg)](https://github.com/foyer-rs/foyer/actions/workflows/ci.yml)
[![License Checker](https://github.com/foyer-rs/foyer/actions/workflows/license-check.yml/badge.svg)](https://github.com/foyer-rs/foyer/actions/workflows/license-check.yml)
[![codecov](https://codecov.io/github/foyer-rs/foyer/branch/main/graph/badge.svg?token=YO33YQCB70)](https://codecov.io/github/foyer-rs/foyer)

*foyer* aims to be an efficient and user-friendly hybrid cache lib in Rust.

foyer draws inspiration from [Facebook/CacheLib](https://github.com/facebook/cachelib), a highly-regarded hybrid cache library written in C++, and [ben-manes/caffeine](https://github.com/ben-manes/caffeine), a popular Java caching library, among other projects.

However, *foyer* is more than just a *rewrite in Rust* effort; it introduces a variety of new and optimized features.

For more details, please visit foyer's website: <https://foyer-rs.github.io/foyer> 🥰

[Website](https://foyer-rs.github.io/foyer) |
[Tutorial](https://foyer-rs.github.io/foyer/docs/overview) |
[API Docs](https://docs.rs/foyer) |
[Crate](https://crates.io/crates/foyer)

## Features

- **Hybrid Cache**: Seamlessly integrates both in-memory and disk cache for optimal performance and flexibility.
- **Plug-and-Play Algorithms**: Empowers users with easily replaceable caching algorithms, ensuring adaptability to diverse use cases.
- **Fearless Concurrency**: Built to handle high concurrency with robust thread-safe mechanisms, guaranteeing reliable performance under heavy loads.
- **Zero-Copy In-Memory Cache Abstraction**: Leveraging Rust's robust type system, the in-memory cache in foyer achieves a better performance with zero-copy abstraction.
- **User-Friendly Interface**: Offers a simple and intuitive API, making cache integration effortless and accessible for developers of all levels.
- **Out-of-the-Box Observability**: Integrate popular observation systems such as Prometheus, Grafana, Opentelemetry, and Jaeger in just *ONE* line.

<!-- rustdoc-ignore-start -->

## Projects Using *foyer*

Feel free to open a PR and add your projects here:

- [RisingWave](https://github.com/risingwavelabs/risingwave): SQL stream processing, analytics, and management.
- [Chroma](https://github.com/chroma-core/chroma): Embedding database for LLM apps.
- [SlateDB](https://github.com/slatedb/slatedb): A cloud native embedded storage engine built on object storage.
- [ZeroFS](https://github.com/Barre/zerofs): File systems and block devices on S3 storage.
- [Percas](https://github.com/scopedb/percas): A distributed persistent cache service optimized for high performance NVMe SSD.
- [dna](https://github.com/apibara/dna): The fastest platform to build production-grade indexers that connect onchain data to web2 services.
- [si](https://github.com/systeminit/si): The System Initiative software.
- [AntTP](https://github.com/traktion/AntTP): Serves Autonomi Network data over HTTP protocol.
- [Cachey](https://github.com/s2-streamstore/cachey): Read-through cache for object storage.

<!-- rustdoc-ignore-end -->

## Quick Start

**This section only shows briefs. Please visit <https://foyer-rs.github.io/foyer> for more details.**

To use *foyer* in your project, add this line to the `dependencies` section of `Cargo.toml`.

```toml
foyer = "0.21"
```

If your project is using the nightly rust toolchain, the `nightly` feature needs to be enabled.

```toml
foyer = { version = "0.21", features = ["nightly"] }
```

### Out-of-the-box In-memory Cache

The in-memory cache setup is extremely easy and can be setup in at least 1 line.

```rust
use foyer::{Cache, CacheBuilder};

let cache: Cache<String, String> = CacheBuilder::new(16).build();

let entry = cache.insert("hello".to_string(), "world".to_string());
let e = cache.get("hello").unwrap();

assert_eq!(entry.value(), e.value());
```

### Easy-to-use Hybrid Cache

The setup of a hybrid cache is extremely easy.

```rust
use foyer::{BlockEngineBuilder, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;

    let device = FsDeviceBuilder::new(dir.path())
        .with_capacity(256 * 1024 * 1024)
        .build()?;

    let hybrid: HybridCache<u64, String> = HybridCacheBuilder::new()
        .memory(64 * 1024 * 1024)
        .storage()
        // use block-based disk cache engine with default configuration
        .with_engine_config(BlockEngineBuilder::new(device))
        .build()
        .await?;

    hybrid.insert(42, "The answer to life, the universe, and everything.".to_string());
    assert_eq!(
        hybrid.get(&42).await?.unwrap().value(),
        "The answer to life, the universe, and everything."
    );

    Ok(())
}
```

### Fully Configured Hybrid Cache

Here is an example of a hybrid cache setup with almost all configurations to show th possibilities of tuning.

```rust
use std::{hash::BuildHasherDefault, num::NonZeroUsize};

use chrono::Datelike;
use foyer::{
    BlockEngineBuilder, DeviceBuilder, FifoPicker, FsDeviceBuilder, HybridCache, HybridCacheBuilder, HybridCachePolicy,
    IoEngineBuilder, IopsCounter, LruConfig, PsyncIoEngineBuilder, RecoverMode, RejectAll, RuntimeOptions,
    StorageFilter, Throttle, TokioRuntimeOptions,
};
use tempfile::tempdir;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dir = tempdir()?;

    let device = FsDeviceBuilder::new(dir.path())
        .with_capacity(64 * 1024 * 1024)
        .with_throttle(
            Throttle::new()
                .with_read_iops(4000)
                .with_write_iops(2000)
                .with_write_throughput(100 * 1024 * 1024)
                .with_read_throughput(800 * 1024 * 1024)
                .with_iops_counter(IopsCounter::PerIoSize(NonZeroUsize::new(128 * 1024).unwrap())),
        )
        .build()?;

    let io_engine = PsyncIoEngineBuilder::new().build().await?;

    let hybrid: HybridCache<u64, String> = HybridCacheBuilder::new()
        .with_name("my-hybrid-cache")
        .with_policy(HybridCachePolicy::WriteOnEviction)
        .memory(1024)
        .with_shards(4)
        .with_eviction_config(LruConfig {
            high_priority_pool_ratio: 0.1,
        })
        .with_hash_builder(BuildHasherDefault::default())
        .with_weighter(|_key, value: &String| value.len())
        .with_filter(|_, _| true)
        .storage()
        .with_io_engine(io_engine)
        .with_engine_config(
            BlockEngineBuilder::new(device)
                .with_block_size(16 * 1024 * 1024)
                .with_indexer_shards(64)
                .with_recover_concurrency(8)
                .with_flushers(2)
                .with_reclaimers(2)
                .with_buffer_pool_size(256 * 1024 * 1024)
                .with_clean_block_threshold(4)
                .with_eviction_pickers(vec![Box::<FifoPicker>::default()])
                .with_admission_filter(StorageFilter::new())
                .with_reinsertion_filter(StorageFilter::new().with_condition(RejectAll))
                .with_tombstone_log(false),
        )
        .with_recover_mode(RecoverMode::Quiet)
        .with_compression(foyer::Compression::Lz4)
        .with_runtime_options(RuntimeOptions::Separated {
            read_runtime_options: TokioRuntimeOptions {
                worker_threads: 4,
                max_blocking_threads: 8,
            },
            write_runtime_options: TokioRuntimeOptions {
                worker_threads: 4,
                max_blocking_threads: 8,
            },
        })
        .build()
        .await?;

    hybrid.insert(42, "The answer to life, the universe, and everything.".to_string());
    assert_eq!(
        hybrid.get(&42).await?.unwrap().value(),
        "The answer to life, the universe, and everything."
    );

    let e = hybrid
        .get_or_fetch(&20230512, || async {
            // Mock fetching data from remote source
            let now = chrono::Utc::now();
            if format!("{}{}{}", now.year(), now.month(), now.day()) == "20230512" {
                return Err(anyhow::anyhow!("Hi, time traveler!"));
            }
            Ok("Hello, foyer.".to_string())
        })
        .await?;
    assert_eq!(e.key(), &20230512);
    assert_eq!(e.value(), "Hello, foyer.");

    hybrid.close().await.unwrap();

    Ok(())
}
```

### `serde` Support

***foyer*** needs to serialize/deserialize entries between memory and disk with hybrid cache. Cached keys and values need to implement the `Code` trait when using hybrid cache.

The `Code` trait has already been implemented for general types, such as:

- Numeric types: `u8`, `u16`, `u32`, `u64`, `u128`, `usize`, `i8`, `i16`, `i32`, `i64`, `i128`, `isize`, `f32`, `f64`.
- Buffer: `Vec<u8>`.
- String: `String`.
- Other general types: `bool`.

For more complex types, you need to implement the Code trait yourself.

To make things easier, ***foyer*** provides support for the `serde` ecosystem. Types implement `serde::Serialize` and `serde::DeserializeOwned`, ***foyer*** will automatically implement the `Code` trait. This feature requires enabling the `serde` feature for foyer.

```toml
foyer = { version = "*", features = ["serde"] }
```

### Other Examples

- [Serialize/Deserialize w/wo serde](https://github.com/foyer-rs/foyer/tree/main/examples/serde.rs)
- [Export Metrics with `prometheus` and `hyper`](https://github.com/foyer-rs/foyer/tree/main/examples/export_metrics_prometheus_hyper.rs)
- [Tail-based Tracing](https://github.com/foyer-rs/foyer/tree/main/examples/tail_based_tracing.rs)

More code examples and details can be found [here](https://github.com/foyer-rs/foyer/tree/main/examples).

Case study of real-world usage in the production system can be found in [Document - Case Study - RisingWave](https://foyer-rs.github.io/foyer/docs/case-study/risingwave)

## Architecture

The architecture of ***foyer*** is still not mature and is undergoing rapid iteration. Currently, the architectural design can refer to [Document - Architecture](https://foyer-rs.github.io/foyer/docs/design/architecture)

## Supported Rust Versions

*foyer* is built against the recent stable release. The minimum supported version is 1.85.0. The current *foyer* version is not guaranteed to build on Rust versions earlier than the minimum supported version.

## Supported Platforms

*foyer* is designed to serve on Linux OS, but can still be built on other OS for development.

However, other components may not support non-Linux OS.

| Component   | Linux | MacOS | Windows |
|-------------|-------|-------|---------|
| foyer       | ✓     | ✓     | ✓       |
| foyer-bench | ✓     | ✗     | ✗       |

## Development State & Roadmap

Currently, *foyer* is still under heavy development.

The development state and the roadmap can be found in [foyer - Development Roadmap](https://github.com/orgs/foyer-rs/projects/2).

## Contributing

Contributions for *foyer* are warmly welcomed! 🥰

Don't forget to pass `cargo x --fast` (which runs most necessary checks and tests) locally before submitting a PR. 🚀

If you want to run a broader range of checks locally, run `cargo x`. 🙌

Thank you for your contribution~ <img src="https://raw.githubusercontent.com/foyer-rs/foyer/main/etc/logo/ferris.min.svg" height="24px" />

<!-- rustdoc-ignore-start -->

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=foyer-rs/foyer&type=Date)](https://www.star-history.com/#foyer-rs/foyer&Date)

<!-- rustdoc-ignore-end -->