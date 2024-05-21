# foyer

![Crates.io Version](https://img.shields.io/crates/v/foyer)
![Crates.io MSRV](https://img.shields.io/crates/msrv/foyer)
![GitHub License](https://img.shields.io/github/license/mrcroxx/foyer)

[![CI (main)](https://github.com/MrCroxx/foyer/actions/workflows/main.yml/badge.svg)](https://github.com/MrCroxx/foyer/actions/workflows/main.yml)
[![License Checker](https://github.com/MrCroxx/foyer/actions/workflows/license_check.yml/badge.svg)](https://github.com/MrCroxx/foyer/actions/workflows/license_check.yml)
[![codecov](https://codecov.io/github/MrCroxx/foyer/branch/main/graph/badge.svg?token=YO33YQCB70)](https://codecov.io/github/MrCroxx/foyer)

*foyer* aims to be a user-friendly hybrid cache lib in Rust. 

*foyer* is inspired by [Facebook/CacheLib](https://github.com/facebook/cachelib), [ben-manes/caffeine](https://github.com/ben-manes/caffeine), and other projects, which is an excellent hybrid cache lib in C++. *foyer* is not only a 'rewrite in Rust project', but provide some features that *CacheLib* doesn't have for now.

## Features

- **Hybrid Cache**: Seamlessly integrates both in-memory and disk-based caching for optimal performance and flexibility.
- **Plug-and-Play Algorithms**: Empowers users with easily replaceable caching algorithms, ensuring adaptability to diverse use cases.
- **Thread Safety**: Built to handle high concurrency with robust thread-safe mechanisms, guaranteeing reliable performance under heavy loads.
- **User-Friendly Interface**: Offers a simple and intuitive API, making cache integration effortless and accessible for developers of all levels.

## Usage

To use *foyer* in your project, add this line to the `dependencies` section of `Cargo.toml`.

```toml
foyer = "0.8"
```

If your project is using the nightly rust toolchain, the `nightly` feature needs to be enabled.

```toml
foyer = { version = "0.8", features = ["nightly"] }
```

### Out-of-the-box In-memory Cache

```rust
use foyer::{Cache, CacheBuilder};

fn main() {
    let cache: Cache<String, String> = CacheBuilder::new(16).build();

    let entry = cache.insert("hello".to_string(), "world".to_string());
    let e = cache.get("hello").unwrap();

    assert_eq!(entry.value(), e.value());
}
```

### Easy-to-use Hybrid Cache

```rust
use foyer::{DirectFsDeviceOptionsBuilder, HybridCache, HybridCacheBuilder};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;

    let hybrid: HybridCache<u64, String> = HybridCacheBuilder::new()
        .memory(64 * 1024 * 1024)
        .storage()
        .with_device_config(
            DirectFsDeviceOptionsBuilder::new(dir.path())
                .with_capacity(256 * 1024 * 1024)
                .build(),
        )
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

```rust
use std::sync::Arc;

use anyhow::Result;
use chrono::Datelike;
use foyer::{
    CacheContext, DirectFsDeviceOptionsBuilder, FifoPicker, HybridCache, HybridCacheBuilder, LruConfig,
    RateLimitPicker, RecoverMode, RuntimeConfigBuilder, TombstoneLogConfigBuilder,
};
use tempfile::tempdir;

#[tokio::main]
async fn main() -> Result<()> {
    let dir = tempdir()?;

    let hybrid: HybridCache<u64, String> = HybridCacheBuilder::new()
        .memory(1024)
        .with_shards(4)
        .with_eviction_config(LruConfig {
            high_priority_pool_ratio: 0.1,
        })
        .with_object_pool_capacity(1024)
        .with_hash_builder(ahash::RandomState::default())
        .with_weighter(|_key, value: &String| value.len())
        .storage()
        .with_device_config(
            DirectFsDeviceOptionsBuilder::new(dir.path())
                .with_capacity(64 * 1024 * 1024)
                .with_file_size(4 * 1024 * 1024)
                .build(),
        )
        .with_flush(true)
        .with_indexer_shards(64)
        .with_recover_mode(RecoverMode::Quiet)
        .with_recover_concurrency(8)
        .with_flushers(2)
        .with_reclaimers(2)
        .with_clean_region_threshold(4)
        .with_eviction_pickers(vec![Box::<FifoPicker>::default()])
        .with_admission_picker(Arc::new(RateLimitPicker::new(100 * 1024 * 1024)))
        .with_reinsertion_picker(Arc::new(RateLimitPicker::new(10 * 1024 * 1024)))
        .with_compression(foyer::Compression::Lz4)
        .with_tombstone_log_config(
            TombstoneLogConfigBuilder::new(dir.path().join("tombstone-log-file"))
                .with_flush(true)
                .build(),
        )
        .with_runtime_config(
            RuntimeConfigBuilder::new()
                .with_thread_name("foyer")
                .with_worker_threads(4)
                .build(),
        )
        .build()
        .await?;

    hybrid.insert(42, "The answer to life, the universe, and everything.".to_string());
    assert_eq!(
        hybrid.get(&42).await?.unwrap().value(),
        "The answer to life, the universe, and everything."
    );

    let e = hybrid
        .fetch(20230512, || async {
            let value = mock().await?;
            Ok((value, CacheContext::default()))
        })
        .await?;
    assert_eq!(e.key(), &20230512);
    assert_eq!(e.value(), "Hello, foyer.");

    hybrid.close().await.unwrap();

    Ok(())
}

async fn mock() -> Result<String> {
    let now = chrono::Utc::now();
    if format!("{}{}{}", now.year(), now.month(), now.day()) == "20230512" {
        return Err(anyhow::anyhow!("Hi, time traveler!"));
    }
    Ok("Hello, foyer.".to_string())
}
```

### Other Cases

More examples and details can be found [here](https://github.com/MrCroxx/foyer/tree/main/examples).

## Supported Rust Versions

*foyer* is built against the latest stable release. The minimum supported version is 1.76. The current *foyer* version is not guaranteed to build on Rust versions earlier than the minimum supported version.

## Development state & Roadmap

Currently, *foyer* is still under heavy development.

The development state and the roadmap can be found [here](https://github.com/users/MrCroxx/projects/4).

## Contributing

Contributions for *foyer* are warmly welcomed! 🥰

Don't forget to pass `make fast` (which means fast check & test) locally before submitting a PR. 🚀

If you want to run a broader range of checks locally, rur `make full`. 🙌
