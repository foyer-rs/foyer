# Setup Hybrid Cache

This article walks through the process of setting up a hybrid cache.

## 1. Add foyer as a dependency

Add ***foyer*** to the `dependencies` section of your `Cargo.toml`.

```toml
foyer = "0.20"
```

When compiling with nightly Rust, enable the `nightly` feature. Toggle `serde` (auto `Code` impls for `Serialize`/`Deserialize` types) or `tracing` (tail-based tracing support) as needed.

```toml
foyer = { version = "0.20", features = ["nightly", "serde", "tracing"] }
```

## 2. Build a `HybridCache`

`HybridCache`[^hybrid-cache] is built with `HybridCacheBuilder`[^hybrid-cache-builder]. The builder has two phases: `memory()` configures the in-memory tier and `storage()` configures the disk tier.

### 2.1 Start from the builder

Global options such as the cache name, policy, metrics registry, tracing options, or event listener are set before entering the memory phase.

```rust
use foyer::{HybridCacheBuilder, HybridCachePolicy};
use mixtrics::registry::prometheus::PrometheusMetricsRegistry;
use prometheus::Registry;

let prometheus = Registry::new();
let registry = PrometheusMetricsRegistry::new(prometheus.clone());

let builder = HybridCacheBuilder::new()
    .with_name("foyer")
    .with_policy(HybridCachePolicy::WriteOnEviction)
    .with_metrics_registry(Box::new(registry));
```

Enable tail-based tracing with `with_tracing_options()` when the `tracing` feature is on. You can update the thresholds at runtime via `hybrid.update_tracing_options()`.

### 2.2 Configure the in-memory tier

Call `.memory(capacity)`[^memory] to enter the memory phase. The API mirrors `CacheBuilder`[^cache-builder]: tune shards, eviction algorithms, weighters, filters, or hash builders exactly as described in the [in-memory tutorial](./01-in-memory-cache.md).

```rust
use foyer::{EvictionConfig, HybridCacheBuilder, LruConfig};

let builder = HybridCacheBuilder::new()
    .memory(64 * 1024 * 1024)
    .with_eviction_config(EvictionConfig::Lru(LruConfig {
        high_priority_pool_ratio: 0.9,
    }))
    .with_weighter(|_, value: &String| value.len());
```

### 2.3 Configure the disk tier

Call `storage()`[^storage] to transition into the disk builder. The storage phase needs:

1. A `Device`[^device-builder] that decides where blocks live.
2. An optional `IoEngine` for async reads/writes.
3. An engine config (currently the block-based engine) that controls layout and eviction policy on disk.

#### Choose a device

Pick the device implementation that matches your deployment:

- `FsDeviceBuilder`[^fs-device-builder]: stores blocks in a directory on an existing filesystem (default choice).
- `FileDeviceBuilder`[^file-device-builder]: uses a pre-allocated file or block device to skip filesystem overhead.
- `CombinedDeviceBuilder`/`PartialDeviceBuilder`: stitch multiple devices or partitions together.

```rust
use foyer::{DeviceBuilder, FsDeviceBuilder};

let device = FsDeviceBuilder::new("/data/foyer")
    .with_capacity(512 * 1024 * 1024) // bytes
    .build()?;
```

#### Protect disk throughput

Disk bandwidth is orders of magnitude slower than RAM. Apply a `Throttle`[^throttle] to the device to cap IOPS/throughput so cache traffic does not starve the rest of your system.

```rust
use foyer::{FsDeviceBuilder, Throttle};

let device = FsDeviceBuilder::new("/data/foyer")
    .with_throttle(
        Throttle::new()
            .with_read_iops(4000)
            .with_write_iops(2000)
            .with_read_throughput(800 * 1024 * 1024)
            .with_write_throughput(100 * 1024 * 1024),
    )
    .build()?;
```

#### Pick an I/O engine (optional)

`HybridCache` ships with a psync-based engine by default. Provide your own implementation (for example `PsyncIoEngineBuilder`[^psync]) when you need to share an IO engine instance or tweak its settings.

```rust
use foyer::PsyncIoEngineBuilder;

let io_engine = PsyncIoEngineBuilder::new().build().await?;
```

Attach it with `.with_io_engine(io_engine)` on the storage builder.

#### Configure the disk engine

The `BlockEngineBuilder`[^block-engine] serves most workloads. It writes entries to 4K-aligned blocks, keeps only metadata in memory, and supports mixed eviction pickers.

```rust
use foyer::{BlockEngineBuilder, DeviceBuilder, FsDeviceBuilder, FifoPicker, RejectAll, StorageFilter};

let device = FsDeviceBuilder::new("/data/foyer").with_capacity(256 * 1024 * 1024).build()?;
let engine = BlockEngineBuilder::new(device)
    .with_block_size(16 * 1024 * 1024)
    .with_indexer_shards(64)
    .with_buffer_pool_size(256 * 1024 * 1024)
    .with_eviction_pickers(vec![Box::<FifoPicker>::default()])
    .with_clean_block_threshold(4);
```

Pass the engine config into `.with_engine_config(engine)`. You can register multiple pickers, adjust block sizes, or enable the tombstone log depending on workload.

#### Finalize the storage builder

Before calling `build()`, tune shared knobs:

- `.with_compression(Compression::Lz4)`[^compression] to enable on-disk compression.
- `.with_recover_mode(RecoverMode::Quiet)`[^recover-mode] to choose between fast or strict recovery.
- `.with_runtime_options(RuntimeOptions::Separated { .. })`[^runtime-options] to dedicate Tokio runtimes for disk IO.

When `.build().await` runs, the builder initializes the memory tier, opens the storage backend, wires up metrics, and returns a `HybridCache`.

### 2.4 A complete example

```rust
use foyer::{
    BlockEngineBuilder, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder, HybridCachePolicy,
    PsyncIoEngineBuilder, RecoverMode,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let device = FsDeviceBuilder::new("/data/foyer")
        .with_capacity(512 * 1024 * 1024)
        .build()?;

    let io_engine = PsyncIoEngineBuilder::new().build().await?;

    let hybrid: HybridCache<u64, String> = HybridCacheBuilder::new()
        .with_policy(HybridCachePolicy::WriteOnEviction)
        .memory(64 * 1024 * 1024)
        .with_weighter(|_, value: &String| value.len())
        .storage()
        .with_io_engine(io_engine)
        .with_engine_config(BlockEngineBuilder::new(device))
        .with_recover_mode(RecoverMode::Quiet)
        .build()
        .await?;

    // Use the hybrid cache ...
    hybrid.insert(42, "The answer to life, the universe, and everything.".to_string());

    Ok(())
}
```

## 3. Use `HybridCache`

`HybridCache` exposes synchronous insert/remove helpers plus async lookups:

- `insert()` / `insert_with_properties()` push entries into the memory tier (and optionally into storage depending on policy).
- `get()` performs an async lookup across both tiers.
- `get_or_fetch()` deduplicates concurrent misses and writes back the fetched value automatically.
- `writer()` returns a `HybridCacheWriter`[^writer] for advanced flows (disk-only inserts, forced flushes, etc.).

```rust
use foyer::{BlockEngineBuilder, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let device = FsDeviceBuilder::new("/data/foyer").with_capacity(256 * 1024 * 1024).build()?;

    let hybrid: HybridCache<u64, String> = HybridCacheBuilder::new()
        .memory(64 * 1024 * 1024)
        .storage()
        .with_engine_config(BlockEngineBuilder::new(device))
        .build()
        .await?;

    let entry = hybrid
        .fetch(42, || async {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            Ok("The answer to life, the universe, and everything.".to_string())
        })
        .await?;

    assert_eq!(entry.value(), "The answer to life, the universe, and everything.");
    hybrid.close().await?;
    Ok(())
}
```

Keys and values must implement the `Code` trait (primitive numeric/string types already do; enable the `serde` feature for auto-impls).

[^hybrid-cache]: https://docs.rs/foyer/latest/foyer/struct.HybridCache.html
[^hybrid-cache-builder]: https://docs.rs/foyer/latest/foyer/struct.HybridCacheBuilder.html
[^memory]: https://docs.rs/foyer/latest/foyer/struct.HybridCacheBuilder.html#method.memory
[^storage]: https://docs.rs/foyer/latest/foyer/struct.HybridCacheBuilderPhaseMemory.html#method.storage
[^cache-builder]: https://docs.rs/foyer/latest/foyer/struct.CacheBuilder.html
[^device-builder]: https://docs.rs/foyer/latest/foyer/trait.DeviceBuilder.html
[^fs-device-builder]: https://docs.rs/foyer/latest/foyer/struct.FsDeviceBuilder.html
[^file-device-builder]: https://docs.rs/foyer/latest/foyer/struct.FileDeviceBuilder.html
[^throttle]: https://docs.rs/foyer/latest/foyer/struct.Throttle.html
[^psync]: https://docs.rs/foyer/latest/foyer/struct.PsyncIoEngineBuilder.html
[^block-engine]: https://docs.rs/foyer/latest/foyer/struct.BlockEngineBuilder.html
[^compression]: https://docs.rs/foyer/latest/foyer/enum.Compression.html
[^recover-mode]: https://docs.rs/foyer/latest/foyer/enum.RecoverMode.html
[^runtime-options]: https://docs.rs/foyer/latest/foyer/enum.RuntimeOptions.html
[^writer]: https://docs.rs/foyer/latest/foyer/struct.HybridCacheWriter.html
