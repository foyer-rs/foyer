# Setup In-memory Cache

This article walks through setting up the in-memory cache that ships with ***foyer***.

:::tip for hybrid cache users

`Cache`[^cache] is a pure in-memory cache. If you want to experiment with ***foyer*** now but may eventually enable disk persistence, start from `HybridCache`[^hybrid-cache]. `HybridCacheBuilder::memory()` spins up the same in-memory layer, so migrating to the hybrid guide later only requires completing the storage phase.

:::

## 1. Add foyer as a dependency

Add ***foyer*** to your project’s `Cargo.toml`.

```toml
foyer = "0.20"
```

When compiling with nightly Rust, enable the `nightly` feature.

```toml
foyer = { version = "0.20", features = ["nightly"] }
```

You can also toggle optional features such as `serde` (implements `Code` for `Serialize`/`Deserialize` types) or `tracing` (tail-based tracing, see the monitor tutorial).

## 2. Build a `Cache`

### 2.1 Build with a builder

`CacheBuilder`[^cache-builder] is the entry point. Only the in-memory capacity (measured by your weighter, see below) is required.

```rust
use foyer::{Cache, CacheBuilder};

let cache: Cache<String, String> = CacheBuilder::new(16).build();
```

### 2.2 Select an eviction algorithm

`Cache` supports FIFO, S3FIFO, LRU, LFU, and Sieve via `EvictionConfig`[^eviction-config]. Swap the default w-TinyLFU flavor with the algorithm that best fits your workload.

```rust
use foyer::{Cache, CacheBuilder, EvictionConfig, LruConfig};

let cache: Cache<String, String> = CacheBuilder::new(256)
    .with_eviction_config(EvictionConfig::Lru(LruConfig {
        high_priority_pool_ratio: 0.8,
    }))
    .build();
```

Each algorithm has its own config struct (e.g. `LruConfig`[^lru-config]) so you can tune parameters such as high-priority pools or FIFO queue sizes.

### 2.3 Count entries by weight

By default, usage equals the number of cached entries. Override that logic with `with_weighter()`[^with-weighter] to express memory consumption, byte length, cost, etc.

```rust
let cache: Cache<String, String> = CacheBuilder::new(10 * 1024 * 1024)
    .with_weighter(|key, value| key.len() + value.len())
    .build();
```

### 2.4 Filter low-value entries

`with_filter()`[^with-filter] lets you keep items out of the cache even if callers insert them.

```rust
use foyer::Filter;

let cache = CacheBuilder::new(1024)
    .with_filter(|_key: &String, value: &String| value.len() < 2048)
    .build::<_>();
```

Rejected entries return lightweight placeholders to callers but are reclaimed immediately so they never count toward usage.

### 2.5 Mitigate hot shards

The in-memory cache is sharded across mutex-protected segments. Increase shard count or provide a custom hash builder to smooth load distribution.

```rust
let cache: Cache<String, String> = CacheBuilder::new(1024)
    .with_shards(64)                               // more shards
    .with_hash_builder(ahash::RandomState::new())  // or your own hasher
    .build();
```

### 2.6 Hook observability

`CacheBuilder::with_metrics_registry()` wires ***foyer***’s counters and histograms into your monitoring stack via [`mixtrics`][^mixtrics]. Combine it with `with_event_listener()`[^event-listener] to receive structured callbacks.

```rust
use foyer::CacheBuilder;
use mixtrics::registry::prometheus::PrometheusMetricsRegistry;
use prometheus::Registry;

let prometheus = Registry::new();
let registry = PrometheusMetricsRegistry::new(prometheus.clone());

let cache = CacheBuilder::new(100)
    .with_metrics_registry(Box::new(registry))
    .build::<_>();
```

Now you can expose the Prometheus registry through HTTP (see `examples/export_metrics_prometheus_hyper.rs`) or reuse the same pattern for StatsD, OTLP, etc.

## 3. Use `Cache` as any other cache library

`Cache` exposes familiar operations.

```rust
use foyer::{Cache, CacheBuilder};

fn main() {
    let cache: Cache<String, String> = CacheBuilder::new(16).build();

    let entry = cache.insert("hello".to_string(), "world".to_string());
    let snapshot = cache.get("hello").unwrap();

    assert_eq!(entry.value(), snapshot.value());
}
```

Call `get_or_fetch()` when you want request de-duplication with async loaders. Refer to the API docs for the full surface.

[^cache]: https://docs.rs/foyer/latest/foyer/enum.Cache.html
[^hybrid-cache]: https://docs.rs/foyer/latest/foyer/struct.HybridCache.html
[^cache-builder]: https://docs.rs/foyer/latest/foyer/struct.CacheBuilder.html
[^eviction-config]: https://docs.rs/foyer/latest/foyer/enum.EvictionConfig.html
[^lru-config]: https://docs.rs/foyer/latest/foyer/struct.LruConfig.html
[^with-weighter]: https://docs.rs/foyer/latest/foyer/struct.CacheBuilder.html#method.with_weighter
[^with-filter]: https://docs.rs/foyer/latest/foyer/trait.Filter.html
[^event-listener]: https://docs.rs/foyer/latest/foyer/trait.EventListener.html
[^mixtrics]: https://github.com/foyer-rs/mixtrics
