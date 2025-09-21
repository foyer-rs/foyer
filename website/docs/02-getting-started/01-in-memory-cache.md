# Setup In-memory Cache

This article will guide you through the process of setting up an in-memory cache.

:::tip for hybrid cache users

The in-memory cache `Cache` is literally an in-memory only cache. Which provides non-overhead in-memory cache implement for users only want to replace their in-memory cache with ***foyer***, and cannot upgrade to a hybrid cache later.

The hybrid cache `HybridCache` also provides a in-memory only mode. If you want to try ***foyer*** out with the in-memory mode and have the needs of a hybrid cache, please use `HybridCache`. See [Tutorial - Hybrid Cache](/docs/getting-started/hybrid-cache).

:::

## 1. Add foyer as a dependency

Add this line to the `[dependencies]` section of your project's `Cargo.toml`.

```toml
foyer = "0.12"
```

If you are using a nightly version of the rust toolchain, the `nightly` feature is needed.

```toml
foyer = { version = "0.12", features = ["nightly"] }
```

## 2. Build a `Cache`

### 2.1 Build with a builder

`Cache`[^cache] can be built via `CacheBuilder`[^cache-builder]. For the default configuration, only `capacity` is required.

```rust
use foyer::{Cache, CacheBuilder};

// ... ...

let cache: Cache<String, String> = CacheBuilder::new(16).build();
```

### 2.2 Count entries by weight

By default, `Cache` counts its usage for eviction by entry count. In the case below, which is 16.

Counting by entry count may be not suitable for all cases. So, ***foyer*** also provides an interface to let the user decide how to count the usage.

```rust
let cache: Cache<String, String> = CacheBuilder::new(10000)
  .with_weighter(|key, value| key.len() + value.len())
  .build();
```

With `with_weighter()`[^with-weighter], you can customize how `Cache` counts its usage. In the case below, the usage is counted by the total character length of the entry.

The weighter can be implemented to count anything. Count, length, memory usage, or anything you want.

### 2.3 Mitigate hot shards

The in-memory cache in ***foyer*** is a sharded cache. Each shard is protected by an individual mutex. Sometimes, one or more shards can be much hotter than others and cause performance regression.

There are two ways to mitigate the hot shards.

1. Scale the shards

Scaling the shard may reduce the possibility to create hot shards. You can scale the shards to a higher number with `with_shards()`.[^with-shards]

```rust
let cache: Cache<String, String> = CacheBuilder::new(1024)
  .with_shards(64)
  .build();
```

2. Use a customized hasher

By default, ***foyer*** uses `ahash`[^ahash] as the hasher to determine which shard a key belongs to. You can use your own hasher with `with_hash_builder()`.[^with-hash-builder]

```rust
let cache: Cache<String, String> = CacheBuilder::new(1024)
  .with_hash_builder(CustomizedRandomState::default())
  .build();
```

### 2.4 More configurations

Please refer to the API document.[^cache-builder]

## 3. Use `Cache` as any other cache library

`Cache` provides similar interfaces to caches from any other cache library.

Here is an example.

```rust
use foyer::{Cache, CacheBuilder};

fn main() {
    let cache: Cache<String, String> = CacheBuilder::new(16).build();

    let entry = cache.insert("hello".to_string(), "world".to_string());
    let e = cache.get("hello").unwrap();

    assert_eq!(entry.value(), e.value());
}
```

For other interfaces, please refer to the API document.[^cache]

[^cache]: https://docs.rs/foyer/latest/foyer/enum.Cache.html

[^cache-builder]: https://docs.rs/foyer/latest/foyer/struct.CacheBuilder.html

[^with-weighter]: https://docs.rs/foyer/latest/foyer/struct.CacheBuilder.html#method.with_weighter

[^with-shards]: https://docs.rs/foyer/latest/foyer/struct.CacheBuilder.html#method.with_shards

[^ahash]: https://crates.io/crates/ahash

[^with-hash-builder]: https://docs.rs/foyer/latest/foyer/struct.CacheBuilder.html#method.with_hash_builder