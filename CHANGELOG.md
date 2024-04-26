## 2024-04-26

| crate | version |
| - | - |
| foyer | 0.8.6 |
| foyer-common | 0.6.3 |
| foyer-intrusive | 0.5.3 |
| foyer-memory | 0.3.3 |
| foyer-storage | 0.7.3 |
| foyer-storage-bench | 0.7.3 |

<details>

### Changes

- Remove unused dependencies.
- Remove hakari workspace hack.

</details>

## 2024-04-26

| crate | version |
| - | - |
| foyer | 0.8.5 |

<details>

### Changes

- Expose `EntryState`, `HybridEntry`.
- Expose `StorageWriter`, `Metrics`, `get_metrics_registry`, `set_metrics_registry`.
- Expose `RangeBoundsExt`, `BufExt`, `BufMutExt`.
- Re-export `ahash::RandomState`.
- Loose `entry()` args trait bounds.

</details>

## 2024-04-25

| crate | version |
| - | - |
| foyer | 0.8.4 |

<details>

### Changes

- Expose `HybridCacheEntry`.

</details>

## 2024-04-25

| crate | version |
| - | - |
| foyer | 0.8.3 |

<details>

### Changes

- Expose `Key`, `Value`, `StorageKey`, `StorageValue` traits.

</details>

## 2024-04-24

| crate | version |
| - | - |
| foyer | 0.8.2 |
| foyer-common | 0.6.2 |
| foyer-intrusive | 0.5.2 |
| foyer-memory | 0.3.2 |
| foyer-storage | 0.7.2 |
| foyer-storage-bench | 0.7.2 |
| foyer-workspace-hack | 0.5.2 |

<details>

### Changes

- Add `nightly` feature to make it compatible with night toolchain.

</details>

## 2024-04-24

| crate | version |
| - | - |
| foyer | 0.8.1 |
| foyer-common | 0.6.1 |
| foyer-intrusive | 0.5.1 |
| foyer-memory | 0.3.1 |
| foyer-storage | 0.7.1 |
| foyer-storage-bench | 0.7.1 |
| foyer-workspace-hack | 0.5.1 |

<details>

### Changes

- Add `with_flush` to enable flush for each io.
- Loose MSRV to 1.76 .
- Flush the device on store close.

</details>

## 2024-04-23

| crate | version |
| - | - |
| foyer | 0.8.0 |
| foyer-common | 0.6.0 |
| foyer-intrusive | 0.5.0 |
| foyer-memory | 0.3.0 |
| foyer-storage | 0.7.0 |
| foyer-storage-bench | 0.7.0 |
| foyer-workspace-hack | 0.5.0 |

<details>

### Changes

- Combine in-memory cache and disk cache into `HybridCache`.
- Refine APIs, make them more user-friendly.
- Refine `Key`, `Value`, `StorageKey`, `StorageValue` traits.
- Support `serde` for storage key and value serialization and deserialization.
- Loose trait bounds for key and value.
- Add configurable ghost queue for S3FIFO.
- Fix S3FIFO eviction bugs.
- Add more examples.

</details>

## 2024-04-11

| crate | version |
| - | - |
| foyer | 0.7.0 |
| foyer-common | 0.5.0 |
| foyer-intrusive | 0.4.0 |
| foyer-memory | 0.2.0 |
| foyer-storage | 0.6.0 |
| foyer-storage-bench | 0.6.0 |
| foyer-workspace-hack | 0.4.0 |

<details>

### Changes

- Make `foyer` compatible with rust stable toolchain (MSRV = 1.77.2). 🎉

</details>

## 2024-04-09

| crate | version |
| - | - |
| foyer-storage | 0.5.1 |
| foyer-memory | 0.1.4 |

<details>

### Changes

- fix: Fix panics on `state()` for s3fifo entry.
- fix: Enable `offset_of` feature for `foyer-storage`.

</details>

## 2024-04-08

| crate | version |
| - | - |
| foyer-intrusive | 0.3.1 |
| foyer-memory | 0.1.3 |

<details>

### Changes

- feat: Introduce s3fifo to `foyer-memory`.
- fix: Fix doctest for `foyer-intrusive`.

</details>

## 2024-03-21

| crate | version |
| - | - |
| foyer-memory | 0.1.2 |

<details>

### Changes

- fix: `foyer-memory` export `DefaultCacheEventListener`.

</details>

## 2024-03-14

| crate | version |
| - | - |
| foyer-memory | 0.1.1 |

<details>

### Changes

- Make eviction config clonable.

</details>

## 2024-03-13

| crate | version |
| - | - |
| foyer-storage-bench | 0.5.1 |

<details>

### Changes

- Fix `foyer-storage-bench` build with `trace` feature.

</details>

## 2024-03-12

| crate | version |
| - | - |
| foyer | 0.6.0 |
| foyer-common | 0.4.0 |
| foyer-intrusive | 0.3.0 |
| foyer-memory | 0.1.0 |
| foyer-storage | 0.5.0 |
| foyer-storage-bench | 0.5.0 |
| foyer-workspace-hack | 0.3.0 |

<details>

### Changes

- Release foyer in-memory cache as crate `foyer-memory`.
- Bump other components with changes.

</details>

## 2023-12-28

| crate | version |
| - | - |
| foyer | 0.5.0 |
| foyer-common | 0.3.0 |
| foyer-intrusive | 0.2.0 |
| foyer-storage | 0.4.0 |
| foyer-storage-bench | 0.4.0 |
| foyer-workspace-hack | 0.2.0 |

<details>

### Changes

- Bump rust-toolchain to "nightly-2023-12-26".
- Introduce time-series distribution args to bench tool. [#253](https://github.com/MrCroxx/foyer/pull/253)

### Fixes

- Fix duplicated insert drop metrics.

</details>

## 2023-12-22

| crate | version |
| - | - |
| foyer | 0.4.0 |
| foyer-storage | 0.3.0 |
| foyer-storage-bench | 0.3.0 |
| foyer-workspace-hack | 0.1.1 |

<details>

### Changes

- Remove config `flusher_buffer_capacity`.

### Fixes

- Fix benchmark tool cache miss ratio.

</details>

## 2023-12-20

| crate | version |
| - | - |
| foyer-storage | 0.2.2 |

<details>

- Fix metrics for writer dropping.
- Add interface `insert_async_with_callback` and `insert_if_not_exists_async_with_callback` for callers to get the insert result.

</details>

## 2023-12-18

| crate | version |
| - | - |
| foyer-storage | 0.2.1 |

<details>

- Introduce the entry size histogram, update metrics.

</details>

## 2023-12-18

| crate | version |
| - | - |
| foyer | 0.3.0 |
| foyer-common | 0.2.0 |
| foyer-storage | 0.2.0 |
| foyer-storage-bench | 0.2.0 |

<details>

- Introduce the associated type `Cursor` for trait `Key` and `Value` to reduce unnecessary buffer copy if possible.
- Remove the ring buffer and continuum tracker for they are no longer needed.
- Update the configuration of the storage engine and the benchmark tool.

</details>

## 2023-11-29

| crate | version |
| - | - |
| foyer | 0.2.0 |
| foyer-common | 0.1.0 |
| foyer-intrusive | 0.1.0 |
| foyer-storage | 0.1.0 |
| foyer-storage-bench | 0.1.0 |
| foyer-workspace-hack | 0.1.0 |

<details>

The first version that can be used as file cache.

The write model and the design of storage engine has been switched from CacheLib navy version to the ring buffer version (which was highly inspired by MySQL 8.0 link_buf).

Introduces `Store`, `RuntimeStore`, `LazyStore` to simplify usage. In most cases, `RuntimeStore` is preferred to use a dedicated tokio runtime to serve **foyer** to avoid the influence to the user's runtime. If lazy-load is needed, use `RuntimeLazyStore` instead.

The implementation of **foyer** is separated into multiple crates. But importing `foyer` is enough for it re-exports the crates that **foyer**'s user needs.

Brief description about the subcrates:

- foyer-common: Provide basic data structures and algorithms.
- foyer-intrusive: Provide intrusive containers for implementing eviction lists and collections. Intrisive data structures provide the ability to implement low-cost multi-index data structures, which will be used for the memory cache in future.
- foyer-storage: Provide the file cache storage engine and wrappers to simplify the code.
- foyer-storage-bench: Runnable benchmark tool for the file cache storage engine.
- foyer-workspace-hack: Generated by [hakari](https://crates.io/crates/hakari) to prevent building each crate from triggering building from scratch.

</details>


## 2023-05-12

| crate | version |
| - | - |
| foyer | 0.1.0 |

<details>

Initial version with just bacis interfaces.

</details>
