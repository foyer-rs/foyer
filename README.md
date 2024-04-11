# foyer

![Crates.io Version](https://img.shields.io/crates/v/foyer)
![Crates.io MSRV](https://img.shields.io/crates/msrv/foyer)
![GitHub License](https://img.shields.io/github/license/mrcroxx/foyer)

[![CI (main)](https://github.com/MrCroxx/foyer/actions/workflows/main.yml/badge.svg)](https://github.com/MrCroxx/foyer/actions/workflows/main.yml)
[![License Checker](https://github.com/MrCroxx/foyer/actions/workflows/license_check.yml/badge.svg)](https://github.com/MrCroxx/foyer/actions/workflows/license_check.yml)
[![codecov](https://codecov.io/github/MrCroxx/foyer/branch/main/graph/badge.svg?token=YO33YQCB70)](https://codecov.io/github/MrCroxx/foyer)

*foyer* aims to be a user-friendly hybrid cache lib in Rust. 

*foyer* is inspired by [Facebook/CacheLib](https://github.com/facebook/cachelib), which is an excellent hybrid cache lib in C++. *foyer* is not only a 'rewrite in Rust project', but provide some features that *CacheLib* doesn't have for now.

## Supported Rust Versions

*foyer* is built against the latest stable release. The minimum supported version is 1.77.2. The current *foyer* version is not guaranteed to build on Rust versions earlier than the minimum supported version.

## Development state

Currently, *foyer* only finished few features, and is still under heavy development.

## Features

- [x] in-memory cache
  - [x] FIFO
  - [x] LRU with priority pool
  - [x] 3-qeue w-TinyLFU (imspired by [caffeine](https://github.com/ben-manes/caffeine))
  - [x] S3FIFO without Ghost Queue
- [x] disk cache
- [ ] TTL (time to live)

## Examples

The examples can be found [here](https://github.com/MrCroxx/foyer/tree/main/examples).

## Roadmap

- [ ] More user-friendly API.
- [ ] User-friendly Documents and examples.
- [ ] Support TTL.
- [ ] Simplify `foyer-storage`.
- [ ] Refactor `foyer-storage` region reclaiming policy.
- [ ] Support on Windows.
- [ ] Unify in-memory cache and disk cache configuration.

## Contributing

Contributions for *foyer* are welcomed! Issues can be found [here](https://github.com/MrCroxx/foyer/issues).

Make sure you've passed `make check` and `make test` before request a review, or CI will fail.
