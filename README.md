# foyer

[![CI (main)](https://github.com/MrCroxx/foyer/actions/workflows/main.yml/badge.svg)](https://github.com/MrCroxx/foyer/actions/workflows/main.yml) [![License Checker](https://github.com/MrCroxx/foyer/actions/workflows/license_check.yml/badge.svg)](https://github.com/MrCroxx/foyer/actions/workflows/license_check.yml)

*foyer* aims to be a user-friendly hybrid cache lib in Rust. 

*foyer* is inspired by [Facebook/CacheLib](https://github.com/facebook/cachelib), which is an excellent hybrid cache lib in C++. *foyer* is not only a 'rewrite in Rust project', but provide some features that *CacheLib* doesn't have for now.

## Development state

Currently, *foyer* only finished few features, and is still under heavy development.

## Roadmap

- [ ] Better intrusive index collectios for memory cache.
- [ ] Hybrid memory and disk cache.
- [ ] Reinsertion for disk cache.
- [ ] Raw device and single file device support.
- [ ] More detailed metrics or statistics.

## Contributing

Contributions for *foyer* are welcomed! Issues can be found [here](https://github.com/MrCroxx/foyer/issues).

Make sure you've passed `make check` and `make test` before request a review, or CI will fail.
