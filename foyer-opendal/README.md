# OpenDAL secondary cache (draft)

`foyer-opendal` implements Foyer's `Engine` interface over a caller-supplied
OpenDAL `Operator`. Foyer keeps memory lookup and request coalescing; OpenDAL
stores cache objects after memory. The engine owns its pending writes, index,
and FIFO eviction. It owns its I/O statistics and constructs no block device or
POSIX I/O engine. Statistics count successful object reads/writes and encoded
bytes; deletes, failed calls, and backend-internal retries are excluded. They do
not measure physical storage or indexed capacity.

Run the filesystem example:

```sh
cargo run -p examples --example opendal --features opendal
```

The example fetches an immutable range, waits for the background write, clears
memory, and reads the value from the OpenDAL cache. Backend configuration belongs
to the caller; the same engine can use an Fs or Redis operator.

## Usage contract

- Keys are `String`; values are `Vec<u8>`. A key must always identify identical
  bytes. Include source identity, object version, and range; source reads must
  request that exact version. Updates use new keys, so business correctness does
  not depend on strict cache deletion.
- Give each cache instance a fresh, exclusive namespace and use
  `RecoverMode::None`. The index lives in memory. Reusing a namespace after a
  restart or sharing it between writers is unsupported.
- The operator must support read, write, and delete. Every IO has a two-second
  timeout. Reads surface backend/format errors; `close()` surfaces recorded
  background failures. `wait()` drains earlier commands but does not report
  their failures.
- Capacity counts indexed encoded bytes; the queue limit counts admitted
  payload bytes. Neither bounds total RAM or physical backend usage. Failed
  writes/deletes can leave objects behind.

## Why Keeper identity is needed

Several callers can submit the same immutable entry while its write is pending.
The engine queues one backend write and retains the current Keeper registration,
so memory eviction cannot make pending data disappear. Registration order and
engine arrival order can differ:

```text
Keeper registers A, then B for the same key.
Engine receives B, then A.
A.is_current() == false: keep B and discard A.
```

Dropping A must not remove B from Keeper. `is_current()` is queried only when
another submission matches a pending entry. A completed entry skips another
write until it is evicted, missing, or rejected as corrupt.

## Remaining design work

This crate is unpublished and experimental. The stack makes the integration
concrete for review; it does not complete the production engine in
[the tracking issue](https://github.com/foyer-rs/foyer/issues/1350).

- [#1353](https://github.com/foyer-rs/foyer/issues/1353): bound total memory,
  command queues, metadata, and physical storage.
- [#1354](https://github.com/foyer-rs/foyer/issues/1354): finish failure and
  lifecycle handling, including cleanup after uncertain writes.
- [#1355](https://github.com/foyer-rs/foyer/issues/1355) and
  [#1356](https://github.com/foyer-rs/foyer/issues/1356): settle namespace,
  recovery, and the persisted format.
- [#1357](https://github.com/foyer-rs/foyer/issues/1357) and
  [#1358](https://github.com/foyer-rs/foyer/issues/1358): finish the supported
  engine API and end-to-end acceptance.
