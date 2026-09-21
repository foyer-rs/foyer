# OpenDAL secondary cache (draft)

`foyer-opendal` is an unpublished experimental `Engine` over a caller-supplied
OpenDAL `Operator`. Foyer keeps memory lookup and request coalescing. The engine
owns pending writes, a process-local FIFO index, and I/O statistics. It builds
no block device or POSIX I/O engine. Statistics count successful object
reads/writes and encoded bytes; deletes, failed calls, and backend-internal
retries are excluded. They do not measure process RSS or physical backend usage.

Run the filesystem example:

```sh
cargo run -p examples --example opendal --features opendal
```

The example fetches an immutable range, waits for the background write, clears
memory, and reads the value from the OpenDAL cache. The caller configures the
operator; Fs and Redis are the intended first-version backends.

## Usage contract

- Keys are `String`; values are `Vec<u8>`. A key always identifies the same
  bytes. Include source identity, object version, and range; source reads must
  request that exact version. Updates use new keys.
- Construct with `OpenDalEngineConfig::new(op, namespace, capacity, queue_limit)`.
  `with_max_object_size(bytes)` is optional. `capacity` and `queue_limit` must be
  nonzero. `max_object_size` defaults to `capacity` and must be nonzero and
  `<= capacity`.
- Give each instance a fresh exclusive namespace and `RecoverMode::None`. The
  namespace is a nonempty relative prefix: no empty, `.`, or `..` segments, and
  no leading or trailing whitespace, NUL, or `\`. Object paths are
  `{namespace}/{hash:016x}/{sequence:016x}`. Exclusivity is the caller's unused
  prefix, not a lock or probe. Nested prefixes such as `cache` and `cache/other`
  are not isolated from each other.
- Restart starts with an empty index and a new unused prefix. Recovery and
  shared writers are unsupported.
- Cache writes are best-effort. Admission rejection, a full command queue, an
  encode failure, or a failed object write discards that cache write and must
  not fail a successful source fetch.
- `get`: missing object → miss; backend or decode error → error.
  `get_or_fetch` does not fall back to the source on load errors.
- `wait()` drains commands admitted before the barrier and does not report
  background errors. `close()` rejects new writes, drains already-admitted
  commands, and returns a bounded set of background failures (up to 8 records,
  plus a dropped count). A repeated `close()` returns `Ok`. Reads stay available
  after close. Hybrid `flush_on_close` (default `true`) flushes memory into the
  engine before that close. Worst-case close can take minutes: a finite command
  queue times the per-command I/O and cleanup timeout.
- Residual objects remain after a normal close, skipped cleanup, crash, timed-out
  write, or failed delete. The caller or operator reclaims them.

## Limits

| Bound | Value |
| --- | --- |
| I/O timeout | 2 s per read, write, or delete |
| Command slots | `clamp(queue_limit / 64, 1, 32)` |
| Indexed entries | `clamp(capacity / 64, 1, 65536)` |
| Error records | 8, plus a dropped-older count |
| Concurrent reads | 8 |
| Cleanup deletions per batch | at most 8; the rest are skipped |
| Minimum accounting charge | 64 bytes |
| Encoded object | whole object, `<= max_object_size` |

`capacity` counts indexed encoded bytes. `queue_limit` counts admitted pending
payload bytes. Neither is an RSS or physical-storage quota. A read that cannot
obtain a permit within the I/O timeout is throttled.

## Cache object format

Little-endian, no padding. Not compatible with the prototype `FOYODL01`
envelope. Not a generic foyer codec or recovery format.

| Offset | Size | Field |
| --- | --- | --- |
| 0 | 8 | magic `FODL0001` |
| 8 | 4 | `key_len` (`u32`) |
| 12 | 4 | `value_len` (`u32`) |
| 16 | 4 | CRC-32/ISO-HDLC of magic, `key_len`, `value_len`, key, and value |
| 20 | `key_len` | key UTF-8 bytes |
| 20 + `key_len` | `value_len` | value bytes |

Encoded size is `20 + key_len + value_len`. The engine estimates with that
header length and reads at most `max_object_size + 1` bytes. Decode checks the
buffer against `max_object_size` before trusting length fields, then requires
exact framing, magic, checksum, and a full match with the requested key. A
failed object returns an error and no value bytes.

## Why Keeper identity is needed

Several callers can submit the same immutable entry while its write is pending.
The engine queues one backend write and retains the current Keeper registration.
Registration order and engine arrival order can differ:

```text
Keeper registers A, then B for the same key.
Engine receives B, then A.
A.is_current() == false: keep B and discard A.
```

Dropping A must not remove B from Keeper. A completed entry skips another write
until it is evicted, missing, or rejected as corrupt.
