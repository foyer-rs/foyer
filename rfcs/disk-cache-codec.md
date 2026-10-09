# Disk Cache Codec and Zero-Copy Decode

- Status: Draft
- Scope: `foyer-common`, `foyer-storage`, and `foyer`
- Target: A staged, backward-compatible refactor
- Translation: [Simplified Chinese](disk-cache-codec.zh-CN.md)

## 1. Summary

This RFC proposes separating application-level key/value codecs from disk cache engines and providing two paths on top of the same engine-neutral contract:

1. A default path for ordinary owned Rust types. Enabling the `serde` feature and deriving `Serialize` and `Deserialize` remains sufficient. An explicit `with_serde_codec` builder shortcut is also provided so that the persisted format is visible in configuration.
2. An advanced path for performance-sensitive workloads. A decoder consumes an owner-backed byte range, and the returned key or value may retain that range and expose validated views over it without copying fields out of the I/O buffer.
3. A common codec service shared by all storage engines. Engines remain responsible for placement, batching, I/O, recovery, and eviction, but no longer decide how application types are encoded.

The proposal is feasible with the current I/O model because reads already return ownership of their buffers. It does not require a borrowed value to escape an asynchronous `load` call.

## 2. Goals

- Keep the simple case simple: serde derives plus a Cargo feature must continue to work.
- Allow each cache instance to select its codec explicitly instead of selecting one implicitly through global features and blanket implementations.
- Decode directly from an owned I/O buffer for formats that support archived or byte-backed values.
- Preserve direct serialization into an engine-owned write buffer; do not introduce mandatory pre-serialization before enqueue.
- Make the same codec usable by the block engine and future engines.
- Store enough format identity in persistent entries to reject incompatible data deterministically during recovery.
- Preserve the current `HybridCache<K, V, S>` result type and memory-cache promotion model.

## 3. Non-goals

- Returning `&K`, `&V`, or another value that borrows from a temporary load future.
- Making compressed reads allocation-free. Decompression necessarily produces another byte buffer.
- Requiring every codec to support zero-copy decode.
- Making complete on-disk layouts portable across storage engines. Codec identity is reusable, but engine metadata and placement formats remain engine-owned.
- Removing the existing `Code` API in the first stage.

## 4. Current State

### 4.1 User-facing Coding API

`foyer-common/src/code.rs` defines `Code`, `StorageKey`, and `StorageValue`:

- `Code::encode` writes to `std::io::Write`.
- `Code::decode` reads from `std::io::Read` and returns an owned `Self`.
- `Code::estimated_size` supplies an estimate for admission and buffer sizing.
- With the `serde` feature enabled, a blanket implementation uses bincode for every `Serialize + DeserializeOwned` type.

The first requirement therefore already has a basic implementation. Its limitations are that the format is selected implicitly by a global feature, blanket implementations constrain custom implementations through coherence, and `Read` cannot transfer ownership of its backing buffer to the decoded value.

### 4.2 Storage Data Path

The block engine currently owns both persistence and application serialization:

```text
Piece<K, V>
  -> block flusher
  -> EntrySerializer::serialize
  -> aligned engine write buffer
  -> IoEngine::write

IoEngine::read
  -> owned IoB
  -> EntryHeader::read
  -> EntryDeserializer::deserialize
  -> owned (K, V)
  -> Load::Entry
  -> optional promotion into the memory cache
```

The write path is already allocation-friendly: `EntrySerializer` writes directly into the final aligned block buffer without a mandatory staging allocation. The read path discards the I/O buffer after constructing owned `K` and `V` values.

### 4.3 Constraints in the Current Layout

- `Engine::load` returns a `'static` future and `Load::Entry` owns `K` and `V`. A result borrowing from the read buffer cannot safely escape.
- A disk hit may be promoted into the memory cache. The result must remain valid independently of the load future.
- `IoEngine::read` returns the submitted buffer, so its owner can be retained without changing the low-level psync or io_uring read model.
- `bytes::Bytes::from_owner` preserves the owner's pointer without copying, but `Bytes` does not provide a type-level alignment guarantee. Slicing may shift the pointer by an arbitrary offset.
- The current entry header is 36 bytes, and the value begins immediately after it. This does not provide sufficient payload alignment for general archived formats.
- Compression runs before `Code::decode`; a view cannot point directly into compressed bytes.
- Compression configuration appears at both the store and block-engine layers. The codec refactor should establish one owner for the transform pipeline and prevent configuration drift.

## 5. Decision

Introduce an owner-backed `OwnedBytes` input and an engine-neutral `EntryCodec<K, V>` service while preserving the owned `Load<K, V, P>` type. A zero-copy value is an owned value whose internal representation retains `OwnedBytes`; it is not a Rust reference borrowing from the load future.

The unified transform pipeline is:

```text
K/V -> codec encode -> optional compression -> checksum -> engine placement
engine read -> checksum -> optional decompression -> codec decode -> K/V
```

The engine owns the physical entry envelope and placement. The codec owns only the application payload format. Compression and checksums remain outside the application codec so that every codec shares consistent corruption handling, error classification, and metrics.

## 6. Proposed API

The names in this section are provisional. The important parts are the ownership and layering contracts.

### 6.1 Owner-backed Bytes

```rust
#[derive(Clone)]
pub struct OwnedBytes {
    inner: bytes::Bytes,
}

impl OwnedBytes {
    pub fn from_owner(owner: impl AsRef<[u8]> + Send + 'static) -> Self;
    pub fn slice(&self, range: impl RangeBounds<usize>) -> Self;
    pub fn address_alignment(&self) -> usize;
    pub fn is_aligned_to(&self, alignment: usize) -> bool;
}

impl AsRef<[u8]> for OwnedBytes { /* ... */ }
```

The concrete implementation may use `bytes::Bytes::from_owner`. The owner is the buffer returned by `IoEngine::read`, not a copied `Vec<u8>`. Slices share the same owner and may begin at arbitrary byte offsets.

`OwnedBytes` must not expose engine-specific buffer types. Its constructor and alignment calculation should remain internal to `foyer-storage` until the safety contract is stable. `address_alignment` reports a property of the current slice pointer, not a stable guarantee of the `OwnedBytes` type. Every derived slice must be validated independently.

The direct-I/O buffer and decode-view layers remain separate:

```text
Direct I/O submission:
Raw / IoSliceMut / IoBuf
  -> mutable when used for reads
  -> pointer and length satisfy direct-I/O alignment

After I/O completion:
IoSliceMut -> IoSlice -> Bytes::from_owner -> OwnedBytes
  -> immutable owner-backed view
  -> arbitrary zero-copy payload slices
```

`Bytes` and `OwnedBytes` are not replacements for `IoBuf` or `IoBufMut`. Direct reads continue to use a mutable aligned buffer. An immutable `Bytes` value may be adapted for direct writes only after validating its pointer, length, and file offset; arbitrary `Bytes::slice` results must not be assumed to remain direct-I/O compatible. Converting an owner-backed `Bytes` into `BytesMut` performs a deep copy and is not a path back to the original mutable I/O allocation.

### 6.2 Value Codec

```rust
pub trait ValueCodec<T>: Send + Sync + Debug + 'static {
    fn format(&self) -> CodecFormat;

    fn estimated_size(&self, value: &T) -> Result<usize>;

    fn encode(&self, value: &T, dst: &mut dyn Write) -> Result<()>;

    fn decode(&self, src: OwnedBytes) -> Result<T>;

    fn required_alignment(&self) -> usize {
        1
    }
}
```

`decode` consumes the byte owner. A normal codec deserializes an owned `T` and drops `src`; a zero-copy codec moves `src`, or one of its slices, into `T`.

The trait is object-safe so that the selected codec can be erased after the builder checks its type bounds. `estimated_size` returns `Result` rather than silently converting a size error to zero. It is only an admission and reservation hint, not an exact length; a bounded writer tracks the actual encoded length.

### 6.3 Entry Codec Service

```rust
pub trait EntryCodec<K, V>: Send + Sync + Debug + 'static {
    fn format(&self) -> EntryCodecFormat;

    fn estimated_size(&self, key: &K, value: &V) -> Result<usize>;

    fn encode_key(&self, key: &K, dst: &mut dyn Write) -> Result<usize>;

    fn encode_value(&self, value: &V, dst: &mut dyn Write) -> Result<usize>;

    fn decode(
        &self,
        key: OwnedBytes,
        value: OwnedBytes,
    ) -> Result<(K, V)>;

    fn layout(&self) -> EntryLayoutRequirements;
}
```

`PairCodec<KC, VC>` composes two `ValueCodec` implementations. A separate entry-level trait keeps key/value framing, ordering, and alignment consistent across engines. The engine aligns the value start according to `layout`, calls `encode_value`, aligns the key start from the actual value length, and then calls `encode_key`. This preserves streaming compression and bounded direct writes without assuming that a size estimate is exact.

The codec service is passed through `EngineBuildContext<K, V>` as `Arc<dyn EntryCodec<K, V>>`. Every engine uses this service instead of calling `K::decode`, `V::decode`, or a specific serialization library directly.

### 6.4 Simple Serde Path

The first compatibility stage retains the current zero-additional-configuration behavior:

```toml
[dependencies]
foyer = { version = "...", features = ["serde"] }
serde = { version = "1", features = ["derive"] }
```

```rust
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
struct Value {
    id: u64,
    payload: String,
}
```

The default `LegacyCodeCodec` adapts the existing `Code` implementation, so no builder call is required.

An explicit shortcut is also provided for new code:

```rust
let cache = HybridCacheBuilder::new()
    .memory(memory_capacity)
    .storage()
    .with_serde_codec(SerdeFormat::BincodeV1)
    .with_engine_config(engine_config)
    .build()
    .await?;
```

The explicit form is recommended for recoverable caches because the persisted format becomes configuration rather than an implicit Cargo-feature side effect. Future serde formats can use distinct codec IDs without silently changing how existing data is interpreted.

### 6.5 Advanced Zero-Copy Path

The supported model is an owner-backed value. A bytes-oriented codec may return a value containing `OwnedBytes`; an archived codec may return a wrapper containing the owner and a validated root offset.

```rust
pub struct ArchivedValue<T> {
    bytes: OwnedBytes,
    root_offset: usize,
    marker: PhantomData<T>,
}

impl<T> ArchivedValue<T> {
    pub fn get(&self) -> &Archived<T>;
}
```

The wrapper must not store a self-referential Rust reference. It stores an owner and offsets, then constructs a validated view on access. Any `unsafe` required by a particular archive library belongs in a small, independently auditable adapter and must not be spread across engines.

Advanced configuration uses the same builder hook:

```rust
let cache = HybridCacheBuilder::new()
    .memory(memory_capacity)
    .storage()
    .with_codec(PairCodec::new(key_codec, archived_value_codec))
    .with_engine_config(engine_config)
    .build()
    .await?;
```

For uncompressed reads, this provides zero-copy payload decode after the device fills the I/O buffer. It does not promise zero-copy kernel I/O or zero-copy serialization of an arbitrary in-memory object graph.

## 7. Persistent Entry Format

The new codec path requires a versioned entry envelope. The complete byte layout remains engine-specific, but the envelope must contain at least:

- Entry magic and envelope version.
- Codec ID and codec version.
- Key offset and length.
- Value offset and length.
- Compression algorithm.
- Checksum algorithm and checksum.
- Payload alignment, or enough information to validate it.

The block-engine v2 layout should use an aligned payload start instead of placing the payload immediately after the current 36-byte header. A fixed 64-byte header is a reasonable initial option, but the implementation must derive offsets from codec-declared alignment rather than relying on a constant that happens to be aligned. Codec alignment must be a power of two and must not exceed I/O page alignment unless an engine explicitly advertises a stronger guarantee. Validation must use the actual payload slice pointer; a 4K-aligned backing allocation does not imply that every `Bytes` slice is aligned.

Recovery behavior:

- Decode only when the envelope version is known and the codec ID and version match.
- Treat an unknown codec or incompatible version as a configuration error in strict recovery mode.
- In quiet recovery mode, skip incompatible entries and record a dedicated metric; do not report them as checksum corruption.
- Keep the legacy envelope readable through `LegacyCodeCodec` during the migration window.
- Changing the codec configuration of a persistent cache requires an explicit empty-cache policy or migration. New code must never reinterpret old bytes under a different format.

## 8. Engine Compatibility Contract

An engine supports the codec layer when it satisfies the following contract:

1. It uses the configured codec for size estimation, encoding, and decoding.
2. It encodes directly into an engine-owned destination or an equivalent bounded writer; pre-serializing every enqueue is not required.
3. It submits a mutable buffer satisfying its direct-I/O pointer and length requirements, then converts the completed read result into `OwnedBytes` without copying.
4. It does not treat arbitrary `OwnedBytes` or `Bytes` slices as direct-I/O-compatible buffers.
5. It honors `EntryLayoutRequirements`, or rejects the codec at build time with a capability error.
6. It stores and validates codec format metadata in its entry envelope.
7. It applies checksums and compression in the common order defined by this RFC.

Add explicit capabilities so that incompatibility is detected at build time:

```rust
pub struct EngineCapabilities {
    pub direct_io_alignment: usize,
    pub owner_backed_reads: bool,
    pub max_payload_alignment: usize,
    pub direct_encode: bool,
}
```

`direct_io_alignment` describes the alignment required when submitting I/O. `max_payload_alignment` describes the strongest alignment the entry layout can provide to a codec payload. These are separate capabilities: preserving a 4K-aligned allocation does not make an arbitrary payload slice 4K-aligned.

The current block engine, psync I/O engine, and io_uring I/O engine can satisfy owner-backed reads because completion returns the submitted buffer. Direct reads continue to use `IoSliceMut`; the returned owner is wrapped as `Bytes` only after completion. `NoopEngine` trivially supports every codec because it never persists or decodes entries.

Future engines do not need serde-, bincode-, or archive-specific code. An engine that cannot retain a read buffer may still support owned codecs, but it must reject codecs that declare `requires_owner_backed_input`.

## 9. Compression Semantics

Zero-copy capability must be reported precisely:

- `Compression::None`: The decoded value may retain slices of the original I/O buffer.
- Compressed entry: Decompression allocates a new aligned owner-backed buffer, and the codec may retain slices of that buffer. This avoids field-by-field copies but is not disk-buffer zero-copy.
- A codec may reject compression when its format or performance contract requires direct access to persisted bytes.

Compression should be configured once at the store transform layer and then passed to engines. During migration, engine-specific compression knobs should be removed or reduced to internal aliases.

## 10. Builder and Type-bound Migration

The current `StorageKey: Key + Code` and `StorageValue: Value + Code` bounds force every disk-cache type through `Code`, even when the user provides an explicit codec. These bounds must be relaxed to provide a clean advanced API.

Add a codec type parameter only to the builder, and erase it after build:

```rust
pub struct StorageBuilder<K, V, S, P, C = LegacyCodeCodec> {
    codec: C,
    marker: PhantomData<(K, V, S, P)>,
}
```

- The default builder provides `build` only when `K: StorageKey` and `V: StorageValue`, preserving source compatibility.
- `with_codec` changes `C` and checks `C: EntryCodec<K, V>` at build time.
- The resulting types remain `Store<K, V, S, P>` and `HybridCache<K, V, S>`; the codec is stored as an erased service.
- Internal engine bounds move from `StorageKey` and `StorageValue` to `Key` and `Value` plus the codec service.
- `Code`, `StorageKey`, and `StorageValue` remain compatibility traits. They should not be removed without a separate deprecation RFC.

This confines generic growth to the builder and avoids adding a codec type parameter to cache handles and user-facing entries.

## 11. Implementation Plan

### Phase 0: Establish Baselines and Terminology

- Add serialization and deserialization benchmarks for representative owned values.
- Record allocation count, copied bytes, decode latency, and retained buffer memory.
- Define metrics for codec mismatch, codec decode failure, and zero-copy-capable reads.
- Document the current serde-feature behavior as the compatibility baseline.

### Phase 1: Owner-backed Read Buffer

- Add an internal `OwnedBytes` abstraction over returned `IoB` owners.
- Convert the block read result into a shared owner before parsing key and value ranges.
- Continue calling `Code::decode` through a compatibility adapter so that external behavior remains unchanged.
- Add bounds, range, checksum, aliasing, and drop-order tests.

This stage validates ownership without changing the public codec API or disk format.

### Phase 2: Codec Service and Builder Integration

- Add `ValueCodec`, `EntryCodec`, `PairCodec`, and `LegacyCodeCodec`.
- Pass the selected erased codec through a generic `EngineBuildContext<K, V>`.
- Route size estimation, direct encoding, and decoding through the codec service.
- Add `with_codec` and `with_serde_codec`.
- Centralize compression ownership in the transform layer.
- Relax internal type bounds while preserving the default `Code` build path.

### Phase 3: Versioned and Aligned Envelope

- Add the block-engine v2 entry header and aligned key/value offsets.
- Persist codec ID, codec version, and transform metadata.
- Read both legacy and v2 entries; write v2 for explicitly configured codecs.
- Add strict and quiet recovery behavior for unknown codecs.
- Commit recovery fixtures so future layout compatibility is tested against fixed bytes.

### Phase 4: Zero-Copy Adapters

- Add a bytes-backed codec as the reference implementation.
- Add one archived-format adapter behind an optional feature after reviewing its validation and MSRV guarantees.
- Keep all archive-specific `unsafe` inside the adapter.
- Expose engine capability validation and actionable build errors.
- Document memory-retention behavior: a small value may retain a page-sized I/O allocation.

### Phase 5: Stabilization

- Compare owned serde, manual `Code`, bytes-backed, archived uncompressed, and archived compressed paths.
- Decide whether the implicit serde blanket implementation remains the default or becomes compatibility-only in a future major release.
- Stabilize public names only after the block engine and at least one additional engine use the same codec service.

## 12. Validation Plan

### Correctness Tests

- Round-trip serde values, manual `Code` values, byte-backed values, and archived values.
- Verify that the hash-collision path still compares the decoded key correctly.
- Verify that the owner remains alive after `Engine::load` returns and after promotion into the memory cache.
- Verify that cloned cache entries safely share the backing owner.
- Verify that wrapping a full aligned I/O owner preserves its pointer and that an unaligned `Bytes` slice fails alignment validation.
- Verify that every drop order releases the buffer exactly once.
- Return typed errors without panicking for malformed offsets, lengths, alignments, checksums, and archive roots.
- Recover legacy entries with `LegacyCodeCodec`.
- Distinguish codec mismatch from corruption in strict and quiet recovery modes.
- Run one common codec conformance suite against every supported engine.

### Compression Tests

- Verify that uncompressed byte-backed decode points into the read owner.
- Verify that compressed decode points into the decompression owner rather than the temporary compressed input.
- Reject a codec that forbids compression at build time.

### Benchmarks

- Serialization and deserialization throughput by payload size.
- Allocations and copied bytes per hit.
- End-to-end hit latency for psync and io_uring.
- Archive-validation cost.
- Retained memory per promoted value, including page amplification.
- Recovery throughput with legacy and v2 envelopes.

Recommended acceptance criteria for the zero-copy path:

- No payload-sized allocation after an uncompressed engine read.
- No payload byte copy between the returned I/O buffer and the decoded value.
- No regression beyond an agreed threshold on the existing serde path.
- Identical behavior across the common engine conformance suite.

## 13. Risks and Mitigations

### Retained I/O Memory

A small value may retain an entire aligned read buffer. Read exact entries where possible, expose retained-byte metrics, and consider copying values below a configurable threshold.

### Unsafe Archived Access

Alignment alone is insufficient; bytes must also be validated before typed access. Centralize validation and unsafe code in a dedicated adapter and fuzz it independently.

### Format Ambiguity

Feature flags must not silently change how persisted bytes are interpreted. Persist codec identity and require explicit mismatch handling.

### Dynamic-dispatch Overhead

Codec methods are called only a few times per key/value encode or decode, and serialization and I/O should dominate for normal entries. Measure the overhead before adding codec parameters to long-lived cache types.

### Ambiguous “Zero-copy” Scope

Document the guarantee as buffer ownership transfer and view construction. Do not claim allocation-free behavior when compression, validation scratch space, or engine limitations require allocation.

## 14. Alternatives Considered

### Return Borrowed Values from `load`

Rejected. This conflicts with `'static` asynchronous futures, object-safe engine dispatch, memory-cache promotion, and the current owned `Load<K, V, P>` API. A callback or lending API would create a second access model without naturally solving promotion.

### Let Each Engine Select Its Own Codec

Rejected. This duplicates application serialization logic, makes format behavior engine-dependent, and prevents a common conformance suite.

### Pre-serialize Before Enqueue

Rejected as a mandatory path. It adds allocation and memory pressure to every write and breaks the current direct-to-block-buffer behavior. Engines should invoke the codec service at their natural batching boundaries.

### Add Only `Code::decode_from_bytes`

Useful as an incremental prototype, but insufficient as the final abstraction. It leaves format selection global, retains coherence conflicts, and does not persist codec identity. Phase 1 may use an internal equivalent to validate ownership before introducing the complete codec service.

## 15. Open Decisions

- Select the first stable explicit serde format. Compatibility requires a bincode-v1 codec; new caches may select another independently versioned format.
- Decide whether key and value codecs use separate alignments and format IDs or whether `EntryCodec` exposes one combined identity.
- Set the maximum supported alignment and copy-below-size threshold for the block engine.
- Select the first archived-format integration after reviewing validation guarantees, MSRV, maintenance status, and compile-time cost.
- Decide whether quiet recovery retains unknown-codec entries for a later compatible reopen or reclaims them immediately.

These decisions do not block Phases 0 and 1, but they must be resolved before the v2 envelope is written by default.
