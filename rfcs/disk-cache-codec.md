# Disk Cache Codec 与 Zero-Copy Decode

- 状态：Draft
- 范围：`foyer-common`、`foyer-storage`、`foyer`
- 目标：分阶段、向后兼容地重构

## 1. 摘要

本 RFC 建议将应用层 key/value codec 从 disk cache engine 中分离，并在同一套、与 engine 无关的契约上提供两条路径：

1. 面向普通 owned Rust 类型的默认路径。开启 `serde` feature 并派生 `Serialize`、`Deserialize` 后即可使用；同时提供显式的 `with_serde_codec` builder 快捷方法，使持久化格式在配置中可见。
2. 面向性能敏感场景的高级路径。decoder 消费一段持有底层 owner 的 bytes，返回的 key/value 可以保留这段 bytes，并在其上暴露经过校验的视图，无需将字段复制出 I/O buffer。
3. 所有 storage engine 共用一个 codec service。engine 仍负责 placement、batch、I/O、recovery 和 eviction，但不再自行决定应用类型如何编码。

该方案与当前 I/O 模型兼容，因为读操作已经会归还 buffer 的所有权；它不要求让借用值逃逸出异步 `load` 调用。

## 2. 目标

- 保持简单场景足够简单：serde derive 加 Cargo feature 必须继续可用。
- 允许每个 cache instance 显式选择 codec，而不是通过全局 feature 和 blanket impl 隐式决定格式。
- 对支持 archived 或 byte-backed value 的格式，直接从 owned I/O buffer 解码。
- 保留直接序列化到 engine write buffer 的能力，不在 enqueue 前强制预序列化。
- 同一 codec 可用于 block engine 和未来的新 engine。
- 在持久化 entry 中记录足够的格式标识，使 recovery 能确定性地拒绝不兼容数据。
- 保持当前 `HybridCache<K, V, S>` 返回类型和 memory cache promotion 模型。

## 3. 非目标

- 从 `load` 返回借用临时 future 的 `&K`、`&V` 或类似类型。
- 让压缩读取完全无分配；解压必然需要另一段输出 buffer。
- 要求所有 codec 都支持 zero-copy。
- 让不同 storage engine 的完整磁盘布局互相兼容；codec identity 可复用，但 engine metadata 和 placement format 仍由 engine 管理。
- 在第一阶段移除现有 `Code` API。

## 4. 当前实现

### 4.1 用户侧编码接口

`foyer-common/src/code.rs` 定义了 `Code`、`StorageKey` 和 `StorageValue`：

- `Code::encode` 写入 `std::io::Write`；
- `Code::decode` 从 `std::io::Read` 读取并返回 owned `Self`；
- `Code::estimated_size` 为 admission 和 buffer size 提供估算；
- 开启 `serde` feature 后，所有 `Serialize + DeserializeOwned` 类型通过 blanket impl 使用 bincode。

因此，第一个需求实际上已经具备基础能力。当前限制是：格式由全局 feature 隐式选择，blanket impl 会通过 coherence 限制自定义实现，而 `Read` 无法把底层 buffer 的所有权转移给解码结果。

### 4.2 Storage 数据流

当前 block engine 同时负责持久化和应用类型序列化：

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

写路径已经比较高效：`EntrySerializer` 直接写入最终的 aligned block buffer，没有强制 staging allocation。读路径则会在构造 owned `K/V` 后丢弃 I/O buffer。

### 4.3 当前布局的关键约束

- `Engine::load` 返回 `'static` future，`Load::Entry` 持有 owned `K/V`，借用 read buffer 的结果无法安全逃逸。
- disk hit 可能被 promotion 到 memory cache，所以结果必须脱离 load future 独立存活。
- `IoEngine::read` 会归还调用方提交的 buffer，因此无需修改 psync 或 io_uring 的底层读模型即可保留 owner。
- 当前 entry header 为 36 bytes，value 紧随其后，无法为通用 archived format 保证足够的 payload alignment。
- compression 在 `Code::decode` 前执行，view 无法直接指向 compressed bytes。
- compression 配置同时出现在 store 和 block-engine 层；codec 重构应确定唯一 owner，避免配置漂移。

## 5. 核心决策

引入 owner-backed `OwnedBytes` 输入和 engine-neutral `EntryCodec<K, V>` service，同时保留 owned `Load<K, V, P>`。所谓 zero-copy value，是内部保留 `OwnedBytes` 的 owned value，而不是借用 load future 的 Rust reference。

统一 transform pipeline：

```text
K/V -> codec encode -> optional compression -> checksum -> engine placement
engine read -> checksum -> optional decompression -> codec decode -> K/V
```

engine 负责物理 entry envelope 和 placement；codec 只负责应用 payload 格式。compression 和 checksum 位于应用 codec 之外，使所有 codec 共享一致的损坏检测、错误分类和 metrics。

## 6. API 草案

以下名称暂定，核心是 ownership 和分层契约。

### 6.1 Owner-backed bytes

```rust
#[derive(Clone)]
pub struct OwnedBytes {
    inner: bytes::Bytes,
}

impl OwnedBytes {
    pub fn from_owner(owner: impl AsRef<[u8]> + Send + 'static) -> Self;
    pub fn slice(&self, range: impl RangeBounds<usize>) -> Self;
    pub fn alignment(&self) -> usize;
}

impl AsRef<[u8]> for OwnedBytes { /* ... */ }
```

具体实现可使用 `bytes::Bytes::from_owner`。owner 是 `IoEngine::read` 返回的 buffer，而不是复制出来的 `Vec<u8>`；slice 共享同一 owner，并允许任意 byte offset。

`OwnedBytes` 不应暴露 engine-specific buffer 类型。在 safety contract 稳定前，构造函数和 alignment 计算保持在 `foyer-storage` 内部。

### 6.2 Value codec

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

`decode` 消费 byte owner。普通 codec 反序列化出 owned `T` 后丢弃 `src`；zero-copy codec 则把 `src` 或其 slice 移入 `T`。

该 trait 保持 object-safe，使 builder 完成类型约束检查后可以 erase codec。`estimated_size` 返回 `Result`，避免 size error 被静默转换成零之类的错误行为。它只用于 admission 和 buffer reservation，不充当精确长度；实际长度由 bounded writer 统计。

### 6.3 Entry codec service

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

`PairCodec<KC, VC>` 组合两个 `ValueCodec`。独立的 entry-level trait 用于保证不同 engine 对 key/value framing、ordering 和 alignment 的处理一致。engine 根据 `layout` 先对齐 value start、调用 `encode_value`，再根据实际 value length 对齐 key start 并调用 `encode_key`；这样既保留 streaming compression 和 bounded direct write，也不会假设 size estimate 是精确值。

codec service 通过 `EngineBuildContext<K, V>` 以 `Arc<dyn EntryCodec<K, V>>` 传给 engine。所有 engine 使用该 service，不再直接调用 `K::decode`、`V::decode` 或特定 serde library。

### 6.4 简单 serde 路径

第一阶段保留当前零额外配置行为：

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

默认 `LegacyCodeCodec` 适配现有 `Code`，因此不要求 builder 调用。

同时为新代码提供显式快捷方法：

```rust
let cache = HybridCacheBuilder::new()
    .memory(memory_capacity)
    .storage()
    .with_serde_codec(SerdeFormat::BincodeV1)
    .with_engine_config(engine_config)
    .build()
    .await?;
```

对需要 recovery 的 cache，推荐显式形式，因为持久化格式由配置表达，而不再只是 Cargo feature 的隐式副作用。未来可以用独立 codec ID 增加其他 serde format，不会静默改变已有数据解释方式。

### 6.5 高级 zero-copy 路径

支持的模型是 owner-backed value。例如 bytes-oriented codec 返回包含 `OwnedBytes` 的 value；archived codec 返回包含 owner 和已校验 root offset 的 wrapper。

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

wrapper 不保存 self-referential Rust reference，而是保存 owner 和 offset，在访问时构造经过校验的 view。特定 archive library 所需的 `unsafe` 必须封装在小型、可独立审计的 adapter 中，不能散落到 engine。

高级配置仍使用同一个 builder hook：

```rust
let cache = HybridCacheBuilder::new()
    .memory(memory_capacity)
    .storage()
    .with_codec(PairCodec::new(key_codec, archived_value_codec))
    .with_engine_config(engine_config)
    .build()
    .await?;
```

在未压缩读取中，device 填充 I/O buffer 后可以做到 payload decode zero-copy。该保证不等同于 kernel I/O zero-copy，也不承诺任意内存对象图的序列化不发生复制。

## 7. 持久化 Entry Format

新 codec 路径需要 versioned entry envelope。完整 byte layout 仍可由 engine 决定，但 envelope 至少包含：

- entry magic 和 envelope version；
- codec ID 和 codec version；
- key offset 和 length；
- value offset 和 length；
- compression algorithm；
- checksum algorithm 和 checksum；
- payload alignment，或足以验证 alignment 的信息。

block-engine v2 layout 应使用 aligned payload start，不再把 payload 紧接在当前 36-byte header 后。固定 64-byte header 可以作为初始方案，但实现必须根据 codec 声明的 alignment 推导 offset，不能依赖常量碰巧对齐。codec alignment 必须是 2 的幂；除非 engine 显式承诺更强能力，否则不得超过 I/O page alignment。

Recovery 行为：

- envelope version 已知且 codec ID/version 匹配时才解码；
- unknown codec 或 incompatible version 在 strict recovery 下属于配置错误；
- quiet recovery 跳过不兼容 entry 并记录专用 metric，不能误报为 checksum corruption；
- migration window 内通过 `LegacyCodeCodec` 继续读取 legacy envelope；
- persistent cache 修改 codec 配置时，必须显式清空或执行迁移，绝不能用新格式解释旧 bytes。

## 8. Engine 兼容契约

一个 engine 支持 codec layer，需要满足：

1. 使用配置的 codec 完成 size estimation、encode 和 decode；
2. 直接编码到 engine-owned destination 或等价的 bounded writer，不要求每次 enqueue 预序列化；
3. read result 能无复制转换成 `OwnedBytes`；
4. 遵守 `EntryLayoutRequirements`，否则在 build 阶段返回 capability error；
5. 在 entry envelope 中存储并校验 codec format metadata；
6. 按 RFC 规定的统一顺序应用 checksum 和 compression。

增加显式 capabilities，在 build 阶段发现不兼容：

```rust
pub struct EngineCapabilities {
    pub owner_backed_reads: bool,
    pub max_payload_alignment: usize,
    pub direct_encode: bool,
}
```

当前 block engine、psync I/O engine 和 io_uring I/O engine 均可满足 owner-backed read，因为完成事件会归还提交的 buffer。`NoopEngine` 不持久化或解码数据，可视为支持所有 codec。

未来 engine 无需包含 serde、bincode 或 archive-specific 代码。无法保留 read buffer 的 engine 仍可支持 owned codec，但必须拒绝声明 `requires_owner_backed_input` 的 codec。

## 9. Compression 语义

必须精确定义 zero-copy capability：

- `Compression::None`：解码结果可以保留原始 I/O buffer 的 slice；
- compressed entry：解压到一段新的 aligned owner-backed buffer，codec 可以保留该 buffer 的 slice；这避免字段级复制，但不是 disk-buffer zero-copy；
- 如果 codec 的格式或性能契约要求直接访问持久化 bytes，它可以拒绝 compression。

compression 应只在 store transform layer 配置一次，再传给 engine。迁移过程中，engine-specific compression knob 应删除或退化为内部 alias。

## 10. Builder 与类型约束迁移

当前 `StorageKey: Key + Code` 和 `StorageValue: Value + Code` 强制所有 disk-cache 类型经过 `Code`，即使用户提供了显式 codec。要获得干净的高级 API，必须放宽这些约束。

只在 builder 上增加 codec type parameter，并在 build 后 erase：

```rust
pub struct StorageBuilder<K, V, S, P, C = LegacyCodeCodec> {
    codec: C,
    marker: PhantomData<(K, V, S, P)>,
}
```

- 默认 builder 仅在 `K: StorageKey`、`V: StorageValue` 时提供 `build`，保持 source compatibility；
- `with_codec` 改变 `C`，并在 build 时检查 `C: EntryCodec<K, V>`；
- 构建结果仍为 `Store<K, V, S, P>` 和 `HybridCache<K, V, S>`，codec 以 erased service 保存；
- engine 内部约束从 `StorageKey`/`StorageValue` 下沉为 `Key`/`Value` 加 codec service；
- `Code`、`StorageKey`、`StorageValue` 保留为兼容接口，除非另有 deprecation RFC，否则不移除。

这样 generic growth 只发生在 builder，不会给 cache handle 和用户侧 entry 增加 codec type parameter。

## 11. 实施计划

### Phase 0：建立基线和术语

- 为代表性的 owned value 增加 serialization/deserialization benchmark；
- 记录 allocation count、copied bytes、decode latency 和 retained buffer memory；
- 定义 codec mismatch、codec decode failure、zero-copy-capable read metrics；
- 将当前 serde feature 行为记录为兼容基线。

### Phase 1：Owner-backed read buffer

- 在内部增加基于 `IoB` owner 的 `OwnedBytes`；
- 解析 key/value range 前，先把 block read result 转成 shared owner；
- 暂时通过兼容 adapter 继续调用 `Code::decode`，保持外部行为不变；
- 增加 bounds、range、checksum、aliasing 和 drop-order 测试。

此阶段只验证 ownership，不修改公开 codec API 和磁盘格式。

### Phase 2：Codec service 与 builder 集成

- 增加 `ValueCodec`、`EntryCodec`、`PairCodec` 和 `LegacyCodeCodec`；
- 通过 generic `EngineBuildContext<K, V>` 传递 erased codec；
- 将 size estimation、direct encode 和 decode 统一路由到 codec service；
- 增加 `with_codec` 和 `with_serde_codec`；
- 将 compression 的所有权集中到 transform layer；
- 放宽内部类型约束，同时保留默认 `Code` build path。

### Phase 3：Versioned、aligned envelope

- 增加 block-engine v2 entry header 和 aligned key/value offset；
- 持久化 codec ID/version 和 transform metadata；
- 同时读取 legacy 和 v2 entry，显式配置 codec 时写 v2；
- 为 unknown codec 增加 strict/quiet recovery 行为；
- 提交 recovery fixture，以固定 bytes 测试未来 layout 兼容性。

### Phase 4：Zero-copy adapter

- 先增加 bytes-backed codec 作为 reference implementation；
- 审查 validation 和 MSRV 保证后，在可选 feature 下增加一个 archived-format adapter；
- 所有 archive-specific `unsafe` 只存在于 adapter；
- 暴露 engine capability validation 和明确的 build error；
- 文档说明 memory retention：一个很小的 value 也可能保留 page-sized I/O allocation。

### Phase 5：稳定化

- 对比 owned serde、manual `Code`、bytes-backed、archived uncompressed、archived compressed 路径；
- 决定隐式 serde blanket impl 长期保留为默认还是在未来 major release 仅作为兼容层；
- 至少让 block engine 和另一个 engine 共用同一 codec service 后，再稳定公开命名。

## 12. 验证计划

### 正确性测试

- serde value、manual `Code` value、byte-backed value 和 archived value round trip；
- hash collision 路径仍正确比较 decoded key；
- owner 在 `Engine::load` 返回后、promotion 到 memory cache 后仍存活；
- cloned cache entry 安全共享 backing owner；
- 任意 drop 顺序都只释放 buffer 一次；
- 非法 offset、length、alignment、checksum 和 archive root 返回 typed error，不 panic；
- `LegacyCodeCodec` 能 recovery legacy entry；
- strict/quiet recovery 下，codec mismatch 与 corruption 正确区分；
- 每个支持的 engine 通过同一套 codec conformance suite。

### Compression 测试

- uncompressed byte-backed decode 指向 read owner；
- compressed decode 指向 decompression owner，而不是临时 compressed input；
- 禁止 compression 的 codec 在 build 阶段失败。

### Benchmark

- 不同 payload size 的 serialization/deserialization throughput；
- 每次 hit 的 allocation 和 copied bytes；
- psync、io_uring 的 end-to-end hit latency；
- archive validation 开销；
- 每个 promoted value 的 retained memory，包括 page amplification；
- legacy 和 v2 envelope 的 recovery throughput。

zero-copy 路径建议验收标准：

- uncompressed engine read 完成后不再产生 payload-sized allocation；
- returned I/O buffer 与 decoded value 之间不复制 payload bytes；
- 现有 serde path 的性能回退不超过预先约定阈值；
- 所有 engine conformance test 行为一致。

## 13. 风险与缓解

### I/O memory 被长期保留

小 value 可能保留整个 aligned read buffer。应尽量按 entry 精确读取，增加 retained-byte metric，并考虑为小于阈值的数据主动 copy。

### Archived access 中的 unsafe

alignment 不是充分条件，typed access 前还必须验证 bytes。validation 和 unsafe 应集中在独立 adapter，并单独 fuzz。

### 格式歧义

feature flag 不能静默改变已有持久化 bytes 的解释方式。必须持久化 codec identity，并显式处理 mismatch。

### Dynamic dispatch 开销

每次 key/value encode 或 decode 只调用少数 codec 方法，正常 entry 中通常由序列化和 I/O 主导。应先测量，再考虑给长生命周期 cache type 增加 codec 泛型。

### “Zero-copy” 范围不清

文档只承诺 buffer ownership transfer 和 view construction。compression、validation scratch space 或 engine limitation 需要分配时，不得宣传 allocation-free。

## 14. 备选方案

### 从 `load` 返回 borrowed value

不采用。它与 `'static` async future、object-safe engine dispatch、memory-cache promotion 和当前 owned `Load<K, V, P>` API 冲突。callback/lending API 会制造第二套访问模型，仍不能自然解决 promotion。

### 每个 engine 自行选择 codec

不采用。这会重复应用序列化逻辑，使格式行为依赖 engine，也无法建立统一 conformance suite。

### enqueue 前预序列化

不作为强制路径。它会给每次写入增加 allocation 和 memory pressure，并破坏当前 direct-to-block-buffer 行为。engine 应在自身 batching boundary 调用 codec service。

### 只增加 `Code::decode_from_bytes`

可作为增量原型，但不适合作为最终抽象。它仍让格式由全局状态选择，保留 coherence conflict，也没有持久化 codec identity。Phase 1 可以使用内部等价接口验证 ownership，再引入完整 codec service。

## 15. 待决问题

- 选择第一个稳定的显式 serde format：兼容必须提供 bincode-v1 codec，新 cache 可以选择另一个独立 versioned format；
- key/value codec 使用独立 alignment、format ID，还是由 `EntryCodec` 暴露组合 identity；
- block engine 支持的最大 alignment 和 copy-below-size threshold；
- 审查 validation guarantee、MSRV、维护状态和编译成本后，选择首个 archived-format integration；
- quiet recovery 遇到 unknown-codec entry 时，是保留到未来兼容 reopen，还是立即 reclaim。

这些问题不阻塞 Phase 0 和 Phase 1，但在 v2 envelope 默认写入前必须确定。
