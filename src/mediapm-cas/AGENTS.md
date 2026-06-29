# CAS Agent Guide

> `mediapm-cas` — content-addressable blob store with delta-compression hints.
> `put(bytes)` → hash; `get(hash)` → bytes; `put_stream(reader)` → hash; `get_to_writer(hash, writer)` → ().
> Deduplicates identical content via Blake3-256. Foundation for deterministic workflows
> used by Conductor and MediaPM.

## 1. Hash

`Hash([u8; 32])` — blake3-256 content address.

- **Content-addressed**: `Hash::from_content(data)` = blake3(data). Same data → same hash.
- **Empty-content sentinel**: `Hash::empty()` = blake3(b"") (hash of empty content). API-level special-casing: `get`/`stat` return empty content immediately without backend lookups; `delete` is a no-op (indelible); constraints are always empty (set/get/patch succeed but no-op).
- **Wire format**: Multihash-encoded (`multihash` crate): `[code: varint(0x1e)][length: varint(0x20)][32-byte digest]`. `storage_bytes()` / `from_storage_bytes_with_len()` use `Multihash::wrap` / `Multihash::read`.
- **Serialization**: Derives `Serialize`/`Deserialize` (serde) and `Ord` (lexicographic on bytes).
- **Composite hash**: `Hash::composite(&[h1, h2, ...])` = blake3(h₁ ‖ h₂ ‖ …). Used by Conductor for StringList identity.

## 2. Public API

### 2.1 CasApi — seven-method contract

```rust
#[async_trait]
pub trait CasApi: Send + Sync {
    async fn put(&self, data: Bytes) -> Result<Hash, CasError>;
    async fn get(&self, hash: Hash) -> Result<Bytes, CasError>;
    async fn stat(&self, hash: Hash) -> Result<ObjectMeta, CasError>;
    async fn delete(&self, hash: Hash) -> Result<(), CasError>;
    async fn flush(&self) -> Result<u64, CasError>;
    async fn put_stream(&self, reader: impl AsyncRead + Send + Unpin) -> Result<Hash, CasError>;
    async fn get_to_writer(&self, hash: Hash, writer: &mut (dyn AsyncWrite + Send + Unpin)) -> Result<(), CasError>;
}
```

`put_stream` and `get_to_writer` have default impls that buffer through `put`/`get`.
Stores may override for zero-copy streaming large objects.

Guarantees (within a single async task):

- **Write-then-read**: After `put(data)` returns `Ok`, `get(hash)` returns the data.
- **Delete-then-read**: After `delete(hash)` returns `Ok`, `get(hash)`/`stat(hash)` return `NotFound`.
- **Idempotent**: `put(data)` twice with same data is no-op. `delete(hash)` twice is no-op.
- **Crash survival**: After any method returns `Ok`, the effect survives process death.

No standalone `exists()` method — use `stat()` or `get()`. Both return `NotFound` on miss, eliminating TOCTOU.

**put**: Hash data with `Hash::from_content`. Dispatches by size:

- **≤ WAL_INLINE_THRESHOLD (64 MiB)**: Append `WalEntry::Put` to WAL (inline data).
- **> WAL_INLINE_THRESHOLD**: Immediately materialize to blob, then append `WalEntry::PutLarge { content_len }` to WAL (external — already on disk).

Write-through vs write-back is compile-time configured via `B::SYNC_MATERIALIZE && I::SYNC_MATERIALIZE`: write-through materializes Blob + Metadata synchronously (immediate visibility); write-back defers to the WAL consumer. Only `Hash::from_content(b"")` produces `Hash::empty()` — normal non-empty content never collides with it.

**get**: Three-layer lookup (Metadata → Blob → WAL fallback) via `ComposedReadView`.
Delta reconstruction is transparent. Returns `CasError::NotFound` if absent.
Returns `CasError::TooLarge { hash, size, limit }` if the object exceeds `WAL_INLINE_THRESHOLD` — use `get_to_writer` instead for streaming large objects.

**stat**: Returns `ObjectMeta { len, encoding }`. Encoding is informational only (Full or Delta { base_hash }). Callers must NOT make decisions based on encoding.

**delete**: Append `WalEntry::Delete` to WAL. Physical removal is deferred to WAL consumer. Idempotent. Does not cascade.
Empty-content sentinel is a no-op — never appended to WAL.

**flush**: Materialize all committed WAL entries into the backing blob + metadata stores, making them visible to future `get`/`stat` calls without WAL fallback. Returns the number of WAL entries consumed. No-op for backends using write-through (e.g. `InMemoryCas`); write-back backends (e.g. `FileSystemCas`) materialize all deferred writes.

**put_stream**: Stream content from an `AsyncRead`, hash incrementally, store to blob, and commit to WAL. Overridden by stores to bypass the in-memory buffering in the default impl.

**get_to_writer**: Stream object content to an `AsyncWrite`.
Overridden by stores to bypass the in-memory buffering in the default impl.

### 2.2 CasError — TooLarge variant

```rust
pub enum CasError {
    // ... existing variants ...
    #[error("object {hash} too large for in-memory get: size={size} limit={limit}")]
    TooLarge { hash: Hash, size: u64, limit: u64 },
}
```

Returned by `get()` when the object exceeds `WAL_INLINE_THRESHOLD`.
The caller should fall back to `get_to_writer()` for streaming.

### 2.3 ConstraintApi — delta-compression hints

```rust
#[async_trait]
pub trait ConstraintApi: Send + Sync {
    async fn set_constraint(&self, target: Hash, bases: BTreeSet<Hash>) -> Result<(), CasError>;
    async fn get_constraint(&self, target: Hash) -> Result<BTreeSet<Hash>, CasError>;
    async fn patch_constraint(&self, target: Hash, patch: ConstraintPatch) -> Result<(), CasError>;
}
```

Constraints are **non-binding hints** — the system never blocks on completeness or accuracy.
Stored in a separate constraint map (in-memory `DashMap<Hash, BTreeSet<Hash>>`, rebuilt from WAL), independent of object metadata. `get_constraint` returns an empty set when no constraint exists (no `Option`). There is no `effective_bases` method — callers that need live filtering must compose `get_constraint` with their own `live` set intersection.

Empty-content sentinel exception: constraints on `Hash::empty()` are always empty. `set_constraint`, `get_constraint`, and `patch_constraint` all succeed but have no effect (always return or leave empty sets).

```rust
pub struct ConstraintPatch {
    pub add_bases: BTreeSet<Hash>,
    pub remove_bases: BTreeSet<Hash>,
    pub clear: bool,   // clear existing bases before applying adds/removes
}
```

### 2.4 CasMaintenanceApi — maintenance operations

```rust
#[async_trait]
pub trait CasMaintenanceApi: Send + Sync {
    async fn run_maintenance_cycle(&self) -> Result<OptimizeReport, CasError>;
    async fn prune_constraints(&self) -> Result<PruneReport, CasError>;
    async fn list_hashes(&self) -> Result<Vec<Hash>, CasError>;
}
```

- **run_maintenance_cycle**: Drain WAL consumer, run GC + optimizer.
- **prune_constraints**: Remove constraint entries whose target or bases no longer exist.
- **list_hashes**: Enumerate all hashes in the store.

### 2.5 Backend types

#### InMemoryCas — ephemeral store

```rust
let cas = new_in_memory_cas(); // or InMemoryCas::new()
cas.put(bytes).await?;
```

Newtype around `CasStore<InMemoryWal, InMemoryMetadataStore, InMemoryBlobStore>`.
Traits implemented via blanket `Deref` impls in `store.rs`.

#### FileSystemCas — persistent store

```rust
let cas = FileSystemCas::open(&Path::new("/path/to/store")).await?;
cas.put(bytes).await?;
```

Newtype around `CasStore<FileWal, FileSystemMetadataStore, FileSystemBlobStore>`.
WAL + blob store + metadata constraints persisted on disk; blob metadata in-memory (rebuilt from WAL on open). Metadata (entries + constraints) are saved per fan-out directory alongside blob files (see `FileSystemBlobStore` aux-file API).

Override of `FileSystemCas::object_path_for_hash` returns `Option<PathBuf>` (returns `None` for in-memory stores, `Some(path)` for filesystem stores).

#### ConfiguredCas — dispatch enum

```rust
pub enum ConfiguredCas {
    InMemory(InMemoryCas),
    FileSystem(FileSystemCas),
}
```

Implements `CasApi`, `CasMaintenanceApi`, `ConstraintApi` by forwarding to inner `cas.deref()` via the local `forward!` macro (defined in `config.rs`). Created via `CasConfig::from_locator_with_options()` + `CasConfig::open()`, or via the convenience `CasConfig::from_locator()` for quick setup with default options and empty integrity config.

### 2.6 Config types

| Type | Role |
|------|------|
| `CasConfig` | Single config object: `storage_locator` + `integrity` |
| `CasStorageLocator` | `InMemory` or `FileSystem { path }` |
| `CasIntegrityConfig` | `verify_on_read` strategies (`Default`: empty — no verification) |
| `CasLocatorParseOptions` | Controls whether plain paths are accepted (`Default`: `true`) |
| `VerifyTriggerStrategy` | `Always`, `Modified`, `Sample { denominator }`, `Stale { timeout }` |

`CasConfig` provides:

- `from_locator_with_options(locator, opts, integrity)` — full control.
- `from_locator(locator)` — convenience using [`default`](CasConfig::default) settings.

`CasIntegrityConfig` and `CasLocatorParseOptions` implement `Default` (defined in [`defaults`](crate::defaults)).

### 2.7 Defaults module

Singular location for all tunable constants in [`defaults`](crate::defaults):

| Constant | Value | Purpose |
|----------|-------|---------|
| `WAL_INLINE_THRESHOLD` | 64 MiB | Max object size inlined in WAL. Beyond this → `PutLarge` + external blob. |
| `DELTA_THRESHOLD` | 16 MiB | Max object size eligible for delta compression. Larger objects stored as Full. |
| `CACHE_MAX_FRACTION_OF_TOTAL_SIZE` | 0.10 | Fraction of total store consumed by bg_engine cache at most. |
| `CACHE_TTL` | 60 s | TTL for cached entries in bg_engine. |
| `WAL_MAX_SEGMENT_SIZE` | 64 MiB | Max bytes per FileWal segment before rotation. |
| `OBJECT_STREAM_BUFFER_SIZE` | 262144 (256 KiB) | Buffer size for streaming blob read/write. |

## 3. Crate structure

```text
src/mediapm-cas/src/
├── lib.rs               — crate root, re-exports
├── api.rs               — CasApi (+ flush), CasApiStreaming, ConstraintApi, CasMaintenanceApi, report types
├── hash.rs              — Hash type (blake3-256, multihash wire format)
├── error.rs             — CasError enum
├── config.rs            — CasConfig, ConfiguredCas, CasStorageLocator, integrity settings
├── main.rs              — CLI binary (feature-gated)
├── cli.rs               — CLI subcommands + run_from_passthrough_args (feature-gated)

├── delta/
│   ├── mod.rs           — module root, versioning boundary guard
│   ├── patch.rs         — DeltaPatch (VCDIFF via oxidelta) + apply_delta_chain
│   ├── object.rs        — DeltaState + StoredObject (version-agnostic)
│   └── versions/        — V1/V2/V3 delta envelope wire formats (mod.rs = canonical API)
└── storage/
    ├── mod.rs             — module root
    ├── store.rs           — CasStore<J,I,B> (composed handle, implements all traits)
    ├── wal/               — Wal trait + InMemoryWal + FileWal + entry types + versions
    │   ├── mod.rs         — Wal trait + entry types + re-exports
    │   ├── mem_wal.rs     — InMemoryWal (VecDeque, ephemeral)
    │   ├── file_wal.rs    — FileWal (segmented file-backed WAL)
    │   └── versions/      — on-disk format V1+
    │       ├── mod.rs
    │       ├── v1.rs     — V1 format (decode only)
    │       └── v2.rs     — V2 format with PutLarge entry type
    ├── blob_store/        — BlobStore trait + FileSystemBlobStore + InMemoryBlobStore + versioned path layout
    │   ├── mod.rs         — Blob trait + re-exports
    │   ├── mem.rs         — InMemoryBlobStore (DashMap, ephemeral)
    │   ├── fs.rs          — FileSystemBlobStore (atomic hash-derived layout)
    │   └── versions/      — path layout versions V1+
    │       ├── mod.rs     — version dispatch
    │       └── v1.rs      — V1 layout: v1/blake3/ab/cd/<hex>
    ├── metadata_store/    — MetadataStore trait + InMemoryMetadataStore + FileSystemMetadataStore
    │   ├── mod.rs         — trait + MetadataEntry + re-exports
    │   ├── mem.rs         — InMemoryMetadataStore (DashMap, separate constraint map)
    │   ├── fs.rs          — FileSystemMetadataStore (persistent snapshot via JSON)
    │   └── versions/      — V1 persistence format (versioned JSON metadata file)
    ├── read_view.rs       — ComposedReadView (3-layer lookup: Metadata → Blob → WAL)
    ├── pending_ops.rs     — PendingOps (in-flight read dedup helper)
    ├── bg_engine.rs       — BackgroundEngine (WAL consumer → Blob + Metadata, maintenance)
    ├── in_memory.rs       — InMemoryCas wrapper
    └── file_system.rs     — FileSystemCas wrapper + open()
```

## 4. Data flow

```text
put(data) → Hash(data)
  │ ≤ WAL_INLINE_THRESHOLD → Wal.append(Put{hash, data})
  │ > WAL_INLINE_THRESHOLD → Blob.write(hash, data) → Wal.append(PutLarge{hash, content_len})
  ↓
WAL consumer (bg_engine) → Blob.write(hash) + Metadata.put(hash) → checkpoint
  (PutLarge: already materialized, just advance checkpoint + add metadata)
                                                                    ↓
get(hash) → ReadView: Metadata → Blob.read/read_delta → WAL fallback (tombstone check)
  ↓ (if size > WAL_INLINE_THRESHOLD)
  returns TooLarge — caller should use get_to_writer() for streaming
                                                                    ↓
get_to_writer(hash, writer) → ReadView: Metadata → Blob.read_to_writer
  (streams directly to writer, bypasses in-memory Bytes buffer)
                                                                    ↓
put_stream(reader) → hash incrementally → Blob.write_stream(hash, reader)
  → Wal.append(PutLarge{hash, content_len})
                                                                    ↓
delete(hash) → Wal.append(Delete{hash})
                                                                    ↓
WAL consumer → re-materialize dependents → Blob.delete(hash) + Metadata.delete(hash)
```

## 5. Internals

### 5.1 WAL

The only crash-safe commitment point. Metadata and Blob are derived — rebuildable by WAL replay.

**Entry types**: `Put { hash, data }`, `PutLarge { hash, content_len }`, `Delete { hash }`, `Constraint { target, bases }`. Only single-entry `append` is exposed. `PutLarge` represents objects already materialized to blob (payload too large to inline in WAL).

**PendingState**: `Present(Bytes)` / `PresentExternal { content_len }` / `Tombstone` / `NotPresent`.
Used by ReadView's L3 WAL fallback. `PresentExternal` indicates the blob is on filesystem but not yet tracked in Metadata (WAL not yet consumed).

**WAL wire format**: V2 binary (active writer). V1 preserved for backward-compatible decoding of legacy segments. V2 adds the `PutLarge` entry type with `content_len`.

**WAL Consumer** (`BackgroundEngine::run_wal_consumer`): Replays WAL entries from checkpoint position, materializing to Blob + Metadata. After processing, position is persisted atomically. Idempotent.

### 5.2 Blob

Pluggable payload backend.

- `InMemoryBlobStore`: `DashMap<Hash, (Bytes, ObjectEncoding)>`. Ignores Full/Delta distinction.
- `FileSystemBlobStore`: Hash-derived paths `<root>/v1/blake3/ab/cd/<remaining>` (full) or `<remaining>.diff` (delta). Atomic write via temp+rename. Hash verification on read. `delete` silently ignores `NotFound` errors (missing files are treated as already-deleted). `delete_encoding(hash, encoding)` removes a specific encoding variant without affecting others. `materialized_path(&self, hash) -> Option<PathBuf>` returns the on-disk path for `hash` (overrides the trait default of `None`).

**`materialized_path`**: non-async method on `BlobStore` trait (default `None`). Returns `Some(path)` for filesystem-backed stores, `None` for in-memory. Used by `FileSystemCas::object_path_for_hash`.

### 5.3 Metadata

Object metadata index. Both implementations share the same trait.

- **`InMemoryMetadataStore`**: `Arc<DashMap<Hash, MetadataEntry>>` for payload metadata (`MetadataEntry { len, encoding }`). Constraint data is stored in a separate map: `constraints: Arc<DashMap<Hash, BTreeSet<Hash>>>`.
- **`FileSystemMetadataStore`**: Wraps `InMemoryMetadataStore` with persistent snapshot. On mutation (`put`/`delete`/`set_constraint`/`prune_targets`), writes full snapshot (entries + constraints) to a V1 JSON file atomically (temp+rename). Flushes are batched via an `AtomicBool` dirty flag: concurrent mutations coalesce into a single write, avoiding redundant I/O. On `rebuild_from_wal`, replays WAL then overlays persisted state.

**Constraint separation**: Object metadata (`put`/`get`/`delete`) and constraint data (`set_constraint`/`get_constraint`) are stored in independent maps. Put entries from WAL replay do not touch the constraint map; only `Constraint` WAL entries populate it. This removes `Option` from constraint representation.

**Versioned persistence** (`storage/metadata_store/versions/`): V1 JSON format:

```json
{ "version": 1, "entries": { "hash_hex": { "len": 123, "encoding": "Full" } }, "constraints": { "target_hex": { "bases": ["base1_hex"] } } }
```

`entries` field uses `#[serde(default)]` for compatibility with snapshots without entry persistence.

Async wrappers use `tokio::task::spawn_blocking`; sync `load()`/`save()` functions are shared by both implementations.

### 5.4 ReadView

Three-layer lookup for get/stat, plus streaming path.

1. **Metadata**. Check entry encoding. If `Full` → Blob.read(). If `Delta { base_hash }` → iterative walk.
2. **Blob**. Read payload bytes (full or delta envelope). Walk delta chains via `read_delta`/`read`.
3. **WAL fallback**. Pending entries not yet materialized. Respects tombstones.

**get_to_writer**: Streaming path. For Full objects, reads blob directly to writer via `BlobStore::read_to_writer` (chunked copy). For Delta objects, reconstructs in memory then writes. Object-safe (`&mut (dyn AsyncWrite + Send + Unpin)`).

**TooLarge enforcement**: If the resolved object exceeds `WAL_INLINE_THRESHOLD`, `get()` returns `CasError::TooLarge`. The caller should use `get_to_writer()` for streaming. Delta chain resolution enforces `MAX_DELTA_CHAIN_DEPTH = 5`; beyond that, `TooLarge` is returned to prevent unbounded recursion.

**Concurrent read dedup**: First caller inserts `PendingResult` with `Notify`; subsequent callers wait for shared result.

**Delta reconstruction**: Metadata → [`resolve_delta_chain`](crate::storage::read_view) → Blob.read_delta(hash) → walk base chain via Metadata → Blob.read(base) → `DeltaPatch::apply(base_bytes, vcdiff)`.
If base not found → `CasError::CorruptObject`.

### 5.5 Delta Codec

- **DeltaPatch**: VCDIFF wrapper via `oxidelta`. `diff(base, target)` → patch; `apply(patch, base)` → reconstructed target.
- Two functions for chain resolution:
  - `apply_delta_chain` in `delta/patch.rs`: Pure `pub(crate)` function that takes base bytes, collected delta envelopes, applies VCDIFF patches innermost-first, returns fully reconstructed payload. Used by `delta_resolve::resolve_delta_chain`.
- `resolve_delta_chain` in [`storage/read_view.rs`](crate::storage::read_view): `pub(super)` async walker that reads delta blobs from Blob and builds the chain, then calls `apply_delta_chain`. Shared by `ComposedReadView::fetch_inner` and `BgEngine::read_full_bytes`.
- **StoredObject**: Struct wrapping `DeltaState`. Encode/decode to/from versioned envelopes.
- **Versioned envelopes**: V1/V2 (read-only, magic `b"MDCASD"`), V3+ (magic `b"CASDLT"`).

**Versioning boundary guard**: Code outside `delta/versions/` must interact with versioned envelopes only through `delta::versions` module APIs (`mod.rs`), never via `delta::versions::vX` imports.

### 5.6 Background Engine

Drives WAL consumer and maintenance pass. GC never deletes objects — only prunes constraint metadata. Objects are removed solely by `CasApi::delete` materialized through the WAL consumer.

**Bounded cache**: A size-bounded LRU-like cache holds recently read objects.
Maximum size = `CACHE_MAX_FRACTION_OF_TOTAL_SIZE × total_store_size_on_disk`.
When over budget, the oldest half of cached entries are evicted.

**Delta threshold enforcement**: Objects > `DELTA_THRESHOLD` (16 MiB) are skipped by the optimizer — they are stored as Full encoding only, never delta-compressed.

**WAL consumer guarantees**: After a `run_wal_consumer` cycle completes, all entries up to the checkpoint position are materialized in Blob + Metadata. After materialization, the checkpoint is advanced and `WAL::trim(checkpoint)` is called. The checkpoint starts from `consumed_position()` (passed as `start_pos` to `CasStore::new` and `BackgroundEngine::new`), not from `WalPosition::ZERO`, so already-consumed entries are skipped on restart. This means:

- Consumed WAL entries are **physically removed** from the WAL:
  - `FileWal::trim()` deletes segment files whose `last_pos <= checkpoint` and prunes pending HashMap entries with `pos <= checkpoint`.
  - `InMemoryWal::trim()` pops entries from the VecDeque front.
  - After trim, the only authoritative copies are in Blob + Metadata.
- L3 WAL fallback in ReadView is only exercised for entries appended after the last cycle (transient, before consumer materializes).
- The tombstone check (always performed on every `get()` even on Metadata hit) is O(1) for FileWal (HashMap lookup) and cheap for InMemoryWal (small pending set after trimming).

**Crash recovery**: On restart, `rebuild_from_wal()` replays all entries from the WAL (starting from `WalPosition::ZERO`), rebuilding Metadata + constraint map. The WAL contains the complete history since the last trim — the last checkpoint always points at or before the oldest surviving segment.

## 6. Delete semantics — no dangling deltas

When the WAL consumer processes `Delete { hash }`:

1. **Scan for dependents**: Find Metadata entries where `encoding == Delta { base_hash: hash }`.
2. **Re-materialize each**: Read delta blob from Blob, decode V3 envelope, fetch base (still available in Blob), apply VCDIFF, store result as Full in Blob + Metadata.
3. **Physically remove**: `Blob.delete(hash)` + `Metadata.delete(hash)`.

The WAL consumer doesn't advance the checkpoint until re-materialization is complete.

**Does not cascade**: Deleting B has zero effect on other hashes. Even if A is delta-compressed against B, A's bytes live under A's content hash.

## 7. Wrapper pattern

`InMemoryCas` and `FileSystemCas` are newtype wrappers around `CasStore<...>` with `Deref`:

```rust
pub struct InMemoryCas(pub(crate) CasStore<InMemoryWal, InMemoryMetadataStore, InMemoryBlobStore>);

impl std::ops::Deref for InMemoryCas { /* → inner CasStore */ }
```

All three CAS traits (`CasApi`, `CasMaintenanceApi`, `ConstraintApi`) are implemented via blanket impls in `store.rs` for any `T: Deref<Target = CasStore<J, M, B>>`. `ConfiguredCas` uses the local `forward!` macro (defined in `config.rs`) to delegate trait methods to the inner variant via `cas.deref()`.

## 8. Invariants & edge cases

### 8.1 Content identity

- Same bytes → same hash. Deterministic. `Hash::empty()` (`blake3(b"")`) is a well-known sentinel; only empty content produces it.

### 8.2 Empty-content sentinel

- `Hash::empty()` = `blake3(b"")` — always present, indelible.
- All operations use normal code paths, except `delete(empty)` which is a no-op (never appended to WAL).

### 8.3 Crash safety

- WAL is the single crash-safe commitment point. All operations append before acknowledging.
- Metadata and Blob are derived — rebuilt by WAL replay.
- PutLarge entries on restart: during WAL replay the WAL consumer skips materialization (blob already on disk) and just inserts metadata.

### 8.4 No TOCTOU

No standalone `exists()` method. Use `get()` or `stat()` — both return `NotFound` on miss.

### 8.5 Delta chain integrity

- Iterative `get(base_hash)` for reconstruction goes through Metadata → Blob walk.
- If base_hash not found → `CorruptObject`.
- Cyclic references prevented by inline `HashSet` visitation during chain traversal.
- Max delta chain depth: 5 hops. Beyond that, `TooLarge` is returned (caller should use `get_to_writer`).

### 8.6 Object size limits

- **WAL_INLINE_THRESHOLD** (64 MiB): `get()` returns `TooLarge` for larger objects. Use `get_to_writer()` for streaming retrieval.
- **DELTA_THRESHOLD** (16 MiB): Objects above this size are never delta-compressed (stored as Full only).
- **TooLarge error**: Contains `hash`, `size`, and `limit` fields for diagnostics.

### 8.7 Constraint invariants

- `get_constraint` returns `BTreeSet<Hash>` — empty set means no constraint (no `Option`).
- `prune_constraints()` removes entries whose target or bases no longer exist.
- No `effective_bases` method — callers compute live intersection themselves.
- Self-referencing constraint (target == base) is rejected at set/patch time.
- Constraint data is stored independently from object metadata (separate `DashMap`). Put/delete/stat operations do not affect constraint state.

### 8.8 Write-through vs write-back compile-time policy

`CasStore::put()` uses compile-time dispatch via associated consts (`BlobStore::SYNC_MATERIALIZE`, `MetadataStore::SYNC_MATERIALIZE`). Effective policy: `B::SYNC_MATERIALIZE && M::SYNC_MATERIALIZE`.

| Backend triplet | Blob | Metadata | Effective |
|-----------------|------|----------|-----------|
| `InMemoryCas` | `InMemoryBlobStore` (true) | `InMemoryMetadataStore` (true) | write-through |
| `FileSystemCas` | `FileSystemBlobStore` (false) | `FileSystemMetadataStore` (false) | write-back |

**`delete()`** is always write-back regardless of `SYNC_MATERIALIZE` — physical removal is deferred to the WAL consumer for correct delta-dependent re-materialization.

### 8.9 Codec versioning

- V1/V2 are read-only legacy. Writers always emit V3.
- New versions go in `delta/versions/vN.rs`. Keep the `DO NOT REMOVE` boundary guard.

## 9. Cross-crate integration

- **Conductor**: CAS hash used for state blob identity, external data keys, StringList input hashing. Conductor uses `C: CasApi + CasMaintenanceApi + ConstraintApi` bounds.
- **MediaPM**: Lock records keyed by `(media_id, variant)` → CAS hash. Materializer reads from CAS.
- **Constraints**: Set by conductor as optimization hints. CAS owns storage and enforcement.

**Contract**: Callers may call CAS concurrently (thread-safe). CAS doesn't reference Conductor/MediaPM types. Failures propagate as-is.

## 10. Build & test

- `cargo test -p mediapm-cas` — unit + integration + doctest.
- `cargo clippy -p mediapm-cas` — lint.
- `cargo build -p mediapm-cas` — build with default features (cli).
- `cargo build -p mediapm-cas --no-default-features` — minimal (no CLI binary).
- Tests use `new_in_memory_cas()` — no filesystem dependencies.
- Streaming/large-file tests (`tests/int/streaming_large.rs`) verify:
  - `put_stream` propagates `content_len` through metadata.
  - `put_stream` + `get_to_writer` round-trip with 1 MiB payload.
  - `get()` returns `CasError::TooLarge` for objects > `WAL_INLINE_THRESHOLD`.
  - `get_to_writer()` succeeds for objects > `WAL_INLINE_THRESHOLD`.
  - Both `InMemoryCas` and `FileSystemCas` are exercised.
