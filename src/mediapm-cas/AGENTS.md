# CAS Agent Guide

> `mediapm-cas` — content-addressable blob store with delta-compression hints.
> `put(bytes)` → hash; `get(hash)` → bytes. Deduplicates identical content via
> Blake3-256. Foundation for deterministic workflows used by Conductor and MediaPM.

## 1. Hash

`Hash([u8; 32])` — blake3-256 content address.

- **Content-addressed**: `Hash::from_content(data)` = blake3(data). Same data → same hash.
- **Zero sentinel**: `Hash::zero()` = `[0u8; 32]`. Never stored: put is no-op, get/stat always succeed (empty data), delete is no-op.
- **Wire format**: Multihash-encoded (`multihash` crate): `[code: varint(0x1e)][length: varint(0x20)][32-byte digest]`.
  `storage_bytes()` / `from_storage_bytes_with_len()` use `Multihash::wrap` / `Multihash::read`.
- **Serialization**: Derives `Serialize`/`Deserialize` (serde) and `Ord` (lexicographic on bytes).
- **Composite hash**: `Hash::composite(&[h1, h2, ...])` = blake3(h₁ ‖ h₂ ‖ …). Used by Conductor for StringList identity.

## 2. Public API

### 2.1 CasApi — four-method contract

```rust
#[async_trait]
pub trait CasApi: Send + Sync {
    async fn put(&self, data: Bytes) -> Result<Hash, CasError>;
    async fn get(&self, hash: Hash) -> Result<Bytes, CasError>;
    async fn stat(&self, hash: Hash) -> Result<ObjectMeta, CasError>;
    async fn delete(&self, hash: Hash) -> Result<(), CasError>;
}
```

Guarantees (within a single async task):

- **Write-then-read**: After `put(data)` returns `Ok`, `get(hash)` returns the data.
- **Delete-then-read**: After `delete(hash)` returns `Ok`, `get(hash)`/`stat(hash)` return `NotFound`.
- **Idempotent**: `put(data)` twice with same data is no-op. `delete(hash)` twice is no-op.
- **Crash survival**: After any method returns `Ok`, the effect survives process death.

No standalone `exists()` method — use `stat()` or `get()`. Both return `NotFound` on miss,
eliminating TOCTOU.

**put**: Hash data with `Hash::from_content`, append `WalEntry::Put` to WAL, hint cache.
Zero hash returns immediately — nothing stored.

**get**: Two-layer lookup (ObjectIndex → WAL fallback).
Delta reconstruction is transparent. Returns `CasError::NotFound` if absent.

**stat**: Returns `ObjectMeta { len, encoding }`. Encoding is informational only
(Full or Delta { base_hash }). Callers must NOT make decisions based on encoding.

**delete**: Append `WalEntry::Delete` to WAL. Physical removal is
deferred to WAL consumer. Idempotent. Does not cascade.

### 2.2 CasApiStreaming — blanket-impl streaming extension

```rust
#[async_trait]
pub trait CasApiStreaming: CasApi {
    async fn put_stream<R: AsyncRead + Send + Unpin>(&self, reader: R) -> Result<Hash, CasError>;
    async fn get_stream<W: AsyncWrite + Send + Unpin>(&self, hash: Hash, writer: W) -> Result<(), CasError>;
}
```

Blanket impl over `CasApi` (buffers through bytes). Override for zero-copy paths.

### 2.3 ConstraintApi — delta-compression hints

```rust
#[async_trait]
pub trait ConstraintApi: Send + Sync {
    async fn set_constraint(&self, target: Hash, bases: BTreeSet<Hash>) -> Result<(), CasError>;
    async fn get_constraint(&self, target: Hash) -> Result<Option<BTreeSet<Hash>>, CasError>;
    async fn patch_constraint(&self, target: Hash, patch: ConstraintPatch) -> Result<(), CasError>;
    async fn effective_bases(&self, target: Hash, live: &HashSet<Hash>) -> Result<BTreeSet<Hash>, CasError>;
}
```

Constraints are **non-binding hints** — the system never blocks on completeness or accuracy.
`effective_bases = stored_bases ∩ live`. Stored in MetadataIndex (in-memory `DashMap`, rebuilt from WAL).

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
    async fn optimize_once(&self) -> Result<OptimizeReport, CasError>;
    async fn prune_constraints(&self) -> Result<PruneReport, CasError>;

    async fn list_all_hashes(&self) -> Result<Vec<Hash>, CasError>;
    async fn repair_index(&self) -> Result<IndexRepairReport, CasError>;
}
```

- **optimize_once**: Drain WAL consumer, run GC + optimizer.
- **prune_constraints**: Remove constraint entries whose target or bases no longer exist.
- **repair_index**: Rebuild index from storage contents.

### 2.5 Backend types

#### InMemoryCas — ephemeral store

```rust
let cas = new_in_memory_cas(); // or InMemoryCas::new()
cas.put(bytes).await?;
```

Wraps `CasStore<InMemoryWal, InMemoryObjectIndex, InMemoryMetadataIndex>`.
Implements `CasApi`, `CasMaintenanceApi`, `ConstraintApi` via `impl_cas_wrapper_traits!` macro.
`Deref` target is the inner `CasStore`.

#### FileSystemCas — persistent store

```rust
let cas = FileSystemCas::open(&Path::new("/path/to/store")).await?;
cas.put(bytes).await?;
```

Wraps `CasStore<FileWal, InMemoryObjectIndex, InMemoryMetadataIndex>`.
Same trait impl pattern as `InMemoryCas`. WAL is persisted on disk; payload and
metadata indexes are in-memory (rebuilt from WAL on open).

#### ConfiguredCas — dispatch enum

```rust
pub enum ConfiguredCas {
    InMemory(InMemoryCas),
    FileSystem(FileSystemCas),
}
```

Implements `CasApi`, `CasMaintenanceApi`, `ConstraintApi` by forwarding to inner `cas.0`.
Created via `CasConfig::from_locator_with_options()` + `CasConfig::open()`.

### 2.6 Config types

| Type | Role |
|------|------|
| `CasConfig` | Single config object: `storage_locator` + `integrity` |
| `CasStorageLocator` | `InMemory` or `FileSystem { path }` |
| `CasIntegrityConfig` | `verify_on_read` strategies + `reconstructed_bytes_cache_ttl` |
| `CasLocatorParseOptions` | Controls whether plain paths are accepted |
| `VerifyTriggerStrategy` | `Always`, `Modified`, `Sample { denominator }`, `Stale { timeout }` |

### 2.7 Report types

| Type                | Fields                                 |
| ------------------- | -------------------------------------- |
| `ObjectMeta`        | `len: u64`, `encoding: ObjectEncoding` |
| `ObjectEncoding`    | `Full` or `Delta { base_hash }`        |
| `OptimizeReport`    | (opaque stats from optimization pass)  |
| `PruneReport`       | (deleted constraint entries)           |
| `IndexRepairReport` | (repair stats)                         |

## 3. Crate structure

```text
src/mediapm-cas/src/
├── lib.rs               — crate root, re-exports
├── api.rs               — CasApi, CasApiStreaming, ConstraintApi, CasMaintenanceApi, report types
├── hash.rs              — Hash type (blake3-256, multihash wire format)
├── error.rs             — CasError enum
├── config.rs            — CasConfig, ConfiguredCas, CasStorageLocator, integrity settings
├── main.rs              — CLI binary (feature-gated)
├── cli.rs               — CLI subcommands + run_from_passthrough_args (feature-gated)

├── delta/
│   ├── mod.rs           — module root, versioning boundary guard
│   ├── delta.rs         — DeltaPatch (VCDIFF via oxidelta) + resolve_delta_chain
│   ├── object.rs        — DeltaState + StoredObject (version-agnostic)
│   └── versions/        — V1/V2/V3 delta envelope wire formats (mod.rs = canonical API)
└── storage/
    ├── mod.rs           — module root + #[macro_use] macros
    ├── macros.rs        — impl_cas_wrapper_traits!($ty) macro
    ├── store.rs         — CasStore<J,S,M> (composed handle, implements all traits)
    ├── wal/             — Wal trait + InMemoryWal + FileWal + entry types + versions
    │   ├── mod.rs       — trait definitions + InMemoryWal
    │   ├── file_wal.rs  — FileWal (segmented file-backed WAL)
    │   └── versions/    — on-disk format V1+
    │       ├── mod.rs
    │       └── v1.rs
    ├── object_index.rs  — ObjectIndex trait + InMemoryObjectIndex
    ├── metadata_index.rs— MetadataIndex trait + InMemoryMetadataIndex
    ├── read_view.rs     — ComposedReadView (2-layer lookup: index → WAL)
    ├── bg_engine.rs     — BackgroundEngine (WAL consumer + maintenance orchestrator)
    ├── in_memory.rs     — InMemoryCas wrapper + new_in_memory_cas()
    └── file_system.rs   — FileSystemCas wrapper + open()
```

## 4. Data flow

```text
put(data) → Hash(data) → Wal.append(Put{hash, data})
                                                                    ↓
WAL consumer (bg_engine) → ObjectIndex.put(hash, data) → checkpoint
                                                                    ↓
get(hash) → ReadView: ObjectIndex → WAL fallback
                                                                    ↓
delete(hash) → Wal.append(Delete{hash})
                                                                    ↓
WAL consumer → re-materialize dependents → ObjectIndex.delete(hash)
```

## 5. Internals

### 5.1 WAL

The only crash-safe commitment point. ObjectIndex and MetadataIndex are derived —
rebuildable by WAL replay.

**Entry types**: `Put { hash, data }`, `Delete { hash }`, `Constraint { target, bases }`.

**PendingState**: `Present(Bytes)` / `Tombstone` / `NotPresent`. Used by ReadView's L3 WAL fallback.

**WAL Consumer** (`BackgroundEngine::run_wal_consumer`): Replays WAL entries from
checkpoint position, materializing to ObjectIndex/MetadataIndex. After processing,
position is persisted atomically. Idempotent.

### 5.2 ObjectIndex

Pluggable payload backend. `InMemoryObjectIndex` uses `DashMap<Hash, (Bytes, ObjectEncoding)>`.
Stores raw bytes for Full encoding or complete V3 delta envelope for Delta encoding.

### 5.3 ReadView

Two-layer lookup for get/stat:

1. **ObjectIndex**. If delta-encoded, reconstruct (decode V3 envelope → recursive get(base_hash) → apply VCDIFF).
2. **WAL fallback**. Pending entries not yet materialized. Respects tombstones.

**Concurrent read dedup**: First caller inserts `PendingResult` with `Notify`; subsequent
callers wait for shared result.

**Delta reconstruction**: Recursive `get(base_hash)` through full 2-layer lookup →
`DeltaPatch::apply(base_bytes, vcdiff)`. If base_hash not found → `CasError::CorruptObject`.

### 5.4 Delta Codec

- **DeltaPatch**: VCDIFF wrapper via `oxidelta`. `diff(base, target)` → patch; `apply(patch, base)` → reconstructed target.
- **resolve_delta_chain**: Shared `pub(crate)` function in `delta/delta.rs`. Takes base bytes + collected delta envelopes, applies deltas inner-to-outer, returns fully reconstructed payload. Used by both `read_view.rs` and `bg_engine.rs`.
- **StoredObject**: `Full { payload }` or `Delta { state }`. Encode/decode to/from versioned envelopes.
- **Versioned envelopes**: V1/V2 (read-only legacy, magic `b"MDCASD"`), V3+ (current writer, magic `b"CASDLT"`).

**Versioning boundary guard**: Code outside `delta/versions/` must interact with versioned
envelopes only through `delta::versions` module APIs (`mod.rs`), never via `delta::versions::vX` imports.

### 5.5 Background Engine

Drives WAL consumer and maintenance pass. GC never deletes objects — only prunes constraint
metadata. Objects are removed solely by `CasApi::delete` materialized through the WAL consumer.

## 6. Delete semantics — no dangling deltas

When the WAL consumer processes `Delete { hash }`:

1. **Scan for dependents**: Find ObjectIndex entries where `encoding == Delta { base_hash: hash }`.
2. **Re-materialize each**: Fetch delta bytes, decode V3 envelope, fetch base (still available),
   apply VCDIFF, store as Full, hint cache.
3. **Physically remove**: `ObjectIndex.delete(hash)`.

The WAL consumer doesn't advance the checkpoint until re-materialization is complete.

**Does not cascade**: Deleting B has zero effect on other hashes. Even if A is delta-compressed
against B, A's bytes live under A's content hash.

## 7. Wrapper pattern

`InMemoryCas` and `FileSystemCas` are newtype wrappers around `CasStore<...>`:

```rust
pub struct InMemoryCas(pub(crate) CasStore<InMemoryWal, InMemoryObjectIndex, InMemoryMetadataIndex>);

impl std::ops::Deref for InMemoryCas { /* → inner CasStore */ }
impl_cas_wrapper_traits!(InMemoryCas);  // CasApi + CasMaintenanceApi + ConstraintApi
```

The `impl_cas_wrapper_traits!` macro (defined in `storage/macros.rs`, uses `paste` crate)
generates trait impls that delegate to `self.0`. This avoids manual forwarding.
`ConfiguredCas` uses the `forward!` macro (defined in `config.rs`) to delegate trait methods to the inner `cas.0`.

## 8. Invariants & edge cases

### 8.1 Content identity

- Same bytes → same hash. Deterministic. Zero hash is sentinel-only, never stored.

### 8.2 Crash safety

- WAL is the single crash-safe commitment point. All operations append before acknowledging.
- ObjectIndex and MetadataIndex are derived — rebuilt by WAL replay.

### 8.3 No TOCTOU

No standalone `exists()` method. Use `get()` or `stat()` (both return `NotFound` on miss).
No `exists_many()`, no `set_constraint_batch()`, no `materialize_to_path()`, no `compact_index()`.
Removed methods are replaced by composition of remaining primitives.

### 8.4 Delta chain integrity

- Recursive `get(base_hash)` for reconstruction goes through full 3-layer lookup.
- If base_hash not found → `CorruptObject`.
- Cyclic references prevented by `check_no_cycle()` during chain traversal.

### 8.5 Constraint invariants

- `effective_bases = stored_bases ∩ live`. Dead bases are excluded.
- `prune_constraints()` removes entries whose target or bases no longer exist.
- DAG validation at set time is future work.

### 8.6 Codec versioning

- V1/V2 are read-only legacy. Writers always emit V3.
- New versions go in `delta/versions/vN.rs`. Keep the `DO NOT REMOVE` boundary guard.

## 9. Cross-crate integration

- **Conductor**: CAS hash used for state blob identity, external data keys, StringList input hashing.
  Conductor uses `C: CasApi + CasMaintenanceApi + ConstraintApi` bounds.
- **MediaPM**: Lock records keyed by `(media_id, variant)` → CAS hash. Materializer reads from CAS.
- **Constraints**: Set by conductor as optimization hints. CAS owns storage and enforcement.

**Contract**: Callers may call CAS concurrently (thread-safe). CAS doesn't reference
Conductor/MediaPM types. Failures propagate as-is.

## 10. Build & test

- `cargo test -p mediapm-cas` — unit + integration + doctest.
- `cargo clippy -p mediapm-cas` — lint.
- `cargo build -p mediapm-cas` — build with default features (cli).
- `cargo build -p mediapm-cas --no-default-features` — minimal (no CLI binary).
- Tests use `new_in_memory_cas()` — no filesystem dependencies.
