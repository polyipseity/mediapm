# CAS Agent Guide

> `mediapm-cas` — content-addressable blob store with delta-compression hints.
> `put(bytes)` → hash; `get(hash)` → bytes. Deduplicates identical content via
> Blake3-256. Foundation for deterministic workflows used by Conductor and MediaPM.
>
> **Note**: this crate is at `src/mediapm-cas/`. The name "mediapm-cas" is the
> canonical Cargo package name; there is no separate "conductor-cas" crate.

## 1. Hash

`Hash([u8; 32])` — blake3-256 content address.

- **Content-addressed**: `Hash::from_content(data)` = blake3(data). Same data → same hash.
- **Empty-content sentinel**: `Hash::empty()` = blake3(b"") (hash of empty content). API-level special-casing: `get`/`stat` return empty content immediately without backend lookups; `delete` is a no-op (indelible); constraints are always empty (set/get/patch succeed but no-op).
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

**put**: Hash data with `Hash::from_content`, append `WalEntry::Put` to WAL.
Write-through vs write-back is compile-time configured via `B::SYNC_MATERIALIZE && I::SYNC_MATERIALIZE`:
write-through materializes BlobStore + Index synchronously (immediate visibility);
write-back defers to the WAL consumer. Only `Hash::from_content(b"")` produces `Hash::empty()` — normal non-empty content never collides with it.

**get**: Three-layer lookup (Index → BlobStore → WAL fallback) via `ComposedReadView`.
Delta reconstruction is transparent. Returns `CasError::NotFound` if absent.

**stat**: Returns `ObjectMeta { len, encoding }`. Encoding is informational only
(Full or Delta { base_hash }). Callers must NOT make decisions based on encoding.

**delete**: Append `WalEntry::Delete` to WAL. Physical removal is
deferred to WAL consumer. Idempotent. Does not cascade.
Empty-content sentinel is a no-op — never appended to WAL.

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
    async fn get_constraint(&self, target: Hash) -> Result<BTreeSet<Hash>, CasError>;
    async fn patch_constraint(&self, target: Hash, patch: ConstraintPatch) -> Result<(), CasError>;
}
```

Constraints are **non-binding hints** — the system never blocks on completeness or accuracy.
Stored in a separate constraint map (in-memory `DashMap<Hash, BTreeSet<Hash>>`, rebuilt
from WAL), independent of object metadata. `get_constraint` returns an empty set when no
constraint exists (no `Option`). There is no `effective_bases` method — callers that need
live filtering must compose `get_constraint` with their own `live` set intersection.

Empty-content sentinel exception: constraints on `Hash::empty()` are always empty. `set_constraint`,
`get_constraint`, and `patch_constraint` all succeed but have no effect (always return
or leave empty sets).

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

Newtype around `CasStore<InMemoryWal, InMemoryIndex, InMemoryBlobStore>`.
Traits implemented via blanket `Deref` impls in `store.rs`.

#### FileSystemCas — persistent store

```rust
let cas = FileSystemCas::open(&Path::new("/path/to/store")).await?;
cas.put(bytes).await?;
```

Newtype around `CasStore<FileWal, FileSystemIndex, FileSystemBlobStore>`.
WAL + blob store + index constraints persisted on disk; blob metadata in-memory
(rebuilt from WAL on open). Index data (entries + constraints) saved to
`<store_dir>/index.json`.

Override of `FileSystemCas::object_path_for_hash` returns `Option<PathBuf>`
(returns `None` for in-memory stores, `Some(path)` for filesystem stores).

#### ConfiguredCas — dispatch enum

```rust
pub enum ConfiguredCas {
    InMemory(InMemoryCas),
    FileSystem(FileSystemCas),
}
```

Implements `CasApi`, `CasMaintenanceApi`, `ConstraintApi` by forwarding to inner
`cas.deref()` via the local `forward!` macro (defined in `config.rs`). Created via
`CasConfig::from_locator_with_options()` + `CasConfig::open()`, or via the convenience
`CasConfig::from_locator()` for quick setup with default options and empty integrity config.

### 2.6 Config types

| Type | Role |
|------|------|
| `CasConfig` | Single config object: `storage_locator` + `integrity` |
| `CasStorageLocator` | `InMemory` or `FileSystem { path }` |
| `CasIntegrityConfig` | `verify_on_read` strategies |
| `CasLocatorParseOptions` | Controls whether plain paths are accepted |
| `VerifyTriggerStrategy` | `Always`, `Modified`, `Sample { denominator }`, `Stale { timeout }` |

`CasConfig` provides:

- `from_locator_with_options(locator, opts, integrity)` — full control.
- `from_locator(locator)` — convenience with `allow_plain_filesystem_path: true` and no verification.

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
    │       └── v1.rs
    ├── blob_store/        — BlobStore trait + FileSystemBlobStore + InMemoryBlobStore + versioned path layout
    │   ├── mod.rs         — BlobStore trait + re-exports
    │   ├── mem_blob_store.rs — InMemoryBlobStore (DashMap, ephemeral)
    │   ├── fs_blob_store.rs — FileSystemBlobStore (atomic hash-derived layout)
    │   └── versions/      — path layout versions V1+
    │       ├── mod.rs     — version dispatch
    │       └── v1.rs      — V1 layout: v1/blake3/ab/cd/<hex>
    ├── index/             — Index trait + InMemoryIndex + FileSystemIndex
    │   ├── mod.rs         — trait + IndexEntry + re-exports
    │   ├── mem_index.rs   — InMemoryIndex (DashMap, separate constraint map)
    │   ├── fs_index.rs    — FileSystemIndex (persistent constraints via JSON)
    │   └── versions/      — V1 persistence format (versioned JSON constraint file)
    ├── read_view.rs       — ComposedReadView (3-layer lookup: Index → BlobStore → WAL)
    ├── pending_ops.rs     — PendingOps (in-flight read dedup helper)
    ├── bg_engine.rs       — BackgroundEngine (WAL consumer → BlobStore + Index, maintenance)
    ├── in_memory.rs       — InMemoryCas wrapper
    └── file_system.rs     — FileSystemCas wrapper + open()
```

## 4. Data flow

```text
put(data) → Hash(data) → Wal.append(Put{hash, data})
                          ↓ (write-through: inline) │ (write-back: deferred)
WAL consumer (bg_engine) → BlobStore.write(hash) + Index.put(hash) → checkpoint
                                                                    ↓
get(hash) → ReadView: Index → BlobStore.read/read_delta → WAL fallback (tombstone check)
                                                                    ↓
delete(hash) → Wal.append(Delete{hash})
                                                                    ↓
WAL consumer → re-materialize dependents → BlobStore.delete(hash) + Index.delete(hash)
```

## 5. Internals

### 5.1 WAL

The only crash-safe commitment point. Index and BlobStore are derived —
rebuildable by WAL replay.

**Entry types**: `Put { hash, data }`, `Delete { hash }`, `Constraint { target, bases }`.
Only single-entry `append` is exposed.

**PendingState**: `Present(Bytes)` / `Tombstone` / `NotPresent`. Used by ReadView's L3 WAL fallback.

**WAL Consumer** (`BackgroundEngine::run_wal_consumer`): Replays WAL entries from
checkpoint position, materializing to BlobStore + Index. After processing,
position is persisted atomically. Idempotent.

### 5.2 BlobStore

Pluggable payload backend.

- `InMemoryBlobStore`: `DashMap<Hash, (Bytes, ObjectEncoding)>`. Ignores Full/Delta distinction.
- `FileSystemBlobStore`: Hash-derived paths `<root>/v1/blake3/ab/cd/<remaining>` (full)
  or `<remaining>.diff` (delta). Atomic write via temp+rename. Hash verification on read.
  `delete` uses `tracing::warn!` on `remove_file` failures (never returns I/O error —
  missing files are treated as already-deleted).
  `materialized_path(&self, hash) -> Option<PathBuf>` returns the on-disk path for `hash`
  (overrides the trait default of `None`).

**`materialized_path`**: non-async method on `BlobStore` trait (default `None`). Returns
`Some(path)` for filesystem-backed stores, `None` for in-memory. Used by
`FileSystemCas::object_path_for_hash`.

### 5.3 Index

Object metadata index. Both implementations share the same trait.

- **`InMemoryIndex`**: `Arc<DashMap<Hash, IndexEntry>>` for payload metadata
  (`IndexEntry { len, encoding }`). Constraint data is stored in a separate map:
  `constraints: Arc<DashMap<Hash, BTreeSet<Hash>>>`.
- **`FileSystemIndex`**: Wraps `InMemoryIndex` with persistent snapshot.
  On mutation (`put`/`delete`/`set_constraint`), writes full snapshot (entries +
  constraints) to a V1 JSON file atomically (temp+rename). On `rebuild_from_wal`,
  replays WAL then overlays persisted state.

**Constraint separation**: Object metadata (`put`/`get`/`delete`) and constraint data
(`set_constraint`/`get_constraint`) are stored in independent maps. Put entries from WAL
replay do not touch the constraint map; only `Constraint` WAL entries populate it. This
keeps the data model clean and removes `Option` from constraint representation.

**Versioned persistence** (`storage/index/versions/`): V1 JSON format:

```json
{ "version": 1, "entries": { "hash_hex": { "len": 123, "encoding": "Full" } }, "constraints": { "target_hex": { "bases": ["base1_hex"] } } }
```

`entries` field uses `#[serde(default)]` for backward compatibility with snapshots
that predate entry persistence.

Async wrappers use `tokio::task::spawn_blocking`; sync `load()`/`save()` functions
are shared by both implementations.

### 5.4 ReadView

Three-layer lookup for get/stat.

1. **Index**. Check entry encoding. If `Full` → BlobStore.read(). If `Delta { base_hash }` → iterative walk.
2. **BlobStore**. Read payload bytes (full or delta envelope). Walk delta chains via `read_delta`/`read`.
3. **WAL fallback**. Pending entries not yet materialized. Respects tombstones.

**Concurrent read dedup**: First caller inserts `PendingResult` with `Notify`; subsequent
callers wait for shared result.

**Delta reconstruction**: Index → `delta_resolve::resolve_delta_chain` → BlobStore.read_delta(hash)
→ walk base chain via Index → BlobStore.read(base) → `DeltaPatch::apply(base_bytes, vcdiff)`.
If base not found → `CasError::CorruptObject`.

### 5.5 Delta Codec

- **DeltaPatch**: VCDIFF wrapper via `oxidelta`. `diff(base, target)` → patch; `apply(patch, base)` → reconstructed target.
- Two functions for chain resolution:
  - `apply_delta_chain` in `delta/patch.rs`: Pure `pub(crate)` function that takes base bytes,
    collected delta envelopes, applies VCDIFF patches innermost-first, returns fully
    reconstructed payload. Used by `delta_resolve::resolve_delta_chain`.
  - `resolve_delta_chain` in `storage/delta_resolve.rs`: `pub(super)` async walker that
    reads delta blobs from BlobStore and builds the chain, then calls `apply_delta_chain`.
    Shared by `ComposedReadView::fetch_inner` and `BgEngine::read_full_bytes`.
- **StoredObject**: Struct wrapping `DeltaState`. Encode/decode to/from versioned envelopes.
- **Versioned envelopes**: V1/V2 (read-only legacy, magic `b"MDCASD"`), V3+ (current writer, magic `b"CASDLT"`).

**Versioning boundary guard**: Code outside `delta/versions/` must interact with versioned
envelopes only through `delta::versions` module APIs (`mod.rs`), never via `delta::versions::vX` imports.

### 5.6 Background Engine

Drives WAL consumer and maintenance pass. GC never deletes objects — only prunes constraint
metadata. Objects are removed solely by `CasApi::delete` materialized through the WAL consumer.

**WAL consumer guarantees**: After a `run_wal_consumer` cycle completes, all entries up to the
checkpoint position are materialized in BlobStore + Index. After materialization, the checkpoint
is advanced and `WAL::trim(checkpoint)` is called. The checkpoint starts from `consumed_position()`
(passed as `start_pos` to `CasStore::new` and `BackgroundEngine::new`), not from
`WalPosition::ZERO`, so already-consumed entries are skipped on restart. This means:

- Consumed WAL entries are **physically removed** from the WAL:
  - `FileWal::trim()` deletes segment files whose `last_pos <= checkpoint` and prunes pending
    HashMap entries with `pos <= checkpoint`.
  - `InMemoryWal::trim()` pops entries from the VecDeque front.
  - After trim, the only authoritative copies are in BlobStore + Index.
- L3 WAL fallback in ReadView is only exercised for entries appended after the last cycle
  (transient, before consumer materializes).
- The tombstone check (always performed on every `get()` even on Index hit) is O(1) for FileWal
  (HashMap lookup) and cheap for InMemoryWal (small pending set after trimming).

**Crash recovery**: On restart, `rebuild_from_wal()` replays all entries from the WAL (starting
from `WalPosition::ZERO`), rebuilding Index + constraint map. The WAL contains the complete
history since the last trim — the last checkpoint always points at or before the oldest
surviving segment.

## 6. Delete semantics — no dangling deltas

When the WAL consumer processes `Delete { hash }`:

1. **Scan for dependents**: Find Index entries where `encoding == Delta { base_hash: hash }`.
2. **Re-materialize each**: Read delta blob from BlobStore, decode V3 envelope, fetch base
   (still available in BlobStore), apply VCDIFF, store result as Full in BlobStore + Index.
3. **Physically remove**: `BlobStore.delete(hash)` + `Index.delete(hash)`.

The WAL consumer doesn't advance the checkpoint until re-materialization is complete.

**Does not cascade**: Deleting B has zero effect on other hashes. Even if A is delta-compressed
against B, A's bytes live under A's content hash.

## 7. Wrapper pattern

`InMemoryCas` and `FileSystemCas` are newtype wrappers around `CasStore<...>` with `Deref`:

```rust
pub struct InMemoryCas(pub(crate) CasStore<InMemoryWal, InMemoryIndex, InMemoryBlobStore>);

impl std::ops::Deref for InMemoryCas { /* → inner CasStore */ }
```

All three CAS traits (`CasApi`, `CasMaintenanceApi`, `ConstraintApi`) are implemented
via blanket impls in `store.rs` for any `T: Deref<Target = CasStore<J, I, B>>`.
`ConfiguredCas` uses the local `forward!` macro (defined in `config.rs`) to delegate
trait methods to the inner variant via `cas.deref()`.

## 8. Invariants & edge cases

### 8.1 Content identity

- Same bytes → same hash. Deterministic. `Hash::empty()` (`blake3(b"")`) is a well-known sentinel; only empty content produces it.

### 8.2 Empty-content sentinel

- `Hash::empty()` = `blake3(b"")` — always present, indelible.
- All operations use normal code paths, except `delete(empty)` which is a no-op
  (never appended to WAL).

### 8.3 Crash safety

- WAL is the single crash-safe commitment point. All operations append before acknowledging.
- Index and BlobStore are derived — rebuilt by WAL replay.

### 8.4 No TOCTOU

No standalone `exists()` method. Use `get()` or `stat()` — both return `NotFound` on miss.

### 8.5 Delta chain integrity

- Iterative `get(base_hash)` for reconstruction goes through Index → BlobStore walk.
- If base_hash not found → `CorruptObject`.
- Cyclic references prevented by `check_no_cycle()` during chain traversal.

### 8.6 Constraint invariants

- `get_constraint` returns `BTreeSet<Hash>` — empty set means no constraint (no `Option`).
- `prune_constraints()` removes entries whose target or bases no longer exist.
- No `effective_bases` method — callers compute live intersection themselves.
- DAG validation at set time is future work.
- Self-referencing constraint (target == base) is rejected at set/patch time.
- Constraint data is stored independently from object metadata (separate `DashMap`).
  Put/delete/stat operations do not affect constraint state.

### 8.7 Write-through vs write-back compile-time policy

`CasStore::put()` uses compile-time dispatch via associated consts (`BlobStore::SYNC_MATERIALIZE`,
`Index::SYNC_MATERIALIZE`). Effective policy: `B::SYNC_MATERIALIZE && I::SYNC_MATERIALIZE`.

| Backend triplet | BlobStore | Index | Effective |
|-----------------|-----------|-------|-----------|
| `InMemoryCas` | `InMemoryBlobStore` (true) | `InMemoryIndex` (true) | write-through |
| `FileSystemCas` | `FileSystemBlobStore` (false) | `FileSystemIndex` (false) | write-back |

**`delete()`** is always write-back regardless of `SYNC_MATERIALIZE` — physical removal
is deferred to the WAL consumer for correct delta-dependent re-materialization.

### 8.8 Codec versioning

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
