# CAS Agent Guide

> CAS is the content-identified object store. `put(bytes)` → hash; `get(hash)` → bytes.
> Deduplicates identical content via Blake3-256. Foundation for deterministic workflows.
> Used by Conductor (state), MediaPM (materialization), and CAS internally.

This guide is the canonical specification. Every statement is part of the
behavioral contract — code must match.

## 1. Hash

`Hash([u8; 32])` — blake3-256 content address.

- **Content-addressed**: `Hash::from_content(data)` = blake3(data). Same data → same hash.
- **Zero sentinel**: `Hash::zero()` = `[0u8; 32]`. Never stored: put is no-op, get/stat always succeed (empty data), delete is no-op.
- **Wire format**: Multihash-encoded (`multihash` crate): `[varint code: 0x1e][varint length: 0x20][32-byte digest]`.
  `storage_bytes()` / `from_storage_bytes_with_len()` use `Multihash::wrap` / `Multihash::read`.
  The varint format enables future hash function adoption without breaking backward
  compatibility at the multihash level.
- **Serialization**: Derives `Serialize`/`Deserialize` (serde) and `Ord` (lexicographic on bytes).

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

**Guarantees** (within a single async task):

- **Write-then-read**: After `put(data)` returns, `get(hash)` returns the data and `stat(hash)` returns metadata immediately.
- **Delete-then-read**: After `delete(hash)` returns, `get(hash)` and `stat(hash)` return NotFound immediately.
- **Idempotent**: `put(data)` twice with same data is no-op. `delete(hash)` twice is no-op.
- **Crash survival**: After any method returns Ok, the effect survives process death.

**put**: Hash data with `Hash::from_content`, append `JournalEntry::Put` to WAL (crash-safe
commit), hint cache. Zero hash returns immediately — nothing stored.

**get**: Three-layer lookup (L1 cache → L2 ObjectStore → L3 journal fallback).
Delta reconstruction is transparent — caller never sees it.
Returns `CasError::NotFound` if absent.

**stat**: Returns `ObjectMeta { len, encoding }`. Encoding field is informational only
(Full or Delta { base_hash }). Callers must NEVER make decisions based on it.

**delete**: Append `JournalEntry::Delete` to WAL, tombstone cache. Physical removal is
deferred to WAL consumer. Idempotent. Does not cascade (see §5).

### 2.2 CasApiStreaming — blanket-impl streaming extension

```rust
#[async_trait]
pub trait CasApiStreaming: CasApi {
    async fn put_stream<R: AsyncRead + Send + Unpin>(&self, reader: R) -> Result<Hash, CasError>;
    async fn get_stream<W: AsyncWrite + Send + Unpin>(&self, hash: Hash, writer: W) -> Result<(), CasError>;
}
```

Blanket impl over CasApi (buffers through bytes). Override for zero-copy paths.

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

Constraints are **non-binding hints** — the system never blocks on completeness or
accuracy. `effective_bases = stored_bases ∩ live_hashes`. Empty effective set = "full
or any base allowed". Stored in MetadataStore (in-memory, rebuilt from journal).

### 2.4 CasMaintenanceApi — maintenance operations

```rust
#[async_trait]
pub trait CasMaintenanceApi: Send + Sync {
    async fn optimize_once(&self) -> Result<OptimizeReport, CasError>;
    async fn prune_constraints(&self) -> Result<PruneReport, CasError>;
    async fn gc_sweep(&self) -> Result<GcSweepReport, CasError>;
    async fn list_all_hashes(&self) -> Result<Vec<Hash>, CasError>;
    async fn repair_index(&self) -> Result<IndexRepairReport, CasError>;
}
```

- **optimize_once**: Drain WAL consumer, run GC + optimizer.
- **prune_constraints**: Remove constraint entries whose target or bases no longer exist.
- **gc_sweep**: Prune constraint metadata only (objects only removed by explicit delete).
- **repair_index**: Rebuild index from storage contents.

## 3. Architecture

### Module tree

```text
src/cas/src/
├── lib.rs              — crate root, re-exports, doc-test
├── api.rs              — CasApi, CasApiStreaming, ConstraintApi, CasMaintenanceApi
├── hash.rs             — Hash type (blake3-256, multihash wire format)
├── error.rs            — CasError enum
├── main.rs             — CLI binary (feature-gated)
├── cli.rs              — CLI subcommands (feature-gated)
├── cli_visualization.rs — topology viz (feature-gated)
├── delta/
│   ├── delta.rs        — DeltaPatch (VCDIFF via oxidelta)
│   ├── object.rs       — DeltaState + StoredObject (version-agnostic)
│   └── versions/       — V1/V2/V3 delta envelope wire formats
└── storage/
    ├── store.rs        — CasStore (composed handle, implements all traits)
    ├── wal.rs          — Journal trait + InMemoryJournal
    ├── payload_store.rs — ObjectStore trait + InMemoryObjectStore
    ├── meta_store.rs   — MetadataStore trait + InMemoryMetadataStore
    ├── read_view.rs    — ReadView + ComposedReadView (3-layer lookup)
    ├── bg_engine.rs    — BackgroundEngine (WAL consumer + maintenance)
    ├── in_memory.rs    — new_in_memory_cas() factory
    └── journal/        — FileJournal (planned, not yet wired)
```

### Data flow

```text
put(data) → Hash(data) → Journal.append(Put{hash, data}) → cache hint
                                                                    ↓
WAL consumer (bg_engine) → ObjectStore.put(hash, data) → checkpoint
                                                                    ↓
get(hash) → ReadView: L1 cache → L2 ObjectStore → L3 Journal fallback
                                                                    ↓
delete(hash) → Journal.append(Delete{hash}) → tombstone cache
                                                                    ↓
WAL consumer → re-materialize dependents → ObjectStore.delete(hash)
```

## 4. Internals

### 4.1 Journal (WAL)

The only crash-safe commitment point. ObjectStore and MetadataStore are derived —
rebuildable by journal replay.

**Entry types**: `Put { hash, data }`, `Delete { hash }`, `Constraint { target, bases }`.

**PendingState**: `Present(Bytes)` / `Tombstone` / `NotPresent`. Used by ReadView's L3 fallback.

**WAL Consumer** (`BackgroundEngine::run_wal_consumer`): Replays journal entries from
checkpoint position, materializing to ObjectStore/MetadataStore. After processing,
position is persisted atomically. Idempotent — re-consuming already-processed entries
is safe.

### 4.2 ObjectStore

Pluggable payload backend. `InMemoryObjectStore` uses `DashMap<Hash, (Bytes, ObjectEncoding)>`.
Stores raw bytes for Full encoding or complete V3 delta envelope for Delta encoding.

**Delta-aware operations**: `put(hash, data, encoding)` overwrites existing. `get(hash)` returns
stored bytes regardless of encoding. `stat(hash)` returns `(len, encoding)`.

### 4.3 ReadView

Three-layer lookup for get/stat:

1. **L1 — Cache** (DashMap, 60s TTL). Fast path. Proactively updated via `hint_state_change()`.
2. **L2 — ObjectStore**. If delta-encoded, reconstruct (decode V3 envelope → recursive get(base_hash) → apply VCDIFF → cache result).
3. **L3 — Journal fallback**. Pending entries not yet materialized. Respects tombstones.

**Concurrent read dedup**: First caller inserts `PendingResult` with `Notify`; subsequent
callers wait for shared result.

**Delta reconstruction**: For L2 delta entries, decode V3 envelope → recursive `get(base_hash)`
(through full 3-layer lookup) → `DeltaPatch::apply(base_bytes, vcdiff)` → cache reconstructed
bytes. If base_hash not found, return `CorruptObject`.

### 4.4 Delta Codec

- **DeltaPatch**: VCDIFF wrapper via `oxidelta`. `diff(base, target)` → patch; `apply(patch, base)` → reconstructed target.
- **StoredObject**: `Full { payload }` or `Delta { state }`. Encode/decode to/from versioned envelopes.
- **Versioned envelopes**: V1/V2 (read-only legacy, magic `b"MDCASD"`), V3+ (current writer,
  magic `b"CASDLT"`). Format: `[magic(6)][version(2)][base_hash(multihash)][diff_hash(32)][content_len(8)][payload]`.

**Versioning boundary guard**: Files outside `delta/versions/` must interact with versioned
envelopes only through `delta::versions` module APIs, never via `delta::versions::vX` imports.

### 4.5 Background Engine

Drives WAL consumer and maintenance pass. GC never deletes objects — only prunes constraint
metadata so orphaned bases are removed individually. Objects are removed solely by
`CasApi::delete` materialized through the WAL consumer.

## 5. Delete Semantics — No Dangling Deltas

When the WAL consumer processes `Delete { hash }`:

1. **Scan for dependents**: Find ObjectStore entries with `encoding == Delta { base_hash: hash }`.
2. **Re-materialize each**: Fetch delta bytes, decode V3 envelope, fetch base (still available),
   apply VCDIFF, store as Full, hint cache.
3. **Physically remove**: `ObjectStore.delete(hash)`.

The WAL consumer doesn't advance the checkpoint until re-materialization is complete.

**Does not cascade**: Deleting B has zero effect on any other hash. Even if A is delta-compressed
against B, A's bytes live under A's content hash. The A→B dependency is a reconstruction concern,
not a storage concern.

## 6. Invariants & Edge Cases

### 6.1 Content identity

- Same bytes → same hash. Deterministic. Zero hash is sentinel-only, never stored.

### 6.2 Crash safety

- Journal is the single crash-safe commitment point. All operations append before acknowledging.
- ObjectStore and MetadataStore are derived — rebuilt by journal replay.

### 6.3 No TOCTOU

No standalone `exists()` method. Use `get()` or `stat()` (both return NotFound on miss).

### 6.4 Delta chain integrity

- Recursive `get(base_hash)` for reconstruction goes through full 3-layer lookup.
- If base_hash not found → `CorruptObject`.
- Cyclic references prevented by `check_no_cycle()` in chain traversal.

### 6.5 Constraint invariants

- `effective_bases = stored_bases ∩ live_hashes`. Dead bases are excluded.
- `prune_constraints()` removes entries whose target or bases no longer exist.
- Constraint-graph DAG validation at set time is future work.

### 6.6 Codec versioning

- V1/V2 are read-only legacy. Writers always emit V3.
- New versions go in `delta/versions/vN.rs` with `DO NOT REMOVE` policy guard.
- Versioned boundary guard: non-versions/ code must import versioned behavior through
  `delta::versions` APIs (mod.rs), never `delta::versions::vX`.

## 7. Cross-Crate Integration

- **Conductor**: CAS hash used for state blob identity, external data keys, StringList input hashing.
- **MediaPM**: Lock records keyed by `(media_id, variant)` → CAS hash. Materializer reads from CAS.
- **Constraints**: Set by conductor as optimization hints. CAS owns storage and enforcement.

**Contract**: Conductor may call CAS concurrently (thread-safe). CAS doesn't reference Conductor
types. Failures propagate as-is.

## 8. Build & Test

- `cargo test -p mediapm-cas` — unit + integration + doctest.
- `cargo clippy -p mediapm-cas` — lint.
- `cargo build -p mediapm-cas` — build with default features (cli).
- `cargo build -p mediapm-cas --no-default-features` — minimal (no CLI binary).
- Tests use `new_in_memory_cas()` — no filesystem dependencies.
- Integration tests in `tests/int/`: api_workflows, concurrent, constraints, in_memory, maintenance, read_view.
  /// - `false` means the object may still exist (conservative — caller
  /// must fall through to storage for a definitive answer).
  pub fn contains(&self, hash: &Hash) -> bool { ... }

    /// Batch variant — checks up to `hashes.len()` entries in one call.
    pub fn contains_many(&self, hashes: &[Hash]) -> CasExistenceBitmap { ... }
}

```text

**Index invalidation strategy**:

- The index is populated lazily on first existence check, then incrementally updated as new objects are stored.
- Object removal (prune, GC) removes entries from the index synchronously.
- Index rebuild is triggered on startup if the stored index version differs from the code version.

**Accepted guarantee trade-off**: False negatives are acceptable (index misses fall back to storage). False positives are NOT acceptable — `contains(hash) == true` must always be correct. This is enforced by:

- Index entries are only added after successful `put()` or confirmed storage-layer `exists()`,
- Index entries are removed synchronously during delete operations,
- On-disk index persistence uses the same atomic-commit pattern as the object store.

**Integration with Conductor**: The `exists_many` method on `CasApi` would first query the index, then batch-check any remaining unknowns against storage. This split ensures the index remains a pure optimization: correctness does not depend on it.

**Performance target**:

- Hot index (fits in RAM): O(1) per check, zero syscalls,
- Cold index (first run, partial load): O(misses) stat(2) calls plus batch fill,
- Expected throughput: 10,000+ checks per millisecond on modern hardware.

### Index Repair & Recovery Scan

The CAS `repair_index()` operation rebuilds the index from the actual storage contents. The scan pipeline uses a two-pass approach to minimize memory pressure:

**Pass 1 — Catalog scan**: Walk the storage backend and classify each object into a `ScannedObjectCatalog` with two maps:

| Map | Type | Contents |
|-----|------|----------|
| `full_objects` | `BTreeMap<Hash, ObjectMeta>` | Metadata only (hash, size, compression). Stream-verified during scan; bytes discarded after verification. |
| `delta_objects` | `BTreeMap<Hash, StoredObject>` | Full bytes retained in memory. Needed for delta-chain reconstruction. |

**Pass 2 — Index reconstruction**: Walk the delta chain roots reachable from `delta_objects`, reconstruct full content on demand, and insert entries into the rebuilt index.

**Memory model**: Recovery memory is `O(delta_count × delta_size)` instead of `O(total_store_bytes)`. Full-object bytes are streamed and discarded; only delta-object bytes are held in memory for reconstruction.

**Error handling**: CAS errors propagate via `?` regardless of workflow purity; no auto-retry on CAS failure.

### Performance

**Hot paths**:

| Path | Target | Technique |
|------|--------|-----------|
| **CAS read** (full object) | O(file_size) | mmap for ≥64KB; buffer pool for small |
| **CAS delta read** | O(depth × patch_size) | Concurrent candidate scoring (8 tasks) |
| **CAS stream read** (large object) | O(file_size) | Streaming chunks (256 KiB) via `stream::unfold`; small objects ≤256 KiB read in one chunk |
| **CAS materialize** (full object fast path) | O(file_size) | `fs::copy` for filesystem backend — kernel-level copy, no userspace buffer allocation; delta fallback via `get()` + write |

**Resource bounds**:

| Resource | Default | Config |
|----------|---------|--------|
| Delta chain depth | 32 | `MAX_DELTA_DEPTH` |
| Buffer pool size | 128 | `FILESYSTEM_STREAM_BUFFER_POOL_MAX_BUFFERS` |
| Actor RPC timeout | 8 sec | `FILESYSTEM_OBJECT_ACTOR_RPC_TIMEOUT_MS` |
| Optimizer concurrency | 8 | `FILESYSTEM_CANDIDATE_EVAL_CONCURRENCY` |

**Mmap lease & actor RPC deadlock prevention**:

The `FileSystemCas` backend uses a `FileObjectActor` (ractor actor) to serialize all file mutations per store. Large objects (≥64 KB) are served via mmap with reference-counted `ActiveMmapLease` entries tracked in an `ActiveMmapRegistry`.

- **Fix C** — `wait_for_no_active_mmap` is compiled out on Unix (`#[cfg(not(target_os = "windows"))]` no-op) because POSIX `rename(2)`/`unlink(2)` keep the old inode alive for existing mmap holders. Preserved on Windows.
- **Two-phase staging** — The optimizer (`optimize_target_if_beneficial`) and `delete()` paths no longer send actor RPCs. Instead they write new object variants to staging paths (`tmp/`) outside any lock (Phase 1), then under the index write lock atomically rename staging→final and update index metadata (Phase 2). This eliminates both the mmap lease deadlock and a TOCTOU race where a concurrent reader could observe new file content with stale index metadata.

**Content-addressed memory lifecycle**: Use `bytes::Bytes` for all CAS-resident data to enable zero-copy sharing and cheap clones (ref-count bumps). Avoid `Vec<u8>` for hot-path CAS data in public APIs. CAS `get()` returns `Bytes`; `materialize_to_path()` skips the `Bytes` round-trip entirely when the backend can fast-path via `fs::copy`.

**Recovery memory**: CAS `repair_index()` uses `O(delta_count × delta_size)` memory instead of `O(total_store_bytes)` — full objects are streamed and discarded; only delta-object bytes are held in memory for chain reconstruction.

---

### Part 1: CAS Edge Cases

#### 1.1 Delta Chain Corruption & Recovery

**Issue**: Specification states "adjacent-only migrations" and "O(depth) reconstruction" but does not address partial delta chain loss.

**Scenarios**:

| Scenario | Current Spec | Gap |
|----------|---|---|
| Intermediate delta base deleted during optimization | "Index repair" mentioned but not detailed | No explicit rollback strategy |
| Delta chain depth exceeds MAX_DELTA_DEPTH (32) | Optimizer avoids creating longer chains | What if old chains exceed limit after config change? |
| Corrupted delta (bytes don't apply cleanly) | Codec error raised | Does CAS fall back to full object? Automatic? |
| Orphaned deltas (no base references them) | Prune removes them | Is prune automatic on GC or manual? |
| Cyclic delta reference (A → B → A) | Addressed via `check_no_cycle()` in `storage/chain.rs` | Detected before chain traversal in both filesystem and in-memory backends; `HashSet`-based visited tracking |

**Risk**: Silent data corruption if intermediate base is manually deleted and reconstruction is attempted.

**Recommendations**:

- ✅ Done: cyclic delta reference detection via shared `check_no_cycle()` helper
- Document fallback: if reconstruction fails, **automatically promote to full object copy**
- Specify prune trigger: automatic (on size threshold), manual (operator invokes), or both
- Add test: "corrupted delta chain recovery" with orphaned intermediate base

#### 1.2 Concurrent Mutation During Optimization

**Issue**: Specification states optimizer "concurrently scores candidates (8 tasks)" but does not detail interaction with concurrent puts/deletes.

**Scenarios**:

- Optimizer reads full object for candidate scoring; meanwhile `put()` writes new version
- `delete()` removes object mid-optimization
- Two optimizations run concurrently on overlapping object sets

**Current Spec**: "CAS doesn't reference Conductor types; failures propagated as-is"

**Gap**: No isolation guarantee (e.g., snapshot vs. live reads)

**Risk**: Optimizer producing invalid encoding if object mutated during scoring; stale indexes if deletes race with optimization.

**Recommendations**:

- ✅ Done: **Two-phase staging isolation** — The optimizer and `delete()` write new variants to staging paths outside any lock, then under the index write lock atomically renames and updates metadata. A concurrent reader holding the read lock is blocked during Phase 2 and observes consistent state.
- Document: **concurrent puts with identical content are deduplicated** (single write, multiple waiters) vs. race (last write wins)
- Add test: "concurrent optimize + put + delete" scenario

#### 1.3 Constraint Satisfaction Impossibility

**Issue**: `set_constraint_batch()` validates each op's bases exist, but no check for **circular or impossible constraints**.

**Scenario**: Object A with current base = B; `set_constraint_batch([Set { target_hash: A, potential_bases: [C] }])` where C depends on A (direct or transitive).

**Current Spec**: "Optimizer honors constraints"

**Gap**: Constraint-graph DAG validation at `set_constraint_batch()` API not yet implemented; delta-chain cycle detection exists at the storage layer.

**Risk**: Optimizer fails at runtime when trying to resolve circular constraint.

**Status**: Delta-chain cycle detection is implemented via `check_no_cycle()` in `storage/chain.rs`. Constraint-graph-level DAG validation on `set_constraint_batch()` remains as future work.

**Changes (Phases 1–3/5)**:

- Per-constraint forced backup snapshots removed — `set_constraint_batch()` persists all ops in a single `persist_index_batch` call.
- Three call sites in `step_worker` now batch into a single `set_constraint_batch()` call.

**Recommendations**:

- **Constraint graph DAG validation** on `set_constraint_batch()`: refuse if introducing cycle
- Add explicit rule: "Constraints must form a DAG; cycles rejected at set time"
- Add test: "circular constraint detection"

#### 1.4 Hash Algorithm Agility

**Issue**: Specification mentions "Add variant to `HashAlgorithm` enum" for future algorithms, but no migration strategy for **existing persisted hashes**.

**Scenario**: System running with Blake3-256 needs to migrate to SHA3-256; existing CAS contains only Blake3 hashes.

**Current Spec**: "No speculative forward-compatibility; only N → N+1 migrations"

**Gap**: No hash algorithm versioning layer; codec doesn't tag algorithm in hash envelope.

**Risk**: If hash algorithm is updated, old CAS becomes incompatible.

**Recommendations**:

- **Hash envelope must include algorithm discriminant** (not implicit from context)
- Add `HashAlgorithm` field to wire format (even if currently always Blake3)
- Document: "Hash algorithm upgrades require data migration (re-hash all objects)"
- Add test: "cross-algorithm hash comparison (should fail or require re-hash)"

#### 1.5 Out-of-Space Handling

**Issue**: Specification mentions "OutOfSpace (triggers prune)" but does not specify **automatic vs. manual prune invocation** or **retry semantics**.

**Current Spec**: "Fail-fast; no partial state"

**Gap**: Who retries after prune? User code or CAS internal?

**Risk**: Silent data loss if prune removes needed objects.

**Recommendations**:

- Explicit policy: **Automatic prune on OutOfSpace** (within transaction) or **return error, caller retries after external prune**
- If automatic: specify prune strategy (LRU, oldest first, cost model)
- If manual: caller responsibility to invoke `prune()` and retry `put()`
- Add test: "out-of-space + prune + retry" happy path

#### 1.6 Mmap Failure & Fallback

**Issue**: Specification states "mmap for ≥64KB; buffer pool for small" but does not address **mmap failure or unsupported file systems**.

**Scenarios**: CAS on network file system without mmap support; file system permissions prevent mmap; mmap request exceeds OS limit.

**Current Spec**: Performance optimization only

**Gap**: No fallback; error handling unspecified.

**Risk**: If mmap fails, entire read fails instead of gracefully degrading to buffer-based read.

**Mmap lease deadlock & optimizer TOCTOU race** (resolved):

- `optimize_target_if_beneficial` previously deadlocked with `FileObjectActor` when a caller held an `ActiveMmapLease` for hash H while sending a `PersistObjectVariant(H)` RPC.
- **Fix**: Both `optimize_target_if_beneficial` and `delete()` now use two-phase staging (see mmap lease section above), removing the need for actor RPC entirely. The batch message variant `PersistObjectVariants` has been removed as a simplification.

**Recommendations**:

- **Fallback to buffer-pool read on mmap failure** (not hard error)
- Log warning if mmap unavailable (may impact performance)
- Add test: "mmap unavailable → fallback to buffer pool"

#### 1.7 Index Repair Semantics

**Issue**: Specification mentions `repair_index()` returns `IndexRepairReport` but does not specify **what corruption is detected or how it's repaired**.

**Current Spec**: "Index repair on startup (optional)"

**Gap**: No definition of "repair" — is it automated or advisory?

**Risk**: Unclear when to invoke; customer doesn't know if index is healthy.

**Recommendations**:

- ✅ Done: Repair scope documented (3-layer defense — startup orphan scan, `exists()`/`exists_many()` filesystem fallback with auto-healing, `put_new_full_object()` rollback on index persistence failure)
- Document repair scope: "Detects orphaned entries, duplicate entries, version mismatches; removes orphaned, de-duplicates, auto-upgrades schema"
- Make explicit: **Repair never deletes user data** (only index/metadata)
- Add test: "index corruption scenarios → repair restores consistency"

#### 1.8 Index/Filesystem Desync (Resolved)

**Issue**: After a process crash or partial write, the CAS `index.redb` may lack entries for blob files that exist on disk. This causes `exists()` to return false for orphaned files.

**Root cause**: Race window between `persist_object_variant()` (file write) and `persist_index_batch()` (redb write) in `put_new_full_object()`.

**Resolution** (3-layer defense):

1. **Startup orphan scan**: `repair_orphaned_objects_invariant()` in `open_with_alpha_and_recovery()` walks the storage root, finds files not in the index, reads their hashes, and heals them into the in-memory index.
2. **`exists()`/`exists_many()` filesystem fallback**: When a hash is absent from the in-memory index, probe the filesystem directly. If found, heal the index entry (`heal_orphaned_object()`) and return true.
3. **`put_new_full_object()` rollback**: If `persist_index_batch()` fails after a successful file write, remove the in-memory index entry and delete the orphaned file.

#### 1.9 Concurrent Access During Recovery

**Issue**: The `repair_index()` scan pipeline opens storage objects for streaming verification while other processes may concurrently write to the store.

**Resolution**: The lock file at `<store_root>/lock` serializes exclusive access. `FileSystemRecoveryOptions.wait_for_lock` controls behavior when the lock is already held:

| `wait_for_lock` | Lock available | Lock held |
|----------------|----------------|-----------|
| `false` (default) | Acquire lock, proceed with recovery | Return `CasError::StoreLocked` immediately |
| `true` | Acquire lock, proceed with recovery | Retry with backoff until lock acquired |

**Recovery memory safety**: After the `ScannedObjectCatalog` split, `full_objects` stores only metadata (bytes discarded after stream verify), and `delta_objects` retains full bytes only for delta objects. Memory drops to `O(delta_count × delta_size)`.

| Scenario | Risk | Mitigation |
|----------|------|------------|
| Recovery scan while concurrent write | Partial-write observation | Lock serializes; exclusive access during scan |
| Concurrent `put()` on same hash | Race: scan may miss new object | Index rebuild re-scans after lock; missed entries = false negative (acceptable) |
| Stale NFS lock after process crash | Lock file exists but holder is dead | Manual lock removal; `wait_for_lock=true` may livelock on stale NFS locks |
| Process crash mid-recovery | Partial index written | Atomic commit: index write is all-or-nothing |

#### 1.10 verify_time = 0 Recovery

**Issue**: Newly stored or migrated objects have `verify_time = 0`. The Stale strategy compares `now - 0 > timeout`, which always triggers verification on first access.

**Current Spec**: "A value of 0 means never verified"

**Gap**: No guidance on how Stale strategy treats `verify_time = 0`. Stale is part of the default config, so every object migrated from v1 will be considered stale and verified on first access after migration.

**Risk**: Every object migrated from v1 gets verified on first access after migration — potentially mass re-verification on next sync.

**Recommendations**:

- Stale strategy should treat `verify_time = 0` as "stale" and trigger verification
- Consider staggering first-access verification across maintenance windows to avoid latency spike
- Document that first sync after v1→v2 migration may be slower due to verification catch-up

#### 1.11 Orchestration State V1→V2 Decode Migration

**Issue**: `decode_state()` in the conductor state model only handled V2 envelope format after the V2 persistence migration, breaking backward compatibility with persisted V1 orchestration state envelopes.

**Fix**: Version dispatch added to `decode_state()`:

1. Parse raw JSON `version` field
2. V2 → existing CAS-ref path
3. V1 → inline-instance path (deserialize V1 envelope, convert each instance via `tool_call_instance_v1_v2_iso` then `tool_call_instance_v2_iso`, return with `latest::VERSION`)
4. Unknown version → error

**Self-healing**: After V1→V2 decode, state is re-persisted via `persist_and_publish_state()` which calls `encode_state()` (always V2). Subsequent loads use the V2 path.

**Risk**: Low. V1→V2 migration is a one-time decode cost per stale state blob. No data loss — the ISO bridges preserve all V1 fields.

#### 1.12 Reconstructed-Bytes Cache Invalidation on Delete/Prune

**Issue**: When an object is deleted or pruned from storage, its `reconstructed_bytes_cache` entry in `FileSystemState` persists until TTL expiry, potentially serving stale or dangling references.

**Current Spec**: "Entries are evicted when the underlying object is deleted or pruned"

**Gap**: Eviction trigger is specified but not how it is enforced — synchronous deletion on write path or lazy check on read?

**Risk**: A concurrent `get(hash)` after `delete(hash)` could hit the cache and return stale data.

**Recommendations**:

- On `delete()`, synchronously remove the corresponding `reconstructed_bytes_cache` entry
- On `prune()`, clear all cache entries for pruned hashes (batch removal)
- Add a lazily-checked generation counter
- Test: "delete then get returns NotFound rather than cached bytes"

#### 1.13 Concurrent get() Race in reconstructed_bytes_cache Fill

**Issue**: Two concurrent `get(hash)` calls both miss the cache and both reconstruct the same object, duplicating work.

**Current Spec**: Not specified

**Gap**: No concurrency control for the cache-fill path

**Risk**: Unnecessary double-reconstruction — doubled latency and I/O.

**Recommendations**:

- Use a per-hash lock (e.g., `HashMap<Hash, Mutex<()>>`) to serialize reconstruction for the same hash
- First caller reconstructs and populates cache; second caller finds cache hit
- Never hold multiple hash locks simultaneously (deadlock avoidance)
- Test: "concurrent get() same hash reconstructs only once"

#### 1.14 Stale Strategy with verify_time = 0

**Issue**: Overlaps with 1.10 but focuses on Stale strategy interaction with `verify_time = 0` in production workloads.

**Scenarios**:

- Large library sync after v1→v2 migration: every object triggers Stale verification
- Mixed environment: some objects have `verify_time` from a previous runtime, some are 0
- After `repair_index()`, all objects reset to `verify_time = 0`

**Current Spec**: Stale triggers when `now - verify_time > timeout`; `0` is treated as "infinitely stale"

**Gap**: Stale strategy does not distinguish between "just written with `verify_time = 0`" and "verified yesterday but now stale"

**Risk**: Mass re-verification after index rebuild or migration

**Recommendations**:

- Clarify: `verify_time = 0` always triggers Stale (equivalent to "never verified")
- Document that index rebuild resets all `verify_time` to 0
- Consider a grace-period parameter: if `verify_time = 0` and object age (file mtime) < grace period, skip Stale verification

#### 1.15 Sample Strategy Determinism Across Restarts

**Issue**: The Sample strategy uses randomness to select which objects to verify. Non-deterministic sampling means the same object may be sampled repeatedly or never, depending on restart state.

**Current Spec**: "Verify a random fraction (default 1%) of recently-fetched objects"

**Gap**: No determinism guarantee; sampling is not reproducible

**Risk**: Unpredictable coverage; hard to audit or test

**Recommendations**:

- Use hash-derived seed (e.g., `hash.bytes[..8]` as u64 seed) so sampling is deterministic per object
- Document that Sample strategy is per-access probabilistic, not per-object guaranteed
- Provide `sample_seed` config option to override the derivation
- Test: "same hash sampled consistently across runs"

#### 1.16 verify_time Interaction with Delta Chain Reconstruction

**Issue**: Delta chain reconstruction produces a full object from base + deltas. The reconstructed object's `verify_time` is ambiguous.

**Current Spec**: Not specified

**Gap**: No rule for what `verify_time` means on a delta-reconstructed object

**Risk**: Stale strategy may re-verify unnecessarily or miss verification.

**Recommendations**:

- Reconstruction should not modify any chain member's `verify_time`
- The reconstructed object's `verify_time` should be the minimum of all chain members' `verify_time` (most conservative)
- Or use reconstruction timestamp when reconstructing for read (do not persist verify_time to disk)

#### 1.17 System Clock Jump (verify_time > now)

**Issue**: If the system clock jumps backward, `verify_time` may be greater than `now`, causing nonsensical duration calculations.

**Current Spec**: None — assumes monotonic time

**Gap**: No handling for `verify_time > now`

**Risk**: Stale strategy computes `now - verify_time` as a negative duration, which underflows to a very large positive value, triggering unnecessary verification on every access.

**Recommendations**:

- Clamp `now - verify_time` to `Duration::ZERO` when `verify_time > now` (treat as "just verified")
- Log a warning when clock skew is detected
- Optionally reset `verify_time` to `now` on clock skew detection
- Test: "clock jumps backward → no mass re-verification"

#### 1.18 Reconstructed-Bytes Cache Interaction with Verification

**Issue**: CAS has a single caching layer — `reconstructed_bytes_cache` that holds fully-reconstructed object bytes with a 3600s TTL. No separate integrity-result cache exists.

**Current Spec**: "Reconstructed-object bytes are cached with a TTL of 3600s"

**Gap**: No ordering rule between cache lookup and verification.

**Risk**: Returning cached bytes without re-verifying could mask silent corruption that occurred after the cache entry was created.

**Recommendations**:

- On `get()`: check verification strategies first (against file metadata). If verification triggers, re-read from disk and re-verify regardless of cache state.
- The cache serves only to avoid redundant delta-chain reconstruction, not to skip integrity checks.
- TTL expiry triggers re-read from disk, which triggers a fresh reconstruction and a fresh verification decision.
- Test: "object in reconstructed_bytes_cache but stale mtime → re-verified on get"

#### 1.19 Concurrent GC During Step Execution

GC sweep runs concurrently with workflow step execution. If a step materializes a new CAS object between `list_all_hashes()` and the actual deletion in `gc_sweep()`, the new object won't be in the initial hash set and won't be deleted.

**Mitigation 1 — Recently-written set**: Sweep computes the set difference
`all_hashes - roots` at the start of the sweep, then excludes hashes in the
`recently_written` set (populated before the durable index write, so any
object that existed at `list_all_hashes` time is covered). Objects added
during sweep execution are not in `all_hashes` and are therefore not deleted.
The sweep is eventually consistent: the next sweep pass will catch any
orphans missed due to concurrent modification.

**Mitigation 2 — `gc_in_progress` guard**: `FileSystemCas::gc_sweep()` uses a
`compare_exchange` guard on `gc_in_progress: AtomicBool` to prevent multiple
concurrent sweep invocations from racing on the index and recently-written
set (same pattern as `optimize_in_progress`). Concurrent calls are rejected
with `CasError::invalid_input`.

#### 1.20 GC vs Active State Pointer

The root set includes `state.state_pointer` and all instance output pointers. If the state pointer changes during GC (e.g., a concurrent workflow commit), the sweep might delete objects referenced by the old state pointer but not the new one.

**Mitigation**: The background GC loop bypasses the actor mailbox and reads the state pointer + current state directly from the shared `StateStoreClient`. The state store actor processes these reads sequentially with workflow commits (same single-threaded mailbox), so each read returns a consistent snapshot.

**Remaining risk (accepted)**: If a workflow commit advances the state pointer between a state snapshot and the next GC cycle, the next sweep uses the old external_data roots (because they came from that snapshot's `state.external_data`). This is bounded: the next GC cycle will use the latest state snapshot.

#### 1.21 Background GC Loop

The conductor node actor spawns a single background GC loop in `pre_start`. The loop:

1. **Phase 1 — Wait for initialization**: Spin-waits on the `gc_initialized` flag. This flag is set after the first successful `LoadResolvedState` or `ReplaceResolvedState` call gives the state a populated `external_data` field.
2. **Phase 2 — Periodic GC**: Enters a loop that loads the current state (whose `external_data` carries root hashes), reads state pointer from the state store actor, calls `run_cas_gc_sweep()`, then sleeps `GC_INTERVAL_SECONDS` (3600).

**Mailbox bypass**: The revised loop calls `run_cas_gc_sweep()` directly from the background task, not through the actor mailbox. The `RunGc` handler is preserved for CLI use.

**Race fixed (2026-06-07)**: Previously, the loop fired `RunGc` immediately in Phase 1 before any state was loaded. With empty `external_data`, the CAS sweep computed an empty root set and deleted all objects not protected by `recently_written`. The two-phase approach eliminates this race by deferring the first GC until after external_data roots are populated.

**Race fixed (2026-06-07) — progress bar blocked by GC**: Previously, the background loop sent `RunGc` through the actor mailbox, blocking `SubmitWorkflow`. The mailbox bypass eliminates this ordering hazard.

#### 1.22 Cross-Crate StringList Hash Drift (RESOLVED)

**Issue**: Four independent implementations of the same composite hash pattern (`blake3(elem₁ ‖ elem₂ ‖ …)` for StringList-to-hash) were spread across the codebase — any future edit to one could silently drift from the others.

**Affected sites (pre-fix)**:

| Site | File | Pattern |
|------|------|---------|
| `ResolvedInput::from_string_list()` | `src/conductor/src/model/state/mod.rs` | `blake3::Hasher` loop |
| `resolve_list_input_binding_hash_only()` | `src/conductor/src/orchestration/actors/step_worker/mod.rs` | `blake3::Hasher` loop |
| `persist_resolved_list_input()` | `src/conductor/src/orchestration/actors/step_worker/mod.rs` | `blake3::Hasher` loop |
| materializer StringList arm | `src/mediapm/src/materializer/resolve.rs` | `blake3::Hasher` loop |

**Root cause (original sync bug)**: The conductor's `resolve_list_input_binding_hash_only()` previously mixed positional index into element hashes, while the materializer used plain `blake3(elem)`. Empty lists matched accidentally (both produce `blake3("")`), but non-empty lists produced different hashes.

**Resolution (`Hash::composite`)**: Extracted the canonical `Hash::composite(&[Hash]) → Hash` into the CAS crate and updated all 4 sites to use it.

**Design notes**:

- `Hash::composite` does not accept raw bytes or length prefixes — callers must pre-hash each element with `Hash::from_content`.
- Empty slices produce a deterministic but distinct hash (`blake3("")`).
- Single-element `Hash::composite(&[h])` wraps the element hash without identity — the output differs from the element hash itself.

#### 1.23 Composite Hash Across Conductor/Materializer

The materializer's StringList arm does **not** parse `${...}` interpolation; any workflow with interpolated StringList elements silently produces different hashes than if the literal `${...}` text were resolved. This was analyzed and explicitly skipped — the materializer lacks env-var context for full interpolation support, and cross-crate coupling risk outweighs benefit given that the deduplication via `Hash::composite` already eliminates future drift risk.

#### 1.24 Streaming (`get_stream`) Edge Cases

- **File deleted mid-stream**: Lazy stream fails on next chunk read with `CasError::Io`. Caller can retry after confirming hash via `contains()`.
- **Truncated file**: `read_exact` on last chunk yields `UnexpectedEof` → `CasError::Io`. Treat as integrity failure; fall back to `contains()` or repair.
- **Zero-length file**: Small-object fast path (≤256 KiB) handles cleanly; large-object path must check length before chunked loop, yield empty `Bytes` on zero length.
- **Concurrent writes during stream**: Immutable CAS objects by convention; concurrent writes represent integrity violation regardless of streaming.

#### 1.25 `materialize_to_path` Edge Cases

- **Destination exists**: `fs::copy` overwrites atomically. Caller should verify intent — accidental overwrite loses user edits.
- **Read-only parent**: Returns `CasError::Io` with `EACCES`. Validate write permissions before materialization.
- **Cross-device copy**: `fs::copy` falls back to read+write via VFS (no kernel fast-path).
- **Delta object fallback**: If requested hash is delta-only (no full object file), falls back to `get()` + `tokio::fs::write`. Fast path only applies to full objects.

#### 1.26 Memory Lifecycle (`Bytes` vs `Vec<u8>`) Edge Cases

- **Large object clone**: `Vec<u8>` clone is O(content_size); `Bytes` is O(1) ref-count increment. Use `Bytes` for all CAS data paths.
- **Zero-copy fast path**: `fs::copy(object_path, dest)` avoids `Bytes` allocation entirely for large materialization.
- **Stream vs full buffer**: Use `get()` for objects ≤256 KiB; `get_stream()` for larger or unknown-size objects.
- **`Bytes`→`Vec<u8>`**: Avoid conversion. Keep as `Bytes` as long as possible; convert only at API boundaries requiring `Vec<u8>`.

## Part 2: Additional CAS Specifications

### 2.1 Decision Rationale

#### Why CAS Instead of Named Files?

Every piece of data hashes to a Blake3-256 hash; store once, reference many. Identical content deduplicates automatically. Verification is hash recomputation. Trade-off: human-readable names are replaced by hashes — lock files map `(media_id, variant) → hash` to bridge this.

#### Why Actor-Based Orchestration for CAS?

Actors (ractor) serialize access to mutable storage/index state, guaranteeing no race conditions on index mutations without lock deadlocks. Slight message-round-trip latency trade-off for thread safety and bounded concurrency.

### 2.2 Performance Constraints

| Path | Constraint | Technique |
|---|---|---|
| CAS optimizer algorithm | Greedy scoring | Score all objects as delta candidates; cost = `delta_size + base_access_time`; top N=8 selected (configurable). Goal: balance encoding size vs reconstruction time. |
| Delta reconstruction cache | LRU, 1 GB max, 1h TTL | Reconstructed full objects cached in memory. Repeated delta-chain reads hit cache instead of re-reconstructing. |

### 2.3 Testing Requirements

**Delta Chain Robustness** — Add `tests/e2e/delta_chain_robustness.rs`:

- [ ] Corrupted delta → recovery path
- [ ] Orphaned deltas (deleted base) → integrity check detects
- [ ] Chain exceeding `MAX_DEPTH` after config change → pruning triggered
- [x] Concurrent optimization + delete → no race condition (two-phase staging)
- [ ] Out-of-space + prune + retry → succeeds

### 2.4 Troubleshooting

#### CAS returns NotFound for a Hash I Just Stored

| Symptom | Cause | Resolution |
|---|---|---|
| `CAS NotFound(hash)` after `put()` | Hash computation mismatch | `assert_eq!(Hash::from_bytes(&data)?, cas.put(data).await?)` |
| Same error | Wrong CAS instance (e.g., `InMemoryCas` vs `FileSystemCas`) | Use same instance for put + get |
| Same error | Index corruption (rare) | Run `repair_index()` |

### 2.5 Implementation Checklist: New CAS Backend

- [ ] Implement `CasApi` (put, get, contains, delete)
- [ ] Implement `CasMaintenanceApi` (optimize, prune, repair, gc_sweep)
- [ ] Include error types: `NotFound`, `OutOfSpace`
- [ ] Property tests: `put(x) → get() == x` determinism
- [ ] Stress tests: concurrent puts, hash collisions
- [ ] Benchmark against `FileSystemCas`
- [ ] Document resource limits and O(1)/O(log n) guarantees
- [ ] Update this file with backend comparison

### 2.6 Extension Points

- **New hash algorithms**: Add variant to `HashAlgorithm` enum, implement multihash trait, update multicodec code table.
- **Index-backed existence checks**: Design proposal — `IndexState::contains(&self, hash) → bool` with false-negative tolerance (miss falls back to storage), no false positives. Populated lazily on first check, updated incrementally. See `future-extensions.md` for full API spec.

### 2.7 Cross-Crate: CAS Versioning vs Conductor Versioning (cf. §6.1)

CAS internal object format version is independent of Conductor config version. No cross-crate version coupling. Both must carry explicit version markers, but they evolve independently with separate migration bridges.

### 2.8 Ambiguity Resolved: Index Repair Semantics (§7.6)

`repair_index()` updates the on-disk index to the current schema version and removes orphaned entries. It does **not** re-hash objects — only metadata is updated. Original object data is untouched. This is an in-place update, not a full rebuild.

## Architecture Diagram

```mermaid
graph TD
    subgraph "CAS Crate"
        API[Public API<br/>CasApi, CasMaintenanceApi]
        HASH[hash module<br/>Blake3, multihash]
        STORE[storage module<br/>FileSystemCas, InMemoryCas]
        INDEX[index module<br/>Persistence, repair]
        CODEC[codec module<br/>Versioned encode/decode]
        ORCH[orchestration module<br/>Actor-based coordination]
        CLI[cli module]
        ERROR[error module]
    end

    API --> HASH
    API --> STORE
    API --> INDEX
    API --> CODEC
    API --> ORCH
    API --> CLI
    API --> ERROR
    STORE --> INDEX
    ORCH --> STORE
    ORCH --> INDEX
    CODEC --> STORE

    subgraph "External Dependencies"
        B3[blake3]
        SERDE[serde]
        TOKIO[tokio]
        RACTOR[ractor]
    end

    HASH --> B3
    INDEX --> SERDE
    API --> TOKIO
    ORCH --> RACTOR
```
