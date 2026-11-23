# CAS agent guide

`mediapm-cas` is a content-addressable blob store with delta-compression hints. Core contract: `put(bytes) -> Hash`, `get(hash) -> Bytes`, `stat(hash) -> ObjectMeta`, `delete(hash)`, `flush() -> u64`, `put_stream(reader) -> Hash`, `get_to_writer(hash, writer)`. Backends deduplicate identical content by BLAKE3-256 and serve as the storage foundation for Conductor and MediaPM.

## Hash and object layout

- `Hash([u8; 32])` in `src/mediapm-cas/src/hash.rs`: BLAKE3-256 content address. `Hash::from_content` hashes raw bytes; `Hash::composite` hashes concatenated digests for ordered composite identity, giving domain separation from raw-content hashing. Ordering is lexicographic on bytes.
- Empty-content sentinel: `Hash::empty()` is `blake3(b"")`. It is always present and indelible. `get`/`stat` answer empty content without backend lookup; `delete` is a no-op; constraints on empty are accepted but have no effect.
- Wire/storage form is multihash (`0x1e` blake3, 32-byte digest) via `multihash`. Serde format is human-readable `blake3:<hex>`.
- Filesystem object layout (`storage/blob_store/versions/v1.rs`): `<root>/v1/blake3/ab/cd/<remaining>` for full objects, `<remaining>.diff` for delta payloads. Directory fan-out keeps one per-directory index file, `metadata-v3.json`, carrying `delta_bases`, `lengths`, and `corrupted`, with V1 and V2 snapshots readable through the oldest-first overlay. The index records payload lengths and constraints; which physical form an object has is a property of the tree.

## Public API

Source of truth: `src/mediapm-cas/src/api.rs`, `error.rs`, `hash.rs`.

- `CasApi` (7 methods): `put`, `get`, `stat`, `delete`, `flush`, `put_stream`, `get_to_writer`. `get` defaults to buffering `get_to_writer`; `put_stream` defaults to buffering into `put`; stores may override both for streaming. No `exists()`; use `get`/`stat` to avoid TOCTOU.
- Guarantees within one async task: write-then-read, delete-then-read, idempotent put/delete, crash survival after `Ok`. `flush()` materializes committed WAL entries for write-back backends and returns consumed entry count; write-through backends may return 0.
- `CasError` (`error.rs`): `NotFound`, `InvalidArgument`, `Internal`, `Io`, `CorruptObject { hash, details }`, `TooLarge { hash, size, limit }`, `LockContention { path }`. `TooLarge` is returned when a delta chain exceeds the max depth; normal `get` resolves through `get_to_writer`.
- `ConstraintApi`: `set_constraint`, `set_constraint_with`, `get_constraint`, `patch_constraint`. A constraint answers two concerns: which objects a target may be compressed against, and whether the target is stored full. A non-empty base set restricts the choice; an empty or absent set leaves it open; storing a target full is `set_constraint(target, {empty})`, not a separate marker. `set_constraint_with` takes `CasDurability`; `{empty}` is hard regardless, and `STORE`/`METADATA` add the fsyncs a caller may need before it can use the bytes. Constraint target must be held by the store; bases must be distinct and cannot include target.
- `CasMaintenanceApi`: `run_maintenance_cycle() -> OptimizeReport`, `prune_constraints() -> PruneReport`, `list_hashes() -> Vec<Hash>`, `list_corrupted_objects() -> Vec<Hash>`. `OptimizeReport` reports `corrupted_objects`; `PruneReport` counts target removals and dead bases removed from live constraints separately. Corruption records are maintained by the collapse pass and by read-time detection; the `mediapm-cas corrupted` subcommand lists known corrupt objects.

## Backend and config types

Source of truth: `src/mediapm-cas/src/config.rs`, `defaults.rs`, `storage/in_memory.rs`, `storage/file_system.rs`.

- `InMemoryCas`: ephemeral newtype over `CasStore<InMemoryWal, InMemoryMetadataStore, InMemoryBlobStore>`.
- `FileSystemCas`: persistent newtype over `CasStore<FileWal, FileSystemMetadataStore, FileSystemBlobStore>`. `open()` takes a two-layer fail-fast directory lock (in-process `DashMap` + `try_lock_owned`, inter-process `fs4` lock file). Paths are canonicalized before lock checks.
- `ConfiguredCas` dispatches `InMemory`/`FileSystem` and forwards CAS traits. `CasConfig` holds `storage_locator: CasStorageLocator` and `integrity: CasIntegrityConfig`; build with `from_locator`, `from_locator_with_options`, then `open()`.
- Config types: `CasStorageLocator::{InMemory, FileSystem{path}}`, `CasIntegrityConfig { verify_on_read }`, `CasLocatorParseOptions`, `VerifyTriggerStrategy::{Always, Modified, Sample{denominator}, Stale{timeout}}`.
- Tunable defaults live in `defaults.rs` (`WAL_INLINE_LIMIT`, `DELTA_THRESHOLD`, `CACHE_*`, `WAL_MAX_SEGMENT_SIZE`, `OBJECT_STREAM_BUFFER_SIZE`). Do not duplicate values outside that module.

## Crate structure

- Public: `api`, `config`, `defaults`, `error`, `hash`, `storage`, `background`; feature-gated `cli`; re-exports in `lib.rs`.
- Internal: `delta/` (patch/object/versioned envelopes), `io_gate`, `verify`.
- Storage: `store.rs` composes `CasStore<J,I,B>` and implements traits by blanket `Deref`; `wal/` (`mem_wal`, `file_wal`, versioned formats); `blob_store/`; `metadata_store/`; `read_view.rs`; `pending_ops.rs`; `bg_engine.rs`; `reconstructed_cache.rs`; `directory_lock.rs`.
- Delta codecs live under `delta/versions/`; code outside that boundary uses `delta::versions` APIs only.

## Data flow

`put` hashes content, then writes inline WAL `Put` for small objects or materializes blob and appends `PutLarge` for large objects. `delete` appends a WAL tombstone; physical removal is deferred. The background engine consumes WAL, materializes blob+metadata, advances checkpoint, trims WAL, and runs maintenance, whose phases are: delta optimization, blob-form collapse, constraint pruning, and on the first iteration a legacy snapshot sweep. Corruption records (a set, no timestamp) are maintained by the collapse pass and by read-time detection: a sole corrupt form read records the hash, while a verified fallback read or a repair clears it best-effort. A clear failure on a verified read logs a warning and still serves the bytes. A `read_full_bytes` recovery clear in maintenance helpers is also best-effort and logs a warning; collapse-pass clear propagation remains authoritative. Both files and the index entry remain, so reads keep returning `CorruptObject`. `OptimizeReport.corrupted_objects` and `mediapm-cas corrupted` surface that record. Reads go metadata -> blob -> WAL fallback through `ComposedReadView`, reconstructing delta chains transparently. `flush` forces WAL consumption for write-back stores.

## Key invariants

- **Content identity**: same bytes produce same BLAKE3 hash. Only empty content produces `Hash::empty()`, which is indelible.
- **Crash safety and atomic writes**: WAL is the crash-safe commitment point; blob/metadata are derived and rebuildable by replay. Filesystem writes use atomic temp-write/rename patterns; metadata aux files are versioned.
- **No TOCTOU**: no `exists()` API. `get`/`stat` return `NotFound` on miss.
- **Blob forms**: one content address has at most two physical forms, `<hash>` (full) and `<hash>.diff` (delta). Neither means the data does not exist, in which case the index entry is dropped at load. Either means it does. Both are legitimate only while a transition is in flight and are collapsed toward the recorded `encoding`: intent `Full` deletes the delta, intent `Delta` deletes the full payload once the delta reconstructs the target. A read that finds the indexed form corrupt serves the other, logs a warning, and clears any corruption record for that hash best-effort; a clear failure still serves the verified bytes and leaves the record for the collapse pass. A sole corrupt form read records the hash. Physical repair is the collapse pass's job.
- **Delta chain integrity**: delta reconstruction walks metadata/blob chain, rejects missing bases as `CorruptObject`, prevents cycles, and enforces `MAX_DELTA_CHAIN_DEPTH` (5). Objects over `DELTA_THRESHOLD` are not delta-compressed.
- **Object size limits**: `WAL_INLINE_LIMIT` selects inline WAL vs external `PutLarge`; `DELTA_THRESHOLD` caps delta eligibility; `TooLarge` reports delta-chain depth/size limit failures.
- **Constraint strength**: `{Hash::empty()}` is hard and is enforced inside `set_constraint`, which materializes the full payload before returning. Every other base set is soft and converges at a maintenance pass. An absent or empty set leaves base choice open. A metadata-syncing constraint install writes one snapshot.
- **Codec versioning**: delta envelope writers emit current V3; legacy V1/V2 are readable. A legacy V1/V2 delta envelope read by a verifying store is re-encoded in the current format on first read. New formats live in versioned modules and keep boundary guards.
- **Delete semantics, no dangling deltas**: deleting a base re-materializes dependents as full before physical base removal. Delete does not cascade to unrelated hashes.

## Wrapper pattern and integration

- `InMemoryCas` and `FileSystemCas` are `Deref` wrappers around `CasStore<...>`. Trait impls are blanket over `Deref<Target = CasStore<..>>` in `store.rs`; `ConfiguredCas` forwards by enum dispatch.
- Cross-crate contract: CAS is thread-safe for concurrent callers, has no Conductor/MediaPM type dependencies, and propagates `CasError` as-is. Conductor uses `CasApi + CasMaintenanceApi + ConstraintApi` bounds; MediaPM stores records keyed by CAS hashes.
- Build/test pointers: `cargo test -p mediapm-cas`, `cargo clippy -p mediapm-cas`, `cargo build -p mediapm-cas`, `cargo build -p mediapm-cas --no-default-features`. Tests use in-memory stores; streaming/large-file coverage lives in `tests/int/streaming_large.rs`.
