# CAS agent guide

`mediapm-cas` is a content-addressable blob store with delta-compression hints. Core contract: `put(bytes) -> Hash`, `get(hash) -> Bytes`, `stat(hash) -> ObjectMeta`, `delete(hash)`, `flush() -> u64`, `put_stream(reader) -> Hash`, `get_to_writer(hash, writer)`. Backends deduplicate identical content by BLAKE3-256 and serve as the storage foundation for Conductor and MediaPM.

## Hash and object layout

- `Hash([u8; 32])` in `src/mediapm-cas/src/hash.rs`: BLAKE3-256 content address. `Hash::from_content` hashes raw bytes; `Hash::composite` hashes concatenated digests for ordered composite identity, giving domain separation from raw-content hashing. Ordering is lexicographic on bytes.
- Empty-content sentinel: `Hash::empty()` is `blake3(b"")`. It is always present and indelible. `get`/`stat` answer empty content without backend lookup; `delete` is a no-op; constraints on empty are accepted but have no effect.
- Wire/storage form is multihash (`0x1e` blake3, 32-byte digest) via `multihash`. Serde format is human-readable `blake3:<hex>`.
- Filesystem object layout (`storage/blob_store/versions/v1.rs`): `<root>/v1/blake3/ab/cd/<remaining>` for full objects, `<remaining>.diff` for delta payloads. Directory fan-out keeps per-directory metadata aux files (`metadata-v1.json`, `keep-full-v1.json`).

## Public API

Source of truth: `src/mediapm-cas/src/api.rs`, `error.rs`, `hash.rs`.

- `CasApi` (7 methods): `put`, `get`, `stat`, `delete`, `flush`, `put_stream`, `get_to_writer`. `get` defaults to buffering `get_to_writer`; `put_stream` defaults to buffering into `put`; stores may override both for streaming. No `exists()`; use `get`/`stat` to avoid TOCTOU.
- Guarantees within one async task: write-then-read, delete-then-read, idempotent put/delete, crash survival after `Ok`. `flush()` materializes committed WAL entries for write-back backends and returns consumed entry count; write-through backends may return 0.
- `CasError` (`error.rs`): `NotFound`, `InvalidArgument`, `Internal`, `Io`, `CorruptObject { hash, details }`, `TooLarge { hash, size, limit }`, `LockContention { path }`. `TooLarge` is returned when a delta chain exceeds the max depth; normal `get` resolves through `get_to_writer`.
- `ConstraintApi`: `set_constraint`, `get_constraint`, `patch_constraint`, `mark_keep_full`, `is_keep_full`. Constraints are non-binding compression hints stored separately from object metadata. `mark_keep_full` is obeyed: marked objects stay full, may still be delta bases, survive reopen in filesystem stores, and are dropped with the object. Constraint target must be held by the store; bases must be distinct and cannot include target.
- `CasMaintenanceApi`: `run_maintenance_cycle() -> OptimizeReport`, `prune_constraints() -> PruneReport`, `list_hashes() -> Vec<Hash>`. `PruneReport` counts target removals, dead bases removed from live constraints, and dead keep-full markers separately.

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

`put` hashes content, then writes inline WAL `Put` for small objects or materializes blob and appends `PutLarge` for large objects. `delete` appends a WAL tombstone; physical removal is deferred. The background engine consumes WAL, materializes blob+metadata, advances checkpoint, trims WAL, and runs maintenance. Reads go metadata -> blob -> WAL fallback through `ComposedReadView`, reconstructing delta chains transparently. `flush` forces WAL consumption for write-back stores.

## Key invariants

- **Content identity**: same bytes produce same BLAKE3 hash. Only empty content produces `Hash::empty()`, which is indelible.
- **Crash safety and atomic writes**: WAL is the crash-safe commitment point; blob/metadata are derived and rebuildable by replay. Filesystem writes use atomic temp-write/rename patterns; metadata/keep-full aux files are versioned.
- **No TOCTOU**: no `exists()` API. `get`/`stat` return `NotFound` on miss.
- **Delta chain integrity**: delta reconstruction walks metadata/blob chain, rejects missing bases as `CorruptObject`, prevents cycles, and enforces `MAX_DELTA_CHAIN_DEPTH` (5). Objects over `DELTA_THRESHOLD` are not delta-compressed.
- **Object size limits**: `WAL_INLINE_LIMIT` selects inline WAL vs external `PutLarge`; `DELTA_THRESHOLD` caps delta eligibility; `TooLarge` reports delta-chain depth/size limit failures.
- **Constraint invariants**: constraint sets are independent from object metadata; no target/base self-reference; `get_constraint` returns empty set when absent. `prune_constraints` removes dead targets, dead bases in live constraints, and dead keep-full markers. Keep-full markers are durable in filesystem metadata, dropped with object deletion, and cause optimizer to skip delta rewrite.
- **Codec versioning**: delta envelope writers emit current V3; legacy V1/V2 are read-only. New formats live in versioned modules and keep boundary guards.
- **Delete semantics, no dangling deltas**: deleting a base re-materializes dependents as full before physical base removal. Delete does not cascade to unrelated hashes.

## Wrapper pattern and integration

- `InMemoryCas` and `FileSystemCas` are `Deref` wrappers around `CasStore<...>`. Trait impls are blanket over `Deref<Target = CasStore<..>>` in `store.rs`; `ConfiguredCas` forwards by enum dispatch.
- Cross-crate contract: CAS is thread-safe for concurrent callers, has no Conductor/MediaPM type dependencies, and propagates `CasError` as-is. Conductor uses `CasApi + CasMaintenanceApi + ConstraintApi` bounds; MediaPM stores records keyed by CAS hashes.
- Build/test pointers: `cargo test -p mediapm-cas`, `cargo clippy -p mediapm-cas`, `cargo build -p mediapm-cas`, `cargo build -p mediapm-cas --no-default-features`. Tests use in-memory stores; streaming/large-file coverage lives in `tests/int/streaming_large.rs`.
