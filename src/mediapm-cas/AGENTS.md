# CAS agent guide

`mediapm-cas`: content-addressed blob store with delta hints. Contract: `put(bytes) -> Hash`, `get(hash) -> Bytes`, `stat(hash) -> ObjectMeta`, `delete(hash)`, `flush() -> u64`, `put_stream(reader) -> Hash`, `get_to_writer(hash, writer)`. BLAKE3-256 dedup for Conductor/MediaPM.

## Hash and object layout

- `Hash([u8; 32])` (`src/mediapm-cas/src/hash.rs`) BLAKE3-256; `Hash::from_content` raw bytes, `Hash::composite` concatenated digests (domain-separated ordered identity); lexicographic bytes.
- `Hash::empty()` = `blake3(b"")`, indelible: `get`/`stat` answer without lookup, `delete` no-op, constraints inert.
- Multihash (`0x1e` blake3 + 32-byte digest) via `multihash`; serde `blake3:<hex>`.
- Filesystem (`storage/blob_store/versions/v1.rs`): `<root>/v1/blake3/ab/cd/<remaining>` full, `<remaining>.diff` delta; one `metadata-v3.json` per fan-out dir with `delta_bases`, `lengths`, `corrupted`, oldest-first V1/V2 overlay. Index holds lengths/constraints; physical form is a tree property.

## Public API

Source `src/mediapm-cas/src/api.rs`, `error.rs`, `hash.rs`.

- `CasApi` 7 methods (`put`, `get`, `stat`, `delete`, `flush`, `put_stream`, `get_to_writer`); `get` buffers `get_to_writer`, `put_stream` buffers `put`, stores may override. No `exists()` (TOCTOU).
- Per async task: write-then-read, delete-then-read, idempotent put/delete, crash-safe after `Ok`. `flush()` materializes write-back WAL, returns consumed entries; write-through may 0.
- `CasError`: `NotFound`, `InvalidArgument`, `Internal`, `Io`, `CorruptObject { hash, details }`, `TooLarge { hash, size, limit }`, `LockContention { path }`; `TooLarge` for delta depth, normal `get` via `get_to_writer`.
- `ConstraintApi`: `set_constraint`, `set_constraint_with`, `get_constraint`, `patch_constraint`. Constraint = allowed bases + full storage; non-empty bases restrict, empty/absent open; full = `set_constraint(target, {empty})`. `set_constraint_with` takes `CasDurability`; `{empty}` hard, `STORE`/`METADATA` add fsyncs. Target held, bases distinct and not target.
- `CasMaintenanceApi`: `run_maintenance_cycle() -> OptimizeReport`, `prune_constraints() -> PruneReport`, `list_hashes() -> Vec<Hash>`, `list_corrupted_objects() -> Vec<Hash>`; `OptimizeReport.corrupted_objects` (`OptimizeReport`, `corrupted_objects`); `PruneReport` separates target removals; `mediapm-cas corrupted` lists records.

## Backend, config, structure

Source `src/mediapm-cas/src/config.rs`, `defaults.rs`, `storage/in_memory.rs`, `storage/file_system.rs`.

- `InMemoryCas`: `CasStore<InMemoryWal, InMemoryMetadataStore, InMemoryBlobStore>`; `FileSystemCas`: `CasStore<FileWal, FileSystemMetadataStore, FileSystemBlobStore>`, `open()` two-layer fail-fast lock (`DashMap` + `try_lock_owned`, `fs4` file), canonicalized paths.
- `ConfiguredCas` enum dispatch; `CasConfig` = `storage_locator: CasStorageLocator` + `integrity: CasIntegrityConfig`; `from_locator`, `from_locator_with_options`, `open()`. Types `InMemory`/`FileSystem` via `CasStorageLocator::{InMemory, FileSystem{path}}`, `CasIntegrityConfig { verify_on_read }`, `CasLocatorParseOptions`, `VerifyTriggerStrategy::{Always, Modified, Sample{denominator}, Stale{timeout}}`. Tunables in `defaults.rs` (`WAL_INLINE_LIMIT`, `DELTA_THRESHOLD`, `CACHE_*`, `WAL_MAX_SEGMENT_SIZE`, `OBJECT_STREAM_BUFFER_SIZE`), never duplicated.
- Public `api`, `config`, `defaults`, `error`, `hash`, `storage`, `background`, feature `cli`, re-exports `lib.rs`; internal `delta/`, `io_gate`, `verify`; storage `store.rs` (`CasStore<J,I,B>` blanket `Deref`), `wal/` (`mem_wal`, `file_wal`, versioned), `blob_store/`, `metadata_store/`, `read_view.rs`, `pending_ops.rs`, `bg_engine.rs`, `reconstructed_cache.rs`, `directory_lock.rs`; codecs under `delta/versions/`, outsiders use `delta::versions`.

## Data flow

`put` hashes then inline WAL `Put` or blob + `PutLarge`; `delete` tombstone, deferred physical removal; background consumes WAL, materializes blob/metadata, advances checkpoint, trims WAL, maintains (delta optimization, blob-form collapse, constraint pruning, first-iteration legacy sweep). Corruption: timestamp-free set; collapse/read-time detection record sole corrupt form; fallback/repair and `read_full_bytes` clear best-effort, collapse authoritative; clear failure warns and serves verified bytes; file + index remain so reads still `CorruptObject`. Reads metadata -> blob -> WAL via `ComposedReadView`; `flush` forces WAL consumption.

## Key invariants

- Same bytes -> same BLAKE3; only empty -> `Hash::empty()`, indelible. WAL crash-safe commitment, blob/metadata replayable, atomic temp-write/rename, versioned aux files. No `exists()`; misses `NotFound`. Blob forms at most `<hash>` (`full`) and `<hash>.diff` (`delta`): neither -> index dropped, either -> exists; both only in transition, collapsed toward recorded `encoding` (`Full` deletes delta, `Delta` deletes full after reconstruction). Corrupt indexed form -> serve other, warn, best-effort clear; sole corrupt read records hash; collapse repairs.
- Delta reconstruction rejects missing bases as `CorruptObject`, prevents cycles, enforces `MAX_DELTA_CHAIN_DEPTH` (5); objects over `DELTA_THRESHOLD` not delta-compressed. `WAL_INLINE_LIMIT` picks inline vs `PutLarge`; `TooLarge` reports depth/size failures.
- `{Hash::empty()}` hard, enforced in `set_constraint` (materializes full before return); other sets soft, converge at maintenance; absent/empty open; metadata-sync install writes one snapshot.
- Writers emit V3 codec, V1/V2 readable and re-encoded on first verified read; new formats versioned with boundary guards. Deleting a base re-materializes dependents full first; no unrelated cascade.
- `InMemoryCas`/`FileSystemCas` are `CasStore<...>` `Deref<Target = CasStore<..>>` wrappers with blanket impls in `store.rs`; `ConfiguredCas` enum forwarding. Thread-safe, no Conductor/MediaPM types, `CasError` unchanged; Conductor bounds `CasApi + CasMaintenanceApi + ConstraintApi`, MediaPM records keyed by hash.
- Tests: `cargo test -p mediapm-cas`, `cargo clippy -p mediapm-cas`, `cargo build -p mediapm-cas`, `cargo build -p mediapm-cas --no-default-features`; in-memory stores, streaming `tests/int/streaming_large.rs`.
