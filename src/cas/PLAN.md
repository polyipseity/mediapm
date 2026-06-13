# CAS Specification

> Generated 2026-06-13 from full source audit. Every statement is part of
> the behavioral contract — code must match.

---

## 1. Hash (`Hash`)

`Hash([u8; 32])` — blake3-256 content address.

- **Content-addressed**: `Hash::from_content(data)` = blake3(data). Same data,
  same hash. Collision resistance per blake3.
- **Zero sentinel**: `Hash::zero()` = `[0u8; 32]`. Special sentinel meaning
  "full encoding acceptable" in constraint bases. **Never stored**: put is
  no-op (returns hash but stores nothing), get/stat return NotFound, delete
  is no-op.
- **Wire format**: multihash encoding `[0x1e, 0x20, 32-byte digest]` for
  persistence; hex for display.

---

## 2. CasApi — Public Contract

### 2.1 `put(data: Bytes) -> Result<Hash>`

Store data by content hash.

1. `hash = Hash::from_content(&data)`.
2. If `hash.is_zero()`: return Ok(hash) immediately — nothing stored.
3. Append `JournalEntry::Put { hash, data }` to WAL (crash-safe commit).
4. Notify cache: `hint_state_change(hash, Some(data))`.
5. Return hash.

**Postcondition**: `get(hash)` returns data immediately (via journal
fallback before WAL consumer materializes it). `stat(hash)` returns metadata.

**Idempotent**: same data twice = same effect. Duplicate WAL entries are
safe (ObjectStore put is idempotent).

### 2.2 `get(hash: Hash) -> Result<Bytes>`

Retrieve bytes by hash. Always returns the original data regardless of
storage encoding. Never exposes delta encoding to caller.

1. If `hash.is_zero()`: return NotFound.
2. Three-layer lookup via ReadView:
   - L1: In-memory cache (TTL 60s).
   - L2: ObjectStore.
   - L3: Journal fallback (pending entries not yet materialized).
3. If any layer returns reconstructed bytes, return them.
4. If absent, return NotFound.

**Delta reconstruction** (transparent — caller never sees it):

- If ObjectStore entry's encoding is `Delta { base_hash }`, the stored bytes
  are a VCDIFF delta envelope.
- Fetch base: `get(base_hash)` (recursive, goes through same three-layer
  lookup).
- Apply `DeltaPatch::apply(base_bytes, vcdiff)` → original bytes.
- Cache reconstructed result.

**Invariant**: If a hash exists in the store, get() returns its original
content. Full encoding is equivalent to Delta encoding — caller cannot
distinguish.

### 2.3 `stat(hash: Hash) -> Result<ObjectMeta>`

Return metadata (payload_len, encoding). Encoding field is informational —
callers must NEVER make decisions based on it.

1. If `hash.is_zero()`: return NotFound.
2. Check ObjectStore (lookup includes stored encoding), then journal
   fallback.
3. Return NotFound if absent.

**Encoding = Full**: object stored as raw bytes.
**Encoding = Delta { base_hash }**: object stored as delta, transparently
reconstructed by get(). This is a storage optimization hint — NOT a
dependency guarantee. base_hash may be deleted; get() still works.

### 2.4 `delete(hash: Hash) -> Result<()>`

Logical deletion. Acts as-if deleted from CasApi perspective — `get()` and
`stat()` return NotFound immediately after this returns. Physical removal is
deferred.

1. If `hash.is_zero()`: return Ok(()) — no-op.
2. Append `JournalEntry::Delete { hash }` to WAL (crash-safe).
3. Tombstone the cache: `hint_state_change(hash, None)`.
4. Return Ok(()).

**Postcondition**: `get(hash)` and `stat(hash)` return NotFound immediately
(cache tombstone → journal fallback Tombstone → ObjectStore deletion if WAL
consumer already processed it).

**Does NOT cascade**: Deleting hash B has zero effect on any other hash.
Even if A was delta-compressed against B, A remains accessible because
A's delta bytes are stored under A's own content hash. The A→B dependency
is a reconstruction concern, not a storage concern. (Details in §8.)

**Idempotent**: double delete is safe.

---

## 3. ObjectStore — Internal Payload Backend

The ObjectStore is the materialized storage layer. The WAL consumer writes
into it; the ReadView reads from it.

### Storage format

Each entry stores:

- `data: Bytes` — the raw bytes. For Full encoding this is the original
  content. For Delta encoding this is the V3 delta envelope
  (magic + metadata + vcdiff payload).
- `encoding: ObjectEncoding` — metadata indicating Full or Delta{base_hash}.

For `InMemoryObjectStore`, this is `DashMap<Hash, (Bytes, ObjectEncoding)>`.

### Delta-aware operations

**put(hash, data)**: Overwrites whatever was stored. The optimizer calls this
to replace Full bytes with delta-encoded bytes. Idempotent.

**get(hash)**: Returns the stored bytes, regardless of encoding. The caller
(ReadView) is responsible for reconstruction if encoding is Delta.

**stat(hash)**: Returns payload_len (original content length) and encoding.

**delete(hash)**: Removes entry. Called by WAL consumer when processing a
Delete entry. After this, get returns None, stat returns None.

**list_hashes()**: Returns all hashes with entries.

**invariant—no dangling deltas**: The caller of delete() (WAL consumer)
guarantees that before removing a hash that's used as a base, all dependent
deltas are re-materialized. See §8.

---

## 4. Journal (WAL)

The journal is the **only crash-safe commitment point**. ObjectStore and
MetadataStore are derived — they can be rebuilt by replaying the journal.

### Entry types

- **Put { hash, data }**: Store data under hash.
- **Delete { hash }**: Logically delete hash.
- **Constraint { target, bases }**: Set delta-compression hints.

### PendingState semantics

- **Present(Bytes)**: Most recent entry for hash is a Put.
- **Tombstone**: Most recent entry for hash is a Delete.
- **NotPresent**: No entries for this hash.

The ReadView uses `check_pending` as the third lookup layer (L3) to find
data that hasn't been materialized to ObjectStore yet, and to respect
logical deletions that haven't been physically processed.

### WAL Consumer

The WAL consumer (`BackgroundEngine::run_wal_consumer()`) replays journal
entries from the checkpoint position, materializing each to ObjectStore or
MetadataStore:

- **Put**: `ObjectStore.put(hash, data)`.
- **Delete**: `ObjectStore.delete(hash)` (physical removal — see §8 for
  re-materialization guarantees).
- **Constraint**: `MetadataStore.set(target, bases)`.

After processing, entries are trimmed from the journal up to the last
processed position.

**Idempotent reprocessing**: Re-consuming already-processed entries is safe.
Puts overwrite, deletes are no-op if already removed.

---

## 5. ReadView — Read-Through Cache

### Three-layer lookup

For `get(hash)`:

1. **L1 — Cache**: DashMap with TTL (60s). If hash present and TTL valid,
   return cached data (Ok) or NotFound (Err). Fast path.
2. **L2 — ObjectStore**: `ObjectStore.get(hash)`. If found, cache and
   return. If the stored encoding is Delta, reconstruct before caching.
3. **L3 — Journal fallback**: `Journal.check_pending(hash)`. If Present,
   cache data and return. If Tombstone, cache None and return NotFound.

### Delta reconstruction in get()

When L2 ObjectStore returns delta-encoded bytes:

1. Decode V3 envelope → extract base_hash + vcdiff_bytes.
2. Recursive call: `self.get(&base_hash)` — goes through L1/L2/L3 again.
3. Apply `DeltaPatch::apply(base_bytes, vcdiff)` → reconstructed original.
4. Cache reconstructed bytes.
5. Return.

**Recursive get(base_hash)**: Because get() goes through the full three-layer
lookup, base_hash can be cached, in ObjectStore, or pending in journal.

**Corruption**: If base_hash is not found, the store is corrupted.
Return CorruptObject error.

### Concurrent read dedup

When two tasks call `get(hash)` simultaneously and both miss cache:

- First task inserts a PendingResult, performs fetch, notifies waiters.
- Second task finds the PendingResult entry, waits on Notify, gets shared
  result.

### Cache invalidation

Not proactive — uses TTL (60s) for eventual consistency. Acceptable because:

- Most mutable operations go through CasApi, which calls
  `hint_state_change()` to proactively update cache.
- Background operations (optimizer, WAL consumer) also call
  `hint_state_change()`.

---

## 6. ConstraintApi — Delta-Compression Hints

**Constraints are non-binding hints** — the system never blocks on their
completeness or accuracy.

### Effective constraints

`effective_bases(target) = stored_bases ∩ live_hashes`

When the optimizer evaluates a constraint, it intersects with currently live
hashes to get the effective set:

- `effective = ∅` → optimizer is free to store full or delta against any
  live hash.
- `effective = {zero()}` → full encoding is specifically acceptable.
- `effective ≠ ∅` and zero ∉ effective → optimizer may delta-compress
  against any base in the effective set.

### Storage

Constraint entries are stored in MetadataStore (in-memory, rebuilt from
journal on restart). The journal's Constraint entries are the durable source.

### Pruning

`prune_targets(live)` removes constraint entries whose target or bases are
no longer in `live`. Per-base pruning removes dead bases individually but
keeps the entry (and its surviving bases) even if empty — empty effective
set = "full or any base delta allowed."

---

## 7. Delta Codec

### Architecture

- `delta/mod.rs` — module declarations
- `delta/delta.rs` — DeltaPatch (VCDIFF wrapper using oxidelta)
- `delta/object.rs` — DeltaState + StoredObject (version-agnostic bridge)
- `delta/versions/mod.rs` — decode/encode_delta_state dispatch
- `delta/versions/v1.rs` / v2.rs / v3.rs — per-version envelope formats

### DeltaPatch

- `diff(base, target)` → VCDIFF patch from base to target.
- `decode(bytes)` → wrap existing bytes as a patch.
- `encode()` → return vcdiff bytes.
- `apply(vcdiff, base)` → apply patch, return reconstructed target bytes.
  Error → CorruptObject.

### StoredObject

- **Full { payload }**: Raw bytes, no envelope.
- **Delta { state }**: V3-envelope wrapped delta.
- `encode()`: Full → payload bytes. Delta → V3 envelope bytes.
- `decode_delta(bytes)`: Parse bytes as V3 (or V1/V2, migrated forward).
  Returns Delta if valid envelope. Error if not a valid delta envelope.

### Versioned envelopes

V3 format: `[magic(6)][version(2)][base_hash(34)][diff_hash(32)][content_len(8)][payload(variable)]`

### ObjectStore interaction

The ObjectStore stores opaque bytes. For delta-encoded objects, those bytes
are the complete V3 envelope (as returned by `StoredObject::encode()` for
Delta variant). On read, `StoredObject::decode_delta()` parses the envelope.

---

## 8. Delete Semantics — No Dangling Deltas

This is the most critical invariant in the system.

### The problem

If hash A is stored as a delta against base B, and the caller deletes B:

- ObjectStore has A → V3_envelope(base=B, vcdiff), B → full bytes.
- `get(A)` calls `get(B)` to reconstruct.
- If B's bytes are physically removed from ObjectStore, `get(A)` fails.

### The solution: re-materialization in the WAL consumer

**Before the WAL consumer physically removes B from the ObjectStore, it
re-materializes all deltas that depend on B.**

When `run_wal_consumer()` encounters a `Delete{hash}`:

1. **Scan for dependents**: Check all entries in ObjectStore whose encoding
   is `Delta { base_hash }` where `base_hash == hash`.

2. **Re-materialize each dependent**: For each dependent target T:
   a. Fetch T's delta bytes from ObjectStore.
   b. Decode V3 envelope → extract base_hash and vcdiff.
   c. Fetch base bytes (still available — hasn't been deleted yet).
   d. Apply `DeltaPatch::apply(base, vcdiff)` → T's original bytes.
   e. `ObjectStore.put(T, original_bytes, encoding = Full)`.
   f. `hint_state_change(T, Some(original_bytes))` for cache coherency.

3. **Physically remove**: `ObjectStore.delete(hash)`.

### Concurrent access during re-materialization

During step 2, if `get(A)` is called:

- If cache has A (from a previous get), returns cached original data.
- If cache miss, ObjectStore still has A's delta bytes and B's full bytes
  (B not yet deleted), so reconstruction works.
- After step 3, ObjectStore has A's full bytes (from 2e), so any new
  get(A) works directly.

### Performance

For each deleted hash, scanning the ObjectStore for delta dependents is
O(n) in the number of stored objects. For the initial in-memory
implementation, this is acceptable for modest store sizes (<100K objects).

### Journal interaction

Re-materialization happens synchronously within Delete processing. The WAL
consumer doesn't advance the checkpoint or trim until after re-materialization
is complete.

---

## 9. Maintenance — Optimizer + GC

### Optimizer

The optimizer replaces full bytes with delta-encoded bytes. For each constraint
target T with effective bases:

1. Get T's current payload from ObjectStore (full bytes).
2. Get each candidate base's payload from ObjectStore.
3. Choose best base (heuristic: smallest delta, or first).
4. Compute `DeltaPatch::diff(base_payload, target_payload)`.
5. Build V3 envelope: `StoredObject::Delta(base_hash, content_len, vcdiff)`.
6. `ObjectStore.put(T, envelope_bytes, encoding = Delta { base_hash })`.
7. `hint_state_change(T, reconstructed_data)` — cache gets the
   reconstruction, so subsequent get() hits cache without needing
   reconstruction.

**Skip conditions**:

- Target has no constraint or empty effective bases.
- Target is `Hash::zero()` (never stored).
- Target's constraint contains `Hash::zero()` (full is acceptable per hint).

### Constraint pruning

Phase 2: prune constraint metadata so stored constraints converge toward
effective constraints. Uses `MetadataStore::prune_targets()` which
per-base-prunes: removes dead bases individually but keeps target entries
even if empty (empty = "full or any-base delta allowed").

**GC does NOT delete objects.** Objects are only removed by explicit
CasApi::delete() materialized by the WAL consumer.

### optimize_once()

`CasMaintenanceApi::optimize_once()`:

1. `run_wal_consumer()` — materializes pending entries.
2. `run_maintenance()` — optimizer + constraint pruning.
3. Returns report.

---

## 10. CasMaintenanceApi

- `optimize_once()`: Run WAL consumer + maintenance. Standard cycle.
- `prune_constraints()`: Remove constraints for deleted targets/bases.
  Calls `ObjectStore.list_hashes()` for live set, then
  `MetadataStore.prune_targets()`.
- `gc_sweep()`: Run WAL consumer + prune constraints. Named "GC" but only
  prunes metadata — never deletes objects.
- `list_all_hashes()`: Delegate to ObjectStore.
- `repair_index()`: For in-memory, always consistent (no-op). File-based
  implementations scan for inconsistencies.

---

## 11. Error Types

- **NotFound(Hash)**: Object doesn't exist (or was deleted). Returned by
  get/stat. Includes hash for diagnostic.
- **InvalidArgument(String)**: Bad input (self-referencing constraint, etc.).
- **InvalidInput(String)**: Caller-provided input failed validation.
- **CorruptObject { hash, details }**: Data corruption (delta decode failure,
  missing base during reconstruction, envelope parse failure).
- **Io(std::io::Error)**: I/O error.
- **Internal(String)**: Internal invariant violation.

---

## 12. Implementation Phases

### Phase 1: Wire delta into InMemoryObjectStore + ReadView

1. Remove `#[allow(dead_code)]` from DeltaPatch, DeltaState, StoredObject.
2. Change `DeltaPatch.vcdiff` from `Cow<'a, [u8]>` to `Vec<u8>`.
3. Change `InMemoryObjectStore` from `DashMap<Hash, Bytes>` to
   `DashMap<Hash, StoredEntry>` where `StoredEntry { data: Bytes, encoding }`.
4. Update `ObjectStore::stat` to return stored encoding.
5. Update `ReadView::get` to detect delta-encoded entries and reconstruct.

### Phase 2: Re-materialization in WAL consumer

1. In `run_wal_consumer()`, when processing a Delete entry:
   a. Scan ObjectStore for delta dependents (encoding == Delta{base_hash}).
   b. Re-materialize each dependent (fetch delta, apply, store full).
2. The WAL consumer needs access to ObjectStore's encoding metadata.
3. Tests: create delta, delete base, get dependent returns data.

### Phase 3: Effective constraints + real optimizer

1. Add `effective_bases()` helper to ConstraintApi.
2. Replace optimizer placeholder with real delta computation.
3. Wire optimizer to store delta-encoded bytes + update encoding.
4. Tests: constraint → optimize → stat returns Delta → get reconstructs.

### Phase 4: Simplify code

Remove unused re-exports, dead_code allowances, Cow in DeltaPatch,
unused generic parameters.

### Phase 5: Tests

- Delta roundtrip: put full, optimize, get returns reconstructed.
- stat returns correct encoding after optimization.
- Base deletion does NOT break dependent delta retrieval.
- Effective constraints: multi-base, with/without zero, all-deleted.
- Optimizer skips zero-hash targets, missing bases, empty effective sets.

### Phase 6: Update PLAN.md

Replace with this comprehensive spec.
