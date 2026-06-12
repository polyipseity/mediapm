# CAS Architecture Redesign Plan

## 1. Current Architecture Diagnosis

### What's wrong

```text
Current:            Desired:
───────             ───────
CasApi trait        → Thin public API (put/get/delete/exists only)
  ├─ streaming       → Remove from trait (add as optional default)
  ├─ constraints     → Remove from base trait
  ├─ bulk ops        → Remove from base trait
  └─ materialize     → Remove from base trait

FileSystemCas       → Just a handle that composes journal + view
  └─ FileSystemState (1700+ line monolith) → Split into independent components
       ├─ object files     → MaterializedView (filesystem impl)
       ├─ index (redb)     → removed (metadata from stat + filename)
       ├─ mmap tracking    → Move to MaterializedView
       ├─ verify/cache     → Move to MaterializedView
       ├─ GC               → BackgroundEngine (Maintenance pass)
       ├─ optimize         → BackgroundEngine (Maintenance pass)
       ├─ metadata         → MetadataStore
       ├─ metrics          → Observability layer
       ├─ recovery         → Bootstrap/startup concern
       └── object_actor    → Part of MaterializedView

Maintenance disabled  → BackgroundEngine (runs forever, interruptible)
(O(n×m) scaling bug)    Never tries to be "complete" in one pass

InMemoryCas missing  → Real InMemory backend using same architecture
```

### Root causes

1. **No write-ahead log (WAL)** — every operation goes straight to the
   filesystem + index synchronously, mixing crash-safety guarantees with
   efficiency concerns in one layer.

2. **FileSystemState does everything** — object persistence, metadata index,
   constraint tracking, GC, optimization, recovery, metrics all in one
   `impl` block behind one `RwLock`. Changing one concern risks breaking
   another.

3. **CasApi trait is too fat** — it bundles constraint, streaming,
   materialization, and batch APIs alongside the core put/get/delete/exists.
   Every backend must implement all of them.

4. **Optimizer is tied to the write path** — it runs in-process, shares the
   same `IndexState`, and blocks on the same locks. There is no clean
   interruption model.

5. **GC coordinates via fragile hacks** (`recently_written` DashSet) instead
   of an epoch-based or generation-tracking scheme that properly separates
   mutators from collectors.

---

## 2. Target Architecture

### Layers

```text
   ┌─────────────────────────────────────────────┐
   │              CasApi (public trait)            │
   │  put / get / stat / delete                      │
   │  (thin — no constraints, materialize —           │
   │   those are derived APIs)                        │
   │                                               │
   │  Postcondition guarantees (instant, per-thread-of-execution): │
   │   • write-then-read ✓                                       │
   │   • write-then-stat ✓                                       │
   │   • delete-then-NotFound ✓                                  │
   │   • write-then-delete ✓                                     │
   │   • delete-then-write (re-creation) ✓                        │
   │   • idempotency ✓                                            │
   │   • independence (different hashes) ✓                        │
   │   • crash-surviving commitment ✓                             │
   │  [no exists/info — TOCTOU discouraged]                       │
   └──────────────────┬──────────────────────────┘
                      │
   ┌──────────────────▼──────────────────────────┐
   │              CasStore (handle)                │
   │  Composes:                                   │
   │   • Journal (crash-safe WAL, versioned disk)  │
   │   • ReadView (fast reads, journal fallback)   │
   └──────┬──────────────┬──────────────────────┘
          │              │
   ┌──────▼──────┐ ┌─────▼──────────────┐
   │  Journal    │ │  ReadView          │
   │  (WAL)      │ │  (with journal     │
   │             │ │   fallback path)    │
   │ Crash-safe  │ │                    │
   │ append-only │ │  ┌──────────────┐  │
   │ operation   │ │  │ ObjectStore  │  │  payload bytes
   │ log         │ │  │ (pluggable)  │  │
   │             │ │  └──────┬───────┘  │
   │ On-disk     │ │         │          │
   │ segments:   │ │  ┌──────▼───────┐  │
   │  - versioned│ │  │MetadataStore │  │  metadata
   │  - active   │ │  │ (hint map)   │  │
   │  - sealed   │ │  │ (in-memory,  │  │
   │  - trimmed  │ │  │  replay from │  │
   │             │ │  │  journal)    │  │
   └──────┬──────┘ └─────────────────────┘
          │
   ┌──────▼──────────────────────────────────┐
   │         BackgroundEngine                  │
   │                                           │
   │  ┌────────────────┐  ┌──────────────┐    │
   │  │ WALConsumer    │  │ Maintenance  │    │
   │  │ (batch drain   │  │ (combined GC │    │
   │  │  + segment     │  │  + optimizer)│    │
   │  │  trim folded)  │  │              │    │
   │  └────────────────┘  └──────────────┘    │
   │                                           │
   │  Runs forever, fully interruptible.       │
   │  WALConsumer and Maintenance are direct     │
   │  async methods (no Pass trait).            │
   └───────────────────────────────────────────┘
```

### 2.1 CasApi — the public trait

```rust
/// Minimal public CAS contract with intuitive postcondition guarantees.
///
/// Everything else (constraints, streaming, materialize, batch) is
/// built on top of these four methods.
///
/// # TOCTOU discouraged
///
/// There is no standalone `exists()` method. The API is designed to
/// discourage TOCTOU patterns — if you need the payload, use `get()`
/// (returns `NotFound` on miss); if you only need metadata, use `stat()`
/// (returns `NotFound` on miss). Both give an authoritative answer in one
/// operation, removing the temptation to check-then-act separately.
///
/// A determined caller can still construct TOCTOU patterns (e.g.
/// get-before-delete on unrelated hashes), but the API surface steers
/// toward single-operation decisions.
///
/// # Guarantees
///
/// All ordering guarantees below apply **within a single thread of
/// execution** (one async task). Across threads of execution, concurrent
/// operations are commutative where possible, but no cross-thread
/// ordering is promised. This keeps the API intuitive and simple while
/// allowing multiple readers and writers to share the same `CasApi`.
///
/// ## Write-then-read (instant)
/// After `put(h, data)` returns `Ok`:
/// - `get(h)` **will** return `data` immediately
/// - `stat(h)` **will** return the correct metadata immediately
///
/// The mechanism: the entry goes to the journal (fsync'd) first, then
/// `hint_state_change` populates the in-memory cache; if the cache misses,
/// the mandatory journal fallback scans the WAL before returning
/// `NotFound`.
///
/// ## Write-then-stat (instant)
/// Same guarantee as write-then-read — `stat()` reflects the write
/// immediately after `put()` returns.
///
/// ## Delete-then-get / Delete-then-stat (instant)
/// After `delete(h)` returns `Ok`:
/// - `get(h)` **will** return `NotFound` immediately
/// - `stat(h)` **will** return `NotFound` immediately
///
/// The mechanism: the tombstone is journal-committed and the ReadView
/// fallback returns `Tombstone` from `check_pending()` before
/// returning stale data.
///
/// ## Write-then-delete (same hash)
/// `put(h, a)` → `delete(h)` → effect is a no-op from an external
/// observer's perspective (the hash doesn't exist). Re-creation is
/// explicit: `put(h, a)` → `delete(h)` → `put(h, b)` → `get(h)` = `b`.
///
/// ## Delete-then-write (same hash, re-creation)
/// `delete(h)` → `put(h, a)` → `get(h)` = `a`. A deleted hash can be
/// re-created by putting new data for it. The new put is a fresh
/// commitment; the old tombstone is irrelevant.
///
/// ## Idempotency
/// - `put(h, data)` followed by another `put(h, data)` with the same
///   data produces the same hash and is a no-op.
/// - `delete(h)` followed by `delete(h)` is a no-op.
///
/// ## Independence (different hashes)
/// Operations on different hashes never interfere:
/// - `put(h1, a)` → `get(h2)` — never accidentally returns `a`
/// - `delete(h1)` does not affect `get(h2)`
/// - `put(h1, a)` and `put(h2, b)` are commutative
///
/// ## Crash survival
/// After any method returns `Ok`, the operation survives process death.
/// The journal fsync is the sole commitment point; all derived layers
/// are reconstructable from the journal.
///
/// ## Internal layers remain "eventual"
/// The guarantees above are at the `CasApi` level (user-facing). Beneath
/// this API, the ObjectStore backend, GC, and optimizer are intentionally
/// "eventual" — they run asynchronously as background tasks with relaxed
/// ordering. This decoupling lets each layer batch, defer, and optimize
/// independently:
///
/// - **ObjectStore `delete()`**: the backend may defer physical
///   reclamation (the journal guarantees the tombstone is visible).
/// - **WALConsumer**: journal entries reach ObjectStore asynchronously;
///   the journal fallback fills the gap.
/// - **GC**: operates directly on ObjectStore when it runs, not as a
///   synchronous part of any user-facing operation.
/// - **Optimizer**: rewrites deltas when convenient, not on the critical
///   path.
///
/// The user of `CasApi` never needs fallback-retry loops — the journal
/// makes every effect instantly visible to the calling thread.
#[async_trait]
pub trait CasApi: Send + Sync {
    /// Store bytes, return canonical hash. Crash-safe:
    /// after this returns, the operation survives process death.
    /// After this returns, get() / stat() will succeed immediately
    /// (see Guarantees: Write-then-read).
    async fn put(&self, data: Bytes) -> Result<Hash, CasError>;

    /// Retrieve bytes by hash. Returns NotFound if not yet available.
    /// After a successful put(), get() will return the data immediately.
    /// After delete(), get() will return NotFound immediately.
    async fn get(&self, hash: Hash) -> Result<Bytes, CasError>;

    /// Retrieve metadata (size, encoding) without loading payload bytes.
    /// Returns NotFound if hash was never stored or was deleted.
    /// Consistent with write-then-read and delete-then-get guarantees.
    async fn stat(&self, hash: Hash) -> Result<ObjectMeta, CasError>;

    /// Mark hash for deletion. Actual reclamation is eventual.
    /// Crash-safe: after this returns, the deletion survives death.
    /// After this returns, get() / stat() will return NotFound
    /// immediately (see Guarantees: Delete-then-get).
    async fn delete(&self, hash: Hash) -> Result<(), CasError>;
}
```

#### Streaming

```rust
/// Streaming I/O — built atop CasApi with default buffer-through impls.
/// Backends that can stream directly (e.g. file descriptors, sockets)
/// should override for zero-copy paths.
#[async_trait]
pub trait CasApiStreaming: CasApi {
    /// Read from an unbuffered reader, store contents, return hash.
    /// Default: read into Bytes, call put(). Correct but allocates.
    async fn put_stream(
        &self,
        reader: &mut (dyn AsyncRead + Send + Unpin),
    ) -> Result<Hash, CasError> {
        let mut buf = BytesMut::new();
        reader.read_buf(&mut buf).await?;
        self.put(buf.freeze()).await
    }

    /// Retrieve bytes and write to an unbuffered writer.
    /// Default: call get(), write all to writer. Correct but allocates.
    async fn get_stream(
        &self,
        hash: Hash,
        writer: &mut (dyn AsyncWrite + Send + Unpin),
    ) -> Result<(), CasError> {
        let data = self.get(hash).await?;
        writer.write_all(&data).await?;
        Ok(())
    }
}

// Blanket impl: every CasApi automatically provides streaming methods.
impl<T: CasApi + Send + Sync> CasApiStreaming for T {}
```

The core trait stays minimal (no generics, no streaming). The streaming
trait layers on top without any backend needing to know about it — and
backends that want zero-copy streaming override the two methods.

**What moved out:**

- `put_with_metadata`, `put_stream_with_metadata` → extension methods
  on a metadata extension trait.
- `get_many` → standalone method or streaming extension.
- `set_constraint`, `patch_constraint`, `get_constraint` → own
  `ConstraintApi` trait.
- `materialize_to_path` → standalone free function or `MaterializeApi` trait.
- `optimize_once`, `prune_metadata`, `gc_sweep` → BackgroundEngine methods.

**Rationale:** A CAS user who just wants to store and load data shouldn't
need to know about metadata, streaming, or materialization. Separating
these concerns makes the core trait trivially implementable and testable.

### 2.2 Journal (WAL)

#### Role

The journal is the **only** crash-safe commitment point. It is an
append-only log of operations (Put/Delete/Constraint). The user gets an
acknowledgment only after `fsync`. Every other layer (object store, index,
cache) is derived from the journal and can be rebuilt by replaying it.

#### Interface

```rust
/// Unique position in the journal.
/// Opaque token — implementation-defined (byte offset, LSN, etc.).
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct JournalPosition(u64);

/// Entry in the journal.
pub enum JournalEntry {
    Put { hash: Hash, data: Bytes },
    Delete { hash: Hash },
    Constraint { target: Hash, bases: BTreeSet<Hash> },
}

/// Result of a pending entry check.
pub enum PendingState {
    NotPresent,
    Present(Bytes),
    Tombstone,
}

/// Crash-safe operation log.
#[async_trait]
pub trait Journal: Send + Sync {
    /// Append an entry. Returns the position it was written at.
    /// **Guaranteed crash-safe** after this returns: fsync or equivalent
    /// has completed.
    async fn append(&self, entry: JournalEntry) -> Result<JournalPosition>;

    /// Append multiple entries atomically. Default impl calls `append`
    /// in sequence, but backends that support batching (file-based WAL
    /// with batched fsync) should override.
    async fn append_batch(&self, entries: Vec<JournalEntry>) -> Result<JournalPosition> {
        let mut last_pos = self.append(entries[0].clone()).await?;
        for entry in &entries[1..] {
            last_pos = self.append(entry.clone()).await?;
        }
        Ok(last_pos)
    }

    /// Current end-of-log position (for checkpoint tracking).
    fn committed_position(&self) -> JournalPosition;

    /// Approximate count of un-materialized entries (for batching decisions).
    fn pending_count(&self) -> usize;

    /// Read a pending entry for a hash that hasn't been materialized yet.
    /// Scans the active WAL segment(s) — O(n) but bounded by segment size.
    /// Returns NotPresent, Present(bytes), or Tombstone.
    async fn check_pending(&self, hash: Hash) -> Result<PendingState>;

    /// Replay entries from `from` onward.
    async fn replay_from(&self, from: JournalPosition) -> Result<Vec<JournalEntry>>;

    /// Trim fully-consumed segments whose end ≤ `up_to`.
    async fn trim(&self, up_to: JournalPosition) -> Result<()>;
}
```

#### Design — Segmented File-based WAL

```text
<cache-dir>/mediapm/cas/journal/active.journal     ← current writable segment
<cache-dir>/mediapm/cas/journal/sealed-{N}.journal  ← read-only, awaiting consumption
<cache-dir>/mediapm/cas/journal/checkpoint          ← last fully-consumed position
```

- **Active segment**: new entries appended here. Single writer (Mutex).
  Written with: `write(entry) → flush → fsync → update len header`.
  Rotated when size exceeds threshold (e.g. 64 MiB).

- **Sealed segments**: immutable. Consumer reads entries in order, sends
  to ObjectStore for materialization. After all entries consumed, segment
  is deleted.

- **Checkpoint**: the `JournalPosition` (byte offset) up to which all
  entries have been durably applied to the ObjectStore. Persisted atomically.

- **Recovery on startup**: read checkpoint, replay sealed segments from
  checkpoint onward, replay active segment from its start.

#### Performance considerations

- `append` is a buffered write + one `fsync` per call (or batched if caller
  groups puts). This makes it faster than current architecture which writes
  both object file AND index synchronously.
- For massive payloads (GiB+), the journal can *reference* an external blob
  file instead of inlining the data. A future optimization.
- `check_pending` scans the active segment backward (most recent entries
  first). In practice, the ReadView cache covers most read-after-write
  patterns, so `check_pending` is a cold path.

#### Why a file-based WAL and not redb/SQLite?

- Append-only files are the simplest possible crash-safe substrate. No B-tree
  overhead, no compaction needed at the journal level.
- The WAL has *no search index* — entries are identified by hash for the
  `check_pending` fast path, but that scan is bounded by segment size.
- Redb/SQLite stay in the ObjectStore layer, where their indexing/search
  capabilities are actually needed.

#### Write coalescing

Before flushing a batch to disk, the journal implementation **coalesces**
entries that target the same hash:

| Sequence | Coalesced result                  |
|----------|-----------------------------------|
| Put a → Put a | Single Put (idempotent)        |
| Put a → Delete a | Single Delete                |
| Delete a → Put a | Single Put (re-creation)      |
| Delete a → Delete a | Single Delete (idempotent)  |

This reduces redundant fsync traffic and keeps segments compact. Coalescing
happens **in memory** before the batched `writev` / `pwritev` to the active
segment — it never re-reads already-flushed data.

#### Versioned on-disk format

All persisted journal artifacts carry a version marker following the same
pattern established in `codec/versions/` and `index/versions/`:

- **Journal segments**: magic prefix `b"CASJNL"` (6 bytes) + 2-byte LE u16
  version. Version 1 (`\x01\x00`) is the initial format: len-prefixed entries
  with `[hash: 32 bytes] [op_type: 1 byte] [payload_len: 4-byte LE] [payload: bytes]`.
- **Checkpoint file**: magic prefix `b"CASCKP"` (6 bytes) + 2-byte LE u16
  version. Version 1 (`\x01\x00`): `[position: 8-byte LE] [hash: 32 bytes]`
  for integrity verification.

See §2.9 for the full versioning policy.

#### Constraint hints

A `Constraint { target, bases }` journal entry is a **lightweight,
non-binding hint** that `target` would compress well against `bases`.
Recording a constraint is fast (one journal append + fsync — same cost
as Put/Delete). The system never blocks on constraint satisfaction; the
MaintenancePass (§2.6) picks up pending constraints from the
MetadataStore and computes deltas when resources permit.

Setting a constraint guarantees nothing about when (or whether) the delta
will be computed — it only records the intent. Callers that need stronger
assurances (e.g. "compute delta before the next full sync") should use
the `ConstraintApi` with explicit urgency hints, which the MaintenancePass
can prioritize.

### 2.3 ReadView — fast read path (with mandatory journal fallback)

#### Role

The ReadView provides fast (cached/materialized) access to stored objects
and enforces the **write-then-read** and **delete-then-get** guarantees.
It has a **mandatory journal fallback** that guarantees read-after-write
consistency by checking the WAL for both pending puts and delete tombstones
before consulting materialized storage.

#### Interface

```rust
/// Fast read path backed by materialized storage + mandatory journal fallback.
///
/// Unlike try_get() in earlier designs, `get()` never returns None — if
/// materialized storage misses, it falls back to the journal. Only returns
/// NotFound when the hash genuinely doesn't exist (checked against journal
/// tombstones and never-written hashes).
#[async_trait]
pub trait ReadView: Send + Sync {
    /// Get bytes. Falls back to journal if not materialized.
    /// Returns NotFound if hash was never stored or was deleted.
    async fn get(&self, hash: Hash) -> Result<Bytes, CasError>;

    /// Get metadata without loading payload bytes.
    /// Falls back to journal for consistency.
    async fn stat(&self, hash: Hash) -> Result<ObjectMeta, CasError>;

    /// Notify the read path of a state change. `data` is `Some(bytes)`
    /// for puts and `None` for deletes. Best-effort hint for inline caching.
    async fn hint_state_change(&self, hash: Hash, data: Option<Bytes>);

    /// Apply a batch of journal entries (called by WALConsumer).
    async fn apply_batch(&self, entries: Vec<JournalEntry>) -> Result<()>;
}
```

#### ComposedReadView

A `ComposedReadView` chains multiple backends and enforces tombstone checking:

```text
ComposedReadView
  ├─ InMemoryCache (DashMap<Hash, (Instant, Bytes)>, TTL-evicted)
  ├─ ObjectStore (filesystem or database — actual payload storage)
  └─ JournalFallback (mandatory — scans WAL for not-yet-materialized entries
      AND checks tombstones before returning NotFound)
```

Lookup order for `get()`:

1. InMemoryCache (fastest, O(1))
2. ObjectStore (O(1) index lookup + O(payload size) read)
3. JournalFallback — single `Journal::check_pending(hash)` call:
   - `PendingState::Tombstone` → return NotFound
   - `PendingState::Present(bytes)` → return bytes
   - `PendingState::NotPresent` → return NotFound (genuinely never stored)

Lookup order for `stat()`:

1. ObjectStore `stat` (cheap stat + filename parsing, no payload)
2. JournalFallback — single `Journal::check_pending(hash)` call:
   - `PendingState::Tombstone` → return NotFound
   - `PendingState::Present(bytes)` → derive metadata from bytes
   - `PendingState::NotPresent` → return NotFound

The `JournalFallback` is what makes the **guarantees concrete**:

- Write-then-read: after put() returns, the entry is in the journal, and
  `check_pending()` returns `Present` before returning NotFound.
- Delete-then-get: after delete() returns, the tombstone is in the journal,
  and `check_pending()` returns `Tombstone` before returning stale
  ObjectStore data.

#### In-flight read dedup

When multiple concurrent `get()` calls miss in cache and ObjectStore, they
race to the journal fallback. To avoid redundant WAL scans for the same
hash, `ComposedReadView` uses a `DashMap<Hash, Arc<SharedResult>>`:

```rust
struct SharedResult {
    done: Arc<Notify>,
    result: OnceCell<Result<Option<Bytes>>>,
}
```

The first caller to miss starts the journal scan; subsequent callers await
`done.notified()` and reuse the result. This bounds worst-case WAL contention
to one scan per unique hash regardless of concurrent readers.

### 2.4 ObjectStore — payload storage

#### Role

Persistent storage of object payload bytes. Pluggable backend that the
WALConsumer writes into and the ReadView reads from.

#### Interface

```rust
/// Actual storage backend for object payload bytes.
#[async_trait]
pub trait ObjectStore: Send + Sync {
    /// Store raw bytes for a hash (replace if exists).
    async fn put(&self, hash: Hash, data: Bytes) -> Result<()>;

    /// Retrieve raw bytes. Returns None if not stored.
    async fn get(&self, hash: Hash) -> Result<Option<Bytes>>;

    /// Return metadata (size, encoding) without loading payload bytes.
    /// Returns None if hash not stored.
    async fn stat(&self, hash: Hash) -> Result<Option<ObjectMeta>>;

    /// Remove payload bytes (eventual — backend may defer).
    async fn delete(&self, hash: Hash) -> Result<()>;

    /// Optional: list all stored hashes. May be O(n). Returns
    /// Unsupported if the backend cannot enumerate efficiently.
    async fn list_hashes(&self) -> Result<Vec<Hash>>;
}
```

#### Object Store Implementations

**a) FileSystemObjectStore** (evolved from current fan-out layout)

- Same fan-out path scheme: `<root>/v1/<algo>/<h[0:2]>/<h[2:4]>/<h[4..]>`
- Full objects: `<hash>.raw` (raw payload, no headers)
- Delta objects: `<hash>.delta.<base_hash>` (structured diff with base ref)
- Single mutation actor (current `FileObjectActor`) serializes writes
- mmap for reads above 64 KiB threshold
- Read-only permissions for written files (tamper detection)
- `stat` is `lstat()` + filename parsing for encoding info
- `list_hashes` scans the directory tree

## b) DatabaseObjectStore

- Uses redb or SQLite as the single backing store
- All payloads in one (or sharded) database file(s)
- Better for very large object counts (no inode pressure)
- Simpler to backup (one file vs. directory tree)

## c) InMemoryObjectStore

- `DashMap<Hash, Bytes>` — for tests and ephemeral use
- No persistence — should probably be journal-less or journal-in-memory

**d) S3ObjectStore** (future)

- Remote object store backend
- Each hash → S3 key
- Deletion is an S3 DeleteObject call
- Read-through cache on local disk

### 2.5 MetadataStore — metadata (constraints + future entries)

#### Role

Stores **only** constraint hints — the pairings (target hash → base hashes)
that the MaintenancePass uses for delta optimization. This is the **only
durable metadata** not derivable from the ObjectStore alone (object
encoding is embedded in filenames, sizes come from `stat`).

Constraints are **non-binding hints** — setting one is fast and never blocks.

#### Interface

```rust
/// Metadata about a stored object.
/// Encoding is derived from ObjectStore filename convention;
/// size comes from `stat()`. Neither is stored in MetadataStore.
pub struct ObjectMeta {
    pub payload_len: u64,
    pub encoding: ObjectEncoding,
}

pub enum ObjectEncoding {
    Full,
    Delta { base_hash: Hash },
}

/// Lightweight storage for constraint hints (in-memory only).
/// Reconstructed from journal replay on startup — no dedicated file.
#[async_trait]
pub trait MetadataStore: Send + Sync {
    async fn set(&self, target: Hash, bases: BTreeSet<Hash>) -> Result<()>;
    async fn get(&self, target: Hash) -> Result<Option<BTreeSet<Hash>>>;
    async fn patch(&self, target: Hash, patch: ConstraintPatch) -> Result<()>;
    async fn list_targets(&self) -> Result<Vec<Hash>>;
    async fn rebuild_from_journal(&self, journal: &dyn Journal) -> Result<()>;
}
```

#### Implementation

In-memory `RwLock<HashMap<Hash, BTreeSet<Hash>>` with no separate file.
On startup, `rebuild_from_journal` replays journal entries to populate
the map — the WAL is the single persistent source of truth for all CAS
state. This eliminates a separate persistence path and ensures metadata
can never drift out of sync with the journal.

The WALConsumer is the sole writer: it calls `MetadataStore::set` /
`MetadataStore::patch` during replay. No direct user access — all mutation
goes through `CasApi` → journal → WALConsumer.

#### What was removed

- **Dedicated metadata file**: metadata is reconstructed from journal
  replay on startup.
- **Object metadata index**: sizes come from ObjectStore `stat`, encoding
  from filename convention. No separate index needed.
- **Bloom filter**: never justified its complexity; `stat` via filesystem
  metadata is already O(1) for materialized objects.
- **Graph queries** (`delta_children`, `constraint_targets`): the
  MaintenancePass computes delta dependencies by scanning ObjectStore
  filenames once per pass — acceptable for an infrequent background job.
- **`rebuild_from_objects`**: no derived metadata to rebuild; journal
  replay is sufficient.

### 2.6 BackgroundEngine — forever optimization

#### Role

The BackgroundEngine runs a set of **interruptible, never-ending passes**
that keep the system consistent and move it toward optimality. Each pass
is a bounded work item that can be preempted at any point.

#### Passes

| Pass | What it does | Batching | Interruptibility |
|------|-------------|----------|------------------|
| **WALConsumer** | Reads journal entries, writes to ObjectStore | Batch drain (chunk size e.g. 100 entries) | After each chunk |
| **Maintenance** | Combined GC + Optimizer (see below) | GC: batch deletes on ObjectStore directly (chunk 1000) | After each candidate / chunk |

#### Structure

```rust
pub struct BackgroundEngine {
    cancel: CancellationToken,       // global interrupt
    journal: Arc<dyn Journal>,
    object_store: Arc<dyn ObjectStore>,
    wal_consumer: WALConsumer,
}
```

`BackgroundEngine` exposes two direct async methods instead of a generic
`Pass` trait:

```rust
impl BackgroundEngine {
    /// Run the WAL consumer: drain journal → ObjectStore, then trim
    /// consumed segments. Returns true if any work was done.
    pub async fn run_wal_consumer(&self) -> Result<bool>;

    /// Run combined GC + optimizer. Returns true if any work was done.
    pub async fn run_maintenance(&self) -> Result<bool>;
}
```

**Ordering**: `run_wal_consumer` → `run_maintenance` (repeated).
WALConsumer runs first so that newly materialized objects are visible
before the Maintenance pass makes decisions. Segment trim is folded into
the consumer (no separate pass needed).

#### WALConsumer details

This is the most critical operation — without it, acknowledged data never
reaches the object store.

```rust
struct WALConsumer {
    journal: Arc<dyn Journal>,
    object_store: Arc<dyn ObjectStore>,
    checkpoint: AtomicU64,  // last committed journal position
}

impl BackgroundEngine {
    async fn run_wal_consumer(&self) -> Result<bool> {
        let from = JournalPosition(self.wal_consumer.checkpoint.load(Ordering::Acquire));
        let entries = self.journal.replay_from(from).await?;
        if entries.is_empty() {
            return Ok(false);
        }

        for (i, entry) in entries.iter().enumerate() {
            if self.cancel.is_cancelled() { return Ok(true); }
            match entry {
                JournalEntry::Put { hash, data } => {
                    self.object_store.put(*hash, data.clone()).await?;
                }
                JournalEntry::Delete { hash } => {
                    self.object_store.delete(*hash).await?;
                }
            }
            // Advance checkpoint after each entry (crash-safe: replay is idempotent)
            let new_pos = JournalPosition(from.0 + (i + 1) as u64);
            self.wal_consumer.checkpoint.store(new_pos.0, Ordering::Release);
        }

        // Trim fully-consumed journal segments (folded — no separate pass)
        self.journal.trim(last_pos).await?;
        Ok(true)
    }
}
```

**Crash recovery**: `replay_from(checkpoint)` is safe because `put` and
`delete` are **idempotent** — replaying a put that was already applied
just overwrites the same file with the same bytes. So a crash mid-batch
only means re-doing work, never corrupting state.

#### Maintenance logic (combined GC + Optimizer)

```rust
impl BackgroundEngine {
    async fn run_maintenance(&self) -> Result<bool> {

        // === Phase 1: Optimizer ===
        // Replay journal to build constraint map, then rewrite deltas.
        let constraints = self.build_constraint_map().await?;
        let mut did_work = false;
        let rewritten = 0;

        for (target, bases) in &constraints {
            if self.cancel.is_cancelled() { break; }
            let best_base = self.find_best_base(*target, bases).await?;
            if self.try_rewrite(*target, best_base).await? {
                rewritten += 1;
                did_work = true;
            }
            if rewritten >= self.max_rewrites { break; }
        }

        // === Phase 2: GC (directly on ObjectStore, bypassing WAL) ===
        // 1. Build delta dependency map from ObjectStore filename scan.
        let delta_deps = self.scan_delta_dependencies().await?;

        // 2. Compute reachable set from roots.
        let reachable = self.compute_reachable().await?;

        // 3. For each unreachable base that has reachable delta children:
        //    materialize the delta first (read delta, apply to base, write
        //    full), then delete both base and delta from ObjectStore.
        let all_hashes = self.object_store.list_hashes().await?;
        let mut deleted = 0;

        for hash in &all_hashes {
            if self.cancel.is_cancelled() { break; }
            if reachable.contains(hash) { continue; }

            // Orphan prevention: if this hash is a delta base with reachable
            // delta children, materialize them first.
            if let Some(orphans) = delta_deps.get(hash) {
                for orphan in orphans {
                    if reachable.contains(orphan) {
                        self.materialize_delta_to_full(orphan).await?;
                    }
                }
            }

            self.object_store.delete(*hash).await?;
            deleted += 1;
            did_work = true;
        }

        Ok(did_work)
    }
}
```

**Why GC and optimizer are merged**: GC may need to delete an object that
serves as a delta base for another reachable object. If GC ran separately
from the optimizer, it could create orphaned deltas (deltas pointing to
deleted bases). By combining them in one pass, the optimizer phase runs
first so new deltas are visible, then the GC phase materializes any delta
that would be orphaned before deleting its base.

**Generation-based write-GC coordination:**

- Each `put()` increments a global generation counter.
- GC records the generation at GC start, and skips objects from that
  generation or later — they were written after the GC start and should
  not be deleted.
- This replaces the fragile `recently_written` DashSet.

**Why GC bypasses the WAL**: GC is an internal maintenance operation that
removes garbage from the storage backend. It has the same "eventual"
semantics as the underlying ObjectStore — there is no user-facing guarantee
that GC'd deletes are immediately visible to concurrent readers. By deleting
directly on the ObjectStore, GC avoids polluting the WAL with entries that
would never need to be replayed or visible to ReadView journal fallback.

### 2.7 CasStore — the composed handle

```rust
pub struct CasStore {
    journal: Arc<dyn Journal>,
    view: Arc<ComposedReadView>,
    bg: Arc<BackgroundEngine>,
}

#[async_trait]
impl CasApi for CasStore {
    async fn put(&self, data: Bytes) -> Result<Hash, CasError> {
        let hash = Hash::from_content(&data);
        let entry = JournalEntry::Put { hash, data: data.clone() };
        self.journal.append(entry).await?;
        // Best-effort: try to make data readable immediately
        self.view.hint_state_change(hash, Some(data)).await;
        Ok(hash)
    }

    async fn get(&self, hash: Hash) -> Result<Bytes, CasError> {
        // view.get() includes mandatory journal fallback + tombstone checking
        self.view.get(hash).await
    }

    async fn delete(&self, hash: Hash) -> Result<(), CasError> {
        let entry = JournalEntry::Delete { hash };
        self.journal.append(entry).await?;
        self.view.hint_state_change(hash, None).await;
        Ok(())
    }

    async fn stat(&self, hash: Hash) -> Result<ObjectMeta, CasError> {
        // view.stat() includes mandatory journal fallback + tombstone checking
        self.view.stat(hash).await
    }
}
```

### 2.8 Concurrency Model

#### Thread-of-execution scoped guarantees

All `CasApi` ordering guarantees (write-then-read, delete-then-get, etc.)
apply **within a single thread of execution** (one async task). Across
threads of execution, concurrent operations are commutative where possible,
but no cross-thread ordering is promised.

This means multiple tasks can safely share the same `Arc<dyn CasApi>`:
one task may `put(hash)` while another task concurrently `get(hash)` on the
same instance, and both will observe correct per-task ordering. The
underlying `Arc<RwLock<...>>` / `DashMap` primitives provide the necessary
atomicity for shared-memory state.

#### Locking model

```text
Component         Thread Safety           Notes
─────────         ─────────────           ─────
Journal           Mutex around file       Single writer to active segment.
                  append                  Concurrent reads via check_pending.
                                          append_batch issues one fsync for
                                          the entire batch.

ObjectStore       RwLock                  Reads: any number. Writes: serialized
                  per-object              per object via shard-locking.

MetadataStore     RwLock                  In-memory HashMap only — no file
                  (no persistence)        persistence. Reconstructed from
                                          journal replay on startup.

InMemoryCache     DashMap                 Lock-free concurrent reads/writes
                  (or equivalent)         per entry.

BackgroundEngine  Actor or task           Each pass runs on its own task.
                  (tokio tasks)           Shared state via Arc<dyn Trait>.

GC coordination   Generation counter      AtomicU64, incremented per put().
                  (AtomicU64)             GC records starting generation and
                                          skips newer objects.

Read dedup        DashMap<Hash,           First concurrent miss performs
                  Arc<SharedResult>>      journal scan; others await result.
                                          Evicts after result is consumed.
```

### 2.9 Versioning Policy

All data persisted to disk **must** carry an explicit version marker.
This follows the same pattern established in `codec/versions/` and
`index/versions/`.

#### Rules

1. **Every on-disk format has a self-contained version module.**
   Journal segments, checkpoint files, and any future persistent artifact
   each have their own `versions/` directory with:
   - `mod.rs`: version dispatch + `IsoPrime` bridge exposing latest version.
   - `v1.rs`, `v2.rs`, `v3.rs`, etc.: version-local structs (no imports from other
     version modules).
   - The `mod.rs` is the **only** bridge — outside callers never import
     `vX` directly.

2. **Magic prefix + version bytes.** Every file starts with a unique
   6-byte magic prefix followed by a 2-byte LE u16 version. Identified
   formats:
   - Journal segment: `b"CASJNL"` + `\x01\x00` (V1)
   - Checkpoint file: `b"CASCKP"` + `\x01\x00` (V1)
   - Existing delta envelopes: `b"MDCASD"` + `\x01\x00` (V1, legacy) /
     `\x02\x00` (V2, legacy) / `\x03\x00` (V3, current — adds blake3 hash of diff)

3. **`DO NOT REMOVE` guard.** Once a version module exists, it must not
   be deleted — even if the format is superseded. Old versions are
   read-only forever.

4. **Forward-only migration.** A newer version must be able to read all
   older versions. There is no downgrade path.

5. **Schema marker dispatch.** For versioned databases (redb tables),
   a schema marker row (`__schema_version__`) is written on first open
   and checked on every subsequent open. Mismatch → hard abort.

6. **`IsoPrime` bridges.** Each `mod.rs` uses `IsoPrime` from
   `fp_library` to provide compile-time proof that version-to-version
   transformation is isomorphic. Bridges live only in `mod.rs`.

#### Current versioned artifacts

| Artifact | Magic | Current version | Location |
|----------|-------|----------------|----------|
| Delta envelope | `MDCASD` | V3 (V1,V2 legacy) | `codec/versions/` |
| Journal segment | `CASJNL` | V1 (planned) | `journal/versions/` |
| Checkpoint file | `CASCKP` | V1 (planned) | `journal/versions/` |

**V3 delta details**: `magic_with_embedded_version[8] | content_len[8] | payload_len[8] | diff_hash[32] | base_hash[...] | payload[...]`
where `diff_hash = blake3(payload)` (blake3 hash of the VCDIFF diff payload itself).
This allows independent integrity verification of the diff payload without
the base object, enabling safe diff caching and offline validation.

---

## 3. Migration Path

### Phase 1: Extract CasApi (preserve internals, change surface)

1. Define minimal `CasApi` trait (put/get/stat/delete) with Guarantees
   doc comments. No `exists` or `info` — TOCTOU discouraged.
2. Move constraints to `ConstraintApi` trait.
3. Move streaming/bulk to extension trait with default impls.
4. Make `FileSystemCas` implement both `CasApi` and `ConstraintApi`.
5. Update all callers (conductor, mediapm) to use only `CasApi` + `ConstraintApi`.

### Phase 2: Extract Journal (new component)

1. Implement `SegmentedFileJournal` (file-based WAL with versioned on-disk
   format using `CASJNL` magic and `CASCKP` checkpoints).
2. Make `FileSystemCas::put/delete` write to journal + best-effort view update.
3. Journal is not yet consumed — writes also go through old path (dual-write
   mode) for a transition period.
4. Add `WALConsumer` pass that reads journal and mirrors to ObjectStore
   using **batch drain** pattern.

### Phase 3: Extract ObjectStore from FileSystemState

1. Define `ObjectStore` trait (put/get/stat/exists/delete/list_hashes).
2. Refactor `FileSystemState` to delegate payload storage to an inner
   `ObjectStore` impl.
3. Move `FileObjectActor` inside the `FileSystemObjectStore` impl.
4. Move mmap tracking inside `FileSystemObjectStore`.

### Phase 4: Extract MetadataStore from FileSystemState

1. Define `MetadataStore` trait (set/get/patch/list_targets, in-memory,
   no persistence).
2. Remove the redb-backed MetadataIndex entirely — no dedicated index
   for object metadata; encoding from filename, size from `stat`.

### Phase 5: Build BackgroundEngine

1. Implement `BackgroundEngine` with direct async methods
   (`run_wal_consumer`, `run_maintenance`).
2. Implement `WALConsumer` (batch drain + segment trim) and
   `MaintenancePass` (combined GC + Optimizer) as internal methods.
3. Add startup recovery that replays journal → ObjectStore + MetadataStore
   (in-memory HashMap, populated from journal replay).
4. Enable in tests first; make optional in production (off by default).

### Phase 6: Remove old code

1. Remove deprecated methods from `FileSystemState`.
2. Remove `FileSystemCas` old direct-write paths.
3. Make journal the only write path.
4. Enable BackgroundEngine by default.
5. **Versioning audit**: verify all on-disk artifacts carry version markers.

### Phase 7: InMemoryCas

1. Implement `InMemoryObjectStore` + `InMemoryMetadataStore` (HashMap).
2. Implement `InMemoryJournal` (Vec-based, no persistence).
3. Compose into `CasStore` — same architecture, no filesystem dependencies.
4. **Versioning audit**: ensure zero persisted bytes are unversioned.

---

## 4. Key invariants preserved (and simplified)

| Invariant | Current | New |
|-----------|---------|-----|
| Content identity | Byte equality → same hash | Same (unchanged) |
| Crash safety | Object files via tempfile+rename; index via redb; delete ordering | Journal fsync is sole commitment point |
| Delta reconstruction | Must resolve base ancestry | Same, via ObjectStore + MetadataStore |
| Read-only object files | Set after write | Kept in FileSystemObjectStore |
| Deterministic fan-out | `<root>/v1/<algo>/<h[0:2]>/<h[2:4]>/<h[4..]>` | Same default; other ObjectStore impls free to choose |
| Failure resilience | Soft/hard disk thresholds | Evaluated at ObjectStore level |
| Concurrent dedup | DashMap<Hash, Arc<Notify>> | DashMap in ObjectStore or journal-level |
| Type-system invariant enforcement | Per-crate convention | Kept |
| Object metadata source | redb index | ObjectStore `stat()` + filename encoding convention |
| **TOCTOU discouraged** | exists/info create TOCTOU window | No standalone exists/info — use get/stat consistently |
| **Write-then-read** | After put(), immediate get() may fail (WAL + materialize race) | Journal fsync → journal fallback guarantees get() succeeds on same thread |
| **Delete-then-get** | After delete(), get() may still return stale data | Tombstone in journal → `check_pending()` returns `Tombstone` before returning stale data |
| **On-disk versioning** | Partial: delta envelopes are versioned; index has schema marker; WAL files are plain | All on-disk artifacts carry magic + version. §2.9 policy enforced. |
| **Operation batching** | Each put/delete is an individual fsync | Journal.append_batch() coalesces + batch-drain WALConsumer + chunked GC deletes on ObjectStore |

---

## 5. Open Questions

1. **Journal entry format for large payloads**: Decision — inline all bytes
   for simplicity. If profiling later shows WAL segment size becoming a
   bottleneck, add blob-reference optimization at that point.

2. **Constraint satisfaction latency**: Decision — constraints get higher
   priority during optimization (the MaintenancePass should prefer
   constraint-satisfying rewrites over other work when both are pending),
   but not high enough to preempt normal I/O or starve other background
   work. Implemented as a priority queue in the optimizer phase rather
   than best-effort iteration. If a future caller needs synchronous
   deltas, a fast-path inline delta during `put_with_constraints` can be
   added later.

3. **GC and storage backend interaction**: GC (and the optimizer's
   delete-reachable-during-rewrite path) operates directly on the
   ObjectStore — it calls `ObjectStore::delete()` directly, not through
   the WAL. The journal/WAL is for CasApi-level operations (user-facing
   put/get/delete). GC is a backend-internal maintenance activity that
   should not flow through the WAL at all. This avoids the
   Maintenance → journal → WALConsumer → Maintenance cycle entirely and
   keeps the GC/optimizer latency-sensitive (no waiting for consumer
   drain). The journal still records user-facing deletes; backend-internal
   deletes skip the WAL.

   **Implication**: The ObjectStore must be safe to call concurrently from
   both the WALConsumer (user-facing writes) and the GC/optimizer
   (backend-internal deletes/rewrites). `RwLock` per-object sharding
   handles this. The GC must also ensure it does not delete an object
   that the WALConsumer is about to write — handled by generation-based
   coordination (GC skips objects newer than GC start time).

4. **InMemoryJournal and process restart**: Decision — yes, `InMemoryCas`
   does not need persistence. Its journal is a simple
   `VecDeque<JournalEntry>` with no fsync. On drop, the journal is lost,
   matching current `InMemoryCas` behavior.

5. **append_batch flush policy**: Decision — `append_batch` does not flush
   or fsync after every entry. It issues one fsync for the entire batch
   (flush after all entries). This maximizes throughput for bulk
   operations; crash safety is preserved because the batch is atomic
   (all entries or none). If profiling later shows tail-latency issues
   from large batches, add bounded-latency flushes (e.g. every 10ms if
   batch is still filling).

6. **List all non-deleted hashes**: The `CasApi` needs a method to list
   all currently stored (non-deleted) hashes. This is required by
   conductor GC (out of scope for this plan), but the CAS plan must
   ensure the building blocks exist. Decision — keep `list_hashes()` on
   `ObjectStore`, but as an **optional** method (backends that cannot
   enumerate efficiently return `Err(Unsupported)`). The CasApi itself
   does not expose `list_hashes`; conductor GC will use ObjectStore
   directly. This means `ObjectStore` retains `list_hashes` as a
   best-effort method rather than removing it.

---

## 6. Simplifications Applied

The following simplifications from the initial architecture review have been
applied directly to the design sections above. They are no longer "suggestions"
— the changes are reflected in §2–§4.

| # | Suggestion | Disposition |
|---|-----------|-------------|
| 6.1 | `hint_written` / `hint_deleted` → `hint_state_change` | Applied (§2.3 ReadView) |
| 6.2 | Remove `exists()` from ObjectStore | Applied (§2.4 ObjectStore) |
| 6.3 | Fold journal checks into `check_pending()` | Applied (§2.3 JournalFallback) |
| 6.4 | Remove `Pass` trait — direct async methods on BackgroundEngine | Applied (§2.6 BackgroundEngine) |
| 6.5 | Reconsider whether `hint_state_change` is needed | Discarded — retained as valuable performance hint |
| 6.6 | Fold `SegmentTrim` into `WALConsumer` | Applied (§2.6 WALConsumer) |
| 6.7 | Remove `list_hashes` from ObjectStore | Discarded — retained as optional method for conductor GC |
| 6.8 | Eliminate MetadataStore file — rely on journal replay | Applied (§2.5 MetadataStore) |
| 6.9 | Drop the "eventually" mental model for CasApi user | Applied (§2.1 Guarantees); kept for internal layers (backend, GC, optimization) as decoupling mechanism |
| 6.10 | Ship V3-only delta writes after transition | Discarded — not ready; V3 co-exists with V1/V2 read-side |
| 6.11 | Let concurrency model be implicit | Applied (§2.8 Concurrency Model); no separate MultiReaderCas/MultiWriterCas |
| 6.12 | Generalize metadata store semantics | Applied (§2.5 MetadataStore); merged with 6.8 — in-memory only, journal replay |
