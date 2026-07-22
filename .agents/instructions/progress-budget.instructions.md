---
description: "Use when editing the ByteBudget progress size tracking system in mediapm-utils and the provider pipeline. Covers ByteBudget type, SourceProgress, extraction-helper callback protocol, and phase-loop mapping."
name: "Progress Budget and ByteBudget"
applyTo: "src/mediapm-utils/src/progress.rs, src/mediapm-conductor/src/tools/provider/mod.rs"
---

# Progress budget (`ByteBudget`) architecture

## Purpose

`ByteBudget` is the single, generic mechanism for tracking byte-level progress
across all provider phases (Fetch and Postprocess). It replaces ad-hoc
`agg_completed_bytes`, `agg_total_bytes`, `source_input_cost`, and raw
`bytes_done_before`/`bytes_total` parameters that were threaded through
extraction helpers.

## Core type

```rust
/// Thread-safe progress size tracker.
///
/// Tracks (position, total) where position ≤ total at all times. Both fields
/// use `AtomicU64` internally, making this type `Send + Sync` without external
/// locking. Safe to read from one thread (progress bar renderer) while writing
/// from another (download worker).
///
/// # Invariants (hard-fail with `assert!`)
///
/// - `pos ≤ total` — enforced on every mutation.
/// - `pos` never decreases.
/// - `total` may increase or decrease (via `adjust` or `reconcile`).
pub struct ByteBudget {
    pos: AtomicU64,
    total: AtomicU64,
}
```

Methods:

| Method                        | Behavior                                                                                 | pos≤total assert?    |
| ----------------------------- | ---------------------------------------------------------------------------------------- | -------------------- |
| `new(initial_total)`          | pos=0, total=initial_total                                                               | ✅                   |
| `pos()` / `total()`           | Atomic load (Acquire)                                                                    | —                    |
| `snap()`                      | `(pos, total)`                                                                           | —                    |
| `advance(amount)`             | `pos += amount` via `compare_exchange_weak` loop                                         | ✅ hard              |
| `set_pos(pos)`                | Absolute set (Release). Must be ≥ current pos.                                           | ✅ hard              |
| `adjust(delta: i64)`          | total += delta (positive or negative). Uses `compare_exchange_weak` for negative deltas. | ✅ hard              |
| `reconcile(estimate, actual)` | `total += (actual - estimate)`. Wrapper around `adjust`.                                 | ✅ hard via `adjust` |

**Hard assert**: `assert!` (always compiled, not `debug_assert!`). Violation
means a bug in size tracking logic — the invariant `pos ≤ total` is
non-negotiable.

**All atomic operations** use `Ordering::AcqRel`/`Acquire`/`Release` (not
`Relaxed`) so a progress bar thread sees a consistent snapshot.

### `advance`

Uses `compare_exchange_weak` loop to handle concurrent callers:

```text
fn advance(amount):
    loop:
        old = pos.load(Acquire)
        new = old + amount
        total = total.load(Acquire)
        assert(new ≤ total)
        if pos.compare_exchange_weak(old, new, AcqRel, Acquire).is_ok():
            return
```

### `set_pos`

Single load-store; assumes sequential completion (one source at a time):

```text
fn set_pos(pos):
    total = total.load(Acquire)
    assert(pos ≤ total)
    current = pos.load(Acquire)
    assert(pos ≥ current)
    pos.store(pos, Release)
```

### `adjust`

Uses `compare_exchange_weak` loop for thread safety:

```text
fn adjust(delta):
    loop:
        old = total.load(Acquire)
        new = if delta ≥ 0: old.saturating_add(delta) else: old.saturating_sub(|delta|)
        pos = pos.load(Acquire)
        assert(pos ≤ new)
        if total.compare_exchange_weak(old, new, AcqRel, Acquire).is_ok():
            return
```

### `reconcile`

Delegates to `adjust`:

```text
fn reconcile(estimate, actual):
    match actual.cmp(estimate):
        Greater → adjust((actual - estimate) as i64)
        Less → adjust(-((estimate - actual) as i64))
        Equal → // no-op
```

Total may increase _or decrease_ when reconciling. If decreasing total would
violate `pos ≤ total`, `adjust` panics — this is correct behavior (the
estimate was too low or position got ahead of total, which is a bug).

## Extraction-helper callback protocol

Extraction helpers (`extract_zip`, `extract_tar_gz`, `extract_tar_xz`,
`extract_archive`, `CountingReader`, `pack_directory_to_uncompressed_zip_bytes`,
`pack_directory_entries`) MUST NOT receive aggregate progress parameters.

**Old signature (removed)**:
`bytes_done_before: u64, bytes_total: u64, items: (u64, u64)`.

**New signature**: `source_total: u64, local_cb: Option<&dyn Fn(u64)>`

- `source_total` is the total work for this source (precomputed by outer loop).
- `local_cb` fires with `local_pos: u64` where `local_pos ∈ [0, source_total]`.
  The helper never knows about aggregate positions.
- `items` is removed — item tracking belongs exclusively in the outer phase
  loop.
- The helper does NOT call `set_pos`/`advance` on the shared budget. It only
  fires position snapshots through the local callback.

## Phase-loop mapping

The outer phase loop (`fetch_tool_sources`, `postprocess_tool_sources`) owns
the `ByteBudget` and maps local callbacks to aggregate progress.

### Postprocess phase loop

```text
budget = ByteBudget::new(initial_total)  # sum of compressed bytes
for (idx, source) in entries:
    source_cost = precompute_input_cost(source, archive_format)
    budget.reconcile(estimate=bytes.len(), source_cost)

    phase_base = budget.pos()  # snapshot before work starts
    local_cb = |local_pos| {
        agg_pos = phase_base + min(local_pos, source_cost)
        agg_total = budget.total()  # read-only, no mutation
        cb(ProviderProgressSnapshot {
            phase: Postprocess,
            items: (idx, total_items),
            bytes: (agg_pos, agg_total),
        })
    }

    processed = process_single_source(bytes, ..., source_cost, Some(&local_cb))
    budget.set_pos(phase_base + processed.input_cost)

    cb(ProviderProgressSnapshot {
        phase: Postprocess,
        items: (idx + 1, total_items),
        bytes: budget.snap(),
    })
```

Key: `local_cb` only **reads** `budget.total()` — it never mutates the
budget. Position catch-up (`set_pos`) happens once in the outer loop after
the source completes. This avoids races in concurrent scenarios: multiple
workers can fire `local_cb` in parallel without mutating shared state.

### Fetch phase loop

```text
suffix = suffix_expected per source
budget = ByteBudget::new(suffix[0])

for (idx, source) in sources:
    estimate = source.expected_size.or(size_hint_bytes).unwrap_or(0)
    remaining = suffix[idx + 1]

    # Reconcile before download if HEAD probe gave actual size
    budget.reconcile(estimate, source.expected_size.unwrap_or(0))

    phase_base = budget.pos()

    download(source, |downloaded_bytes, content_len| {
        current_estimate = content_len.unwrap_or(downloaded_bytes)
        display_pos = phase_base + downloaded_bytes
        display_total = phase_base + current_estimate + remaining

        # Adjust budget total to reflect latest estimate
        # (may increase or decrease vs current total)
        if display_total != budget.total() {
            diff = display_total as i64 - budget.total() as i64
            budget.adjust(diff)
        }

        cb(ProviderProgressSnapshot {
            phase: Fetch,
            items: (idx, total),
            bytes: (display_pos, budget.total()),
        })
    })

    budget.reconcile(estimate, actual_bytes.len())
    budget.set_pos(phase_base + actual_bytes.len())
    cb(ProviderProgressSnapshot {
        phase: Fetch,
        items: (idx + 1, total),
        bytes: budget.snap(),
    })
```

## ProcessedSource return type

`process_single_source` returns a struct instead of a tuple:

```rust
pub struct ProcessedSource {
    pub content_map: BTreeMap<String, String>,
    pub exec_path: String,
    /// Total work cost (compressed + decompressed for archives,
    /// bytes.len() for binaries).
    pub input_cost: u64,
}
```

## CountingReader (no longer uses atomics)

`CountingReader` wraps a byte slice to track compressed bytes consumed during
tar extraction. Since extraction is single-threaded (sequential source
processing), the `AtomicU64` for `bytes_read` is replaced with a plain `u64`.

```rust
struct CountingReader<'a> {
    cursor: std::io::Cursor<&'a [u8]>,
    bytes_read: u64,
    last_callback_threshold: u64,
    local_cb: Option<&'a dyn Fn(u64)>,
    source_total: u64,
}
```

On each `read()`, increment `bytes_read` and if `bytes_read / COMPRESSED_CHUNK`
exceeds `last_callback_threshold`, fire `local_cb(bytes_read.min(source_total))`.

## Placement

- `ByteBudget` lives in `mediapm-utils/src/progress.rs` in the always-available
  section (before `#[cfg(feature = "progress")] mod inner`).
- The `ProcessedSource` struct lives in
  `src/mediapm-conductor/src/tools/provider/mod.rs`.
- This instruction file replaces the "Progress monotonicity invariants",
  "Progress sizing policy", and "Per-entry progress" sections in the existing
  AGENTS.md files.
