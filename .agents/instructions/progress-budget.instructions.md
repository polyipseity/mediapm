---
description: "Use when editing the MultiItemBudget per-item progress tracking system in mediapm-utils and the provider pipeline. Covers MultiItemBudget type, ItemBudget, extraction-helper callback protocol, and phase-loop mapping."
name: "Progress Budget and MultiItemBudget"
applyTo: "src/mediapm-utils/src/progress.rs, src/mediapm-conductor/src/tools/provider/mod.rs"
---

# Progress budget (`MultiItemBudget`) architecture

## Purpose

`MultiItemBudget` is the primary mechanism for tracking byte-level progress across all three provider phases (Resolve, Fetch, and Postprocess). Each tracked entity (a tool source, a metadata URL) gets its own `ItemBudget` — progress is the aggregate of all items. `ByteBudget` (the legacy single-position type) still exists in the library but is no longer used in the provider pipeline.

## Core type

```rust
/// Thread-safe collection of per-item progress budgets.
pub struct MultiItemBudget {
    items: Vec<ItemBudget>,
}

struct ItemBudget {
    pos: AtomicU64,
    total: AtomicU64,
}
```

Each `ItemBudget` tracks `(position, total)` where `position ≤ total`. Both fields use `AtomicU64`, making the type `Send + Sync` without external locking. Multiple items can be advanced concurrently (e.g., per-chunk download callbacks) while a progress bar renderer reads the aggregate snapshot.

Methods:

| Method                       | Behavior                                                   | pos≤total assert? |
| ---------------------------- | ---------------------------------------------------------- | ----------------- |
| `new()`                      | Empty budget, no items                                     | —                 |
| `with_capacity(capacity)`    | Pre-allocated for `capacity` items                         | —                 |
| `add_item(total)`            | Push one item with pos=0, total=`total`                    | —                 |
| `item_count()`               | Number of items                                            | —                 |
| `set_total(item_idx, total)` | Set total for item (Release store). Must be ≥ current pos. | ✅ hard           |
| `advance(item_idx, amount)`  | `pos += amount` per item via `compare_exchange_weak` loop  | ✅ hard           |
| `set_pos(item_idx, pos)`     | Absolute set (Release). Must be ≥ current pos.             | ✅ hard           |
| `snap(item_idx)`             | `(pos, total)` for one item                                | —                 |
| `aggregate()`                | `(sum_pos, sum_total)` across all items                    | —                 |

**Hard assert**: `assert!` (always compiled). Violation means a bug in size tracking logic — `pos ≤ total` per item is non-negotiable.

**All atomic operations** use `Ordering::AcqRel`/`Acquire`/`Release` so a progress bar thread sees a consistent snapshot.

**Indeterminate items**: Items with `total == 0` are counted in `item_count()` but contribute 0 bytes to the aggregate. This allows creating items before their total is known (e.g., before a Content-Length header arrives).

### `advance` (per-item `compare_exchange_weak` loop)

```text
fn advance(item_idx, amount):
    item = items[item_idx]
    loop:
        old = item.pos.load(Acquire)
        new = old + amount
        total = item.total.load(Acquire)
        assert(new ≤ total)
        if item.pos.compare_exchange_weak(old, new, AcqRel, Acquire).is_ok():
            return
```

### `set_pos` (per-item, single load-store)

```text
fn set_pos(item_idx, pos):
    item = items[item_idx]
    total = item.total.load(Acquire)
    assert(pos ≤ total)
    current = item.pos.load(Acquire)
    assert(pos ≥ current)
    item.pos.store(pos, Release)
```

## Extraction-helper callback protocol

Extraction helpers (`extract_zip`, `extract_tar_gz`, `extract_tar_xz`, `extract_archive`, `CountingReader`) receive only a `local_cb: Option<&dyn Fn(u64)>`. The `source_total` parameter has been removed — helpers never know the total work for the source.

- `local_cb` fires with `local_pos: u64` where `local_pos` is the current compressed bytes consumed. The helper does NOT call `set_pos`/`advance` on the shared budget — it only fires position snapshots through the callback.
- The outer phase loop (`process_single_source`) creates the callback internally from the `MultiItemBudget` using `budget.set_pos(item_idx, pos)`. This maps the helper's local position directly to the item's absolute position.

## Phase-loop mapping

The outer phase loop owns the `MultiItemBudget` and creates one item per tracked entity (source, URL).

### Fetch phase loop (`fetch_tool_sources`)

```text
budget = MultiItemBudget::with_capacity(sources.len())
for src in sources:
    est = src.expected_size.or(size_hint_bytes).unwrap_or(0)
    budget.add_item(est)

for (idx, source) in sources:
    match source.producer:
        Fetch { urls }:
            if cache hit:
                budget.set_total(idx, cached.len())
                budget.advance(idx, cached.len())
            else:
                budget.set_total(idx, estimate)
                fetch_bytes_from_candidates(urls, ..., &budget, idx, ...)
                # Inside fetch (per chunk):
                #   budget.set_total(idx, content_length_estimate)
                #   budget.advance(idx, chunk.len())
                #   cb(aggregate snapshot)  ← fires after EACH chunk
        GenerateLauncher { .. }:
            budget.set_total(idx, launcher_size)
            budget.advance(idx, launcher_size)

    # Per-source callback (cached/launcher: only here; fetch: in addition to per-chunk)
    cb(ProviderProgressSnapshot {
        phase: Fetch,
        items: (idx + 1, total),        # (completed_items, total_items)
        bytes: budget.aggregate(),       # (sum_pos, sum_total)
    })
```

Key: `fetch_bytes_from_candidates` receives `&MultiItemBudget` + `item_idx` + `progress_cb`. During download it calls `budget.set_total` + `budget.advance` per HTTP chunk and fires the progress callback with the aggregate after each chunk. For cached sources the advance is a single step; for launcher sources it is immediate. The end-of-source callback in the outer loop provides additional coverage for cached/launcher sources where no per-chunk callbacks fire.

### Postprocess phase loop (`postprocess_tool_sources`)

Archive sources use **2 budget items** (decompress + compress); binary/launcher sources use **1 item** (CAS import).

```text
total_items = sum(2 if is_archive(source) else 1 for source in entries)
budget = MultiItemBudget::with_capacity(total_items)
for entry in entries:
    is_archive = is_archive_source(&entry.producer)
    budget.add_item(expected_size.unwrap_or(bytes.len()))  # item i: decompress or binary
    if is_archive:
        budget.add_item(0)                                   # item i+1: compress (total set later)

next_item_idx = 0
for source in entries:
    is_archive = is_archive_source(&source.producer)
    item_count = 2 if is_archive else 1

    process_single_source(bytes, ..., &budget, next_item_idx, item_count).await
    # Inside archive arm:
    #   callback 1 → budget.set_pos(item_idx, pos)       # decompress via extraction callback
    #   callback 2 → budget.set_pos(item_idx + 1, pos)   # compress via pack callback
    # Inside binary arm:
    #   budget.advance(item_idx, bytes.len())             # single CAS import

    cb(ProviderProgressSnapshot {
        phase: Postprocess,
        items: (next_item_idx + item_count, total_items),  # (completed_items, total_items)
        bytes: budget.aggregate(),                          # (sum_pos, sum_total)
    })
    next_item_idx += item_count
```

`process_single_source` receives `budget: &MultiItemBudget`, `item_idx: usize`, and `item_count: usize`. For archive sources (`item_count=2`): it creates a decompress callback `\|pos| budget.set_pos(item_idx, pos)` for extraction and a compress callback `\|pos| budget.set_pos(item_idx + 1, pos)` for packing. After each sub-phase it calls `budget.set_pos(...)` to ensure completion. For binary sources (`item_count=1`): it calls `budget.advance(item_idx, bytes.len())` for the single CAS import step.

### Resolve phase

`resolve_tool_metadata` (when it uses progress) creates a `MultiItemBudget` with `items = metadata_urls.len()`. Each URL resolution (GitHub API call, cache lookup) advances the corresponding item. After all URLs resolve, `budget.aggregate()` reports complete progress. The `ProviderPhase` is `Resolve` and `items` reports `(resolved_count, total_urls)`.

## ProviderPhase enum

```rust
pub enum ProviderPhase {
    Resolve,
    Fetch,
    Postprocess,
}

pub struct ProviderProgressSnapshot {
    pub phase: ProviderPhase,
    /// Items completed vs total: (completed, total).
    /// Resolve: metadata URLs resolved.
    /// Fetch: sources fetched.
    /// Postprocess: sources postprocessed.
    pub items: (u64, u64),
    /// Bytes completed vs total: (completed, total).
    pub bytes: (u64, u64),
}
```

The `items` field reports `(completed_items, total_items)` in fetch and postprocess — this drives the `{tool} [process] 1/3` → `2/3` → `3/3` prefix in progress bars.

## ByteBudget (legacy)

`ByteBudget` still exists in `mediapm-utils/src/progress.rs` as a single-position `(pos, total)` tracker with `advance`, `set_pos`, `adjust`, and `reconcile` methods. It is NOT used in the provider pipeline. All provider progress now goes through `MultiItemBudget`. `ByteBudget` remains available for other use cases that need a simple atomic byte counter.

## CountingReader (Cell-based, no atomics)

```rust
struct CountingReader<'a> {
    cursor: std::io::Cursor<&'a [u8]>,
    bytes_read: &'a Cell<u64>,
    last_cb_pos: Cell<u64>,
    progress_cb: Option<&'a dyn Fn(u64)>,
}
```

Wraps a byte slice to track compressed bytes consumed during tar extraction. Uses `Cell<u64>` (not `AtomicU64`) since extraction is single-threaded. Fires `progress_cb` at `COMPRESSED_CHUNK` (128 KB) boundaries. The callback maps to `budget.set_pos(item_idx, pos)` created by `process_single_source`.

## Placement

- `MultiItemBudget` and `ItemBudget` live in `mediapm-utils/src/progress.rs` in the always-available section (before `#[cfg(feature = "progress")] mod inner`).
- `ProviderPhase`, `ProviderProgressSnapshot`, and `ProcessedSource` live in `src/mediapm-conductor/src/tools/provider/mod.rs`.
- This instruction file replaces the "Progress monotonicity invariants", "Progress sizing policy", and "Per-entry progress" sections in the existing AGENTS.md files.
