# `provider/` — Tool provider source descriptors

Three-phase provisioning pipeline: **resolve → fetch → postprocess**.
Each file defines per-OS source descriptors consumed by
`provider::mod::resolve_tool_fetch()`.

Phases:

1. **Resolve** — Select the correct source URL/launcher for the target OS
2. **Fetch** — Download bytes, extract archives, generate launcher scripts
3. **Postprocess** — Import into CAS, create content maps, build sandbox payload

Dispatched tool IDs: `sd`, `echo`, `archive`, `export`, `fs`, `import`.

See `crate::tools::preset::AGENTS.md` for the corresponding preset builders.
See `crate::tools::provider::mod.rs` for the pipeline implementation and types.

## Invariants

### Item semantics per phase

Progress item counters (`items_done`/`total`) measure **distinct operations in each phase**, not OS-platform count:

| Phase | Total | What each item represents |
|-------|-------|--------------------------|
| Resolve | `1` | One `resolve_tool_fetch()` call |
| Fetch | `sources.len()` | One download or launcher generation per source |
| Postprocess | `∑(2 if archive else 1)` per source | Decompress + compress for archives, CAS-import for binaries |

For postprocess, archive sources contribute **2 items**: item `i` tracks decompress progress (compressed bytes consumed), item `i+1` tracks compress progress (decompressed bytes packed). Binary/launcher sources contribute **1 item** for direct CAS import.

The `total_items` field is not part of `ResolvedToolFetch` — consumers derive phase-specific totals from `sources.len()` or the literal `1` for resolve.

- **Bytes are always aggregate**: `ProviderProgressSnapshot.bytes` reports values summed across all sources/entries in the phase. Individual source/entry sizes are never exposed. This is an architectural invariant that decouples the bridge adapter and progress bar from internal provider structure.
- **SI prefixes are 1000-based**: `format_count` and friends use SI decimal prefixes (`k` = 1,000, `M` = 1,000,000, `G` = 1,000,000,000), not binary prefixes (`Ki` = 1,024, etc.). Progress rates (`format_rate`) follow the same convention.

### Progress size tracking (MultiItemBudget)

Progress size tracking uses the [`MultiItemBudget`](../../../../.agents/instructions/progress-budget.instructions.md)
architecture: a per-item budget model where each tool source or archive entry
is one budget item. See that file for the full `MultiItemBudget` API,
extraction-helper callback protocol, and phase-loop mapping.

Key differences from the legacy ad-hoc system:

- Extraction helpers use local callbacks only (`local_cb: Option<&dyn Fn(u64)>`);
  the `source_total` parameter has been removed.
- The outer phase loop owns a `MultiItemBudget` instance and calls `aggregate()`
  to derive combined progress for progress bars.
- `ByteBudget` still exists in the codebase but is **unused in the provider
  pipeline** — all new code uses `MultiItemBudget`.
- `MultiItemBudget` uses `AtomicU64` per item for thread safety.
- `pos ≤ total` is enforced by hard `assert!` on every mutation per item.
