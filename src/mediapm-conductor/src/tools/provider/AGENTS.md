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
| Postprocess | `sources.len()` | One extraction or CAS-import per source |

The `total_items` field is not part of `ResolvedToolFetch` — consumers derive phase-specific totals from `sources.len()` or the literal `1` for resolve.

- **Bytes are always aggregate**: `ProviderProgressSnapshot.bytes` reports values summed across all sources/entries in the phase. Individual source/entry sizes are never exposed. This is an architectural invariant that decouples the bridge adapter and progress bar from internal provider structure.
- **SI prefixes are 1000-based**: `format_count` and friends use SI decimal prefixes (`k` = 1,000, `M` = 1,000,000, `G` = 1,000,000,000), not binary prefixes (`Ki` = 1,024, etc.). Progress rates (`format_rate`) follow the same convention.

### Progress size tracking (ByteBudget)

Progress size tracking uses the [`ByteBudget`](../../../../.agents/instructions/progress-budget.instructions.md)
architecture. See that file for the full `ByteBudget` API, extraction-helper
callback protocol, and phase-loop mapping.

Key differences from the legacy ad-hoc system:

- Extraction helpers use local callbacks only (`source_total: u64, local_cb: Option<&dyn Fn(u64)>`).
- The outer phase loop owns a `ByteBudget` instance and maps local → aggregate progress.
- `ByteBudget` uses `AtomicU64` internals for thread safety.
- `pos ≤ total` is enforced by hard `assert!` on every mutation.
- Total may increase or decrease (via `adjust`/`reconcile`).
