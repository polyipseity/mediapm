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

- **Bytes are always aggregate**: `ProviderProgressSnapshot.bytes` reports values summed across all sources/entries in the phase. Individual source/entry sizes are never exposed. This is an architectural invariant that decouples the bridge adapter and progress bar from internal provider structure.
- **SI prefixes are 1000-based**: `format_count` and friends use SI decimal prefixes (`k` = 1,000, `M` = 1,000,000, `G` = 1,000,000,000), not binary prefixes (`Ki` = 1,024, etc.). Progress rates (`format_rate`) follow the same convention.
