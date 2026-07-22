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

### Per-entry progress during archive extraction

When a postprocess source is an archive (ZIP, tar.gz, tar.xz), the extraction
loop fires one `ProviderProgressSnapshot` callback per extracted entry
(file or directory) within that archive. This ensures the progress bar
updates smoothly during multi-second archive extractions (for example,
yt-dlp or ffmpeg) instead of freezing until extraction completes.

- **ZIP**: bytes counter uses decompressed entry sizes from the central
directory, providing accurate per-entry progress.
- **tar.gz / tar.xz**: bytes counter uses compressed bytes consumed (from
a `CountingReader` wrapper), providing a smoothed estimate of extraction
progress.
- **Per-entry callbacks are nested within the per-source item**: the
`items` counter still advances per source, not per entry — the per-entry
callbacks are sub-steps within one source's item. The bytes counter
tracks total extraction progress within the source's item.
