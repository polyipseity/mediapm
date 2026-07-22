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

### Progress sizing policy (input-size principle)

All progress bars measure **input work**, never output work:

- **Fetch phase**: counts bytes downloaded over the network (wire bytes).
  The user is waiting on bandwidth consumption — decompressed size is
  irrelevant to progress perception.
- **Postprocess phase (extraction)**: counts compressed archive bytes
  consumed by the decompressor. The user is waiting on local I/O and CPU
  for decompression — decompressed output size does not represent the
  work remaining.
- **General principle**: progress tracks the resource consumed (network
  bytes, storage bytes read), not the resource produced (uncompressed
  files, CAS blobs).

> **Note:** This policy is now the official progress bar policy. See [`src/mediapm-conductor/AGENTS.md`](../../../AGENTS.md) for the full canonical rules, constants, and invariants.

### Per-entry progress during archive extraction

When a postprocess source is an archive (ZIP, tar.gz, tar.xz), the extraction
loop fires one `ProviderProgressSnapshot` callback per extracted entry
(file or directory) within that archive. This ensures the progress bar
updates smoothly during multi-second archive extractions (for example,
yt-dlp or ffmpeg) instead of freezing until extraction completes.

- **ZIP**: bytes counter uses `file.compressed_size()` (compressed size
  from the ZIP central directory) as each entry's weight. The compressed
  sizes naturally sum to approximately `source.bytes.len()` (slightly less
  due to ZIP local-file-header overhead). This ensures the position stays
  within the compressed-size budget and never overshoots the outer total.
- **tar.gz**: bytes counter uses compressed bytes consumed by the
  decompressor (tracked via a `CountingReader` wrapper).
- **tar.xz**: bytes counter uses compressed bytes consumed by the
  decompressor (tracked via a `CountingReader` wrapper).
- **Per-entry callbacks are nested within the per-source item**: the
  `items` counter still advances per source, not per entry — the per-entry
  callbacks are sub-steps within one source's item. The bytes counter
  tracks total extraction progress within the source's item.

Within each entry, progress callbacks fire every `COMPRESSED_CHUNK` (128 KB) of compressed bytes consumed. For ZIP archives, the chunked `io::copy` loop estimates compressed position proportional to decompressed bytes written. For tar archives, the `CountingReader` fires periodic callbacks as compressed bytes flow through.

### Progress monotonicity invariants

All progress callbacks must satisfy:

- **Position is monotonically non-decreasing** within a single phase.
  Position must never decrease (no backward jumps).
- **Total is monotonically non-decreasing** within a single phase.
  Total must never decrease within the same source. Total may increase
  when a new source's size becomes known (fetch phase: Content-Length
  arrives; postprocess phase: per-source adjustment for decompressed
  cost). The official policy allows bounded non-monotonicity (<10%
  decrease) when required for ASAP info propagation.
- **Postprocess total adjusts per source**: `agg_total_bytes` starts as
  the sum of compressed sizes and is re-adjusted after each source to
  `agg_total_bytes - total_compressed + source_input_cost`. For archives
  (`source_input_cost = compressed + decompressed`) this increases the
  total to reflect actual extraction work. For binaries (`source_input_cost
  = compressed`) the total stays unchanged.
- **Position never exceeds total** at any point. Position strictly ≤ total.
