# `provider/` — Tool provider source descriptors

Three-phase provisioning pipeline: **resolve → fetch → process**.
Each file defines per-OS source descriptors consumed by
`provider::mod::resolve_tool_fetch()`.

Phases:

1. **Resolve** — Select the correct source URL/launcher for the target OS
2. **Fetch** — Download bytes, extract archives, generate launcher scripts
3. **Process** — Import into CAS, create content maps, build sandbox payload

Dispatched tool IDs: `sd`, `echo`, `archive`, `export`, `fs`, `import`.

See `crate::tools::preset::AGENTS.md` for the corresponding preset builders.
See `crate::tools::provider::mod.rs` for the pipeline implementation and types.

## Invariants

### OS-conditional selector flavors

`crate::tools::helpers` provides two selector functions. **Do not mix them up.**

| Function | Input shape | Output | Use case |
| --- | --- | --- | --- |
| `build_os_conditional_selector` | Flat binary names (no OS in path) | Prepends `{os}/` to each value | Command path selectors: `{"linux": "sd-x86_64-linux"}` → `linux/sd-x86_64-linux` |
| `build_raw_os_conditional_selector` | Pre-qualified paths (OS already in path) | Uses values as-is | Companion dep selectors: `{"macos": "deps/ffmpeg/macos/ffmpeg"}` → `deps/ffmpeg/macos/ffmpeg` |

**Invariant:** Inlined companion dep content map keys follow `deps/{tool_id}/{os}/{filename}`. The OS directory component is already embedded in the key. Callers MUST use `build_raw_os_conditional_selector` for these paths. Using `build_os_conditional_selector` produces double-prefixed paths (`macos/deps/ffmpeg/macos/ffmpeg`) that break runtime resolution.

### Item semantics per phase

Progress item counters (`items_done`/`total`) measure **distinct operations in each phase**, not OS-platform count:

| Phase | Total | What each item represents |
| --- | --- | --- |
| Resolve | `1` | One `resolve_tool_fetch()` call |
| Fetch | `sources.len()` | One download or launcher generation per source |
| Process | `∑(2 if archive else 1)` per source | Decompress + compress for archives, CAS-import for binaries |

For process, archive sources contribute **2 items**: item `i` tracks decompress progress (compressed bytes consumed), item `i+1` tracks compress progress (decompressed bytes packed). Binary/launcher sources contribute **1 item** for direct CAS import.

The `total_items` field is not part of `ResolvedToolFetch` — consumers derive phase-specific totals from `sources.len()` or the literal `1` for resolve.

- **Bytes are always aggregate**: `ProviderProgressSnapshot.bytes` reports values summed across all sources/entries in the phase. Individual source/entry sizes are never exposed. This is an architectural invariant that decouples the bridge adapter and progress bar from internal provider structure.
- **SI prefixes are 1000-based**: `format_count` and friends use SI decimal prefixes (`k` = 1,000, `M` = 1,000,000, `G` = 1,000,000,000), not binary prefixes (`Ki` = 1,024, etc.). Progress rates (`format_rate`) follow the same convention.

### Counting mechanism invariants

The byte-level progress tracking system across all three phases guarantees:

- **Monotonic non-decreasing**: position never decreases within a phase.
- **Position never exceeds total**: per-item `pos ≤ total` enforced by hard `assert!`.
- **Eventual completion**: position reaches total at the end of each phase.
- **Per-format extraction tracking**:
  - **ZIP**: proportional estimation via `(written * entry_compressed) / entry_decompressed` —
    endpoint is exact (final position = entry compressed size), mid-entry is approximate
    (assumes uniform compression ratio across the entry).
  - **tar.gz**: [`CountingReader`] tracks compressed bytes consumed by `GzDecoder`.
    GzDecoder may read ahead of the compressed stream, causing occasional progress
    "jumps" of up to the decoder's internal buffer size (~32 KB). Per-entry callbacks
    after each tar entry smooth the visual appearance.
  - **tar.xz**: [`CountingReader`] tracks compressed bytes consumed by `XzDecoder`.
    `XzDecoder::total_in()` could provide exact tracking but `CountingReader` is used
    for more responsive sub-block progress (xz block boundaries can be multi-MB).
  - **Compress (packing to ZIP)**: file sizes are accumulated as decompressed bytes
    are written. ZIP metadata overhead (~KB) is not included in the total, causing
    a small undercount vs final ZIP file size — negligible for payloads in the
    MB–GB range.
- **Fidelity over precision**: progress tracking prioritizes smooth visual updates
  and monotonic progress over byte-exact accuracy. All counting paths guarantee
  monotonicity and eventual completion.

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

## deno permission wrapper contract

yt-dlp invokes deno as `[deno, 'run', *options, '-']` with `--no-config` and no
`--allow-*` flags. deno 2.x enforces a permission sandbox, so the `ws` npm
package's `WS_NO_BUFFER_UTIL` env access is denied (`NotCapable`), breaking the
YouTube JS-challenge solver (HTTP 403). Because yt-dlp's `--js-runtimes` accepts
only `RUNTIME[:PATH]` (no args), the only fix is to wrap the deno binary:
rename the real executable and place a shim that re-execs it with `--allow-all`.

`wrap_deno_binary(os_dir, exec_rel)` in `mod.rs` implements the wrapper. Its
invariants (tracked as `S-DENO-1..6` in the workspace coverage matrix):

- **S-DENO-1 (layout-agnostic discovery):** `wrap_deno_binary` locates the deno
  executable via the `exec_rel` path discovered by `find_os_executable`, which
  already handles nested per-OS subdirectories (`windows/deno.exe`,
  `darwin/deno`, `linux/deno`) and the flat `deno` case. It MUST NOT assume a
  hardcoded flat `os_dir/deno` path.
- **S-DENO-2 (in-place rename):** The real binary is renamed to `deno.real`
  (or `deno.real.exe` on Windows) **within the same directory** as the original
  executable, so the shim re-execs it via a sibling-relative path
  (`$(dirname "$0")/deno.real` on unix, `%~dp0deno.real.exe` on Windows).
- **S-DENO-3 (shim contract):** A shim is written at the original executable
  path that injects `--allow-all` **after** the first arg (typically `run`).
  deno 2.x only accepts `--allow-all` as a post-subcommand flag, not at top
  level. On unix the shim is
  `#!/bin/sh\nfirst="$1"; shift; exec "$(dirname "$0")/{real_name}" "$first" --allow-all "$@"\n`
  and is marked executable (`+0o111`). On Windows it is
  `@set "first=%1"\r\n@shift\r\n@\"%~dp0{real_name}\" %first% --allow-all %*\r\n`.
- **S-DENO-4 (exec_path stability):** The `exec_path` returned by
  `process_single_source` is UNCHANGED by the wrap — it still points at the shim
  location.
- **S-DENO-5 (failure surfaced):** If the discovered deno binary is absent, the
  error is surfaced as a clear `ENOENT` I/O error (no silent success).
- **S-DENO-6 (regression guard):** After a full sync in the online demo, deno's
  spec MUST be present in the generated doc and the deno process bar MUST NOT
  end in `[W]`.
