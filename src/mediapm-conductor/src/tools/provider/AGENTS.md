# `provider/` — Tool provider source descriptors

Three-phase provisioning pipeline: **resolve → fetch → process**. Each file defines per-OS source descriptors consumed by `provider::mod::resolve_tool_fetch()`.

Phases:

1. **Resolve** — Select the per-tool source descriptors, one per OS slot, each carrying its URL candidates
2. **Fetch** — Download the bytes of every source, trying each source's candidates in order
3. **Process** — Extract archives, import into CAS, create content maps, build sandbox payload

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

**Invariant:** Inlined companion dep content map keys follow `deps/{tool_id}/{os}/{filename}` (arch-segmented as `deps/{tool_id}/{os}/{arch}/{filename}` when the dep's OS slot is dual-arch — ffmpeg's macOS chain). The OS directory component is already embedded in the key. Callers MUST use `build_raw_os_conditional_selector` for these paths. Using `build_os_conditional_selector` produces double-prefixed paths (`macos/deps/ffmpeg/macos/ffmpeg`) that break runtime resolution.

### Content-map key shapes

`process_tool_sources` keys payloads by OS-label slot occupancy: an OS label with **one** source keeps the flat shape (`{os}/` for archives, `{os}/{filename}` for binaries), while an OS label shared by **two** sources — a dual-arch slot, e.g. ffmpeg's `aarch64` + `x86_64` macOS chain — gains an arch segment (`{os}/{arch}/`, `{os}/{arch}/{filename}`). Same-OS same-arch duplicates are still rejected outright. `os_exec_paths` stays os-keyed: a dual-arch slot records only the host-arch payload's exec path prefixed with `{arch}/` (so the `{os}/`-prefixed selector resolves to the real location); a single-source slot is unchanged.

### Item semantics and progress tracking

Item semantics per phase, counting mechanism invariants, per-format extraction tracking, and the `MultiItemBudget` API are fully documented in `progress-budget.instructions.md` ("Phase-loop mapping" and "Counting mechanism accuracy guarantees"). Read that file before editing provider progress code.

SI prefixes are 1000-based: `format_count` and `format_rate` use decimal SI prefixes, not binary.
