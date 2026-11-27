# `provider/` — Tool provider source descriptors

Three phases: **resolve** (per-tool descriptors, one per OS slot) -> **fetch** (download bytes, candidates in order) -> **process** (extract, CAS import, content maps, sandbox payload). Per-OS descriptors feed `provider::mod::resolve_tool_fetch()`. Ids `sd`, `echo`, `archive`, `export`, `fs`, `import`; see `crate::tools::preset::AGENTS.md`, `crate::tools::provider::mod.rs`.

## Invariants

### OS-conditional selector flavors

`crate::tools::helpers` provides two selectors; **do not mix them up**.

| Function | Input | Output |
| --- | --- | --- |
| `build_os_conditional_selector` | Flat binaries | Prepends `{os}/`: `{"linux": "sd-x86_64-linux"}` -> `linux/sd-x86_64-linux` |
| `build_raw_os_conditional_selector` | Pre-qualified paths | As-is: `{"macos": "deps/ffmpeg/macos/ffmpeg"}` -> `deps/ffmpeg/macos/ffmpeg` |

Companion dep keys `deps/{tool_id}/{os}/{filename}` (arch `deps/{tool_id}/{os}/{arch}/{filename}` for dual-arch slots, e.g. ffmpeg macOS) already contain OS: MUST use `build_raw_os_conditional_selector`; other selector double-prefixes (`macos/deps/ffmpeg/macos/ffmpeg`).

### Content-map key shapes

`process_tool_sources`: one source -> flat (`{os}/` archives, `{os}/{filename}` binaries); two (ffmpeg `aarch64` + `x86_64` macOS) -> arch (`{os}/{arch}/`, `{os}/{arch}/{filename}`); same-OS same-arch duplicates rejected. `os_exec_paths` stays os-keyed: dual-arch records host-arch path prefixed `{arch}/`.

### Item semantics and progress

`MultiItemBudget`/counting/tracking: `workflow-runtime.instructions.md`. SI prefixes 1000-based: `format_count`/`format_rate` decimal.
