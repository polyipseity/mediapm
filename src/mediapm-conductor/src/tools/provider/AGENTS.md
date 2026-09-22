# `provider/` — Tool provider source descriptors

Three-phase provisioning pipeline: **resolve → fetch → process**. Each file defines per-OS source descriptors consumed by `provider::mod::resolve_tool_fetch()`.

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

### Item semantics and progress tracking

Item semantics per phase, counting mechanism invariants, per-format extraction tracking, and the `MultiItemBudget` API are fully documented in `progress-budget.instructions.md` ("Phase-loop mapping" and "Counting mechanism accuracy guarantees"). Read that file before editing provider progress code.

SI prefixes are 1000-based: `format_count` and `format_rate` use decimal SI prefixes, not binary.

## deno permission wrapper contract

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
