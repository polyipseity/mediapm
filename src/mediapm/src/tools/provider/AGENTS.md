# `provider/` — Managed tool provider source descriptors

Per-OS source URL definitions for the 5 managed tools with presets (`ffmpeg`, `yt-dlp`, `deno`, `rsgain`, `sd`). `media_tagger.rs` and `import.rs` answer under their own tool id with an empty source list instead: both are builtins the conductor dispatches in-process, so neither has a payload to fetch and neither has a preset module. Each file defines platform-specific download URLs consumed by `provider::mod::resolve_tool_fetch()`.

## size_hint_bytes policy

- All `Fetch` providers must set `size_hint_bytes` to an approximate
  upper bound of the expected download size. This provides a stable initial
  total for the fetch progress bar when HTTP HEAD probing fails.
- A builtin provider declares no sources, so it has no size hint to set. The guard that its list stays empty is `builtin_providers_have_no_sources` (`mediapm-conductor/src/tools/provider/mod.rs`).

See `crate::tools::preset::AGENTS.md` for the corresponding preset builders.
See `crate::tools::provider::mod.rs` for the resolve dispatch table.
