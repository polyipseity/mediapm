# `provider/` — Managed tool provider source descriptors

Per-OS source URL definitions for the 6 managed tools.
Each file defines platform-specific download URLs consumed by
`provider::mod::resolve_tool_fetch()`.

Managed tools: `ffmpeg`, `yt-dlp`, `deno`, `rsgain`, `media-tagger`, `sd`.

## size_hint_bytes policy

- All `Fetch`-type providers must set `size_hint_bytes` to an approximate
  upper bound of the expected download size. This provides a stable initial
  total for the fetch progress bar when HTTP HEAD probing fails.
- `GenerateLauncher`-type providers (builtins) must set
  `size_hint_bytes: None` — launcher scripts have negligible size and do
  not need a size hint.

See `crate::tools::preset::AGENTS.md` for the corresponding preset builders.
See `crate::tools::provider::mod.rs` for the resolve dispatch table.
