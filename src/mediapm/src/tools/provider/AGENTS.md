# `provider/` — Managed tool provider source descriptors

Per-OS source URLs for 5 tools with presets (`ffmpeg`, `yt-dlp`, `deno`, `rsgain`, `sd`). `media_tagger.rs`/`import.rs` answer under their own tool id with an empty source list: both are in-process builtins with no payload or preset module. Each file defines platform URLs consumed by `provider::mod::resolve_tool_fetch()`.

## `size_hint_bytes` policy

- Every `Fetch` provider sets `size_hint_bytes` to an approximate upper bound of expected download size, giving a stable initial fetch total when HTTP HEAD probing fails.
- Builtin providers declare no sources, so no size hint. Guard: `builtin_providers_have_no_sources` (`mediapm-conductor/src/tools/provider/mod.rs`).

See `crate::tools::preset::AGENTS.md`, `crate::tools::provider::mod.rs`.
