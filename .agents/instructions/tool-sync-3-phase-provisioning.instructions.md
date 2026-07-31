---
description: "Use when editing tool payload provisioning in src/mediapm/src/conductor_bridge/sync/provision.rs. Covers the 3-phase pipeline, progress bar lifecycle per phase, FetchedToolPayload fields, and prefetch logic."
name: "Tool Sync 3-Phase Provisioning Pipeline"
applyTo: "src/mediapm/src/conductor_bridge/sync/provision.rs"
---

# Tool sync 3-phase provisioning pipeline

## Purpose

- Fetch tool payloads for all platforms, extract to CAS, build content maps and OS-conditional command selectors.
- Provide accurate progress reporting through the mediapm progress bar API.

## 3-phase pipeline

`fetch_and_import_tool_payload()` runs three phases sequentially, preceded
by a pre-resolve step:

### Pre-resolve: `PreResolveOutcome`

The caller resolves the tool fetch before calling
`fetch_and_import_tool_payload()` and passes the outcome as
`PreResolveOutcome`:

- **`Resolved(fetch, metadata)`** — normal provisioning path, where `metadata` is a `ResolvedToolMetadata` carrying `human_readable_version`, `canonical_version`, `metadata_cached` (bool: all version/tag lookups were cache hits), `metadata_fetch_count` (u32: number of metadata lookups, e.g., ffmpeg has 2: btbn tag + evermeet version), and the three `resolved_*` provenance fields (all `Option<String>` — `None` when the provider has no value).
- **`Skip { name, version, metadata_cached, metadata_fetch_count, resolved_tag, resolved_version, resolved_vcs_hash }`** — tool is already provisioned at the given canonical version. Only the resolve bar is shown, then the function returns `Ok(None)` immediately. `metadata_cached` controls whether the message shows `"skipped cached (N)"` vs `"skipped"` (where `N` = `metadata_fetch_count`). The `resolved_*` fields (all `Option<String>`) carry fresh provider provenance so the coordinator can backfill skipped entries.

This separation keeps the function single-responsibility: it renders a
resolve bar for every tool (avoiding a bare `pb.advance(1)` with no
per-tool visual feedback).

### Phase 1: Resolve

- Receives the resolved `ResolvedToolFetch` from the `PreResolveOutcome::Resolved` variant.
- No longer calls `provider::resolve_tool_fetch()` internally — that is done by the caller.
- Progress: resolve bar shows `metadata_fetch_count` items (one per metadata
  lookup, e.g., ffmpeg shows 2). The bar total is `metadata_fetch_count`;
  when `metadata_fetch_count` is 0 (no cache lookups performed), the bar is
  indeterminate (total is 0).
- When `metadata_cached` is true, the bar shows `"cached (N)"` where `N` =
  `metadata_fetch_count`.

### Phase 1b: HEAD prefetch

- Sends HEAD requests to populate `expected_size` on each `Fetch`-producer source.
- Failures are silently ignored (Content-Length fallback applies).
- **Evermeet and getrelease URLs are skipped** — they return dynamic builds (HEAD Content-Length wouldn't match GET response).
- Timeout: 10 seconds per HEAD request.

### Phase 2: Fetch

- Delegates to `mediapm_conductor::tools::provider::fetch_tool_sources(fetch, cache, progress)`.
- Downloads bytes for each source (or generates launcher scripts).
- Progress: per-source bar showing `items.current/items.total` and `bytes.current/bytes.total`.
- Bar created on-demand (only appears when phase runs).
- **Cached sources**: when `DownloadedSources.cached_count > 0`, the fetch bar
  shows `"cached (N)"` via `set_message()` before finishing.

### Phase 3: Process

- Delegates to `mediapm_conductor::tools::provider::process_tool_sources(downloaded, cas, progress)`.
- Extracts archives (ZIP, tgz), re-packs to uncompressed ZIP, imports files to CAS with `./{os}/` key prefixes.
- Builds OS-conditional command-selector template.
- Progress: per-source bar showing items and bytes.
- Bar created on-demand.

## `FetchedToolPayload` fields

| Field                  | Type                       | Purpose                                                                                     |
| ---------------------- | -------------------------- | ------------------------------------------------------------------------------------------- |
| `content_map`          | `BTreeMap<String, String>` | Sandbox-relative path → CAS hash hex                                                        |
| `os_exec_paths`        | `BTreeMap<String, String>` | OS label → relative executable path (no OS prefix)                                          |
| `human_readable_version` | `String`                 | Human-readable version (informational only, no semantic use)                                 |
| `canonical_version`    | `String`                   | Canonical version for skip-if-up-to-date logic. Populated from the resolved fetch metadata. |
| `resolved_tag`         | `Option<String>`           | Provenance: resolved upstream git tag, or `None`                                             |
| `resolved_version`     | `Option<String>`           | Provenance: resolved upstream version, or `None`                                             |
| `resolved_vcs_hash`    | `Option<String>`           | Provenance: resolved upstream VCS commit hash, or `None`                                     |

## Error handling

- Each phase that creates progress bars adds them to `error_bars: Vec<Arc<dyn ProgressBarApi>>`.
- On error in any phase: all tracked bars are marked `finish_error()`, then `Err` is returned.
- Empty sources (`fetch.sources.is_empty()`) → returns `Ok(None)` — no bars beyond resolve are created.

## Key invariants

- Progress bar values are relayed directly from conductor's `ProviderProgressCallback` — the bridge does not interpret item or byte counts.
- All progress bars are `group.add_bar()` — they are owned by the calling coordinator's progress group.
- The metadata cache must NOT have `touch()` called — its TTL (1 day) is anchored to creation time, not last use.
- `set_message("skipped")` or `set_message("skipped cached (N)")` (where `N` =
  `metadata_fetch_count`) is called on the resolve bar before
  `finish_success()`, depending on `metadata_cached`. `set_message("cached (N)")`
  (where `N` = `downloaded.cached_count`) is called on the fetch bar before
  `finish()`. Both work because the daemon ticker still syncs `SharedState` to
  the indicatif bar until the bar is removed from `MultiProgress`.
