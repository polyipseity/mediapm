---
description: "Use when editing cache and HTTP client configuration. Covers three-tier cache hierarchy, TTL policies, shared HTTP client configuration, and decoupling invariants for HTTP modules."
name: "Cache and HTTP Client Invariants"
applyTo: "src/mediapm-conductor/src/http/**/*.rs, src/mediapm-conductor-builtins/import/src/lib.rs, src/mediapm/src/conductor_bridge/sync/provision.rs"
---

# Cache and HTTP client

## Three-tier cache hierarchy

| Cache | TTL | Basis | Content | Key |
| --- | --- | --- | --- | --- |
| **Content cache** (`tools.json`) | 7d | Last-use | Raw downloaded tool payload bytes | Download URI, plus the source's resolved identity when the URI is a moving permalink |
| **Metadata cache** (`tool_metadata.json`) | 1d | Creation-time | GitHub API responses (tag names, versions), the jellyfin macOS repo index, and its per-arch `*-gpl.txt` pointer files | API endpoint URL (any metadata read) |
| **Provision cache** (RAII) | 24h | Creation-time | Extracted tool binaries (per-platform unpack) | Tool identity hash |

- Content cache TTL is **last-use based** — `lookup_bytes()` / `store_bytes()` reset the clock.
- **A moving download URI carries its resolved identity in the key.** A permalink that 302-redirects to a versioned download (the martin-riedl macOS candidates) would otherwise be found again on every sync, refreshing its last-use clock and freezing the payload while the state recorded the newer build; the key is `uri#identity=<id>` for such a source and the bare URI otherwise. The identity is per source, not per candidate, so every candidate URL a source names is keyed, and a cache hit reports the *requested* URL rather than the key, because the macOS leg is re-derived by parsing the URI that was actually served.
- Metadata cache TTL is **creation-time based** — never call `touch()`; doing so extends TTL and defeats the 1-day freshness guarantee.
- Provision cache is **RAII** — the extracted temp dir lives for the `ProvisionCache` handle (24h default).
- **A download may leave the host it was requested from.** The jellyfin macOS archive 302-redirects to `*.mirror.jellyfin.org` (`sgp1.mirror.jellyfin.org` in practice), so that host is as load-bearing as `repo.jellyfin.org` and degrades the macOS fallback when it is unreachable.

All caches live under `default_mediapm_user_download_cache_root()` (`~/.cache/mediapm/` on Linux, `~/Library/Caches/mediapm/` on macOS):

```text
<os-cache-dir>/mediapm/
  tools.json          # Content cache (7d, last-use)
  tool_metadata.json  # Metadata cache (1d, creation-time)
  provision/          # RAII provision cache (24h)
```

Cache-using examples (`mediapm_cli_add_tools`, `mediapm_cli_add_hierarchy`, `mediapm_demo`, `mediapm_demo_online`) resolve this via `mediapm::example_isolation::user_level_cache_root()` so they share the cache with regular syncs. Embedded tests override it with `MEDIAPM_EXAMPLE_CACHE_ROOT` → `MediaRuntimeStorage.cache_root_override` to stay hermetic.

## Shared HTTP client

Configured once via `OnceLock`:

| Client | Connect timeout | Request timeout | User-Agent |
| --- | --- | --- | --- |
| `mediapm-conductor` (async, unconditional) | 30s | 30 min | `mediapm/<version> (+https://github.com/mediapm/mediapm)` |
| `mediapm-conductor-builtins/import` (blocking, `fetch`) | 60s | 60s | `mediapm/<version> (+https://github.com/mediapm/mediapm)` |

Both override request timeout via `MEDIAPM_HTTP_TIMEOUT_SECONDS` (minimum 30s). The MusicBrainz client in the `media-tagger` builtin is a third reader of the same variable on its own path: minimum 30s, default 30s, retry cap 2.

## Hard boundary rules

- Workspace-scoped conductor tool-content storage (`<runtime_root>/tools/`) and user-level download cache (`<os-cache-dir>/mediapm/`) are **never interchangeable**.
- Content cache holds raw downloaded bytes for cross-workspace reuse; tools dir holds materialized binaries for one workspace.

## HTTP client invariants

Two shared clients (one async + one blocking), both `OnceLock`:

| Client | Crate | Feature | Runtime |
| --- | --- | --- | --- |
| `shared_http_client()` / `shared_no_redirect_http_client()` | `mediapm-conductor` | unconditional | Tokio (reqwest async) |
| `shared_http_client()` | `mediapm-conductor-builtins/import` | `fetch` | Sync (reqwest blocking) |

- The import builtin uses a **blocking** client (synchronous context, no tokio runtime) and must not depend on `mediapm-conductor` (dependency direction is opposite).
- Both use the same User-Agent with their `CARGO_PKG_VERSION`.
- `shared_no_redirect_http_client()` (conductor) disables redirect following — use for download sources that must not follow redirects.
- MediaPM crate code imports the shared client via `mediapm_conductor::http::client::shared_http_client()`.

### Decoupling invariant

The `src/http/` module in `mediapm-conductor` must be **fully self-contained**:

- **Zero `use crate::` imports** — import nothing from its own crate.
- **Zero `ConductorError` references** — define and use `HttpClientError` instead.
- The module must be extractable into a standalone crate by copying the directory and adjusting `Cargo.toml` — no module-body changes.

Error mapping from `HttpClientError` to `ConductorError` happens **at the call site** (`src/tools/provider/mod.rs`), never inside `http/`.

Enforcement: `build.rs` in `mediapm-conductor` scans `src/http/` for `use crate::` and `ConductorError` and panics on violation; `HttpClientError` is the only error type inside `http/`; verify no new `crate::`/`ConductorError` deps when editing `src/http/`.

## Docstring policy

Every function calling a shared HTTP client must include an `HTTP client policy` subsection in its docstring:

```rust
/// # HTTP client policy
///
/// Uses the process-wide shared client from [`mediapm_conductor::http::client`].
/// Connection pooling, TLS reuse, and DNS caching are managed centrally.
/// Do NOT create a [`reqwest::Client`] locally — always use the shared
/// instance.
```

- Link to the conductor HTTP client module (`[`mediapm_conductor::http::client`]`).
- Mention the no-redirect variant explicitly if used.
- This section must appear before any `# Panics` or `# Errors` sections.
