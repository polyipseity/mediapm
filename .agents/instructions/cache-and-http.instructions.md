---
description: "Use when editing cache and HTTP client configuration. Covers three-tier cache hierarchy, TTL policies, shared HTTP client configuration, and decoupling invariants for HTTP modules."
name: "Cache and HTTP Client Invariants"
applyTo: "src/mediapm-conductor/src/http/**/*.rs, src/mediapm-conductor-builtins/import/src/lib.rs, src/mediapm/src/conductor_bridge/sync/provision.rs"
---

# Cache and HTTP client

## Three-tier cache hierarchy

| Cache | TTL | Basis | Content | Key |
| --- | --- | --- | --- | --- |
| **Content cache** (`tools.json`) | 7d | Last-use | Raw downloaded tool payload bytes | Download URI (actual URL used) |
| **Metadata cache** (`tool_metadata.json`) | 1d | Creation-time | GitHub API responses (tag names, versions) | API endpoint URL |
| **Provision cache** (RAII) | 24h | Creation-time | Extracted tool binaries (per-platform unpack) | Tool identity hash |

- Content cache TTL is **last-use based** — `lookup_bytes()` / `store_bytes()` reset the clock.
- Metadata cache TTL is **creation-time based** — never call `touch()`; doing so extends TTL and defeats the 1-day freshness guarantee.
- Provision cache is **RAII** — the extracted temp dir lives for the `ProvisionCache` handle (24h default).

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

Both override request timeout via `MEDIAPM_HTTP_TIMEOUT_SECONDS` (minimum 30s).

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
