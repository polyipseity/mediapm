---
description: "Use when editing cache and HTTP client configuration. Covers three-tier cache hierarchy, TTL policies, shared HTTP client configuration, and decoupling invariants for HTTP modules."
name: "Cache and HTTP Client Invariants"
applyTo: "src/mediapm-conductor/src/http/**/*.rs, src/mediapm-conductor-builtins/import/src/lib.rs, src/mediapm/src/conductor_bridge/sync/provision.rs"
---

# Cache and HTTP client

## Purpose

- Provide efficient caching for downloaded tool payloads and metadata (GitHub tags) to avoid redundant network transfers.
- Share one `reqwest::Client` process-wide for connection pooling and TLS reuse.

## Three-tier cache hierarchy

| Cache                                     | TTL | Basis         | Content                                               | Key                                         |
| ----------------------------------------- | --- | ------------- | ----------------------------------------------------- | ------------------------------------------- |
| **Content cache** (`tools.json`)          | 30d | Last-use      | Raw downloaded tool payload bytes                     | Download URI (actual URL used for download) |
| **Metadata cache** (`tool_metadata.json`) | 1d  | Creation-time | GitHub API responses (tag names, versions)            | API endpoint URL                            |
| **Provision cache** (RAII)                | 24h | Creation-time | Extracted tool binaries (per-platform unpack results) | Tool identity hash                          |

### Important: TTL basis differences

- Content cache TTL is **last-use based** — touching a cached entry resets its TTL clock. Used via `lookup_bytes()` / `store_bytes()`.
- Metadata cache TTL is **creation-time based** — entries expire based on when they were stored, not when last accessed. Caller must NOT call `touch()` — doing so would extend the TTL, defeating the 1-day freshness guarantee.
- Provision cache is **RAII** — the extracted temp directory lives for the duration of the `ProvisionCache` handle (24h default).

## Cache location

All caches live under `default_mediapm_user_download_cache_root()` (OS-specific user-level cache directory, typically `~/.cache/mediapm/` on Linux or `~/Library/Caches/mediapm/` on macOS).

```text
<os-cache-dir>/mediapm/
  tools.json            # Content cache (30d, last-use)
  tool_metadata.json    # Metadata cache (1d, creation-time)
  provision/            # RAII provision cache (24h)
```

## Shared HTTP client

Configured once via `OnceLock`. All three shared clients use the same
configuration pattern:

| Client                                                  | Connect timeout | Request timeout | User-Agent                                                |
| ------------------------------------------------------- | --------------- | --------------- | --------------------------------------------------------- |
| `mediapm-conductor` (async, unconditional)              | 30s             | 30 min          | `mediapm/<version> (+https://github.com/mediapm/mediapm)` |
| `mediapm-conductor-builtins/import` (blocking, `fetch`) | 60s             | 60s             | `mediapm/<version> (+https://github.com/mediapm/mediapm)` |

Both override the request timeout via `MEDIAPM_HTTP_TIMEOUT_SECONDS` env var (minimum 30s).

## Hard boundary rules

- Workspace-scoped conductor tool-content storage (`<runtime_root>/tools/`) and user-level download cache (`<os-cache-dir>/mediapm/`) are **never interchangeable**.
- The content cache holds raw downloaded bytes for cross-workspace reuse.
- The tools directory holds materialized (extracted) binaries for one specific workspace.

## HTTP client invariants

The codebase has **two** shared HTTP clients (one async + one blocking), both using the `OnceLock` pattern.

| Client                                                      | Crate                               | Feature       | Runtime                 |
| ----------------------------------------------------------- | ----------------------------------- | ------------- | ----------------------- |
| `shared_http_client()` / `shared_no_redirect_http_client()` | `mediapm-conductor`                 | unconditional | Tokio (reqwest async)   |
| `shared_http_client()`                                      | `mediapm-conductor-builtins/import` | `fetch`       | Sync (reqwest blocking) |

- The import builtin uses a **blocking** client because it runs in a synchronous context (not a tokio runtime). It must not depend on `mediapm-conductor` (the dependency direction is the opposite).
- Both clients use the same User-Agent format with their respective `CARGO_PKG_VERSION`.
- The `shared_no_redirect_http_client()` variant (conductor) disables redirect following. Use it for download sources that should not follow redirects (e.g. binary distribution mirrors).
- MediaPM crate code imports the shared client via `mediapm_conductor::http::client::shared_http_client()`.

### Decoupling invariant (critical)

The HTTP client module in `mediapm-conductor` (`src/http/`) must be **fully self-contained**:

- **Zero `use crate::` imports** — the module must import nothing from its own crate.
- **Zero `ConductorError` references** — define and use `HttpClientError` instead.
- The module must be designed so it can be extracted into a standalone crate by copying the directory and adjusting `Cargo.toml` dependencies — no code changes to the module body.

Error mapping from `HttpClientError` to `ConductorError` happens **at the call site** (`src/tools/provider/mod.rs`), never inside the `http/` module.

### Decoupling enforcement (three layers)

1. **Build-time regression** — `build.rs` in `mediapm-conductor` scans `src/http/` for `use crate::` and `ConductorError` and panics on violation.
2. **Module-scoped types** — `HttpClientError` is the only error type used inside `http/`.
3. **Review checklist** — when editing `src/http/`, verify no new dependencies on `crate::` or `ConductorError` were introduced.

## Docstring policy

Every function that calls a shared HTTP client must include an `HTTP client
policy` subsection in its docstring:

```rust
/// # HTTP client policy
///
/// Uses the process-wide shared client from [`mediapm_conductor::http::client`].
/// Connection pooling, TLS reuse, and DNS caching are managed centrally.
/// Do NOT create a [`reqwest::Client`] locally — always use the shared
/// instance.
```

- Link to the conductor HTTP client module (`[`mediapm_conductor::http::client`]`).
- If the call site uses the no-redirect variant, mention it explicitly.
- This section must appear in the function's doc comment, before any
  `# Panics` or `# Errors` sections.
