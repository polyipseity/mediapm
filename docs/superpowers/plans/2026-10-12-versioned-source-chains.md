# Plan: versioned source chains

Date: 2026-10-12. Spec: `docs/superpowers/specs/2026-10-12-versioned-source-chains-design.md` (rulings recorded there). Execution: subagent-driven-development, fresh worker per task, delegated review, controller commits.

Grounding facts for implementers: session model is `cline-pass/mimo-v2.5` on provider `clinepass`. Every cargo command is prefixed `RUSTC_WRAPPER= CARGO_BUILD_RUSTC_WRAPPER=`. `wiremock = "0.6"` is already a `mediapm` dev-dependency (Cargo.toml:76) — hermetic HTTP tests use it; no real network in tests. Content-map key prefixes are built by the conductor at `src/mediapm-conductor/src/tools/provider/mod.rs:1217,1235` (`format!("{os_label}/...")`); `os_exec_paths` is `BTreeMap<String, String>` built in `src/mediapm/src/conductor_bridge/sync/provision.rs:41,384` and consumed by presets (`tools/preset/*.rs`) into `tools/workflows/*::build_*_spec`.

## Task 1 — version targeting plumbing + GitHub pinning + BtbN refusal + explicit arch selection

Scope (provider layer only, `src/mediapm/src/tools/`):

- Extend the conductor-side source types (they live in `mediapm-conductor/src/tools/provider/`): every resolved source carries an `arch: String` field (`"x86_64"` | `"aarch64"`), defaulting to the host arch where a provider publishes a single universal artifact. No content-key changes in this task — arch is carried, not yet used in keys.
- `resolve_tool_fetch` (`src/mediapm/src/tools/provider/mod.rs:349`) gains a version-target parameter (the already-resolved `VersionSpec` — `Latest` or `Exact(VersionSpecFields)`). Call sites: `conductor_bridge/sync/mod.rs:251` (provision) and the `logical_tool_requires_sync` comparison path in `service.rs` (both pass the tool's `version_spec`).
- GitHub tools (`yt_dlp.rs`, `deno.rs`, `rsgain.rs`, `sd.rs`): `Latest` path byte-identical to today. `Exact { tag: Some(t), .. }` resolves through `/repos/{o}/{r}/releases/tags/{t}` (same metadata-cache domain, same `HttpClientError` mapping) and builds fetch URLs from the release's asset list (match the same per-platform asset-name patterns the current templates use; refuse with a clear error when the tag has no matching asset). `Exact` with only `vcs_hash`/`version` for these tools: resolve latest, then the existing sync/mod.rs validation gate stays as-is (this task moves URL construction only for `tag`).
- BtbN-backed ffmpeg legs (`ffmpeg.rs`): `Exact` of any flavor fails at resolve with `BtbN does not retain historical builds; pinning ffmpeg's BtbN legs is impossible` (a `ProvisionFailure { phase: Resolve }` from the caller's validation match, or the provider's `Err` — match the existing error plumbing).
- Explicit arch selection replaces candidate conflation: `deno.rs` and `sd.rs` pick ONE arch per os (host arch via `std::env::consts::ARCH`), never a cross-arch candidate list. `rsgain`/`yt-dlp` keep their single-artifact-per-os shape with arch labeled.
- Tests (red first where behavior changes): wiremock-backed `Exact(tag)` resolution hitting `releases/tags/{tag}` and constructing `releases/download/{tag}/asset` URLs; asset-missing refusal message; BtbN pin refusal; arch selection picks host arch for deno/sd; `Latest` regression (URLs unchanged — existing provider tests must stay green).
- Not in scope: evermeet, martin-riedl, jellyfin, content-key arch segments, composite format, docs/matrix.

Validation: `cargo test-pkg mediapm` + `cargo test-pkg mediapm-conductor` + `cargo clippy -p mediapm --all-targets --all-features`.

Commit: `feat(mediapm): plumb version-targeted tool fetches`.

### Task 2 — ffmpeg macOS swap: martin-riedl chain, dual-arch payloads, evermeet removal

Depends on Task 1 (arch field + target plumbing).

- `ffmpeg.rs` macOS sources become the chain: primary martin-riedl (both arches: `macos` slot emits two sources, arch `aarch64` and `x86_64`), fallback jellyfin-ffmpeg macOS portable archive per arch. `Latest` resolution: HEAD/GET the redirect permalink `https://ffmpeg.martin-riedl.de/redirect/latest/macos/{arch}/snapshot/ffmpeg.zip`, parse build id from the redirect target (`/download/macos/{arch}/{unixtime}_{buildid}/...`), cache the parsed id in the metadata cache (domain `tool_metadata`); fetch the permalink URL. `Exact`: fetch `/download/macos/{arch}/{id}/ffmpeg.zip` directly; verify the sibling `.sha256` when the endpoint serves one (compare after download, fail the source on mismatch, try next chain member). jellyfin fallback URLs are versioned archive paths from repo.jellyfin.org; its version is parsed from the archive name at resolve time.
- Conductor content keys gain an optional arch segment: when a source carries an arch that is not the os default (or always, for the ffmpeg macOS slot — pick one rule, document it in the conductor AGENTS/instructions), keys become `{os}/{arch}/{filename}` (`provider/mod.rs:1217,1235`). `os_exec_paths` stays os-keyed: for tools with dual-arch payloads the built value resolves the host arch (`provision.rs:384` site), so conductor templates and `{os}` substitution are untouched.
- evermeet deletion, everywhere (61 hits at grep time): `ffmpeg.rs` source + HEAD probe + redirect parsing, `provider/mod.rs` prefetch-skip special cases, `provision.rs` mentions, `examples/support/*`, `examples/mediapm_demo.rs`, `tests/int/*` references, instruction-file rows (the two doc files are edited in Task 3 — in this task, only code + tests).
- Composite version: the ffmpeg canonical becomes `"{btbn_tag}+{macos_leg_version}"` where the macOS leg is the martin-riedl build id or jellyfin version actually fetched (`sync/mod.rs` composite site; the `+evermeet-` format string and its tests updated).
- Tests (red first): martin-riedl redirect-parse + permalink construction (wiremock); sha256 mismatch fails over to the next chain member (wiremock); chain exhaustion message names every endpoint (wiremock, all endpoints erroring); dual-arch ffmpeg content map carries both `macos/{arch}/...` keys and `os_exec_paths["macos"]` points at the host arch; composite format assertion; `grep -rn evermeet src/` returns nothing (assert in a review checklist, not a test).
- The provisioned-binary check stays level-3 (online demo), not a hermetic test.

Validation: `cargo test-pkg mediapm` + `cargo test-pkg mediapm-conductor` + `cargo clippy --workspace --all-targets --all-features` (conductor change included).

Commit: `feat(mediapm): provision ffmpeg macOS from versioned chains`.

### Task 3 — docs, matrix, whole-workspace validation, demo smoke

- Rewrite `.agents/instructions/provider-dispatch.instructions.md`: per-tool source table (martin-riedl/jellyfin for macOS, BtbN refusal note), canonical-version table (`+{macos_leg}` composite), resolved-provenance rows for the new sources, the why-empty invariants for anything new.
- `.agents/instructions/cache-and-http.instructions.md`: remove the evermeet/getrelease HEAD-prefetch special case; document the martin-riedl redirect-HEAD as the new resolve probe if it gets the same treatment.
- `.agents/instructions/tool-sync-coordinator-and-identity.instructions.md`: resolve-flow wording where it names evermeet/BtbN assumptions.
- `.agents/coverage-matrix.md`: dated section covering Task 1 + Task 2 spec items with test names; no stale evermeet citations anywhere.
- Full gates: `cargo test-all`, `cargo test-doc-all`, `cargo clippy-all`, `cargo fmt-check`.
- Level-3 demo smoke: `cargo run --package mediapm --example mediapm_demo_online` (record outcome either way; both full-sync and a truthful fail-fast are acceptable results to report — a martin-riedl-shaped failure message would itself prove the swap).

Commit: `docs(mediapm): document versioned source chains`.

## Definition of done

- `Exact(tag)` on yt-dlp/deno/rsgain/sd fetches that tag's assets; BtbN pins refuse with the documented message.
- macOS ffmpeg provisions from martin-riedl with jellyfin fallback, both arches in the content map, host arch in `os_exec_paths`; `grep -rn evermeet src/` is empty.
- Suites green at every task boundary; instruction files and matrix match the shipped code; demo outcome recorded.
