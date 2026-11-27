---
description: "Use for validation timing baselines and CI evidence."
name: "Validation Timing Baselines"
applyTo: ".github/workflows/**/*.yml, .github/workflows/**/*.yaml, prek.toml, .config/nextest.toml, scripts/test-runner/**, .agents/instructions/rust-workflow.instructions.md"
---

# Validation timing baselines

Durable timing/CI evidence; history predates `91776606`. Keep template, newest baselines, dispositions.

## R1/R4 metadata template

Every field: `unknown`/`not captured` if missing, never guess.

| Field | Record |
| --- | --- |
| ID / date | Unique label, UTC/local |
| Commit | Full SHA, branch/ref |
| Commands | Exact lines, order, scope |
| Environment | Local/CI, runner/host, OS, arch, sandbox |
| Toolchain | `rustc`, `cargo`, components |
| Build toolchain | `cc/cxx/ar/ld`, SDK, target, env |
| `RUSTC_WRAPPER` | Set/unset and value |
| Target/cache | Warm/cold, size/state |
| Disk/headroom | Free space before/after |
| Timing | Real/user/sys, totals |
| Exit/counts | Exit, pass/fail/skip/slow, totals |
| Anomalies | Failures, retries, throttling, gaps |
| Evidence | Log path, run ID/URL, artifact |

## Newest CI baseline

| Field | Value |
| --- | --- |
| Record | `ci-build-test-44900229817`, 2026-11-21T11:40:35Z-11:53:55Z |
| Commit | `9188a9dcfbe17ebfefd40195a46cb6851ff9d504` (`main`, push, attempt 1, green) |
| Environment | ubuntu-latest (3 jobs) + windows-latest (1 job) |
| Result | success ~800 s (Build & Test 797, Feature Matrix 102, Release 127, Windows 303) |
| Cache | Four exact-key full matches, `full match: true`, `Cache up-to-date` |
| Counts | `[128.694s] 3061 run: 3061 passed (5 slow), 0 skipped`; matrix 58/58; release green |
| Steps | cheap 329.5 s, ffprobe 14.0 s, tests 353.4 s, doctests+rustdoc 34.9 s, build 47.6 s |
| Evidence | <https://github.com/polyipseity/mediapm/actions/runs/44900229817>, `/tmp/ci-evidence/44900229817/` |

Older runs 44865482029, 44880796368 (attempt-1 flake), 43609375823, 43379058938, 44753912879, commitlint, D0-D7: git history before `91776606`.

## Warmed exact-key CI cache pair

Before 44865482029 (`6847623b`, 2026-11-21T04:18:50Z, green 1) restored partial, saved exact; after 44900229817 (`9188a9dc`, 2026-11-21T11:40:35Z, green 1) restored exact. 44880796368 (`e9b75631`) excluded: scheduler flake.

| Job | Before | After | Before wall | After wall |
| --- | --- | --- | --- | --- |
| Build & Test | `v0-rust-ci-build-Linux-x64-bbe94a93-8274b117` partial, `full match: false` | same key, `full match: true` | 846 s | 797 s |
| Feature Matrix | `v0-rust-ci-feature-matrix-Linux-x64-bbe94a93-a142a65c` partial, `full match: false` | `v0-rust-ci-feature-matrix-Linux-x64-9c09dd5e-8274b117`, `full match: true` | 51 s | 102 s |
| Release Build | `v0-rust-ci-release-Linux-x64-bbe94a93-a142a65c` partial, `full match: false` | `v0-rust-ci-release-Linux-x64-bbe94a93-8274b117`, `full match: true` | 152 s | 127 s |

## Newest local baseline

| Field | Value |
| --- | --- |
| Record | `local-isolated-full-r4-9a25da7a`, 2026-11-20 |
| Commit | `9a25da7adae00827000fa2ab740b62ee96f1ad31` |
| Environment | Local macOS arm64; `RUSTC_WRAPPER=`, fixed TMPDIR |
| Toolchain | `rustc 1.99.1 (58cf8f9c3 2026-10-13)`, `cargo 1.99.1 (5f94df478 2026-08-27)` |
| Commands | `fmt-check` 0.86s, `rumdl` 4.62s, `clippy-all` 29.98s, `check --workspace --all-targets --all-features` 8.52s, `test-all` 123.04s (exit 1, janitor/tempdir leftover; nextest 3051 passed), `test-doc-all` 20.06s (9 doctests), `build-all` 16.87s, `feature-matrix --locked` 93.78s (58/58) |
| Anomalies | No release; disk 97%->93%; compare per command, not 1229.18s (included release). |
| Evidence | `/tmp/mediapm-diag/r4-bundle-9a25da7a/`; full table in git history |

2026-11-14 partial/warm time-to-fail, 2026-11-15 clean sequential (1229.18s), 2026-11-17 Task 8 rerun: history before `91776606`.

## Residual dispositions

| Item | Disposition | Rest |
| --- | --- | --- |
| R12 | No-go closure scope. | D5/D0 history; owner-approved plan only. |
| Feature-matrix D1/D2 contention, safety, P5 accounting | No-go proof-gated; sequential D0. | WP5 (`.superpowers/sdd/2026-11-16-task-5-feature-matrix-parallelism-proof-plan/wp5-disposition.md`). |
| CI per-combination rows | Measured 44865482029; D3.2 history. | Add future rows here. |
| Warmed post-cache pair | Closed by 44865482029 + 44900229817. | Table above; 44880796368 red. |
| Runner hardware / queue internals | Accepted constraint. | GitHub Actions did not expose. |
| Windows-local runs | Accepted constraint. | CI Windows in history. |
| `/tmp` diagnostics ephemeral | Accepted; transcriptions in history. | Re-stage logs if needed. |
| Nightly `-Z timings` | Excluded from comparisons. | Owner approval to re-run. |
| Fresh local full-suite R4 | Resolved `local-isolated-full-r4-9a25da7a`; no release. | Record release bundle if needed. |
| Release aggregates / `rmeta_time` / phase attribution | Accepted gap. | Capture with suitable tooling. |
| Real-online demo `--online` (Task 8) | Resolved `3e0e2d35`, `870cb693`, `aaca1dd2`, `21ef0e57`. | Re-run after capture/demo changes. |
