---
description: "Use when recording validation timing baselines, CI timing evidence, or R1/R4 measurement metadata for mediapm checks and tests."
name: "Validation Timing Baselines"
applyTo: ".github/workflows/**/*.yml, .github/workflows/**/*.yaml, prek.toml, .config/nextest.toml, scripts/test-runner/**, .agents/instructions/rust-workflow.instructions.md"
---

# Validation timing baselines

Durable record for validation timing baselines and CI timing evidence. The full history lives in git history before commit `91776606` (this file was 101,020 bytes / 742 lines). This copy keeps the template, the newest baselines, and the disposition of each residual item.

## R1/R4 metadata template

Record these fields for every baseline. Write `unknown` or `not captured`, never guess.

| Field | Record |
| --- | --- |
| ID / date | Unique label, UTC/local as recorded |
| Commit | Full SHA, branch/ref |
| Commands | Exact command lines, order, scope |
| Environment | Local/CI, runner/host, OS, arch, sandbox |
| Toolchain | `rustc`, `cargo`, components if known |
| Build toolchain | `cc/cxx/ar/ld`, SDK, target, relevant env |
| `RUSTC_WRAPPER` | Set/unset and value |
| Target/cache | Warm/cold, size/state if known |
| Disk/headroom | Free space before and after |
| Timing | Real/user/sys per command and totals |
| Exit/counts | Exit code, pass/fail/skip/slow, totals |
| Anomalies | Failures, retries, throttling, missing data |
| Evidence | Log path, run ID/URL, artifact |

## Newest CI baseline

| Field | Value |
| --- | --- |
| Record | `ci-build-test-44900229817`, 2026-11-21T11:40:35Z-11:53:55Z |
| Commit | `9188a9dcfbe17ebfefd40195a46cb6851ff9d504` (`main`, push, attempt 1, green) |
| Environment | GitHub Actions ubuntu-latest (3 jobs) + windows-latest (1 job) |
| Result | success, ~800 s wall (Build & Test 797 s, Feature Matrix 102 s, Release Build 127 s, Windows 303 s) |
| Cache | All four jobs exact-key full matches, `full match: true`, Post Cache `Cache up-to-date` (see warmed pair below) |
| Counts | Build & Test nextest `[128.694s] 3061 run: 3061 passed (5 slow), 0 skipped`; Feature Matrix 58/58 check clean; Release Build green; Windows nextest 13 + 43 passed |
| Build & Test steps | cheap checks 329.5 s, install ffprobe 14.0 s, tests 353.4 s (nextest 128.694 s), doctests+rustdoc 34.9 s, build 47.6 s |
| Evidence | <https://github.com/polyipseity/mediapm/actions/runs/44900229817>, `/tmp/ci-evidence/44900229817/` |

Older CI runs (44865482029, 44880796368 attempt-1 flake/rerun, 43609375823, 43379058938, 44753912879, commitlint rows) and the D0-D7 diagnostic pass remain in git history before `91776606`.

## Warmed exact-key CI cache pair

Before: 44865482029 (`6847623b`, 2026-11-21T04:18:50Z, green, attempt 1) restored on partial keys and saved exact keys. After: 44900229817 (`9188a9dc`, 2026-11-21T11:40:35Z, green, attempt 1) restored those exact keys. The pair is clean (both attempt 1 green, no rerun-to-green). 44880796368 (`e9b75631`) is not used as the after record because attempt 1 failed on the scheduler flake before rerun.

| Job | Before key / restore | After key / restore | Before wall | After wall |
| --- | --- | --- | --- | --- |
| Build & Test | `v0-rust-ci-build-Linux-x64-bbe94a93-8274b117` partial, `full match: false` | same key, `full match: true` | 846 s | 797 s |
| Feature Matrix | `v0-rust-ci-feature-matrix-Linux-x64-bbe94a93-a142a65c` partial, `full match: false` | `v0-rust-ci-feature-matrix-Linux-x64-9c09dd5e-8274b117`, `full match: true` | 51 s | 102 s |
| Release Build | `v0-rust-ci-release-Linux-x64-bbe94a93-a142a65c` partial, `full match: false` | `v0-rust-ci-release-Linux-x64-bbe94a93-8274b117`, `full match: true` | 152 s | 127 s |
| Windows: Workspace Tests | `v0-rust-ci-windows-Windows_NT-x64-21b8dcfc-dd242995` partial, `full match: false` | `v0-rust-ci-windows-Windows_NT-x64-21b8dcfc-c597198c`, `full match: true` | 380 s | 303 s |

## Newest local baseline

| Field | Value |
| --- | --- |
| Record | `local-isolated-full-r4-9a25da7a`, 2026-11-20 |
| Commit | `9a25da7adae00827000fa2ab740b62ee96f1ad31` |
| Environment | Local macOS arm64 sandbox; `RUSTC_WRAPPER=`, fixed TMPDIR |
| Toolchain | `rustc 1.99.1 (58cf8f9c3 2026-10-13)`, `cargo 1.99.1 (5f94df478 2026-08-27)` |
| Commands | `fmt-check` 0.86s, `rumdl` 4.62s, `clippy-all` 29.98s, `check --workspace --all-targets --all-features` 8.52s, `test-all` 123.04s (exit 1, janitor/tempdir gate leftover in run TMPDIR; nextest 3051 passed, 0 skipped), `test-doc-all` 20.06s (9 doctests), `build-all` 16.87s, `feature-matrix --locked` 93.78s (58/58 clean) |
| Anomalies | No release build in bundle; disk 97% to 93%; compare per command, not to the 1229.18s baseline that included release. |
| Evidence | `/tmp/mediapm-diag/r4-bundle-9a25da7a/` (git history keeps the full table) |

The 2026-11-14 partial/warm time-to-fail record, the 2026-11-15 clean sequential baseline (1229.18s, all passed), and the 2026-11-17 Task 8 rerun remain in git history before `91776606`.

## Residual dispositions

| Item | Disposition | Where the rest went |
| --- | --- | --- |
| R12 | Excluded, no-go for this closure scope. | D5/D0 in git history. Revisit only with an owner-approved scoped plan. |
| Feature-matrix parallelism D1/D2 contention, safety, P5 accounting | No-go, proof-gated; sequential D0 retained. | WP5 disposition in git history (`.superpowers/sdd/2026-11-16-task-5-feature-matrix-parallelism-proof-plan/wp5-disposition.md`). |
| CI per-combination timing rows | Measured for 44865482029; D3.2 table in git history. | Add future run rows here when captured. |
| Warmed post-cache CI exact-key pair | Closed by 44865482029 (before, partial save) and 44900229817 (after, exact hit, both attempt 1 green). | Warmed pair table above; 44880796368 excluded because attempt 1 red. |
| Runner hardware / queue internals | Accepted environment constraint. | GitHub Actions did not expose them. |
| Windows-local runs | Accepted environment constraint. | CI Windows evidence in git history; local Windows not run. |
| `/tmp` diagnostics ephemeral | Accepted; durable transcriptions kept in git history. | Re-stage raw logs durably if needed. |
| Nightly `-Z timings` | Excluded from baseline comparisons; diagnostic-only in git history. | Re-run only with owner approval and comparable metadata. |
| Fresh local full-suite R4 reruns | Resolved by `local-isolated-full-r4-9a25da7a`; release build not included. | Record a bundle that includes release if needed. |
| Release per-crate aggregates / `rmeta_time` / complete phase attribution | Accepted evidence gap. | Capture with tooling that exposes those fields. |
| Real-online demo `--online` (Task 8) | Resolved by `3e0e2d35`, `870cb693`, `aaca1dd2`, `21ef0e57`. | Re-run after capture/demo-contract changes. |
