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
| Record | `ci-build-test-44865482029`, 2026-11-21T04:18:50Z-04:33:00Z |
| Commit | `6847623b99ef40b23da1ac1d1c95a4accded113d` (`main`, push, attempt 1) |
| Environment | GitHub Actions ubuntu-latest (3 jobs) + windows-latest (1 job) |
| Result | success, ~850 s wall |
| Cache | All four jobs restore-key partial hits, `full match: false`; not an exact-key before/after pair |
| Counts | Build & Test nextest `[148.026s] 3059 run: 3059 passed (5 slow), 0 skipped`; Feature Matrix 58/58 clean with per-combination timing lines; Release Build and Windows job green |
| Evidence | <https://github.com/polyipseity/mediapm/actions/runs/44865482029>, `/tmp/t7-evidence.txt` |

Older CI runs (43609375823, 43379058938, 44753912879 failure, commitlint rows) and the D0-D7 diagnostic pass remain in git history before `91776606`.

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
| Warmed post-cache CI exact-key pair | Open; 44865482029 is restore-key partial only. | Record a clean exact-key green before/after pair here when available. |
| Runner hardware / queue internals | Accepted environment constraint. | GitHub Actions did not expose them. |
| Windows-local runs | Accepted environment constraint. | CI Windows evidence in git history; local Windows not run. |
| `/tmp` diagnostics ephemeral | Accepted; durable transcriptions kept in git history. | Re-stage raw logs durably if needed. |
| Nightly `-Z timings` | Excluded from baseline comparisons; diagnostic-only in git history. | Re-run only with owner approval and comparable metadata. |
| Fresh local full-suite R4 reruns | Resolved by `local-isolated-full-r4-9a25da7a`; release build not included. | Record a bundle that includes release if needed. |
| Release per-crate aggregates / `rmeta_time` / complete phase attribution | Accepted evidence gap. | Capture with tooling that exposes those fields. |
| Real-online demo `--online` (Task 8) | Resolved by `3e0e2d35`, `870cb693`, `aaca1dd2`, `21ef0e57`. | Re-run after capture/demo-contract changes. |
