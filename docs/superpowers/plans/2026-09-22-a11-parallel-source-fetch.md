---
status: complete
committed: yes
current-step: 7
inputs:
  atomicCommits: yes
  backwardsCompat: no
  maxConcurrency: 2
---

# Plan: A11 — parallelize per-source fetch within a single tool

## Context

R6 results (cold-run `measure-tool-sync-split.sh`):

```text
total_wall_clock: 297s
R_tool: 0.49 (146.6s / 297s)
R_split: fetch=133.7s process=43.0s
R_split_ratio: fetch=0.76 process=0.24
slowest_tool: deno (115.9s)
per_tool (provisioned tools, sorted by duration):
  deno: 115.9s
  ffmpeg: 68.6s
  yt-dlp: 29.1s
  rsgain: 3.2s
  sd: 1.4s
```

Decision rule applied:

- R_tool=0.49 ≥ 0.25 → A10/A11 evaluated
- fetch share=0.91 ≥ 0.4 → **A11 candidate**
- process share=0.29 < 0.4 → **A10 closed**

Cross-tool parallelism (MAX_CONCURRENT=4) compressed per_tool sum 218.2s → 146.6s (−33%), so the pipeline is not purely bandwidth-saturated. The critical path is **one tool: deno at 115.9s = 79% of tool-sync**.

## Phase 1 — Measurement (no production change)

The decisive unknown: is deno's 115.9s **one dominant source** or **N balanced sources**, and is it **bandwidth-bound** or **serialization-bound**?

The retained JSONL at `/var/folders/p_/7x6tl4551dz0mkcclw1xqph00000gn/T/tmp.chZp5cwGwz` has per-tick data. The fetch bar's item counter (`{completed}/{total}` sources) is emitted each tick via `suffix`. We can extract per-source completion timestamps.

### Tasks

| # | Task | Gate |
| --- | --- | --- |
| **A11-0a** | Add `--source-report <ticks> <run-txt>` to the measure script: read the retained JSONL, identify deno's `[fch]` bar, extract per-source completion timestamps from the item counter transitions, print per-source wall-clock. Validate per-source sum ≈ 115.9s. | Q1 |
| **A11-0b** | Compute link throughput from ffmpeg: ffmpeg=163MB in 68.6s ≈ 2.4 MB/s. If deno's total_bytes / 2.4 MB/s ≈ 115.9s → bandwidth-bound (A11 closed). If much less → serialization-bound (A11 proceeds). Derive deno's total_bytes from the fetch bar's final `position` value. | Q2 |
| **A11-0c** | Verify media-tagger absence: check the JSONL for any `media-tagger` bar with `is_active(b) == True`. If none found → launcher artifact (benign). If found but skipped → log as data-quality note. | data quality |

### Decision gate after Phase 1

| Condition | Outcome |
| --- | --- |
| deno has one source ≥ 80% of its time | **close A11** — no parallelism benefit |
| deno is bandwidth-bound (link-rate ≈ measured) | **close A11** — pipe already saturated |
| deno has ≥ 2 balanced sources **and** is serialization-bound | **proceed to Phase 2** |
| media-tagger absent for unexpected reason | document as known limitation |

## Phase 2 — Implementation (only if gate clears)

| # | Task |
| --- | --- |
| **A11-1** | Prefix-sum `item_idx` offsets over `fetch.sources` → each source owns `(item_idx, item_count)`. |
| **A11-2** | Bounded parallel fetch: `buffer_unordered(MAX_CONCURRENT_SOURCE_FETCHES=3)`. |
| **A11-3** | Index-preserving collection — results reassembled in source order. |
| **A11-4** | Per-source extraction dirs (`temp_root/{os_label}/{source_idx}`). |
| **A11-5** | Progress-callback safety under concurrent `advance`/`set_total`. |
| **A11-6** | Determinism test (order-independence). |
| **A11-7** | Re-measure: keep if `R_tool` drops from 0.49 to < 0.40 (≥ 25s saved); revert otherwise. |

## Non-goals

- A10 (closed: process share 0.29 < 0.4).
- Raising `MAX_CONCURRENT_TOOL_PROVISIONING` (critical path is one tool).
- Reopening R1-R5 harness.

## Risks

| Risk | Mitigation |
| --- | --- |
| Bandwidth-bound; parallelism splits the same pipe | A11-0b resolves before any code |
| deno = one dominant source | A11-0a resolves; close A11 with measurement |
| Non-deterministic content_map under concurrency | index-preserving collection + BTreeMap |
| Progress bar churn under parallel updates | atomic budget; 50ms ticker coalesces |
| media-tagger gap hides a real bug | A11-0c makes it explicit |

## Outcome

**A11 kept.** Parallel source fetch is implemented, tested, and committed. The A/B gate (A11-7) is **inconclusive** due to three protocol defects discovered during Measurement 1:

1. **D5**: `MAX_CONCURRENT_SOURCE_FETCHES` is a Rust `const`, not env-driven — the sequential control run was a no-op.
2. **D6**: The demo's 600s hard watchdog killed both runs before tool sync completed.
3. **Path collision**: `--keep-ticks` paths collided, overwriting run 1's ticks.

The data does not measure A11's effect. No revert is warranted based on timeout-bound data. The corrected protocol is documented in `docs/superpowers/plans/2026-09-22-a11-remaining-tasks.md` (D5, D6) and the invalid measurement data is recorded in `docs/superpowers/plans/2026-09-22-a11-measurement-results.md`.

**Commits:**

- `3ecd9f53` — `perf(conductor): parallelize per-source fetch within tool download`
- `5804f4a6` — `test(conductor): add determinism test for parallel source fetch`
- `e145e18e` — `docs(coverage): add parallel source fetch coverage rows`
- `5ecc8c0d` — `docs(conductor): document parallel source fetch in 3-phase provisioning spec`
- `e339cffc` — `fix(conductor): report monotone completed-source count in parallel fetch` (A11-5)
- `f2b049ba` — `fix(conductor): reject duplicate content_map keys in process phase` (A11-4)
- `39349e82` — `docs(plans): record A11-7 Measurement 1 results (invalid — protocol defects)`

**A11-4 re-scoped:** The original spec called for per-source extraction directories as the primary fix. Investigation showed every current provider emits one source per OS, so the collision is unreachable today. The real latent defect is silent `content_map` overwrite via `content_map.extend`. Task 2 fixes the real defect (fail-fast duplicate-key guard) and adds per-source extraction dirs as defense-in-depth. This is recorded as decision D2 in the remaining-tasks plan.

**Re-measurement deferred.** A11-7 requires a corrected re-run (edit the source constant for the sequential control, extend the demo timeout, use different `--keep-ticks` paths). The re-run is tracked in the remaining-tasks plan as Tasks 4-5 (currently invalidated, pending re-execution with the corrected protocol).
