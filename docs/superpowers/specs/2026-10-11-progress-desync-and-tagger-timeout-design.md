# Progress desync, overall-row label, and media-tagger timeout design

Approved in chat 2026-10-11. Implements three user-approved fixes; the plan is `docs/superpowers/plans/2026-10-11-progress-desync-and-tagger-timeout.md`.

## Problem 1: progress-bar rows out of sync (user-approved direction: "remove dirty; all draws via the budget")

The width budget is already re-derived unconditionally every tick: `run_frame` step 2 calls `recompute_layout()` (`src/mediapm-utils/src/progress/inner/frame.rs:147`) before any dirty filter, and it re-syncs every bound slot. Measured at width 100, every row in one frame shares the budget (bar ends identical at column 81). The desync comes from paths that bypass the settled budget, stacked on three change-detection layers:

- per-slot `dirty` flag (`inner/tracked.rs:74`, set at ~11 sites, read at `inner/frame.rs:96-101` and `:166`);
- `SlotCache` `style_*` fields and the `style_changed` comparison (`inner/renderer.rs:379-398`) as gating;
- string/position dedup inside `sync_snapshot_to_bar` (`inner/layout.rs:542-582`), which stays as write suppression.

Bypasses: blank rows render through `blank_bar_style()` = `{wide_msg}` with no width pinning (`inner/components.rs:946`); the owed-draw flush re-pushes only slot 0's prefix (`inner/frame.rs:225-230`); `attach` shifts sources between slots outside the frame; `maybe_adjust_for_resize` keeps a `last_width` force-dirty (`inner/frame.rs:152-160`).

Design: delete the dirty flag and the style dedup as gating. Every bound slot re-syncs every frame through measure → settle → apply. Blank rows, attach shifts, and resize all go through the same frame/budget. String/position dedup is retained solely to suppress redundant terminal writes. Debug JSONL loses its `dirty` field.

## Problem 2: overall row label (user-approved: static "running workflows")

The workflow screen's overall row is a re-installed `StepBarLabel` naming workflow + step + tool + version (`src/mediapm-conductor/src/orchestration/coordinator.rs:286-343`, seeds `data s3 (media-tagger) 0.0.0+6086b2`). The other two screens' overall rows are static words: `syncing tools` (`src/mediapm/src/service.rs:913`), `materializing` (`service.rs:1349`). Design: the overall row becomes the static seed `running workflows` (`service.rs:1269`), and the coordinator's label machinery (`OverallBarState`, `overall_bar_label`, `set_overall_label` and its call sites) is deleted; the pinned row only receives `set_total(step_count)`.

## Problem 3: media-tagger step slowness and RPC-timeout failure (user-approved: bound MusicBrainz client)

Root cause chain (all verified):

1. `ConductorActorClient::run_workflow` issues one RPC with `Some(self.rpc_timeout)` = 300s default (`src/mediapm-conductor/src/orchestration/node.rs:188-216`, `config.rs:30`, `defaults.rs:28`), spanning the entire workflow — levels, workers, and nested `StepWorker` RPCs (`coordinator.rs:564-566`, also 300s). The child executable timeout (`step_worker/process.rs:71-76`; 300s in the demo) starts later, so the outer RPC fires first: the observed `4m59s` tagger row is the abort moment.
2. Media-tagger is `mediapm builtin media-tagger` (launcher `exec "${MEDIAPM_EXECUTABLE}" media-tagger "$@"`). Its MusicBrainz traffic uses `musicbrainz_rs` 0.13.0 → `api_bindium` 0.4.0 with `ureq::agent()` (no timeout anywhere) and `ApiClient { max_retries: 10 }` with quadratic backoff (2, 8, 18, … ≈570s of sleeps); `rate_limit` (1/s with burst 5) is not compiled in. `MEDIAPM_HTTP_TIMEOUT_SECONDS` never reaches this path because it only configures the conductor's reqwest client.
3. Compounding: the tool is `impure = true` (never cache-hits), demo retries are `max_retries = 0`, the demo's on-disk cache dir is cold, an RPC timeout does not kill the child (`kill_on_drop` only fires when the process future is dropped).

Design: build a bounded MusicBrainz client in the media-tagger builtin — a `ureq::Agent` with `timeout_global(...)` (derived from `MEDIAPM_HTTP_TIMEOUT_SECONDS`, default 30s) and `ApiClient::builder().agent(...).max_retries(<small, e.g. 2>)`, passed through `execute_with_client_async`. A bad MusicBrainz response then fails the step in bounded time with a real error instead of grinding for ~570s and dying at an opaque RPC timeout.

## Decisions (user-approved)

1. Problem 1: remove the dirty flag; all draws through the shared budget.
2. Problem 2: overall row label is the static word `running workflows`.
3. Problem 3: bound the MusicBrainz client (timeout + capped retries); no crate replacement, no RPC-budget or child-kill changes in this cycle.
4. Sequencing: three atomic commits, progress resync → label → tagger.
