# Spec-to-test coverage matrix

<!--
Legend:
  🟢 Covered
  🟡 Partial
  🔴 Uncovered / not yet implemented
-->

## State persistence (`state.json`)

| # | Spec item | Test status | Notes |
|---|-----------|-------------|-------|
| 1 | V2 round-trip: construct → `to_json_value` → `from_json_value` → compare | � | `int::state_persistence::v2_round_trip` |
| 2 | V1→V2 migration: pre-rewrite wrapper → `from_json_value` → verify v2 model | 🟢 | `int::state_persistence::migrate_v1_wrapper_to_v2` |
| 3 | Flat→V2 migration: current flat format → `migrate_from_old_nickel` → verify v2 model | 🟢 | `int::state_persistence::migrate_flat_to_v2` |
| 4 | `state.ncl`→`state.json` migration: on-disk `state.ncl` → load → `state.json` written, `.ncl` removed | 🟢 | `int::state_persistence::ncl_to_json_file_migration` |
| 5 | Idempotency: save → re-save → byte-identical | 🟢 | `int::state_persistence::json_save_idempotent` |
| 6 | Regression: tool sync skips already-deployed tools (second sync is no-op) | 🟢 | `int::tool_sync::sync_is_idempotent` (covers same behavior) |

## Tool sync

| # | Spec item | Test status | Notes |
|---|-----------|-------------|-------|
| 1 | sync creates state document (`state.json`) | � | `int::tool_sync::sync_creates_state_document` |
| 2 | sync is idempotent (byte-identical `state.json` on re-sync) | 🟢 | `int::tool_sync::sync_is_idempotent` |
