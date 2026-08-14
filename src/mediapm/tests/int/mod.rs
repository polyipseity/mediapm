//! Focused integration tests for mediapm contracts.

mod builtins;
mod demo;
mod demo_hierarchy_golden;
mod demo_online;
/// Shared integration-test scaffolding (hermetic service construction).
mod helpers;
mod online_sync_post_sync_dump;
/// Nickel schema strictness tests (S-C1..S-C10) — validates the strict
/// closed-contract surface of `v1.ncl`/`mod.ncl` and the exported JSON schema.
mod schema_strictness;
/// Nickel schema sync-prevention tests — validates v1.ncl stays in sync
/// with `MediaPmDocument` / `MediaPmDocumentEnvelopeV1` Rust types.
mod schema_sync;
// CAUTION: This is tool-sync integration (MediaPmService::sync_tools()).
// Do NOT put workflow-sync or state-sync tests here.
mod tool_sync;

/// All-platform document structure integration: verifies that managed
/// tools have per-OS content-map entries and non-empty command selectors.
mod all_platform;
/// Dual-write strategy: state.json always-writes, NCL files skip when unchanged.
mod dual_write;
/// Runtime root `.gitignore` creation on service construction.
mod runtime_gitignore;
/// State JSON persistence and migration tests.
mod state_persistence;
