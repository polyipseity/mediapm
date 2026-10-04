//! Focused integration tests for mediapm contracts.

mod builtins;
mod demo;
mod demo_hierarchy_golden;
mod demo_online;
mod online_sync_post_sync_dump;
/// Nickel schema strictness tests (S-C1..S-C10) — validates the strict
/// closed-contract surface of `v1.ncl`/`mod.ncl` and the exported JSON schema.
mod schema_strictness;
/// Nickel schema sync-prevention tests — pins the live Nickel contract
/// sources against the `MediaPmDocument` Rust runtime model they resolve to,
/// and pins that no Rust migration dispatcher mirrors the Nickel ladder.
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
/// Per-phase sync observer wiring and centralized output primitives.
mod sync_observer;
/// One-terminal-per-sync progress ownership across all three sync phases.
mod sync_progress;
