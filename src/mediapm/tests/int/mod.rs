//! Focused integration tests for mediapm contracts.

mod builtins;
mod demo;
mod demo_online;
/// Nickel schema sync-prevention tests — validates v1.ncl stays in sync
/// with `MediaPmDocument` / `MediaPmDocumentEnvelopeV1` Rust types.
mod schema_sync;
// CAUTION: This is tool-sync integration (MediaPmService::sync_tools()).
// Do NOT put workflow-sync or state-sync tests here.
mod tool_sync;
