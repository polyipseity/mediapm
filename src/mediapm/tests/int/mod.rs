//! Focused integration tests for mediapm contracts.

mod builtins;
mod demo;
mod demo_online;
// CAUTION: This is tool-sync integration (MediaPmService::sync_tools()).
// Do NOT put workflow-sync or state-sync tests here.
mod tool_sync;
