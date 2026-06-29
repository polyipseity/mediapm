//! Contract-focused integration scenarios for the conductor.

/// Bootstrap, validation, and state-shape focused checks.
mod bootstrap;

/// Instance GC with configurable TTL.
mod gc;

/// Decode + migration pipeline (regression: record field shorthand).
mod decode_migration;

/// Nickel schema sync-prevention tests — validates v2.ncl stays in sync
/// with `NickelDocument` Rust types.
mod schema_sync;

/// Platform filtering: cfg-derived FOREIGN_PLATFORM_DIRS correctness and
/// the explicit link_to_sandbox_filtered API.
mod platform_filtering;
