//! Contract-focused integration scenarios for the conductor.

/// Bootstrap, validation, and state-shape focused checks.
mod bootstrap;

/// Instance GC with configurable TTL.
mod gc;

/// Decode + migration pipeline (regression: record field shorthand).
mod decode_migration;
