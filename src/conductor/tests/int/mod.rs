//! Contract-focused integration scenarios for the conductor.

/// Bootstrap, validation, and state-shape focused checks.
mod bootstrap;

/// CLI state-command parse parity checks.
mod state_cli;

/// Instance GC with configurable TTL.
mod gc;
