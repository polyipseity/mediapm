//! Terminal wrapper — moved to `gate.rs`.
//!
//! This module is intentionally empty. The buffered-term write gate the old
//! doc pointed at is gone too: since the plan became the single writer,
//! frames are written directly through the frame writer's terminal, and the
//! gate is just that terminal handle plus the screen-commit cursor advance.
