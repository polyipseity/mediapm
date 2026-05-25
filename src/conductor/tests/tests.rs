//! Integration test harness for `mediapm-conductor`.
//!
//! The `int` module covers contract-focused integration behavior, `e2e`
//! contains multi-step workflow scenarios, and `prop` is reserved for
//! property-driven coverage as the conductor evolves.

mod e2e;
mod int;
mod prop;
