//! Integration test harness for `mediapm`.
//!
//! The `int` module covers API-level integration behavior and `e2e` covers
//! multi-step end-to-end workflows.

mod e2e;
mod int;
#[cfg(feature = "proptest")]
mod prop;
