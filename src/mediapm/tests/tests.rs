//! Integration test harness for `mediapm`.
//!
//! The `int` module covers API-level integration behavior, `e2e` covers
//! multi-step end-to-end workflows, and `prop` is reserved for property-style
//! coverage as Phase 3 behavior expands.

mod e2e;
mod int;
mod prop;
