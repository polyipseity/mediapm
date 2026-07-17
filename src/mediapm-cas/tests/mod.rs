//! Integration test harness for `mediapm-cas`.
//!
//! Tests the public API surface: `CasApi`, `ConstraintApi`, and
//! `CasMaintenanceApi`.
//!
//! All tests use [`InMemoryCas`](mediapm_cas::storage::in_memory::new_in_memory_cas).

mod common;
mod int;
