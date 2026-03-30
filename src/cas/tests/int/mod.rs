//! Comprehensive integration tests for public CAS APIs.

/// API workflow and round-trip integration coverage.
mod api_workflows;
/// Actor orchestration and wire-command integration coverage.
mod orchestration;
/// Reopen and persistence durability integration coverage.
mod persistence;
/// Redb durable index side-effect integration coverage.
mod redb_persistence;
/// On-disk object format/layout integration coverage.
mod storage_format;
/// Input validation and parse error integration coverage.
mod validation;
