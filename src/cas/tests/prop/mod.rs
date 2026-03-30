//! Property-testing suite wiring.

/// Hash identity and encoding property checks.
mod property_hash_tests;

/// End-to-end round-trip properties for in-memory backend.
mod property_cas_roundtrip_tests;

/// Filesystem persistence/reopen property checks.
mod property_filesystem_tests;

/// Constraint-pruning and candidate-consistency property checks.
mod property_constraint_tests;

/// Delta-chain reconstruction property checks across backends.
mod property_delta_chain_tests;
