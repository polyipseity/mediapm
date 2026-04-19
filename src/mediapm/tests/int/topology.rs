//! Workspace topology and built-in registration tests.

use mediapm::registered_builtin_ids;

/// Ensures the Phase 3 built-in registry exposes the migrated import/export
/// tool set with stable date-versioned identifiers.
#[test]
fn builtins_are_date_versioned_and_registered() {
    let ids = registered_builtin_ids();

    assert_eq!(ids.len(), 5);
    assert!(ids.iter().all(|id| id.contains('@')));
    assert!(ids.iter().any(|id| id.starts_with("builtins.import@")));
    assert!(ids.iter().any(|id| id.starts_with("builtins.export@")));
    assert!(!ids.iter().any(|id| id.starts_with("builtins.fetch@")));
}
