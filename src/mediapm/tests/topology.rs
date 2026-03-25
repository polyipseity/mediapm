//! Workspace topology and built-in registration tests.

use mediapm::registered_builtin_ids;

#[test]
fn builtins_are_date_versioned_and_registered() {
    let ids = registered_builtin_ids();

    assert_eq!(ids.len(), 3);
    assert!(ids.iter().all(|id| id.contains('@')));
}
