//! Built-in tool registration tests.

use mediapm::registered_builtin_ids;

/// Ensures the mediapm built-in registry exposes the expected set of tool
/// ids.
#[test]
fn builtins_are_registered() {
    let ids = registered_builtin_ids();

    // The built-in registry returns simple tool ids (no version suffix).
    assert_eq!(ids.len(), 5);
    assert!(ids.contains(&"echo".to_string()));
    assert!(ids.contains(&"fs".to_string()));
    assert!(ids.contains(&"import".to_string()));
    assert!(ids.contains(&"export".to_string()));
    assert!(ids.contains(&"archive".to_string()));
}
