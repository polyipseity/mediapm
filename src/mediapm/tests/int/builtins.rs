//! Built-in tool registration tests.

use mediapm::registered_builtin_ids;

/// Ensures the mediapm built-in registry exposes the expected set of tool
/// ids.
#[test]
fn builtins_are_registered() {
    let ids = registered_builtin_ids();

    assert_eq!(ids.len(), 5);
    assert!(ids.contains(&"echo@v1".to_string()));
    assert!(ids.contains(&"fs@v1".to_string()));
    assert!(ids.contains(&"import@v1".to_string()));
    assert!(ids.contains(&"export@v1".to_string()));
    assert!(ids.contains(&"archive@v1".to_string()));
}
