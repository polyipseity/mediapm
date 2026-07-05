//! Built-in tool registration tests.

use mediapm::registered_builtin_ids;

/// Ensures the mediapm built-in registry exposes the expected set of tool
/// ids.
#[test]
fn builtins_are_registered() {
    let ids = registered_builtin_ids();

    assert_eq!(ids.len(), 5);
    assert!(ids.contains(&"echo@1.0.0".to_string()));
    assert!(ids.contains(&"fs@1.0.0".to_string()));
    assert!(ids.contains(&"import@1.0.0".to_string()));
    assert!(ids.contains(&"export@1.0.0".to_string()));
    assert!(ids.contains(&"archive@1.0.0".to_string()));
}
