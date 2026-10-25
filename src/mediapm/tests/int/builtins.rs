//! Built-in tool registration tests.

use mediapm::registered_builtin_ids;

/// The ids mediapm offers are exactly the registry's production ids.
///
/// The list used to be restated here and in `service_standalone`, and the two
/// disagreed: the registry carried the `flaky` test double and mediapm's copy
/// did not. Both sides now derive from the registry, and this test fails if
/// either regains a literal — a restated list drifts the moment the registry
/// gains an entry.
#[test]
fn parity_builtin_id_lists_cannot_diverge() {
    let ids = registered_builtin_ids();
    let expected: Vec<String> = mediapm_conductor::tools::ALL_BUILTINS
        .iter()
        .filter(|entry| !entry.test_only)
        .map(|entry| entry.builtin_id.to_string())
        .collect();

    assert_eq!(ids, expected, "mediapm must offer exactly the production builtin ids");
    assert!(
        !ids.iter().any(|id| id == "flaky@v1"),
        "a test double in the offered list would let a generated document carry a tool that fails on purpose"
    );
}
