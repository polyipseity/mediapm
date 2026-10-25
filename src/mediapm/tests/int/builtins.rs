//! Built-in tool registration tests.

use mediapm::registered_builtin_ids;

/// The ids mediapm offers are exactly the registry's production ids.
///
/// The list is derived from the registry: a literal here would drift the moment
/// the registry gains an entry, and the parity assertion turns that drift into a
/// failure at the next test run instead of a document that silently omits a
/// builtin.
#[test]
fn parity_builtin_id_lists_cannot_diverge() {
    let ids = registered_builtin_ids();
    let expected: Vec<String> = mediapm_conductor::tools::production_builtins()
        .map(|entry| entry.builtin_id.to_string())
        .collect();

    assert_eq!(ids, expected, "mediapm must offer exactly the production builtin ids");
    assert!(
        !ids.iter().any(|id| id == "flaky@v1"),
        "a test double in the offered list would let a generated document carry a tool that fails on purpose"
    );
}
