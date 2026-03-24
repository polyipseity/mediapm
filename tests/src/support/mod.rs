pub mod util;

#[test]
fn support_module_is_reachable() {
    let mut value = serde_json::json!({ "b": 2, "a": 1 });
    mediapm::support::util::sort_json_value(&mut value);

    assert!(value.get("a").is_some());
    assert!(value.get("b").is_some());
}
