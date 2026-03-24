#[test]
fn crate_exports_core_modules() {
    let _default_config = mediapm::configuration::config::DEFAULT_CONFIG_FILE;
    let _paths = mediapm::infrastructure::store::WorkspacePaths::new(".");

    assert_eq!(_default_config, "mediapm.json");
}
