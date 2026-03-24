pub mod config;

#[test]
fn configuration_module_is_reachable() {
    let config = mediapm::configuration::config::AppConfig::default();
    assert!(config.sources.is_empty());
    assert!(config.links.is_empty());
}
