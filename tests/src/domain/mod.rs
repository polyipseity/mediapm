pub mod canonical;
pub mod metadata;
pub mod migration;
pub mod model;
pub mod provider;

#[test]
fn domain_modules_are_reachable() {
    assert_eq!(mediapm::domain::model::LATEST_SCHEMA_VERSION, 1);
}
