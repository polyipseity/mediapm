pub mod formatter;
pub mod gc;
pub mod store;
pub mod verify;

#[test]
fn infrastructure_modules_are_reachable() {
    let report = mediapm::infrastructure::verify::VerifyReport::default();
    assert!(report.is_clean());
}
