pub mod executor;
pub mod planner;

#[test]
fn application_modules_are_reachable() {
    let _empty_plan = mediapm::application::planner::Plan { effects: vec![] };
    let _summary = mediapm::application::executor::SyncSummary::default();

    assert!(_empty_plan.is_empty());
    assert_eq!(_summary.planned_effects, 0);
}
