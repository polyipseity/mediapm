pub mod enrichment;
pub mod executor;
pub mod history;
pub mod planner;

#[test]
fn application_modules_are_reachable() {
    let _empty_plan = mediapm::application::planner::Plan { effects: vec![] };
    let _summary = mediapm::application::executor::SyncSummary::default();
    let _enrichment = mediapm::application::enrichment::ProviderEnrichmentSummary::default();
    let _history_summary = mediapm::application::history::HistoryRecordSummary {
        canonical_uri: "file:///tmp/song.flac".to_owned(),
        event_id: "evt_test".to_owned(),
        kind: mediapm::domain::model::EditKind::Revertable,
        operation: "metadata_update".to_owned(),
        from_variant_hash: "00".repeat(32),
        to_variant_hash: "00".repeat(32),
        variant_created: false,
    };

    assert!(_empty_plan.is_empty());
    assert_eq!(_summary.planned_effects, 0);
    assert_eq!(_enrichment.sidecars_updated, 0);
    assert_eq!(_history_summary.operation, "metadata_update");
}
