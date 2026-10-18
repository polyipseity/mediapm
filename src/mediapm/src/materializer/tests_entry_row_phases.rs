//! Placeholder for the entry-row rendering tests.
//!
//! The tests that lived here asserted the phase-tag and per-file sub-bar model
//! (`EntryPhaseBar`, `add_variant_sub_bar`, `MaterializationPhase` tags on the
//! rendered row). The file-count redesign (Task 7) removed that machinery: a
//! row now advances by files written and carries only identity (media id,
//! variant, basename), with no phase tag. Rewriting these assertions to the
//! identity + file-count model, and renaming this file to
//! `tests_entry_rows.rs`, is Task 8's scope. This placeholder keeps the module
//! wired so that rename stays a clean operation.

#[test]
fn entry_row_identity_tests_are_task_8_scope() {
    // Intentionally empty: the phase-tag/sub-bar assertions this file held are
    // rewritten to the file-count model under Task 8, which also renames this
    // module. See the module doc above.
}
