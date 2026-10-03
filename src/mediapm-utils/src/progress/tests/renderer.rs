use super::super::inner::*;
use super::*;

// ---- Phase 4: BufferedTerm / dirty tracking tests ----------------------

#[test]
fn dirty_tracking_initial_state_starts_dirty() {
    // The SharedState dirty flag starts true so the very first tick always
    // draws, even without explicit mutations.
    let term = indicatif::InMemoryTerm::new(10, 80);
    let terminal = ProgressTerminal::builder()
        .with_term_like(Box::new(term.clone()))
        .with_pre_roll_capture(super::pre_roll_capture())
        .capacity(4)
        // Disable the daemon ticker: it fires group.tick() on real
        // wall-clock time, injecting extra ticks/draws beyond the manual
        // ticks this test controls.
        .with_ticker_enabled(false)
        .build();
    let group = terminal.screen().build();
    let _bar = group.add_bar(100, "test");

    group.tick();
    let content = term.contents();
    assert!(!content.is_empty(), "first tick must draw, got empty");
}

#[test]
fn multiple_mutations_before_tick_single_draw() {
    // Several mutations between ticks should all be reflected in a single
    // coherent draw after the next tick, without intermediate draws.
    let term = indicatif::InMemoryTerm::new(10, 80);
    let terminal = ProgressTerminal::builder()
        .with_term_like(Box::new(term.clone()))
        .with_pre_roll_capture(super::pre_roll_capture())
        .capacity(4)
        // Disable the daemon ticker: it fires group.tick() on real
        // wall-clock time, injecting extra ticks/draws beyond the manual
        // ticks this test controls.
        .with_ticker_enabled(false)
        .build();
    let group = terminal.screen().build();
    let bar = group.add_bar(100, "test");

    bar.set_position(20);
    bar.set_total(200);
    bar.set_prefix_components(PrefixComponents { tool_name: "multi".into(), ..Default::default() });

    group.tick();
    let content = term.contents();
    assert!(content.contains("20/200"), "expected 20/200 in output: {content:?}");
    assert!(content.contains("multi"), "expected prefix 'multi' in output: {content:?}");
}

#[test]
fn finalize_produces_final_output() {
    // join_and_clear (finalize) must produce visible output showing the
    // final state of all bars.  Uses TestTimeSource for deterministic
    // timing and exact terminal content matching.
    use std::sync::Arc;
    use std::time::Duration;

    let term = indicatif::InMemoryTerm::new(10, 80);
    let ts = Arc::new(super::super::TestTimeSource::new());
    let terminal = ProgressTerminal::builder()
        .with_term_like(Box::new(term.clone()))
        .with_pre_roll_capture(super::pre_roll_capture())
        .with_time_source(Arc::clone(&ts) as Arc<dyn super::super::TimeSource>)
        // Disable the daemon ticker: it fires group.tick() on real
        // wall-clock time, injecting extra ticks/draws beyond the manual
        // ticks this test controls.
        .with_ticker_enabled(false)
        // 3 child slots + the overall slot: 4 slots total, as before.
        .capacity(3)
        .build();
    let (group, overall) = terminal.screen().with_overall("overall", 10).build();
    let bar = group.add_bar(100, "child");
    bar.advance(50);
    overall.advance(5);
    ts.advance(Duration::from_secs(1));

    // Sync state from SharedState to bars before finalize (finalize
    // only syncs non-Active bars).
    group.tick();
    group.join_and_clear();
    let actual = term.contents();
    let lines: Vec<&str> = actual.lines().collect();

    // EXACT line count: exactly 2 visible lines (child + overall).
    assert_eq!(
        lines.len(),
        2,
        "finalize must show exactly 2 bars, got {} lines:\n{actual}",
        lines.len(),
    );

    // Line 0: child bar — prefix and position visible.
    assert!(lines[0].contains("child"), "child prefix in line 0: {0}", lines[0]);
    assert!(lines[0].contains("50/100"), "child pos 50/100: {0}", lines[0]);

    // Line 1: overall bar — prefix and position visible.
    assert!(lines[1].contains("overall"), "overall prefix in line 1: {0}", lines[1]);
    assert!(lines[1].contains("5/10"), "overall pos 5/10: {0}", lines[1]);

    // Both lines show elapsed (1s).
    assert!(lines[0].contains("1s"), "child shows 1s elapsed: {0}", lines[0]);
    assert!(lines[1].contains("1s"), "overall shows 1s elapsed: {0}", lines[1]);
}

#[test]
fn dirty_tracking_skips_clean_ticks() {
    // A tick without any mutation must produce identical terminal content
    // to the previous tick (dirty tracking skips the slot, bar.tick() is
    // not called, no redraw occurs).

    // Helper: extract the body (everything after the spinner char).
    fn content_body(s: &str) -> &str {
        s.lines().find_map(|l| l.get(1..)).unwrap_or("")
    }

    use super::super::inner::DimensionSource;
    use std::sync::Arc;

    let term = indicatif::InMemoryTerm::new(10, 80);
    let dims = Arc::new(super::super::inner::TestDimensionSource::new((10, 80)));
    let ts = Arc::new(super::super::TestTimeSource::new());

    let terminal = ProgressTerminal::builder()
        .with_term_like(Box::new(term.clone()))
        .with_pre_roll_capture(super::pre_roll_capture())
        .with_dim_source(dims as Arc<dyn DimensionSource>)
        .with_time_source(ts.clone() as Arc<dyn super::super::TimeSource>)
        // Disable the daemon ticker: it fires group.tick() on real
        // wall-clock time, injecting extra ticks/draws beyond the manual
        // ticks this test controls.
        .with_ticker_enabled(false)
        .capacity(4)
        .build();
    let group = terminal.screen().build();
    let bar = group.add_bar(100, "test");
    bar.set_position(42);
    ts.advance(std::time::Duration::from_millis(100));

    // Tick 1: baseline (bar appears).
    group.tick();
    let baseline = term.contents();
    assert!(!baseline.is_empty(), "baseline must have content");

    let baseline_body = content_body(&baseline);

    // Tick 2: no mutations → content body (everything except spinner)
    // must be identical.  The daemon ticker is disabled above, so the
    // spinner char is deterministic too; we still compare only the body
    // to keep the assertion focused on dirty tracking.
    ts.advance(std::time::Duration::from_millis(50));
    group.tick();
    let second = term.contents();
    assert_eq!(
        content_body(&second),
        baseline_body,
        "clean tick should not change content body\n\
         expected body: {baseline_body:?}\n\
         got body:      {:?}",
        content_body(&second)
    );

    // Tick 3: still no mutations → content body remains unchanged.
    ts.advance(std::time::Duration::from_millis(50));
    group.tick();
    let third = term.contents();
    assert_eq!(
        content_body(&third),
        baseline_body,
        "second clean tick should also not change content body\n\
         expected body: {baseline_body:?}\n\
         got body:      {:?}",
        content_body(&third)
    );
}

#[test]
fn dirty_tracking_draws_on_mutation() {
    // A tick after mutation must reflect the new state in the output.
    use super::super::inner::DimensionSource;
    use std::sync::Arc;

    let term = indicatif::InMemoryTerm::new(10, 80);
    let dims = Arc::new(super::super::inner::TestDimensionSource::new((10, 80)));
    let ts = Arc::new(super::super::TestTimeSource::new());

    let terminal = ProgressTerminal::builder()
        .with_term_like(Box::new(term.clone()))
        .with_pre_roll_capture(super::pre_roll_capture())
        .with_dim_source(dims as Arc<dyn DimensionSource>)
        .with_time_source(ts.clone() as Arc<dyn super::super::TimeSource>)
        // Disable the daemon ticker: it fires group.tick() on real
        // wall-clock time, injecting extra ticks/draws beyond the manual
        // ticks this test controls.
        .with_ticker_enabled(false)
        .capacity(4)
        .build();
    let group = terminal.screen().build();
    let bar = group.add_bar(100, "test");
    bar.set_position(10);
    ts.advance(std::time::Duration::from_millis(100));

    // Tick 1: baseline shows 10/100.
    group.tick();
    let baseline = term.contents();
    assert!(baseline.contains("10/100"), "baseline must contain 10/100, got: {baseline:?}");

    // Mutate position between ticks.
    bar.set_position(80);
    ts.advance(std::time::Duration::from_millis(50));

    // Tick 2: must show the new position (body changes even if
    // the spinner also advanced via daemon ticker).
    group.tick();
    let after = term.contents();
    assert!(after.contains("80/100"), "expected 80/100 after mutation+tick, got: {after:?}");
}

// ---- Pre-roll tests ----------------------------------------------------

#[test]
fn pre_roll_reserves_full_terminal_height() {
    // Pre-roll writes one blank line per dimension-source row and restores the
    // cursor: move_cursor_down(rows) → rows × write_line("") → move_cursor_up(rows).
    use super::super::inner::DimensionSource;
    use std::sync::Arc;

    let mp_term = indicatif::InMemoryTerm::new(10, 80);
    let cap_term = indicatif::InMemoryTerm::new(100, 80);
    let dims = Arc::new(super::super::inner::TestDimensionSource::new((10, 80)));
    let ts = Arc::new(super::super::TestTimeSource::new());

    let terminal = super::super::ProgressTerminal::builder()
        .with_term_like(Box::new(mp_term.clone()))
        .with_pre_roll_capture(Box::new(cap_term.clone()))
        .with_dim_source(dims as Arc<dyn DimensionSource>)
        .with_time_source(ts as Arc<dyn super::super::TimeSource>)
        .capacity(10)
        .with_ticker_enabled(false)
        .build();

    // Pre-roll fires synchronously when the first screen is created.
    let _screen = terminal.screen().build();

    let moves = cap_term.moves_since_last_check();
    let newline_count = moves.lines().filter(|l| l.trim() == "NewLine").count();
    assert_eq!(
        newline_count, 10,
        "pre_roll should write exactly 10 blank lines (rows=10), got {newline_count}:\n{moves}"
    );
    assert!(moves.contains("Up(10)"), "pre_roll should move cursor up 10 rows:\n{moves}");
}

/// Pre-roll belongs to the terminal, not the screen: three phase screens of
/// one sync must scroll the terminal exactly once.
#[test]
fn pre_roll_fires_once_per_terminal() {
    use super::super::inner::DimensionSource;
    use std::sync::Arc;

    let mp_term = indicatif::InMemoryTerm::new(10, 80);
    let cap_term = indicatif::InMemoryTerm::new(100, 80);
    let dims = Arc::new(super::super::inner::TestDimensionSource::new((10, 80)));
    let ts = Arc::new(super::super::TestTimeSource::new());

    let terminal = super::super::ProgressTerminal::builder()
        .with_term_like(Box::new(mp_term.clone()))
        .with_pre_roll_capture(Box::new(cap_term.clone()))
        .with_dim_source(dims.clone() as Arc<dyn DimensionSource>)
        .with_time_source(ts as Arc<dyn super::super::TimeSource>)
        .capacity(10)
        .with_ticker_enabled(false)
        .build();

    let first = terminal.screen().build();
    let first_moves = cap_term.moves_since_last_check();
    let first_newlines = first_moves.lines().filter(|l| l.trim() == "NewLine").count();
    assert_eq!(first_newlines, 10, "first screen must pre-roll:\n{first_moves}");

    first.join();
    // A terminal height change must not re-arm the one-shot pre-roll.
    dims.set((20, 80));
    let _second = terminal.screen().build();

    let second_moves = cap_term.moves_since_last_check();
    let second_newlines = second_moves.lines().filter(|l| l.trim() == "NewLine").count();
    assert_eq!(
        second_newlines, 0,
        "second screen must NOT pre-roll again, got {second_newlines} newlines:\n{second_moves}"
    );
}

#[test]
fn pre_roll_with_overall() {
    use super::super::inner::DimensionSource;
    use std::sync::Arc;

    let mp_term = indicatif::InMemoryTerm::new(10, 80);
    let cap_term = indicatif::InMemoryTerm::new(100, 80);
    let dims = Arc::new(super::super::inner::TestDimensionSource::new((10, 80)));
    let ts = Arc::new(super::super::TestTimeSource::new());

    let terminal = super::super::ProgressTerminal::builder()
        .with_term_like(Box::new(mp_term.clone()))
        .with_pre_roll_capture(Box::new(cap_term.clone()))
        .with_dim_source(dims as Arc<dyn DimensionSource>)
        .with_time_source(ts as Arc<dyn super::super::TimeSource>)
        .capacity(10)
        .with_ticker_enabled(false)
        .build();

    let (_screen, _overall) = terminal.screen().with_overall("total", 100).build();

    let moves = cap_term.moves_since_last_check();
    let newline_count = moves.lines().filter(|l| l.trim() == "NewLine").count();
    assert_eq!(
        newline_count, 10,
        "pre_roll (with overall) should write exactly 10 blank lines, got {newline_count}:\n{moves}"
    );
    assert!(
        moves.contains("Up(10)"),
        "pre_roll (with overall) should move cursor up 10 rows:\n{moves}"
    );
}

#[test]
fn pre_roll_with_existing_content_scrolls_it_away() {
    use super::super::inner::DimensionSource;
    use indicatif::TermLike;
    use std::sync::Arc;

    let term = indicatif::InMemoryTerm::new(10, 80);
    // Write content BEFORE progress group (simulates terminal state).
    for i in 1..=5 {
        let _ = term.write_line(&format!("existing content line {i}"));
    }
    // Move cursor UP 5 to simulate cursor at top (worst case).
    let _ = term.move_cursor_up(5);
    let initial_content = term.contents();
    assert!(initial_content.contains("existing content line 1"), "content must be written");
    assert!(initial_content.contains("existing content line 5"), "content must be written");

    // Same InMemoryTerm for both draw target AND pre_roll capture.
    let dims = Arc::new(super::super::inner::TestDimensionSource::new((10, 80)));
    let ts = Arc::new(super::super::TestTimeSource::new());

    let terminal = super::super::ProgressTerminal::builder()
        .with_term_like(Box::new(term.clone()))
        .with_pre_roll_capture(Box::new(term.clone()))
        .with_dim_source(dims as Arc<dyn DimensionSource>)
        .with_time_source(ts.clone() as Arc<dyn super::super::TimeSource>)
        .capacity(10)
        .with_ticker_enabled(false)
        .build();

    let group = terminal.screen().build();

    let bar = group.add_bar(100, "work");
    bar.set_position(100);
    ts.advance(std::time::Duration::from_millis(100));
    bar.finish_success();

    group.tick();
    group.join();

    let after = term.contents();
    assert!(
        !after.contains("existing content"),
        "existing content must be scrolled away, got: {after:?}",
    );
    let bar_line = after.lines().next().expect("expected at least one bar line");
    let spinner_len = bar_line.chars().next().unwrap().len_utf8();
    let body = &bar_line[spinner_len..];
    assert_eq!(
        body, "     work █████████████████████████████████████████████████████████  100/100 0s",
        "bar body after spinner must match exactly",
    );
}

#[test]
fn sync_slot_preserves_custom_suffix_on_attach() {
    // Regression: when `add_bar` triggers `attach` → `sync_slot`, the
    // slot is synced with only the auto-computed RHS suffix, dropping
    // any custom suffix that was set via `set_suffix_components`.
    //
    // Without the fix, sync_slot overwrites the suffix with only the
    // auto-computed RHS, dropping the custom part.  With the fix,
    // sync_slot delegates to sync_snapshot_to_bar which appends the
    // custom suffix.
    //
    // Bar A is kept ACTIVE (not finished) so the tick drain-loop (which
    // unconditionally re-syncs non-active bars) does not rescue the
    // suffix.  Only the dirty-tracking loop processes A — and without
    // the fix A is not dirty after the attach, so the wrong suffix
    // persists.
    use std::sync::Arc;
    use std::time::Duration;

    let term = indicatif::InMemoryTerm::new(10, 80);
    let ts = Arc::new(super::super::TestTimeSource::new());
    let terminal = super::super::ProgressTerminal::builder()
        .with_term_like(Box::new(term.clone()))
        .with_pre_roll_capture(super::pre_roll_capture())
        .with_time_source(Arc::clone(&ts) as Arc<dyn super::super::TimeSource>)
        .capacity(2)
        .build();
    let group = terminal.screen().build();

    // Phase 1: add bar A, set custom suffix and partial progress, sync.
    let bar_a = group.add_bar(100, "resolve");
    bar_a.set_position(50);
    bar_a.set_suffix_components(SuffixComponents {
        custom: "cached (1)".into(),
        ..Default::default()
    });
    ts.advance(Duration::from_millis(100));
    group.tick();

    // Baseline: custom suffix is visible after first tick.
    let baseline = term.contents();
    assert!(
        baseline.contains("cached (1)"),
        "baseline must show custom suffix after tick:\n{baseline}",
    );

    // Phase 2: add bar B — triggers attach → shift → sync_slot on A.
    let bar_b = group.add_bar(100, "fetch");
    bar_b.set_position(0);

    // Check terminal output IMMEDIATELY after add_bar + set_position.
    let after_attach = term.contents();
    assert!(
        after_attach.contains("cached (1)"),
        "custom suffix lost after add_bar + set_position (before tick).\n\
             Terminal output:\n{after_attach}",
    );
    std::mem::drop(bar_a);
    std::mem::drop(bar_b);
}

// ---- structured suffix merge semantics (Phase 5) --------------------

#[test]
fn suffix_stored_state_is_structured() {
    // set_suffix_components must store the full structured set (all six
    // fields), not just `custom` — snapshot() carries them through so the
    // sync path can merge field-by-field.
    let h = ProgressBarHandle::new(100);
    h.set_suffix_components(SuffixComponents {
        count: "9".into(),
        total: "9".into(),
        elapsed: "1:23:45".into(),
        rate: Some("10/s".into()),
        eta: Some("[0:00:07]".into()),
        custom: "cached".into(),
    });
    let snap = h.snapshot();
    assert_eq!(snap.suffix_components.count, "9");
    assert_eq!(snap.suffix_components.total, "9");
    assert_eq!(snap.suffix_components.elapsed, "1:23:45");
    assert_eq!(snap.suffix_components.rate.as_deref(), Some("10/s"));
    assert_eq!(snap.suffix_components.eta.as_deref(), Some("[0:00:07]"));
    assert_eq!(snap.suffix_components.custom, "cached");
}

#[test]
fn suffix_merge_user_count_total_overrides_auto() {
    // User-set count/total components must override the auto-derived
    // count/total from the snapshot position.
    use std::sync::Arc;
    use std::time::Duration;

    let term = indicatif::InMemoryTerm::new(10, 80);
    let ts = Arc::new(super::super::TestTimeSource::new());
    let terminal = ProgressTerminal::builder()
        .with_term_like(Box::new(term.clone()))
        .with_pre_roll_capture(super::pre_roll_capture())
        .with_time_source(Arc::clone(&ts) as Arc<dyn super::super::TimeSource>)
        .capacity(2)
        .build();
    let group = terminal.screen().build();

    let bar = group.add_bar(100, "test");
    bar.set_position(50);
    bar.set_suffix_components(SuffixComponents {
        count: "9".into(),
        total: "9".into(),
        ..Default::default()
    });
    ts.advance(Duration::from_millis(100));
    group.tick();

    let content = term.contents();
    assert!(content.contains("9/9"), "user count/total must override auto-derived:\n{content}");
    assert!(
        !content.contains("50/100"),
        "auto count/total must not show when user overrides:\n{content}",
    );
}

#[test]
fn suffix_merge_user_rate_eta_elapsed_override_auto() {
    // User-set elapsed/rate/eta components must override the auto-derived
    // ticker fields.
    use std::sync::Arc;
    use std::time::Duration;

    let term = indicatif::InMemoryTerm::new(10, 80);
    let ts = Arc::new(super::super::TestTimeSource::new());
    let terminal = ProgressTerminal::builder()
        .with_term_like(Box::new(term.clone()))
        .with_pre_roll_capture(super::pre_roll_capture())
        .with_time_source(Arc::clone(&ts) as Arc<dyn super::super::TimeSource>)
        .capacity(2)
        .build();
    let group = terminal.screen().build();

    let bar = group.add_bar(100, "test");
    bar.set_position(50);
    bar.set_suffix_components(SuffixComponents {
        elapsed: "1:23:45".into(),
        rate: Some("10/s".into()),
        eta: Some("[0:00:07]".into()),
        ..Default::default()
    });
    ts.advance(Duration::from_millis(100));
    group.tick();

    let content = term.contents();
    assert!(content.contains("1:23:45"), "user elapsed must override auto-derived:\n{content}");
    assert!(content.contains("10/s"), "user rate must override auto-derived:\n{content}");
    assert!(content.contains("[0:00:07]"), "user eta must override auto-derived:\n{content}");
}

#[test]
fn suffix_truncation_order_unchanged_after_merge() {
    // A merged component set (user overrides on top of auto fields) must
    // still truncate in the normative order: custom progressive → eta →
    // rate → elapsed → count/total atomic → fallback. Uses the same width
    // ladder as the semantic_truncate_suffix suite.
    let merged = SuffixComponents {
        count: "9".into(),
        total: "9".into(),
        elapsed: "0:00:05".into(),
        rate: Some("12.3 MiB/s".into()),
        eta: Some("[0:00:02]".into()),
        custom: "cached (1)".into(),
    };
    // Fits: all fields present.
    let fits = super::super::inner::semantic_truncate_suffix(&merged, 100);
    assert_eq!(
        super::components::render_suffix(&fits),
        " \x1b[33m9/9\x1b[0m 0:00:05 12.3 MiB/s [0:00:02] cached (1)",
        "full merged suffix when fits",
    );
    // Custom shrunk to keep=5 (39 visible).
    let partial = super::super::inner::semantic_truncate_suffix(&merged, 39);
    assert_eq!(
        super::components::render_suffix(&partial),
        " \x1b[33m9/9\x1b[0m 0:00:05 12.3 MiB/s [0:00:02] cache",
        "merged custom shrunk first",
    );
    // Eta removed entirely (23 <= 30).
    let eta_off = super::super::inner::semantic_truncate_suffix(&merged, 30);
    assert_eq!(
        super::components::render_suffix(&eta_off),
        " \x1b[33m9/9\x1b[0m 0:00:05 12.3 MiB/s",
        "merged eta removed second",
    );
    // Rate removed entirely (12 <= 22).
    let rate_off = super::super::inner::semantic_truncate_suffix(&merged, 22);
    assert_eq!(
        super::components::render_suffix(&rate_off),
        " \x1b[33m9/9\x1b[0m 0:00:05",
        "merged rate removed third",
    );
    // Count/total still the last atomic unit.
    let count_only = super::super::inner::semantic_truncate_suffix(&merged, 4);
    assert_eq!(
        super::components::render_suffix(&count_only),
        " \x1b[33m9/9\x1b[0m",
        "merged count/total atomic at end",
    );
}

// ---- Phase 4: client-defined truncation contract ----------------------

/// Dummy truncation that ignores the width budget and returns fixed
/// strings. Proves the renderer *calls* the trait at the single push
/// point and uses its output verbatim (the client owns the layout).
struct FixedTruncation {
    prefix: &'static str,
    suffix: &'static str,
}

impl crate::progress::BarLabelTruncation for FixedTruncation {
    fn truncate_prefix(&self, _max_width: usize) -> String {
        self.prefix.to_string()
    }
    fn truncate_suffix(
        &self,
        _max_width: usize,
        _suffix: &crate::progress::SuffixComponents,
    ) -> String {
        self.suffix.to_string()
    }
}

#[test]
fn set_truncation_replaces_builtin_rendering() {
    // When a bar has client truncation installed, the terminal output must
    // contain the client's strings and NOT the built-in component render.
    let term = indicatif::InMemoryTerm::new(10, 80);
    let terminal = ProgressTerminal::builder()
        .with_term_like(Box::new(term.clone()))
        .with_pre_roll_capture(super::pre_roll_capture())
        .capacity(4)
        .with_ticker_enabled(false)
        .build();
    let group = terminal.screen().build();
    let bar = group.add_bar(100, "test");

    bar.set_truncation(Arc::new(FixedTruncation {
        prefix: "CLIENT-PREFIX",
        suffix: "CLIENT-SUFFIX",
    }));
    bar.set_position(20);
    bar.set_total(200);

    group.tick();
    let content = term.contents();
    assert!(content.contains("CLIENT-PREFIX"), "client prefix must appear in output: {content:?}");
    assert!(content.contains("CLIENT-SUFFIX"), "client suffix must appear in output: {content:?}");
    assert!(!content.contains("20/200"), "built-in component render must be bypassed: {content:?}");
}

// ---- Task 4: the width budget handed to client-defined truncation -----

/// A truncation that *honours* its budget, unlike [`FixedTruncation`], and
/// records the budgets it is handed so the renderer's arithmetic can be
/// asserted.
///
/// [`FixedTruncation`] returns fixed strings and ignores the budget, which
/// proves the renderer calls the trait and uses its output verbatim. It
/// cannot show that the budget is *correct*: a renderer that passed any
/// width at all would satisfy it. This one returns a run of one character
/// per column it was granted, so the rendered slot shows the granted width
/// directly and each budget can be read back after a tick.
struct BudgetTruncation {
    seen_prefix: std::sync::Mutex<Option<usize>>,
    seen_suffix: std::sync::Mutex<Option<usize>>,
}

impl BudgetTruncation {
    /// Creates a recorder with no budget observed yet.
    fn new() -> Self {
        Self { seen_prefix: std::sync::Mutex::new(None), seen_suffix: std::sync::Mutex::new(None) }
    }

    /// Returns the prefix budget the renderer granted, or `None` if the
    /// client branch never ran.
    fn prefix_budget(&self) -> Option<usize> {
        *self.seen_prefix.lock().expect("prefix budget poisoned")
    }

    /// Returns the suffix budget the renderer granted, or `None` if the
    /// client branch never ran.
    fn suffix_budget(&self) -> Option<usize> {
        *self.seen_suffix.lock().expect("suffix budget poisoned")
    }
}

impl crate::progress::BarLabelTruncation for BudgetTruncation {
    fn truncate_prefix(&self, max_width: usize) -> String {
        *self.seen_prefix.lock().expect("prefix budget poisoned") = Some(max_width);
        "p".repeat(max_width)
    }

    fn truncate_suffix(
        &self,
        max_width: usize,
        _suffix: &crate::progress::SuffixComponents,
    ) -> String {
        *self.seen_suffix.lock().expect("suffix budget poisoned") = Some(max_width);
        "s".repeat(max_width)
    }
}

/// The ANSI reset the renderer prepends to a client-truncated prefix.
///
/// The tests below use `ANSI_RESET.len()` for the width the renderer
/// reserves for it, so the reserved width stays tied to the escape that
/// actually gets prepended rather than to a hand-written `4`.
const ANSI_RESET: &str = "\x1b[0m";

/// The width the suffix slot settles at for a client-truncated bar.
///
/// The renderer reserves the width of the fully composed suffix — the
/// auto-derived count, elapsed, rate and eta fields included — and clamps it
/// to the suffix ceiling. That composed width clears the ceiling in every
/// bar state exercised here (in progress, finished, and before any position
/// is set), so the ceiling is what a client is granted. Referencing the
/// constant rather than repeating its value means the assertion pins "the
/// ceiling" instead of a snapshot of it, and a change to the ceiling shows
/// up as this test failing rather than silently redefining both.
const SUFFIX_SLOT: usize = MAX_SUFFIX_WIDTH;

/// The prefix components the budget tests use, chosen so the rendered prefix
/// is a distinctive 15 columns wide.
fn budget_test_components() -> PrefixComponents {
    PrefixComponents {
        marker: String::new(),
        tool_name: "tool".into(),
        version: String::new(),
        phase: "stg".into(),
        count: "3".into(),
        total: "10".into(),
    }
}

/// A terminal, one bar, and a [`BudgetTruncation`] that records the budgets
/// the renderer grants it.
///
/// The terminal is held alongside the screen because `screen()` borrows it:
/// dropping the terminal tears the renderer down, and the client branch of
/// the draw never runs.
///
/// `cols` sizes both the captured grid and the injected dimension source, so
/// the frame the test reads and the width the renderer budgets against are the
/// same number. Left on the default the renderer would read the host terminal
/// through [`RealTerminalSource`] and the assertions would hold or fail with
/// the window this test ran in.
struct BudgetFixture {
    /// The in-memory terminal the rendered frame is read back from.
    term: indicatif::InMemoryTerm,
    /// Owns the renderer. Never read; kept alive so draws still happen.
    _terminal: ProgressTerminal,
    /// The screen group, ticked to force a draw.
    group: ProgressScreen,
    /// Records the budgets the renderer granted the client.
    recorder: Arc<BudgetTruncation>,
}

impl BudgetFixture {
    /// Builds the fixture around a single bar carrying `components` on a
    /// terminal `cols` columns wide.
    fn new(components: PrefixComponents, cols: u16) -> Self {
        let term = indicatif::InMemoryTerm::new(10, cols);
        let terminal = ProgressTerminal::builder()
            .with_term_like(Box::new(term.clone()))
            .with_dim_source(Arc::new(TestDimensionSource::new((10, cols))))
            .with_pre_roll_capture(super::pre_roll_capture())
            .capacity(4)
            .with_ticker_enabled(false)
            .build();
        let group = terminal.screen().build();
        let bar = group.add_bar(100, "test");
        bar.set_prefix_components(components);
        let recorder = Arc::new(BudgetTruncation::new());
        bar.set_truncation(Arc::clone(&recorder) as Arc<dyn crate::progress::BarLabelTruncation>);
        Self { term, _terminal: terminal, group, recorder }
    }

    /// Forces one draw and returns the rendered frame.
    fn tick(&self) -> String {
        self.group.tick();
        self.term.contents()
    }
}

/// Terminal width for the budget tests that need the prefix ceiling to bind.
/// [`BudgetTruncation`] claims the whole suffix ceiling, so the prefix only
/// reaches its own ceiling once the line carries that suffix plus the spinner,
/// the separators and [`MIN_BAR_FILL`] as well.
const WIDE_TERMINAL_COLS: u16 = 120;

/// Terminal width where a suffix filling the whole suffix ceiling leaves the
/// prefix only what the spinner and the separators do not claim.
///
/// [`MIN_BAR_FILL`] is not in that list because a suffix this wide takes the
/// line down to a fill the budget cannot grow past its floor, so the frame
/// drops the fill and gives the floor's columns to the label instead.
const NARROW_TERMINAL_COLS: u16 = 80;

/// A client that draws a fixed 13-column label is granted 13 columns, whatever
/// the bar was seeded with and whatever its built-in components measure.
///
/// The bar is seeded `test` and given a 15-column resolve label before the
/// client replaces both, so a slot sized from either of those would not grant
/// the client what it drew. Every screen in this workspace seeds its bars, and
/// a slot sized from a seed can only ever shorten a label.
#[test]
fn client_prefix_budget_follows_what_the_client_drew() {
    let term = indicatif::InMemoryTerm::new(10, 80);
    let terminal = ProgressTerminal::builder()
        .with_term_like(Box::new(term.clone()))
        .with_dim_source(Arc::new(TestDimensionSource::new((10, 80))))
        .with_pre_roll_capture(super::pre_roll_capture())
        .capacity(4)
        .with_ticker_enabled(false)
        .build();
    let group = terminal.screen().build();
    let bar = group.add_bar(100, "test");
    bar.set_prefix_components(budget_test_components());
    bar.set_truncation(Arc::new(FixedTruncation {
        prefix: "CLIENT-PREFIX",
        suffix: "CLIENT-SUFFIX",
    }));
    group.tick();

    let content = term.contents();
    assert!(
        content.contains("CLIENT-PREFIX"),
        "a 13-column client label needs a 13-column slot plus the ANSI reserve: {content:?}",
    );
}

/// A client that fills every budget it is handed reaches the prefix ceiling,
/// and the ceiling less the ANSI reserve is what it ends up granted.
///
/// The reserve is deducted once, at draw time. Deducting it before the ceiling
/// is applied grants 36 columns and this fails, as does not deducting it at
/// all.
#[test]
fn client_prefix_budget_is_capped_by_the_prefix_ceiling() {
    let fixture = BudgetFixture::new(budget_test_components(), WIDE_TERMINAL_COLS);
    fixture.tick();

    assert_eq!(
        fixture.recorder.prefix_budget(),
        Some(MAX_PREFIX_WIDTH - ANSI_RESET.len()),
        "clamped prefix budget must be the ceiling less the ANSI reserve",
    );
}

/// A line too narrow for both labels gives the prefix only what is left after
/// the suffix has taken what it measured.
///
/// This is the case the budget exists for: with the suffix filling its own
/// ceiling, the prefix gets the terminal width less the spinner, the
/// separators and the suffix. A rule that never subtracted the terminal width
/// would hand the client [`MAX_PREFIX_WIDTH`] here and the row would wrap.
///
/// [`MIN_BAR_FILL`] is absent from the subtraction because a suffix that wide
/// leaves the fill at its floor, and a frame whose fill is at its floor draws
/// no bar at all. Those four columns are the label's, which is why the prefix
/// budget here is [`MIN_BAR_FILL`] wider than it would be on a barful frame.
#[test]
fn client_prefix_budget_yields_to_the_suffix_on_a_narrow_line() {
    let expected =
        usize::from(NARROW_TERMINAL_COLS) - FRAME_OVERHEAD_COLUMNS - SUFFIX_SLOT - ANSI_RESET.len();
    let fixture = BudgetFixture::new(budget_test_components(), NARROW_TERMINAL_COLS);
    fixture.tick();

    assert_eq!(
        fixture.recorder.prefix_budget(),
        Some(expected),
        "prefix budget must be what the line has left after the suffix",
    );
}

#[test]
fn client_truncation_receives_full_suffix_width_with_no_ansi_subtraction() {
    // The suffix is granted its slot in full, and unlike the prefix it is
    // granted the slot *unreduced*. The asymmetry is deliberate and is the
    // point of the assertion: the renderer prepends an ANSI reset to the
    // prefix only, so the reserve deducted from the prefix budget has no
    // counterpart here. Were the subtraction applied to the suffix as well,
    // the granted width would be `SUFFIX_SLOT - ANSI_RESET.len()` and this
    // would fail.
    let fixture = BudgetFixture::new(budget_test_components(), WIDE_TERMINAL_COLS);
    fixture.tick();

    assert_eq!(
        fixture.recorder.suffix_budget(),
        Some(SUFFIX_SLOT),
        "client suffix budget must be the full slot, with no ANSI reserve subtracted",
    );
}

#[test]
fn client_truncated_prefix_fills_exactly_its_budget_and_fits_its_slot() {
    // Closes the loop from the granted budget to the rendered frame: the
    // client returns one `p` per column it was granted, so the frame must
    // show exactly the derived prefix width, and the drawn line must not
    // overflow the terminal.
    //
    // The `\x1b[0m` reset the renderer prepends is zero-width, so it adds no
    // visible columns to the slot. The raw escape bytes are not observable
    // here because `InMemoryTerm` strips ANSI from what it reports; that the
    // reset is present is asserted by no test, as recorded in the
    // `inner::renderer` module documentation.
    let expected = MAX_PREFIX_WIDTH - ANSI_RESET.len();
    let fixture = BudgetFixture::new(budget_test_components(), WIDE_TERMINAL_COLS);
    let content = fixture.tick();

    assert_eq!(
        content.matches('p').count(),
        expected,
        "frame must show exactly the granted budget, one p per column: {content:?}",
    );
    for line in content.lines() {
        assert!(
            visible_width(line) <= usize::from(WIDE_TERMINAL_COLS),
            "rendered line overflows the terminal: {line:?}",
        );
    }
}
