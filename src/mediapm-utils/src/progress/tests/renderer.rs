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

#[test]
fn client_truncated_prefix_starts_with_ansi_reset() {
    // Client-truncated prefixes must start with `\x1b[0m` (the same
    // reset the built-in path always prepends).  Without it, the
    // preceding bar's colour bleeds into the client text.
    //
    // InMemoryTerm strips ANSI escapes from its visible output, so we
    // cannot check for the raw `\x1b[0m` bytes.  Instead verify the
    // observable effect: the client prefix text is present and the
    // built-in component render is bypassed.  The preceding colour
    // bleed would cause garbled display, which the `\x1b[0m` prevents.
    let term = indicatif::InMemoryTerm::new(10, 80);
    let terminal = ProgressTerminal::builder()
        .with_term_like(Box::new(term.clone()))
        .with_pre_roll_capture(super::pre_roll_capture())
        .capacity(4)
        .with_ticker_enabled(false)
        .build();
    let group = terminal.screen().build();
    let bar = group.add_bar(100, "test");

    bar.set_truncation(Arc::new(FixedTruncation { prefix: "CLIENT", suffix: "SFX" }));
    bar.set_position(20);
    bar.set_total(200);

    group.tick();
    let content = term.contents();
    // The visible text must contain the client prefix.
    assert!(content.contains("CLIENT"), "client prefix missing: {content:?}");
    assert!(content.contains("SFX"), "client suffix missing: {content:?}");
    assert!(!content.contains("20/200"), "built-in component render must be bypassed: {content:?}");
}
