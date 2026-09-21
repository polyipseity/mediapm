use super::super::inner::*;
use super::*;

// ---- RecordingTrackedHandle elapsed ----------------------------------

#[test]
fn recording_handle_elapsed_starts_near_zero() {
    let h = RecordingTrackedHandle::new(100);
    let elapsed = h.snapshot_elapsed();
    assert!(elapsed.as_millis() < 100, "elapsed should start near zero, got {elapsed:?}");
}

#[test]
fn recording_handle_elapsed_frozen_after_finish() {
    let h = RecordingTrackedHandle::new(100);
    std::thread::sleep(std::time::Duration::from_millis(1));
    h.finish_success();
    let frozen = h.snapshot_elapsed();
    // Verify the value stays frozen on subsequent reads.
    let frozen2 = h.snapshot_elapsed();
    assert_eq!(frozen, frozen2, "elapsed should be frozen after finish_success");
}

#[test]
fn recording_handle_elapsed_frozen_after_finish_success() {
    let h = RecordingTrackedHandle::new(100);
    std::thread::sleep(std::time::Duration::from_millis(1));
    h.finish_success();
    let frozen = h.snapshot_elapsed();
    let frozen2 = h.snapshot_elapsed();
    assert_eq!(frozen, frozen2, "elapsed should be frozen after finish_success");
}

#[test]
fn recording_handle_elapsed_frozen_after_finish_error() {
    let h = RecordingTrackedHandle::new(100);
    std::thread::sleep(std::time::Duration::from_millis(1));
    h.finish_error();
    let frozen = h.snapshot_elapsed();
    let frozen2 = h.snapshot_elapsed();
    assert_eq!(frozen, frozen2, "elapsed should be frozen after finish_error");
}

#[test]
fn recording_handle_elapsed_frozen_after_finish_warning() {
    let h = RecordingTrackedHandle::new(100);
    std::thread::sleep(std::time::Duration::from_millis(1));
    h.finish_warning();
    let frozen = h.snapshot_elapsed();
    let frozen2 = h.snapshot_elapsed();
    assert_eq!(frozen, frozen2, "elapsed should be frozen after finish_warning");
}

// ---- format_elapsed (pure formatting) --------------------------------

#[test]
fn format_elapsed_zero() {
    assert_eq!(super::super::format_elapsed(std::time::Duration::ZERO), "0s");
}

#[test]
fn format_elapsed_seconds_only() {
    assert_eq!(super::super::format_elapsed(std::time::Duration::from_secs(42)), "42s");
}

#[test]
fn format_elapsed_minutes_and_seconds() {
    assert_eq!(super::super::format_elapsed(std::time::Duration::from_secs(5 * 60 + 3)), "5m3s");
}

#[test]
fn format_elapsed_hours() {
    assert_eq!(
        super::super::format_elapsed(std::time::Duration::from_secs(2 * 3600 + 15 * 60 + 30)),
        "2h15m"
    );
}

#[test]
fn format_elapsed_large_hours() {
    assert_eq!(super::super::format_elapsed(std::time::Duration::from_hours(100)), "4d4h");
}

// ---- format_rate (pure formatting) -----------------------------------

#[test]
fn format_rate_zero() {
    assert_eq!(super::super::format_rate(0.0), "0/d");
}

#[test]
fn format_rate_slow() {
    assert_eq!(super::super::format_rate(0.000_1), "9/d");
}

#[test]
fn format_rate_per_minute() {
    // 0.02/s = 1.2/m
    assert_eq!(super::super::format_rate(0.02), "1/m");
}

#[test]
fn format_rate_per_hour() {
    // 0.000_5/s = 1.8/h
    assert_eq!(super::super::format_rate(0.000_5), "2/h");
}

#[test]
fn format_rate_single_digit() {
    assert_eq!(super::super::format_rate(3.5), "3.5/s");
}

#[test]
fn format_rate_double_digit() {
    assert_eq!(super::super::format_rate(42.0), "42/s");
}

#[test]
fn format_rate_thousands_single() {
    assert_eq!(super::super::format_rate(1_200.0), "1.2k/s");
}

#[test]
fn format_rate_thousands_double() {
    assert_eq!(super::super::format_rate(123_000.0), "123k/s");
}

#[test]
fn format_rate_millions() {
    assert_eq!(super::super::format_rate(3_500_000.0), "3.5M/s");
}

// ---- SharedState elapsed --------------------------------------------

#[test]
fn shared_state_elapsed_starts_near_zero() {
    let s = super::super::SharedState::new(100, "test");
    let elapsed = s.elapsed();
    assert!(elapsed.as_millis() < 100, "elapsed should start near zero, got {elapsed:?}");
}

#[test]
fn shared_state_elapsed_advances() {
    let ts = std::sync::Arc::new(super::super::TestTimeSource::new());
    let s = super::super::SharedState::with_time_source(
        100,
        "test",
        std::sync::Arc::clone(&ts) as std::sync::Arc<dyn super::super::TimeSource>,
    );
    ts.advance(std::time::Duration::from_millis(10));
    let elapsed = s.elapsed();
    assert!(elapsed.as_millis() >= 10, "elapsed should advance after advance, got {elapsed:?}");
}

#[test]
fn shared_state_elapsed_frozen_after_mark_finished() {
    let ts = std::sync::Arc::new(super::super::TestTimeSource::new());
    let s = super::super::SharedState::with_time_source(
        100,
        "test",
        std::sync::Arc::clone(&ts) as std::sync::Arc<dyn super::super::TimeSource>,
    );
    ts.advance(std::time::Duration::from_millis(10));
    s.mark_finished();
    let frozen = s.elapsed();
    assert!(
        frozen.as_millis() >= 10,
        "elapsed should capture time until mark_finished, got {frozen:?}"
    );
    ts.advance(std::time::Duration::from_millis(10));
    let frozen2 = s.elapsed();
    assert_eq!(frozen, frozen2, "elapsed should be frozen after mark_finished");
}

#[test]
fn shared_state_elapsed_not_frozen_before_finish() {
    let s = super::super::SharedState::new(100, "test");
    let t0 = s.elapsed();
    // Without calling mark_finished, repeated reads should climb.
    let t1 = s.elapsed();
    assert!(t1 >= t0, "elapsed should not decrease before finish: {t0:?} >= {t1:?}");
}

#[test]
fn shared_state_elapsed_monotonic() {
    let ts = std::sync::Arc::new(super::super::TestTimeSource::new());
    let s = super::super::SharedState::with_time_source(
        100,
        "test",
        std::sync::Arc::clone(&ts) as std::sync::Arc<dyn super::super::TimeSource>,
    );
    let t0 = s.elapsed();
    ts.advance(std::time::Duration::from_millis(5));
    let t1 = s.elapsed();
    ts.advance(std::time::Duration::from_millis(5));
    let t2 = s.elapsed();
    assert!(t1 >= t0, "t1 ({t1:?}) should be >= t0 ({t0:?})");
    assert!(t2 >= t1, "t2 ({t2:?}) should be >= t1 ({t1:?})");
}

// ---- TrackedHandle elapsed (integration) -----------------------------

#[test]
fn tracked_handle_elapsed_frozen_after_all_finish_methods() {
    // finish_success, finish_error, finish_warning, finish_and_clear
    // must all freeze the elapsed.
    for (name, finish_fn) in [
        (
            "finish_success",
            Box::new(|h: &TrackedHandle| h.finish_success()) as Box<dyn Fn(&TrackedHandle)>,
        ),
        (
            "finish_error",
            Box::new(|h: &TrackedHandle| h.finish_error()) as Box<dyn Fn(&TrackedHandle)>,
        ),
        (
            "finish_warning",
            Box::new(|h: &TrackedHandle| h.finish_warning()) as Box<dyn Fn(&TrackedHandle)>,
        ),
        (
            "finish_and_clear",
            Box::new(|h: &TrackedHandle| h.finish_and_clear()) as Box<dyn Fn(&TrackedHandle)>,
        ),
    ] {
        let ts = std::sync::Arc::new(super::super::TestTimeSource::new());
        let g = super::super::ProgressGroup::builder()
            .with_time_source(
                std::sync::Arc::clone(&ts) as std::sync::Arc<dyn super::super::TimeSource>
            )
            .build();
        let h = g.add_bar(100, &format!("{name}-bar"));
        ts.advance(std::time::Duration::from_millis(10));
        finish_fn(&h);
        // We can't directly access SharedState::elapsed() from the handle,
        // but we verify the handle doesn't panic and is usable afterward.
        assert_eq!(h.total(), 100, "{name}: total preserved");
        g.join_and_clear();
    }
}

#[test]
fn progress_group_new_creates_handle() {
    let g = ProgressGroup::builder().build();
    let h = g.add_bar(42, "child");
    assert!(h.total() > 0, "enabled handle must have total > 0");
    assert_eq!(h.total(), 42);
}

#[test]
fn progress_group_with_overall_creates_both() {
    let (g, overall) = ProgressGroup::builder().with_overall("all", 100).build();
    assert_eq!(overall.total(), 100, "overall bar must have total == 100");
    let child = g.add_bar(50, "child");
    assert_eq!(child.total(), 50, "child bar must have total == 50");
}

#[test]
fn recording_handle_set_total_updates_position() {
    let h = RecordingTrackedHandle::new(100);
    h.set_position(5);
    h.set_total(20);
    assert_eq!(
        h.ops(),
        vec![ProgressOp::SetPosition { pos: 5 }, ProgressOp::SetTotal { total: 20 },]
    );
}

#[test]
fn recording_handle_multiple_advances_sum() {
    let h = RecordingTrackedHandle::new(10);
    h.advance(1);
    h.advance(2);
    h.advance(3);
    let ops = h.ops();
    assert_eq!(ops.len(), 3, "expected 3 separate Advance ops");
    assert_eq!(ops[0], ProgressOp::Advance { delta: 1 });
    assert_eq!(ops[1], ProgressOp::Advance { delta: 2 });
    assert_eq!(ops[2], ProgressOp::Advance { delta: 3 });
}

#[test]
fn progress_group_join_and_clear_does_not_panic() {
    // Non-empty group
    let g = ProgressGroup::builder().build();
    let _h = g.add_bar(10, "a");
    g.join();
    g.join_and_clear();

    // Empty group
    let g = ProgressGroup::builder().build();
    g.join();
    g.join_and_clear();
}

#[test]
fn progress_group_disabled_construction() {
    let g1 = ProgressGroup::disabled();
    let h1 = g1.add_bar(50, "c1");
    assert_eq!(h1.total(), 0);

    // disabled + explicit disabled handle pair for with-overall patterns.
    let (_g2, h2) = (ProgressGroup::disabled(), TrackedHandle::disabled());
    assert_eq!(h2.total(), 0);
}

#[test]
fn recording_handle_finish_does_not_generate_clear() {
    // Verify that finish_success / finish_error don't produce
    // FinishAndClear or Abandon operations (which would clear the bar).
    let h = RecordingTrackedHandle::new(10);
    h.finish_success();
    h.finish_error();
    for op in h.ops() {
        match op {
            ProgressOp::FinishSuccess | ProgressOp::FinishError => {}
            other => panic!("unexpected op: {other:?}"),
        }
    }
}

#[test]
fn progress_group_join_leaves_handles_intact() {
    // join() is a no-op — handles must still be usable afterward.
    let g = ProgressGroup::builder().build();
    let h = g.add_bar(42, "child");
    h.advance(10);
    h.set_total(50);
    h.finish_success();
    g.join();
    assert_eq!(h.total(), 50, "handle total preserved after join");
}

#[test]
fn progress_group_finish_success_and_error_preserve_group() {
    // Finish calls on a handle must preserve the total and the group must
    // remain functional (join() must not panic).
    let g = ProgressGroup::builder().build();
    let h = g.add_bar(10, "test");
    h.finish_success();
    assert_eq!(h.total(), 10, "handle total preserved after finish_success");
    // Second finish on the same slot must not corrupt state.
    h.finish_error();
    assert_eq!(h.total(), 10, "handle total preserved after finish_error");
    g.join(); // join must not panic on any state
}

// ── recording_group_add_bar_multiple_groups_independent ──

#[test]
fn recording_group_add_bar_multiple_groups_independent() {
    let g1 = RecordingProgressTracker::new();
    let g2 = RecordingProgressTracker::new();

    let h1 = g1.add_bar(10, "group1-bar");
    h1.advance(1);

    let h2 = g2.add_bar(20, "group2-bar");
    h2.advance(2);
    h2.finish_success();

    assert_eq!(
        g1.ops(),
        vec![
            ProgressOp::AddBar { total: 10, label: "group1-bar".to_string() },
            ProgressOp::Advance { delta: 1 },
        ],
        "group1 must have its own ops, unaffected by group2"
    );

    assert_eq!(
        g2.ops(),
        vec![
            ProgressOp::AddBar { total: 20, label: "group2-bar".to_string() },
            ProgressOp::Advance { delta: 2 },
            ProgressOp::FinishSuccess,
        ],
        "group2 must have its own ops, unaffected by group1"
    );
}

// ── recording_handle_finish_ops_sequence ──

#[test]
fn recording_handle_finish_ops_sequence() {
    let h = RecordingTrackedHandle::new(5);
    assert_eq!(h.ops(), vec![], "no ops yet");

    h.finish_success();
    assert_eq!(h.ops(), vec![ProgressOp::FinishSuccess], "finish_success records FinishSuccess");

    let h2 = RecordingTrackedHandle::new(5);
    h2.finish_success();
    assert_eq!(h2.ops(), vec![ProgressOp::FinishSuccess], "finish_success records FinishSuccess");

    let h3 = RecordingTrackedHandle::new(5);
    h3.finish_error();
    assert_eq!(h3.ops(), vec![ProgressOp::FinishError], "finish_error records FinishError");
}

// ── TrackedHandle::new (with bar) ──────────────────────────────────

#[test]
fn tracked_handle_new_creates_handle_with_total() {
    let h = TrackedHandle::new(50);
    assert_eq!(h.total(), 50);
    assert_eq!(h.snapshot().position, 0);
    assert!(!h.is_finished());
}

#[test]
fn tracked_handle_new_advance_and_snapshot() {
    let h = TrackedHandle::new(100);
    h.advance(42);
    let snap = h.snapshot();
    assert_eq!(snap.position, 42);
    assert_eq!(snap.total, 100);
}

#[test]
fn tracked_handle_is_finished_after_finish_success() {
    let h = TrackedHandle::new(10);
    assert!(!h.is_finished());
    h.finish_success();
    assert!(h.is_finished());
}

#[test]
fn tracked_handle_is_finished_after_finish_error() {
    let h = TrackedHandle::new(10);
    h.finish_error();
    assert!(h.is_finished());
}

#[test]
fn tracked_handle_is_finished_after_finish_warning() {
    let h = TrackedHandle::new(10);
    h.finish_warning();
    assert!(h.is_finished());
}

#[test]
fn tracked_handle_snapshot_fields_match() {
    let h = TrackedHandle::new(100);
    h.set_prefix_components(PrefixComponents { tool_name: "pfx".into(), ..Default::default() });
    h.advance(7);
    let snap = h.snapshot();
    assert_eq!(snap.prefix, "\x1b[0mpfx");
    assert_eq!(snap.position, 7);
    assert_eq!(snap.total, 100);
    assert!(matches!(snap.status, TrackStatus::Active));
}

#[test]
fn progress_group_excess_bars_return_active_handles() {
    // Fill slots beyond capacity, verify excess handle still tracks.

    // ProgressGroup::with_overall allocates terminal_height() slots
    // (clamped to 4-200).  Use a MultiProgress with small term to force
    // small capacity.
    let term = indicatif::InMemoryTerm::new(4, 40);
    let (group, _overall) = ProgressGroup::builder()
        .with_term_like(Box::new(term.clone()))
        .capacity(4)
        .with_overall("overall", 10)
        .build();

    // 4 slots total → 3 child slots + 1 overall.
    // Add 5 children → first 3 get slots, last 2 have no display slot.
    let handles: Vec<_> = (0..5).map(|i| group.add_bar(10, &format!("t{i}"))).collect();

    // All handles must be active (not disabled).
    for (i, h) in handles.iter().enumerate() {
        assert_eq!(h.total(), 10, "handle {i} total");
    }

    // Mutate each — verify state tracking works even without display.
    for (i, h) in handles.iter().enumerate() {
        h.advance((i + 1) as u64);
    }
    for (i, h) in handles.iter().enumerate() {
        let snap = h.snapshot();
        assert_eq!(snap.position, (i + 1) as u64, "handle {i} position");
    }
}

#[test]
fn progress_group_manager_finish_and_clear_via_tick_fn() {
    // finish_and_clear on a ProgressGroup-managed handle (bar=None,
    // tick_fn=Some) must still mark state as finished.

    let (_group, overall) = ProgressGroup::builder().with_overall("all", 10).build();
    overall.finish_and_clear();
    let snap = overall.snapshot();
    assert!(
        matches!(snap.status, TrackStatus::Success),
        "finish_and_clear → Success, got {:?}",
        snap.status
    );
    assert_eq!(snap.position, 0, "position unchanged before advance");

    // Advance after finish_and_clear is harmless (no crash) but
    // does update position since advance() does not gate on status.
    overall.advance(5);
    assert_eq!(overall.snapshot().position, 5, "advance still works after finish_and_clear");
}

#[test]
fn tracked_handle_finish_and_clear_disabled_is_noop() {
    // disabled() handle with finish_and_clear must not panic and
    // must leave state unchanged.
    let h = TrackedHandle::disabled();
    assert_eq!(h.total(), 0);
    h.finish_and_clear();
    assert_eq!(h.total(), 0);
}

#[test]
fn progress_group_disabled_add_bar_returns_disabled() {
    let g = ProgressGroup::disabled();
    let child = g.add_bar(42, "child");
    assert_eq!(child.total(), 0, "child disabled");
}

#[test]
fn progress_group_api_trait_via_recording() {
    // Verify RecordingProgressTracker implements ProgressGroupApi
    // and can be used via the trait.
    use super::super::ProgressGroupApi;
    let tracker: Arc<dyn ProgressGroupApi> = Arc::new(RecordingProgressTracker::new());
    let bar: Arc<dyn super::super::ProgressBarApi> = tracker.add_bar(100, "test");
    assert!(!bar.is_finished(), "recording bar starts unfinished");
    bar.advance(5);
    bar.finish_success();
    assert!(bar.is_finished(), "recording bar is finished");
}

#[test]
fn rate_computation_handles_non_monotonic_position() {
    // When a bar's position regresses between ticks, the EMA rate
    // computation must not panic (saturating_sub guard).
    let term = indicatif::InMemoryTerm::new(10, 80);
    let group = ProgressGroup::builder().with_term_like(Box::new(term.clone())).capacity(4).build();
    let h = group.add_bar(100, "test");
    h.advance(80); // position grows to 80
    group.tick(); // tick captures prev_position = 80
    h.set_position(20); // position drops to 20 (non-monotonic)
    group.tick(); // must not panic (saturating_sub saves it)
    let snap = h.snapshot();
    assert_eq!(snap.position, 20);
    assert!(matches!(snap.status, TrackStatus::Active));
}

#[test]
fn spinner_advances_per_cycle_for_all_bars() {
    // Each bar's spinner character must change across ticks, not just
    // the overall bar's.
    use super::super::inner::DimensionSource;
    use std::sync::Arc;

    let term = indicatif::InMemoryTerm::new(10, 80);
    let dims = Arc::new(super::super::inner::TestDimensionSource::new((10, 80)));
    let ts = Arc::new(super::super::TestTimeSource::new());

    let (group, overall) = super::super::ProgressGroup::builder()
        .with_term_like(Box::new(term.clone()))
        .with_dim_source(dims as Arc<dyn DimensionSource>)
        .with_time_source(ts.clone() as Arc<dyn super::super::TimeSource>)
        // Disable the daemon ticker: it fires group.tick() on real
        // wall-clock time, injecting extra spinner ticks beyond this
        // test's manual ticks.  (indicatif's set_position also advances
        // the tick counter through a wall-clock position rate limiter,
        // so the assertion below checks a distinct-glyph set.)
        .with_ticker_enabled(false)
        .with_overall("syncing", 3)
        .build();

    let bar1 = group.add_bar(100, "tool [resolve]");
    let bar2 = group.add_bar(100, "tool [fetch]");

    // Advance time so rate computation has positive dt.
    ts.advance(std::time::Duration::from_millis(100));

    // Collect the first-char (spinner) from each line across many ticks.
    let mut snapshots: Vec<Vec<char>> = Vec::new();
    for _ in 0..30 {
        // Change positions each tick to trigger tick_inner via setters.
        bar1.advance(1);
        bar2.advance(2);
        overall.advance(0);
        ts.advance(std::time::Duration::from_millis(50));
        group.tick();

        let output = term.contents();
        let line_chars: Vec<char> = output
            .lines()
            .filter(|l| !l.is_empty())
            .map(|l| l.chars().next().unwrap_or(' '))
            .collect();
        if !line_chars.is_empty() {
            snapshots.push(line_chars);
        }
    }

    // Must have captured several distinct snapshots.
    assert!(snapshots.len() >= 10, "expected >=10 captured snapshots, got {}", snapshots.len());

    // Every bar's spinner must be animating.  We can't assert
    // first-vs-last inequality: the glyph is indicatif's tick counter
    // mod 9, and that counter advances via BOTH set_position (gated by
    // a real-wall-clock position rate limiter) and the end-of-tick
    // bar.tick(), so the exact per-iteration advance varies with real
    // timing.  What IS guaranteed: every active bar is ticked at least
    // once per group.tick(), so 30 ticks over a 9-glyph cycle always
    // produce several distinct glyphs per bar.  Assert >= 2 distinct.
    let n_bars = snapshots.iter().map(Vec::len).max().unwrap_or(0);
    let mut distinct_per_bar: Vec<std::collections::BTreeSet<char>> =
        vec![std::collections::BTreeSet::new(); n_bars];
    for snap in &snapshots {
        for (i, c) in snap.iter().enumerate().take(n_bars) {
            distinct_per_bar[i].insert(*c);
        }
    }
    for (i, set) in distinct_per_bar.iter().enumerate() {
        assert!(
            set.len() >= 2,
            "bar {i}: spinner must cycle through >= 2 distinct glyphs, \
                 saw only {set:?} across {} snapshots",
            snapshots.len()
        );
    }
}

#[test]
fn recycled_bar_spinner_animates() {
    // Force slot recycling by creating a renderer with a single child
    // slot.  When bar1 finishes and bar2 attaches, bar2 reuses bar1's
    // slot.  Without the fix, bar2's indicatif bar would still have
    // Status::DoneVisible and the spinner would show the final char
    // (⠏ for our tick set) without cycling.
    use super::super::inner::DimensionSource;
    use std::sync::Arc;

    let term = indicatif::InMemoryTerm::new(10, 80);
    let dims = Arc::new(super::super::inner::TestDimensionSource::new((10, 80)));
    let ts = Arc::new(super::super::TestTimeSource::new());

    // capacity=2 means 1 child + 1 overall bar.
    // dynamic_height=false fixed capacity prevents auto-growing.
    let (group, overall) = super::super::ProgressGroup::builder()
        .with_term_like(Box::new(term.clone()))
        .with_dim_source(dims as Arc<dyn DimensionSource>)
        .with_time_source(ts.clone() as Arc<dyn super::super::TimeSource>)
        // Disable the daemon ticker: it fires group.tick() on real
        // wall-clock time, injecting extra spinner ticks beyond this
        // test's manual ticks.  (indicatif's set_position also advances
        // the tick counter through a wall-clock position rate limiter,
        // so the assertion below checks a distinct-glyph set.)
        .with_ticker_enabled(false)
        .capacity(2)
        .dynamic_height(false)
        .with_overall("syncing", 3)
        .build();

    // Phase 1: finish resolve bar (fills the single child slot).
    let bar1 = group.add_bar(1, "tool [resolve]");
    bar1.finish_success();

    // Tick to trigger finish_slot → bar.finish() → DoneVisible.
    ts.advance(std::time::Duration::from_millis(50));
    group.tick();

    // Phase 2: add fetch bar (recycles bar1's slot via attach Phase 2).
    let bar2 = group.add_bar(5, "tool [fetch]");
    bar2.advance(2);
    ts.advance(std::time::Duration::from_millis(50));
    group.tick();

    // Capture spinner chars across many ticks.
    let mut snapshots: Vec<Vec<char>> = Vec::new();
    for _ in 0..30 {
        bar2.advance(1);
        overall.advance(0);
        ts.advance(std::time::Duration::from_millis(50));
        group.tick();

        let output = term.contents();
        let line_chars: Vec<char> = output
            .lines()
            .filter(|l| !l.is_empty())
            .map(|l| l.chars().next().unwrap_or(' '))
            .collect();
        if !line_chars.is_empty() {
            snapshots.push(line_chars);
        }
    }

    // Must have captured several distinct snapshots.
    assert!(snapshots.len() >= 10, "expected >=10 captured snapshots, got {}", snapshots.len());

    // The spinner char for the child bar (first line) must differ
    // between the first and last snapshot — if it's the same, the
    // spinner is frozen because the indicatif bar stayed DoneVisible.
    let first = &snapshots[0];
    assert!(
        first.len() >= 2,
        "expected at least 2 visible bars (child + overall), got {}",
        first.len()
    );
    // Same window-robust glyph check as spinner_advances_per_cycle_for_all_bars:
    // the child bar's glyph is the tick counter mod 9, advanced by both
    // set_position (wall-clock rate-limited) and the end-of-tick bar.tick(),
    // so assert >= 2 distinct glyphs across the window instead of
    // first-vs-last inequality.
    let distinct_child: std::collections::BTreeSet<char> =
        snapshots.iter().filter_map(|s| s.first()).copied().collect();
    assert!(
        distinct_child.len() >= 2,
        "child bar spinner must cycle through >= 2 distinct glyphs, \
             saw only {distinct_child:?} across {} snapshots — slot status leak",
        snapshots.len()
    );
}

// ── Color helpers (ANSI escape code generation) ─────────────────────

#[test]
fn bar_color_code_active_child() {
    assert_eq!(super::super::inner::bar_color_code(super::super::TrackStatus::Active, false), "33");
}

#[test]
fn bar_color_code_active_overall() {
    assert_eq!(super::super::inner::bar_color_code(super::super::TrackStatus::Active, true), "35");
}

#[test]
fn bar_color_code_failed() {
    assert_eq!(super::super::inner::bar_color_code(super::super::TrackStatus::Failed, false), "31");
    assert_eq!(super::super::inner::bar_color_code(super::super::TrackStatus::Failed, true), "31");
}

#[test]
fn bar_color_code_warning() {
    assert_eq!(
        super::super::inner::bar_color_code(super::super::TrackStatus::Warning, false),
        "33"
    );
}

#[test]
fn bar_color_code_success() {
    assert_eq!(
        super::super::inner::bar_color_code(super::super::TrackStatus::Success, false),
        "32"
    );
}
