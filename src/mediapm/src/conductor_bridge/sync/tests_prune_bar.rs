//! The prune bar, the screen it lands on, and the disabled-screen path.

use crate::output::ProgressScreen;
use crate::output::{DimensionSource, ProgressTerminal, TestDimensionSource};
use mediapm_conductor::{NickelDocument, ToolKindSpec, ToolSpec};
use mediapm_utils::progress::PrefixComponents;
use mediapm_utils::progress::recording::{ProgressOp, RecordingProgressTracker};
use std::collections::BTreeMap;
use std::sync::Mutex;

use super::*;

// A sync that has something to prune registers the `[prn]` bar on the screen
// that drives it, after the overall bar.
#[tokio::test]
async fn reconcile_desired_tools_registers_prune_bar_on_the_caller_screen() {
    let tmp = mediapm_utils::temp::artifact_dir().unwrap();
    let cache_root = mediapm_utils::temp::cache_dir().unwrap();
    let paths = MediaPmPaths::from_root(tmp.path());
    // Seed a generated doc holding one entry the rewrite must prune, so the
    // prune bar has a real total.
    let mut content_map = BTreeMap::new();
    content_map.insert("linux/user_script".to_string(), "blake3:manual".to_string());
    let tool_spec = ToolSpec {
        version: None,
        name: "user_script".to_string(),
        kind: ToolKindSpec::default(),
        runtime: ToolRuntime { content_map, ..Default::default() },
        ..Default::default()
    };
    let doc = NickelDocument {
        tools: BTreeMap::from([("user_script@somehash".to_string(), tool_spec)]),
        ..Default::default()
    };
    save_conductor_generated_document(&paths, &doc).expect("pre-save generated doc");

    let tracker = RecordingProgressTracker::new();
    let state = MediaPmState::default();
    let workspace_cas = super::open_workspace_cas_store(&paths).await.expect("open workspace cas");
    let result = reconcile_desired_tools(
        workspace_cas,
        &paths,
        &BTreeMap::new(),
        &BTreeMap::new(),
        RecheckPolicy::default(),
        &state,
        Some(cache_root.path()),
        &tracker,
        None,
    )
    .await;
    assert!(result.is_ok(), "reconcile_desired_tools failed: {:?}", result.err());
    let report = result.unwrap();
    assert!(
        report.pruned_tools >= 1,
        "fixture must prune the seeded manual entry, got {}",
        report.pruned_tools
    );

    let ops = tracker.ops();
    let labels: Vec<String> = ops
        .iter()
        .filter_map(|op| match op {
            ProgressOp::AddBar { label, .. } => Some(label.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        labels,
        vec!["syncing tools".to_string(), "pruning [prn]".to_string()],
        "the sync registers its overall bar, then the prune bar, on the caller's screen"
    );
    let prune_total = ops.iter().find_map(|op| match op {
        ProgressOp::AddBar { total, label } if label == "pruning [prn]" => Some(*total),
        _ => None,
    });
    assert_eq!(
        prune_total,
        Some(1),
        "prune bar total must be the candidate count (the seeded manual entry)"
    );
}

// The `[prn]` prune bar must reach the display: it is created on the screen
// that drives the sync (the caller's, when there is one) and the sync keeps
// that screen live until the prune phase is done. Asserted on a real draw
// target, because "nothing panicked" is exactly the failure mode this test
// exists to catch: the pre-fix code added the bar to an already-committed
// screen, whose finalized renderer ignored it, so the bar never rendered.
//
// Red in both directions: sourcing the bar from the sync's own fallback
// screen creates no bar here at all (the caller owns this screen), and
// joining the sync's own screen before the prune phase panics — see
// `reconcile_desired_tools_records_progress_ops`, which runs that path.
#[tokio::test]
async fn prune_bar_reaches_the_display() {
    let tmp = mediapm_utils::temp::artifact_dir().unwrap();
    let cache_root = mediapm_utils::temp::cache_dir().unwrap();
    let paths = MediaPmPaths::from_root(tmp.path());
    // Seed a generated doc holding one entry the rewrite must prune: it is
    // absent from the desired set, so the prune bar gets a real total.
    let mut content_map = BTreeMap::new();
    content_map.insert("linux/user_script".to_string(), "blake3:manual".to_string());
    let tool_spec = ToolSpec {
        version: None,
        name: "user_script".to_string(),
        kind: ToolKindSpec::default(),
        runtime: ToolRuntime { content_map, ..Default::default() },
        ..Default::default()
    };
    let doc = NickelDocument {
        tools: BTreeMap::from([("user_script@somehash".to_string(), tool_spec)]),
        ..Default::default()
    };
    save_conductor_generated_document(&paths, &doc).expect("pre-save generated doc");

    // The caller-owned screen, drawing into a captured grid.
    let term = indicatif::InMemoryTerm::new(24, 80);
    let target = indicatif::ProgressDrawTarget::term_like(Box::new(term.clone()));
    let dims = Arc::new(TestDimensionSource::new((24, 80)));
    let terminal = ProgressTerminal::builder()
        .with_multi_progress(indicatif::MultiProgress::with_draw_target(target))
        .with_dim_source(dims as Arc<dyn DimensionSource>)
        .with_pre_roll_capture(Box::new(indicatif::InMemoryTerm::new(24, 80)))
        .with_ticker_enabled(false)
        .build();
    let screen = terminal.screen().build();

    let state = MediaPmState::default();
    let workspace_cas = super::open_workspace_cas_store(&paths).await.expect("open workspace cas");
    let result = reconcile_desired_tools(
        workspace_cas,
        &paths,
        &BTreeMap::new(),
        &BTreeMap::new(),
        RecheckPolicy::default(),
        &state,
        Some(cache_root.path()),
        &screen,
        None,
    )
    .await;
    assert!(result.is_ok(), "reconcile_desired_tools failed: {:?}", result.err());
    let report = result.unwrap();
    assert!(
        report.pruned_tools >= 1,
        "fixture must prune the seeded manual entry, got {}",
        report.pruned_tools
    );

    terminal.tick();
    let contents = term.contents();
    let prune_line = contents.lines().find(|line| line.contains("[prn]")).map_or_else(
        || panic!("the `[prn]` bar must reach the display:\n{contents}"),
        str::to_owned,
    );
    // The bar renders `position/total`, so the whole count token is matched
    // rather than a `"/1"` substring: that substring also matches a bar
    // whose total is 100 (`1/100`), which would hide a wrong candidate
    // count. The position is 2 because the sync advances the prune bar once
    // per pruned document entry and once more for the filesystem prune.
    let counts: Vec<&str> =
        prune_line.split_whitespace().filter(|token| token.contains('/')).collect();
    assert_eq!(
        counts,
        vec!["2/1"],
        "the drawn prune bar must carry the candidate count as its total: {prune_line:?}"
    );
}

/// A caller-owned screen with the inert behaviour of
/// [`ProgressScreen::disabled`] that remembers what the sync asked it for.
///
/// A disabled screen draws nothing and hands out no-op handles, so the bars
/// the sync routes through it are invisible from the outside: the recorded
/// `(label, reported total)` pairs are the only way a test can tell "the
/// sync asked for the prune bar and got an inert handle" apart from "the
/// sync never asked for the prune bar at all". The reported total is the
/// one the handle the disabled screen returned carries: `0` for a no-op
/// handle, the requested total for a live screen's handle.
struct RecordingDisabledScreen {
    /// The inert screen the sync is handed; it does the real work.
    inner: ProgressScreen,
    /// One `(label, reported total)` pair per `add_bar` call, in order.
    calls: Mutex<Vec<(String, u64)>>,
}

impl RecordingDisabledScreen {
    /// Create an inert screen whose `add_bar` calls are recorded.
    fn new() -> Self {
        Self { inner: ProgressScreen::disabled(), calls: Mutex::new(Vec::new()) }
    }

    /// The total the handle for `label` reported, or `None` when the sync
    /// never asked this screen for that bar.
    fn reported_total(&self, label: &str) -> Option<u64> {
        self.calls
            .lock()
            .expect("recording lock")
            .iter()
            .find(|(recorded, _)| recorded == label)
            .map(|(_, total)| *total)
    }
}

impl ProgressScreenApi for RecordingDisabledScreen {
    /// Record the request, then hand back the inert handle.
    fn add_bar(&self, total: u64, label: &str) -> Arc<dyn ProgressBarApi> {
        let handle = self.inner.add_bar(total, label);
        let reported = handle.snapshot().total;
        self.calls.lock().expect("recording lock").push((label.to_string(), reported));
        Arc::new(handle)
    }

    /// Log the components under the label they render to, so a
    /// structured bar is looked up the same way a string one is.
    fn add_bar_with_prefix(
        &self,
        total: u64,
        prefix: &PrefixComponents,
    ) -> Arc<dyn ProgressBarApi> {
        self.add_bar(total, &prefix.display_label())
    }

    /// Joining the inert screen is a no-op; delegate so the sync's join
    /// path stays exercised.
    fn join(&self) {
        self.inner.join();
    }
}

// The `--no-progress` path hands the disabled screen down as the caller's
// screen, so the prune bar is added to it. That must stay inert: the sync
// still routes the prune bar through this screen and still does its work,
// while every handle the screen hands out reports no total, so nothing is
// allocated and nothing draws.
//
// Both halves are asserted per label. The `[prn]` half is what separates
// this test from the other prune-bar tests: a prune bar sourced from the
// sync's own fallback screen is never requested here (the caller owns the
// screen), so `reported_total("pruning [prn]")` would be `None`, while a
// disabled screen that began allocating would report the candidate count.
#[tokio::test]
async fn prune_bar_is_inert_on_a_disabled_screen() {
    let tmp = mediapm_utils::temp::artifact_dir().unwrap();
    let cache_root = mediapm_utils::temp::cache_dir().unwrap();
    let paths = MediaPmPaths::from_root(tmp.path());
    let mut content_map = BTreeMap::new();
    content_map.insert("linux/user_script".to_string(), "blake3:manual".to_string());
    let tool_spec = ToolSpec {
        version: None,
        name: "user_script".to_string(),
        kind: ToolKindSpec::default(),
        runtime: ToolRuntime { content_map, ..Default::default() },
        ..Default::default()
    };
    let doc = NickelDocument {
        tools: BTreeMap::from([("user_script@somehash".to_string(), tool_spec)]),
        ..Default::default()
    };
    save_conductor_generated_document(&paths, &doc).expect("pre-save generated doc");

    let screen = RecordingDisabledScreen::new();
    let state = MediaPmState::default();
    let workspace_cas = super::open_workspace_cas_store(&paths).await.expect("open workspace cas");
    let result = reconcile_desired_tools(
        workspace_cas,
        &paths,
        &BTreeMap::new(),
        &BTreeMap::new(),
        RecheckPolicy::default(),
        &state,
        Some(cache_root.path()),
        &screen,
        None,
    )
    .await;
    assert!(result.is_ok(), "a disabled screen must not fail the sync: {:?}", result.err());
    assert!(result.unwrap().pruned_tools >= 1, "the prune must still run with progress disabled");
    assert_eq!(
        screen.reported_total("pruning [prn]"),
        Some(0),
        "the sync must route the prune bar through the caller's screen, and the disabled \
         screen must answer it with a no-op handle"
    );
    assert_eq!(
        screen.reported_total("syncing tools"),
        Some(0),
        "the overall bar must be inert on a disabled screen too"
    );
}
