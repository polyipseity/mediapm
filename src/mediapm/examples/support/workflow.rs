//! Screen B: the conductor workflow progress screen.
//!
//! Renders what `run_workflow` puts on the terminal, using the captured
//! terminal and synthetic clock the shared harness in `support/mod.rs` builds.
//! The example that pulls this module in is `mediapm_progress_workflow`.
//!
//! Every bar here carries the seed `idle`, the string the coordinator
//! installs at
//! `src/mediapm-conductor/src/orchestration/coordinator.rs:348`, and every
//! worker row overrides what it draws with its own [`WorkerBarLabel`]. The
//! renderer sizes the prefix slot from what a bar will draw rather than from
//! the seed it was constructed with (`recompute_layout`), so the seed is four
//! characters and the row reads `default s3 (ffmpeg) [active]`.
//! Nothing here has to pick a longer seed to make room, which is the change
//! from when the seed was the ceiling and an active row read `[active]` and
//! nothing else.
//!
//! Where the labels give way, measured on 2026-10-04 by rendering this screen
//! at every width from 8 to 130 and reading the shape each row's prefix takes.
//!
//! The two labels on this screen give their columns up from opposite ends. A
//! worker row shortens its tool name from the front and keeps the tail; on the
//! baseline screen the bare `ffmpeg` is whole from 29, its parentheses come back
//! at 40, and the row gives up that decoration before it cuts the name at all.
//! The `yt-dlp` row beside it reads `dlp` at 35 and `t-dlp` at 37. Below that
//! the name is gone and the row reads `default s3 [active]` at 31 and 32, and at
//! 30 the activity tag goes as well. The longest of the managed tool names,
//! `media-tagger`, reaches its whole `(media-tagger)` at 46 on the `dense`
//! screen, which is where the worker rows run a whole pool at once. A step row
//! shortens its version from the end and keeps the head, one column at a time:
//! the version reads `v` at 32, `v7` at 33, `v7.` at 34 and `v7.1` whole at 35,
//! by which time the tool name beside it has been whole since 29. The
//! parentheses round those two come back a column later, at 36, because the pair
//! belongs to both fields and neither end of it draws until both are whole.
//!
//! The `states` scenario pushes both labels past the point where they fit. The
//! worker's twenty-eight column tool name is never whole, and reads `flac` at 35
//! and `to-flac` at 38. The step bar's forty-one column version starts shrinking
//! at 31 and stops at `autobuild-2025-10` on width 47, because the prefix slot
//! caps at forty columns whatever the terminal is, and the pair round the tool
//! name never comes back either, since a group with a shortened member draws no
//! brackets. The yield order shows on the other step row: its tool name is the
//! thirty-two column `user-declared-lossless-transcode`, and the version beside it
//! never renders at any width at all, because a trailing segment is dropped whole
//! before the segment ahead of it is shortened.
//!
//! Below the fill crossing the rows give up their timing to keep their label,
//! and a row whose label does not fit either keeps the timing instead, so the
//! active workers read `⠙  22s` on a line too narrow for `default s3 [active]`.
//! Those are facts about the labels quoted above, not a contract, and they move
//! when a label changes length. Nothing in the suite is named after them,
//! because a fixture called for one of those widths would go on testing that
//! width after the label it was measured against had moved, and its name would
//! be the only thing left still claiming the number mattered.
//!
//! # Scenarios
//!
//! [`SCENARIOS`] holds the three shapes this screen is captured in. Each entry
//! declares how many bars its renderer draws, and the harness derives the
//! terminal height from that count, so a scenario and its transcripts cannot
//! drift apart.

use std::sync::Arc;
use std::time::Duration;

use mediapm_conductor::orchestration::progress_labels::{StepBarLabel, WorkerBarLabel};
use mediapm_utils::progress::{BarStyle, ProgressBarHandle};

use crate::scenarios::{Scenario, ScenarioName};
use crate::support::{ScreenConfig, capture_terminal};

/// The workflow screen in each of the three shapes it is captured in.
///
/// `baseline` draws the screen as it shipped. `dense` runs a whole worker pool
/// at once so the band of slots is long. `states` draws tool names and a version
/// wider than any committed transcript, which is the only thing the other two
/// leave out once the baseline has already drawn a warned slot, a failed slot and
/// an idle one.
pub const SCENARIOS: [Scenario; 3] = [
    Scenario::new(ScenarioName::Baseline, 7, render_workflow_screen),
    Scenario::new(ScenarioName::Dense, 9, render_dense),
    Scenario::new(ScenarioName::States, 7, render_states),
];

/// The scenario this screen draws under `name`.
///
/// The table covers every name [`ScenarioName`] has, so the match is total by
/// construction rather than by a fallback that would quietly draw the wrong
/// screen.
#[must_use]
pub fn scenario(name: ScenarioName) -> Scenario {
    match name {
        ScenarioName::Baseline => SCENARIOS[0],
        ScenarioName::Dense => SCENARIOS[1],
        ScenarioName::States => SCENARIOS[2],
    }
}

/// The states a worker-slot bar distinguishes.
///
/// The coordinator builds the same shape from its private `WorkerSlotState` in
/// `worker_slot_label`; this mirrors the four arms so the demo can show each
/// one without reaching into the coordinator.
#[derive(Debug, Clone, PartialEq, Eq)]
enum WorkerSlot {
    /// No step on the slot. The prefix carries the activity marker alone, so
    /// the row reads `[idle]`.
    Idle,
    /// A step is running. This is the only state that fills in the workflow,
    /// step, and tool names.
    Active {
        /// Workflow the step belongs to.
        workflow_id: String,
        /// Step identifier inside the workflow.
        step_id: String,
        /// Conductor tool name the step runs.
        tool: String,
    },
    /// The step failed and the coordinator will retry it, so the marker is
    /// `W`. Identifiers are cleared, exactly as the coordinator clears them.
    PendingRetry,
    /// The step failed with no retry left, so the marker is `F`.
    Failed,
}

impl WorkerSlot {
    /// Apply this state to a slot bar.
    ///
    /// The coordinator repeats the same two calls at every dispatch and at
    /// every step outcome, so they live here rather than at each call site
    /// below. `set_total(1)` records that the slot has one step assigned,
    /// which is the denominator its fill ratio uses.
    fn apply(&self, bar: &ProgressBarHandle) {
        bar.set_truncation(Arc::new(self.label()));
        bar.set_total(1);
    }

    /// Build the label the coordinator would install for this state.
    fn label(&self) -> WorkerBarLabel {
        match self {
            Self::Idle => WorkerBarLabel {
                status_marker: String::new(),
                workflow_id: String::new(),
                step_id: String::new(),
                tool: String::new(),
                activity: "idle".to_string(),
            },
            Self::Active { workflow_id, step_id, tool } => WorkerBarLabel {
                status_marker: String::new(),
                workflow_id: workflow_id.clone(),
                step_id: step_id.clone(),
                tool: tool.clone(),
                activity: "active".to_string(),
            },
            Self::PendingRetry => WorkerBarLabel {
                status_marker: "W".to_string(),
                workflow_id: String::new(),
                step_id: String::new(),
                tool: String::new(),
                activity: "idle".to_string(),
            },
            Self::Failed => WorkerBarLabel {
                status_marker: "F".to_string(),
                workflow_id: String::new(),
                step_id: String::new(),
                tool: String::new(),
                activity: "idle".to_string(),
            },
        }
    }
}

/// Hand a slot a step, the way the coordinator does just before it dispatches.
///
/// `restart` clears the terminal marker and resets the elapsed clock, which is
/// what turns a finished idle slot back into a running one.
fn dispatch(bar: &ProgressBarHandle, workflow_id: &str, step_id: &str, tool: &str) {
    bar.restart();
    WorkerSlot::Active {
        workflow_id: workflow_id.to_string(),
        step_id: step_id.to_string(),
        tool: tool.to_string(),
    }
    .apply(bar);
}

/// What the pinned overall row is naming.
///
/// Mirrors the coordinator's private `OverallBarState` in `overall_bar_label`,
/// so the demo draws the two states the coordinator moves the bar between: a
/// step that is running, and a step that is not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OverallRow<'a> {
    /// No step is running. The row names the workflow alone.
    Idle,
    /// A step is running. This is the only state that fills in the step and
    /// tool names.
    Running {
        /// Step identifier inside the workflow.
        step_id: &'a str,
        /// Conductor tool name the step calls.
        tool: &'a str,
    },
}

impl OverallRow<'_> {
    /// Install the label the coordinator would install for this state.
    ///
    /// `version` is empty here for the same reason it is empty in the
    /// coordinator: no versioned tool field exists to read it from.
    fn apply(self, overall: &ProgressBarHandle, workflow_id: &str) {
        let (step_id, tool) = match self {
            Self::Idle => (String::new(), String::new()),
            Self::Running { step_id, tool } => (step_id.to_string(), tool.to_string()),
        };
        overall.set_truncation(Arc::new(StepBarLabel {
            status_marker: String::new(),
            workflow_id: workflow_id.to_string(),
            step_id,
            tool,
            version: String::new(),
            completed: String::new(),
            total: String::new(),
        }));
    }
}

/// Tell the overall row a step started, the way a dispatch does.
fn dispatch_overall(overall: &ProgressBarHandle, step_id: &str, tool: &str) {
    OverallRow::Running { step_id, tool }.apply(overall, "default");
}

/// Tell the overall row a step stopped, the way each step outcome does.
fn release_overall(overall: &ProgressBarHandle) {
    OverallRow::Idle.apply(overall, "default");
}

/// Render the workflow screen at `config`'s size and return the raw grid.
///
/// The overall bar is pinned at the bottom and labelled through
/// [`StepBarLabel`], so it names the workflow and the step running on it, and
/// falls back to naming the workflow alone whenever no step is running. The
/// child bars are the worker slots the coordinator pre-creates one per pool
/// member. Two slots end in a warning state (`[W]` for a step the
/// coordinator will retry, `[F]` for one it will not), one drops its
/// identifiers and reads `[idle]`, one never receives a step, and one is still
/// running when the transcript is read.
///
/// The last child bar is a per-step bar carrying a version and a progress
/// tally. A real run registers worker-slot bars only, so that row shows a
/// label the renderer supports rather than a frame a live workflow draws.
/// The coordinator now installs the same label on the overall bar, where the
/// tally is empty, so the two rows agree on what the label means.
/// `progress-output.instructions.md` still describes per-step child bars on
/// this screen, and that description is what this row is checking.
///
/// The grid keeps its ANSI colour escapes, the same bytes the terminal received.
/// Pass the result through [`strip_ansi_escapes`](crate::support::strip_ansi_escapes)
/// for a readable transcript.
#[must_use]
pub fn render_workflow_screen(config: ScreenConfig) -> String {
    /// Steps in the synthetic workflow, which is what the overall bar totals.
    const STEP_COUNT: u64 = 12;
    /// Worker slots on screen. The coordinator sizes the pool the same way,
    /// one fixed bar per pool member, so the bar count never changes.
    const POOL_SIZE: usize = 5;
    /// Outputs the per-step bar tracks.
    const STEP_OUTPUTS: u64 = 3;
    /// Seed the coordinator installs on every worker slot, quoted from
    /// `coordinator.rs:348`.
    const WORKFLOW_SEED: &str = "idle";

    let (terminal, grid, clock) = capture_terminal(config);
    let (screen, overall) = terminal.screen().with_overall("workflow", 1).build();
    // The caller pins the overall bar with a placeholder total; the coordinator
    // sets the step count once the workflow is resolved.
    overall.set_total(STEP_COUNT);

    // Every pool member gets a slot up front, idle and already finished, so no
    // spinner animates until a step is dispatched to it.
    let workers: Vec<_> = (0..POOL_SIZE)
        .map(|_| {
            let bar = screen.add_bar(0, WORKFLOW_SEED);
            bar.set_style(BarStyle::WorkerSpinner);
            WorkerSlot::Idle.apply(&bar);
            bar.finish_success();
            bar
        })
        .collect();
    screen.tick();
    clock.advance(Duration::from_secs(1));

    // The first slot takes step `s3`, and the per-step bar opens on it with a
    // tally partway through its three outputs.
    dispatch(&workers[0], "default", "s3", "ffmpeg");
    dispatch_overall(&overall, "s3", "ffmpeg");
    // The coordinator registers no per-step bar, so there is no production seed
    // to copy for this one. It takes the worker seed: the slot follows what a
    // bar draws, so a longer seed here would buy nothing.
    let step_bar = screen.add_bar(STEP_OUTPUTS, WORKFLOW_SEED);
    step_bar.set_truncation(Arc::new(StepBarLabel {
        status_marker: String::new(),
        workflow_id: "default".to_string(),
        step_id: "s3".to_string(),
        tool: "ffmpeg".to_string(),
        version: "v7.1".to_string(),
        completed: "1".to_string(),
        total: STEP_OUTPUTS.to_string(),
    }));
    step_bar.set_position(1);
    screen.tick();
    clock.advance(Duration::from_secs(3));

    // The second slot takes step `s4`.
    dispatch(&workers[1], "default", "s4", "yt-dlp");
    dispatch_overall(&overall, "s4", "yt-dlp");
    screen.tick();
    clock.advance(Duration::from_secs(2));

    // The third slot's step fails while the step still allows a retry, so the
    // slot takes the `W` marker.
    dispatch(&workers[2], "default", "s5", "import");
    dispatch_overall(&overall, "s5", "import");
    screen.tick();
    clock.advance(Duration::from_secs(4));
    WorkerSlot::PendingRetry.apply(&workers[2]);
    workers[2].advance(1);
    workers[2].finish_warning();
    release_overall(&overall);
    overall.advance(1);
    screen.tick();
    clock.advance(Duration::from_secs(1));

    // The fourth slot's step fails with no retry left, so the slot takes `F`.
    dispatch(&workers[3], "default", "s6", "export");
    dispatch_overall(&overall, "s6", "export");
    screen.tick();
    clock.advance(Duration::from_secs(5));
    WorkerSlot::Failed.apply(&workers[3]);
    workers[3].advance(1);
    workers[3].finish_warning();
    release_overall(&overall);
    overall.advance(1);
    screen.tick();
    clock.advance(Duration::from_secs(1));

    // The fifth slot's step succeeds, and the slot drops its identifiers and
    // goes back to `[idle]`.
    dispatch(&workers[4], "default", "s2", "archive");
    dispatch_overall(&overall, "s2", "archive");
    screen.tick();
    clock.advance(Duration::from_secs(6));
    WorkerSlot::Idle.apply(&workers[4]);
    workers[4].advance(1);
    workers[4].finish_success();
    release_overall(&overall);
    overall.advance(1);
    screen.tick();

    // The per-step bar is still running and the overall bar has counted four
    // of the twelve steps. A workflow that lost a step finishes with a warning
    // rather than success.
    step_bar.advance(1);
    overall.set_position(4);
    overall.finish_warning();
    screen.join();
    drop(terminal);
    grid.contents()
}

/// A pool with every slot busy, which is what a wide host looks like.
///
/// The coordinator sizes the pool from the host's available parallelism and lets
/// `MEDIAPM_CONDUCTOR_WORKER_POOL_SIZE` override it
/// (`orchestration/config.rs:40`), so a machine with room for it runs a level's
/// steps side by side and the screen fills with active slots. The baseline leaves
/// two of its five slots running and puts the other three through a warning, a
/// failure and a return to idle; this one has nothing in it but running steps.
///
/// There is no per-step bar in this scenario, and that is a measurement rather
/// than an oversight. A band wide enough to want one stops being reproducible:
/// past a certain number of animating rows at once, one row's spinner glyph comes
/// out a step different depending on how tall the terminal was, which
/// [`Scenario::render_at`](crate::scenarios::Scenario::render_at) refuses as a
/// frame that moves with the height. Measured on 2026-10-03, eight animating rows
/// reproduce at every width from 8 to 500, ten of them fail at width 8 about four
/// runs in ten, and sixteen of them fail from width 8 to width 15 on every run.
/// The draw target throttles redraws against the wall clock and every unfinished
/// bar animates on every frame, so a wide band is the likely cause. The bar count
/// here is the largest this renderer can hold still, not the largest a host could
/// reach.
///
/// The steps carry the six managed tools' bare ids, which is what a conductor
/// step names (`WorkerBarLabel.tool` is `ToolSpec.name`, and a managed tool's
/// generated-document key is its id plus a content hash, while the spec's own
/// name is the bare id). A workflow reaches for the same tool from more than one
/// step, so ffmpeg and yt-dlp each appear twice.
#[must_use]
fn render_dense(config: ScreenConfig) -> String {
    /// Slots the pool holds. A pool this size is a host with the cores to fill
    /// it, which is the case the baseline never draws.
    const POOL_SIZE: usize = 8;
    /// Steps in the workflow, which is what the overall bar totals. The level
    /// below runs eight of them, so the overall bar is part-way through the whole
    /// workflow rather than through the level.
    const STEP_COUNT: u64 = 24;
    /// Seed the coordinator installs on every worker slot, quoted from
    /// `coordinator.rs:348`.
    const WORKFLOW_SEED: &str = "idle";
    /// The step each slot runs and the tool it runs.
    const LEVEL_STEPS: [(&str, &str); POOL_SIZE] = [
        ("s1", "ffmpeg"),
        ("s2", "yt-dlp"),
        ("s3", "ffmpeg"),
        ("s4", "deno"),
        ("s5", "rsgain"),
        ("s6", "yt-dlp"),
        ("s7", "sd"),
        ("s8", "media-tagger"),
    ];

    let (terminal, grid, clock) = capture_terminal(config);
    let (screen, overall) = terminal.screen().with_overall("workflow", 1).build();
    overall.set_total(STEP_COUNT);

    let workers: Vec<_> = (0..POOL_SIZE)
        .map(|_| {
            let bar = screen.add_bar(0, WORKFLOW_SEED);
            bar.set_style(BarStyle::WorkerSpinner);
            WorkerSlot::Idle.apply(&bar);
            bar.finish_success();
            bar
        })
        .collect();
    screen.tick();
    clock.advance(Duration::from_secs(1));

    // Each slot takes its step and stays on it. The frame is caught mid-level,
    // so no slot has gone back to idle and no slot has failed.
    for (index, (step_id, tool)) in LEVEL_STEPS.iter().enumerate() {
        dispatch(&workers[index], "default", step_id, tool);
        dispatch_overall(&overall, step_id, tool);
        overall.advance(1);
        screen.tick();
        clock.advance(Duration::from_secs(1 + index as u64));
    }
    overall.finish_success();
    screen.join();
    drop(terminal);
    grid.contents()
}

/// The labels wider than any committed transcript.
///
/// Four slots and two per-step bars. The baseline has already drawn a warned
/// slot, a failed slot and an idle one, so this scenario spends its width on the
/// two ways a label gives up columns, which is what the shorter seeds never
/// reach.
///
/// The tool names come from the workflow document: `WorkerBarLabel.tool` is the
/// step's own `ToolSpec.name`, which for a managed tool is the bare id and for a
/// tool the user declared is whatever the user spelled. Both names here are
/// hyphenated, which is what makes the front clip bite rather than discard the
/// segment: a tail with no boundary in it names nothing, so it is dropped whole.
///
/// The step rows are split the other way round. One carries ffmpeg's composite
/// human-readable version, forty-one columns wide, which is wider than the prefix
/// slot's own ceiling of forty, so the head-keeping shrink runs at every width the
/// harness accepts and the version never renders whole, which also keeps the
/// parentheses round the tool name off the row. The other carries sd's `v1.10.0`
/// beside a thirty-two column tool name, and the version never renders there
/// either: it trails the tool name, so it is dropped whole before the name ahead of
/// it is shortened, and what is left on the row is the front clip working on the
/// name.
#[must_use]
fn render_states(config: ScreenConfig) -> String {
    /// Slots the pool holds.
    const POOL_SIZE: usize = 4;
    /// Steps in the workflow, which is what the overall bar totals.
    const STEP_COUNT: u64 = 9;
    /// Outputs the per-step bar tracking the wide version tracks.
    const WIDE_VERSION_OUTPUTS: u64 = 5;
    /// Outputs the per-step bar tracking the wide tool name tracks.
    const WIDE_TOOL_OUTPUTS: u64 = 3;
    /// Seed the coordinator installs on every worker slot, quoted from
    /// `coordinator.rs:348`.
    const WORKFLOW_SEED: &str = "idle";
    /// ffmpeg's `human_readable_version` is its `BtbN` autobuild tag and the
    /// evermeet version as one string (`tools/provider/mod.rs:391`).
    const FFMPEG_VERSION: &str = "autobuild-2025-10-03-12-00+evermeet-8.1.2";
    /// sd's reported version.
    const SD_VERSION: &str = "v1.10.0";

    let (terminal, grid, clock) = capture_terminal(config);
    let (screen, overall) = terminal.screen().with_overall("workflow", 1).build();
    overall.set_total(STEP_COUNT);

    let workers: Vec<_> = (0..POOL_SIZE)
        .map(|_| {
            let bar = screen.add_bar(0, WORKFLOW_SEED);
            bar.set_style(BarStyle::WorkerSpinner);
            WorkerSlot::Idle.apply(&bar);
            bar.finish_success();
            bar
        })
        .collect();
    screen.tick();
    clock.advance(Duration::from_secs(1));

    // The first slot runs a step whose tool name is wider than the prefix slot's
    // ceiling, so its parenthesised name shortens from the front at every width.
    dispatch(&workers[0], "default", "s7", "transcode-lossless-to-flac");
    dispatch_overall(&overall, "s7", "transcode-lossless-to-flac");
    screen.tick();
    clock.advance(Duration::from_secs(3));

    // The second slot's step failed and the coordinator will retry it.
    dispatch(&workers[1], "default", "s8", "sd");
    dispatch_overall(&overall, "s8", "sd");
    screen.tick();
    clock.advance(Duration::from_secs(2));
    WorkerSlot::PendingRetry.apply(&workers[1]);
    workers[1].advance(1);
    workers[1].finish_warning();
    release_overall(&overall);
    overall.advance(1);
    screen.tick();
    clock.advance(Duration::from_secs(1));

    // The third slot's step failed with no retry left.
    dispatch(&workers[2], "default", "s9", "import");
    dispatch_overall(&overall, "s9", "import");
    screen.tick();
    clock.advance(Duration::from_secs(2));
    WorkerSlot::Failed.apply(&workers[2]);
    workers[2].advance(1);
    workers[2].finish_warning();
    release_overall(&overall);
    overall.advance(1);
    screen.tick();
    clock.advance(Duration::from_secs(1));

    // The fourth slot never receives a step at all, which is a different idle row
    // from one whose step finished: neither carries a marker, and this one has
    // no counters to have advanced.
    screen.tick();

    // The wide version and the wide tool name are on separate steps so each row
    // shows one shrink biting rather than both at once.
    let wide_version = screen.add_bar(WIDE_VERSION_OUTPUTS, WORKFLOW_SEED);
    wide_version.set_truncation(Arc::new(StepBarLabel {
        status_marker: String::new(),
        workflow_id: "default".to_string(),
        step_id: "s7".to_string(),
        tool: "ffmpeg".to_string(),
        version: FFMPEG_VERSION.to_string(),
        completed: "2".to_string(),
        total: WIDE_VERSION_OUTPUTS.to_string(),
    }));
    wide_version.set_position(2);

    let wide_tool = screen.add_bar(WIDE_TOOL_OUTPUTS, WORKFLOW_SEED);
    wide_tool.set_truncation(Arc::new(StepBarLabel {
        status_marker: String::new(),
        workflow_id: "default".to_string(),
        step_id: "s8".to_string(),
        tool: "user-declared-lossless-transcode".to_string(),
        version: SD_VERSION.to_string(),
        completed: "1".to_string(),
        total: WIDE_TOOL_OUTPUTS.to_string(),
    }));
    wide_tool.set_position(1);
    screen.tick();
    clock.advance(Duration::from_secs(4));

    overall.set_position(2);
    overall.finish_warning();
    screen.join();
    drop(terminal);
    grid.contents()
}
