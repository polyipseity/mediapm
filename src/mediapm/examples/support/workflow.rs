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
//! Where the label gives way, measured on 2026-10-03 by rendering this screen
//! at every width the harness accepts: 41 columns is the narrowest at which all
//! three rows that name a tool still show that tool in full. One column
//! narrower both worker rows are down to the tail `ffmpeg)`, and by 30 a
//! running worker reads `default s3` and nothing else. The per-step row holds
//! its parenthesised tool name down to 32, so it goes on naming the tool eight
//! columns after the worker rows have stopped. Below the fill crossing the
//! rows give up their timing to keep their label, and a row whose label does
//! not fit either keeps the timing instead, so the active workers read
//! `⠙  22s` on a line too narrow for `default s3 [active]`.
//! Those are facts about the seeds quoted above, not a contract, and they move
//! when a label changes length. Nothing in the suite is named after them,
//! because a fixture called for one of those widths would go on testing that
//! width after the seed it was measured against had moved, and its name would
//! be the only thing left still claiming the number mattered.

use std::sync::Arc;
use std::time::Duration;

use mediapm_conductor::orchestration::progress_labels::{StepBarLabel, WorkerBarLabel};
use mediapm_utils::progress::{BarStyle, ProgressBarHandle};

use crate::support::{ScreenConfig, capture_terminal};

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

/// Render the workflow screen at `config`'s size and return the raw grid.
///
/// The overall bar is labelled `workflow`, as the service builds it, and
/// the child bars are the worker slots the coordinator pre-creates one per
/// pool member. Two slots end in a warning state (`[W]` for a step the
/// coordinator will retry, `[F]` for one it will not), one drops its
/// identifiers and reads `[idle]`, one never receives a step, and one is still
/// running when the transcript is read.
///
/// The last child bar is a per-step bar carrying a version and a progress
/// tally. It is drawn with [`StepBarLabel`], which the coordinator declares and
/// tests but never installs: a real run registers worker-slot bars only, so
/// that row shows a label the renderer supports rather than a frame a live
/// workflow draws. `progress-output.instructions.md` still describes per-step
/// child bars on this screen, and that description is what this row is
/// checking.
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
    // The coordinator registers no per-step bar, so there is no production seed
    // to copy for this one. It takes the worker seed: the slot follows what a
    // bar draws, so a longer seed here would buy nothing.
    let step_bar = screen.add_bar(STEP_OUTPUTS, WORKFLOW_SEED);
    step_bar.set_truncation(Arc::new(StepBarLabel {
        status_marker: String::new(),
        workflow_id: "default".to_string(),
        step_id: "s3".to_string(),
        tool: "ffmpeg".to_string(),
        version: "7.1".to_string(),
        completed: "1".to_string(),
        total: STEP_OUTPUTS.to_string(),
    }));
    step_bar.set_position(1);
    screen.tick();
    clock.advance(Duration::from_secs(3));

    // The second slot takes step `s4`.
    dispatch(&workers[1], "default", "s4", "yt-dlp");
    screen.tick();
    clock.advance(Duration::from_secs(2));

    // The third slot's step fails while the step still allows a retry, so the
    // slot takes the `W` marker.
    dispatch(&workers[2], "default", "s5", "import");
    screen.tick();
    clock.advance(Duration::from_secs(4));
    WorkerSlot::PendingRetry.apply(&workers[2]);
    workers[2].advance(1);
    workers[2].finish_warning();
    overall.advance(1);
    screen.tick();
    clock.advance(Duration::from_secs(1));

    // The fourth slot's step fails with no retry left, so the slot takes `F`.
    dispatch(&workers[3], "default", "s6", "export");
    screen.tick();
    clock.advance(Duration::from_secs(5));
    WorkerSlot::Failed.apply(&workers[3]);
    workers[3].advance(1);
    workers[3].finish_warning();
    overall.advance(1);
    screen.tick();
    clock.advance(Duration::from_secs(1));

    // The fifth slot's step succeeds, and the slot drops its identifiers and
    // goes back to `[idle]`.
    dispatch(&workers[4], "default", "s2", "archive");
    screen.tick();
    clock.advance(Duration::from_secs(6));
    WorkerSlot::Idle.apply(&workers[4]);
    workers[4].advance(1);
    workers[4].finish_success();
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
