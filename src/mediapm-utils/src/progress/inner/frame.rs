//! One frame of drawing: [`ProgressRenderer::tick`] into
//! [`ProgressRenderer::run_frame`], the debug snapshot the tick emits, and the
//! terminal-state pass a finished slot gets.
//!
//! `run_frame` is the whole sequence in one place: absorb a resize, build the
//! frame plan, emit the debug snapshot, write the rendered frame once. Every
//! other entry point either delegates here or is a step of it, so there is one
//! place to read to know what a frame contains and in what order.
//!
//! [`ProgressRenderer::finish_slot`] is the pass that runs outside that
//! sequence, when a row reaches a terminal state and has to stop looking
//! active.

use std::sync::atomic::Ordering;
use std::time::Duration;

use super::super::writer::write_frame;
use super::super::{
    DebugSlotState, DebugTickSnapshot, PrefixComponents, SuffixComponents, apply_done_bar_style,
    apply_failed_bar_style, apply_overall_bar_style,
};
use super::ProgressRenderer;
use super::tracked::{TrackSnapshot, TrackStatus};

impl ProgressRenderer {
    /// Advance all progress bars by one frame.
    ///
    /// Recomputes the uniform alignment width, then redraws every visible
    /// bar from its tracked source state. Delegates entirely to `run_frame`
    /// to guarantee exactly one draw per tick.
    pub fn tick(&mut self) {
        self.run_frame();
    }

    /// Emit a JSONL snapshot of every slot's state to the configured debug sink.
    ///
    /// No-op when no sink is configured.  Called by
    /// [`run_frame`](Self::run_frame) **after** the slots' timing is synced, so
    /// `rate_bytes_per_sec` and `eta_secs` report the frame just computed
    /// rather than the previous one.
    fn emit_debug_snapshot(&self) {
        if let Some(ref sink) = self.debug_sink {
            let bars: Vec<DebugSlotState> = self
                .slots
                .iter()
                .enumerate()
                .map(|(i, slot)| {
                    let (bound, snap) = match slot.source.borrow().as_ref() {
                        Some(s) => (true, s.snapshot()),
                        None => (
                            false,
                            TrackSnapshot {
                                position: 0,
                                total: 0,
                                label: String::new(),
                                prefix: String::new(),
                                prefix_components: PrefixComponents::default(),
                                suffix: String::new(),
                                suffix_components: SuffixComponents::default(),
                                status: TrackStatus::Active,
                                elapsed: Duration::ZERO,
                                animating: false,
                            },
                        ),
                    };
                    let rate = if bound && snap.status == TrackStatus::Active {
                        self.slots_timing[i].rate
                    } else {
                        0.0
                    };
                    #[expect(
                        clippy::cast_precision_loss,
                        reason = "progress ETA math tolerates u64 to f64 precision loss"
                    )]
                    let eta = if bound
                        && snap.status == TrackStatus::Active
                        && snap.total > snap.position
                        && self.slots_timing[i].rate > 0.0
                    {
                        Some((snap.total - snap.position) as f64 / self.slots_timing[i].rate)
                    } else {
                        None
                    };
                    DebugSlotState {
                        slot: i,
                        bound,
                        label: snap.label.clone(),
                        prefix: snap.prefix.clone(),
                        position: snap.position,
                        total: snap.total,
                        status: format!("{:?}", snap.status),
                        elapsed_secs: snap.elapsed.as_secs_f64(),
                        rate_bytes_per_sec: rate,
                        eta_secs: eta,
                        suffix: snap.suffix.clone(),
                    }
                })
                .collect();
            let snapshot = DebugTickSnapshot {
                r#type: "tick".to_string(),
                tick: sink.tick_count.load(Ordering::Relaxed),
                elapsed_secs: self.time_source.now().duration_since(sink.start).as_secs_f64(),
                bars,
            };
            sink.emit(&snapshot);
        }
    }

    /// Execute one frame: absorb a resize, build the plan, emit the debug
    /// snapshot, write the rendered frame once.
    ///
    /// Every row's strings, geometry and spinner policy are decided by
    /// [`build_plan`](Self::build_plan); this method's only terminal contact
    /// is the single `write_frame` pass, which repaints nothing when the
    /// frame equals the last one written (spec AC1). The write goes through
    /// the gate's paired terminal, so a configuration without one (the no-op
    /// gate the test builders hand out) draws nothing here.
    ///
    /// This is the terminal path's single frame entry point; pre-roll is not
    /// part of it (see [`finalize`](Self::finalize)).
    ///
    /// Returns immediately once [`finalize`](Self::finalize) has run: a
    /// finalized screen is committed, and repainting it would both rewrite the
    /// committed frame and re-draw bars the terminal has already released.
    pub(crate) fn run_frame(&mut self) {
        if self.finalized.get() {
            return;
        }
        // Nesting guard: panic in debug builds if called re-entrantly.
        debug_assert!(!self.in_frame.get(), "run_frame called while already in a frame");
        self.in_frame.set(true);

        // Resize and height bookkeeping still own slots and orphans; they no
        // longer restyle, because geometry comes from the plan.
        let resized = self.maybe_adjust_for_resize();

        // One build, one write: everything a row shows is decided here.
        let plan = self.build_plan();
        // After the build, not before: the snapshot reports this frame's
        // `slots_timing` rates, which `build_plan` just advanced.
        self.emit_debug_snapshot();

        let frame = plan.render();
        if let Some(term) = self.gate.term() {
            let previous = self.last_frame.borrow().as_deref().map(<[String]>::to_vec);
            // A resize owed a write on the old path even when every dedup
            // comparison landed (`pushed = true`), because the rebinds and
            // re-synchs a resize triggers report no pushes of their own. The
            // single-writer equivalent is `force`: the writer skips only its
            // equality short-circuit and keeps the cursor back-up, so a resize
            // repaints in place instead of appending a duplicated block.
            let blank_first_frame =
                previous.is_none() && frame.iter().all(|line| line.trim().is_empty());
            if blank_first_frame {
                // A screen that has never drawn content owes no first frame:
                // an all-blank plan is the old no-bars contract
                // (`gated_screen_without_bars_commits_nothing`) and writing it
                // would scroll a real terminal for no pixels. Keep `last`
                // unset — nothing exists on the terminal to back up over.
            } else if write_frame(term, previous.as_deref(), &frame, resized)
                .expect("terminal write")
            {
                let mut last = self.last_frame.borrow_mut();
                *last = Some(frame);
            }
        }
        self.in_frame.set(false);
    }

    /// Apply finish/abandon visual state to a completed slot.
    ///
    /// Sets the correct style for the slot's terminal status, calls
    /// `bar.finish()` or `bar.abandon()`, disables steady tick, and
    /// forces a final render.
    ///
    /// `pub(super)` because the parent's slot bookkeeping calls it when a
    /// bound track reaches a terminal state.
    ///
    /// Returns `true` unconditionally: the final `tick` is a real push.
    /// [`finalize`](Self::finalize) runs it once per terminal slot at screen
    /// teardown; the frame no longer drives bar transitions — a row takes
    /// its terminal style from the plan.
    pub(super) fn finish_slot(&self, i: usize, status: TrackStatus) -> bool {
        let slot = &self.slots[i];
        let prefix_w = self.prefix_w.get();
        let suffix_w = self.suffix_w.get();
        let draw_fill = self.draw_fill.get();
        if self.has_overall && i == self.slots.len() - 1 {
            apply_overall_bar_style(&slot.bar, prefix_w, suffix_w, draw_fill);
        } else if status == TrackStatus::Failed {
            apply_failed_bar_style(&slot.bar, prefix_w, suffix_w, draw_fill);
        } else {
            apply_done_bar_style(&slot.bar, prefix_w, suffix_w, draw_fill);
        }
        match status {
            TrackStatus::Failed | TrackStatus::Warning => slot.bar.abandon(),
            _ => slot.bar.finish(),
        }
        slot.bar.tick();
        true
    }
}
