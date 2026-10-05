//! One frame of drawing: [`ProgressRenderer::tick`] into
//! [`ProgressRenderer::run_frame`], the debug snapshot the tick emits, and the
//! terminal-state pass a finished slot gets.
//!
//! `run_frame` is the whole sequence in one place: suppress writes, settle the
//! layout, absorb a resize, sync the rows that are dirty, emit the debug
//! snapshot, advance the spinners, draw once, unsuppress. Every other entry
//! point either delegates here or is a step of it, so there is one place to
//! read to know what a frame contains and in what order.
//!
//! [`ProgressRenderer::finish_slot`] is the pass that runs outside that
//! sequence, when a row reaches a terminal state and has to stop looking
//! active.

use std::sync::atomic::Ordering;
use std::time::Duration;

use super::super::{
    DebugSlotState, DebugTickSnapshot, PrefixComponents, SuffixComponents, apply_done_bar_style,
    apply_failed_bar_style, apply_overall_bar_style, format_eta, format_rate,
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
    /// [`run_frame`](Self::run_frame) **after** the dirty slots are synced, so
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
                        dirty: slot
                            .source
                            .borrow()
                            .as_ref()
                            .is_some_and(|s| s.dirty.load(Ordering::Acquire)),
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

    /// Execute one frame: suppress writes, recompute layout, handle resize,
    /// sync every dirty slot (rate/ETA), emit the debug snapshot, advance
    /// spinners and draw once, then unsuppress.
    ///
    /// Every write happens while the gate is suppressed except the final
    /// draw, which is triggered by opening the gate (indicatif draws on the
    /// next bar operation while the gate is open).
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

        // Step 1: Suppress writes.
        self.gate.suppress();

        // Step 2: Recompute layout (styles buffered).
        self.recompute_layout();

        // Step 3: Resize handling.
        let resized = self.maybe_adjust_for_resize();
        if resized {
            for slot in &self.slots {
                if let Some(ref source) = *slot.source.borrow() {
                    source.dirty.store(true, Ordering::Release);
                }
            }
        }

        // Step 4: Sync all dirty slots to bars (still suppressed).
        for (i, slot) in self.slots.iter().enumerate() {
            if let Some(ref source) = *slot.source.borrow() {
                let dirty = resized || source.dirty.swap(false, Ordering::AcqRel);
                if !dirty {
                    continue;
                }
                let snap = source.snapshot();

                let rate_str: Option<String> = if snap.status == TrackStatus::Active {
                    if snap.position != self.slots_timing[i].prev_position {
                        let now = self.time_source.now();
                        let dt =
                            now.duration_since(self.slots_timing[i].prev_instant).as_secs_f64();
                        if dt > 0.001 {
                            #[allow(clippy::cast_precision_loss)]
                            let current =
                                (snap.position.saturating_sub(self.slots_timing[i].prev_position))
                                    as f64
                                    / dt;
                            self.slots_timing[i].rate =
                                self.slots_timing[i].rate * 0.9 + current * 0.1;
                            self.slots_timing[i].prev_position = snap.position;
                            self.slots_timing[i].prev_instant = now;
                        }
                    }
                    Some(format_rate(self.slots_timing[i].rate))
                } else {
                    None
                };

                let eta_str = if snap.status == TrackStatus::Active
                    && snap.total > snap.position
                    && self.slots_timing[i].rate > 0.0
                {
                    #[allow(clippy::cast_precision_loss)]
                    let remaining = (snap.total - snap.position) as f64 / self.slots_timing[i].rate;
                    Some(format_eta(remaining))
                } else {
                    None
                };

                self.sync_snapshot_to_bar(i, &snap, rate_str.as_deref(), eta_str.as_deref());
                if snap.status == TrackStatus::Active {
                    // bar.tick() called in the spinner loop below.
                } else if source.is_cleared() {
                    self.blank_bar(i);
                } else {
                    self.finish_slot(i, snap.status);
                }
            }
        }

        // Step 5: Emit the debug snapshot now that rate/ETA are current.
        //
        // This must come after Step 4, not before it: the snapshot reports
        // `slots_timing[i].rate`, which Step 4 recomputes for every dirty
        // slot, so emitting earlier would make `rate_bytes_per_sec` and
        // `eta_secs` one frame stale on terminal screens.
        self.emit_debug_snapshot();

        // Step 6: Advance spinners and draw once.
        // Open the gate — indicatif draws on the next bar operation.
        let _guard = self.gate.open();
        for slot in &self.slots {
            if let Some(ref source) = *slot.source.borrow()
                && !source.is_finished()
            {
                slot.bar.tick();
            }
        }
        // _guard drops → gate re-suppressed.
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
    pub(super) fn finish_slot(&self, i: usize, status: TrackStatus) {
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
    }
}
