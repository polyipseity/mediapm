//! One frame of drawing: [`ProgressRenderer::tick`] into
//! [`ProgressRenderer::run_frame`], the debug snapshot the tick emits, and the
//! terminal-state pass a finished slot gets.
//!
//! `run_frame` is the whole sequence in one place: suppress writes, settle the
//! layout, absorb a resize, sync every bound row, emit the debug
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
    apply_failed_bar_style, apply_overall_bar_style,
};
use super::ProgressRenderer;
use super::layout::timing_strings;
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

    /// Execute one frame: suppress writes, recompute layout, handle resize,
    /// sync every slot (rate/ETA), emit the debug snapshot, advance
    /// spinners and draw once, then unsuppress.
    ///
    /// Every write happens while the gate is suppressed except the final
    /// draw, which is triggered by opening the gate (indicatif draws on the
    /// next bar operation while the gate is open).
    ///
    /// The frame also tracks whether steps 2–4 pushed anything: every push
    /// happens while the gate is shut, so a pushed line owes the block a
    /// draw, and on a frame whose bars are all finished the tick loop below
    /// is what would normally pay that debt (any bar's draw repaints every
    /// dirty line of the block). When nothing ticks, the flush at the end of
    /// the loop pays it instead — see the comment there.
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
        let mut pushed = self.recompute_layout();

        // Step 3: Resize handling.
        let resized = self.maybe_adjust_for_resize();
        if resized {
            // A resize re-syncs slots through paths that report their own
            // pushes to no one (height growth rebinds orphaned bars and
            // restyles fresh slots), so count the resize itself: those
            // pushes owe a draw whether or not their dedup comparisons fired.
            // Every frame re-syncs every slot by width anyway, so there is
            // no per-slot dirtying left to do here.
            pushed = true;
        }

        // Step 4: Sync every bound slot to its bar (still suppressed).
        //
        // No slot is skipped. The settled widths and the row's timing strings
        // move with the clock and the terminal, so a slot that mutated is not
        // distinguishable from one that did not, and filtering on it is how a
        // row went stale — an elapsed string that grew from `0s` to `4m18s`
        // between mutations never re-settled the row that carried it. String,
        // position, and length dedup inside `sync_snapshot_to_bar` is what
        // keeps the terminal writes down.
        for (i, slot) in self.slots.iter().enumerate() {
            if let Some(ref source) = *slot.source.borrow() {
                let snap = source.snapshot();

                if source.is_cleared() {
                    // A cleared row is governed by `sync_slot`'s clear
                    // transition: blank on the frame the clear lands, blank
                    // while it stays cleared, and re-shown only by the rebind
                    // that puts a live source in the slot. Pushing content
                    // here would re-show a row the clear emptied and defeat
                    // the one-shot blank.
                    continue;
                }

                // One helper computes the timing for every caller: the strings
                // below are what the measurement pass in `recompute_layout`
                // reserved columns for on this very frame. Unlike the read-only
                // callers, the frame's slot sync owns the EMA advance, so
                // it applies the update for this snapshot before pushing.
                let timing = timing_strings(&snap, &self.slots_timing[i], &*self.time_source);
                if let Some(update) = timing.ema_update {
                    let slot_timing = &mut self.slots_timing[i];
                    slot_timing.rate = update.rate;
                    slot_timing.prev_position = update.position;
                    slot_timing.prev_instant = update.instant;
                }

                pushed |= self.sync_snapshot_to_bar(
                    i,
                    &snap,
                    timing.rate_str.as_deref(),
                    timing.eta_str.as_deref(),
                );
                if snap.status == TrackStatus::Active {
                    // bar.tick() called in the spinner loop below.
                } else if !slot.cache.has_terminal_finished_for(source) {
                    // The terminal transition is applied once per source per
                    // bar. A finished row keeps its done style through
                    // `sync_slot`'s per-frame re-style; what must not repeat
                    // is the abandon/finish and its extra tick, which would
                    // re-draw a row that finished long ago on every frame —
                    // while a source shifted onto a bar that has never
                    // carried its transition applies it there exactly once.
                    // A cleared row never reaches here: `sync_slot` in step 2
                    // blanks it on the clear transition and holds it blank.
                    pushed |= self.finish_slot(i, snap.status);
                    slot.cache.mark_terminal_finished(source);
                }
            }
        }

        // Step 5: Emit the debug snapshot now that rate/ETA are current.
        //
        // This must come after Step 4, not before it: the snapshot reports
        // `slots_timing[i].rate`, which Step 4 recomputes for every slot, so
        // emitting earlier would make `rate_bytes_per_sec` and `eta_secs` one
        // frame stale on terminal screens.
        self.emit_debug_snapshot();

        // Step 6: Advance spinners and draw once.
        // Open the gate — indicatif draws on the next bar operation.
        let _guard = self.gate.open();
        let mut ticked = false;
        for slot in &self.slots {
            if let Some(ref source) = *slot.source.borrow()
                && !source.is_finished()
            {
                slot.bar.tick();
                ticked = true;
            }
        }
        // A frame whose bars are all finished has no tick of its own, but it
        // can still owe the block a draw: a row that finished between frames
        // had its final state pushed while the window was shut, and nothing
        // else will flush that line before [`finalize`](Self::finalize). One
        // bar's draw repaints every dirty line of the block, and re-pushing
        // the first bound slot's prefix is a transmit that force-draws a
        // finished bar without advancing its tick counter (an idle screen
        // must not animate). Ungated configurations have no owed draws —
        // every push already reached the term.
        if !ticked
            && pushed
            && !self.gate.is_noop()
            && let Some(slot) = self.slots.iter().find(|s| s.source.borrow().is_some())
        {
            let prefix = slot.cache.prefix.borrow().clone();
            slot.bar.set_prefix(prefix);
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
    ///
    /// Returns `true` unconditionally: the final `tick` is a real push that
    /// the frame's open window has to draw, so the caller's owed-draw
    /// bookkeeping must count the slot even when every string dedup landed.
    /// Callers apply it once per source per bar — the frame guards it with
    /// [`SlotCache::terminal_finished_for`] (a rebind re-applies it to the
    /// bar the source just landed on), and [`finalize`](Self::finalize)
    /// runs it once at screen teardown.
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
