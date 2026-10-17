//! The column budget: how many columns the prefix and the suffix get, and the
//! single sync point that spends them on a slot's cache.
//!
//! A frame answers two questions in order. [`ProgressRenderer::settle_layout`]
//! measures every row the frame would draw and settles one prefix width and one
//! suffix width for all of them, so rows align across the screen;
//! [`ProgressRenderer::sync_snapshot_to_slot`] then records one row into the
//! widths that pass settled. The free helpers below are the row-shaped halves
//! of those two steps: they compose and measure a single suffix, they own the
//! ANSI overhead the prefix slot has to reserve before a label is measured,
//! and they own [`timing_strings`], the one rule set that turns a slot's
//! timing state into the rate and eta segments every caller measures and draws
//! with.
//!
//! Both steps read tracked state through a [`TrackSnapshot`] rather than
//! through the `SharedState` behind it, so a layout pass cannot advance
//! anything and two passes over unchanged state produce identical widths.

use std::sync::Arc;
use std::time::Instant;

use super::super::{
    FRAME_OVERHEAD_COLUMNS, MIN_BAR_FILL, SuffixComponents, TimeSource, bar_color_code,
    close_sgr_state, format_count, format_elapsed, format_eta, format_rate, max_prefix_width,
    max_suffix_width, render_prefix_components, render_suffix_components, semantic_truncate_prefix,
    semantic_truncate_suffix, visible_width,
};
use crate::progress::BarStyle;

use super::tracked::{TrackSnapshot, TrackStatus};
use super::{ProgressRenderer, SlotTiming};

/// Rate divisor used for a row whose EMA has not started.
///
/// A row attached but not yet progressed has no measured rate, yet its slot
/// still has to reserve the columns an eta will occupy the moment it does.
/// One unit per second is that placeholder: it is what [`timing_strings`]
/// divides the remaining work by until a real rate exists, and it is the same
/// floor the measurement used before the helper existed, so the reservation a
/// fresh bar gets does not move.
const NOMINAL_ETA_RATE_FLOOR: f64 = 1.0;

/// A slot's rate and eta suffix segments, plus the EMA update that produced them.
///
/// Returned by [`timing_strings`]. Every path that measures or draws a row's
/// suffix goes through that function, so a row cannot be measured with one
/// timing string and drawn with another — the equality this task exists to
/// protect.
pub(super) struct SlotTimingStrings {
    /// Rate segment (e.g. `2.9k/s`); `None` once the row is terminal.
    pub(super) rate_str: Option<String>,
    /// ETA segment (e.g. `1m35s`); `None` once the row is terminal or done.
    pub(super) eta_str: Option<String>,
    /// The EMA commit the strings were computed from; see [`EmaUpdate`].
    pub(super) ema_update: Option<EmaUpdate>,
}

/// The EMA advance that produced a [`SlotTimingStrings::rate_str`].
///
/// Only the frame's slot sync applies it, and it applies it for the very
/// snapshot the strings were computed from, so a caller that drops it — the
/// measurement pass and `sync_slot`, both of which are read-only — still reads
/// back the rate it was shown on that frame. The frame syncs every slot on
/// every frame, so the frame that observes a moved position is also the frame
/// that commits it.
pub(super) struct EmaUpdate {
    /// Smoothed rate to store for the next frame.
    pub(super) rate: f64,
    /// Position the smoothing window now ends at.
    pub(super) position: u64,
    /// Instant that smoothing window ends at.
    pub(super) instant: Instant,
}

/// Compute one slot's rate and eta suffix segments — the single rule set.
///
/// `settle_layout` (measurement), `sync_slot`, the frame's slot sync
/// and `build_plan` all call this instead of computing the pair themselves,
/// which is what makes the columns the budget reserves equal the columns the
/// row renders. The rules, in order:
///
/// - A terminal row gets `(None, None)` — no rate, no eta.
/// - A live row's rate is the EMA, **prospectively** updated when the snapshot's
///   position has moved past the last committed one: the update is simulated
///   here and returned as [`SlotTimingStrings::ema_update`] so a read-only
///   caller (measurement, `sync_slot`) sees the value the draw will commit
///   rather than the stale one it would otherwise render.
/// - The rate renders through [`format_rate`], which already prints the
///   zero-rate placeholder `0/d`, so no caller needs its own fallback.
/// - The eta is present exactly when the row is live and still has work left,
///   divided by that same prospective rate — falling back to
///   [`NOMINAL_ETA_RATE_FLOOR`] while the EMA has not started, so a just-attached
///   bar reserves the eta width it will draw on its first progressed frame.
///
/// `time_source` is read only when an update is actually simulated, so two
/// calls over unchanged state return identical strings.
pub(super) fn timing_strings(
    snap: &TrackSnapshot,
    timing: &SlotTiming,
    time_source: &dyn TimeSource,
) -> SlotTimingStrings {
    let active = snap.status == TrackStatus::Active;
    let mut rate = timing.rate;
    let mut ema_update = None;
    if active && snap.position != timing.prev_position {
        let now = time_source.now();
        let dt = now.duration_since(timing.prev_instant).as_secs_f64();
        if dt > 0.001 {
            #[expect(
                clippy::cast_precision_loss,
                reason = "progress ETA math tolerates u64 to f64 precision loss"
            )]
            let current = (snap.position.saturating_sub(timing.prev_position)) as f64 / dt;
            rate = rate * 0.9 + current * 0.1;
            ema_update = Some(EmaUpdate { rate, position: snap.position, instant: now });
        }
    }
    let rate_str = if active { Some(format_rate(rate)) } else { None };
    let eta_str = if active && snap.total > snap.position {
        let divisor = if rate > 0.0 { rate } else { NOMINAL_ETA_RATE_FLOOR };
        #[expect(
            clippy::cast_precision_loss,
            reason = "progress ETA math tolerates u64 to f64 precision loss"
        )]
        let remaining = (snap.total - snap.position) as f64 / divisor;
        Some(format_eta(remaining))
    } else {
        None
    };
    SlotTimingStrings { rate_str, eta_str, ema_update }
}

/// Compute the ANSI byte overhead for the prefix template field.
///
/// The prefix field counts ANSI escape bytes as visible characters.
/// Client-truncated bars add only a 4-byte reset (`\x1b[0m`); built-in
/// rendering adds 13 bytes for failed/warning status markers
/// (`\x1b[0m\x1b[3Xm\x1b[0m`). This helper centralizes the calculation so
/// both `sync_snapshot_to_slot` and `settle_layout` use the same value.
///
/// `pub(super)` because the layout test sibling measures a prefix width against
/// this value through the parent module.
pub(super) fn compute_ansi_overhead(status: TrackStatus, has_client_truncation: bool) -> usize {
    if has_client_truncation {
        4
    } else {
        match status {
            TrackStatus::Failed | TrackStatus::Warning => 13,
            _ => 4,
        }
    }
}

/// Wrap a client-truncated label in the reset it is drawn under.
///
/// A prefix is drawn into whatever state the field before it left the terminal
/// in, so a row that starts coloured colours everything after it as well, and
/// the bar fill sits directly after the prefix. The reset hands the label a
/// default start, which is what lets the fill take its colour from the bar's
/// own status instead of from the label.
///
/// The four bytes it adds are the overhead [`compute_ansi_overhead`] reserves
/// out of the prefix slot, which is why `width` is what is left after that
/// subtraction rather than the whole slot.
///
/// The client's own label is closed through [`close_sgr_state`] before it is
/// prefixed: a label that embeds an unbalanced colour cannot leak it into the
/// bar fill that follows. Production labels are escape-free, so their rendered
/// bytes are unchanged.
///
/// `pub(super)` because the client-truncation test sibling pins both halves of
/// the wrapping through the parent module.
pub(super) fn client_truncated_prefix(
    truncation: &Arc<dyn crate::progress::BarLabelTruncation>,
    width: usize,
) -> String {
    format!("\x1b[0m{}", close_sgr_state(truncation.truncate_prefix(width)))
}

/// Compose the full suffix component set from snapshot data and timing.
///
/// `keep_timing` decides whether the row draws its timing columns. See
/// [`keeps_timing`] for why a row that has dropped its fill sometimes keeps
/// them anyway. The strip itself lives in [`without_timing`], so the draw path
/// and the measurement pass in [`ProgressRenderer::settle_layout`] cannot
/// disagree about what a row without its timing would hold.
fn compose_suffix(
    snap: &TrackSnapshot,
    rate_str: Option<&str>,
    eta_str: Option<&str>,
    keep_timing: bool,
) -> SuffixComponents {
    let auto_suffix = SuffixComponents {
        count: format_count(snap.position),
        total: format_count(snap.total),
        elapsed: format_elapsed(snap.elapsed),
        rate: rate_str.map(str::to_owned),
        eta: eta_str.map(str::to_owned),
        custom: String::new(),
    };
    let merged = SuffixComponents::merge(&auto_suffix, &snap.suffix_components);
    if keep_timing { merged } else { without_timing(&merged) }
}

/// Drop the timing columns from a merged suffix, keeping the tally and any
/// caller-set text.
///
/// `elapsed`, `rate` and `eta` answer "how long", and they are the first thing
/// a frame that has given up its fill gives up too, because the columns they
/// want are the ones the label and the tally want. `count`/`total` answer "how
/// far along" and `custom` carries the caller's own status words, so both stay.
fn without_timing(suffix: &SuffixComponents) -> SuffixComponents {
    SuffixComponents { elapsed: String::new(), rate: None, eta: None, ..suffix.clone() }
}

/// Whether a row draws its timing columns on a frame that has dropped its fill.
///
/// A frame whose fill is at [`MIN_BAR_FILL`] draws no bar at all, so its
/// columns go to the label and the tally, and the timing is what gives way
/// first. That trade is only worth making while the row still says something
/// without it, so the condition is about the row rather than about the frame:
/// `prefix` and `timingless_suffix` are what this row would draw with the
/// timing stripped, at the widths [`ProgressRenderer::settle_layout`]
/// settled.
///
/// A row with nothing left keeps its timing instead. A worker slot on the
/// workflow screen has no tally of its own and a label that does not fit a
/// narrow line, so a stripped worker row renders `⠙` and nothing else, which
/// is a worse frame than the four-cell bar the fill would have drawn. Reading
/// this the other way round looks wrong until the bare-spinner case is on
/// screen; the timing is what is left, not what is spent first.
///
/// A frame that draws its fill is never bare, since four cells of `░░░░` are
/// the row's own report of how far along it is.
fn keeps_timing(draw_fill: bool, prefix: &str, timingless_suffix: &str) -> bool {
    draw_fill || (visible_width(prefix) == 0 && visible_width(timingless_suffix) == 0)
}

/// Render the suffix a slot draws into a `suffix_w`-column slot.
///
/// A client label decides which of its fields render, so it is asked; a bar
/// with no client label truncates the built-in components and renders those.
/// `sync_snapshot_to_slot` records through this and
/// [`ProgressRenderer::settle_layout`] measures through it, so what the
/// budget reserves and what the row shows are the same string.
///
/// Both branches close their result through [`close_sgr_state`]: a suffix that
/// contains an escape ends in a reset, so a row can never end in an open
/// colour state. The reset is zero visible columns, so the measured width
/// stays equal to the drawn width.
fn render_slot_suffix(
    suffix: &SuffixComponents,
    truncation: Option<&Arc<dyn crate::progress::BarLabelTruncation>>,
    suffix_w: usize,
    color_code: &str,
) -> String {
    match truncation {
        Some(t) => close_sgr_state(t.truncate_suffix(suffix_w, suffix)),
        None => close_sgr_state(render_suffix_components(
            &semantic_truncate_suffix(suffix, suffix_w),
            color_code,
        )),
    }
}

/// Measure the columns a merged suffix occupies once truncated to `ceiling`.
///
/// The measurement is what the budget reserves, so it goes through the same
/// [`render_slot_suffix`] the draw path uses.
fn measure_suffix_width(
    ceiling: usize,
    suffix: &SuffixComponents,
    truncation: Option<&Arc<dyn crate::progress::BarLabelTruncation>>,
) -> usize {
    visible_width(render_slot_suffix(suffix, truncation, ceiling, "").as_str())
}

/// The strings and numbers one slot's row shows on one frame.
///
/// Produced by [`ProgressRenderer::slot_row_strings`]; both consumers draw
/// from it — the cache sync in [`sync_snapshot_to_slot`](ProgressRenderer::sync_snapshot_to_slot)
/// and the frame's `PlanRow`s in the renderer's `build_plan` — so a plan's
/// rows and the slot cache cannot answer differently about the same snapshot.
pub(super) struct SlotRowStrings {
    /// Prefix, truncated and wrapped exactly as the bar's field drew it.
    pub(super) prefix: String,
    /// Message (suffix), rendered into the settled suffix slot.
    pub(super) message: String,
    /// Position after the `WorkerSpinner` zero-total guard.
    pub(super) position: u64,
    /// Total after the `WorkerSpinner` zero-total guard.
    pub(super) total: u64,
}

impl ProgressRenderer {
    /// Settle the uniform `prefix_w`/`suffix_w`/`draw_fill` every row of this
    /// frame draws at.
    ///
    /// Measures the rendered prefix/suffix width of each bound slot (via
    /// [`SharedState::snapshot`](super::tracked::SharedState::snapshot)), takes the max across all bound slots, and
    /// spends the terminal's columns on them: what is left after the spinner,
    /// the separators and [`MIN_BAR_FILL`] is split between the two, the
    /// suffix taking what it measured first and the prefix the remainder.
    ///
    /// The result is stored in the `prefix_w`/`suffix_w`/`draw_fill` cells,
    /// and every consumer — the slot cache sync
    /// ([`sync_snapshot_to_slot`](Self::sync_snapshot_to_slot)) and the frame's plan
    /// ([`build_plan`](super::ProgressRenderer::build_plan)) — reads the same
    /// cells, so all rows share the same alignment width — short labels no
    /// longer waste space and long labels no longer overflow the bar.
    ///
    /// This is the budget and nothing else: it says how many columns a label
    /// may use, not which of its fields survive. That is
    /// [`semantic_truncate_prefix`] for the built-in components and the client's
    /// own [`BarLabelTruncation`](crate::progress::BarLabelTruncation) for a
    /// client label, and each ranks its own fields. Moving a ranking in here is
    /// tempting and wrong: it puts one cut in two places, and the two disagree
    /// the first time a screen gains a field.
    ///
    /// # The fill threshold
    ///
    /// A fill at [`MIN_BAR_FILL`] is worth nothing: four cells of `░░░░` show a
    /// quarter-resolution fraction the count beside it already states. So this
    /// function asks what the fill would actually be, and when the answer is
    /// the floor it spends those columns on the label and the count instead and
    /// draws the frame from the no-fill template.
    ///
    /// The question is asked against the budget that still reserves the floor,
    /// so it cannot answer differently next frame and oscillate: reserving the
    /// floor is what pins the fill at the floor. Only once the answer says no
    /// fill does the line drop the reservation, which frees four columns for
    /// the label.
    ///
    /// What that costs a screen is its own label widths, so the width at which
    /// the fill comes back differs per screen rather than being one number the
    /// renderer could hard-code. Measured over the three example screens at
    /// every width from 8 to 120, the fill sits at the floor at every width up
    /// to 58 on tool sync and materialization and up to 65 on the workflow
    /// screen, whose labels are seven columns wider.
    ///
    /// # The budget a row that kept its timing is drawn into
    ///
    /// A row with nothing left on it keeps its timing (see [`keeps_timing`]),
    /// and that timing is rendered into the suffix slot this function settled,
    /// truncated to that slot, rather than widening it. Reserving it here
    /// would let one row's clock take columns from the next row's label,
    /// which is the failure this budget exists to prevent, and it would make
    /// the width depend on which rows happen to be bare this frame.
    ///
    /// The timing a bare row keeps is therefore not measured here, so nothing
    /// this pass settles depends on which rows are bare this frame. The draw
    /// path reads the same two cells, asks [`keeps_timing`] the same question
    /// against them, and strips the timing through the same [`without_timing`]
    /// the measurement below uses, so the budget and the message cannot answer
    /// differently about the same row.
    ///
    /// Pure measurement: nothing is pushed and nothing is written, so
    /// [`build_plan`](super::ProgressRenderer::build_plan) can settle and
    /// then build this frame's rows without a second measurement pass.
    pub(super) fn settle_layout(&self, cols: u16) {
        let prefix_ceiling = max_prefix_width(cols);
        let suffix_ceiling = max_suffix_width(cols);
        let mut max_prefix = 0usize;
        let mut max_suffix = 0usize;
        // The composed suffix with the frame's timing stripped, and the
        // client's truncation, for every bound slot in slot order. Filled on
        // the first pass so the fill-threshold second pass can measure it
        // without re-snapshotting.
        let mut measured: Vec<(
            SuffixComponents,
            Option<Arc<dyn crate::progress::BarLabelTruncation>>,
        )> = Vec::with_capacity(self.slots.len());
        for (i, slot) in self.slots.iter().enumerate() {
            if let Some(ref source) = *slot.source.borrow() {
                let snap = source.snapshot();
                let truncation =
                    source.truncation.read().expect("shared_state truncation lock").clone();
                let has_client_truncation = truncation.is_some();
                let status_overhead = compute_ansi_overhead(snap.status, has_client_truncation);
                // Measure what this bar will actually draw.  A bar with a
                // client label draws that label, so measuring the seed it was
                // constructed with would cap the slot at the placeholder text
                // every screen seeds its slots with, and a client label can
                // only ever be shortened.  Ask the client for its own output
                // at the ceiling, exactly as the suffix is measured below.
                let prefix_width = if let Some(ref t) = truncation {
                    let rendered =
                        t.truncate_prefix(prefix_ceiling.saturating_sub(status_overhead));
                    visible_width(rendered.as_str()) + status_overhead
                } else {
                    visible_width(snap.prefix.as_str()) + status_overhead
                };
                max_prefix = max_prefix.max(prefix_width);
                // Measure the full rendered suffix (auto fields + custom),
                // not just the stored custom text — the rendered RHS also
                // carries count/total/elapsed/rate/eta which consume the
                // width budget. The timing comes from `timing_strings`, the
                // same helper the draw path calls, so the columns reserved
                // here are the columns the row renders. The EMA commit it
                // returns is deliberately dropped: this pass is read-only, and
                // the frame's slot sync applies the same update for the
                // same snapshot.
                let timing = timing_strings(&snap, &self.slots_timing[i], &*self.time_source);
                // Measure the MERGED suffix (auto fields + user-set
                // overrides), not just the auto-derived fields.  A wider
                // user-set `rate`/`eta`/`custom` must widen `suffix_w` or
                // it would overflow at draw and get truncated away.
                //
                // On a frame that keeps its fill, that is the whole suffix.
                // On a frame that drops the fill, what the budget reserves is
                // the suffix with the timing stripped, because that is the
                // narrower of the two and a row that keeps its timing renders
                // it into the slot this reserves. The timingless suffix is
                // kept so the no-fill pass below can measure it without
                // timing on a frame that turns out to draw no fill, which
                // saves re-snapshotting every slot.
                let suffix_with_timing = compose_suffix(
                    &snap,
                    timing.rate_str.as_deref(),
                    timing.eta_str.as_deref(),
                    true,
                );
                let suffix_without_timing = compose_suffix(
                    &snap,
                    timing.rate_str.as_deref(),
                    timing.eta_str.as_deref(),
                    false,
                );
                measured.push((suffix_without_timing, truncation.clone()));
                let suffix_width =
                    measure_suffix_width(suffix_ceiling, &suffix_with_timing, truncation.as_ref());
                max_suffix = max_suffix.max(suffix_width);
            }
        }
        // What is left of the line once the spinner, the separators and a
        // floor under the fill are paid for. Both label fields come out of it.
        // The suffix is settled first because it is the field that must not
        // wrap: a suffix past the end of the line spills onto the row below,
        // where it reads as a second bar.
        //
        // Settling a width is pure arithmetic over the two measured maxima, so
        // the fill-threshold second pass below reuses it rather than repeating
        // the suffix and prefix caps by hand.
        // The prefix cap carries the terminal term on purpose: without it the
        // slot is the widest seed label on screen, and a bar seeded with a short
        // placeholder never grows past it however much room the line has.
        let settle = |label_columns: usize, max_suffix: usize| {
            let suffix_w = max_suffix.min(label_columns);
            let prefix_w = max_prefix.min(prefix_ceiling).min(label_columns - suffix_w);
            (prefix_w, suffix_w)
        };
        let reserved_columns =
            usize::from(cols).saturating_sub(FRAME_OVERHEAD_COLUMNS + MIN_BAR_FILL);
        let (prefix_w, suffix_w) = settle(reserved_columns, max_suffix);
        // A fill at the floor is four cells of `░░░░` or `████`, which says
        // nothing the count beside it does not. Once the labels take the rest
        // of the line the fill is pinned there at every narrower width, so this
        // is the widest terminal at which the bar has still stopped growing.
        // This subtraction is deliberately local: it is a threshold probe for
        // the `draw_fill` decision below. `build_plan` re-derives the same
        // arithmetic for the rows' render width (see the `fill_w` comment
        // there) — one is the boolean, the other the columns, and neither
        // function needs the other's result.
        let fill = usize::from(cols).saturating_sub(FRAME_OVERHEAD_COLUMNS + prefix_w + suffix_w);
        let draw_fill = fill > MIN_BAR_FILL;
        let (prefix_w, suffix_w) = if draw_fill {
            (prefix_w, suffix_w)
        } else {
            // The bar is gone, so the floor it was holding is not: give those
            // columns to the label, and re-measure the suffix without the
            // timing the frame no longer draws, or the slot it reserves would
            // be wider than the message that fills it. The measurement uses
            // the same `compose_suffix` the draw path composes with, so the
            // two are the same string rather than two rules that agree today.
            max_suffix = 0;
            for (suffix, truncation) in &measured {
                max_suffix = max_suffix.max(measure_suffix_width(
                    suffix_ceiling,
                    suffix,
                    truncation.as_ref(),
                ));
            }
            settle(usize::from(cols).saturating_sub(FRAME_OVERHEAD_COLUMNS), max_suffix)
        };
        self.prefix_w.set(prefix_w);
        self.suffix_w.set(suffix_w);
        self.draw_fill.set(draw_fill);
    }

    /// Compute slot `i`'s row strings for one snapshot — the single producer
    /// of what a row shows this frame.
    ///
    /// A pure read of the settled cells ([`Self::settle_layout`] must have
    /// run): the prefix through the client's
    /// [`BarLabelTruncation`](crate::progress::BarLabelTruncation) when
    /// installed or the built-in truncate-then-render pair otherwise, the
    /// message through [`render_slot_suffix`] with the [`keeps_timing`]
    /// decision, and the position/total after the `WorkerSpinner` zero-total
    /// guard.
    pub(super) fn slot_row_strings(
        &self,
        i: usize,
        snap: &TrackSnapshot,
        rate_str: Option<&str>,
        eta_str: Option<&str>,
    ) -> SlotRowStrings {
        let slot = &self.slots[i];
        let is_overall = self.has_overall && i == self.slots.len() - 1;
        // Style-specific div-by-zero guard: a `WorkerSpinner` slot whose
        // assigned count is `0` (idle worker) would otherwise render
        // `total = 0`, which indicatif treats as indeterminate and hides
        // the bar. Pin it to `total = 1, pos = 0` so the idle worker shows
        // a fully-dimmed empty bar (all `░`). `StepCount` bars keep their
        // real total/position.
        let style = slot.source.borrow().as_ref().map_or(BarStyle::StepCount, |s| s.style());
        let (total, position) = if style == BarStyle::WorkerSpinner && snap.total == 0 {
            (1, 0)
        } else {
            (snap.total, snap.position)
        };
        let color_code = bar_color_code(snap.status, is_overall);

        // Client-defined truncation takes precedence when installed. The
        // renderer only *calls* the trait; it owns no field layout. The
        // `None` branch keeps the built-in component rendering as the
        // fallback so existing callers and tests stay green.
        let truncation = slot
            .source
            .borrow()
            .as_ref()
            .and_then(|s| s.truncation.read().expect("shared_state truncation lock").clone());
        let has_client_truncation = truncation.is_some();
        let ansi_overhead = compute_ansi_overhead(snap.status, has_client_truncation);
        let prefix = if let Some(t) = truncation.as_ref() {
            client_truncated_prefix(t, self.prefix_w.get().saturating_sub(ansi_overhead))
        } else {
            let truncated_prefix = semantic_truncate_prefix(
                &snap.prefix_components,
                self.prefix_w.get().saturating_sub(ansi_overhead),
            );
            render_prefix_components(&truncated_prefix, snap.status)
        };
        // Whether this row draws its timing depends on what the row holds
        // without it, which takes both of this frame's settled widths to
        // render. The two are composed either way; the one that loses is
        // the one the row draws.
        let timingless_suffix = compose_suffix(snap, rate_str, eta_str, false);
        let timingless_rendered = render_slot_suffix(
            &timingless_suffix,
            truncation.as_ref(),
            self.suffix_w.get(),
            color_code,
        );
        let fresh_suffix =
            if keeps_timing(self.draw_fill.get(), prefix.as_str(), timingless_rendered.as_str()) {
                compose_suffix(snap, rate_str, eta_str, true)
            } else {
                timingless_suffix
            };
        let message =
            render_slot_suffix(&fresh_suffix, truncation.as_ref(), self.suffix_w.get(), color_code);
        SlotRowStrings { prefix, message, position, total }
    }

    /// Record a snapshot's position/length/suffix/prefix into the cache of
    /// the slot at `i`. **This is the authoritative `SharedState` → slot-cache
    /// sync point.** Every code path that still reflects `SharedState`
    /// (position, total, suffix, prefix) in a slot's cache — the `attach`/
    /// resize re-syncs — goes through here; the frame itself renders the same
    /// strings, from [`slot_row_strings`](Self::slot_row_strings), into a
    /// plan.
    ///
    /// Does **not** record the row's style — that is
    /// [`sync_slot`](Self::sync_slot)'s `StyleKey` record.
    ///
    /// Returns whether anything was actually recorded (any dedup comparison
    /// that fired).
    ///
    /// `pub(super)` because the parent's [`sync_slot`](Self::sync_slot) both
    /// record through it.
    pub(super) fn sync_snapshot_to_slot(
        &self,
        i: usize,
        snap: &TrackSnapshot,
        rate_str: Option<&str>,
        eta_str: Option<&str>,
    ) -> bool {
        let slot = &self.slots[i];
        let strings = self.slot_row_strings(i, snap, rate_str, eta_str);
        let mut pushed = false;
        if strings.prefix != *slot.cache.prefix.borrow() {
            *slot.cache.prefix.borrow_mut() = strings.prefix;
            pushed = true;
        }
        if strings.message != *slot.cache.suffix.borrow() {
            *slot.cache.suffix.borrow_mut() = strings.message;
            pushed = true;
        }
        if strings.total != slot.cache.total.get() {
            slot.cache.total.set(strings.total);
            pushed = true;
        }
        if strings.position != slot.cache.position.get() {
            slot.cache.position.set(strings.position);
            pushed = true;
        }
        pushed
    }
}

#[cfg(test)]
mod tests {
    //! Unit tests for [`timing_strings`], the one rule set every measurement
    //! and draw path reads its rate and eta from.
    //!
    //! The invariant worth pinning at this level is the *pairing*: the eta a
    //! row reserves is divided by the same prospective rate its rate segment
    //! renders, and a terminal row reports neither. The frame-level consequence
    //! — a stale tick redraws the reserved eta rather than dropping it — is
    //! pinned by `progress_output::render::a_stale_tick_still_draws_the_reserved_eta`.

    use std::sync::Arc;
    use std::time::Duration;

    use super::super::super::{PrefixComponents, SuffixComponents, TestTimeSource, visible_width};
    use super::super::tracked::{TrackSnapshot, TrackStatus};
    use super::{SlotTiming, client_truncated_prefix, render_slot_suffix, timing_strings};
    use crate::progress::BarLabelTruncation;

    /// Build a minimal snapshot of one row at `position` of `total`.
    fn snap(status: TrackStatus, position: u64, total: u64) -> TrackSnapshot {
        TrackSnapshot {
            position,
            total,
            label: "test".into(),
            prefix: "test".into(),
            prefix_components: PrefixComponents::default(),
            suffix: String::new(),
            suffix_components: SuffixComponents::default(),
            status,
            elapsed: Duration::ZERO,
            animating: false,
        }
    }

    /// Build a slot's timing state as a fresh slot starts it: nothing
    /// committed, no rate, window opened at the time source's now.
    fn timing(time_source: &TestTimeSource) -> SlotTiming {
        SlotTiming::new(time_source)
    }

    /// A terminal row reports no timing at all, which is what lets
    /// [`super::ProgressRenderer::finalize`] reach the same `(None, None)` the
    /// measurement pass produces.
    #[test]
    fn a_terminal_row_reports_no_timing() {
        let time_source = TestTimeSource::new();
        let strings =
            timing_strings(&snap(TrackStatus::Success, 4, 4), &timing(&time_source), &time_source);
        assert_eq!(strings.rate_str, None, "a finished row draws no rate");
        assert_eq!(strings.eta_str, None, "a finished row draws no eta");
        assert!(strings.ema_update.is_none(), "a finished row advances no EMA");
    }

    /// A fresh bar has no EMA yet, so its rate is the placeholder and its eta
    /// is the remaining work divided by the nominal floor: the columns the
    /// budget reserves for the eta that appears once the bar progresses.
    #[test]
    fn a_bar_before_the_ema_starts_reserves_the_eta_at_the_nominal_floor() {
        let time_source = TestTimeSource::new();
        let strings =
            timing_strings(&snap(TrackStatus::Active, 0, 10), &timing(&time_source), &time_source);
        assert_eq!(strings.rate_str.as_deref(), Some("0/d"), "zero rate renders the placeholder");
        assert_eq!(strings.eta_str.as_deref(), Some("10s"), "10 remaining at the 1.0/s floor");
        assert!(strings.ema_update.is_none(), "an unmoved position advances no EMA");
    }

    /// A moved position is smoothed *prospectively*: the returned strings use
    /// the rate the caller will commit, and the commit itself comes back so
    /// the frame's slot sync can store exactly what was rendered.
    #[test]
    fn a_moved_position_smooths_prospectively_and_hands_back_the_commit() {
        let time_source = TestTimeSource::new();
        let mut slot_timing = timing(&time_source);
        time_source.advance(Duration::from_secs(5));
        let strings = timing_strings(&snap(TrackStatus::Active, 5, 10), &slot_timing, &time_source);
        // 5 units over 5 s = 1.0/s observed; 0 * 0.9 + 1.0 * 0.1 = 0.1/s.
        assert_eq!(strings.rate_str.as_deref(), Some("6/m"), "the smoothed rate is rendered");
        assert_eq!(strings.eta_str.as_deref(), Some("50s"), "the eta divides by that same rate");

        let update = strings.ema_update.expect("a moved position hands back its EMA update");
        assert_eq!(update.position, 5, "the window ends at the observed position");
        slot_timing.rate = update.rate;
        slot_timing.prev_position = update.position;
        slot_timing.prev_instant = update.instant;

        // Re-reading the same state must not smooth again: the commit closed
        // the window the strings were computed from.
        let again = timing_strings(&snap(TrackStatus::Active, 5, 10), &slot_timing, &time_source);
        assert_eq!(
            again.rate_str.as_deref(),
            Some("6/m"),
            "a committed window renders the same rate"
        );
        assert!(again.ema_update.is_none(), "and hands back no second commit");
    }

    /// Work completed leaves no remaining units, so the row reports its rate
    /// and no eta — the segment is present iff the row is still incomplete.
    #[test]
    fn a_completed_row_reports_its_rate_and_no_eta() {
        let time_source = TestTimeSource::new();
        let strings =
            timing_strings(&snap(TrackStatus::Active, 10, 10), &timing(&time_source), &time_source);
        assert_eq!(
            strings.rate_str.as_deref(),
            Some("0/d"),
            "an active row still reports its rate"
        );
        assert_eq!(strings.eta_str, None, "and no eta once the work is done");
    }

    /// A client label that ends mid-escape must be closed at the push point.
    ///
    /// Production labels are escape-free, so the plain-label test below pins
    /// that their bytes stay exactly `reset + label`; this fake pins the
    /// defensive rule for any label that embeds an unbalanced colour.
    struct OpenEscapeLabel;

    impl BarLabelTruncation for OpenEscapeLabel {
        fn truncate_prefix(&self, _max_width: usize) -> String {
            "x\x1b[31m".into()
        }

        fn truncate_suffix(&self, _max_width: usize, _suffix: &SuffixComponents) -> String {
            "y\x1b[31m".into()
        }
    }

    /// The client prefix wrapper leads with the reset it always has and now
    /// trails one when — and only when — the client's own string embeds an
    /// escape, so a production escape-free label keeps its exact bytes.
    #[test]
    fn client_truncated_prefix_closes_an_open_escape_and_leaves_plain_labels_alone() {
        /// An escape-free label standing in for every production client.
        struct PlainLabel;
        impl BarLabelTruncation for PlainLabel {
            fn truncate_prefix(&self, _max_width: usize) -> String {
                "plain".into()
            }

            fn truncate_suffix(&self, _max_width: usize, _suffix: &SuffixComponents) -> String {
                "plain".into()
            }
        }
        let plain: Arc<dyn BarLabelTruncation> = Arc::new(PlainLabel);
        let open: Arc<dyn BarLabelTruncation> = Arc::new(OpenEscapeLabel);
        assert_eq!(client_truncated_prefix(&plain, 10), "\x1b[0mplain");
        assert_eq!(
            client_truncated_prefix(&open, 10),
            "\x1b[0mx\x1b[31m\x1b[0m",
            "an open client escape is closed at the same push point"
        );
    }

    /// The suffix funnel is the single string measure and draw both read, so
    /// whatever it returns must end in a state that cannot leak and must fit
    /// the slot it was truncated to — an over-wide value is what would make
    /// indicatif slice the string and cut an escape in half.
    #[test]
    fn render_slot_suffix_closes_state_and_fits_its_width() {
        // Built-in branch: the coloured count segment already self-closes and
        // the reset keeps the invariant true after truncation to a small slot.
        let parts = SuffixComponents {
            count: "0".into(),
            total: "5".into(),
            elapsed: "1234567s".into(),
            rate: Some("2.2/s".into()),
            eta: Some("9s".into()),
            custom: String::new(),
        };
        let builtin = render_slot_suffix(&parts, None, 4, "35");
        assert!(
            builtin.contains('\x1b'),
            "the coloured count keeps an escape in the raw string: {builtin:?}"
        );
        assert!(
            builtin.ends_with("\x1b[0m"),
            "a suffix with escapes must terminate in a reset: {builtin:?}"
        );
        assert!(visible_width(&builtin) <= 4, "the suffix exceeded its slot: {builtin:?}");

        // Client branch: an open escape from the label is closed here.
        let open: Arc<dyn BarLabelTruncation> = Arc::new(OpenEscapeLabel);
        let client = render_slot_suffix(&parts, Some(&open), 10, "35");
        assert_eq!(client, "y\x1b[31m\x1b[0m");
    }
}
