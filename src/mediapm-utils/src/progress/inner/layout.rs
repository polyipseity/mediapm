//! The column budget: how many columns the prefix and the suffix get, and the
//! single push point that spends them on a slot's indicatif bar.
//!
//! A frame answers two questions in order. [`ProgressRenderer::recompute_layout`]
//! measures every row the frame would draw and settles one prefix width and one
//! suffix width for all of them, so rows align across the screen;
//! [`ProgressRenderer::sync_snapshot_to_bar`] then renders one row into the
//! widths that pass settled. The seven free helpers below are the row-shaped
//! halves of those two steps: they compose and measure a single suffix, and
//! they own the ANSI overhead the prefix slot has to reserve before a label is
//! measured.
//!
//! Both steps read tracked state through a [`TrackSnapshot`] rather than
//! through the `SharedState` behind it, so a layout pass cannot advance
//! anything and two passes over unchanged state produce identical widths.

use std::sync::Arc;

use super::super::{
    FRAME_OVERHEAD_COLUMNS, MIN_BAR_FILL, SuffixComponents, bar_color_code, format_count,
    format_elapsed, format_eta, format_rate, max_prefix_width, max_suffix_width,
    render_prefix_components, render_suffix_components, semantic_truncate_prefix,
    semantic_truncate_suffix, visible_width,
};
use crate::progress::BarStyle;

use super::ProgressRenderer;
use super::tracked::{TrackSnapshot, TrackStatus};

/// Compute the ANSI byte overhead for the prefix template field.
///
/// The `{prefix:N.N}` template in indicatif counts ANSI escape bytes as
/// visible characters. Client-truncated bars add only a 4-byte reset
/// (`\x1b[0m`); built-in rendering adds 13 bytes for failed/warning
/// status markers (`\x1b[0m\x1b[3Xm\x1b[0m`). This helper centralizes
/// the calculation so both `sync_snapshot_to_bar` and `recompute_layout`
/// use the same value.
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
/// `pub(super)` because the client-truncation test sibling pins both halves of
/// the wrapping through the parent module.
pub(super) fn client_truncated_prefix(
    truncation: &Arc<dyn crate::progress::BarLabelTruncation>,
    width: usize,
) -> String {
    format!("\x1b[0m{}", truncation.truncate_prefix(width))
}

/// Compose the full suffix component set from snapshot data and timing.
///
/// `keep_timing` decides whether the row draws its timing columns. See
/// [`keeps_timing`] for why a row that has dropped its fill sometimes keeps
/// them anyway. The strip itself lives in [`without_timing`], so the draw path
/// and the measurement pass in [`ProgressRenderer::recompute_layout`] cannot
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
/// timing stripped, at the widths [`ProgressRenderer::recompute_layout`]
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
/// `sync_snapshot_to_bar` draws through this and
/// [`ProgressRenderer::recompute_layout`] measures through it, so what the
/// budget reserves and what the row shows are the same string.
fn render_slot_suffix(
    suffix: &SuffixComponents,
    truncation: Option<&Arc<dyn crate::progress::BarLabelTruncation>>,
    suffix_w: usize,
    color_code: &str,
) -> String {
    match truncation {
        Some(t) => t.truncate_suffix(suffix_w, suffix),
        None => render_suffix_components(&semantic_truncate_suffix(suffix, suffix_w), color_code),
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

impl ProgressRenderer {
    /// Defensive sync: refresh all render slots from their tracked sources.
    ///
    /// Includes resize reactivity and full style re-application.
    ///
    /// When the write gate is active (production),
    /// property-setter terminal writes are suppressed during the update
    /// loop, then exactly one draw is released at the end.  This ensures
    /// the 50 ms daemon ticker is the sole draw authority and eliminates
    /// flicker from burst writes.
    /// Recompute the uniform `prefix_w`/`suffix_w` applied to every visible
    /// bar this frame.
    ///
    /// Measures the rendered prefix/suffix width of each bound slot (via
    /// [`SharedState::snapshot`](super::tracked::SharedState::snapshot)), takes the max across all bound slots, and
    /// spends the terminal's columns on them: what is left after the spinner,
    /// the separators and [`MIN_BAR_FILL`] is split between the two, the
    /// suffix taking what it measured first and the prefix the remainder.
    ///
    /// The result is stored in the `prefix_w`/`suffix_w` cells and every bound
    /// slot is re-synced so all bars share the same alignment width — short
    /// labels no longer waste space and long labels no longer overflow the bar.
    ///
    /// This is the budget and nothing else: it says how many columns a label
    /// may use, not which of its fields survive. That is
    /// [`semantic_truncate_prefix`] for the built-in components and the
    /// client's own [`BarLabelTruncation`] for a client label, and each ranks
    /// its own fields. Moving a ranking in here is tempting and wrong: it puts
    /// one cut in two places, and the two disagree the first time a screen
    /// gains a field.
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
    pub(crate) fn recompute_layout(&self) {
        let (_, cols) = self.dim_source.dimensions();
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
                // width budget. Replicate the rate/eta computation from the
                // tick loop so the estimate matches what will actually draw.
                let rate_str: Option<String> = if snap.status == TrackStatus::Active {
                    // Prospective rate: replicate the tick-loop EMA update
                    // read-only so the measured width matches what will draw.
                    let mut rate = self.slots_timing[i].rate;
                    if snap.position != self.slots_timing[i].prev_position {
                        let now = self.time_source.now();
                        let dt =
                            now.duration_since(self.slots_timing[i].prev_instant).as_secs_f64();
                        if dt > 0.001 {
                            #[allow(clippy::cast_precision_loss)]
                            let current =
                                (snap.position - self.slots_timing[i].prev_position) as f64 / dt;
                            rate = rate * 0.9 + current * 0.1;
                        }
                    }
                    Some(format_rate(rate))
                } else {
                    None
                };
                // Reserve eta width for any in-progress bar.  At measure
                // time `slots_timing[i].rate` may still be 0 (the bar was
                // just attached and has not ticked yet), but the draw path
                // computes eta whenever rate > 0 — which it will be once
                // the bar progresses.  Under-reserving here would let the
                // later wider draw overflow `suffix_w` and truncate the
                // custom suffix.  Use a nominal rate floor so the budget
                // covers the eta segment that will appear on the next tick.
                let eta_str = if snap.status == TrackStatus::Active && snap.total > snap.position {
                    let rate = if self.slots_timing[i].rate > 0.0 {
                        self.slots_timing[i].rate
                    } else {
                        1.0
                    };
                    #[allow(clippy::cast_precision_loss)]
                    let remaining = (snap.total - snap.position) as f64 / rate;
                    Some(format_eta(remaining))
                } else {
                    None
                };
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
                let suffix_with_timing =
                    compose_suffix(&snap, rate_str.as_deref(), eta_str.as_deref(), true);
                let suffix_without_timing =
                    compose_suffix(&snap, rate_str.as_deref(), eta_str.as_deref(), false);
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
        for (i, slot) in self.slots.iter().enumerate() {
            if slot.source.borrow().is_some() {
                self.sync_slot(i);
            }
        }
    }

    /// Apply a snapshot's position/length/suffix/prefix to the indicatif bar
    /// at slot `i`. **This is the single authoritative push point for
    /// `SharedState` → indicatif.** All code paths that reflect `SharedState`
    /// (position, total, suffix, prefix) on the terminal bar must call
    /// through here — both the daemon ticker and [`finalize`](Self::finalize)
    /// do.
    ///
    /// Does **not** change the bar's style — callers manage style
    /// independently via [`finish_slot`](Self::finish_slot) or explicit
    /// `set_style` calls during attach/resize.
    ///
    /// `pub(super)` because the parent's [`recompute_layout`](Self::recompute_layout) and
    /// [`finish_slot`](Self::finish_slot) both push
    /// through it.
    pub(super) fn sync_snapshot_to_bar(
        &self,
        i: usize,
        snap: &TrackSnapshot,
        rate_str: Option<&str>,
        eta_str: Option<&str>,
    ) {
        let slot = &self.slots[i];
        let is_overall = self.has_overall && i == self.slots.len() - 1;
        // Style-specific div-by-zero guard: a `WorkerSpinner` slot whose
        // assigned count is `0` (idle worker) would otherwise render
        // `total = 0`, which indicatif treats as indeterminate and hides
        // the bar. Pin it to `total = 1, pos = 0` so the idle worker shows
        // a fully-dimmed empty bar (all `░`). `StepCount` bars keep their
        // real total/position.
        let style = slot.source.borrow().as_ref().map_or(BarStyle::StepCount, |s| s.style());
        let (render_total, render_pos) = if style == BarStyle::WorkerSpinner && snap.total == 0 {
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
        let new_prefix = if let Some(t) = truncation.as_ref() {
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
        // render. The two are composed either way; the one that loses is the
        // one the row draws. Decided before the prefix is pushed so the
        // prefix is still in hand to answer it.
        let timingless_suffix = compose_suffix(snap, rate_str, eta_str, false);
        let timingless_rendered = render_slot_suffix(
            &timingless_suffix,
            truncation.as_ref(),
            self.suffix_w.get(),
            color_code,
        );
        let fresh_suffix = if keeps_timing(
            self.draw_fill.get(),
            new_prefix.as_str(),
            timingless_rendered.as_str(),
        ) {
            compose_suffix(snap, rate_str, eta_str, true)
        } else {
            timingless_suffix
        };
        if new_prefix != *slot.cache.prefix.borrow() {
            slot.bar.set_prefix(new_prefix.clone());
            *slot.cache.prefix.borrow_mut() = new_prefix;
        }
        // Build display suffix: client truncation when installed, else
        // truncate the fresh component set then render.
        let display_suffix =
            render_slot_suffix(&fresh_suffix, truncation.as_ref(), self.suffix_w.get(), color_code);
        if display_suffix != *slot.cache.suffix.borrow() {
            slot.bar.set_message(display_suffix.clone());
            *slot.cache.suffix.borrow_mut() = display_suffix;
        }
        if render_total != slot.cache.total.get() {
            slot.bar.set_length(render_total);
            slot.cache.total.set(render_total);
        }
        if render_pos != slot.cache.position.get() {
            slot.bar.set_position(render_pos);
            slot.cache.position.set(render_pos);
        }
    }
}
