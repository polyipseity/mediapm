//! The [`ProgressRenderer`]: a fixed-size grid of indicatif bars that a
//! [`ProgressScreen`](super::ProgressScreen) fills and commits, and the state
//! that decides which row each bar shows.
//!
//! A renderer owns one indicatif bar per slot and moves rows through them as
//! handles attach, tick and finish. Slots are handed out from the bottom of
//! the active band upwards, so an attach never reshuffles the rows already on
//! screen. A capacity change prepends or drains blank slots rather than
//! rebuilding the grid, which keeps a resize from reordering live rows.
//!
//! The renderer is the meeting point between the parts it is split across. [`tracked`] holds what a producer advances and knows nothing about
//! indicatif; [`layout`] spends terminal columns and is the single push point
//! from tracked state to a bar; and [`frame`] runs one frame end to end. This
//! module holds the renderer itself: the slot grid, its per-slot cache and
//! timing, and the lifecycle that binds a handle to a slot and commits the
//! result.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::Instant;

use indicatif::{MultiProgress, ProgressBar, ProgressFinish};

use super::{
    DimensionSource, MAX_SLOTS, MIN_PREFIX_WIDTH, MIN_SUFFIX_WIDTH, ProgressDebugSink, TimeSource,
    WriteGate, apply_bar_style, apply_done_bar_style, apply_failed_bar_style,
    apply_overall_bar_style, blank_bar_style,
};
use layout::timing_strings;

// The three themed modules below are declared by path so they stay flat
// siblings of `renderer.rs` rather than moving into a `renderer/` directory.
// They are children of `renderer`, which is what lets each one read the
// renderer's private grid fields without widening those fields; `renderer.rs`
// re-exports the three items its own parent names.
#[path = "frame.rs"]
mod frame;
#[path = "layout.rs"]
mod layout;
#[path = "tracked.rs"]
mod tracked;

pub(crate) use tracked::SharedState;
pub use tracked::{ProgressBarHandle, TrackSnapshot, TrackStatus};

// The two budget helpers and the suffix component the layout test siblings name
// through `use super::*`, kept here so their `use super::*` paths keep working.
#[cfg(test)]
use super::SuffixComponents;
#[cfg(test)]
use layout::{client_truncated_prefix, compute_ansi_overhead};

// ---- ProgressRenderer + ProgressScreen (rendering + combined) ----------

/// A single slot in the renderer's fixed-size grid.
struct RenderedSlot {
    /// The indicatif [`ProgressBar`] that draws to the terminal.
    bar: ProgressBar,
    /// Optional tracking state this slot is currently bound to.
    /// `None` means the slot is blank (unused).
    source: RefCell<Option<Arc<SharedState>>>,
    /// Cached last values pushed to the bar, used to skip redundant
    /// indicatif calls and reduce terminal flicker.
    cache: SlotCache,
}

/// Manages a fixed-size grid of [`ProgressBar`] slots in [`MultiProgress`]
/// with shift-based allocation and automatic recycling of finished slots.
///
/// All slots are pre-allocated at construction so the draw height never
/// changes — eliminating the root cause of terminal ghosting.
///
/// # Allocation strategy
///
/// 1. `attach` places new children into the **bottom** of the
///    active band (just above the overall bar if one exists) and shifts all
///    existing active children up by one slot, preserving chronological order
///    top-to-bottom.
/// 2. When all slots are occupied by active handles, finished slots are
///    recycled (scanning from the bottom upward).
/// 3. When no finished slot can be recycled, the new handle is pushed into
///    `orphaned_states` — tracked but with no render slot until the terminal
///    grows.
/// 4. Finished bars stay visible — their slots are only recycled when new
///    handles need display space.
pub struct ProgressRenderer {
    inner: MultiProgress,
    /// Render slots in draw order: children first, the overall bar last when
    /// one exists. Slot count is fixed per terminal height (see
    /// `dynamic_height`), and slots are recycled rather than removed.
    slots: Vec<RenderedSlot>,
    has_overall: bool,
    dim_source: Arc<dyn DimensionSource>,
    last_width: Option<u16>,
    /// When `true`, the slot count may be adjusted on terminal height
    /// changes.  `false` when the caller specified an explicit capacity
    /// (e.g. via [`from_mp`](Self::from_mp)).
    pub(crate) dynamic_height: bool,
    /// Queue of [`SharedState`] handles evicted from render slots during
    /// height shrink.  Reattached (FIFO) when the terminal grows back.
    orphaned_states: RefCell<VecDeque<Arc<SharedState>>>,
    /// Guard against double-`finalize` from both
    /// [`ProgressScreen::join_and_clear`](super::ProgressScreen::join_and_clear)
    /// and [`Drop`].
    finalized: Cell<bool>,
    /// Injectable time source (real or synthetic for testing).
    pub(crate) time_source: Arc<dyn TimeSource>,

    /// EMA-smoothed rate tracking, one entry per slot.
    slots_timing: Vec<SlotTiming>,
    /// Write gate controlling terminal-write suppression during frames.
    /// Owned exclusively by the renderer — the only way to open/close
    /// the write window is through `gate.open()` (private to `gate.rs`).
    gate: WriteGate,

    /// Nesting guard: `true` while inside [`run_frame`](Self::run_frame).  Panics
    /// in debug builds if `run_frame` or `tick` is called re-entrantly.
    in_frame: Cell<bool>,

    /// Optional JSONL debug sink — emits bar-state snapshots on every tick.
    /// Shared (`Arc`) because one sink belongs to the terminal and is used by
    /// every screen's renderer, so tick numbering stays monotonic across a
    /// sync instead of restarting per phase.
    debug_sink: Option<Arc<ProgressDebugSink>>,

    /// Current uniform prefix width applied to every visible bar this frame.
    /// Recomputed each tick from the widest measured prefix among bound slots,
    /// then held to what the terminal has left after the suffix and the bar
    /// floor, and never past [`MIN_PREFIX_WIDTH`] or
    /// [`max_prefix_width`](super::max_prefix_width).
    prefix_w: Cell<usize>,
    /// Current uniform suffix width applied to every visible bar this frame.
    /// See [`Self::prefix_w`] — same contract with [`MIN_SUFFIX_WIDTH`] /
    /// [`max_suffix_width`](super::max_suffix_width).
    suffix_w: Cell<usize>,
    /// Whether this frame's bars draw their fill.
    ///
    /// `false` once the fill the budget affords is at
    /// [`MIN_BAR_FILL`](super::MIN_BAR_FILL), which
    /// is the width a frame's labels leave the bar at every terminal narrower
    /// than the one where the labels stop overflowing the line. A fill that
    /// size carries a fixed quarter-resolution fraction the count beside it
    /// already states, and it is paid for out of a label clipped down to its
    /// tail, so below that point the frame drops it and the columns go to the
    /// label and the count. See [`Self::recompute_layout`].
    draw_fill: Cell<bool>,
}

/// EMA-smoothed rate tracking for a render slot.
struct SlotTiming {
    prev_position: u64,
    prev_instant: Instant,
    rate: f64,
}

impl SlotTiming {
    fn new(time_source: &dyn TimeSource) -> Self {
        Self { prev_position: 0, prev_instant: time_source.now(), rate: 0.0 }
    }
}

/// Message a blanked slot carries.
///
/// One space rather than the empty string, because an empty message writes no
/// line at all and the rows above would lose their anchor.
const BLANK_MESSAGE: &str = " ";

/// Cached last values pushed to a bar, used to skip redundant indicatif
/// setter calls and reduce terminal flicker.
///
/// The cache describes **the bar**, not the source bound to it. A slot's
/// source moves between bars as the band shifts, but the bar's own state does
/// not, so a rebind keeps the cache and the next sync can still tell whether
/// the value it is about to push is already on screen. Rebuilding the cache on
/// a rebind throws that away and makes every rebind push a value the bar
/// already shows.
struct SlotCache {
    /// Last position sent to `set_position`.
    position: Cell<u64>,
    /// Last total sent to `set_length`.
    total: Cell<u64>,
    /// Last display suffix sent to the bar.
    suffix: RefCell<String>,
    /// Last prefix sent to `set_prefix`.
    prefix: RefCell<String>,
    /// `true` while the slot's bar carries the blank state
    /// [`blank_new_bar`] pushed. Cleared by the first real
    /// content pushed onto the bar, so a rebound-and-released slot is never
    /// reported as blank.
    blank: Cell<bool>,
    /// The source whose terminal-state transition this bar has already
    /// carried.
    ///
    /// `Some` once [`finish_slot`](ProgressRenderer::finish_slot) has applied
    /// its abandon/finish to the bar for exactly that source. The record is
    /// per (slot, source) pair rather than a per-slot flag: a band shift that
    /// moves an already-terminal source into another slot re-applies the
    /// transition for that new bar (which has never carried it), while an
    /// untouched terminal row is never re-finished — and re-ticked — on later
    /// frames. A row that goes active again clears the record.
    terminal_finished_for: RefCell<Option<Weak<SharedState>>>,
    /// `true` while this slot holds the blank a cleared source drew. A cleared
    /// row stays blank until a rebind — [`attach`](ProgressRenderer::attach)
    /// or the height-growth reattach — lands a source in the slot, which
    /// clears the flag and lets the row be drawn again.
    cleared_blank_held: Cell<bool>,
    /// The style inputs last pushed through `set_style`: settled
    /// `prefix_w`, settled `suffix_w`, the overall-row flag, the draw-fill
    /// decision, and the row's status code.
    ///
    /// This tuple is **write suppression, not staleness gating**, and the
    /// difference is what makes it safe after the dirty-flag removal: every
    /// member is derived by the frame itself — the settled widths come from
    /// `recompute_layout` on this very frame, the status from the source's
    /// snapshot, the flags from the renderer's own layout — so nothing a
    /// caller could forget to declare feeds it. A moved input re-applies the
    /// style immediately. What it suppresses is the indicatif tick a
    /// `set_style` carries: re-styling an unchanged row every frame would
    /// advance that row's spinner glyph on a finished bar.
    style_key: Cell<StyleKey>,
}

/// The derived inputs of one row's bar style, compared to suppress the
/// redundant `set_style` write (and the spinner tick it carries).
///
/// [`StyleKey::unknown`] is the value no settled frame can produce. A slot
/// carries it before its first real sync and after a blank, so the next bind
/// re-applies the style even when every derived input happens to match what
/// the blank's own bar was last styled with — the blank is a style input the
/// frame does not derive, and a key that ignored it left a re-bound row
/// drawing its content through the blank `{wide_msg}` template.
#[derive(Clone, Copy, PartialEq, Eq)]
struct StyleKey {
    /// Settled prefix slot width.
    prefix_w: usize,
    /// Settled suffix slot width.
    suffix_w: usize,
    /// Whether this slot is the screen's pinned overall row.
    is_overall: bool,
    /// Whether the frame draws a fill this row.
    draw_fill: bool,
    /// Status code the style colours by.
    status_code: u8,
}

impl StyleKey {
    /// A key no settled frame produces, forcing the next `set_style`.
    fn unknown() -> Self {
        Self {
            prefix_w: usize::MAX,
            suffix_w: usize::MAX,
            is_overall: false,
            draw_fill: false,
            status_code: u8::MAX,
        }
    }
}

impl SlotCache {
    /// A cache that has never pushed onto its bar.
    ///
    /// `position` and `total` hold the `u64::MAX` never-pushed sentinel, so
    /// nothing a fresh bar already shows can be mistaken for a repeat of an
    /// earlier push.
    fn new() -> Self {
        Self {
            position: Cell::new(u64::MAX),
            total: Cell::new(u64::MAX),
            suffix: RefCell::new(String::new()),
            prefix: RefCell::new(String::new()),
            blank: Cell::new(false),
            terminal_finished_for: RefCell::new(None),
            cleared_blank_held: Cell::new(false),
            style_key: Cell::new(StyleKey::unknown()),
        }
    }

    /// Forget the position last pushed, because the bar no longer holds it.
    ///
    /// [`indicatif::ProgressBar::reset`] returns the bar's position to zero and
    /// leaves its length alone, so only the position has to be invalidated:
    /// [`total`](Self::total) still describes the bar.
    fn invalidate_position(&self) {
        self.position.set(u64::MAX);
    }

    /// Whether this bar has already carried the terminal transition for
    /// `source`.
    ///
    /// Answers false after a rebind — a source this bar has never finished
    /// for upgrades to nothing here or fails the pointer comparison — which
    /// is what re-applies the transition to the bar the source just landed on.
    fn has_terminal_finished_for(&self, source: &Arc<SharedState>) -> bool {
        self.terminal_finished_for
            .borrow()
            .as_ref()
            .and_then(Weak::upgrade)
            .is_some_and(|held| Arc::ptr_eq(&held, source))
    }

    /// Record that this bar has carried the terminal transition for `source`.
    fn mark_terminal_finished(&self, source: &Arc<SharedState>) {
        *self.terminal_finished_for.borrow_mut() = Some(Arc::downgrade(source));
    }

    /// Forget the recorded source: the row is live again (or blanked), so its
    /// next terminal state re-applies the transition.
    fn clear_terminal_finished(&self) {
        self.terminal_finished_for.borrow_mut().take();
    }

    /// Record the blank state [`blank_new_bar`] just pushed onto a fresh bar.
    fn mark_blanked(&self) {
        self.prefix.borrow_mut().clear();
        *self.suffix.borrow_mut() = BLANK_MESSAGE.to_string();
        self.blank.set(true);
        self.clear_terminal_finished();
        // The blank is a style input the frame does not derive, so the key
        // goes unknown: the next bind re-styles the row whatever its derived
        // inputs say, which is what keeps a re-bound slot from drawing real
        // content through the blank `{wide_msg}` template.
        self.style_key.set(StyleKey::unknown());
    }

    /// Whether this slot was blanked and nothing has touched its bar since.
    ///
    /// A fresh cache carries a cleared blank flag and an empty suffix, so the
    /// two cannot be confused. A real bind pushes content onto the bar through
    /// [`sync_snapshot_to_bar`](ProgressRenderer::sync_snapshot_to_bar), which
    /// clears the flag, so a slot that was rebound and released again is never
    /// reported as blank.
    fn is_blanked(&self) -> bool {
        self.blank.get() && self.suffix.borrow().as_str() == BLANK_MESSAGE
    }
}

/// Put a freshly created bar into the blank state a new slot starts in.
///
/// Shared by slot construction and height growth so the state a fresh slot's
/// cache records is the state its bar is really in, and so the first
/// [`ProgressRenderer::blank_bar`] on it has nothing to do.
fn blank_new_bar(bar: &ProgressBar, cache: &SlotCache) {
    bar.set_style(blank_bar_style());
    bar.set_message(BLANK_MESSAGE);
    bar.set_prefix("");
    cache.mark_blanked();
}

/// Apply the screen's slot finish policy to a bar that has just been added to a [`MultiProgress`].
///
/// Slot bars use [`ProgressFinish::AndLeave`] so that a bar which is still **unfinished** when its screen is committed keeps the last line it drew, just like a finished one: indicatif's default [`ProgressFinish::AndClear`] makes `BarState::drop` run `finish_using_style`, which sets `Status::DoneHidden` and clears the line, so a partially-filled line — exactly what a `?` early return leaves behind — would be cleared instead of committed. That clearing draw only reaches a target that writes it: an **ungated** [`MultiProgress`] does, while the production target is a `BufferedTerm` write gate that suppresses every write outside an open window, leaving the frame [`ProgressRenderer::finalize`] drew as the last one the terminal sees.
///
/// The policy is therefore **defense-in-depth**, not a fix for an observed production defect: it keeps the retention contract from depending on the gate's window timing. See [`ProgressScreen::join`](super::terminal::ProgressScreen::join) for the full per-configuration statement.
///
/// `AndLeave` cannot change the finished-bar path: `BarState::drop` short-circuits on `is_finished()` (`indicatif/src/state.rs`), returns before `finish_using_style` is reached, and therefore never consults this policy.
///
/// Callers must add the bar to the [`MultiProgress`] first (as `from_mp` documents); this only returns the same bar with the policy applied.
fn with_slot_finish_policy(bar: ProgressBar) -> ProgressBar {
    bar.with_finish(ProgressFinish::AndLeave)
}

impl ProgressRenderer {
    /// Pre-allocate `capacity` blank bars in an existing [`MultiProgress`].
    pub(crate) fn from_mp(
        mp: MultiProgress,
        capacity: usize,
        dim_source: Arc<dyn DimensionSource>,
        gate: WriteGate,
        time_source: Arc<dyn TimeSource>,
        debug_sink: Option<Arc<ProgressDebugSink>>,
    ) -> Self {
        let mut slots = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            let pb = ProgressBar::new(0);
            // IMPORTANT: add to MultiProgress FIRST, then configure.
            // Configuring before mp.add() prevents InMemoryTerm from
            // capturing blank bar output in tests.
            let bar = with_slot_finish_policy(mp.add(pb));
            let cache = SlotCache::new();
            blank_new_bar(&bar, &cache);
            slots.push(RenderedSlot { bar, source: RefCell::new(None), cache });
        }
        // Trigger a final draw so all bars are captured by InMemoryTerm
        // even when capacity == terminal height.
        if let Some(slot) = slots.last() {
            slot.bar.tick();
        }
        let slots_timing = (0..capacity).map(|_| SlotTiming::new(&*time_source)).collect();
        Self {
            inner: mp,
            slots,
            has_overall: false,
            dim_source,
            last_width: None,
            dynamic_height: false,
            orphaned_states: RefCell::new(VecDeque::new()),
            finalized: Cell::new(false),
            time_source,
            slots_timing,
            gate,
            in_frame: Cell::new(false),
            debug_sink,
            prefix_w: Cell::new(MIN_PREFIX_WIDTH),
            suffix_w: Cell::new(MIN_SUFFIX_WIDTH),
            draw_fill: Cell::new(true),
        }
    }

    /// Add an overall aggregate bar pinned at the bottom slot, drawing from
    /// `state`.
    ///
    /// `state` is the caller's own handle state, and the new slot adopts that
    /// very `Arc` as its render source: the caller's handle and the drawn
    /// overall bar are two views of one state, so every mutation made through
    /// the handle reaches the next frame. The bar's total and prefix are read
    /// from `state`, which stays the single source of truth for the overall bar
    /// — this method never fabricates a second state of its own.
    pub(crate) fn add_overall(&mut self, state: Arc<SharedState>) {
        let total = state.total.load(Ordering::Acquire);
        let label = {
            let guard = state.label.read().unwrap_or_else(std::sync::PoisonError::into_inner);
            guard.clone()
        };
        let inner = ProgressBar::new(total);
        let overall_bar = with_slot_finish_policy(self.inner.add(inner));
        // The style set here is the first frame's, before any
        // `recompute_layout` has run, so it carries the initial `draw_fill` and
        // is replaced on that first layout pass.
        apply_overall_bar_style(
            &overall_bar,
            MIN_PREFIX_WIDTH,
            MIN_SUFFIX_WIDTH,
            self.draw_fill.get(),
        );
        overall_bar.set_prefix(label);
        self.slots.push(RenderedSlot {
            bar: overall_bar,
            source: RefCell::new(Some(state)),
            cache: SlotCache::new(),
        });
        self.has_overall = true;
        self.slots_timing.push(SlotTiming::new(&*self.time_source));
    }

    /// Re-configure the bar at slot index `i` to reflect its current
    /// tracked source (or blank state if unbound).
    ///
    /// Runs every frame for every slot, bound or not: the settled widths and
    /// the row's own timing are re-derived each frame, so nothing about a
    /// row's geometry can go stale between mutations.
    ///
    /// Returns whether anything was pushed — the [`sync_snapshot_to_bar`](Self::sync_snapshot_to_bar)
    /// report, or `true` when a blank transition actually re-blanked the bar.
    pub(crate) fn sync_slot(&self, i: usize) -> bool {
        let slot = &self.slots[i];
        if let Some(ref source) = *slot.source.borrow() {
            let snap = source.snapshot();
            if source.is_cleared() {
                // A cleared source blanks its row exactly once, when the clear
                // lands — the one transition the consumer declares by calling
                // `finish_and_clear`. The row then stays blank until a rebind
                // re-enables it: `attach`'s band shift and the height-growth
                // reattach both clear the hold, so a slot a source is moved
                // into draws the row again. No per-frame change detection is
                // involved: the pending flag is the declared clear, the hold
                // is the slot's own memory, and the rebind is the event that
                // says a row may be drawn again.
                if source.take_cleared_blank() {
                    slot.cache.cleared_blank_held.set(true);
                    return self.blank_bar(i);
                }
                if slot.cache.cleared_blank_held.get() {
                    return false;
                }
            }
            let is_overall = self.has_overall && i == self.slots.len() - 1;
            let prefix_w = self.prefix_w.get();
            let suffix_w = self.suffix_w.get();
            let draw_fill = self.draw_fill.get();

            // The style is re-applied whenever one of its derived inputs
            // moved, and suppressed otherwise. The suppression is a write
            // filter, not a change-detection gate: every member of the key is
            // computed by this frame (see `StyleKey`), so there is no event a
            // caller must remember to declare — a moved settled width, status,
            // overall flag or draw-fill decision re-styles the row on the
            // same frame. What it prevents is the indicatif `set_style` tick
            // animating the spinner of a row whose style did not change,
            // finished rows above all.
            let style_key = StyleKey {
                prefix_w,
                suffix_w,
                is_overall,
                draw_fill,
                status_code: snap.status.code(),
            };
            if style_key != slot.cache.style_key.get() {
                if is_overall {
                    apply_overall_bar_style(&slot.bar, prefix_w, suffix_w, draw_fill);
                } else if snap.status == TrackStatus::Failed {
                    apply_failed_bar_style(&slot.bar, prefix_w, suffix_w, draw_fill);
                } else if snap.status != TrackStatus::Active {
                    apply_done_bar_style(&slot.bar, prefix_w, suffix_w, draw_fill);
                } else {
                    // Slot recycling may leave the indicatif bar with
                    // Status::DoneVisible from the previous phase.  Reset
                    // it to InProgress so the spinner cycles again.
                    if slot.bar.is_finished() {
                        slot.bar.reset();
                        slot.cache.invalidate_position();
                    }
                    apply_bar_style(&slot.bar, prefix_w, suffix_w, draw_fill);
                }
                slot.cache.style_key.set(style_key);
            }
            if snap.status == TrackStatus::Active {
                // The row is live, so its terminal transition is unrecorded:
                // when it next reaches a terminal state the frame applies the
                // transition again exactly once for that source.
                slot.cache.clear_terminal_finished();
            }
            // The timing comes from `timing_strings`, the same helper the
            // measurement pass calls, so the message this pushes occupies the
            // columns the budget reserved for it (the measurement/draw
            // equality invariant). The EMA commit it returns is dropped here:
            // this path is read-only and the frame's slot sync applies
            // the same update for the same snapshot.
            let timing = timing_strings(&snap, &self.slots_timing[i], &*self.time_source);
            self.sync_snapshot_to_bar(
                i,
                &snap,
                timing.rate_str.as_deref(),
                timing.eta_str.as_deref(),
            )
        } else {
            // An unbound slot keeps whatever its bar last drew.
            //
            // This is the retention contract: a slot whose source was
            // recycled or evicted still shows the line it last drew — the
            // recycled row in the lifecycle tests and the row a partially
            // filled screen commits both depend on it. Only two things write
            // a blank: a slot that never held content (blanked at construction
            // and on height growth) and a source that cleared itself.
            // Re-blanking a vacated slot every frame would erase those
            // retained rows, and a blank row is full-width spaces either way,
            // so there is no alignment to recover here.
            false
        }
    }

    /// Return slot `i` to a blank row and record that it is blank.
    ///
    /// Returns whether the bar was actually re-blanked: a slot already blank
    /// pushes nothing, and the caller's frame-owed-draw bookkeeping must not
    /// count it.
    ///
    /// All three pushes are real `update_estimate_and_draw` calls, so blanking
    /// a slot that is already blank spends three draws to change nothing. The
    /// skip is what makes this safe to call from the paths that walk every slot
    /// on a height change, and it holds because a rebind clears the marker: see
    /// [`SlotCache::is_blanked`].
    fn blank_bar(&self, i: usize) -> bool {
        let slot = &self.slots[i];
        if slot.cache.is_blanked() {
            return false;
        }
        blank_new_bar(&slot.bar, &slot.cache);
        true
    }

    /// Attach a tracked state to the next available render slot.
    ///
    /// Places the new child at the **bottom** of the active band (just
    /// above the overall bar when one exists).  Existing active children
    /// are shifted up by one slot, preserving chronological order:
    /// first-created child at the top of the band, last-created adjacent
    /// to the overall bar.
    ///
    /// When all slots are occupied by active handles, recycles the
    /// oldest finished slot from the top of the band and shifts all
    /// remaining bars up, keeping the newest bars contiguous at the
    /// bottom.  When no finished slot is available, the handle is pushed to
    /// [`orphaned_states`](Self::orphaned_states) — it remains tracked but has
    /// no render slot until the terminal grows back.
    ///
    /// Every slot keeps its [`SlotCache`]. The shift moves sources between
    /// slots, never bars between slots, so a slot's cache still describes
    /// what its own bar shows, and the sync that follows a rebind can skip
    /// a value that bar already holds. Rebuilding the cache here is what
    /// made every added bar cost a second `set_position` on the slot it
    /// landed in.
    pub(crate) fn attach(&mut self, state: &Arc<SharedState>) {
        // Buffer all draws during attach — slot shifts + sync_slot + recompute_layout.
        //
        // Every push here goes through `sync_slot`, and a `MultiProgress` draw
        // repaints the whole block from each bar's CURRENT state. A write
        // window open across this body would release the shift-phase syncs and
        // the first part of `recompute_layout`'s slot-order loop before it has
        // reached the last row, so the block a user sees would mix rows drawn
        // at the previous template widths with rows drawn at the new ones —
        // the per-row geometry divergence the live demo capture caught
        // (`.superpowers/sdd/progress-labels-alignment/task-1-report.md`,
        // Verdict 2). Suppressed pushes fail with retry, so every line stays
        // dirty and the next frame's open window draws the whole block at the
        // one `(prefix_w, suffix_w)` pair that frame settled: the daemon
        // ticker is the sole draw authority, and attach costs at most one
        // tick (≤50 ms on an active screen, ≤1 s on an idle one) of latency.
        self.gate.suppress();
        let child_cap = self.slots.len() - usize::from(self.has_overall);
        let bottom = child_cap.saturating_sub(1);
        // A rebind re-enables every row in the band: a slot this attach moves
        // a source into draws that source again, a cleared one included — the
        // recycled-row contract the lifecycle tests pin.
        for slot in &self.slots[..=bottom] {
            slot.cache.cleared_blank_held.set(false);
        }

        // Phase 1: shift active band up, place new child at bottom
        let active = self.slots[..=bottom].iter().filter(|s| s.source.borrow().is_some()).count();

        if active < child_cap {
            // Shift existing active children up by one slot (ascending
            // order preserves relative positions).
            for i in (bottom + 1 - active)..=bottom {
                let (left, right) = self.slots.split_at_mut(i);
                std::mem::swap(&mut left[left.len() - 1].source, &mut right[0].source);
                self.slots_timing.swap(i, i - 1);
            }
            // Sync shifted slots (sources moved to different bars).
            for i in (bottom.saturating_sub(active))..=bottom {
                self.sync_slot(i);
            }
            // Place new child at the freed bottom slot.
            self.slots[bottom].source.replace(Some(Arc::clone(state)));
            self.slots_timing[bottom] = SlotTiming::new(&*self.time_source);
            self.sync_slot(bottom);
            self.recompute_layout();
            return;
        }

        // Phase 2: compact — recycle the oldest finished slot and shift
        // all bars below it up by one slot, placing the new bar at the
        // bottom.  This keeps the most recent bars visible and contiguous.
        for old_i in 0..=bottom {
            if self.slots[old_i].source.borrow().as_ref().is_some_and(|s| s.is_finished()) {
                // Bubble the source at old_i rightward through bottom,
                // shifting all sources up by one slot.
                for j in old_i..bottom {
                    let (left, right) = self.slots.split_at_mut(j + 1);
                    std::mem::swap(&mut left[left.len() - 1].source, &mut right[0].source);
                    self.slots_timing.swap(j + 1, j);
                }
                // Sync shifted slots (sources moved to different bars).
                for j in old_i..bottom {
                    self.sync_slot(j);
                }
                // Place new child at the freed bottom slot.
                self.slots[bottom].source.replace(Some(Arc::clone(state)));
                self.slots_timing[bottom] = SlotTiming::new(&*self.time_source);
                self.sync_slot(bottom);
                self.recompute_layout();
                return;
            }
        }
        // Phase 3: no free slot — push to orphaned queue.
        self.orphaned_states.borrow_mut().push_back(Arc::clone(state));
    }

    /// Returns `true` when at least one tracked slot still has an active
    /// (non-terminal) source.  When this returns `false`, the daemon ticker
    /// can sleep longer since no spinner animation or progress updates are
    /// needed.
    pub(crate) fn has_active_slots(&self) -> bool {
        self.slots
            .iter()
            .any(|slot| slot.source.borrow().as_ref().is_some_and(|s| !s.is_finished()))
    }

    /// Respond to terminal dimension changes since the last tick.
    ///
    /// Adjusts the slot capacity when height changes (prepending or
    /// draining blank slots) and records a width change so the frame counts
    /// the redraw it owes. Styles are not applied here: every frame re-derives
    /// the settled widths and re-styles every slot itself, which is what makes
    /// a width change take effect on all rows.
    ///
    /// No template is selected by width. The styles in `components.rs`
    /// are built per frame from the live `prefix_w`/`suffix_w` cells, and those
    /// cells are already paid for out of the terminal width, so a narrower
    /// terminal takes columns from the label first and only then from the
    /// fill. The width at which a screen runs out of fill is therefore a
    /// property of that screen's own labels and suffix, not a threshold in
    /// this function: `MIN_BAR_FILL` is what every screen keeps in common.
    ///
    /// Returns `true` if any dimension actually changed.
    pub(crate) fn maybe_adjust_for_resize(&mut self) -> bool {
        let (rows, cols) = self.dim_source.dimensions();
        let mut changed = false;

        // --- Width reactivity ---
        //
        // Recording the change is all this does. The frame re-measures every
        // row from the live terminal width each tick and re-styles every slot
        // from the settled budget, so no per-slot re-sync is owed here: what
        // the flag buys is the frame counting the redraw those re-styles owe.
        if self.last_width != Some(cols) {
            self.last_width = Some(cols);
            changed = true;
        }

        // --- Height reactivity ---
        if self.dynamic_height {
            let desired_cap = (rows as usize).clamp(1, MAX_SLOTS);
            let current_cap = self.slots.len();
            if desired_cap > current_cap {
                changed = true;
                // Grow: append blank slots before the overall bar (or at
                // end when no overall bar exists).  New terminal space
                // appears at the bottom, so extending downward fills it
                // naturally instead of shifting existing bars.
                let insert_pos = self.slots.len() - usize::from(self.has_overall);
                for _ in 0..(desired_cap - current_cap) {
                    let pb = ProgressBar::new(0);
                    let bar = with_slot_finish_policy(self.inner.insert(insert_pos, pb));
                    let cache = SlotCache::new();
                    blank_new_bar(&bar, &cache);
                    let slot = RenderedSlot { bar, source: RefCell::new(None), cache };
                    if let Some(orphan) = self.orphaned_states.borrow_mut().pop_back() {
                        slot.source.replace(Some(orphan));
                        // The reattach is a rebind: the row may be drawn again
                        // even when the reattached source was cleared.
                        slot.cache.cleared_blank_held.set(false);
                    }
                    self.slots.insert(insert_pos, slot);
                    self.slots_timing.insert(insert_pos, SlotTiming::new(&*self.time_source));
                }
                // Sync slots that may have been reattached.
                for i in 0..self.slots.len() {
                    self.sync_slot(i);
                }
            } else if desired_cap < current_cap {
                changed = true;
                // Shrink: evict from top until desired capacity is met.
                while self.slots.len() > desired_cap
                    && self.slots.len().saturating_sub(usize::from(self.has_overall)) > 0
                {
                    if let Some(source) = self.slots[0].source.borrow_mut().take() {
                        self.orphaned_states.borrow_mut().push_back(source);
                    }
                    self.inner.remove(&self.slots[0].bar);
                    self.slots.remove(0);
                    self.slots_timing.remove(0);
                }
            }
        }
        changed
    }

    /// Remove blank (unbound) reserved slots from [`MultiProgress`] and
    /// trigger a final draw so that only the non-blank finished bars
    /// remain visible in the terminal and in scrollback.
    ///
    /// This is intended as a replacement for [`MultiProgress::clear`]
    /// when the caller wants the final state of progress bars to
    /// persist in scrollback without empty reserved lines.
    ///
    /// The commit is completed by advancing the cursor past the frame
    /// ([`WriteGate::commit_frame`]): the draw protocol alone leaves the
    /// cursor on the frame's last row, which would leave the committed frame
    /// inside the next screen's band instead of below it.
    ///
    /// Safe to call multiple times — only the first call has any effect.
    pub(crate) fn finalize(&self) {
        if self.finalized.replace(true) {
            return;
        }
        // Pre-roll is deliberately absent here: the renderer does not own it.
        //
        // Pre-roll fires once per `ProgressTerminal`, from `build_screen`,
        // before the first bar of that terminal's first screen draws — and a
        // `ProgressTerminal` is the only way to build a screen, so every
        // on-screen bar gets the scroll of existing terminal content it needs
        // before its first frame. Do not add a renderer-side pre-roll fallback:
        // one pre-roll owner is the point of the split.
        // RAII guard: buffer OFF during final draw, re-enabled on drop.
        let _guard = self.gate.open();
        // Finish all bound bars that have reached a terminal state:
        // sync their final state FIRST (so position/total/elapsed/suffix
        // is up-to-date), then call finish_slot which applies the done
        // visual style.
        for (i, slot) in self.slots.iter().enumerate() {
            let snap = slot.source.borrow().as_ref().map(|s| s.snapshot());
            if let Some(ref snap) = snap
                && snap.status != TrackStatus::Active
            {
                // Routed through the same helper the measurement pass uses, so
                // this terminal sync cannot drift from what the budget
                // reserved: for a terminal row the helper answers
                // `(None, None)`, which is exactly what this loop passed by
                // hand before the helper existed.
                let timing = timing_strings(snap, &self.slots_timing[i], &*self.time_source);
                self.sync_snapshot_to_bar(
                    i,
                    snap,
                    timing.rate_str.as_deref(),
                    timing.eta_str.as_deref(),
                );
                self.finish_slot(i, snap.status);
            }
        }
        // Remove all blank (unbound) slots from MultiProgress.
        for slot in &self.slots {
            if slot.source.borrow().is_none() {
                self.inner.remove(&slot.bar);
            }
        }

        // Trigger one final draw with the reduced bar set, and record whether
        // that draw happened: it is what the cursor advance below is
        // conditional on.
        let mut drew_frame = false;
        for slot in &self.slots {
            if slot.source.borrow().is_some() {
                slot.bar.tick();
                drew_frame = true;
                break;
            }
        }

        // Hand the frame to the terminal for good: the final draw leaves the
        // cursor on the frame's last row, so without this the frame is still
        // the live region and the next screen's frame claims its rows (see
        // `WriteGate::commit_frame`). Its caller contract puts it inside an
        // open write window — the `_guard` opened above holds one for the whole
        // of this method — because the write bypasses the gate and is the one
        // write of the commit that must not be suppressed.
        //
        // The advance is conditional on this screen having drawn a frame, and
        // that is not the obvious reading of "nothing was committed, so commit
        // nothing": the advance is not part of committing, it is compensation
        // for the draws the gate discarded while the frame was in progress (the
        // released draw walks the cursor to the frame's last row; a discarded
        // one does not).
        //
        // The condition is "a slot is bound NOW", which is what `drew_frame`
        // measures above, and a bound slot does imply at least one released
        // draw: `add_bar` writes the bar's first frame. It is not equivalent to
        // "this screen left no frame on the terminal". The two coincide only
        // for a screen that never bound a bar, where no draw reached the gate,
        // so no frame of this screen is on the terminal and the cursor is not
        // resting inside one; advancing there would write a blank row the
        // screen never drew.
        //
        // Known, accepted gap (public API only, cosmetic): a screen that binds
        // a bar, draws it, then has it evicted by a height shrink leaves that
        // frame on the terminal while `drew_frame` reads false at `finalize`,
        // so the advance is skipped and the next screen reclaims the row.
        // Reachable through the public builder with no overall bar,
        // `dynamic_height(true)`, and a dimension source whose row count grows
        // past the drawn slot and then shrinks back — the shrink evicts from
        // the top of the band, so the blank reserved slots above the drawn bar
        // go first and the drawn bar is orphaned only once they are gone. No
        // in-repo screen is measured to reach it: every production screen built
        // from a dynamic-height terminal registers an overall bar
        // (`mediapm-conductor/src/cli.rs:291`,
        // `mediapm/src/service.rs:866`/`1221`/`1301`/`1423`), whose slot is
        // still bound here, and the no-overall test screens that shrink their
        // injected source never join with an empty band. The consequence is a
        // stale row of a bar this screen has already orphaned being reclaimed,
        // which is why it is accepted rather than fixed. Any future fix must
        // keep the never-bound case, which
        // `gated_screen_without_bars_commits_nothing` pins.
        if drew_frame {
            self.gate.commit_frame();
        }
    }
}

// The test bodies live beside this file rather than under a `renderer/`
// directory, so each declaration names its own path. This is the same shape
// the progress examples use to reach their `support/` siblings.
#[cfg(test)]
#[path = "tests_blank_slot_draws.rs"]
mod tests_blank_slot_draws;
#[cfg(test)]
#[path = "tests_client_truncated_prefix.rs"]
mod tests_client_truncated_prefix;
#[cfg(test)]
#[path = "tests_layout_column_budget.rs"]
mod tests_layout_column_budget;
