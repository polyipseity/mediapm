//! Client-defined bar-label truncation for conductor workflow progress.
//!
//! mediapm-utils owns the render push point but not the field layout. The
//! two structs here carry conductor's own semantically-named fields and
//! implement [`BarLabelTruncation`] with their own order. A step bar
//! ([`StepBarLabel`]) carries real-progress fields (a version and a
//! `completed`/`total` tally); a worker-slot bar ([`WorkerBarLabel`]) carries
//! an `activity` marker (`active`/`idle`) alongside its identifiers and tool
//! name, and never a progress tally.
//!
//! Both labels put the status marker first and are walked from the tail, so the
//! last field is the first to go. A worker bar ends on its activity marker and
//! yields that tag before it shortens a tool name, which is the order
//! tool-sync already renders: `ffmpeg v7.1 [res]` names the thing before it
//! says what state the thing is in.

use mediapm_utils::progress::{BarLabelTruncation, Segment, SuffixComponents, fit_segments};

/// Truncation order for a per-step (real-progress) bar.
///
/// Segments are ordered most important first and yield from the tail. The
/// prefix ends with the version, so the version is what width pressure reaches
/// for first: it gives columns back from its end, and it drops whole before
/// the `tool` name ahead of it is shortened at all. Once neither can shrink
/// further the identifiers drop, and the status marker with them. The suffix
/// leads with the tally, then `elapsed`, `rate`, `eta`, and an elastic
/// `custom`.
///
/// The order is the whole mechanism. `fit_segments` drops from the tail
/// unconditionally, so a field survives by its position in the list.
#[cfg(feature = "progress")]
#[derive(Debug, Clone)]
pub struct StepBarLabel {
    /// Terminal-state marker, rendered bracketed as `[F]` or `[W]`. Empty
    /// when the step has no status, in which case the segment is omitted.
    pub status_marker: String,
    /// Name of the workflow the step belongs to, e.g. `"default"`.
    pub workflow_id: String,
    /// Step identifier within the workflow, e.g. `"s3"`.
    pub step_id: String,
    /// Conductor tool name, rendered parenthesized as `(ffmpeg)`. Elastic,
    /// so a narrow row shortens the name from its front before dropping it.
    pub tool: String,
    /// Tool version, rendered verbatim as `7.1`, with no brackets. The only
    /// head-keeping prefix segment: the columns it gives back come off the
    /// end, so a narrowed row reads `7.` instead of a tail that names
    /// nothing. It trails the tool name, so it yields before the name is
    /// touched.
    pub version: String,
    /// Completed count for the progress tally, rendered as
    /// `{completed}/{total}` in the suffix. Names belong in the prefix and
    /// counts in the suffix, so this field has no prefix rendering. Empty
    /// when the step has no tally.
    pub completed: String,
    /// Total count for the progress tally. Rendered only when `completed`
    /// is non-empty.
    pub total: String,
}

#[cfg(feature = "progress")]
impl StepBarLabel {
    /// Build the prefix segments, most important first.
    ///
    /// The status marker leads, so it is the last segment to go. A `[F]` that
    /// got clipped away would leave a failed row reading as a succeeded one,
    /// which is the one thing a row must not do.
    ///
    /// The identifiers are `Keep` rather than `Elastic` because clipping them
    /// destroys what identifies them: `default` clipped to `ult` names no
    /// workflow. The version is the exception, and it is the one head-keeping
    /// segment, because a version numbers itself from the left: `7.1` at two
    /// columns reads `7.`, where a clip that kept the tail could yield nothing
    /// at all, since the value holds no boundary to cut after.
    fn prefix_segments(&self) -> Vec<Segment> {
        let mut segs = Vec::new();
        if !self.status_marker.is_empty() {
            segs.push(Segment::keep(format!("[{}]", self.status_marker)));
        }
        if !self.workflow_id.is_empty() {
            segs.push(Segment::keep(self.workflow_id.clone()));
        }
        if !self.step_id.is_empty() {
            segs.push(Segment::keep(self.step_id.clone()));
        }
        if !self.tool.is_empty() {
            segs.push(Segment::elastic(format!("({})", self.tool)));
        }
        if !self.version.is_empty() {
            segs.push(Segment::elastic_head(self.version.clone()));
        }
        segs
    }

    /// Build the suffix segments, most important first.
    ///
    /// The tally leads, because it is the only count the step bar has and the
    /// renderer supplies one of its own alongside the timing fields, so the
    /// label has to render exactly one of them.
    ///
    /// Free-form `custom` text is elastic so a long user string is
    /// shortened from the front rather than cut at its head.
    ///
    /// `elapsed`, `rate`, and `eta` are auto-derived: they reach this
    /// builder only through the merged [`SuffixComponents`] the renderer
    /// passes in, so a suffix that stopped reading them would strip timing
    /// from every step bar. They are `Keep`, so width pressure drops them
    /// whole rather than shortening them, and `eta` is rendered only when
    /// `rate` is present.
    fn suffix_segments(&self, suffix: &SuffixComponents) -> Vec<Segment> {
        let mut segs = Vec::new();
        if !self.completed.is_empty() && !self.total.is_empty() {
            segs.push(Segment::keep(format!("{}/{}", self.completed, self.total)));
        }
        if !suffix.elapsed.is_empty() {
            segs.push(Segment::keep(suffix.elapsed.clone()));
        }
        if let Some(ref rate) = suffix.rate {
            segs.push(Segment::keep(rate.clone()));
        }
        if let Some(ref eta) = suffix.eta {
            segs.push(Segment::keep(eta.clone()));
        }
        if !suffix.custom.is_empty() {
            segs.push(Segment::elastic(suffix.custom.clone()));
        }
        segs
    }
}

#[cfg(feature = "progress")]
impl BarLabelTruncation for StepBarLabel {
    fn truncate_prefix(&self, max_width: usize) -> String {
        fit_segments(&self.prefix_segments(), max_width)
    }

    fn truncate_suffix(&self, max_width: usize, suffix: &SuffixComponents) -> String {
        fit_segments(&self.suffix_segments(suffix), max_width)
    }
}

/// Truncation order for a worker-slot (activity) bar.
///
/// Segments are ordered most important first and yield from the tail. The
/// prefix ends with the `tool` name and the `activity` marker. Under width
/// pressure the `tool` name shortens from the front first, and once it cannot
/// shrink further the trailing marker is dropped, then the identifiers, and
/// only then the status marker.
/// A worker carries no workflow phase and no progress tally, so the suffix
/// has no tally segment; the auto-derived `elapsed`, `rate`, and `eta` still
/// render, ahead of an elastic `custom`.
#[cfg(feature = "progress")]
#[derive(Debug, Clone)]
pub struct WorkerBarLabel {
    /// Terminal-state marker, rendered bracketed as `[F]` or `[W]`. Empty
    /// when the slot has no status, in which case the segment is omitted.
    pub status_marker: String,
    /// Workflow the slot is executing, e.g. `"default"`. Empty unless the
    /// slot is running a step.
    pub workflow_id: String,
    /// Step the slot is executing, e.g. `"s5"`. Empty unless the slot is
    /// running a step.
    pub step_id: String,
    /// Conductor tool name, rendered parenthesized as `(echo)`. The only
    /// elastic prefix segment.
    pub tool: String,
    /// Worker state marker, rendered bracketed as `[active]` or `[idle]`. It
    /// trails the identifiers so a narrowed row gives up the tag before it
    /// gives up the names of what is running.
    pub activity: String,
}

#[cfg(feature = "progress")]
impl WorkerBarLabel {
    /// Build the prefix segments, most important first.
    ///
    /// The status marker leads and the activity marker trails, so the
    /// trailing tag is the first segment dropped and the marker is never
    /// dropped. A `[F]` that got clipped away would leave a failed slot
    /// reading as an idle one, which is the one thing a row must not do.
    /// Tool-sync gives up its phase tag the same way.
    ///
    /// The identifiers are `Keep` rather than `Elastic` because clipping
    /// them destroys what identifies them: `default` clipped to `ult` names
    /// no workflow. The tool name is the one elastic segment, because its
    /// parenthesised tail is the informative end, so `(ffmpeg)` clipped to
    /// four columns reads `mpeg)`.
    fn prefix_segments(&self) -> Vec<Segment> {
        let mut segs = Vec::new();
        if !self.status_marker.is_empty() {
            segs.push(Segment::keep(format!("[{}]", self.status_marker)));
        }
        if !self.workflow_id.is_empty() {
            segs.push(Segment::keep(self.workflow_id.clone()));
        }
        if !self.step_id.is_empty() {
            segs.push(Segment::keep(self.step_id.clone()));
        }
        if !self.tool.is_empty() {
            segs.push(Segment::elastic(format!("({})", self.tool)));
        }
        if !self.activity.is_empty() {
            segs.push(Segment::keep(format!("[{}]", self.activity)));
        }
        segs
    }

    /// Build the suffix segments, most important first.
    ///
    /// Takes no `&self`: a worker slot has no suffix fields of its own, so
    /// nothing from the label can contribute to the suffix.
    ///
    /// A worker slot has no progress tally of its own, but that does not
    /// mean it has no timing information: the renderer passes its merged
    /// [`SuffixComponents`] to `truncate_suffix`, and the auto-derived
    /// `elapsed`, `rate`, and `eta` fields arrive there. They are rendered
    /// as `Segment::keep` so width pressure drops them whole rather than
    /// shortening them. Only free-form `custom` text is elastic.
    fn suffix_segments(suffix: &SuffixComponents) -> Vec<Segment> {
        let mut segs = Vec::new();
        if !suffix.elapsed.is_empty() {
            segs.push(Segment::keep(suffix.elapsed.clone()));
        }
        if let Some(ref rate) = suffix.rate {
            segs.push(Segment::keep(rate.clone()));
        }
        if let Some(ref eta) = suffix.eta {
            segs.push(Segment::keep(eta.clone()));
        }
        if !suffix.custom.is_empty() {
            segs.push(Segment::elastic(suffix.custom.clone()));
        }
        segs
    }
}

#[cfg(feature = "progress")]
impl BarLabelTruncation for WorkerBarLabel {
    fn truncate_prefix(&self, max_width: usize) -> String {
        fit_segments(&self.prefix_segments(), max_width)
    }

    fn truncate_suffix(&self, max_width: usize, suffix: &SuffixComponents) -> String {
        fit_segments(&Self::suffix_segments(suffix), max_width)
    }
}
