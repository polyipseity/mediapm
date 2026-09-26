//! Client-defined bar-label truncation for conductor workflow progress.
//!
//! mediapm-utils owns the render push point but not the field layout. The
//! two structs here carry conductor's own semantically-named fields and
//! implement [`BarLabelTruncation`] with their own order. A step bar
//! ([`StepBarLabel`]) carries real-progress fields (`version`,
//! `completed`/`total`, `phase`); a worker-slot bar ([`WorkerBarLabel`])
//! carries an `activity` marker (`active`/`idle`) alongside its identifiers
//! and tool name, and never a workflow phase or progress tally.

use mediapm_utils::progress::{BarLabelTruncation, Segment, SuffixComponents, fit_segments};

/// Truncation order for a per-step (real-progress) bar.
///
/// Segments are ordered most important first and yield from the tail. The
/// prefix leads with `phase`, `status_marker`, and `completed`/`total`, so
/// width pressure sheds the `tool` name first, then the version and
/// identifiers, before the head. The `tool` name is elastic and is shortened
/// from the front rather than dropped whole. The suffix repeats the tally,
/// then `elapsed`, `rate`, `eta`, and an elastic `custom`.
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
    /// Conductor tool name, rendered parenthesized as `(ffmpeg)`. The only
    /// elastic prefix segment.
    pub tool: String,
    /// Tool version, rendered bracketed as `[7.1]`.
    pub version: String,
    /// Workflow phase tag, rendered bracketed as `[wf]`.
    pub phase: String,
    /// Completed count for the progress tally. Rendered only when both it
    /// and `total` are non-empty.
    pub completed: String,
    /// Total count for the progress tally. Rendered only when both it and
    /// `completed` are non-empty.
    pub total: String,
}

#[cfg(feature = "progress")]
impl StepBarLabel {
    /// Build the prefix segments, most important first.
    ///
    /// The three leading segments come first, so width pressure sheds the
    /// version and identifiers before the phase, status marker, or tally.
    fn prefix_segments(&self) -> Vec<Segment> {
        let mut segs = Vec::new();
        if !self.phase.is_empty() {
            segs.push(Segment::keep(format!("[{}]", self.phase)));
        }
        if !self.status_marker.is_empty() {
            segs.push(Segment::keep(format!("[{}]", self.status_marker)));
        }
        if !self.completed.is_empty() && !self.total.is_empty() {
            segs.push(Segment::keep(format!("{}/{}", self.completed, self.total)));
        }
        if !self.version.is_empty() {
            segs.push(Segment::keep(format!("[{}]", self.version)));
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
        segs
    }

    /// Build the suffix segments, most important first.
    ///
    /// Free-form `custom` text is elastic so a long user string is
    /// shortened from the front rather than cut at its head.
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
/// prefix leads with `status_marker` and `activity`, so width pressure sheds
/// the `tool` name first, then the identifiers, before the head. The `tool`
/// name is elastic and is shortened from the front rather than dropped whole.
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
    /// Worker state marker, rendered bracketed as `[active]` or `[idle]`.
    pub activity: String,
}

#[cfg(feature = "progress")]
impl WorkerBarLabel {
    /// Build the prefix segments, most important first.
    ///
    /// The two leading segments come first, so width pressure sheds the
    /// identifiers and tool name before the status marker or activity.
    fn prefix_segments(&self) -> Vec<Segment> {
        let mut segs = Vec::new();
        if !self.status_marker.is_empty() {
            segs.push(Segment::keep(format!("[{}]", self.status_marker)));
        }
        if !self.activity.is_empty() {
            segs.push(Segment::keep(format!("[{}]", self.activity)));
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
