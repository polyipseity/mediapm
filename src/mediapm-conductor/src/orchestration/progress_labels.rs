//! Client-defined bar-label truncation for conductor workflow progress.
//!
//! mediapm-utils owns the render push point but not the field layout. The
//! two structs here carry conductor's own semantically-named fields and
//! implement [`BarLabelTruncation`] with their own order. A step bar
//! (`StepBarLabel`) carries real-progress fields (`version`, `completed`/
//! `total`, `phase`); a worker-slot bar (`WorkerBarLabel`) carries only an
//! `activity` flag (`` `active` ``/`` `idle` ``) and never a workflow phase or progress
//! tally.

use mediapm_utils::progress::{BarLabelTruncation, Segment, SuffixComponents, fit_segments};

/// Truncation order for a per-step (real-progress) bar.
///
/// Segments are ordered most important first and yield from the tail. The
/// prefix protects `phase`, `status_marker`, and `completed`/`total`; the
/// `tool` name is elastic and is shortened from the front before any
/// earlier segment is dropped. The suffix repeats the protected tally,
/// then `elapsed`, `rate`, `eta`, and an elastic `custom`.
#[cfg(feature = "progress")]
#[derive(Debug, Clone)]
pub struct StepBarLabel {
    pub status_marker: String,
    pub workflow_id: String,
    pub step_id: String,
    pub tool: String,
    pub version: String,
    pub phase: String,
    pub completed: String,
    pub total: String,
}

#[cfg(feature = "progress")]
impl StepBarLabel {
    /// Minimum prefix width at which the protected head still fits whole.
    ///
    /// Measured as the rendered width of `[wf] [F] 1/4`. Below this width
    /// protection lifts and the tail is dropped as a whole segment.
    pub const PREFIX_FLOOR: usize = 12;

    /// Minimum suffix width at which the protected tally still fits.
    pub const SUFFIX_FLOOR: usize = 3;

    /// Build the prefix segments, most important first.
    ///
    /// The three protected segments lead, so width pressure sheds the
    /// version and identifiers before the phase, status marker, or tally.
    fn prefix_segments(&self) -> Vec<Segment> {
        let mut segs = Vec::new();
        if !self.phase.is_empty() {
            segs.push(Segment::protected(format!("[{}]", self.phase)));
        }
        if !self.status_marker.is_empty() {
            segs.push(Segment::protected(format!("[{}]", self.status_marker)));
        }
        if !self.completed.is_empty() && !self.total.is_empty() {
            segs.push(Segment::protected(format!("{}/{}", self.completed, self.total)));
        }
        if !self.version.is_empty() {
            segs.push(Segment::new_keep(format!("[{}]", self.version)));
        }
        if !self.workflow_id.is_empty() {
            segs.push(Segment::new_keep(self.workflow_id.clone()));
        }
        if !self.step_id.is_empty() {
            segs.push(Segment::new_keep(self.step_id.clone()));
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
            segs.push(Segment::protected(format!("{}/{}", self.completed, self.total)));
        }
        if !suffix.elapsed.is_empty() {
            segs.push(Segment::new_keep(suffix.elapsed.clone()));
        }
        if let Some(ref rate) = suffix.rate {
            segs.push(Segment::new_keep(rate.clone()));
        }
        if let Some(ref eta) = suffix.eta {
            segs.push(Segment::new_keep(eta.clone()));
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
        fit_segments(&self.prefix_segments(), max_width, Self::PREFIX_FLOOR)
    }

    fn truncate_suffix(&self, max_width: usize, suffix: &SuffixComponents) -> String {
        fit_segments(&self.suffix_segments(suffix), max_width, Self::SUFFIX_FLOOR)
    }
}

/// Truncation order for a worker-slot (activity) bar.
///
/// Segments are ordered most important first and yield from the tail. The
/// prefix protects `status_marker` and `activity`; the `tool` name is
/// elastic and is shortened from the front before any earlier segment is
/// dropped. A worker carries no workflow phase and no progress tally, so no
/// suffix segment is protected and its floor is zero; the auto-derived
/// `elapsed`, `rate`, and `eta` still render, ahead of an elastic `custom`.
#[cfg(feature = "progress")]
#[derive(Debug, Clone)]
pub struct WorkerBarLabel {
    pub status_marker: String,
    pub workflow_id: String,
    pub step_id: String,
    pub tool: String,
    pub activity: String,
}

#[cfg(feature = "progress")]
impl WorkerBarLabel {
    /// Minimum prefix width at which the protected head still fits whole.
    ///
    /// Measured as the rendered width of `[F] [active]`. Below this width
    /// protection lifts and the tail is dropped as a whole segment.
    pub const PREFIX_FLOOR: usize = 12;

    /// Worker suffixes carry only free-form custom text, so no protected
    /// segment applies and the floor is zero.
    pub const SUFFIX_FLOOR: usize = 0;

    /// Build the prefix segments, most important first.
    ///
    /// The two protected segments lead, so width pressure sheds the
    /// identifiers and tool name before the status marker or activity.
    fn prefix_segments(&self) -> Vec<Segment> {
        let mut segs = Vec::new();
        if !self.status_marker.is_empty() {
            segs.push(Segment::protected(format!("[{}]", self.status_marker)));
        }
        if !self.activity.is_empty() {
            segs.push(Segment::protected(format!("[{}]", self.activity)));
        }
        if !self.workflow_id.is_empty() {
            segs.push(Segment::new_keep(self.workflow_id.clone()));
        }
        if !self.step_id.is_empty() {
            segs.push(Segment::new_keep(self.step_id.clone()));
        }
        if !self.tool.is_empty() {
            segs.push(Segment::elastic(format!("({})", self.tool)));
        }
        segs
    }

    /// Build the suffix segments, most important first.
    ///
    /// A worker slot has no progress tally of its own, but that does not
    /// mean it has no timing information: the renderer passes its merged
    /// [`SuffixComponents`] to `truncate_suffix`, and the auto-derived
    /// `elapsed`, `rate`, and `eta` fields arrive there. They are rendered
    /// as `new_keep` so width pressure drops them whole rather than
    /// shortening them. Only free-form `custom` text is elastic.
    fn suffix_segments(suffix: &SuffixComponents) -> Vec<Segment> {
        let mut segs = Vec::new();
        if !suffix.elapsed.is_empty() {
            segs.push(Segment::new_keep(suffix.elapsed.clone()));
        }
        if let Some(ref rate) = suffix.rate {
            segs.push(Segment::new_keep(rate.clone()));
        }
        if let Some(ref eta) = suffix.eta {
            segs.push(Segment::new_keep(eta.clone()));
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
        fit_segments(&self.prefix_segments(), max_width, Self::PREFIX_FLOOR)
    }

    fn truncate_suffix(&self, max_width: usize, suffix: &SuffixComponents) -> String {
        fit_segments(&Self::suffix_segments(suffix), max_width, Self::SUFFIX_FLOOR)
    }
}
