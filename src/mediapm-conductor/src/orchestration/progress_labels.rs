//! Client-defined bar-label truncation for conductor workflow progress.
//!
//! mediapm-utils owns the render push point but not the field layout. The
//! two structs here carry conductor's own semantically-named fields and
//! implement [`BarLabelTruncation`] with their own order. A step bar
//! (`StepBarLabel`) carries real-progress fields (`version`, `completed`/
//! `total`, `phase`); a worker-slot bar (`WorkerBarLabel`) carries only an
//! `activity` flag (`` `active` ``/`` `idle` ``) and never a workflow phase or progress
//! tally.

use mediapm_utils::progress::BarLabelTruncation;

/// Truncation order for a per-step (real-progress) bar.
///
/// Prefix order: `version` → `completed`/`total` → `phase` → `status_marker` →
/// `workflow_id` → `step_id` → `tool` → fallback. Suffix order: `custom` → `eta` →
/// `rate` → `elapsed` → `completed`/`total` → fallback.
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
    fn prefix_parts(&self) -> Vec<String> {
        let mut parts = Vec::new();
        if !self.version.is_empty() {
            parts.push(format!("[{}]", self.version));
        }
        if !self.completed.is_empty() && !self.total.is_empty() {
            parts.push(format!("{}/{}", self.completed, self.total));
        }
        if !self.phase.is_empty() {
            parts.push(format!("[{}]", self.phase));
        }
        if !self.status_marker.is_empty() {
            parts.push(format!("[{}]", self.status_marker));
        }
        if !self.workflow_id.is_empty() {
            parts.push(self.workflow_id.clone());
        }
        if !self.step_id.is_empty() {
            parts.push(self.step_id.clone());
        }
        if !self.tool.is_empty() {
            parts.push(format!("({})", self.tool));
        }
        parts
    }

    fn suffix_parts(&self, eta: &str, rate: &str, elapsed: &str, custom: &str) -> Vec<String> {
        let mut parts = Vec::new();
        if !custom.is_empty() {
            parts.push(custom.to_string());
        }
        if !eta.is_empty() {
            parts.push(format!("eta {eta}"));
        }
        if !rate.is_empty() {
            parts.push(format!("rate {rate}"));
        }
        if !elapsed.is_empty() {
            parts.push(elapsed.to_string());
        }
        if !self.completed.is_empty() && !self.total.is_empty() {
            parts.push(format!("{}/{}", self.completed, self.total));
        }
        parts
    }
}

#[cfg(feature = "progress")]
impl BarLabelTruncation for StepBarLabel {
    fn truncate_prefix(&self, max_width: usize) -> String {
        let parts = self.prefix_parts();
        truncate_ordered(&parts, max_width)
    }

    fn truncate_suffix(&self, max_width: usize) -> String {
        let parts = self.suffix_parts("", "", "", "");
        truncate_ordered(&parts, max_width)
    }
}

/// Truncation order for a worker-slot (activity) bar.
///
/// Prefix order: `workflow_id` → `step_id` → `tool` → `activity` → `status_marker` →
/// fallback. Suffix order: `custom` → `eta` → `rate` → `elapsed` → fallback. A
/// worker has no workflow phase and no progress tally, so those fields are
/// absent by construction.
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
    fn prefix_parts(&self) -> Vec<String> {
        let mut parts = Vec::new();
        if !self.workflow_id.is_empty() {
            parts.push(self.workflow_id.clone());
        }
        if !self.step_id.is_empty() {
            parts.push(self.step_id.clone());
        }
        if !self.tool.is_empty() {
            parts.push(format!("({})", self.tool));
        }
        if !self.activity.is_empty() {
            parts.push(format!("[{}]", self.activity));
        }
        if !self.status_marker.is_empty() {
            parts.push(format!("[{}]", self.status_marker));
        }
        parts
    }

    fn suffix_parts(eta: &str, rate: &str, elapsed: &str, custom: &str) -> Vec<String> {
        let mut parts = Vec::new();
        if !custom.is_empty() {
            parts.push(custom.to_string());
        }
        if !eta.is_empty() {
            parts.push(format!("eta {eta}"));
        }
        if !rate.is_empty() {
            parts.push(format!("rate {rate}"));
        }
        if !elapsed.is_empty() {
            parts.push(elapsed.to_string());
        }
        parts
    }
}

#[cfg(feature = "progress")]
impl BarLabelTruncation for WorkerBarLabel {
    fn truncate_prefix(&self, max_width: usize) -> String {
        let parts = self.prefix_parts();
        truncate_ordered(&parts, max_width)
    }

    fn truncate_suffix(&self, max_width: usize) -> String {
        let parts = Self::suffix_parts("", "", "", "");
        truncate_ordered(&parts, max_width)
    }
}

/// Join `parts` with a single space, dropping trailing parts until the
/// joined string fits `max_width` visible columns. When the first part
/// alone exceeds the budget, it is hard-truncated to fit.
#[cfg(feature = "progress")]
fn truncate_ordered(parts: &[String], max_width: usize) -> String {
    let mut kept: Vec<&String> = Vec::new();
    let mut width = 0usize;
    for part in parts {
        let add = if kept.is_empty() { part.len() } else { part.len() + 1 };
        if width + add > max_width && !kept.is_empty() {
            break;
        }
        // Hard-truncate the first part if it alone exceeds the budget.
        if kept.is_empty() && add > max_width {
            kept.push(part);
            break;
        }
        width += add;
        kept.push(part);
    }
    let result: String = kept.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(" ");
    // Safety-net: chars().take handles non-ASCII edge cases.
    if result.len() > max_width { result.chars().take(max_width).collect() } else { result }
}
