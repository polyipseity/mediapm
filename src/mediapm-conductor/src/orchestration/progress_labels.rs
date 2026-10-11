//! Client-defined bar-label truncation for conductor workflow progress.
//!
//! mediapm-utils owns the render push point but not the field layout. The
//! two structs here carry conductor's own semantically-named fields and
//! implement [`BarLabelTruncation`] with their own allocation policy.
//! [`StepBarLabel`] labels the workflow example's per-step row: it names the
//! workflow, the step running on it, and the tool's declared version. The
//! coordinator no longer installs it at all — the pinned overall row is a
//! static seed — and its tally is filled only by a bar that tracks outputs;
//! [`WorkerBarLabel`] labels one pool slot, carrying an `activity` marker
//! (`active`/`idle`) alongside its identifiers and tool name, and never a
//! tally.
//!
//! The prefix allocation decouples survival rank from display order. The
//! status marker is reserved first and always drawn whole; the tool is sized
//! next at its natural rendered width because it outranks both names;
//! workflow and step split what remains down the middle (each cut from the
//! front so its tail survives, a name needing less than its half handing the
//! unused columns to the other); and the activity tag is drawn only from
//! columns left after those three, so it yields first. The tool gives
//! columns back only when a name would otherwise fall below `NAME_FLOOR`.
//! Display order is unchanged: `marker workflow step (tool) activity`.
//!
//! A client label also replaces the built-in prefix components outright, so a
//! label that wants a status marker has to carry one.

use mediapm_utils::progress::{
    BarLabelTruncation, Brackets, Segment, SuffixComponents, fit_segments, front_tail,
};

/// Columns a name is held at before the tool gives any of its own back.
///
/// A name shorter than this says nothing about which workflow or step the
/// row names, and the tool — which outranks both names — donates columns
/// until each present name reaches the floor or the tool has nothing left.
/// A name whose own natural width is below the floor is floored at that
/// natural width: there is nothing more of the name to hold.
#[cfg(feature = "progress")]
const NAME_FLOOR: usize = 4;

/// Allocation policy for the overall workflow bar's label, and for a bar
/// that tracks a step's outputs.
///
/// Fields are displayed `marker workflow step (tool) version`, but survival
/// rank is decoupled from that order: the status marker is reserved first
/// and always drawn whole; the tool, with the version inside its bracket
/// group, is sized next at its natural rendered width; workflow and step
/// split what remains down the middle (front-cut, keep tail, with a name
/// needing less than its half handing the unused columns to the other), and
/// the tool gives columns back only when a name would otherwise fall below
/// `NAME_FLOOR`. The version yields head-columns first inside the group,
/// and a shortened version takes the pair's brackets down with it. The
/// suffix leads with the tally, then `elapsed`, `rate`, `eta`, and an
/// elastic `custom`.
///
/// The allocation lives in `allocate_prefix`, shared with
/// [`WorkerBarLabel`].
#[cfg(feature = "progress")]
#[derive(Debug, Clone)]
pub struct StepBarLabel {
    /// Terminal-state marker, rendered bracketed as `[F]` or `[W]`. Empty
    /// when the step has no status, in which case the segment is omitted.
    /// Reserved first: a row too narrow to carry it carries nothing.
    pub status_marker: String,
    /// Name of the workflow the step belongs to, e.g. `"default"`.
    /// Displayed ahead of the step, but the two split width evenly rather
    /// than by display order, so a long workflow id can no longer evict the
    /// step: under pressure the name is cut from the front and its tail
    /// survives.
    pub workflow_id: String,
    /// Step identifier within the workflow, e.g. `"s3"`. Splits width
    /// evenly with the workflow name, cut from the front like it.
    pub step_id: String,
    /// Conductor tool name, rendered parenthesized beside the version as
    /// `(ffmpeg v7.1)`, or on its own as `(ffmpeg)` when there is no version.
    /// Sized at its natural rendered width before the names are split,
    /// because the tool outranks both; it shrinks from the front — giving
    /// columns to the names — only when a name would otherwise fall below
    /// `NAME_FLOOR`, and its parentheses are the group's decoration rather
    /// than part of the text, so a shortened name renders bare.
    pub tool: String,
    /// Tool version, rendered verbatim as `v7.1`, with no brackets of its
    /// own: it is the second member of the tool name's bracket group, so the
    /// parentheses around the two come from the name and not from here. The
    /// only head-keeping field: the columns it gives back come off the end,
    /// so a narrowed row reads `v7.` instead of a tail that names nothing.
    /// It trails the tool name, so it yields before the name is touched,
    /// and a version that had to shorten takes the pair down with it.
    ///
    /// Filled only by the workflow example, which is the sole caller of this
    /// label since the coordinator stopped installing it: the overall row is
    /// a static seed now, so production renders no version on this screen and
    /// the example's step rows are the only place `Shrink::Head` has anything
    /// to cut.
    pub version: String,
    /// Completed count for the progress tally, rendered as
    /// `{completed}/{total}` in the suffix. Names belong in the prefix and
    /// counts in the suffix, so this field has no prefix rendering. The
    /// coordinator never installs this label — the overall bar counts steps
    /// through its fill rather than through a tally — so only the example's
    /// step rows fill a tally here.
    pub completed: String,
    /// Total count for the progress tally. Rendered only when `completed`
    /// is non-empty.
    pub total: String,
}

#[cfg(feature = "progress")]
impl StepBarLabel {
    /// Build the tool bracket group: the tool name alone when there is no
    /// version, the name and the version as one two-member group when there
    /// is, so [`fit_segments`] keeps the parentheses whole-or-not around
    /// whichever members survive.
    ///
    /// The name is [`Shrink::Front`](mediapm_utils::progress::Shrink::Front)
    /// (its tail is the informative end) and the version is head-keeping,
    /// because a version numbers itself from the left: `v7.1` at two columns
    /// reads `v7`, where a clip that kept the tail could yield nothing at
    /// all, since the value holds no boundary to cut after.
    fn tool_segments(&self) -> Vec<Segment> {
        if self.tool.is_empty() {
            return Vec::new();
        }
        if self.version.is_empty() {
            vec![Segment::elastic(self.tool.clone()).brackets(Brackets::Round)]
        } else {
            vec![
                Segment::elastic(self.tool.clone()).spanning_brackets(Brackets::Round, 2),
                Segment::elastic_head(self.version.clone()),
            ]
        }
    }

    /// Rendered width of [`Self::tool_segments`] when nothing has been cut.
    ///
    /// Measured by fitting the group at an unbounded width rather than by
    /// re-deriving the bracket and separator arithmetic, so the number the
    /// allocation budgets with is the number the fitter will draw.
    fn tool_natural_width(&self) -> usize {
        fit_segments(&self.tool_segments(), usize::MAX).chars().count()
    }

    /// Build the suffix segments, most important first.
    ///
    /// The tally leads, because it is the only count the bar has and the
    /// renderer supplies one of its own alongside the timing fields, so the
    /// label has to render exactly one of them. A label with no tally of its
    /// own falls back to the renderer's derived `count`/`total`, which is what
    /// the overall workflow bar shows: it counts steps through its fill rather
    /// than through a tally, and a suffix that ignored the derived pair would
    /// take `4/12` off the row.
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
        } else if !suffix.count.is_empty() && !suffix.total.is_empty() {
            segs.push(Segment::keep(format!("{}/{}", suffix.count, suffix.total)));
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
        allocate_prefix(
            &self.status_marker,
            &self.workflow_id,
            &self.step_id,
            &self.tool_segments(),
            self.tool_natural_width(),
            "",
            max_width,
        )
    }

    fn truncate_suffix(&self, max_width: usize, suffix: &SuffixComponents) -> String {
        fit_segments(&self.suffix_segments(suffix), max_width)
    }
}

/// Allocation policy for a worker-slot (activity) bar.
///
/// Same allocation as [`StepBarLabel`] (see `allocate_prefix`): marker
/// reserved, tool sized, names split evenly, and the `activity` tag drawn
/// only from columns left after those three, so it yields first — matching
/// the tail-drop behavior it replaces, where the tag was the last segment
/// and the first to go. A worker carries no workflow phase and no progress
/// tally, so the suffix has no tally segment; the auto-derived `elapsed`,
/// `rate`, and `eta` still render, ahead of an elastic `custom`.
#[cfg(feature = "progress")]
#[derive(Debug, Clone)]
pub struct WorkerBarLabel {
    /// Terminal-state marker, rendered bracketed as `[F]` or `[W]`. Empty
    /// when the slot has no status, in which case the segment is omitted.
    /// Reserved first: a row too narrow to carry it carries nothing.
    pub status_marker: String,
    /// Workflow the slot is executing, e.g. `"default"`. Empty unless the
    /// slot is running a step. Splits width evenly with [`Self::step_id`],
    /// cut from the front so its tail survives.
    pub workflow_id: String,
    /// Step the slot is executing, e.g. `"s5"`. Empty unless the slot is
    /// running a step. Splits width evenly with [`Self::workflow_id`], cut
    /// from the front like it.
    pub step_id: String,
    /// Conductor tool name, rendered parenthesized as `(echo)`. Sized at its
    /// natural rendered width before the names are split, because the tool
    /// outranks both; it shrinks from the front only when a name would
    /// otherwise fall below `NAME_FLOOR`.
    pub tool: String,
    /// Worker state marker, rendered bracketed as `[active]` or `[idle]`. It
    /// trails the identifiers and is drawn only from columns left after the
    /// marker, tool and names, so a narrowed row gives up the tag before it
    /// gives up what is running.
    pub activity: String,
}

#[cfg(feature = "progress")]
impl WorkerBarLabel {
    /// Build the tool bracket group: the tool name inside round brackets,
    /// the one-member case [`fit_segments`] already draws.
    ///
    /// The name is elastic with its tail kept, because a shortened name's
    /// informative end is its end; its parentheses are decoration on the
    /// segment, not text inside it, so a clipped name renders bare rather
    /// than still wearing its brackets.
    fn tool_segments(&self) -> Vec<Segment> {
        if self.tool.is_empty() {
            return Vec::new();
        }
        vec![Segment::elastic(self.tool.clone()).brackets(Brackets::Round)]
    }

    /// Rendered width of [`Self::tool_segments`] when nothing has been cut.
    ///
    /// Measured by fitting the group at an unbounded width rather than by
    /// re-deriving the bracket arithmetic, so the number the allocation
    /// budgets with is the number the fitter will draw.
    fn tool_natural_width(&self) -> usize {
        fit_segments(&self.tool_segments(), usize::MAX).chars().count()
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
        allocate_prefix(
            &self.status_marker,
            &self.workflow_id,
            &self.step_id,
            &self.tool_segments(),
            self.tool_natural_width(),
            &self.activity,
            max_width,
        )
    }

    fn truncate_suffix(&self, max_width: usize, suffix: &SuffixComponents) -> String {
        fit_segments(&Self::suffix_segments(suffix), max_width)
    }
}

/// Rendered width of a marker or tag rendered inside square brackets, or
/// zero for the empty string (an absent field contributes nothing).
#[cfg(feature = "progress")]
fn bracketed_width(field: &str) -> usize {
    if field.is_empty() { 0 } else { field.chars().count() + 2 }
}

/// Columns a present name is missing after its even-split allotment, capped
/// at its [`NAME_FLOOR`]: the columns the tool has to donate for the name to
/// say something. An absent name donates nothing and needs nothing.
#[cfg(feature = "progress")]
fn name_deficit(allotment: usize, natural: usize) -> usize {
    NAME_FLOOR.min(natural).saturating_sub(allotment)
}

/// Split `budget` columns between the workflow and step names.
///
/// Each present name is held at the lesser of its natural width and its
/// share; the extra column of an odd split goes to the longer name (ties to
/// the workflow, the first name in display order); and a name that needs
/// less than its share hands the unused columns to the other name when the
/// other still needs them. Returns `(workflow, step)` column allotments;
/// an absent name is allotted zero.
#[cfg(feature = "progress")]
fn split_names(
    budget: usize,
    workflow_len: usize,
    step_len: usize,
    has_workflow: bool,
    has_step: bool,
) -> (usize, usize) {
    match (has_workflow, has_step) {
        (false, false) => (0, 0),
        (true, false) => (budget.min(workflow_len), 0),
        (false, true) => (0, budget.min(step_len)),
        (true, true) => {
            let base_workflow =
                budget / 2 + usize::from(budget % 2 == 1 && workflow_len >= step_len);
            let base_step = budget - base_workflow;
            let workflow = base_workflow.min(workflow_len);
            let step = base_step.min(step_len);
            let unused = budget - workflow - step;
            match (
                workflow_len > workflow && step_len <= step,
                step_len > step && workflow_len <= workflow,
            ) {
                (true, _) => (workflow + unused, step),
                (_, true) => (workflow, step + unused),
                _ => (workflow, step),
            }
        }
    }
}

/// Assemble a bar-label prefix under the allocation policy.
///
/// Survival rank is decoupled from display order. The status marker is
/// reserved first and always drawn whole — a row too narrow to carry it
/// carries nothing; the tool (already [`fit_segments`]-prepared as `tool`
/// segments, its natural width in `tool_natural`) is sized next, because the
/// tool outranks both names; workflow and step split what remains down the
/// middle, each front-cut so its tail survives, a name needing less than
/// its half handing the unused columns to the other; and `activity` is
/// drawn only from columns left after those three, whole or not at all, so
/// it yields first. The tool shrinks — front-cut, its brackets dropped with
/// the clip — only while a name is below [`NAME_FLOOR`], then at its plain
/// fit so the row never exceeds `width`.
///
/// Both fields being cut from the front makes the row's length monotone in
/// `width`: a narrower slot never draws a longer row, which is what stops a
/// resizing terminal from reading as jitter.
#[cfg(feature = "progress")]
fn allocate_prefix(
    status_marker: &str,
    workflow: &str,
    step: &str,
    tool: &[Segment],
    tool_natural: usize,
    activity: &str,
    width: usize,
) -> String {
    let marker_w = bracketed_width(status_marker);
    let activity_w = bracketed_width(activity);
    let has_workflow = !workflow.is_empty();
    let has_step = !step.is_empty();
    let has_tool = !tool.is_empty();
    let named = usize::from(marker_w > 0)
        + usize::from(has_workflow)
        + usize::from(has_step)
        + usize::from(has_tool);
    let separators = named.saturating_sub(1);
    if marker_w > width {
        return String::new();
    }
    let workflow_len = workflow.chars().count();
    let step_len = step.chars().count();
    let tool_cap = width.saturating_sub(marker_w + separators);
    let mut tool_w = tool_natural.min(tool_cap);
    let (workflow_w, step_w) = loop {
        let names_budget = width.saturating_sub(marker_w + separators + tool_w);
        let allotment = split_names(names_budget, workflow_len, step_len, has_workflow, has_step);
        let deficit = name_deficit(allotment.0, workflow_len) + name_deficit(allotment.1, step_len);
        if deficit == 0 || tool_w == 0 {
            break allotment;
        }
        tool_w = tool_w.saturating_sub(deficit);
    };
    // A name the tool could not lift to its floor says nothing at one or
    // two columns, and a row carrying it reads as empty while the suffix
    // that carries the timing loses its columns — the shape the
    // `assert_narrow_rows_carry_a_label_or_a_count` sweep rejects. Drop the
    // name instead of drawing a fragment; a name short enough to fit whole
    // below the floor keeps its natural width.
    let floor_workflow = if workflow_w < NAME_FLOOR.min(workflow_len) { 0 } else { workflow_w };
    let floor_step = if step_w < NAME_FLOOR.min(step_len) { 0 } else { step_w };
    let mut parts: Vec<String> = Vec::new();
    if marker_w > 0 {
        parts.push(format!("[{status_marker}]"));
    }
    if floor_workflow > 0 {
        parts.push(front_tail(workflow, floor_workflow));
    }
    if floor_step > 0 {
        parts.push(front_tail(step, floor_step));
    }
    let tool_text = if tool_w == 0 { String::new() } else { fit_segments(tool, tool_w) };
    if !tool_text.is_empty() {
        parts.push(tool_text);
    }
    let mut row = parts.join(" ");
    let used = row.chars().count();
    if activity_w > 0 {
        let room = if parts.is_empty() { width } else { width.saturating_sub(used + 1) };
        if activity_w <= room {
            if !row.is_empty() {
                row.push(' ');
            }
            row.push('[');
            row.push_str(activity);
            row.push(']');
        }
    }
    row
}

/// Allocation-policy tests: a running row shows workflow AND step under
/// space pressure, the tool outranks both names, the activity tag yields
/// first, and idle rows keep their exact shapes. Each test pins one clause
/// of the policy in [`allocate_prefix`] at a width chosen so the clause is
/// the only thing that can make the assertion hold.
#[cfg(all(test, feature = "progress"))]
mod allocation_policy_tests {
    use super::{StepBarLabel, WorkerBarLabel};
    use mediapm_utils::progress::BarLabelTruncation;

    /// A step label whose workflow and step both overflow their half of the
    /// row splits the remainder down the middle — the user-facing guarantee
    /// that a long workflow name can no longer evict the step. Each name is
    /// cut from the front so its tail survives, and the tool comes through
    /// whole because it outranks both names.
    #[test]
    fn step_label_splits_both_names_evenly_when_both_overflow() {
        let label = StepBarLabel {
            status_marker: String::new(),
            workflow_id: "mediapm.media.workflow-alpha".into(),
            step_id: "s12-transform".into(),
            tool: "ffmpeg".into(),
            version: String::new(),
            completed: String::new(),
            total: String::new(),
        };
        let out = label.truncate_prefix(20);
        assert_eq!(out, "alpha sform (ffmpeg)");
        assert_eq!(out.chars().count(), 20, "the split must fill the row exactly: {out:?}");
    }

    /// A name needing less than its half hands the unused columns to the
    /// other name (flex): the short step stays whole and the long workflow
    /// gets everything its neighbour could not use.
    #[test]
    fn step_label_hands_the_unused_step_columns_to_the_workflow() {
        let label = StepBarLabel {
            status_marker: String::new(),
            workflow_id: "mediapm.media.workflow-alpha".into(),
            step_id: "s1".into(),
            tool: "echo".into(),
            version: String::new(),
            completed: String::new(),
            total: String::new(),
        };
        assert_eq!(label.truncate_prefix(20), "flow-alpha s1 (echo)");
    }

    /// A long workflow id is cut from the front so its tail survives: the
    /// row keeps the part of the name that tells workflow ids apart rather
    /// than a head that names nothing.
    #[test]
    fn step_label_cuts_the_workflow_id_from_the_front() {
        let label = StepBarLabel {
            status_marker: String::new(),
            workflow_id: "mediapm.media.this-workflow".into(),
            step_id: "s2".into(),
            tool: "echo".into(),
            version: String::new(),
            completed: String::new(),
            total: String::new(),
        };
        let out = label.truncate_prefix(14);
        assert_eq!(out, "flow s2 (echo)");
        assert!(!out.contains("mediapm"), "the cut must remove the head, not the tail: {out:?}");
    }

    /// The tool is sized at its natural width before the names are split, so
    /// it comes through whole (brackets included) at a width where both
    /// names must shrink.
    #[test]
    fn step_label_keeps_the_tool_whole_while_the_names_shrink() {
        let label = StepBarLabel {
            status_marker: String::new(),
            workflow_id: "mediapm.media.workflow-alpha".into(),
            step_id: "s12-transform".into(),
            tool: "echo".into(),
            version: String::new(),
            completed: String::new(),
            total: String::new(),
        };
        assert_eq!(label.truncate_prefix(16), "lpha form (echo)");
    }

    /// The 4-column name floor shrinks the tool: at this width the names
    /// could hold only two columns each while the tool sat at its natural
    /// width, so the tool gives columns back until each name says something,
    /// and the shortened name drops its brackets with its clip.
    #[test]
    fn step_label_shrinks_the_tool_to_hold_the_name_floor() {
        let label = StepBarLabel {
            status_marker: String::new(),
            workflow_id: "mediapm.media.workflow-alpha".into(),
            step_id: "s12-transform".into(),
            tool: "a-very-long-tool".into(),
            version: String::new(),
            completed: String::new(),
            total: String::new(),
        };
        let out = label.truncate_prefix(16);
        assert_eq!(out, "lpha form g-tool");
        assert!(!out.contains('('), "a shortened tool renders bare: {out:?}");
    }

    /// Idle and terminal worker rows keep their exact shapes at every width
    /// where they fit: the policy touches running rows, and `[idle]`,
    /// `[W] [idle]`, `[F] [idle]` are the states a reader already knows.
    #[test]
    fn idle_and_terminal_prefixes_are_unchanged() {
        for marker in ["", "W", "F"] {
            let label = WorkerBarLabel {
                status_marker: marker.into(),
                workflow_id: String::new(),
                step_id: String::new(),
                tool: String::new(),
                activity: "idle".into(),
            };
            let expected =
                if marker.is_empty() { "[idle]".to_string() } else { format!("[{marker}] [idle]") };
            assert_eq!(label.truncate_prefix(40), expected);
            // At the width the row fills exactly it still draws whole.
            assert_eq!(label.truncate_prefix(expected.chars().count()), expected);
            // One column narrower the activity tag yields (whole or nothing),
            // and below the marker's own width the row carries nothing.
            assert_eq!(
                label.truncate_prefix(expected.chars().count() - 1),
                if marker.is_empty() { String::new() } else { format!("[{marker}]") }
            );
        }
    }

    /// A running worker row splits workflow and step evenly and gives the
    /// activity tag nothing while names and tool are claiming columns: the
    /// tag is drawn only from columns left after the marker, tool and names.
    #[test]
    fn worker_label_sheds_the_activity_tag_after_the_names_and_tool() {
        let label = WorkerBarLabel {
            status_marker: String::new(),
            workflow_id: "mediapm.media.workflow-alpha".into(),
            step_id: "s12-transform".into(),
            tool: "ffmpeg".into(),
            activity: "active".into(),
        };
        // 26 columns: names split evenly 8/8 (no marker, so two
        // separators), tool whole, nothing left for the tag.
        assert_eq!(label.truncate_prefix(26), "ow-alpha ransform (ffmpeg)");
        // 60 columns: everything natural plus the tag, exactly full.
        assert_eq!(
            label.truncate_prefix(60),
            "mediapm.media.workflow-alpha s12-transform (ffmpeg) [active]"
        );
    }

    /// A tool the document declared no version for renders its name alone
    /// inside the parentheses.
    ///
    /// `builtin_id` names a registration and a document key names a payload,
    /// and neither is a claim a user wrote, so an empty version is a decision
    /// rather than a field nothing reads: the pair of tests below pins both
    /// halves side by side, which an assertion of one alone could not do.
    #[test]
    fn step_label_renders_a_builtin_tool_without_a_version() {
        let label = StepBarLabel {
            status_marker: String::new(),
            workflow_id: "default".into(),
            step_id: "s1".into(),
            tool: "echo@v1".into(),
            version: String::new(),
            completed: String::new(),
            total: String::new(),
        };
        assert_eq!(label.truncate_prefix(usize::MAX), "default s1 (echo@v1)");
    }

    /// A version the document declared renders inside the tool's parentheses,
    /// so the builtin's empty one above is a choice and not a dead field.
    #[test]
    fn step_label_renders_a_declared_version_inside_the_parentheses() {
        let label = StepBarLabel {
            status_marker: String::new(),
            workflow_id: "default".into(),
            step_id: "s1".into(),
            tool: "managed@v1".into(),
            version: "v9.9.9".into(),
            completed: String::new(),
            total: String::new(),
        };
        assert_eq!(label.truncate_prefix(usize::MAX), "default s1 (managed@v1 v9.9.9)");
    }
}
