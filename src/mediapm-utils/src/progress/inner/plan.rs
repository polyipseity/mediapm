//! The frame as a value: one complete render plan per tick.
//!
//! Everything that reaches the terminal is computed here first, so the
//! writer in [`super::writer`] can emit exactly one repaint and a row can
//! never carry geometry from an older frame (spec §4.1).
//
// Nothing in this module is wired into the running renderer yet: the writer
// (`super::writer`) will call `FramePlan::render` and the frame settle will
// construct the rows. Until those land, every item here looks unused to the
// plain-lib build, so `dead_code` is allowed module-wide; remove this
// attribute when `FramePlan::render` has its production caller.
#![allow(dead_code)]

use super::{FRAME_OVERHEAD_COLUMNS, TICK_CHARS, visible_width};
use console::style;

/// Which color treatment a row renders with. Selected from the frame's
/// overall/child flag and the row's status, exactly as the four
/// `apply_*_bar_style` builders selected a template before the rewrite.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RenderStyle {
    /// The pinned aggregate bar (`syncing tools`, `running workflows`).
    Overall,
    /// A child row still working (fill in yellow, spinner in green).
    Active,
    /// A finished row (fill in green, spinner dimmed white).
    Done,
    /// A failed row (fill and spinner in red).
    Failed,
    /// A blank row: full-width spaces, no spinner, no fill.
    Blank,
}

/// Whether this row's spinner column cycles (spec R3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RowAnimation {
    /// The row carries in-flight work; its spinner advances each frame.
    Animate,
    /// The row is parked; it renders a fixed glyph. The reason is carried
    /// for the debug seam and for tests that assert *why* a row is still.
    Static(&'static str),
}

/// One row of a [`FramePlan`]: final strings plus the numbers the fill
/// derives from. Geometry (prefix/suffix/fill widths) lives on the plan,
/// never on the row, so no row can outlive a settle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlanRow {
    /// Prefix, already truncated to `prefix_w`.
    pub prefix: String,
    /// Message (timing/status), already truncated to `suffix_w`.
    pub message: String,
    /// Work completed, for the fill ratio.
    pub position: u64,
    /// Work total, for the fill ratio.
    pub total: u64,
    /// Color treatment.
    pub style: RenderStyle,
    /// Spinner policy for this row.
    pub animation: RowAnimation,
}

/// A complete frame: shared geometry plus the ordered rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FramePlan {
    /// Rows in screen order.
    pub rows: Vec<PlanRow>,
    /// Settled prefix width, shared by every row (spec F1).
    pub prefix_w: usize,
    /// Settled suffix width, shared by every row.
    pub suffix_w: usize,
    /// The settled fill width for the frame: the cells between the prefix
    /// and the message. Ignored when `draw_fill` is false, and the width a
    /// `RenderStyle::Blank` row pads across.
    pub fill_w: usize,
    /// Whether the frame is wide enough to draw fills at all.
    pub draw_fill: bool,
    /// Which spinner glyph this frame shows for [`RowAnimation::Animate`].
    pub spinner_index: usize,
}

impl FramePlan {
    /// Render every row into one terminal line each, in screen order.
    ///
    /// Pure: the plan carries the settled geometry, so no terminal access is
    /// needed and identical plans render identical lines. Color goes through
    /// [`console::style`], so enablement follows the same global switch
    /// indicatif's colors did (on for a TTY, off otherwise).
    pub(crate) fn render(&self) -> Vec<String> {
        self.rows.iter().map(|row| self.render_row(row)).collect()
    }

    /// Render one row into its final terminal line.
    ///
    /// Columns follow `frame_template` in `components`: spinner, prefix,
    /// optional fill, message, joined by single spaces. A `RenderStyle::Blank`
    /// row short-circuits to full-width spaces — the `blank_bar_style`
    /// contract: neither glyph nor fill may appear on a row that must read
    /// as empty.
    fn render_row(&self, row: &PlanRow) -> String {
        if row.style == RenderStyle::Blank {
            return " "
                .repeat(FRAME_OVERHEAD_COLUMNS + self.prefix_w + self.suffix_w + self.fill_w);
        }
        let spinner = self.render_spinner(row);
        let prefix = pad_right(&row.prefix, self.prefix_w);
        let message = pad_left(&row.message, self.suffix_w);
        if self.draw_fill {
            let fill = self.render_fill(row);
            format!("{spinner} {prefix} {fill} {message}")
        } else {
            format!("{spinner} {prefix} {message}")
        }
    }

    /// Render the spinner column: a style-colored tick char cycled by
    /// `spinner_index` for `RowAnimation::Animate`, an uncolored `·` for
    /// `RowAnimation::Static`.
    ///
    /// `TICK_CHARS` is a non-empty const, so the modulo divisor and the
    /// vector index are always valid.
    fn render_spinner(&self, row: &PlanRow) -> String {
        let RowAnimation::Animate = row.animation else {
            // A parked row never cycles (spec R3): fixed dot, no color.
            return "·".to_string();
        };
        let ticks: Vec<char> = TICK_CHARS.chars().collect();
        let glyph = ticks[self.spinner_index % ticks.len()];
        match row.style {
            RenderStyle::Done => style(glyph).white().dim().to_string(),
            RenderStyle::Failed => style(glyph).red().to_string(),
            // Unreachable: `render_row` returns full-width spaces for `Blank`.
            RenderStyle::Blank => String::new(),
            // Overall and Active share the green spinner.
            RenderStyle::Overall | RenderStyle::Active => style(glyph).green().to_string(),
        }
    }

    /// Render the fill column: the done run in the style's palette color and
    /// the rest dimmed, split across `fill_w` by `position / total`.
    ///
    /// A row with `total == 0` shades no done cells (nothing is known to be
    /// complete).
    fn render_fill(&self, row: &PlanRow) -> String {
        let done_columns = if row.total == 0 {
            0
        } else {
            // Cap at `total` so the ratio stays within `fill_w`; `u128` keeps
            // the multiply from overflowing for a huge position.
            let capped = row.position.min(row.total);
            let scaled = u128::from(capped) * self.fill_w as u128 / u128::from(row.total);
            // `scaled <= fill_w` by the cap above, so this never truncates.
            usize::try_from(scaled).unwrap_or(self.fill_w)
        };
        let done_str = "█".repeat(done_columns);
        let rest_str = "░".repeat(self.fill_w - done_columns);
        let mut out = String::new();
        if !done_str.is_empty() {
            out.push_str(&done_fill_color(&done_str, row.style));
        }
        if !rest_str.is_empty() {
            out.push_str(&style(rest_str).dim().to_string());
        }
        out
    }
}

/// Style the done run of a fill in its palette color: `magenta` for
/// `RenderStyle::Overall`, `yellow` for `Active`, `green` for `Done`, `red`
/// for `Failed` — the four `apply_*_bar_style` templates.
///
/// `RenderStyle::Blank` is unreachable here: `render_row` short-circuits it
/// to full-width spaces before any fill is built, so its arm returns the run
/// unstyled rather than picking a color a blank row must never show.
fn done_fill_color(run: &str, render_style: RenderStyle) -> String {
    match render_style {
        RenderStyle::Overall => style(run).magenta().to_string(),
        RenderStyle::Active => style(run).yellow().to_string(),
        RenderStyle::Done => style(run).green().to_string(),
        RenderStyle::Failed => style(run).red().to_string(),
        RenderStyle::Blank => run.to_string(),
    }
}

/// Right-pad `s` with spaces to exactly `width` visible columns, truncating
/// from the right when it is longer — the contract `{prefix:>width}` provided.
fn pad_right(s: &str, width: usize) -> String {
    let kept = truncate_visible(s, width);
    let mut out = kept;
    let pad = width.saturating_sub(visible_width(&out));
    out.push_str(&" ".repeat(pad));
    out
}

/// Left-pad `s` with spaces to exactly `width` visible columns, truncating
/// from the right when it is longer — the contract `{msg:<width}` provided.
fn pad_left(s: &str, width: usize) -> String {
    let kept = truncate_visible(s, width);
    let pad = width.saturating_sub(visible_width(&kept));
    let mut out = " ".repeat(pad);
    out.push_str(&kept);
    out
}

/// Truncate `s` to at most `width` visible characters, keeping ANSI escape
/// sequences (zero columns) intact; returns `s` unchanged when it already
/// fits.
fn truncate_visible(s: &str, width: usize) -> String {
    if visible_width(s) <= width {
        return s.to_string();
    }
    let mut out = String::new();
    let mut visible = 0;
    let mut in_escape = false;
    for c in s.chars() {
        if in_escape {
            out.push(c);
            if c == 'm' {
                in_escape = false;
            }
        } else if c == '\x1b' {
            in_escape = true;
            out.push(c);
        } else if visible < width {
            out.push(c);
            visible += 1;
        } else {
            break;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::super::components::visible_width;
    use super::*;

    /// One rendered line per style, compared against `console`'s own output
    /// so the color authority is the library, not a hard-coded escape.
    ///
    /// Colors are forced on for this test: `nextest` runs each test in its
    /// own process, so the global switch cannot leak to a sibling.
    #[test]
    fn render_styles_match_their_console_palette() {
        use console::style;
        console::set_colors_enabled(true);

        let row = |style, animation| PlanRow {
            prefix: "media-tagger 0.0.0 [fch]".to_string(),
            message: "189/189 0s".to_string(),
            position: 3,
            total: 6,
            style,
            animation,
        };
        let plan = FramePlan {
            rows: vec![row(RenderStyle::Active, RowAnimation::Animate)],
            prefix_w: 25,
            suffix_w: 12,
            fill_w: 6,
            draw_fill: true,
            spinner_index: 0,
        };
        let line = &plan.render()[0];

        // Fill: 3/6 of the run in yellow, the rest dim (template `yellow/dim`).
        assert!(
            line.contains(&format!("{}{}", style("███").yellow(), style("░░░").dim())),
            "active fill must be yellow/dim; got {line:?}"
        );
        // A cycling row starts on the first tick char of TICK_CHARS.
        let first = super::TICK_CHARS.chars().next().expect("tick chars");
        assert!(line.starts_with(&format!("{} ", style(first).green())), "got {line:?}");
    }

    /// A parked row renders a fixed dot and never the cycle (spec R3/D4).
    #[test]
    fn static_rows_render_a_fixed_dot() {
        let plan = FramePlan {
            rows: vec![PlanRow {
                prefix: "[idle]".to_string(),
                message: "22s".to_string(),
                position: 0,
                total: 0,
                style: RenderStyle::Active,
                animation: RowAnimation::Static("idle"),
            }],
            prefix_w: 6,
            suffix_w: 4,
            fill_w: 4,
            draw_fill: false,
            spinner_index: 3,
        };
        assert!(
            plan.render()[0].starts_with("· "),
            "a parked row must not show the spinner cycle; got {:?}",
            plan.render()[0]
        );
    }

    /// Geometry is shared: every row of one plan pads to the same columns.
    #[test]
    fn every_row_pads_to_the_shared_geometry() {
        let plan = FramePlan {
            rows: vec![
                PlanRow {
                    prefix: "a".into(),
                    message: "1s".into(),
                    position: 0,
                    total: 1,
                    style: RenderStyle::Active,
                    animation: RowAnimation::Animate,
                },
                PlanRow {
                    prefix: "longer prefix".into(),
                    message: "22s".into(),
                    position: 1,
                    total: 2,
                    style: RenderStyle::Active,
                    animation: RowAnimation::Animate,
                },
            ],
            prefix_w: 13,
            suffix_w: 3,
            fill_w: 6,
            draw_fill: true,
            spinner_index: 1,
        };
        let rendered = plan.render();
        let widths: Vec<usize> = rendered.iter().map(|l| visible_width(l)).collect();
        assert_eq!(widths[0], widths[1], "one settle, one width: {rendered:?}");
    }
}
