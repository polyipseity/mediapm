//! CLI-friendly result output for operation summaries and diagnostics.
//!
//! Every result line follows a uniform structure:
//!
//! ```text
//! {icon} {op}    {k}={v}  {k}={v}    in {duration}
//! ```
//!
//! Warnings and hints appear as indented lines below the result.

use std::time::Duration;

use console::style;

// ---------------------------------------------------------------------------
// StatusIcon
// ---------------------------------------------------------------------------

/// Semantic status indicator used as the first visual element of a result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusIcon {
    /// Operation completed with changes applied.
    Success,
    /// Everything was already up to date — nothing changed.
    NoChange,
    /// Completed but with non-fatal degradation.
    Warning,
    /// Operation errored (handled gracefully, not a panic).
    Error,
}

impl StatusIcon {
    /// The Unicode glyph for this icon.
    #[must_use]
    pub fn glyph(self) -> &'static str {
        match self {
            Self::Success => "\u{2713}",  // ✓
            Self::NoChange => "\u{2013}", // –
            Self::Warning => "\u{0394}",  // Δ
            Self::Error => "\u{2717}",    // ✗
        }
    }

    /// The glyph with terminal styling applied.
    #[must_use]
    pub fn styled_glyph(self) -> String {
        let g = self.glyph();
        match self {
            Self::Success => style(g).green().bold().to_string(),
            Self::NoChange => style(g).dim().to_string(),
            Self::Warning => style(g).yellow().bold().to_string(),
            Self::Error => style(g).red().bold().to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// Top-level result
// ---------------------------------------------------------------------------

/// Print one result line to **stdout**.
///
/// # Format
///
/// ```text
/// {icon} {bold_op}    {k}={v}  {k}={v}    in {duration}
/// ```
///
/// When `fields` is empty the trailing spaces and field list are omitted.
/// When `duration` is `None` the trailing `in ...` is omitted.
/// Zero-valued fields are included when the caller passes them — the
/// caller decides which fields are meaningful to show.
pub fn print_result(
    icon: StatusIcon,
    op: &str,
    fields: &[(&str, &dyn std::fmt::Display)],
    duration: Option<Duration>,
) {
    use std::fmt::Write;
    let mut line = String::new();

    // Icon + bold operation name
    write!(line, "{} {}", icon.styled_glyph(), style(op).bold()).ok();

    // Fields
    if !fields.is_empty() {
        line.push_str("    ");
        for (i, (k, v)) in fields.iter().enumerate() {
            if i > 0 {
                line.push_str("  ");
            }
            write!(line, "{k}={v}").ok();
        }
    }

    // Duration
    if let Some(d) = duration {
        write!(line, "  in {}", format_duration(d)).ok();
    }

    println!("{line}");
}

// ---------------------------------------------------------------------------
// Ancillary output
// ---------------------------------------------------------------------------

/// Print a non-fatal warning line to **stderr**.
pub fn print_warning(msg: impl std::fmt::Display) {
    eprintln!("  {} {}", style("\u{0394}").yellow(), msg);
}

/// Print a hint line to **stderr** (suppressed by `--quiet`).
pub fn print_hint(msg: impl std::fmt::Display) {
    eprintln!("  {} {}", style("\u{2192}").cyan().bold(), msg);
}

/// Print a section heading to **stderr** with a dim underline.
pub fn print_heading(heading: &str) {
    eprintln!("{}", style(heading).bold());
    eprintln!("{}", style("\u{2500}".repeat(heading.len())).dim());
}

/// Print an aligned key-value status report to **stdout**.
///
/// All keys are left-aligned to the width of the longest key.
pub fn print_status_report(entries: &[(&str, &dyn std::fmt::Display)]) {
    let max_key_len = entries.iter().map(|(k, _)| k.len()).max().unwrap_or(0);
    for (key, value) in entries {
        println!("{key:<max_key_len$}  {value}");
    }
}

/// Print an error summary to **stderr**.
pub fn print_error(msg: impl std::fmt::Display) {
    eprintln!("{} {}", style("\u{2717}").red().bold(), msg);
}

// ---------------------------------------------------------------------------
// Duration formatting
// ---------------------------------------------------------------------------

fn format_duration(d: Duration) -> String {
    let secs = d.as_secs();
    if secs >= 3600 {
        let h = secs / 3600;
        let m = (secs % 3600) / 60;
        let s = secs % 60;
        format!("{h}h {m}m {s}s")
    } else if secs >= 60 {
        let m = secs / 60;
        let s = secs % 60;
        format!("{m}m {s}s")
    } else if secs >= 10 {
        format!("{secs}s")
    } else {
        // Show sub-second precision for short durations
        let ms = d.subsec_millis();
        if secs > 0 || ms >= 100 {
            let mut cs = (ms + 5) / 10; // round to centiseconds
            let mut s = secs;
            if cs >= 100 {
                s += 1;
                cs -= 100;
            }
            format!("{s}.{cs:02}s")
        } else {
            format!("{:.2}s", d.as_secs_f64())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_duration_sub_second() {
        assert_eq!(format_duration(Duration::from_millis(5)), "0.01s");
        assert_eq!(format_duration(Duration::from_millis(50)), "0.05s");
        assert_eq!(format_duration(Duration::from_millis(999)), "1.00s");
    }

    #[test]
    fn format_duration_seconds() {
        assert_eq!(format_duration(Duration::from_secs(1)), "1.00s");
        assert_eq!(format_duration(Duration::from_secs(9)), "9.00s");
        assert_eq!(format_duration(Duration::from_secs(10)), "10s");
        assert_eq!(format_duration(Duration::from_secs(42)), "42s");
    }

    #[test]
    fn format_duration_minutes() {
        assert_eq!(format_duration(Duration::from_secs(60)), "1m 0s");
        assert_eq!(format_duration(Duration::from_secs(90)), "1m 30s");
        assert_eq!(format_duration(Duration::from_secs(3599)), "59m 59s");
    }

    #[test]
    fn format_duration_hours() {
        assert_eq!(format_duration(Duration::from_secs(3600)), "1h 0m 0s");
        assert_eq!(format_duration(Duration::from_secs(3661)), "1h 1m 1s");
    }
}
