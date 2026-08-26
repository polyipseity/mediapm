//! Prefix/suffix components, bar styles, and ANSI-safe truncation helpers.

use std::fmt::Write as _;
use std::io::Write;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use indicatif::{ProgressBar, ProgressStyle};
use serde::Serialize;

use super::TrackStatus;

/// A single status word with an optional count.
///
/// `count: None` renders the bare word (qualitative, e.g. `skipped`).
/// `count: Some(n)` with `n > 0` renders `n word` (quantitative).
/// `count: Some(0)` is dropped entirely (the status did not occur).
pub struct StatusCount {
    /// The status word to render (e.g. `skipped`, `cached`).
    pub word: &'static str,
    /// Optional count. `None` renders the bare word; `Some(0)` is dropped.
    pub count: Option<u32>,
}

/// Render a list of statuses as a comma-joined string with no parentheses.
///
/// Qualitative entries (`count: None`) render as the bare word.
/// Quantitative entries (`count: Some(n)`, `n > 0`) render as `n word`.
/// Zero-count entries are omitted. Empty input yields an empty string.
pub fn format_status_list(statuses: &[StatusCount]) -> String {
    let parts: Vec<String> = statuses
        .iter()
        .filter_map(|s| match s.count {
            None => Some(s.word.to_string()),
            Some(0) => None,
            Some(n) => Some(format!("{n} {}", s.word)),
        })
        .collect();
    parts.join(", ")
}

// ---- style constants --------------------------------------------------

/// Format a duration compactly: `0s`, `3s`, `42s`, `1m35s`, `12m4s`, `2h15m`, `1d8h`, `30d`.
pub(crate) fn format_elapsed(d: Duration) -> String {
    let total_secs = d.as_secs();
    if total_secs == 0 {
        return "0s".into();
    }
    let secs = total_secs % 60;
    let total_mins = total_secs / 60;
    if total_mins == 0 {
        return format!("{secs}s");
    }
    let mins = total_mins % 60;
    let total_hours = total_mins / 60;
    if total_hours == 0 {
        if secs > 0 {
            return format!("{total_mins}m{secs}s");
        }
        return format!("{total_mins}m");
    }
    let hours = total_hours % 24;
    let days = total_hours / 24;
    if days == 0 {
        if mins > 0 {
            return format!("{total_hours}h{mins}m");
        }
        return format!("{total_hours}h");
    }
    if hours > 0 {
        return format!("{days}d{hours}h");
    }
    format!("{days}d")
}

/// Format an ETA (seconds remaining) compactly: `5s`, `42s`, `1m35s`, `2h15m`, `1d8h`.
/// Returns `"?"` when rate is zero or negative.
pub(crate) fn format_eta(eta_secs: f64) -> String {
    if !eta_secs.is_finite() || eta_secs <= 0.0 {
        return "?".into();
    }
    format_elapsed(Duration::from_secs_f64(eta_secs))
}

/// Format a rate (units/second) compactly: `3.5/s`, `42/s`, `1.2k/s`, `123k/s`, `3.5M/s`.
pub(crate) fn format_rate(rate: f64) -> String {
    if rate >= 1_000_000.0 {
        let v = rate / 1_000_000.0;
        if v < 10.0 { format!("{v:.1}M/s") } else { format!("{v:.0}M/s") }
    } else if rate >= 1_000.0 {
        let v = rate / 1_000.0;
        if v < 10.0 { format!("{v:.1}k/s") } else { format!("{v:.0}k/s") }
    } else if rate >= 1.0 {
        if rate < 10.0 { format!("{rate:.1}/s") } else { format!("{rate:.0}/s") }
    } else if rate * 60.0 >= 1.0 {
        format!("{:.0}/m", rate * 60.0)
    } else if rate * 3600.0 >= 1.0 {
        format!("{:.0}/h", rate * 3600.0)
    } else {
        format!("{:.0}/d", rate * 86400.0)
    }
}

// ---- progress debug instrumentation ----------------------------------

/// JSONL sink for progress debug snapshots.
///
/// Every tick cycle writes one JSON line to the configured writer with a
/// snapshot of all bar states.  Controlled via
/// [`ProgressGroupBuilder::with_progress_debug_sink`] or the
/// `MEDIAPM_PROGRESS_DEBUG` environment variable.
pub struct ProgressDebugSink {
    /// Output writer (e.g. file, stderr).
    pub(crate) writer: Mutex<Box<dyn Write + Send>>,
    /// Monotonic tick counter — incremented on every emit.
    pub(crate) tick_count: AtomicU64,
    /// Time the sink was created.
    pub(crate) start: Instant,
}

impl std::fmt::Debug for ProgressDebugSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProgressDebugSink")
            .field("writer", &"Box<dyn Write + Send>")
            .field("tick_count", &self.tick_count)
            .field("start", &self.start)
            .finish()
    }
}

/// Snapshot of one bar slot at a point in time, ready for JSON
/// serialization.
#[derive(Debug, Serialize)]
pub struct DebugSlotState {
    /// Slot index within the renderer's fixed grid.
    pub slot: usize,
    /// Whether this slot is bound to a tracked source.
    pub bound: bool,
    /// Current label (always present).
    pub label: String,
    /// Current prefix (always present).
    pub prefix: String,
    /// Current position (work completed).
    pub position: u64,
    /// Total work units (0 = indeterminate).
    pub total: u64,
    /// Current status as debug string (e.g. `"Active"`, `"Warning"`).
    pub status: String,
    /// Elapsed seconds since the handle was created.
    pub elapsed_secs: f64,
    /// Rate in bytes/second (0.0 when inactive or indeterminate).
    pub rate_bytes_per_sec: f64,
    /// Estimated seconds remaining (None when unknown or inactive).
    pub eta_secs: Option<f64>,
    /// Custom suffix (empty string when none).
    pub suffix: String,
    /// Whether the source had the dirty flag set this tick.
    pub dirty: bool,
}

/// Snapshot of a single tick cycle, serialized as one JSON line.
#[derive(Debug, Serialize)]
pub struct DebugTickSnapshot {
    /// Discriminant: `"tick"` (and later `"attach"`, `"finish"` etc.).
    pub r#type: String,
    /// Monotonic tick counter from the sink.
    pub tick: u64,
    /// Seconds since the sink was created.
    pub elapsed_secs: f64,
    /// Per-slot bar states.
    pub bars: Vec<DebugSlotState>,
}

impl ProgressDebugSink {
    /// Create a new debug sink that writes JSONL to `writer`.
    #[must_use]
    pub fn new(writer: Box<dyn Write + Send>) -> Self {
        Self { writer: Mutex::new(writer), tick_count: AtomicU64::new(0), start: Instant::now() }
    }

    /// Emit one JSONL line with the given snapshot.
    ///
    /// Increments `tick_count`, serializes the snapshot as compact JSON,
    /// writes it followed by a newline, and flushes the writer.
    ///
    /// # Panics
    ///
    /// Panics if the writer mutex is poisoned or the writer returns an
    /// I/O error.
    pub fn emit(&self, snapshot: &DebugTickSnapshot) {
        self.tick_count.fetch_add(1, Ordering::Relaxed);
        let json = serde_json::to_string(snapshot).expect("debug snapshot serialization");
        let mut writer = self.writer.lock().expect("debug sink writer lock");
        writer.write_all(json.as_bytes()).expect("debug sink write");
        writer.write_all(b"\n").expect("debug sink newline");
        writer.flush().expect("debug sink flush");
    }
}

/// Format a count with SI suffix: `0`, `999`, `1.2k`, `12.3k`, `123k`, `1.2M`, `12.3M`.
#[allow(clippy::cast_precision_loss)]
pub(crate) fn format_count(n: u64) -> String {
    if n >= 1_000_000_000 {
        format!("{:.1}G", n as f64 / 1_000_000_000.0)
    } else if n >= 1_000_000 {
        let v = n as f64 / 1_000_000.0;
        if v < 10.0 { format!("{v:.1}M") } else { format!("{v:.0}M") }
    } else if n >= 1_000 {
        let v = n as f64 / 1_000.0;
        if v < 10.0 { format!("{v:.1}k") } else { format!("{v:.0}k") }
    } else {
        n.to_string()
    }
}

const COMPACT_BAR_TEMPLATE: &str = "{spinner:.green} {prefix:>18.18} {msg:<10.30}";

const COMPACT_OVERALL_BAR_TEMPLATE: &str = "{spinner:.green} {prefix:>18.18} {msg:<10.30}";

const COMPACT_DONE_BAR_TEMPLATE: &str = "{spinner:.white/.dim} {prefix:>18.18} {msg:<10.30}";

const COMPACT_FAILED_BAR_TEMPLATE: &str = "{spinner:.red} {prefix:>18.18} {msg:<10.30}";

/// Maximum number of pre-allocated slot bars (safety cap).
pub(crate) const MAX_SLOTS: usize = 256;

/// ANSI SGR foreground color code matching the `{wide_bar}` template color.
pub(crate) fn bar_color_code(status: TrackStatus, is_overall: bool) -> &'static str {
    match status {
        TrackStatus::Failed => "31",
        TrackStatus::Active if is_overall => "35",
        // Active (non-overall) and Warning share yellow (33).
        TrackStatus::Active | TrackStatus::Warning => "33",
        TrackStatus::Success => "32",
    }
}

/// Render [`SuffixComponents`] into the `{msg}` display string.
///
/// Format: ` \x1b[{color_code}m{count}/{total}\x1b[0m` + ` {elapsed}` +
/// ` {rate}` (when `Some`) + ` {eta}` (when `rate` AND `eta` are both
/// `Some` — eta-only-when-rate guard) + ` {custom}` (when non-empty).
/// The count/total segment is omitted when both fields are empty.
///
/// Normative truncation spec: see [`semantic_truncate_suffix`].
pub(crate) fn render_suffix_components(parts: &SuffixComponents, color_code: &str) -> String {
    let mut s = String::new();
    if !parts.count.is_empty() || !parts.total.is_empty() {
        let _ = write!(s, " \x1b[{color_code}m{}/{}\x1b[0m", parts.count, parts.total);
    }
    if !parts.elapsed.is_empty() {
        s.push(' ');
        s.push_str(&parts.elapsed);
    }
    if let Some(rate) = &parts.rate {
        s.push(' ');
        s.push_str(rate);
        if let Some(eta) = &parts.eta {
            s.push(' ');
            s.push_str(eta);
        }
    }
    if !parts.custom.is_empty() {
        s.push(' ');
        s.push_str(&parts.custom);
    }
    s
}

// ---- Source-data component types -----------------------------------

/// Source components of a progress prefix, stored separately so
/// [`semantic_truncate_prefix`] receives individual fields directly
/// rather than a combined string that must be re-parsed.
///
/// Normative truncation spec (removal order, least-important first):
///
/// 1. `version` — progressive shrink (right-to-left, keeping left prefix)
/// 2. `count`/`total` — removed entirely, as one atomic pair
/// 3. `phase` — removed entirely
/// 4. `marker` — removed entirely
/// 5. `tool_name` — progressive shrink
/// 6. fallback — hard truncate whatever remains to the width budget
///
/// Storage rule: `count` and `total` are stored as separate fields but
/// rendered together (`{count}/{total}`) and trimmed together as one
/// atomic unit — a bare count or bare total is never shown.
///
/// Marker render rule: `marker` (set to `F` for failed, `W` for
/// warning, else empty) renders as a colored `[{marker}] ` bracket —
/// red for [`TrackStatus::Failed`], yellow for
/// [`TrackStatus::Warning`] — when non-empty. The marker brackets are
/// visible characters that participate in truncation.
///
/// This struct is the prefix's structured single mechanism: initial
/// values come from parsing the `add_bar`/`with_overall` label at
/// construction, and [`TrackedHandle::set_prefix_components`] is the
/// only runtime mutation API. The removal order above is a normative
/// spec, verified verbatim by the `semantic_truncate_prefix_*` unit
/// suites.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PrefixComponents {
    /// Status marker (`F` failed, `W` warning), empty when no marker.
    pub marker: String,
    /// Tool/binary name (always present).
    pub tool_name: String,
    /// Version suffix (e.g. `@7.1`), empty when absent.
    pub version: String,
    /// Phase tag (e.g. `pro`, `fch`, `dl`), empty when absent.
    pub phase: String,
    /// Count (numerator), rendered with `total` as `{count}/{total}`.
    pub count: String,
    /// Total (denominator), rendered with `count` as `{count}/{total}`.
    pub total: String,
}

/// Source components of a progress suffix, stored separately so
/// [`semantic_truncate_suffix`] receives each field directly without
/// string re-parsing.
///
/// Normative truncation spec (removal order, least-important first):
///
/// 1. `custom` — progressive shrink (right-to-left, keeping left prefix)
/// 2. `eta` — removed entirely
/// 3. `rate` — removed entirely
/// 4. `elapsed` — removed entirely
/// 5. `count`/`total` — removed entirely, as one atomic pair
/// 6. fallback — hard truncate whatever remains to the width budget
///
/// Storage rule: `count` and `total` are stored as separate fields but
/// rendered together (`{count}/{total}`) and trimmed together as one
/// atomic unit — a bare count or bare total is never shown.
///
/// Render rule: `eta` is rendered only when `rate` is present
/// (eta-only-when-rate guard).
///
/// This struct is the suffix's structured single mechanism:
/// [`TrackedHandle::set_suffix_components`] is the only mutation API
/// (the legacy `set_suffix(String)` is removed), and user-set fields
/// override the auto-derived ticker fields at sync time with empty
/// fields auto-filled. The removal order above is a normative spec,
/// verified verbatim by the `semantic_truncate_suffix_*` unit suites
/// and preserved unchanged through the user-override merge (the merge
/// guard tests assert truncation order still holds after merging).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SuffixComponents {
    /// Count (numerator), rendered with `total` as `{count}/{total}`.
    pub count: String,
    /// Total (denominator), rendered with `count` as `{count}/{total}`.
    pub total: String,
    /// Elapsed time display text (e.g. `0:00:05`), empty when absent.
    pub elapsed: String,
    /// Rate display text (e.g. `12.3 MiB/s`), `None` when no rate yet.
    pub rate: Option<String>,
    /// ETA display text (already bracketed, e.g. `[0:00:02]`), `None` when unavailable.
    pub eta: Option<String>,
    /// Custom suffix text appended after the auto-computed RHS.
    pub custom: String,
}

impl SuffixComponents {
    /// Merge user-set components over auto-derived ticker fields.
    ///
    /// This is the single source of truth for the suffix merge used by both
    /// the draw path ([`crate::progress::inner::ProgressRenderer::sync_snapshot_to_bar`])
    /// and the layout estimator ([`crate::progress::inner::ProgressRenderer::recompute_layout`]).
    /// Keeping one implementation guarantees the width estimate matches what
    /// actually draws — otherwise a wider user-set `rate`/`eta`/`custom` would
    /// overflow the reserved `suffix_w` and get truncated away at draw time.
    ///
    /// Stored non-empty fields override the auto-derived ones; stored
    /// `rate`/`eta` override when `Some`; empty fields auto-fill from the
    /// ticker-derived `auto` set.
    pub(crate) fn merge(auto: &SuffixComponents, stored: &SuffixComponents) -> SuffixComponents {
        SuffixComponents {
            count: if stored.count.is_empty() { auto.count.clone() } else { stored.count.clone() },
            total: if stored.total.is_empty() { auto.total.clone() } else { stored.total.clone() },
            elapsed: if stored.elapsed.is_empty() {
                auto.elapsed.clone()
            } else {
                stored.elapsed.clone()
            },
            rate: stored.rate.clone().or_else(|| auto.rate.clone()),
            eta: stored.eta.clone().or_else(|| auto.eta.clone()),
            custom: stored.custom.clone(),
        }
    }

    /// Build a `SuffixComponents` whose `custom` field is a comma-joined
    /// status list (no parentheses). All other fields default to empty.
    #[must_use]
    pub fn status_list(statuses: &[StatusCount]) -> Self {
        SuffixComponents { custom: format_status_list(statuses), ..Default::default() }
    }
}

/// Parse an `add_bar` label into [`PrefixComponents`].
///
/// Parsing rules:
/// - First whitespace-bounded token = `tool_name`.
/// - Trailing `[phase]` (last bracket pair) = phase inner text.
/// - Trailing `digits/digits` after bracket = count.
/// - Remainder between `tool_name` and `[` = `version`.
/// - If no `[` found: `tool_name` = entire string (single or multi-word),
///   except a trailing space-separated `count/total` token is split off.
///
/// Labels are the single identity source for a bar; structured
/// `set_prefix_components` calls override the parsed components later.
pub fn prefix_components_from_str(s: &str) -> PrefixComponents {
    let s = s.trim();
    if s.is_empty() {
        return PrefixComponents::default();
    }
    let tool_name = s.split_whitespace().next().unwrap_or("").to_string();
    if let Some(bracket_start) = s.rfind('[') {
        let bracket_end = bracket_start + s[bracket_start..].find(']').map_or(0, |i| i + 1);
        let phase = s[(bracket_start + 1)..(bracket_end - 1)].trim().to_string();
        let after_bracket = s[bracket_end..].trim();
        let (count, total) = split_count_total(after_bracket);
        let between = s[tool_name.len()..bracket_start].trim();
        // between might contain the count text if it appears before bracket
        let version = if !between.is_empty() && !between.contains('/') {
            between.to_string()
        } else {
            String::new()
        };
        PrefixComponents { marker: String::new(), tool_name, version, phase, count, total }
    } else {
        // No brackets — the entire string is the tool name (single or
        // multi-word), except a trailing space-separated count/total
        // token (containing `/`) is split off into `count`/`total`.
        let (tool_name, count, total) = match s.rfind(' ') {
            Some(space) => {
                let (head, tail) = s.split_at(space + 1);
                let (count, total) = split_count_total(tail.trim());
                if total.is_empty() {
                    (s.to_string(), String::new(), String::new())
                } else {
                    (head.trim().to_string(), count, total)
                }
            }
            None => (s.to_string(), String::new(), String::new()),
        };
        PrefixComponents {
            marker: String::new(),
            tool_name,
            version: String::new(),
            phase: String::new(),
            count,
            total,
        }
    }
}

/// Split a `count/total` token (e.g. `2/5`) into separate fields.
/// Returns empty strings when the token contains no `/`.
pub(crate) fn split_count_total(token: &str) -> (String, String) {
    match token.split_once('/') {
        Some((count, total)) => (count.trim().to_string(), total.trim().to_string()),
        None => (String::new(), String::new()),
    }
}

/// Render [`PrefixComponents`] into the combined prefix display string.
///
/// The render always starts with an ANSI reset to clear any SGR state
/// from preceding template fields (e.g. `{spinner:.green}`). A non-empty
/// `marker` renders as a colored `[{marker}] ` bracket — red for
/// [`TrackStatus::Failed`], yellow for [`TrackStatus::Warning`],
/// uncolored for other statuses. Then components render as
/// `{tool_name}[ {version}][ [{phase}]][ {count}/{total}]`; empty
/// sections are omitted, and `count`/`total` render only as a pair.
pub fn render_prefix_components(parts: &PrefixComponents, status: TrackStatus) -> String {
    let mut s = String::from("\x1b[0m");
    if !parts.marker.is_empty() {
        match status {
            TrackStatus::Failed => {
                s.push_str("\x1b[31m[");
                s.push_str(&parts.marker);
                s.push_str("]\x1b[0m ");
            }
            TrackStatus::Warning => {
                s.push_str("\x1b[33m[");
                s.push_str(&parts.marker);
                s.push_str("]\x1b[0m ");
            }
            _ => {
                s.push('[');
                s.push_str(&parts.marker);
                s.push_str("] ");
            }
        }
    }
    s.push_str(&parts.tool_name);
    if !parts.version.is_empty() {
        s.push(' ');
        s.push_str(&parts.version);
    }
    if !parts.phase.is_empty() {
        s.push_str(" [");
        s.push_str(&parts.phase);
        s.push(']');
    }
    if !parts.count.is_empty() || !parts.total.is_empty() {
        s.push(' ');
        s.push_str(&parts.count);
        s.push('/');
        s.push_str(&parts.total);
    }
    s
}

// ---- ANSI-safe truncation helpers -----------------------------------

/// Strip ANSI SGR escape sequences (`\x1b[...m`) from `s`.
/// Non-SGR escape sequences (not ending with `m`) are left untouched.
pub(crate) fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\x1b' {
            // Try to match \x1b[...m (SGR sequence)
            let saved = chars.clone();
            if chars.next() == Some('[') {
                let mut seq = String::new();
                for ch in chars.by_ref() {
                    seq.push(ch);
                    if ch == 'm' {
                        break;
                    }
                    if !ch.is_ascii_digit() && ch != ';' {
                        // Not an SGR sequence — restore and emit the escape.
                        out.push('\x1b');
                        out.push('[');
                        out.push_str(&seq);
                        break;
                    }
                }
                // If we broke because of 'm', the sequence was consumed — drop it.
                // If we pushed saved chars, continue.
            } else {
                out.push('\x1b');
                chars = saved;
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// Number of visible characters in `s` (after stripping ANSI SGR codes).
pub(crate) fn visible_width(s: &str) -> usize {
    strip_ansi(s).chars().count()
}

/// Minimum reserved prefix width (decreasable floor). Bars never shrink
/// below this, so short prefixes still leave room for the bar to start.
pub(crate) const MIN_PREFIX_WIDTH: usize = 12;
/// Maximum reserved prefix width (hard ceiling). Beyond this, prefixes are
/// truncated by `semantic_truncate_prefix`.
pub(crate) const MAX_PREFIX_WIDTH: usize = 40;
/// Minimum reserved suffix width (decreasable floor).
pub(crate) const MIN_SUFFIX_WIDTH: usize = 12;
/// Maximum reserved suffix width (hard ceiling).
pub(crate) const MAX_SUFFIX_WIDTH: usize = 50;

/// Maximum visible width for the prefix field.
///
/// The dynamic `prefix_w`/`suffix_w` cells in [`ProgressRenderer`] hold the
/// actual per-frame width (clamped between the `MIN_*` floor and `MAX_*`
/// ceiling); this constant is the hard ceiling used when clamping. The
/// `cols` argument is intentionally unused — alignment is driven by the
/// measured max across visible bars, not by terminal width.
pub(crate) const fn max_prefix_width(_cols: u16) -> usize {
    MAX_PREFIX_WIDTH
}

/// Maximum visible width for the suffix field. See [`max_prefix_width`].
pub(crate) const fn max_suffix_width(_cols: u16) -> usize {
    MAX_SUFFIX_WIDTH
}

/// Progressively truncate prefix [`PrefixComponents`] so the rendered
/// visible width fits within `max_width`.
///
/// Normative truncation spec (removal order, least-important first):
///
/// 1. `version` — progressive shrink (right-to-left, keeping left prefix)
/// 2. `count`/`total` — removed entirely, as one atomic pair
/// 3. `phase` — removed entirely
/// 4. `marker` — removed entirely
/// 5. `tool_name` — progressive shrink
/// 6. fallback — hard truncate whatever remains to the width budget
///
/// Storage rule: `count` and `total` are stored as separate fields but
/// rendered together (`{count}/{total}`) and trimmed together as one
/// atomic unit — a bare count or bare total is never shown. The `marker`
/// is data like any other component: its brackets are visible characters
/// that participate in truncation, and it is dropped before the tool name
/// starts shrinking.
///
/// Fit is measured on the component render (the components themselves
/// carry no ANSI escapes — coloring is applied at render time).
pub(crate) fn semantic_truncate_prefix(
    parts: &PrefixComponents,
    max_width: usize,
) -> PrefixComponents {
    let fits = |p: &PrefixComponents| {
        let mut w = p.tool_name.chars().count();
        if !p.marker.is_empty() {
            w += p.marker.chars().count() + 3; // `[`, `]`, ` `
        }
        if !p.version.is_empty() {
            w += 1 + p.version.chars().count(); // ` `
        }
        if !p.phase.is_empty() {
            w += p.phase.chars().count() + 3; // ` [`, `]`
        }
        if !p.count.is_empty() || !p.total.is_empty() {
            w += p.count.chars().count() + p.total.chars().count() + 2; // ` `, `/`
        }
        w <= max_width
    };
    if fits(parts) {
        return parts.clone();
    }
    let mut out = parts.clone();

    // 1. Version: progressive shrink (right-to-left).
    if !out.version.is_empty() {
        let version_len = out.version.chars().count();
        for keep in (0..version_len).rev() {
            out.version = out.version.chars().take(keep).collect();
            if fits(&out) {
                return out;
            }
        }
        // Fully removed — continue to the next step.
    }

    // 2. Count/total: removed entirely, as one atomic pair.
    if !out.count.is_empty() || !out.total.is_empty() {
        out.count = String::new();
        out.total = String::new();
        if fits(&out) {
            return out;
        }
    }

    // 3. Phase: removed entirely (including brackets).
    if !out.phase.is_empty() {
        out.phase = String::new();
        if fits(&out) {
            return out;
        }
    }

    // 4. Marker: removed entirely.
    if !out.marker.is_empty() {
        out.marker = String::new();
        if fits(&out) {
            return out;
        }
    }

    // 5. Tool name: progressive shrink.
    if !out.tool_name.is_empty() {
        let tool_len = out.tool_name.chars().count();
        for keep in (0..tool_len).rev() {
            out.tool_name = out.tool_name.chars().take(keep).collect();
            if fits(&out) {
                return out;
            }
        }
    }

    // 6. Fallback: hard truncate whatever remains (only reachable when
    // every component is already empty).
    out
}

/// Progressively truncate suffix [`SuffixComponents`] so the rendered
/// visible width fits within `max_width`.
///
/// Normative truncation spec (removal order, least-important first):
///
/// 1. `custom` — progressive shrink (right-to-left, keeping left prefix)
/// 2. `eta` — removed entirely
/// 3. `rate` — removed entirely
/// 4. `elapsed` — removed entirely
/// 5. `count`/`total` — removed entirely, as one atomic pair
/// 6. fallback — hard truncate whatever remains to the width budget
///
/// Storage rule: `count` and `total` are stored as separate fields but
/// rendered together (`{count}/{total}`) and trimmed together as one
/// atomic unit — a bare count or bare total is never shown.
///
/// Fit is measured on the render with an empty color code (ANSI escapes
/// add no visible width).
pub(crate) fn semantic_truncate_suffix(
    parts: &SuffixComponents,
    max_width: usize,
) -> SuffixComponents {
    let fits = |p: &SuffixComponents| {
        strip_ansi(&render_suffix_components(p, "")).chars().count() <= max_width
    };
    if fits(parts) {
        return parts.clone();
    }
    let mut out = parts.clone();

    // 1. Custom: progressive shrink (right-to-left, keeping left prefix).
    if !out.custom.is_empty() {
        let custom_len = out.custom.chars().count();
        for keep in (0..custom_len).rev() {
            out.custom = out.custom.chars().take(keep).collect();
            if fits(&out) {
                return out;
            }
        }
        // Custom fully removed but still doesn't fit — continue below.
    }

    // 2. Eta: removed entirely.
    if out.eta.is_some() {
        out.eta = None;
        if fits(&out) {
            return out;
        }
    }

    // 3. Rate: removed entirely.
    if out.rate.is_some() {
        out.rate = None;
        if fits(&out) {
            return out;
        }
    }

    // 4. Elapsed: removed entirely.
    if !out.elapsed.is_empty() {
        out.elapsed.clear();
        if fits(&out) {
            return out;
        }
    }

    // 5. Count/total: removed entirely, as one atomic pair.
    if !out.count.is_empty() || !out.total.is_empty() {
        out.count.clear();
        out.total.clear();
        if fits(&out) {
            return out;
        }
    }

    // 6. Fallback: hard truncate whatever remains to the width budget.
    out.custom = strip_ansi(&render_suffix_components(&out, "")).chars().take(max_width).collect();
    out
}

fn compact_overall_bar_style() -> ProgressStyle {
    ProgressStyle::with_template(COMPACT_OVERALL_BAR_TEMPLATE)
        .expect("invalid compact overall bar template")
        .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
}

pub(crate) fn apply_overall_bar_style(pb: &ProgressBar, prefix_w: usize, suffix_w: usize) {
    if prefix_w == 0 {
        // Compact path: no wide_bar — prefix/suffix are rendered inline.
        pb.set_style(compact_overall_bar_style());
    } else {
        let tpl = format!(
            "{{spinner:.green}} {{prefix:>{prefix_w}.{prefix_w}}} {{wide_bar:0.magenta/dim}} {{msg:<{suffix_w}.{suffix_w}}}"
        );
        pb.set_style(
            ProgressStyle::with_template(&tpl)
                .expect("valid dynamic overall template")
                .progress_chars("█░")
                .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
        );
    }
}

fn compact_bar_style() -> ProgressStyle {
    ProgressStyle::with_template(COMPACT_BAR_TEMPLATE)
        .expect("invalid compact bar template")
        .progress_chars("█░")
        .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
}

pub(crate) fn apply_bar_style(pb: &ProgressBar, prefix_w: usize, suffix_w: usize) {
    if prefix_w == 0 {
        pb.set_style(compact_bar_style());
    } else {
        let tpl = format!(
            "{{spinner:.green}} {{prefix:>{prefix_w}.{prefix_w}}} {{wide_bar:0.yellow/dim}} {{msg:<{suffix_w}.{suffix_w}}}"
        );
        pb.set_style(
            ProgressStyle::with_template(&tpl)
                .expect("valid dynamic child template")
                .progress_chars("█░")
                .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
        );
    }
}

fn compact_done_bar_style() -> ProgressStyle {
    ProgressStyle::with_template(COMPACT_DONE_BAR_TEMPLATE)
        .expect("invalid compact done bar template")
        .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
}

fn compact_failed_bar_style() -> ProgressStyle {
    ProgressStyle::with_template(COMPACT_FAILED_BAR_TEMPLATE)
        .expect("invalid compact failed bar template")
        .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
}

pub(crate) fn apply_done_bar_style(pb: &ProgressBar, prefix_w: usize, suffix_w: usize) {
    if prefix_w == 0 {
        pb.set_style(compact_done_bar_style());
    } else {
        let tpl = format!(
            "{{spinner:.white/.dim}} {{prefix:>{prefix_w}.{prefix_w}}} {{wide_bar:0.green/dim}} {{msg:<{suffix_w}.{suffix_w}}}"
        );
        pb.set_style(
            ProgressStyle::with_template(&tpl)
                .expect("valid dynamic done template")
                .progress_chars("█░")
                .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
        );
    }
}

pub(crate) fn apply_failed_bar_style(pb: &ProgressBar, prefix_w: usize, suffix_w: usize) {
    if prefix_w == 0 {
        pb.set_style(compact_failed_bar_style());
    } else {
        let tpl = format!(
            "{{spinner:.red}} {{prefix:>{prefix_w}.{prefix_w}}} {{wide_bar:0.red/dim}} {{msg:<{suffix_w}.{suffix_w}}}"
        );
        pb.set_style(
            ProgressStyle::with_template(&tpl)
                .expect("valid dynamic failed template")
                .progress_chars("█░")
                .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
        );
    }
}

pub(crate) fn blank_bar_style() -> ProgressStyle {
    ProgressStyle::with_template("{wide_msg}").expect("invalid blank bar template")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_list_qualitative_no_count() {
        let out = format_status_list(&[StatusCount { word: "skipped", count: None }]);
        assert_eq!(out, "skipped");
    }

    #[test]
    fn status_list_counted_shows_number() {
        let out = format_status_list(&[StatusCount { word: "cached", count: Some(2) }]);
        assert_eq!(out, "2 cached");
    }

    #[test]
    fn status_list_drops_zero_count() {
        let out = format_status_list(&[StatusCount { word: "cached", count: Some(0) }]);
        assert_eq!(out, "");
    }

    #[test]
    fn status_list_joins_commas() {
        let out = format_status_list(&[
            StatusCount { word: "cached", count: Some(1) },
            StatusCount { word: "failed", count: Some(2) },
        ]);
        assert_eq!(out, "1 cached, 2 failed");
    }

    #[test]
    fn status_list_mixed_qualitative_and_counted() {
        let out = format_status_list(&[
            StatusCount { word: "skipped", count: None },
            StatusCount { word: "retried", count: Some(3) },
        ]);
        assert_eq!(out, "skipped, 3 retried");
    }

    #[test]
    fn suffix_components_status_list_sets_custom_only() {
        let s = SuffixComponents::status_list(&[StatusCount { word: "cached", count: Some(1) }]);
        assert_eq!(s.custom, "1 cached");
        assert!(s.count.is_empty());
        assert!(s.total.is_empty());
        assert!(s.elapsed.is_empty());
        assert!(s.rate.is_none());
        assert!(s.eta.is_none());
    }
}
