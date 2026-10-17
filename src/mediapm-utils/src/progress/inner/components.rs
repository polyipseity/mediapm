//! Prefix/suffix components, bar styles, and ANSI-safe truncation helpers.

use std::fmt::Write as _;
use std::io::Write;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

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
/// [`crate::progress::ProgressTerminalBuilder::with_progress_debug_sink`] or
/// the
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

/// Source components of a progress prefix, stored separately so the internal
/// `semantic_truncate_prefix` receives individual fields directly rather than a
/// combined string that must be re-parsed.
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
/// Initial values come from the components the bar was constructed with: the
/// [`add_bar_with_prefix`](super::ProgressScreen::add_bar_with_prefix)
/// components as given, or the parsed `add_bar`/`with_overall` label.
/// [`set_prefix_components`](super::ProgressBarHandle::set_prefix_components)
/// is the only runtime mutation API. The removal order above is a normative spec,
/// verified verbatim by the `semantic_truncate_prefix_*` unit suites.
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

impl PrefixComponents {
    /// Render the components as plain text, without the reset and the marker
    /// color the renderer adds on top: `{tool_name}[ {version}]
    /// [ [{phase}]][ {count}/{total}]`.
    ///
    /// This is what a bar built from components reports as its `label`, and
    /// what the recording tracker logs in place of the display string a
    /// string-seeded bar would have carried.
    #[must_use]
    pub fn display_label(&self) -> String {
        let mut s = String::with_capacity(self.tool_name.len() + 8);
        s.push_str(&self.tool_name);
        if !self.version.is_empty() {
            s.push(' ');
            s.push_str(&self.version);
        }
        if !self.phase.is_empty() {
            s.push_str(" [");
            s.push_str(&self.phase);
            s.push(']');
        }
        if !self.count.is_empty() || !self.total.is_empty() {
            s.push(' ');
            s.push_str(&self.count);
            s.push('/');
            s.push_str(&self.total);
        }
        s
    }
}

/// Source components of a progress suffix, stored separately so the internal
/// `semantic_truncate_suffix` receives each field directly without string
/// re-parsing.
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
/// [`set_suffix_components`](super::ProgressBarHandle::set_suffix_components)
/// is the only mutation API (the
/// legacy `set_suffix(String)` is removed); user-set fields override the
/// auto-derived ticker fields at sync time with empty fields auto-filled.
/// The removal order above is a normative spec, verified verbatim by the
/// `semantic_truncate_suffix_*` unit suites and preserved unchanged through
/// the user-override merge.
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
    /// Single implementation shared by the slot cache sync
    /// ([`crate::progress::inner::ProgressRenderer::sync_snapshot_to_slot`])
    /// and the layout estimator
    /// ([`crate::progress::inner::ProgressRenderer::settle_layout`]) so the
    /// width estimate matches what actually draws — otherwise a wider
    /// user-set `rate`/`eta`/`custom` would overflow the reserved `suffix_w`
    /// and get truncated away at draw time.
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
/// This is the fallback for labels that carry no structure: a single word, or
/// a word plus a trailing `count/total`. Anything with a name, a version and a
/// phase belongs in [`PrefixComponents`] passed to
/// [`add_bar_with_prefix`](super::ProgressScreen::add_bar_with_prefix), which
/// keeps the fields instead of reading them back out of a rendered string.
/// A tool name containing a space is the case this cannot express: the first
/// whitespace token becomes the name and the rest is read as a version.
///
/// Parsing rules:
/// - First whitespace-bounded token = `tool_name`.
/// - Trailing `[phase]` (last bracket pair) = phase inner text.
/// - Trailing `digits/digits` after bracket = count.
/// - Remainder between `tool_name` and `[` = `version`.
/// - If no `[` found: `tool_name` = entire string (single or multi-word),
///   except a trailing space-separated `count/total` token is split off.
///
/// A later [`set_prefix_components`](super::ProgressBarHandle::set_prefix_components)
/// replaces whatever was parsed.
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
    s.push_str(&parts.display_label());
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

/// Close a rendered slot string so it cannot leave the terminal mid-escape.
///
/// Appends `\x1b[0m` when `s` contains an SGR escape and does not already end
/// with one. A reset occupies zero visible columns, so the measured width and
/// the drawn width stay equal and the slot budget is unchanged. Both the
/// suffix funnel (`inner::renderer::layout::render_slot_suffix`) and the
/// client-label wrapper (`inner::renderer::layout::client_truncated_prefix`)
/// route through this, so no branch can hand a row an open colour state
/// regardless of what a client label contains.
pub(crate) fn close_sgr_state(s: String) -> String {
    if s.contains('\x1b') && !s.ends_with("\x1b[0m") { s + "\x1b[0m" } else { s }
}

/// Counts the characters of `s` that render on screen (after stripping ANSI
/// SGR codes).
pub(crate) fn visible_width(s: &str) -> usize {
    strip_ansi(s).chars().count()
}

/// Minimum reserved prefix width (decreasable floor). The floor is 0 — under
/// terminal pressure the prefix shrinks before the bar fill is dropped. The
/// *effective* prefix floor is ~4 because the built-in prefix carries a
/// leading `\x1b[0m` reset that `compute_ansi_overhead` reserves out of the
/// slot.
pub(crate) const MIN_PREFIX_WIDTH: usize = 0;
/// Maximum reserved prefix width (hard ceiling). Beyond this, prefixes are
/// truncated by `semantic_truncate_prefix`.
pub(crate) const MAX_PREFIX_WIDTH: usize = 40;
/// Minimum reserved suffix width (decreasable floor). The floor is 0 — the
/// suffix shrinks before the bar fill is dropped.
pub(crate) const MIN_SUFFIX_WIDTH: usize = 0;
/// Maximum reserved suffix width (hard ceiling).
pub(crate) const MAX_SUFFIX_WIDTH: usize = 65;

/// Columns a frame spends outside its label fields: the spinner glyph and the
/// single spaces between the spinner, the prefix, the fill and the message.
///
/// Every frame lays its line out as those four fields, so this is the width
/// no label can have at any terminal size.
pub(crate) const FRAME_OVERHEAD_COLUMNS: usize = 4;

/// Column a frame's spinner glyph occupies.
///
/// A frame writes its spinner as its first field, before the spaces and the
/// label slots that make up [`FRAME_OVERHEAD_COLUMNS`], so the glyph is the
/// first character of the row. Code that has to find the spinner on a
/// rendered frame reads this constant rather than counting a column itself,
/// so a frame that reorders its fields moves the answer with it.
pub(crate) const SPINNER_COLUMN: usize = 0;

/// Glyphs every live row cycles its spinner through.
///
/// The plan's spinner cycles this string with `spinner_index % len`, so with
/// these ten characters every glyph is reachable on a live row. Which glyph a
/// frame lands on is a function of timing, which is why
/// [`assert_frames_match`](crate::progress::assert_frames_match) exempts the
/// spinner column and why the whole list — not a nine-glyph slice — is what a
/// comparison has to accept.
pub(crate) const TICK_CHARS: &str = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏";

/// The spinner-column character a parked row draws (spec R3/AC2).
///
/// A row with no declared in-flight work renders this dot instead of cycling
/// `TICK_CHARS`, so it is as legitimate a spinner-column character as any
/// glyph and the transcript matcher treats it as one.
pub(crate) const PARKED_GLYPH: char = '·';

/// Columns held back for the bar fill before the prefix slot grows past the
/// measured label.
///
/// Under that the prefix is clamped to [`MIN_PREFIX_WIDTH`] and the suffix
/// truncates instead, so the fill keeps these cells for as long as the line
/// has room for it. Narrower still and the fill is the field that gets
/// nothing.
///
/// This is also the width at which a frame stops drawing its fill. Measured
/// across the three example screens at every width from 8 to 120, a fill sits
/// at exactly this width at every width below the point where the screen's
/// labels stop overflowing the line, and grows by one cell per column above
/// it. Four cells of `░░░░` carry a fixed quarter-resolution fraction that the
/// count in the suffix already states, and they are paid for out of a label
/// clipped down to its tail. `ProgressRenderer::settle_layout` drops the fill at
/// that point and hands the columns to the label and the count instead.
pub(crate) const MIN_BAR_FILL: usize = 4;

/// Ceiling for the prefix slot on a terminal `cols` columns wide.
///
/// Alignment follows the widest label on screen, so the ceiling is what stops
/// one long label from taking the whole line, and `cols` is what stops the
/// ceiling from being wider than the line.
/// [`ProgressRenderer::settle_layout`](super::ProgressRenderer::settle_layout)
/// applies the per-frame budget on top of this.
pub(crate) fn max_prefix_width(cols: u16) -> usize {
    usize::from(cols).min(MAX_PREFIX_WIDTH)
}

/// Ceiling for the suffix slot on a terminal `cols` columns wide.
/// See [`max_prefix_width`].
pub(crate) fn max_suffix_width(cols: u16) -> usize {
    usize::from(cols).min(MAX_SUFFIX_WIDTH)
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

    /// The suffix component renderer may only emit the intended count/total
    /// colour and its own reset — never a width-derived `38;5;` sequence or
    /// the basic blue/bright-blue SGR codes.
    ///
    /// `bar_color_code` maps every status to a member of this whitelist, and
    /// this test pins that nothing else leaks into the raw string.
    #[test]
    fn render_suffix_components_carry_no_width_derived_color() {
        let parts = SuffixComponents {
            count: "4".into(),
            total: "12".into(),
            elapsed: "0s".into(),
            rate: Some("2.2/s".into()),
            eta: Some("8s".into()),
            custom: "skipped".into(),
        };
        for (status, is_overall) in [
            (TrackStatus::Active, true),
            (TrackStatus::Active, false),
            (TrackStatus::Warning, true),
            (TrackStatus::Success, true),
            (TrackStatus::Failed, true),
        ] {
            let rendered = render_suffix_components(&parts, bar_color_code(status, is_overall));
            assert!(!rendered.contains("38;5;"), "width-derived colour in {rendered:?}");
            assert!(!rendered.contains("\x1b[34"), "blue SGR in {rendered:?}");
            assert!(!rendered.contains("\x1b[94"), "bright-blue SGR in {rendered:?}");
        }
    }

    /// The prefix component renderer ends in a state that cannot leak: with
    /// or without a marker, the trailing bytes are a reset or plain text,
    /// never an open colour.
    #[test]
    fn render_prefix_components_end_in_a_closed_state() {
        let parts = PrefixComponents {
            marker: "F".into(),
            tool_name: "echo".into(),
            version: String::new(),
            phase: "s3".into(),
            count: "2".into(),
            total: "5".into(),
        };
        for status in
            [TrackStatus::Failed, TrackStatus::Warning, TrackStatus::Active, TrackStatus::Success]
        {
            let rendered = render_prefix_components(&parts, status);
            // `!ends_with('m')` treats a non-reset final byte as plain text; it
            // would misread a plain-text tail ending in the letter `m`, so keep
            // every rendered prefix ending in a reset or a non-`m` byte.
            assert!(
                rendered.ends_with("\x1b[0m") || !rendered.ends_with('m'),
                "prefix ends mid-escape: {rendered:?}"
            );
            assert!(!rendered.contains("38;5;"), "width-derived colour in {rendered:?}");
        }
    }
}
