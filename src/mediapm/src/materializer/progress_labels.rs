//! Client-defined bar-label truncation for materialization progress.
//!
//! Materialization bars carry file-path identity and phase tags. The
//! [`MaterializationBarLabel`] struct owns its field names and truncation
//! order independently of the conductor's [`StepBarLabel`] and
//! [`WorkerBarLabel`].

use mediapm_utils::progress::{BarLabelTruncation, truncate_ordered};

/// Truncation order for a materialization bar.
///
/// Prefix order: `entry_path` (progressive) → `phase` (atomic) →
/// `status_marker` (atomic) → `entry_name` (progressive) → `file_name`
/// (progressive, sub-bars only) → fallback.
///
/// Materialization bars carry no version, no count/total, no workflow/step
/// identity.  The `entry_path` (directory portion) is truncated first
/// because losing leading path segments is least harmful; `entry_name`
/// (basename) is preserved as long as possible.
#[derive(Debug, Clone, Default)]
pub(crate) struct MaterializationBarLabel {
    pub status_marker: String,
    /// Directory portion of hierarchy path, e.g. `"Music/Artist/Album"`.
    pub entry_path: String,
    /// Basename of hierarchy entry, e.g. `"song.mkv"`.
    pub entry_name: String,
    /// Extracted file basename (sub-bars only), e.g. `"cover.jpg"`.
    pub file_name: String,
    /// Materialization phase tag: `"stg"` / `"vrf"` / `"cmt"` / `"wrt"` / `"mat"`.
    pub phase: String,
}

impl MaterializationBarLabel {
    fn prefix_parts(&self) -> Vec<String> {
        let mut parts = Vec::new();
        // Entry name is most important — placed first so truncate_ordered
        // keeps it longest (dropping trailing parts first).
        if !self.entry_name.is_empty() {
            parts.push(self.entry_name.clone());
        }
        if !self.file_name.is_empty() {
            parts.push(self.file_name.clone());
        }
        if !self.status_marker.is_empty() {
            parts.push(format!("[{}]", self.status_marker));
        }
        if !self.phase.is_empty() {
            parts.push(format!("[{}]", self.phase));
        }
        // Entry path is least important — placed last so truncate_ordered
        // drops it first under width pressure.
        if !self.entry_path.is_empty() {
            parts.push(self.entry_path.clone());
        }
        parts
    }
}

impl BarLabelTruncation for MaterializationBarLabel {
    fn truncate_prefix(&self, max_width: usize) -> String {
        let parts = self.prefix_parts();
        truncate_ordered(&parts, max_width)
    }

    fn truncate_suffix(&self, _max_width: usize) -> String {
        // Materialization bars have no suffix components.
        String::new()
    }
}

/// Split a relative path into `(entry_path, entry_name)`.
///
/// Returns `("", path)` when the path has no `/` separator.
pub(crate) fn split_entry_path(path: &str) -> (&str, &str) {
    match path.rfind('/') {
        Some(pos) => (&path[..pos], &path[pos + 1..]),
        None => ("", path),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_entry_path_with_separators() {
        let (dir, name) = split_entry_path("Music/Artist/Album/song.mkv");
        assert_eq!(dir, "Music/Artist/Album");
        assert_eq!(name, "song.mkv");
    }

    #[test]
    fn split_entry_path_no_separators() {
        let (dir, name) = split_entry_path("song.mkv");
        assert_eq!(dir, "");
        assert_eq!(name, "song.mkv");
    }

    #[test]
    fn truncate_drops_entry_path_first() {
        let label = MaterializationBarLabel {
            entry_path: "Music/Artist/Album".into(),
            entry_name: "song.mkv".into(),
            phase: "stg".into(),
            ..Default::default()
        };
        let wide = label.truncate_prefix(80);
        assert!(wide.contains("Music/Artist/Album"), "wide prefix missing entry_path: {wide:?}");
        assert!(wide.contains("song.mkv"), "wide prefix missing entry_name: {wide:?}");

        // Tight width: entry_path should be dropped first
        let tight = label.truncate_prefix(25);
        assert!(!tight.contains("Music/Artist/Album"), "entry_path not dropped: {tight:?}");
        assert!(tight.contains("song.mkv"), "entry_name dropped too early: {tight:?}");
    }

    #[test]
    fn truncate_preserves_entry_name_over_path() {
        let label = MaterializationBarLabel {
            entry_path: "very/long/path/segments".into(),
            entry_name: "important-file.mkv".into(),
            phase: "cmt".into(),
            ..Default::default()
        };
        let tight = label.truncate_prefix(25);
        // entry_name should survive even when entry_path must be dropped
        assert!(tight.contains("important-file.mkv"), "entry_name dropped: {tight:?}");
    }

    #[test]
    fn sub_bar_file_name_truncated_last() {
        let label = MaterializationBarLabel {
            entry_path: "Music".into(),
            entry_name: "album".into(),
            file_name: "cover.jpg".into(),
            phase: "wrt".into(),
            ..Default::default()
        };
        let tight = label.truncate_prefix(15);
        assert!(!tight.is_empty(), "prefix must not be empty: {tight:?}");
        // file_name is last in the truncation order, so it survives longest
        let wide = label.truncate_prefix(80);
        assert!(wide.contains("cover.jpg"), "file_name missing: {wide:?}");
    }
}
