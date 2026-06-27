//! Hierarchy manipulation helpers for mediapm.
//!
//! This module provides utilities for building, inserting, and removing
//! hierarchy nodes in the mediapm config, supporting preset-based hierarchy
//! generation for local and yt-dlp media sources.

use crate::config::hierarchy_types::{
    HierarchyFolderRenameRule, HierarchyNode, HierarchyNodeKind, HierarchyPath, PlaylistFormat,
    SanitizeNamesConfig,
};
use crate::error::MediaPmError;
use crate::{AddInsertPosition, MediaHierarchyPreset};

// ---------------------------------------------------------------------------
// Template constants
// ---------------------------------------------------------------------------

/// Media root template key for local-file hierarchy presets.
pub(crate) const HIERARCHY_MEDIA_ROOT_TEMPLATE: &str = "media_root";
/// Media root template key for yt-dlp hierarchy presets.
pub(crate) const HIERARCHY_YT_DLP_MEDIA_ROOT_TEMPLATE: &str = "media_root_ytdlp";
/// Tagged media file template key for local-file hierarchy.
#[allow(dead_code)]
pub(crate) const HIERARCHY_TAGGED_MEDIA_FILE_TEMPLATE: &str = "tagged_media_file";
/// Album folder template key for local-file hierarchy.
pub(crate) const HIERARCHY_ALBUM_FOLDER_TEMPLATE: &str = "album_folder";
/// Artist folder template key for local-file hierarchy.
pub(crate) const HIERARCHY_ARTIST_FOLDER_TEMPLATE: &str = "artist_folder";
/// yt-dlp playlist extractor template key.
pub(crate) const HIERARCHY_YT_DLP_PLAYLIST_EXTRACTOR: &str = "yt_dlp_playlist_extractor";
/// yt-dlp media entry template key.
#[allow(dead_code)]
pub(crate) const HIERARCHY_YT_DLP_MEDIA_ENTRY: &str = "yt_dlp_media_entry";
/// Media folder template key (for multi-variant entries).
pub(crate) const HIERARCHY_MEDIA_FOLDER_TEMPLATE: &str = "media_folder_entry";
/// Media file template key (for single-variant entries).
pub(crate) const HIERARCHY_MEDIA_FILE_TEMPLATE: &str = "media_file_entry";

/// Returns the hierarchy preset node id for a given preset kind.
#[must_use]
#[allow(dead_code)]
pub(crate) fn hierarchy_preset_node_id(preset: MediaHierarchyPreset) -> &'static str {
    match preset {
        MediaHierarchyPreset::Local => HIERARCHY_MEDIA_ROOT_TEMPLATE,
        MediaHierarchyPreset::YtDlpChannel => HIERARCHY_YT_DLP_MEDIA_ROOT_TEMPLATE,
    }
}

/// Returns the sort id used for comparing hierarchy preset nodes.
#[must_use]
pub(crate) fn hierarchy_preset_sort_id(preset: MediaHierarchyPreset) -> &'static str {
    match preset {
        MediaHierarchyPreset::Local => "01",
        MediaHierarchyPreset::YtDlpChannel => "02",
    }
}

/// Compares two hierarchy node ids for ordering.
///
/// Returns `std::cmp::Ordering` based on the numeric prefix or string comparison.
#[must_use]
#[allow(dead_code)]
pub(crate) fn compare_hierarchy_ids(a: &str, b: &str) -> std::cmp::Ordering {
    a.cmp(b)
}

// ---------------------------------------------------------------------------
// Node building
// ---------------------------------------------------------------------------

/// Builds a hierarchy preset node for the given preset kind.
#[must_use]
pub(crate) fn build_hierarchy_preset_node(preset: MediaHierarchyPreset) -> HierarchyNode {
    let sort_id = hierarchy_preset_sort_id(preset);
    match preset {
        MediaHierarchyPreset::Local => HierarchyNode {
            id: Some(HIERARCHY_MEDIA_ROOT_TEMPLATE.to_string()),
            kind: HierarchyNodeKind::Folder,
            path: HierarchyPath::simple(&format!("{{{sort_id} Artists}}")),
            media_id: None,
            rename_files: Vec::new(),
            variant: None,
            variants: Vec::new(),
            format: PlaylistFormat::default(),
            ids: Vec::new(),
            sanitize_names: SanitizeNamesConfig::default(),
            children: vec![HierarchyNode {
                id: Some(HIERARCHY_ARTIST_FOLDER_TEMPLATE.to_string()),
                kind: HierarchyNodeKind::Folder,
                path: HierarchyPath::simple("{{artist}}"),
                media_id: None,
                rename_files: Vec::new(),
                variant: None,
                variants: Vec::new(),
                format: PlaylistFormat::default(),
                ids: Vec::new(),
                sanitize_names: SanitizeNamesConfig::default(),
                children: vec![HierarchyNode {
                    id: Some(HIERARCHY_ALBUM_FOLDER_TEMPLATE.to_string()),
                    kind: HierarchyNodeKind::Folder,
                    path: HierarchyPath::simple("{{album}}"),
                    media_id: None,
                    rename_files: Vec::new(),
                    variant: None,
                    variants: Vec::new(),
                    format: PlaylistFormat::default(),
                    ids: Vec::new(),
                    sanitize_names: SanitizeNamesConfig::default(),
                    children: vec![hierarchy_media_folder_node(None)],
                }],
            }],
        },
        MediaHierarchyPreset::YtDlpChannel => HierarchyNode {
            id: Some(HIERARCHY_YT_DLP_MEDIA_ROOT_TEMPLATE.to_string()),
            kind: HierarchyNodeKind::Folder,
            path: HierarchyPath::simple(&format!("{{{sort_id} YouTube}}")),
            media_id: None,
            rename_files: Vec::new(),
            variant: None,
            variants: Vec::new(),
            format: PlaylistFormat::default(),
            ids: Vec::new(),
            sanitize_names: SanitizeNamesConfig::default(),
            children: vec![HierarchyNode {
                id: Some(HIERARCHY_YT_DLP_PLAYLIST_EXTRACTOR.to_string()),
                kind: HierarchyNodeKind::Folder,
                path: HierarchyPath::template("{{playlist}}"),
                media_id: None,
                rename_files: Vec::new(),
                variant: None,
                variants: Vec::new(),
                format: PlaylistFormat::default(),
                ids: Vec::new(),
                sanitize_names: SanitizeNamesConfig::default(),
                children: vec![hierarchy_media_folder_node(None)],
            }],
        },
    }
}

/// Creates a media folder node for hierarchy presets.
#[must_use]
pub(crate) fn hierarchy_media_folder_node(media_id: Option<String>) -> HierarchyNode {
    HierarchyNode {
        id: Some(HIERARCHY_MEDIA_FOLDER_TEMPLATE.to_string()),
        kind: HierarchyNodeKind::MediaFolder,
        path: HierarchyPath::simple("{{title}}.{{ext}}"),
        media_id,
        rename_files: vec![HierarchyFolderRenameRule {
            pattern: "{{title}}.{{ext}}".to_string(),
            replacement: "{{title}}".to_string(),
        }],
        variant: None,
        variants: Vec::new(),
        format: PlaylistFormat::default(),
        ids: Vec::new(),
        sanitize_names: SanitizeNamesConfig::default(),
        children: vec![hierarchy_media_file_node(None)],
    }
}

/// Creates a media file node for hierarchy presets.
#[must_use]
pub(crate) fn hierarchy_media_file_node(media_id: Option<String>) -> HierarchyNode {
    HierarchyNode {
        id: Some(HIERARCHY_MEDIA_FILE_TEMPLATE.to_string()),
        kind: HierarchyNodeKind::Media,
        path: HierarchyPath::simple("{{title}}.{{ext}}"),
        media_id,
        rename_files: Vec::new(),
        variant: None,
        variants: Vec::new(),
        format: PlaylistFormat::default(),
        ids: Vec::new(),
        sanitize_names: SanitizeNamesConfig::default(),
        children: Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// Local and yt-dlp hierarchy children helpers
// ---------------------------------------------------------------------------

/// Builds the default children for a local-file media hierarchy entry.
#[must_use]
pub(crate) fn local_hierarchy_media_children() -> Vec<HierarchyNode> {
    vec![hierarchy_media_file_node(None)]
}

/// Builds the default children for a yt-dlp media hierarchy entry.
#[must_use]
#[allow(dead_code)]
pub(crate) fn yt_dlp_hierarchy_media_children() -> Vec<HierarchyNode> {
    vec![hierarchy_media_folder_node(None)]
}

// ---------------------------------------------------------------------------
// Insertion
// ---------------------------------------------------------------------------

/// Inserts a hierarchy preset node into an existing hierarchy.
///
/// The node is inserted based on the provided position. The hierarchy
/// is normalized to ensure consistent indentation and structure.
///
/// # Errors
///
/// Returns [`MediaPmError::Workflow`] if the position specifies a sibling
/// that does not exist in the hierarchy.
pub(crate) fn insert_hierarchy_preset_node(
    hierarchy: &mut Vec<HierarchyNode>,
    preset: MediaHierarchyPreset,
    position: AddInsertPosition,
) -> Result<(), MediaPmError> {
    let node = build_hierarchy_preset_node(preset);
    let existing_ids: Vec<&str> = hierarchy.iter().filter_map(|n| n.id.as_deref()).collect();

    let insert_index = match &position {
        AddInsertPosition::Sorted => {
            // Insert alphabetically based on id or empty-string fallback.
            let new_id = node.id.as_deref().unwrap_or("");
            let pos = hierarchy
                .iter()
                .position(|n| n.id.as_deref().unwrap_or("") > new_id)
                .unwrap_or(hierarchy.len());
            pos
        }
        AddInsertPosition::Beginning => 0,
        AddInsertPosition::End => hierarchy.len(),
    };

    // Check for duplicate id
    if let Some(ref node_id) = node.id {
        if existing_ids.contains(&node_id.as_str()) {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy node with id '{}' already exists",
                node_id
            )));
        }
    }

    hierarchy.insert(insert_index, node);
    Ok(())
}

// ---------------------------------------------------------------------------
// Removal
// ---------------------------------------------------------------------------

/// Removes hierarchy nodes matching a predicate, returning the count removed.
fn remove_hierarchy_nodes_by<F>(hierarchy: &mut Vec<HierarchyNode>, predicate: &F) -> usize
where
    F: Fn(&HierarchyNode) -> bool,
{
    let mut count = 0;
    let mut i = 0;
    while i < hierarchy.len() {
        if predicate(&hierarchy[i]) {
            count += 1;
            hierarchy.swap_remove(i);
        } else {
            count += remove_hierarchy_nodes_by(&mut hierarchy[i].children, predicate);
            i += 1;
        }
    }
    count
}

/// Removes hierarchy nodes matching the given node id, returning count removed.
#[must_use]
pub(crate) fn remove_hierarchy_nodes_by_id(
    hierarchy: &mut Vec<HierarchyNode>,
    node_id: &str,
) -> usize {
    remove_hierarchy_nodes_by(hierarchy, &|node| node.id.as_deref() == Some(node_id))
}

/// Removes hierarchy nodes matching the given media id, returning count removed.
#[must_use]
pub(crate) fn remove_hierarchy_nodes_by_media_id(
    hierarchy: &mut Vec<HierarchyNode>,
    media_id: &str,
) -> usize {
    remove_hierarchy_nodes_by(hierarchy, &|node| node.media_id.as_deref() == Some(media_id))
}

/// Checks if a hierarchy contains a node with the given id.
#[must_use]
#[allow(dead_code)]
pub(crate) fn hierarchy_contains_node_id(hierarchy: &[HierarchyNode], node_id: &str) -> bool {
    hierarchy.iter().any(|node| {
        if node.id.as_deref() == Some(node_id) {
            return true;
        }
        if node.children.is_empty() {
            false
        } else {
            hierarchy_contains_node_id(&node.children, node_id)
        }
    })
}

/// Normalizes the hierarchy folder root path based on the given preset.
#[must_use]
#[allow(dead_code)]
pub(crate) fn normalize_hierarchy_folder_root(
    path: &str,
    preset: Option<MediaHierarchyPreset>,
) -> String {
    match preset {
        Some(MediaHierarchyPreset::Local) => {
            if !path.contains("Artists") {
                format!("{{01 {}}}", path.trim_start_matches('{').trim_end_matches('}'))
            } else {
                path.to_string()
            }
        }
        Some(MediaHierarchyPreset::YtDlpChannel) => {
            if !path.contains("YouTube") {
                format!("{{02 {}}}", path.trim_start_matches('{').trim_end_matches('}'))
            } else {
                path.to_string()
            }
        }
        None => path.to_string(),
    }
}

/// Returns the default hierarchy folder root path for the given preset.
#[must_use]
#[allow(dead_code)]
pub(crate) fn default_hierarchy_folder_root_for_preset(
    preset: MediaHierarchyPreset,
) -> &'static str {
    match preset {
        MediaHierarchyPreset::Local => "{01 Artists}",
        MediaHierarchyPreset::YtDlpChannel => "{02 YouTube}",
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Ensures the local preset builds a valid hierarchy tree.
    #[test]
    fn build_local_preset_creates_valid_tree() {
        let node = build_hierarchy_preset_node(MediaHierarchyPreset::Local);
        assert_eq!(node.id.as_deref(), Some(HIERARCHY_MEDIA_ROOT_TEMPLATE));
        assert_eq!(node.kind, HierarchyNodeKind::Folder);
        assert!(!node.children.is_empty());
        assert_eq!(node.children.len(), 1);
        let artist = &node.children[0];
        assert_eq!(artist.id.as_deref(), Some(HIERARCHY_ARTIST_FOLDER_TEMPLATE));
    }

    /// Ensures the yt-dlp preset builds a valid hierarchy tree.
    #[test]
    fn build_yt_dlp_preset_creates_valid_tree() {
        let node = build_hierarchy_preset_node(MediaHierarchyPreset::YtDlpChannel);
        assert_eq!(node.id.as_deref(), Some(HIERARCHY_YT_DLP_MEDIA_ROOT_TEMPLATE));
        assert_eq!(node.kind, HierarchyNodeKind::Folder);
    }

    /// Ensures hierarchy_contains_node_id finds existing nodes.
    #[test]
    fn hierarchy_contains_node_id_finds_existing_node() {
        let hierarchy = build_hierarchy_preset_node(MediaHierarchyPreset::Local);
        assert!(hierarchy_contains_node_id(&hierarchy.children, HIERARCHY_ARTIST_FOLDER_TEMPLATE));
        assert!(!hierarchy_contains_node_id(&hierarchy.children, "nonexistent"));
    }

    /// Ensures remove_hierarchy_nodes_by_id removes the correct node.
    #[test]
    fn remove_hierarchy_nodes_by_id_removes_matching_node() {
        let mut hierarchy = vec![
            build_hierarchy_preset_node(MediaHierarchyPreset::Local),
            build_hierarchy_preset_node(MediaHierarchyPreset::YtDlpChannel),
        ];
        let count = remove_hierarchy_nodes_by_id(&mut hierarchy, HIERARCHY_MEDIA_ROOT_TEMPLATE);
        assert_eq!(count, 1);
        assert_eq!(hierarchy.len(), 1);
        assert_eq!(hierarchy[0].id.as_deref(), Some(HIERARCHY_YT_DLP_MEDIA_ROOT_TEMPLATE));
    }

    /// Ensures insert_hierarchy_preset_node inserts at the correct position.
    #[test]
    fn insert_hierarchy_preset_node_inserts_at_last_position() {
        let mut hierarchy = vec![build_hierarchy_preset_node(MediaHierarchyPreset::Local)];
        let result = insert_hierarchy_preset_node(
            &mut hierarchy,
            MediaHierarchyPreset::YtDlpChannel,
            AddInsertPosition::End,
        );
        assert!(result.is_ok());
        assert_eq!(hierarchy.len(), 2);
    }

    /// Ensures insert_hierarchy_preset_node rejects duplicate ids.
    #[test]
    fn insert_hierarchy_preset_node_rejects_duplicate_id() {
        let mut hierarchy = vec![];
        let r1 = insert_hierarchy_preset_node(
            &mut hierarchy,
            MediaHierarchyPreset::Local,
            AddInsertPosition::End,
        );
        assert!(r1.is_ok());
        let r2 = insert_hierarchy_preset_node(
            &mut hierarchy,
            MediaHierarchyPreset::Local,
            AddInsertPosition::End,
        );
        assert!(r2.is_err());
    }
}
