//! Hierarchy preset builder helpers for media library management.

use crate::config::{HierarchyFolderRenameRule, HierarchyNode, HierarchyNodeKind, PlaylistFormat};
use crate::error::MediaPmError;
use crate::{AddInsertPosition, MediaHierarchyPreset};

pub(crate) fn hierarchy_contains_node_id(nodes: &[HierarchyNode], target_id: &str) -> bool {
    nodes.iter().any(|node| {
        node.id.as_deref().is_some_and(|value| value == target_id)
            || hierarchy_contains_node_id(&node.children, target_id)
    })
}

/// Removes all nodes in one recursive hierarchy tree whose `id` matches
/// `target_id` and returns the number of removed nodes.
pub(crate) fn remove_hierarchy_nodes_by_id(
    nodes: &mut Vec<HierarchyNode>,
    target_id: &str,
) -> usize {
    let mut removed = 0;
    let mut index = 0;

    while index < nodes.len() {
        if nodes[index].id.as_deref().is_some_and(|value| value == target_id) {
            nodes.remove(index);
            removed += 1;
            continue;
        }

        removed += remove_hierarchy_nodes_by_id(&mut nodes[index].children, target_id);
        index += 1;
    }

    removed
}

/// Removes all nodes in one recursive hierarchy tree whose `media_id` matches
/// `target_media_id` and returns the number of removed nodes.
pub(crate) fn remove_hierarchy_nodes_by_media_id(
    nodes: &mut Vec<HierarchyNode>,
    target_media_id: &str,
) -> usize {
    let mut removed = 0;
    let mut index = 0;

    while index < nodes.len() {
        if nodes[index].media_id.as_deref().is_some_and(|value| value == target_media_id) {
            nodes.remove(index);
            removed += 1;
            continue;
        }

        removed += remove_hierarchy_nodes_by_media_id(&mut nodes[index].children, target_media_id);
        index += 1;
    }

    removed
}

/// Stable media-root folder template used by hierarchy presets.
pub(crate) const HIERARCHY_MEDIA_ROOT_TEMPLATE: &str = "${media.metadata.title} [${media.id}]";

/// Demo-style yt-dlp media-root folder template used by hierarchy presets.
pub(crate) const HIERARCHY_YT_DLP_MEDIA_ROOT_TEMPLATE: &str =
    "${media.metadata.artist} - ${media.metadata.title} [${media.id}]";

/// Stable tagged-media filename template used by hierarchy presets.
pub(crate) const HIERARCHY_TAGGED_MEDIA_FILE_TEMPLATE: &str =
    "${media.metadata.title} [${media.id}]${media.metadata.video_ext}";

/// Demo-style yt-dlp tagged-media filename template.
pub(crate) const HIERARCHY_YT_DLP_TAGGED_MEDIA_FILE_TEMPLATE: &str =
    "${media.metadata.artist} - ${media.metadata.title} [${media.id}]${media.metadata.video_ext}";

/// Demo-style yt-dlp info-json filename template.
pub(crate) const HIERARCHY_YT_DLP_INFOJSON_FILE_TEMPLATE: &str =
    "${media.metadata.artist} - ${media.metadata.title} [${media.id}].info.json";

/// Demo-style yt-dlp description filename template.
pub(crate) const HIERARCHY_YT_DLP_DESCRIPTION_FILE_TEMPLATE: &str =
    "${media.metadata.artist} - ${media.metadata.title} [${media.id}].description.txt";

/// Demo-style root-sidecar rename pattern for flattened file-family variants.
pub(crate) const HIERARCHY_YT_DLP_ROOT_RENAME_PATTERN: &str = "^.*\\.([^.]*)$";

/// Normalizes one hierarchy-root folder CLI value.
///
/// Returned values use slash separators and never carry surrounding slashes.
///
/// # Errors
///
/// Returns [`MediaPmError`] when the provided folder is empty after trimming.
pub(crate) fn normalize_hierarchy_folder_root(folder: &str) -> Result<String, MediaPmError> {
    let normalized = folder.trim().replace('\\', "/").trim_matches('/').to_string();
    if normalized.is_empty() {
        return Err(MediaPmError::Workflow(
            "hierarchy preset folder must be non-empty".to_string(),
        ));
    }

    Ok(normalized)
}

/// Returns default hierarchy root folder for one preset.
#[must_use]
pub(crate) fn default_hierarchy_folder_root_for_preset(
    preset: MediaHierarchyPreset,
) -> &'static str {
    match preset {
        MediaHierarchyPreset::Local => "music videos/local",
        MediaHierarchyPreset::YtDlp => "music videos/online",
    }
}

/// Returns sortable hierarchy-id text from one preset root node.
#[must_use]
pub(crate) fn hierarchy_preset_sort_id(node: &HierarchyNode) -> Option<&str> {
    node.children.first().and_then(|child| child.id.as_deref())
}

/// Compares two optional hierarchy ids with explicit missing-id precedence.
///
/// Ordering policy:
/// - missing id (`None`) comes first,
/// - empty id (`Some("")`) comes after missing,
/// - non-empty ids compare lexicographically.
#[must_use]
pub(crate) fn compare_hierarchy_ids(left: Option<&str>, right: Option<&str>) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    let rank = |value: Option<&str>| match value {
        None => 0u8,
        Some("") => 1u8,
        Some(_) => 2u8,
    };

    let left_rank = rank(left);
    let right_rank = rank(right);
    match left_rank.cmp(&right_rank) {
        Ordering::Equal => match (left, right) {
            (Some(left), Some(right)) => left.cmp(right),
            _ => Ordering::Equal,
        },
        other => other,
    }
}

/// Inserts one top-level hierarchy preset node according to the selected
/// insertion policy within the affected root-folder group.
pub(crate) fn insert_hierarchy_preset_node(
    hierarchy: &mut Vec<HierarchyNode>,
    node: HierarchyNode,
    normalized_folder: &str,
    position: AddInsertPosition,
) {
    let matching_indices = hierarchy
        .iter()
        .enumerate()
        .filter_map(|(index, existing)| {
            (existing.kind == HierarchyNodeKind::Folder && existing.path == normalized_folder)
                .then_some(index)
        })
        .collect::<Vec<_>>();

    if matching_indices.is_empty() {
        hierarchy.push(node);
        return;
    }

    match position {
        AddInsertPosition::Beginning => {
            hierarchy.insert(matching_indices[0], node);
        }
        AddInsertPosition::End => {
            hierarchy.insert(matching_indices[matching_indices.len() - 1] + 1, node);
        }
        AddInsertPosition::Sorted => {
            let new_id = hierarchy_preset_sort_id(&node);
            let insert_at = matching_indices
                .iter()
                .copied()
                .find(|index| {
                    let existing_id = hierarchy_preset_sort_id(&hierarchy[*index]);
                    compare_hierarchy_ids(new_id, existing_id).is_lt()
                })
                .unwrap_or_else(|| matching_indices[matching_indices.len() - 1] + 1);
            hierarchy.insert(insert_at, node);
        }
    }
}

/// Builds hierarchy id for one media-root folder.
#[must_use]
pub(crate) fn hierarchy_preset_node_id(media_id: &str) -> String {
    media_id.to_string()
}

/// Builds one media-file hierarchy node bound to one output variant.
#[must_use]
pub(crate) fn hierarchy_media_file_node(
    path: &str,
    media_id: &str,
    variant: &str,
) -> HierarchyNode {
    HierarchyNode {
        path: path.to_string(),
        kind: HierarchyNodeKind::Media,
        id: None,
        media_id: Some(media_id.to_string()),
        variant: Some(variant.to_string()),
        variants: Vec::new(),
        rename_files: Vec::new(),
        format: PlaylistFormat::default(),
        ids: Vec::new(),
        children: Vec::new(),
    }
}

/// Builds one media-folder hierarchy node bound to ordered variant selectors.
#[must_use]
pub(crate) fn hierarchy_media_folder_node(
    path: &str,
    media_id: &str,
    variants: Vec<String>,
    rename_files: Vec<HierarchyFolderRenameRule>,
) -> HierarchyNode {
    HierarchyNode {
        path: path.to_string(),
        kind: HierarchyNodeKind::MediaFolder,
        id: None,
        media_id: Some(media_id.to_string()),
        variant: None,
        variants,
        rename_files,
        format: PlaylistFormat::default(),
        ids: Vec::new(),
        children: Vec::new(),
    }
}

/// Builds media-root children for the local hierarchy preset.
#[must_use]
pub(crate) fn local_hierarchy_media_children(media_id: &str) -> Vec<HierarchyNode> {
    let mut media =
        hierarchy_media_file_node(HIERARCHY_TAGGED_MEDIA_FILE_TEMPLATE, media_id, "media");
    media.id = Some(format!("{media_id}.media"));
    vec![media]
}

/// Builds media-root children for the yt-dlp hierarchy preset.
#[must_use]
pub(crate) fn yt_dlp_hierarchy_media_children(media_id: &str) -> Vec<HierarchyNode> {
    let mut video =
        hierarchy_media_file_node(HIERARCHY_YT_DLP_TAGGED_MEDIA_FILE_TEMPLATE, media_id, "video");
    video.id = Some(format!("{media_id}.video"));

    let mut archive = hierarchy_media_file_node(
        "${media.metadata.artist} - ${media.metadata.title} [${media.id}].archive.txt",
        media_id,
        "archive",
    );
    archive.id = Some(format!("{media_id}.archive"));

    let mut description = hierarchy_media_file_node(
        HIERARCHY_YT_DLP_DESCRIPTION_FILE_TEMPLATE,
        media_id,
        "description",
    );
    description.id = Some(format!("{media_id}.description"));

    let mut infojson =
        hierarchy_media_file_node(HIERARCHY_YT_DLP_INFOJSON_FILE_TEMPLATE, media_id, "infojson");
    infojson.id = Some(format!("{media_id}.infojson"));

    // Separate media folder nodes for each output variant family, all materialized
    // directly to media root with path="" (empty). Validation allows these separate
    // nodes to coexist at the same path because each has non-overlapping variants.
    // Only the sidecars/ folder should use nested directory organization.
    // All other output variants materialize directly to prevent unnecessary intermediate nesting.

    // Subtitles materialized directly to media root with path="" (empty)
    let mut subtitles = hierarchy_media_folder_node(
        "",
        media_id,
        vec!["subtitles".to_string()],
        vec![HierarchyFolderRenameRule {
            pattern: "^(?:.*/)?(?:.*\\.)?([^.\\/]+)\\.([^.\\/]+)$".to_string(),
            replacement: "${media.metadata.artist} - ${media.metadata.title} [${media.id}].$1.$2"
                .to_string(),
        }],
    );
    subtitles.id = Some(format!("{media_id}.subtitles"));

    // Thumbnails materialized directly to media root with path="" (empty)
    let mut thumbnails = hierarchy_media_folder_node(
        "",
        media_id,
        vec!["thumbnails".to_string()],
        vec![HierarchyFolderRenameRule {
            pattern: HIERARCHY_YT_DLP_ROOT_RENAME_PATTERN.to_string(),
            replacement:
                "${media.metadata.artist} - ${media.metadata.title} [${media.id}].thumbnail.$1"
                    .to_string(),
        }],
    );
    thumbnails.id = Some(format!("{media_id}.thumbnails"));

    // Links materialized directly to media root with path="" (empty)
    let mut links = hierarchy_media_folder_node(
        "",
        media_id,
        vec!["links".to_string()],
        vec![HierarchyFolderRenameRule {
            pattern: HIERARCHY_YT_DLP_ROOT_RENAME_PATTERN.to_string(),
            replacement: "${media.metadata.artist} - ${media.metadata.title} [${media.id}].link.$1"
                .to_string(),
        }],
    );
    links.id = Some(format!("{media_id}.links"));

    vec![video, archive, description, infojson, subtitles, thumbnails, links]
}

/// Builds one hierarchy node tree for the selected preset.
#[must_use]
pub(crate) fn build_hierarchy_preset_node(
    preset: MediaHierarchyPreset,
    media_id: &str,
    normalized_folder: &str,
    hierarchy_id: String,
) -> HierarchyNode {
    let (media_root_template, media_children) = match preset {
        MediaHierarchyPreset::Local => {
            (HIERARCHY_MEDIA_ROOT_TEMPLATE.to_string(), local_hierarchy_media_children(media_id))
        }
        MediaHierarchyPreset::YtDlp => (
            HIERARCHY_YT_DLP_MEDIA_ROOT_TEMPLATE.to_string(),
            yt_dlp_hierarchy_media_children(media_id),
        ),
    };

    HierarchyNode {
        path: normalized_folder.to_string(),
        kind: HierarchyNodeKind::Folder,
        id: None,
        media_id: None,
        variant: None,
        variants: Vec::new(),
        rename_files: Vec::new(),
        format: PlaylistFormat::default(),
        ids: Vec::new(),
        children: vec![HierarchyNode {
            path: media_root_template,
            kind: HierarchyNodeKind::Folder,
            id: Some(hierarchy_id),
            media_id: Some(media_id.to_string()),
            variant: None,
            variants: Vec::new(),
            rename_files: Vec::new(),
            format: PlaylistFormat::default(),
            ids: Vec::new(),
            children: media_children,
        }],
    }
}
