//! Hierarchy preset builder helpers for media library management.

use crate::config::{
    HierarchyFolderRenameRule, HierarchyNode, HierarchyNodeKind, HierarchyPath, PlaylistFormat,
    SanitizeNamesConfig,
};
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
    remove_hierarchy_nodes_by(nodes, &|node| {
        node.id.as_deref().is_some_and(|value| value == target_id)
    })
}

/// Removes all nodes in one recursive hierarchy tree whose `media_id` matches
/// `target_media_id` and returns the number of removed nodes.
pub(crate) fn remove_hierarchy_nodes_by_media_id(
    nodes: &mut Vec<HierarchyNode>,
    target_media_id: &str,
) -> usize {
    remove_hierarchy_nodes_by(nodes, &|node| {
        node.media_id.as_deref().is_some_and(|value| value == target_media_id)
    })
}

/// Generic recursive node removal by predicate.
fn remove_hierarchy_nodes_by<F>(nodes: &mut Vec<HierarchyNode>, predicate: &F) -> usize
where
    F: Fn(&HierarchyNode) -> bool,
{
    let mut removed = 0;
    let mut index = 0;
    while index < nodes.len() {
        if predicate(&nodes[index]) {
            nodes.remove(index);
            removed += 1;
            continue;
        }
        removed += remove_hierarchy_nodes_by(&mut nodes[index].children, predicate);
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
pub(crate) fn normalize_hierarchy_folder_root(folder: &str) -> Result<HierarchyPath, MediaPmError> {
    let normalized = folder.trim().replace('\\', "/").trim_matches('/').to_string();
    if normalized.is_empty() {
        return Err(MediaPmError::Workflow(
            "hierarchy preset folder must be non-empty".to_string(),
        ));
    }

    Ok(HierarchyPath::from(normalized.as_str()))
}

/// Returns default hierarchy root folder for one preset.
#[must_use]
pub(crate) fn default_hierarchy_folder_root_for_preset(
    preset: MediaHierarchyPreset,
) -> HierarchyPath {
    match preset {
        MediaHierarchyPreset::Local => HierarchyPath::from("music videos/local"),
        MediaHierarchyPreset::YtDlp => HierarchyPath::from("music videos/online"),
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
    normalized_folder: &HierarchyPath,
    position: AddInsertPosition,
    overwrite: bool,
) {
    // Remove existing nodes that will be overwritten.
    if overwrite {
        let ids_to_remove: Vec<String> = std::iter::once(node.id.clone())
            .flatten()
            .chain(node.children.iter().filter_map(|c| c.id.clone()))
            .collect();
        for id in &ids_to_remove {
            remove_hierarchy_nodes_by_id(hierarchy, id);
        }
    }

    // Do-not-overwrite: skip if any node id in the tree already exists
    // (only when overwrite is false).
    if !overwrite
        && (node.id.as_deref().is_some_and(|id| hierarchy_contains_node_id(hierarchy, id))
            || node.children.iter().any(|child| {
                child.id.as_deref().is_some_and(|id| hierarchy_contains_node_id(hierarchy, id))
            }))
    {
        return;
    }

    let matching_indices = hierarchy
        .iter()
        .enumerate()
        .filter_map(|(index, existing)| {
            (existing.kind == HierarchyNodeKind::Folder && existing.path == *normalized_folder)
                .then_some(index)
        })
        .collect::<Vec<_>>();

    if matching_indices.is_empty() {
        hierarchy.push(node);
        return;
    }

    // Merge scenario: incoming node has no identity (id + media_id both None)
    // and matches exactly one existing nameless container. Merge children into
    // the existing node to avoid duplicating the parent folder.
    if matching_indices.len() == 1
        && node.id.is_none()
        && node.media_id.is_none()
        && hierarchy[matching_indices[0]].id.is_none()
        && hierarchy[matching_indices[0]].media_id.is_none()
    {
        let target = &mut hierarchy[matching_indices[0]];
        match position {
            AddInsertPosition::Beginning => {
                let mut new_children = node.children;
                new_children.extend(std::mem::take(&mut target.children));
                target.children = new_children;
            }
            AddInsertPosition::End => {
                target.children.extend(node.children);
            }
            AddInsertPosition::Sorted => {
                for child in node.children {
                    let sort_key = hierarchy_preset_sort_id(&child);
                    let insert_at = target
                        .children
                        .iter()
                        .position(|existing| {
                            let existing_key = hierarchy_preset_sort_id(existing);
                            compare_hierarchy_ids(sort_key, existing_key).is_lt()
                        })
                        .unwrap_or(target.children.len());
                    target.children.insert(insert_at, child);
                }
            }
        }
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
        path: HierarchyPath::from(path),
        kind: HierarchyNodeKind::Media,
        id: None,
        media_id: Some(media_id.to_string()),
        variant: Some(variant.to_string()),
        variants: Vec::new(),
        rename_files: Vec::new(),
        format: PlaylistFormat::default(),
        ids: Vec::new(),
        sanitize_names: SanitizeNamesConfig::Inherit,
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
        path: HierarchyPath::from(path),
        kind: HierarchyNodeKind::MediaFolder,
        id: None,
        media_id: Some(media_id.to_string()),
        variant: None,
        variants,
        rename_files,
        format: PlaylistFormat::default(),
        ids: Vec::new(),
        sanitize_names: SanitizeNamesConfig::Inherit,
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
    // nodes to coexist at the same path as long as they have non-overlapping
    // variants or different rename_files rules (rename_files produce distinct
    // output filenames at materialization time). Only the sidecars/ folder should
    // use nested directory organization. All other output variants materialize
    // directly to prevent unnecessary intermediate nesting.

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

    // Extra thumbnails folder projection with folder-level rename so thumbnails
    // have both a `thumbnail.<ext>` named file (above) and a `folder.<ext>` file.
    let mut thumbnails_folder = hierarchy_media_folder_node(
        "",
        media_id,
        vec!["thumbnails".to_string()],
        vec![HierarchyFolderRenameRule {
            pattern: r"^.*\.([^.]*)$".to_string(),
            replacement: "folder.$1".to_string(),
        }],
    );
    thumbnails_folder.id = Some(format!("{media_id}.thumbnails.folder"));

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

    vec![video, archive, description, infojson, subtitles, thumbnails, thumbnails_folder, links]
}

/// Builds one hierarchy node tree for the selected preset.
#[must_use]
pub(crate) fn build_hierarchy_preset_node(
    preset: MediaHierarchyPreset,
    media_id: &str,
    normalized_folder: &HierarchyPath,
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
        path: normalized_folder.clone(),
        kind: HierarchyNodeKind::Folder,
        id: None,
        media_id: None,
        variant: None,
        variants: Vec::new(),
        rename_files: Vec::new(),
        format: PlaylistFormat::default(),
        ids: Vec::new(),
        sanitize_names: SanitizeNamesConfig::Inherit,
        children: vec![HierarchyNode {
            path: HierarchyPath::from(media_root_template.as_str()),
            kind: HierarchyNodeKind::Folder,
            id: Some(hierarchy_id),
            media_id: Some(media_id.to_string()),
            variant: None,
            variants: Vec::new(),
            rename_files: Vec::new(),
            format: PlaylistFormat::default(),
            ids: Vec::new(),
            sanitize_names: SanitizeNamesConfig::Inherit,
            children: media_children,
        }],
    }
}

#[cfg(test)]
mod tests {
    use crate::config::{
        HierarchyNode, HierarchyNodeKind, HierarchyPath, PlaylistFormat, SanitizeNamesConfig,
    };
    use crate::{AddInsertPosition, MediaHierarchyPreset};

    use super::{build_hierarchy_preset_node, insert_hierarchy_preset_node};

    /// Ensures adding a hierarchy preset merges into an existing nameless
    /// container folder at the same path instead of creating a duplicate
    /// sibling.
    #[test]
    fn add_hierarchy_preset_merges_into_existing_nameless_container() {
        let folder = HierarchyPath::from("music videos");
        let mut hierarchy = vec![HierarchyNode {
            path: folder.clone(),
            kind: HierarchyNodeKind::Folder,
            id: None,
            media_id: None,
            variant: None,
            variants: Vec::new(),
            rename_files: Vec::new(),
            format: PlaylistFormat::default(),
            ids: Vec::new(),
            sanitize_names: SanitizeNamesConfig::Inherit,
            children: vec![HierarchyNode {
                path: HierarchyPath::from("existing-media-root"),
                kind: HierarchyNodeKind::Folder,
                id: Some("existing-id".to_string()),
                media_id: Some("existing-media".to_string()),
                variant: None,
                variants: Vec::new(),
                rename_files: Vec::new(),
                format: PlaylistFormat::default(),
                ids: Vec::new(),
                sanitize_names: SanitizeNamesConfig::Inherit,
                children: Vec::new(),
            }],
        }];

        let inserted = build_hierarchy_preset_node(
            MediaHierarchyPreset::Local,
            "new-media",
            &folder,
            "new-media".to_string(),
        );
        insert_hierarchy_preset_node(
            &mut hierarchy,
            inserted,
            &folder,
            AddInsertPosition::End,
            false,
        );

        // Verify no duplicate folder: exactly one node at the target path.
        let matching: Vec<_> = hierarchy
            .iter()
            .filter(|node| node.kind == HierarchyNodeKind::Folder && node.path == folder)
            .collect();
        assert_eq!(matching.len(), 1, "should not create a duplicate container folder");

        // Verify both media roots are present as children of the same folder.
        let container = &matching[0];
        let child_ids: Vec<Option<&str>> =
            container.children.iter().map(|child| child.id.as_deref()).collect();
        assert!(
            child_ids.contains(&Some("existing-id")),
            "existing media root should still be present"
        );
        assert!(child_ids.contains(&Some("new-media")), "new media root should be present");
    }

    /// Ensures sorted hierarchy insertion places missing ids first, then empty
    /// ids, then lexicographically ordered non-empty ids within one root
    /// folder.
    #[test]
    fn add_hierarchy_preset_sorted_order_uses_missing_empty_then_id() {
        let root_folder = HierarchyPath::from("music videos/online");
        let mut hierarchy = vec![
            HierarchyNode {
                path: root_folder.clone(),
                kind: HierarchyNodeKind::Folder,
                id: None,
                media_id: None,
                variant: None,
                variants: Vec::new(),
                rename_files: Vec::new(),
                format: PlaylistFormat::default(),
                ids: Vec::new(),
                sanitize_names: SanitizeNamesConfig::Inherit,
                children: vec![HierarchyNode {
                    path: HierarchyPath::from("missing-id"),
                    kind: HierarchyNodeKind::Folder,
                    id: None,
                    media_id: None,
                    variant: None,
                    variants: Vec::new(),
                    rename_files: Vec::new(),
                    format: PlaylistFormat::default(),
                    ids: Vec::new(),
                    sanitize_names: SanitizeNamesConfig::Inherit,
                    children: Vec::new(),
                }],
            },
            HierarchyNode {
                path: root_folder.clone(),
                kind: HierarchyNodeKind::Folder,
                id: None,
                media_id: None,
                variant: None,
                variants: Vec::new(),
                rename_files: Vec::new(),
                format: PlaylistFormat::default(),
                ids: Vec::new(),
                sanitize_names: SanitizeNamesConfig::Inherit,
                children: vec![HierarchyNode {
                    path: HierarchyPath::from("empty-id"),
                    kind: HierarchyNodeKind::Folder,
                    id: Some(String::new()),
                    media_id: None,
                    variant: None,
                    variants: Vec::new(),
                    rename_files: Vec::new(),
                    format: PlaylistFormat::default(),
                    ids: Vec::new(),
                    sanitize_names: SanitizeNamesConfig::Inherit,
                    children: Vec::new(),
                }],
            },
            HierarchyNode {
                path: root_folder.clone(),
                kind: HierarchyNodeKind::Folder,
                id: None,
                media_id: None,
                variant: None,
                variants: Vec::new(),
                rename_files: Vec::new(),
                format: PlaylistFormat::default(),
                ids: Vec::new(),
                sanitize_names: SanitizeNamesConfig::Inherit,
                children: vec![HierarchyNode {
                    path: HierarchyPath::from("zzz-id"),
                    kind: HierarchyNodeKind::Folder,
                    id: Some("zzz".to_string()),
                    media_id: None,
                    variant: None,
                    variants: Vec::new(),
                    rename_files: Vec::new(),
                    format: PlaylistFormat::default(),
                    ids: Vec::new(),
                    sanitize_names: SanitizeNamesConfig::Inherit,
                    children: Vec::new(),
                }],
            },
        ];

        let inserted = build_hierarchy_preset_node(
            MediaHierarchyPreset::YtDlp,
            "aaa",
            &root_folder,
            "aaa".to_string(),
        );
        insert_hierarchy_preset_node(
            &mut hierarchy,
            inserted,
            &root_folder,
            AddInsertPosition::Sorted,
            false,
        );

        let observed_ids: Vec<Option<String>> = hierarchy
            .iter()
            .filter(|node| node.path == root_folder)
            .map(|node| node.children.first().and_then(|child| child.id.clone()))
            .collect();

        assert_eq!(
            observed_ids,
            vec![None, Some(String::new()), Some("aaa".to_string()), Some("zzz".to_string())]
        );
    }
}
