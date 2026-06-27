//! Cross-field validation for hierarchy declaration entries.
//!
//! Validates hierarchy structural invariants: media-id references, playlist
//! target resolution, path ordering, and duplicate detection beyond the
//! flattening constraints enforced in [`hierarchy_types`].

#![allow(dead_code)]
// TODO: Stream A stubs — wired when provisioning pipeline is complete.

use std::collections::BTreeSet;

use crate::error::MediaPmError;

use super::super::hierarchy_types::{HierarchyNode, HierarchyNodeKind, PlaylistItemRef};

/// Validates the full hierarchy declaration for cross-field consistency.
///
/// Checks:
/// - `media_id` entries reference declared media sources.
/// - Playlist `ids` reference known hierarchy ids.
/// - No duplicate ids at the same nesting level.
pub fn validate_hierarchy(
    hierarchy: &[HierarchyNode],
    known_media_ids: &BTreeSet<String>,
) -> Result<(), MediaPmError> {
    // Collect all hierarchy ids and validate node-level consistency.
    let mut hierarchy_ids = BTreeSet::new();
    validate_hierarchy_node_list(hierarchy, known_media_ids, &mut hierarchy_ids)?;

    // Validate playlist item references resolve to known hierarchy ids.
    for node in hierarchy {
        collect_playlist_references(node, &hierarchy_ids)?;
    }

    Ok(())
}

/// Recursively validates hierarchy nodes.
fn validate_hierarchy_node_list(
    nodes: &[HierarchyNode],
    known_media_ids: &BTreeSet<String>,
    hierarchy_ids: &mut BTreeSet<String>,
) -> Result<(), MediaPmError> {
    for node in nodes {
        // Validate id uniqueness.
        if let Some(ref id) = node.id
            && !hierarchy_ids.insert(id.clone())
        {
            return Err(MediaPmError::InvalidSource(format!(
                "duplicate hierarchy id '{id}' at the same nesting level"
            )));
        }

        // Validate media_id when the node kind requires one.
        match node.kind {
            HierarchyNodeKind::Media | HierarchyNodeKind::MediaFolder => {
                if let Some(ref media_id) = node.media_id {
                    if !known_media_ids.contains(media_id) {
                        return Err(MediaPmError::InvalidSource(format!(
                            "hierarchy references unknown media id '{media_id}'"
                        )));
                    }
                } else {
                    // TODO: Should nested media nodes inherit parent media_id?
                    // The current design requires explicit media_id on media
                    // and media_folder nodes.
                    return Err(MediaPmError::InvalidSource(format!(
                        "hierarchy node '{:?}' of kind '{}' must define media_id",
                        node.id,
                        match node.kind {
                            HierarchyNodeKind::Media => "media",
                            HierarchyNodeKind::MediaFolder => "media_folder",
                            _ => unreachable!(),
                        }
                    )));
                }
            }
            _ => {}
        }

        // Validate variant presence for media nodes.
        if node.kind == HierarchyNodeKind::Media && node.variant.is_none() {
            return Err(MediaPmError::InvalidSource(format!(
                "hierarchy node '{}' of kind 'media' must define variant",
                node.media_id.as_deref().unwrap_or("?")
            )));
        }

        // Validate variant list for media_folder.
        if node.kind == HierarchyNodeKind::MediaFolder && node.variants.is_empty() {
            return Err(MediaPmError::InvalidSource(format!(
                "hierarchy node '{}' of kind 'media_folder' must define at least one variant",
                node.media_id.as_deref().unwrap_or("?")
            )));
        }

        // Recurse into children.
        if !node.children.is_empty() {
            validate_hierarchy_node_list(&node.children, known_media_ids, hierarchy_ids)?;
        }
    }

    Ok(())
}

/// Validates playlist references resolve against known hierarchy ids.
fn collect_playlist_references(
    node: &HierarchyNode,
    hierarchy_ids: &BTreeSet<String>,
) -> Result<(), MediaPmError> {
    if node.kind != HierarchyNodeKind::Playlist {
        // Recurse into children.
        for child in &node.children {
            collect_playlist_references(child, hierarchy_ids)?;
        }
        return Ok(());
    }

    for item_ref in &node.ids {
        let target_id = match item_ref {
            PlaylistItemRef::Shorthand(id) | PlaylistItemRef::Object { id, .. } => id,
        };

        if !hierarchy_ids.contains(target_id) {
            return Err(MediaPmError::InvalidSource(format!(
                "playlist references unknown hierarchy id '{target_id}'"
            )));
        }
    }

    // Recurse into children.
    for child in &node.children {
        collect_playlist_references(child, hierarchy_ids)?;
    }

    Ok(())
}
