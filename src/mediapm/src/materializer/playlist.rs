//! Playlist index collection, path resolution, and format rendering.

use std::collections::BTreeMap;

use crate::config::{
    FlattenedHierarchyEntry, HierarchyEntry, HierarchyEntryKind, MediaPmDocument, PlaylistFormat,
};
use crate::error::MediaPmError;
use crate::paths::MediaPmPaths;

use super::metadata::resolve_hierarchy_relative_path;
use super::{MaterializationLookupContext, RenderedPlaylistItem};

/// Collects effective hierarchy-id -> hierarchy media-path mappings.
pub(super) fn collect_playlist_media_index(
    flattened_hierarchy: &[FlattenedHierarchyEntry],
) -> Result<BTreeMap<String, Vec<String>>, MediaPmError> {
    let mut index = BTreeMap::new();

    for flattened_entry in flattened_hierarchy {
        if !matches!(flattened_entry.entry.kind, HierarchyEntryKind::Media) {
            continue;
        }

        let Some(hierarchy_id) = flattened_entry.hierarchy_id.as_deref() else {
            continue;
        };

        if let Some(previous_path_components) =
            index.insert(hierarchy_id.to_string(), flattened_entry.path_components.clone())
            && previous_path_components != flattened_entry.path_components
        {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy id '{}' resolves to multiple media paths ('{}' and '{}')",
                hierarchy_id,
                previous_path_components.join("/"),
                flattened_entry.path_str()
            )));
        }
    }

    Ok(index)
}

/// Collects hierarchy id → HierarchyEntry mapping for playlist resolution.
///
/// Only includes media entries with an explicit hierarchy id and non-empty
/// media_id. Keyed by hierarchy id so playlist resolution can look up
/// the correct entry regardless of template path.
pub(super) fn collect_media_entries_by_id(
    flattened_hierarchy: &[FlattenedHierarchyEntry],
) -> BTreeMap<String, HierarchyEntry> {
    let mut entries = BTreeMap::new();

    for flattened_entry in flattened_hierarchy {
        let entry = &flattened_entry.entry;

        if !matches!(entry.kind, HierarchyEntryKind::Media) || entry.media_id.trim().is_empty() {
            continue;
        }

        let Some(hierarchy_id) = flattened_entry.hierarchy_id.as_deref() else {
            continue;
        };

        entries.insert(hierarchy_id.to_string(), entry.clone());
    }

    entries
}

/// Resolves one media output relative path used by playlist generation.
///
/// Keyed by requested hierarchy id to avoid cache collisions when different
/// media entries share the same template path.
pub(super) async fn resolve_playlist_media_target_relative_path(
    document: &MediaPmDocument,
    lookup: &MaterializationLookupContext,
    path_components: &[String],
    requested_id: &str,
    media_entries_by_id: &BTreeMap<String, HierarchyEntry>,
    cache: &mut BTreeMap<String, String>,
) -> Result<String, MediaPmError> {
    if let Some(cached) = cache.get(requested_id) {
        return Ok(cached.clone());
    }

    let entry = media_entries_by_id.get(requested_id).ok_or_else(|| {
        MediaPmError::Workflow(format!(
            "playlist resolution references unknown hierarchy id '{requested_id}'"
        ))
    })?;
    let source = document.media.get(entry.media_id.as_str()).ok_or_else(|| {
        MediaPmError::Workflow(format!(
            "playlist resolution references unknown media '{}'",
            entry.media_id
        ))
    })?;

    let resolved_components =
        resolve_hierarchy_relative_path(path_components, entry, source, lookup).await?;
    let resolved = resolved_components.join("/");

    cache.insert(requested_id.to_string(), resolved.clone());
    Ok(resolved)
}

/// Renders one playlist-relative path from playlist file to media target file.
pub(super) fn render_relative_playlist_path(playlist_path: &str, target_path: &str) -> String {
    let mut playlist_components = normalize_path_components(playlist_path);
    if !playlist_components.is_empty() {
        let _ = playlist_components.pop();
    }
    let target_components = normalize_path_components(target_path);

    let mut shared_prefix = 0usize;
    while shared_prefix < playlist_components.len()
        && shared_prefix < target_components.len()
        && playlist_components[shared_prefix] == target_components[shared_prefix]
    {
        shared_prefix = shared_prefix.saturating_add(1);
    }

    let mut relative_components = Vec::new();
    for _ in shared_prefix..playlist_components.len() {
        relative_components.push("..".to_string());
    }
    relative_components.extend(target_components[shared_prefix..].iter().cloned());

    relative_components.join("/")
}

/// Normalizes one path string into non-empty slash-separated components.
fn normalize_path_components(path: &str) -> Vec<String> {
    path.replace('\\', "/")
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>()
}

/// Joins two managed relative path segments using forward slashes.
#[must_use]
pub(super) fn join_relative_paths(base: &str, child: &str) -> String {
    let normalized_base = base.trim_end_matches(['/', '\\']).replace('\\', "/");
    let normalized_child = child.trim_start_matches(['/', '\\']);
    if normalized_base.is_empty() {
        normalized_child.to_string()
    } else {
        format!("{normalized_base}/{normalized_child}")
    }
}

/// Renders absolute playlist path text from one library-relative target path.
pub(super) fn render_absolute_playlist_path(paths: &MediaPmPaths, target_path: &str) -> String {
    let absolute = paths.hierarchy_root_dir.join(target_path);
    absolute.to_string_lossy().replace('\\', "/")
}

/// Renders playlist payload bytes for one configured format.
pub(super) fn render_playlist_bytes(
    format: PlaylistFormat,
    items: &[RenderedPlaylistItem],
) -> Vec<u8> {
    let content = match format {
        PlaylistFormat::M3u8 | PlaylistFormat::M3u => {
            let mut rendered = String::from("#EXTM3U\n");
            for item in items {
                rendered.push_str(item.path.as_str());
                rendered.push('\n');
            }
            rendered
        }
        PlaylistFormat::Pls => {
            let mut rendered = String::from("[playlist]\n");
            for (index, item) in items.iter().enumerate() {
                let item_number = index.saturating_add(1);
                rendered.push_str(format!("File{item_number}={}\n", item.path).as_str());
                rendered.push_str(format!("Title{item_number}={}\n", item.id).as_str());
                rendered.push_str(format!("Length{item_number}=-1\n").as_str());
            }
            rendered.push_str(format!("NumberOfEntries={}\n", items.len()).as_str());
            rendered.push_str("Version=2\n");
            rendered
        }
        PlaylistFormat::Xspf => {
            let mut rendered = String::from(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<playlist version=\"1\" xmlns=\"http://xspf.org/ns/0/\">\n  <trackList>\n",
            );
            for item in items {
                let location = escape_xml(item.path.as_str());
                let title = escape_xml(item.id.as_str());
                rendered.push_str("    <track>\n");
                rendered.push_str(format!("      <title>{title}</title>\n").as_str());
                rendered.push_str(format!("      <location>{location}</location>\n").as_str());
                rendered.push_str("    </track>\n");
            }
            rendered.push_str("  </trackList>\n</playlist>\n");
            rendered
        }
        PlaylistFormat::Wpl => {
            let mut rendered =
                String::from("<?wpl version=\"1.0\"?>\n<smil>\n  <body>\n    <seq>\n");
            for item in items {
                let source = escape_xml(item.path.as_str());
                rendered.push_str(format!("      <media src=\"{source}\" />\n").as_str());
            }
            rendered.push_str("    </seq>\n  </body>\n</smil>\n");
            rendered
        }
        PlaylistFormat::Asx => {
            let mut rendered = String::from("<asx version=\"3.0\">\n");
            for item in items {
                let source = escape_xml(item.path.as_str());
                let title = escape_xml(item.id.as_str());
                rendered.push_str("  <entry>\n");
                rendered.push_str(format!("    <title>{title}</title>\n").as_str());
                rendered.push_str(format!("    <ref href=\"{source}\" />\n").as_str());
                rendered.push_str("  </entry>\n");
            }
            rendered.push_str("</asx>\n");
            rendered
        }
    };

    content.into_bytes()
}

/// Returns stable format label for lockfile provenance records.
pub(super) const fn playlist_format_label(format: PlaylistFormat) -> &'static str {
    match format {
        PlaylistFormat::M3u8 => "m3u8",
        PlaylistFormat::M3u => "m3u",
        PlaylistFormat::Pls => "pls",
        PlaylistFormat::Xspf => "xspf",
        PlaylistFormat::Wpl => "wpl",
        PlaylistFormat::Asx => "asx",
    }
}

/// Escapes XML-special characters in one text value.
fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
