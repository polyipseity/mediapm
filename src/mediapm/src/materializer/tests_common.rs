//! Fixtures and helpers the materializer's themed test modules share.

use crate::config::hierarchy_types::{
    HierarchyNode, HierarchyNodeKind, HierarchyPath, PlaylistFormat, SanitizeNamesConfig,
};
use crate::config::source_types::{MediaStep, MediaStepTool};
use crate::config::{GenericOutputVariantConfig, OutputVariantValue};
use mediapm_utils::progress::recording::{ProgressOp, RecordingProgressTracker};

use super::*;

/// The phase tag a rendered prefix ends with, or `None` when it carries
/// none, which is how a row is checked against its declaration.
pub(super) fn rendered_phase_tag(prefix: &str) -> Option<&str> {
    prefix.rsplit_once('[').and_then(|(_, tail)| tail.strip_suffix(']'))
}

/// Builds a ZIP payload holding `members`, stored without compression.
///
/// Stored rather than deflated on purpose: it keeps the archive readable
/// back through the same `ZipArchive` the materializer uses, and it does
/// not ask for a compression backend the crate's features do not name.
pub(super) fn zip_payload(members: &[(&str, &[u8])]) -> Vec<u8> {
    use std::io::Write as _;

    let mut buffer = std::io::Cursor::new(Vec::new());
    let mut writer = zip::ZipWriter::new(&mut buffer);
    for (name, content) in members {
        writer
            .start_file::<&str, ()>(name, zip::write::SimpleFileOptions::default())
            .expect("a stored member starts");
        writer.write_all(content).expect("the member is written");
    }
    writer.finish().expect("the archive closes");
    buffer.into_inner()
}

/// A playlist at `playlists/broken.m3u8` whose only item names a hierarchy
/// id no media entry declares, so the arm fails while resolving references
/// and never reaches the write.
pub(super) fn playlist_with_unknown_reference_document() -> MediaPmDocument {
    MediaPmDocument {
        hierarchy: vec![HierarchyNode {
            path: HierarchyPath::from("playlists"),
            kind: HierarchyNodeKind::Folder,
            id: None,
            media_id: None,
            variant: None,
            variants: vec![],
            rename_files: vec![],
            format: PlaylistFormat::M3u8,
            ids: vec![],
            sanitize_names: Some(SanitizeNamesConfig::Inherit),
            children: vec![HierarchyNode {
                path: HierarchyPath::from("broken.m3u8"),
                kind: HierarchyNodeKind::Playlist,
                id: None,
                media_id: None,
                variant: None,
                variants: vec![],
                rename_files: vec![],
                format: PlaylistFormat::M3u8,
                ids: vec![PlaylistItemRef::Shorthand("absent.local.1".to_string())],
                sanitize_names: Some(SanitizeNamesConfig::Inherit),
                children: vec![],
            }],
        }],
        ..MediaPmDocument::default()
    }
}

/// A media folder at `album` whose single variant writes one file, and a
/// playlist at `playlists/rickroll.m3u8` that references the media entry
/// beside it.
///
/// The playlist names the media entry by its hierarchy id, which is why
/// the media node carries an explicit `id`.
pub(super) fn folder_and_playlist_document(variant_hash: &str) -> MediaPmDocument {
    let media_node = |id: Option<&str>| HierarchyNode {
        path: HierarchyPath::simple("song"),
        kind: HierarchyNodeKind::Media,
        id: id.map(str::to_string),
        media_id: Some("src1".to_string()),
        variant: Some("default".into()),
        variants: vec![],
        rename_files: vec![],
        format: PlaylistFormat::M3u8,
        ids: vec![],
        sanitize_names: Some(SanitizeNamesConfig::Inherit),
        children: vec![],
    };
    MediaPmDocument {
        media: BTreeMap::from([(
            "src1".to_string(),
            MediaSourceSpec {
                steps: vec![MediaStep {
                    tool: MediaStepTool::Import,
                    input_variants: vec![],
                    output_variants: BTreeMap::from([(
                        "default".into(),
                        OutputVariantValue::Generic(GenericOutputVariantConfig {
                            kind: "primary".to_string(),
                            ..Default::default()
                        }),
                    )]),
                    options: BTreeMap::new(),
                }],
                variant_hashes: BTreeMap::from([("default".to_string(), variant_hash.into())]),
                ..MediaSourceSpec::default()
            },
        )]),
        hierarchy: vec![
            media_node(Some("song.local.1")),
            HierarchyNode {
                path: HierarchyPath::simple("album"),
                kind: HierarchyNodeKind::MediaFolder,
                id: None,
                media_id: Some("src1".to_string()),
                variant: Some("default".into()),
                variants: vec![],
                rename_files: vec![],
                format: PlaylistFormat::M3u8,
                ids: vec![],
                sanitize_names: Some(SanitizeNamesConfig::Inherit),
                children: vec![],
            },
            HierarchyNode {
                path: HierarchyPath::from("playlists"),
                kind: HierarchyNodeKind::Folder,
                id: None,
                media_id: None,
                variant: None,
                variants: vec![],
                rename_files: vec![],
                format: PlaylistFormat::M3u8,
                ids: vec![],
                sanitize_names: Some(SanitizeNamesConfig::Inherit),
                children: vec![HierarchyNode {
                    path: HierarchyPath::from("rickroll.m3u8"),
                    kind: HierarchyNodeKind::Playlist,
                    id: None,
                    media_id: None,
                    variant: None,
                    variants: vec![],
                    rename_files: vec![],
                    format: PlaylistFormat::M3u8,
                    ids: vec![PlaylistItemRef::Shorthand("song.local.1".to_string())],
                    sanitize_names: Some(SanitizeNamesConfig::Inherit),
                    children: vec![],
                }],
            },
        ],
        ..MediaPmDocument::default()
    }
}

/// Builds a one-media-entry document whose hierarchy path is `path`.
pub(super) fn single_media_document(media_id: &str, path: HierarchyPath) -> MediaPmDocument {
    MediaPmDocument {
        media: BTreeMap::from([(
            media_id.to_string(),
            MediaSourceSpec {
                steps: vec![MediaStep {
                    tool: MediaStepTool::Import,
                    input_variants: vec![],
                    output_variants: BTreeMap::from([(
                        "default".into(),
                        OutputVariantValue::Generic(GenericOutputVariantConfig {
                            kind: "primary".to_string(),
                            ..Default::default()
                        }),
                    )]),
                    options: BTreeMap::new(),
                }],
                ..MediaSourceSpec::default()
            },
        )]),
        hierarchy: vec![HierarchyNode {
            path,
            kind: HierarchyNodeKind::Media,
            id: None,
            media_id: Some(media_id.to_string()),
            variant: Some("default".into()),
            variants: vec![],
            rename_files: vec![],
            format: PlaylistFormat::M3u8,
            ids: vec![],
            sanitize_names: Some(SanitizeNamesConfig::Inherit),
            children: vec![],
        }],
        ..MediaPmDocument::default()
    }
}

/// A one-media-entry document whose variant resolves from `variant_hash`, so
/// a `sync_hierarchy` over it writes rather than finding nothing to commit.
///
/// Derived from [`folder_and_playlist_document`] by dropping the nodes the
/// materializer would reach through a different arm, which leaves the single
/// media entry the short-circuit tests re-sync.
pub(super) fn resolvable_media_document(variant_hash: &str) -> MediaPmDocument {
    let mut document = folder_and_playlist_document(variant_hash);
    document.hierarchy.retain(|node| matches!(node.kind, HierarchyNodeKind::Media));
    document
}

/// Opens a CAS under the workspace runtime root for a `sync_hierarchy` call.
pub(super) async fn open_hierarchy_cas(paths: &MediaPmPaths) -> FileSystemCas {
    let cas_root = paths.runtime_root.join("store");
    tokio::fs::create_dir_all(&cas_root).await.unwrap();
    FileSystemCas::open(&cas_root).await.unwrap()
}

/// The terminal op of one handle's own bar, or `None` when it never finished.
///
/// Keyed on the [`BarId`] of the handle rather than found by scanning, because
/// the overall bar and the entry bars share one op vocabulary and an
/// unfiltered search reports whichever finished first.
pub(super) fn overall_finish(
    tracker: &RecordingProgressTracker,
    overall: &mediapm_utils::progress::recording::RecordingTrackedHandle,
) -> Option<ProgressOp> {
    let bar = overall.bar();
    tracker.recorded().iter().find_map(|entry| {
        if entry.bar != bar {
            return None;
        }
        match entry.op {
            ProgressOp::FinishSuccess | ProgressOp::FinishWarning | ProgressOp::FinishError => {
                Some(entry.op.clone())
            }
            _ => None,
        }
    })
}
