use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::sync::{Mutex, OnceLock};

/// Serialization guard for tests that race on shared tempdir / CAS state.
static SERIAL_GUARD: OnceLock<Mutex<()>> = OnceLock::new();

use mediapm_cas::{CasApi, FileSystemCas, Hash};
use mediapm_conductor::model::config::ImpureTimestamp;
use mediapm_conductor::model::config::ToolInputKind;
use mediapm_conductor::{
    InputBinding, MachineNickelDocument, OrchestrationState, OutputCaptureSpec, OutputPolicy,
    OutputRef, OutputSaveMode, PersistenceFlags, StateNickelDocument, ToolCallInstance,
    ToolInputSpec, ToolKindSpec, ToolOutputSpec, ToolSpec, WorkflowSpec, WorkflowStepSpec,
    encode_state_document,
};
use unicode_normalization::UnicodeNormalization;

use crate::config::{
    HierarchyEntry, HierarchyEntryKind, HierarchyFolderRenameRule, HierarchyNode,
    HierarchyNodeKind, MaterializationMethod, MediaMetadataRegexTransform, MediaMetadataValue,
    MediaMetadataVariantBinding, MediaPmDocument, MediaSourceSpec, MediaStep, MediaStepTool,
    PlaylistEntryPathMode, PlaylistFormat, PlaylistItemRef, SanitizeNamesConfig,
    TransformInputValue,
};
use crate::lockfile::MediaLockFile;
use crate::paths::MediaPmPaths;

use super::{
    instance_matches_expected_inputs, resolve_managed_ffprobe_path, sanitize_hierarchy_path,
    sync_hierarchy, validate_hierarchy_path,
};

/// Protects managed ffprobe metadata lookup by resolving relative command
/// selectors under the active managed ffmpeg payload root.
#[test]
fn resolve_managed_ffprobe_path_anchors_relative_selector_to_tool_root() {
    let temp = tempfile::tempdir().expect("tempdir");
    let paths = MediaPmPaths::from_root(temp.path());

    let ffmpeg_tool_id = "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@demo".to_string();
    let mut machine = MachineNickelDocument::default();
    machine.tools.insert(
        ffmpeg_tool_id.clone(),
        ToolSpec {
            kind: ToolKindSpec::Executable {
                command: vec![
                    "windows/ffmpeg-master-latest-win64-gpl-shared/bin/ffmpeg.exe".to_string(),
                ],
                env_vars: BTreeMap::new(),
                success_codes: vec![0],
            },
            ..ToolSpec::default()
        },
    );

    let managed_ffprobe_path = paths
        .tools_dir
        .join(&ffmpeg_tool_id)
        .join("payload/windows/ffmpeg-master-latest-win64-gpl-shared/bin")
        .join(if cfg!(windows) { "ffprobe.exe" } else { "ffprobe" });
    fs::create_dir_all(managed_ffprobe_path.parent().expect("ffprobe parent directory"))
        .expect("create managed ffprobe parent directory");
    fs::write(&managed_ffprobe_path, b"stub").expect("write managed ffprobe stub");

    let mut lock = MediaLockFile::default();
    lock.active_tools.insert("ffmpeg".to_string(), ffmpeg_tool_id.clone());

    let resolved = resolve_managed_ffprobe_path(&paths, &machine, &lock)
        .expect("managed ffprobe path should resolve");
    let expected_file_name = if cfg!(windows) { "ffprobe.exe" } else { "ffprobe" };
    let expected = paths
        .tools_dir
        .join(ffmpeg_tool_id)
        .join("payload/windows/ffmpeg-master-latest-win64-gpl-shared/bin")
        .join(expected_file_name);

    assert_eq!(resolved, expected);
}

/// Protects managed ffprobe lookup when only payload-layout binaries exist.
#[test]
fn resolve_managed_ffprobe_path_requires_payload_layout() {
    let temp = tempfile::tempdir().expect("tempdir");
    let paths = MediaPmPaths::from_root(temp.path());

    let ffmpeg_tool_id = "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@demo".to_string();
    let mut machine = MachineNickelDocument::default();
    machine.tools.insert(
        ffmpeg_tool_id.clone(),
        ToolSpec {
            kind: ToolKindSpec::Executable {
                command: vec![
                    "windows/ffmpeg-master-latest-win64-gpl-shared/bin/ffmpeg.exe".to_string(),
                ],
                env_vars: BTreeMap::new(),
                success_codes: vec![0],
            },
            ..ToolSpec::default()
        },
    );

    let payload_ffprobe_path = paths
        .tools_dir
        .join(&ffmpeg_tool_id)
        .join("payload/windows/ffmpeg-master-latest-win64-gpl-shared/bin")
        .join(if cfg!(windows) { "ffprobe.exe" } else { "ffprobe" });
    fs::create_dir_all(payload_ffprobe_path.parent().expect("ffprobe parent directory"))
        .expect("create payload managed ffprobe parent directory");
    fs::write(&payload_ffprobe_path, b"stub").expect("write payload managed ffprobe stub");

    let mut lock = MediaLockFile::default();
    lock.active_tools.insert("ffmpeg".to_string(), ffmpeg_tool_id.clone());

    let resolved = resolve_managed_ffprobe_path(&paths, &machine, &lock)
        .expect("managed ffprobe payload path should resolve");

    assert_eq!(resolved, payload_ffprobe_path);
}

fn yt_dlp_output_variant(kind: &str) -> serde_json::Value {
    serde_json::json!({ "kind": kind, "save": "full" })
}

/// Builds one in-memory ZIP payload from relative file entries.
fn zip_payload(entries: &[(&str, &[u8])]) -> Vec<u8> {
    let mut bytes = Vec::new();
    {
        let cursor = std::io::Cursor::new(&mut bytes);
        let mut writer = zip::ZipWriter::new(cursor);
        let options = zip::write::SimpleFileOptions::default();

        for (path, payload) in entries {
            writer.start_file(path, options).expect("start zip file entry");
            std::io::Write::write_all(&mut writer, payload).expect("write zip file entry");
        }

        writer.finish().expect("finish zip payload");
    }
    bytes
}

/// Ensures two files are realized as one hardlinked inode/file record.
fn assert_hardlinked_paths(source_path: &Path, destination_path: &Path) {
    assert!(
        same_file::is_same_file(source_path, destination_path)
            .expect("compare hardlinked file identity"),
        "source '{}' should share the same backing file identity as destination '{}'",
        source_path.display(),
        destination_path.display()
    );
}

/// Protects default-order behavior by preferring hard links when available.
#[tokio::test]
async fn materialize_file_from_cas_with_order_prefers_hardlink_when_available() {
    let temp = tempfile::tempdir().expect("tempdir");
    let cas_root = temp.path().join("store");
    let cas = FileSystemCas::open(&cas_root).await.expect("open cas");

    let payload = b"hardlink-preferred".to_vec();
    let hash = cas.put(payload.clone()).await.expect("put bytes");
    let destination_path = temp.path().join("materialized.bin");

    let mut notices = Vec::new();
    super::materialize_file_from_cas_with_order(
        &cas,
        hash,
        &destination_path,
        "library/materialized.bin",
        &[
            MaterializationMethod::Hardlink,
            MaterializationMethod::Symlink,
            MaterializationMethod::Reflink,
            MaterializationMethod::Copy,
        ],
        &mut notices,
    )
    .await
    .expect("materialize with preferred hardlink");

    let source_path = cas.object_path_for_hash(hash);
    assert_hardlinked_paths(&source_path, &destination_path);
    assert_eq!(std::fs::read(&destination_path).expect("read destination"), payload);
    assert!(notices.is_empty(), "hardlink-first success should not emit fallback notices");
}

/// Protects fallback ordering by continuing after failed methods.
#[tokio::test]
async fn materialize_file_from_cas_with_order_falls_back_to_copy() {
    let temp = tempfile::tempdir().expect("tempdir");
    let cas_root = temp.path().join("store");
    let cas = FileSystemCas::open(&cas_root).await.expect("open cas");

    let payload = b"copy-fallback".to_vec();
    let hash = cas.put(payload.clone()).await.expect("put bytes");
    let destination_path = temp.path().join("fallback.bin");

    let mut notices = Vec::new();
    super::materialize_file_from_cas_with_order(
        &cas,
        hash,
        &destination_path,
        "library/fallback.bin",
        &[MaterializationMethod::Reflink, MaterializationMethod::Copy],
        &mut notices,
    )
    .await
    .expect("materialize with fallback to copy");

    assert_eq!(std::fs::read(&destination_path).expect("read destination"), payload);
    assert_eq!(
        notices,
        vec![
            "hierarchy file 'library/fallback.bin' materialization fell back to 'copy'".to_string()
        ]
    );
}

/// Protects strict failure behavior when every configured method fails.
#[tokio::test]
async fn materialize_file_from_cas_with_order_errors_when_all_methods_fail() {
    let temp = tempfile::tempdir().expect("tempdir");
    let cas_root = temp.path().join("store");
    let cas = FileSystemCas::open(&cas_root).await.expect("open cas");

    let hash = cas.put(b"all-methods-fail".to_vec()).await.expect("put bytes");
    let destination_path = temp.path().join("failed.bin");

    let mut notices = Vec::new();
    let error = super::materialize_file_from_cas_with_order(
        &cas,
        hash,
        &destination_path,
        "library/failed.bin",
        &[MaterializationMethod::Reflink],
        &mut notices,
    )
    .await
    .expect_err("all configured methods should fail");

    assert!(error.to_string().contains(
        "materializing hierarchy file 'library/failed.bin' failed for all configured methods"
    ));
    assert!(!destination_path.exists());
    assert!(notices.is_empty());
}

fn hierarchy_nodes(entries: BTreeMap<String, HierarchyEntry>) -> Vec<crate::config::HierarchyNode> {
    let mut media_id_counts = BTreeMap::<String, usize>::new();
    for entry in entries.values() {
        if matches!(entry.kind, HierarchyEntryKind::Media | HierarchyEntryKind::MediaFolder) {
            *media_id_counts.entry(entry.media_id.clone()).or_insert(0) += 1;
        }
    }

    entries
        .into_iter()
        .map(|(path, entry)| {
            let hierarchy_id = if matches!(
                entry.kind,
                HierarchyEntryKind::Media | HierarchyEntryKind::MediaFolder
            ) {
                let count = media_id_counts.get(&entry.media_id).copied().unwrap_or(0);
                Some(if count <= 1 {
                    entry.media_id.clone()
                } else {
                    format!("{}:{path}", entry.media_id)
                })
            } else {
                None
            };

            match entry.kind {
                HierarchyEntryKind::Media => crate::config::HierarchyNode {
                    path,
                    kind: HierarchyNodeKind::Media,
                    id: hierarchy_id,
                    media_id: Some(entry.media_id),
                    variant: entry.variants.first().cloned(),
                    variants: Vec::new(),
                    rename_files: Vec::new(),
                    format: PlaylistFormat::M3u8,
                    ids: Vec::new(),
                    sanitize_names: entry.sanitize_names.clone(),
                    children: Vec::new(),
                },
                HierarchyEntryKind::MediaFolder => crate::config::HierarchyNode {
                    path,
                    kind: HierarchyNodeKind::MediaFolder,
                    id: hierarchy_id,
                    media_id: Some(entry.media_id),
                    variant: None,
                    variants: entry.variants,
                    rename_files: entry.rename_files,
                    format: PlaylistFormat::M3u8,
                    ids: Vec::new(),
                    sanitize_names: entry.sanitize_names.clone(),
                    children: Vec::new(),
                },
                HierarchyEntryKind::Playlist => crate::config::HierarchyNode {
                    path,
                    kind: HierarchyNodeKind::Playlist,
                    id: None,
                    media_id: None,
                    variant: None,
                    variants: Vec::new(),
                    rename_files: Vec::new(),
                    format: entry.format,
                    ids: entry.ids,
                    sanitize_names: entry.sanitize_names.clone(),
                    children: Vec::new(),
                },
            }
        })
        .collect()
}

/// Protects strict forbidden-character enforcement in hierarchy paths.
#[test]
fn hierarchy_path_rejects_forbidden_characters() {
    let err = validate_hierarchy_path("movies/Star:Wars.mkv").expect_err("path should fail");
    assert!(err.to_string().contains("forbidden characters"));
}

/// Protects NFD-only normalization policy for hierarchy path segments.
#[test]
fn hierarchy_path_rejects_non_nfd_segments() {
    let err = validate_hierarchy_path("movies/épisode.mkv").expect_err("NFD should fail");
    assert!(err.to_string().contains("NFD"));
}

#[test]
fn sanitize_hierarchy_path_replaces_reserved_characters() {
    let replacements = BTreeMap::from([(':', '_'), ('<', '_'), ('?', '_')]);

    assert_eq!(
        sanitize_hierarchy_path("movies/Star:Wars?.mkv", &replacements),
        "movies/Star_Wars_.mkv"
    );
}

/// Ensures media metadata placeholder expansion is normalized to NFD
/// before filesystem-path validation and materialization.
#[tokio::test]
async fn sync_hierarchy_normalizes_expanded_metadata_placeholder_paths_to_nfd() {
    let temp = tempfile::tempdir().expect("tempdir");
    let paths = MediaPmPaths::from_root(temp.path());
    let cas_root = paths.root_dir.join(".mediapm").join("store");
    let cas = FileSystemCas::open(&cas_root).await.expect("open cas");
    let hash = cas.put(b"abc".to_vec()).await.expect("put local bytes");

    let artist_name = "Beyoncé".to_string();

    let document = MediaPmDocument {
        media: BTreeMap::from([(
            "media-a".to_string(),
            MediaSourceSpec {
                id: None,
                description: Some("file: source.bin".to_string()),
                title: None,
                workflow_id: None,
                metadata: Some(BTreeMap::from([(
                    "artist".to_string(),
                    MediaMetadataValue::Literal(artist_name.clone()),
                )])),
                variant_hashes: BTreeMap::from([("default".to_string(), hash.to_string())]),
                steps: Vec::new(),
            },
        )]),
        hierarchy: hierarchy_nodes(BTreeMap::from([(
            "library/${media.metadata.artist}/track.mkv".to_string(),
            HierarchyEntry {
                kind: HierarchyEntryKind::Media,
                format: PlaylistFormat::M3u8,
                ids: Vec::new(),
                media_id: "media-a".to_string(),
                variants: vec!["default".to_string()],
                rename_files: Vec::new(),
                sanitize_names: SanitizeNamesConfig::Inherit,
            },
        )])),
        ..MediaPmDocument::default()
    };

    let mut lock = MediaLockFile::default();
    sync_hierarchy(&paths, &document, &MachineNickelDocument::default(), &cas_root, &mut lock)
        .await
        .expect("sync hierarchy");

    let normalized_path = format!("library/{}/track.mkv", artist_name.nfd().collect::<String>());
    assert!(paths.hierarchy_root_dir.join(&normalized_path).is_file());
    assert!(lock.managed_files.contains_key(&normalized_path));
}

/// Ensures `${media.id}` expansion is also normalized to NFD before path
/// validation/commit.
#[tokio::test]
async fn sync_hierarchy_normalizes_expanded_media_id_placeholder_paths_to_nfd() {
    let temp = tempfile::tempdir().expect("tempdir");
    let paths = MediaPmPaths::from_root(temp.path());
    let cas_root = paths.root_dir.join(".mediapm").join("store");
    let cas = FileSystemCas::open(&cas_root).await.expect("open cas");
    let hash = cas.put(b"abc".to_vec()).await.expect("put local bytes");

    let media_id = "média-a".to_string();

    let document = MediaPmDocument {
        media: BTreeMap::from([(
            media_id.clone(),
            MediaSourceSpec {
                id: None,
                description: Some("file: source.bin".to_string()),
                title: None,
                workflow_id: None,
                metadata: None,
                variant_hashes: BTreeMap::from([("default".to_string(), hash.to_string())]),
                steps: Vec::new(),
            },
        )]),
        hierarchy: hierarchy_nodes(BTreeMap::from([(
            "library/${media.id}/track.mkv".to_string(),
            HierarchyEntry {
                kind: HierarchyEntryKind::Media,
                format: PlaylistFormat::M3u8,
                ids: Vec::new(),
                media_id: media_id.clone(),
                variants: vec!["default".to_string()],
                rename_files: Vec::new(),
                sanitize_names: SanitizeNamesConfig::Inherit,
            },
        )])),
        ..MediaPmDocument::default()
    };

    let mut lock = MediaLockFile::default();
    sync_hierarchy(&paths, &document, &MachineNickelDocument::default(), &cas_root, &mut lock)
        .await
        .expect("sync hierarchy");

    let normalized_path = format!("library/{}/track.mkv", media_id.nfd().collect::<String>());
    assert!(paths.hierarchy_root_dir.join(&normalized_path).is_file());
    assert!(lock.managed_files.contains_key(&normalized_path));
}

/// Protects flattened sidecar materialization by allowing duplicate ZIP
/// file names to co-exist as first-writer-wins entries.
#[test]
fn register_zip_file_entry_allows_duplicate_file_paths() {
    let mut extracted_entries = BTreeMap::new();

    let first_write = super::register_zip_file_entry("captions.en.vtt", &mut extracted_entries)
        .expect("first file registration should succeed");
    let duplicate_write = super::register_zip_file_entry("captions.en.vtt", &mut extracted_entries)
        .expect("duplicate file registration should preserve first writer");

    assert!(first_write, "first registration should request file write");
    assert!(
        !duplicate_write,
        "duplicate registration should skip write to keep first-writer bytes"
    );
    assert_eq!(extracted_entries.get("captions.en.vtt"), Some(&false));
}

/// Protects folder-rename rule sequencing by applying rules in declaration
/// order against normalized ZIP file paths.
#[test]
fn folder_rename_rules_apply_in_declaration_order() {
    let rules = super::compile_hierarchy_folder_rename_rules(
        &[
            HierarchyFolderRenameRule {
                pattern: "^(.+)\\.en\\.vtt$".to_string(),
                replacement: "$1.subtitles.en.vtt".to_string(),
            },
            HierarchyFolderRenameRule {
                pattern: "^(.+)\\.subtitles\\.en\\.vtt$".to_string(),
                replacement: "$1.en.vtt".to_string(),
            },
        ],
        "library/subtitles/",
        "media-a",
    )
    .expect("compile rename rules");

    let renamed = super::apply_hierarchy_folder_rename_rules(
        "Artist - Title [rickroll].en.vtt",
        &rules,
        "library/subtitles/",
        "media-a",
        "subtitles",
    )
    .expect("apply rename rules");

    assert_eq!(renamed, "Artist - Title [rickroll].en.vtt");
}

/// Protects rename replacement semantics by treating `$0` as the entire
/// matched path while still allowing `$1..$N` group rewrites.
#[test]
fn folder_rename_rules_support_dollar_zero_full_match_token() {
    let rules = super::compile_hierarchy_folder_rename_rules(
        &[HierarchyFolderRenameRule {
            pattern: "^(.+)$".to_string(),
            replacement: "mirror/$0".to_string(),
        }],
        "library/subtitles/",
        "media-a",
    )
    .expect("compile rename rules");

    let renamed = super::apply_hierarchy_folder_rename_rules(
        "captions.en.vtt",
        &rules,
        "library/subtitles/",
        "media-a",
        "subtitles",
    )
    .expect("apply rename rules");

    assert_eq!(renamed, "mirror/captions.en.vtt");
}

/// Protects folder-rename safety by rejecting traversal/escaping outputs.
#[test]
fn folder_rename_rules_reject_invalid_paths_after_rewrite() {
    let rules = super::compile_hierarchy_folder_rename_rules(
        &[HierarchyFolderRenameRule {
            pattern: "^(.+)$".to_string(),
            replacement: "../$1".to_string(),
        }],
        "library/subtitles/",
        "media-a",
    )
    .expect("compile rename rules");

    let error = super::apply_hierarchy_folder_rename_rules(
        "captions.en.vtt",
        &rules,
        "library/subtitles/",
        "media-a",
        "subtitles",
    )
    .expect_err("invalid rewritten path should fail");

    assert!(error.to_string().contains("invalid path"));
}

/// Protects yt-dlp subtitle rename behavior by ensuring preset-style rules
/// handle nested ZIP member paths and optional filename prefixes.
#[test]
fn folder_rename_rules_support_nested_yt_dlp_subtitle_paths() {
    let rules = super::compile_hierarchy_folder_rename_rules(
        &[HierarchyFolderRenameRule {
            pattern: "^(?:.*/)?(?:.*\\.)?([^.\\/]+)\\.([^.\\/]+)$".to_string(),
            replacement: "Rick Astley - Never Gonna Give You Up [youtube.dQw4w9WgXcQ].$1.$2"
                .to_string(),
        }],
        "library/rickroll",
        "youtube.dQw4w9WgXcQ",
    )
    .expect("compile rename rules");

    let renamed = super::apply_hierarchy_folder_rename_rules(
        "subtitles/Rick Astley - Never Gonna Give You Up [youtube.dQw4w9WgXcQ].en.vtt",
        &rules,
        "library/rickroll",
        "youtube.dQw4w9WgXcQ",
        "subtitles",
    )
    .expect("apply rename rules");

    assert_eq!(renamed, "Rick Astley - Never Gonna Give You Up [youtube.dQw4w9WgXcQ].en.vtt");
}

/// Protects yt-dlp root sidecar naming by forcing flattened, media-prefixed
/// file names even when ZIP members include nested directories.
#[test]
fn folder_rename_rules_flatten_nested_yt_dlp_root_sidecars() {
    let thumbnail_rules = super::compile_hierarchy_folder_rename_rules(
        &[HierarchyFolderRenameRule {
            pattern: "^.*\\.([^.]+)$".to_string(),
            replacement: "Rick Astley - Never Gonna Give You Up [youtube.dQw4w9WgXcQ].thumbnail.$1"
                .to_string(),
        }],
        "library/rickroll",
        "youtube.dQw4w9WgXcQ",
    )
    .expect("compile thumbnail rules");

    let link_rules = super::compile_hierarchy_folder_rename_rules(
        &[HierarchyFolderRenameRule {
            pattern: "^.*\\.([^.]+)$".to_string(),
            replacement: "Rick Astley - Never Gonna Give You Up [youtube.dQw4w9WgXcQ].link.$1"
                .to_string(),
        }],
        "library/rickroll",
        "youtube.dQw4w9WgXcQ",
    )
    .expect("compile link rules");

    let thumbnail_renamed = super::apply_hierarchy_folder_rename_rules(
        "thumbnails/maxresdefault.jpg",
        &thumbnail_rules,
        "library/rickroll",
        "youtube.dQw4w9WgXcQ",
        "thumbnails",
    )
    .expect("apply thumbnail rules");
    let link_renamed = super::apply_hierarchy_folder_rename_rules(
        "links/watch.url",
        &link_rules,
        "library/rickroll",
        "youtube.dQw4w9WgXcQ",
        "links",
    )
    .expect("apply link rules");

    assert_eq!(
        thumbnail_renamed,
        "Rick Astley - Never Gonna Give You Up [youtube.dQw4w9WgXcQ].thumbnail.jpg"
    );
    assert_eq!(
        link_renamed,
        "Rick Astley - Never Gonna Give You Up [youtube.dQw4w9WgXcQ].link.url"
    );
}

/// Protects sync wiring by ensuring folder rename rules rewrite extracted
/// ZIP member file names before final materialization.
#[tokio::test]
async fn sync_hierarchy_applies_folder_rename_rules_to_zip_members() {
    let _guard = SERIAL_GUARD.get_or_init(|| Mutex::new(())).lock().unwrap();
    let temp = tempfile::tempdir().expect("tempdir");
    let paths = MediaPmPaths::from_root(temp.path());
    let cas_root = paths.root_dir.join(".mediapm").join("store");
    let cas = FileSystemCas::open(&cas_root).await.expect("open cas");

    let zip_bytes = zip_payload(&[
        ("Rick Astley - Never Gonna Give You Up [dQw4w9WgXcQ].jpg", b"jpg"),
        ("Rick Astley - Never Gonna Give You Up [dQw4w9WgXcQ].url", b"[InternetShortcut]"),
    ]);
    let zip_hash = cas.put(zip_bytes).await.expect("put zip bytes");

    let document = MediaPmDocument {
        media: BTreeMap::from([(
            "rickroll".to_string(),
            MediaSourceSpec {
                id: None,
                description: Some("local sidecars".to_string()),
                title: None,
                workflow_id: None,
                metadata: None,
                variant_hashes: BTreeMap::from([("sidecars".to_string(), zip_hash.to_string())]),
                steps: Vec::new(),
            },
        )]),
        hierarchy: hierarchy_nodes(BTreeMap::from([(
            "library/rickroll".to_string(),
            HierarchyEntry {
                kind: HierarchyEntryKind::MediaFolder,
                format: PlaylistFormat::M3u8,
                ids: Vec::new(),
                media_id: "rickroll".to_string(),
                variants: vec!["sidecars".to_string()],
                rename_files: vec![HierarchyFolderRenameRule {
                    pattern: "^Rick Astley - Never Gonna Give You Up \\[dQw4w9WgXcQ\\](\\..+)$"
                        .to_string(),
                    replacement: "Rick Astley - Never Gonna Give You Up [rickroll]$1".to_string(),
                }],
                sanitize_names: SanitizeNamesConfig::Inherit,
            },
        )])),
        ..MediaPmDocument::default()
    };

    let mut lock = MediaLockFile::default();
    let report =
        sync_hierarchy(&paths, &document, &MachineNickelDocument::default(), &cas_root, &mut lock)
            .await
            .expect("sync hierarchy");

    assert_eq!(report.materialized_paths, 1);
    assert!(
        paths
            .hierarchy_root_dir
            .join("library/rickroll/Rick Astley - Never Gonna Give You Up [rickroll].jpg")
            .is_file()
    );
    assert!(
        paths
            .hierarchy_root_dir
            .join("library/rickroll/Rick Astley - Never Gonna Give You Up [rickroll].url")
            .is_file()
    );
    assert!(
        !paths
            .hierarchy_root_dir
            .join("library/rickroll/Rick Astley - Never Gonna Give You Up [dQw4w9WgXcQ].jpg")
            .exists()
    );
    assert!(
        !lock.managed_files.contains_key("library/rickroll/"),
        "managed_files must track extracted files, not folder paths"
    );
    assert!(
        lock.managed_files
            .contains_key("library/rickroll/Rick Astley - Never Gonna Give You Up [rickroll].jpg")
    );
    assert!(
        lock.managed_files
            .contains_key("library/rickroll/Rick Astley - Never Gonna Give You Up [rickroll].url")
    );
    assert_eq!(
        lock.managed_files
            .get("library/rickroll/Rick Astley - Never Gonna Give You Up [rickroll].jpg")
            .expect("jpg managed record")
            .variant,
        "sidecars"
    );
    assert_eq!(
        lock.managed_files
            .get("library/rickroll/Rick Astley - Never Gonna Give You Up [rickroll].url")
            .expect("url managed record")
            .variant,
        "sidecars"
    );
}

/// Protects overlapping media-folder commits by ensuring parent-folder
/// updates preserve previously materialized nested outputs.
#[tokio::test]
async fn sync_hierarchy_preserves_nested_outputs_when_parent_media_folder_commits_later() {
    let _guard = SERIAL_GUARD.get_or_init(|| Mutex::new(())).lock().unwrap();
    let temp = tempfile::tempdir().expect("tempdir");
    let paths = MediaPmPaths::from_root(temp.path());
    let cas_root = paths.root_dir.join(".mediapm").join("store");
    let cas = FileSystemCas::open(&cas_root).await.expect("open cas");

    let sidecar_hash =
        cas.put(zip_payload(&[("info.json", br#"{"id":"demo"}"#)])).await.expect("put sidecar zip");
    let root_hash = cas.put(zip_payload(&[("thumb.webp", b"webp")])).await.expect("put root zip");

    let document = MediaPmDocument {
        media: BTreeMap::from([(
            "media-a".to_string(),
            MediaSourceSpec {
                id: None,
                description: Some("local zip variants".to_string()),
                title: None,
                workflow_id: None,
                metadata: None,
                variant_hashes: BTreeMap::from([
                    ("sidecars".to_string(), sidecar_hash.to_string()),
                    ("root".to_string(), root_hash.to_string()),
                ]),
                steps: Vec::new(),
            },
        )]),
        hierarchy: vec![
            HierarchyNode {
                path: "library/${media.id}/sidecars".to_string(),
                kind: HierarchyNodeKind::MediaFolder,
                id: Some("media-a-sidecars".to_string()),
                media_id: Some("media-a".to_string()),
                variant: None,
                variants: vec!["sidecars".to_string()],
                rename_files: Vec::new(),
                sanitize_names: SanitizeNamesConfig::Inherit,
                format: PlaylistFormat::M3u8,
                ids: Vec::new(),
                children: Vec::new(),
            },
            HierarchyNode {
                path: "library/${media.id}".to_string(),
                kind: HierarchyNodeKind::MediaFolder,
                id: Some("media-a-root".to_string()),
                media_id: Some("media-a".to_string()),
                variant: None,
                variants: vec!["root".to_string()],
                rename_files: Vec::new(),
                sanitize_names: SanitizeNamesConfig::Inherit,
                format: PlaylistFormat::M3u8,
                ids: Vec::new(),
                children: Vec::new(),
            },
        ],
        ..MediaPmDocument::default()
    };

    let mut lock = MediaLockFile::default();
    let report =
        sync_hierarchy(&paths, &document, &MachineNickelDocument::default(), &cas_root, &mut lock)
            .await
            .expect("sync hierarchy");

    assert_eq!(report.materialized_paths, 2);
    assert!(paths.hierarchy_root_dir.join("library/media-a/sidecars/info.json").is_file());
    assert!(paths.hierarchy_root_dir.join("library/media-a/thumb.webp").is_file());
    assert!(lock.managed_files.contains_key("library/media-a/sidecars/info.json"));
    assert!(lock.managed_files.contains_key("library/media-a/thumb.webp"));
    assert_eq!(
        lock.managed_files
            .get("library/media-a/sidecars/info.json")
            .expect("info managed record")
            .variant,
        "sidecars"
    );
    assert_eq!(
        lock.managed_files.get("library/media-a/thumb.webp").expect("thumb managed record").variant,
        "root"
    );
}

/// Protects nested folder-merge semantics by preserving existing children
/// when a later parent media-folder commit contributes overlapping
/// directory names.
#[tokio::test]
async fn sync_hierarchy_preserves_nested_children_on_directory_name_collision() {
    let _guard = SERIAL_GUARD.get_or_init(|| Mutex::new(())).lock().unwrap();
    let temp = tempfile::tempdir().expect("tempdir");
    let paths = MediaPmPaths::from_root(temp.path());
    let cas_root = paths.root_dir.join(".mediapm").join("store");
    let cas = FileSystemCas::open(&cas_root).await.expect("open cas");

    let sidecar_hash =
        cas.put(zip_payload(&[("info.json", br#"{"id":"demo"}"#)])).await.expect("put sidecar zip");
    let root_hash = cas
        .put(zip_payload(&[("sidecars/links.url", b"[InternetShortcut]"), ("thumb.webp", b"webp")]))
        .await
        .expect("put root zip");

    let document = MediaPmDocument {
        media: BTreeMap::from([(
            "media-a".to_string(),
            MediaSourceSpec {
                id: None,
                description: Some("local zip variants".to_string()),
                title: None,
                workflow_id: None,
                metadata: None,
                variant_hashes: BTreeMap::from([
                    ("sidecars".to_string(), sidecar_hash.to_string()),
                    ("root".to_string(), root_hash.to_string()),
                ]),
                steps: Vec::new(),
            },
        )]),
        hierarchy: vec![
            HierarchyNode {
                path: "library/${media.id}/sidecars".to_string(),
                kind: HierarchyNodeKind::MediaFolder,
                id: Some("media-a-sidecars".to_string()),
                media_id: Some("media-a".to_string()),
                variant: None,
                variants: vec!["sidecars".to_string()],
                rename_files: Vec::new(),
                sanitize_names: SanitizeNamesConfig::Inherit,
                format: PlaylistFormat::M3u8,
                ids: Vec::new(),
                children: Vec::new(),
            },
            HierarchyNode {
                path: "library/${media.id}".to_string(),
                kind: HierarchyNodeKind::MediaFolder,
                id: Some("media-a-root".to_string()),
                media_id: Some("media-a".to_string()),
                variant: None,
                variants: vec!["root".to_string()],
                rename_files: Vec::new(),
                sanitize_names: SanitizeNamesConfig::Inherit,
                format: PlaylistFormat::M3u8,
                ids: Vec::new(),
                children: Vec::new(),
            },
        ],
        ..MediaPmDocument::default()
    };

    let mut lock = MediaLockFile::default();
    let report =
        sync_hierarchy(&paths, &document, &MachineNickelDocument::default(), &cas_root, &mut lock)
            .await
            .expect("sync hierarchy");

    assert_eq!(report.materialized_paths, 2);
    assert!(paths.hierarchy_root_dir.join("library/media-a/sidecars/info.json").is_file());
    assert!(paths.hierarchy_root_dir.join("library/media-a/sidecars/links.url").is_file());
    assert!(paths.hierarchy_root_dir.join("library/media-a/thumb.webp").is_file());
    assert!(lock.managed_files.contains_key("library/media-a/sidecars/info.json"));
    assert!(lock.managed_files.contains_key("library/media-a/sidecars/links.url"));
    assert!(lock.managed_files.contains_key("library/media-a/thumb.webp"));
    assert_eq!(
        lock.managed_files
            .get("library/media-a/sidecars/info.json")
            .expect("info managed record")
            .variant,
        "sidecars"
    );
    assert_eq!(
        lock.managed_files
            .get("library/media-a/sidecars/links.url")
            .expect("links managed record")
            .variant,
        "root"
    );
    assert_eq!(
        lock.managed_files.get("library/media-a/thumb.webp").expect("thumb managed record").variant,
        "root"
    );
}

/// Protects rename-rule replacement interpolation by supporting
/// `${media.id}` and `${media.metadata.*}` placeholders.
#[tokio::test]
async fn sync_hierarchy_applies_folder_rename_replacement_media_placeholders() {
    let temp = tempfile::tempdir().expect("tempdir");
    let paths = MediaPmPaths::from_root(temp.path());
    let cas_root = paths.root_dir.join(".mediapm").join("store");
    let cas = FileSystemCas::open(&cas_root).await.expect("open cas");

    let zip_bytes = zip_payload(&[("thumb [video-id].jpg", b"jpg")]);
    let zip_hash = cas.put(zip_bytes).await.expect("put zip bytes");

    let document = MediaPmDocument {
        media: BTreeMap::from([(
            "media-a".to_string(),
            MediaSourceSpec {
                id: None,
                description: Some("local sidecars".to_string()),
                title: None,
                workflow_id: None,
                metadata: Some(BTreeMap::from([(
                    "title".to_string(),
                    MediaMetadataValue::Literal("Demo Title".to_string()),
                )])),
                variant_hashes: BTreeMap::from([("sidecars".to_string(), zip_hash.to_string())]),
                steps: Vec::new(),
            },
        )]),
        hierarchy: hierarchy_nodes(BTreeMap::from([(
            "library/renamed".to_string(),
            HierarchyEntry {
                kind: HierarchyEntryKind::MediaFolder,
                format: PlaylistFormat::M3u8,
                ids: Vec::new(),
                media_id: "media-a".to_string(),
                variants: vec!["sidecars".to_string()],
                rename_files: vec![HierarchyFolderRenameRule {
                    pattern: "^thumb \\[.+\\](\\.[^/\\\\]+)$".to_string(),
                    replacement: "${media.metadata.title} [${media.id}]$1".to_string(),
                }],
                sanitize_names: SanitizeNamesConfig::Inherit,
            },
        )])),
        ..MediaPmDocument::default()
    };

    let mut lock = MediaLockFile::default();
    let report =
        sync_hierarchy(&paths, &document, &MachineNickelDocument::default(), &cas_root, &mut lock)
            .await
            .expect("sync hierarchy");

    assert_eq!(report.materialized_paths, 1);
    assert!(paths.hierarchy_root_dir.join("library/renamed/Demo Title [media-a].jpg").is_file());
    assert!(!paths.hierarchy_root_dir.join("library/renamed/thumb [video-id].jpg").exists());
}

/// Protects stage-commit materialization for local source entries.
#[tokio::test]
async fn sync_hierarchy_materializes_local_source_from_cas_variant_pointer() {
    let temp = tempfile::tempdir().expect("tempdir");
    let paths = MediaPmPaths::from_root(temp.path());
    let cas_root = paths.root_dir.join(".mediapm").join("store");
    let cas = FileSystemCas::open(&cas_root).await.expect("open cas");
    let hash = cas.put(b"abc".to_vec()).await.expect("put local bytes");

    let document = MediaPmDocument {
        media: BTreeMap::from([(
            "media-a".to_string(),
            MediaSourceSpec {
                id: None,
                description: Some("file: source.bin".to_string()),
                title: None,
                workflow_id: None,
                metadata: None,
                variant_hashes: BTreeMap::from([("default".to_string(), hash.to_string())]),
                steps: Vec::new(),
            },
        )]),
        hierarchy: hierarchy_nodes(BTreeMap::from([(
            "library/media-a.bin".to_string(),
            HierarchyEntry {
                kind: HierarchyEntryKind::Media,
                format: PlaylistFormat::M3u8,
                ids: Vec::new(),
                media_id: "media-a".to_string(),
                variants: vec!["default".to_string()],
                rename_files: Vec::new(),
                sanitize_names: SanitizeNamesConfig::Inherit,
            },
        )])),
        ..MediaPmDocument::default()
    };

    let mut lock = MediaLockFile::default();
    let report =
        sync_hierarchy(&paths, &document, &MachineNickelDocument::default(), &cas_root, &mut lock)
            .await
            .expect("sync hierarchy");

    assert_eq!(report.materialized_paths, 1);
    assert!(paths.hierarchy_root_dir.join("library/media-a.bin").exists());
    let metadata =
        std::fs::metadata(paths.hierarchy_root_dir.join("library/media-a.bin")).expect("metadata");
    assert!(metadata.permissions().readonly(), "managed file should be readonly");
    assert_eq!(
        std::fs::read(paths.hierarchy_root_dir.join("library/media-a.bin")).expect("read output"),
        b"abc"
    );
    let record = lock.managed_files.get("library/media-a.bin").expect("managed record");
    assert_eq!(record.media_id, "media-a");
    assert_eq!(record.hash, hash.to_string());

    let source_path = cas.object_path_for_hash(hash);
    let output_path = paths.hierarchy_root_dir.join("library/media-a.bin");
    assert_hardlinked_paths(&source_path, &output_path);
}

/// Protects playlist hierarchy generation by preserving declared id order,
/// default relative path rendering, and explicit absolute-path overrides.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn sync_hierarchy_generates_playlist_with_relative_and_absolute_entries() {
    let _guard = SERIAL_GUARD.get_or_init(|| Mutex::new(())).lock().unwrap();
    let temp = tempfile::tempdir().expect("tempdir");
    let paths = MediaPmPaths::from_root(temp.path());
    let cas_root = paths.root_dir.join(".mediapm").join("store");
    let cas = FileSystemCas::open(&cas_root).await.expect("open cas");
    let alpha_hash = cas.put(b"alpha".to_vec()).await.expect("put alpha bytes");
    let beta_hash = cas.put(b"beta".to_vec()).await.expect("put beta bytes");

    let document = MediaPmDocument {
        media: BTreeMap::from([
            (
                "alpha-source".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: Some("file: alpha.bin".to_string()),
                    title: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::from([(
                        "default".to_string(),
                        alpha_hash.to_string(),
                    )]),
                    steps: Vec::new(),
                },
            ),
            (
                "beta-source".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: Some("file: beta.bin".to_string()),
                    title: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::from([(
                        "default".to_string(),
                        beta_hash.to_string(),
                    )]),
                    steps: Vec::new(),
                },
            ),
        ]),
        hierarchy: hierarchy_nodes(BTreeMap::from([
            (
                "library/music/alpha.mp3".to_string(),
                HierarchyEntry {
                    kind: HierarchyEntryKind::Media,
                    format: PlaylistFormat::M3u8,
                    ids: Vec::new(),
                    media_id: "alpha-source".to_string(),
                    variants: vec!["default".to_string()],
                    rename_files: Vec::new(),
                    sanitize_names: SanitizeNamesConfig::Inherit,
                },
            ),
            (
                "library/music/beta.mp3".to_string(),
                HierarchyEntry {
                    kind: HierarchyEntryKind::Media,
                    format: PlaylistFormat::M3u8,
                    ids: Vec::new(),
                    media_id: "beta-source".to_string(),
                    variants: vec!["default".to_string()],
                    rename_files: Vec::new(),
                    sanitize_names: SanitizeNamesConfig::Inherit,
                },
            ),
            (
                "library/playlists/demo.m3u8".to_string(),
                HierarchyEntry {
                    kind: HierarchyEntryKind::Playlist,
                    format: PlaylistFormat::M3u8,
                    ids: vec![
                        PlaylistItemRef {
                            id: "alpha-source".to_string(),
                            path: PlaylistEntryPathMode::Relative,
                        },
                        PlaylistItemRef {
                            id: "beta-source".to_string(),
                            path: PlaylistEntryPathMode::Absolute,
                        },
                        PlaylistItemRef {
                            id: "alpha-source".to_string(),
                            path: PlaylistEntryPathMode::Relative,
                        },
                    ],
                    media_id: String::new(),
                    variants: Vec::new(),
                    rename_files: Vec::new(),
                    sanitize_names: SanitizeNamesConfig::Inherit,
                },
            ),
        ])),
        ..MediaPmDocument::default()
    };

    let mut lock = MediaLockFile::default();
    let report =
        sync_hierarchy(&paths, &document, &MachineNickelDocument::default(), &cas_root, &mut lock)
            .await
            .expect("sync hierarchy");

    assert_eq!(report.materialized_paths, 3);

    let playlist_path = paths.hierarchy_root_dir.join("library/playlists/demo.m3u8");
    let playlist_text = std::fs::read_to_string(&playlist_path).expect("read playlist file");
    let expected_absolute_beta = paths
        .hierarchy_root_dir
        .join("library/music/beta.mp3")
        .to_string_lossy()
        .replace('\\', "/");
    let expected =
        format!("#EXTM3U\n../music/alpha.mp3\n{expected_absolute_beta}\n../music/alpha.mp3\n");
    assert_eq!(playlist_text, expected);

    let record =
        lock.managed_files.get("library/playlists/demo.m3u8").expect("playlist lock record");
    assert_eq!(record.media_id, "playlist");
    assert_eq!(record.variant, "playlist:m3u8");
    let playlist_hash = record.hash.parse::<Hash>().expect("playlist hash");
    let playlist_bytes_from_cas = cas.get(playlist_hash).await.expect("playlist bytes from cas");
    assert_eq!(playlist_bytes_from_cas.as_ref(), playlist_text.as_bytes());
}

/// Protects playlist id resolution by ensuring runtime uses
/// `PlaylistItemRef.id` text as lookup key against media-node hierarchy
/// `id` fields.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn sync_hierarchy_playlist_resolves_hierarchy_id_mapping() {
    let temp = tempfile::tempdir().expect("tempdir");
    let paths = MediaPmPaths::from_root(temp.path());
    let cas_root = paths.root_dir.join(".mediapm").join("store");
    let cas = FileSystemCas::open(&cas_root).await.expect("open cas");
    let alpha_hash = cas.put(b"alpha".to_vec()).await.expect("put alpha bytes");
    let beta_hash = cas.put(b"beta".to_vec()).await.expect("put beta bytes");

    let document = MediaPmDocument {
        media: BTreeMap::from([
            (
                "alpha-source".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: Some("file: alpha.bin".to_string()),
                    title: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::from([(
                        "default".to_string(),
                        alpha_hash.to_string(),
                    )]),
                    steps: Vec::new(),
                },
            ),
            (
                "beta-source".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: Some("file: beta.bin".to_string()),
                    title: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::from([(
                        "default".to_string(),
                        beta_hash.to_string(),
                    )]),
                    steps: Vec::new(),
                },
            ),
        ]),
        hierarchy: vec![
            crate::config::HierarchyNode {
                path: "library/music/alpha.mp3".to_string(),
                kind: HierarchyNodeKind::Media,
                id: Some("alpha-playlist-id".to_string()),
                media_id: Some("alpha-source".to_string()),
                variant: Some("default".to_string()),
                variants: Vec::new(),
                rename_files: Vec::new(),
                sanitize_names: SanitizeNamesConfig::Inherit,
                format: PlaylistFormat::M3u8,
                ids: Vec::new(),
                children: Vec::new(),
            },
            crate::config::HierarchyNode {
                path: "library/music/beta.mp3".to_string(),
                kind: HierarchyNodeKind::Media,
                id: Some("beta-source".to_string()),
                media_id: Some("beta-source".to_string()),
                variant: Some("default".to_string()),
                variants: Vec::new(),
                rename_files: Vec::new(),
                sanitize_names: SanitizeNamesConfig::Inherit,
                format: PlaylistFormat::M3u8,
                ids: Vec::new(),
                children: Vec::new(),
            },
            crate::config::HierarchyNode {
                path: "library/playlists/mixed-ids.m3u8".to_string(),
                kind: HierarchyNodeKind::Playlist,
                id: None,
                media_id: None,
                variant: None,
                variants: Vec::new(),
                rename_files: Vec::new(),
                sanitize_names: SanitizeNamesConfig::Inherit,
                format: PlaylistFormat::M3u8,
                ids: vec![
                    PlaylistItemRef {
                        id: "alpha-playlist-id".to_string(),
                        path: PlaylistEntryPathMode::Relative,
                    },
                    PlaylistItemRef {
                        id: "beta-source".to_string(),
                        path: PlaylistEntryPathMode::Relative,
                    },
                ],
                children: Vec::new(),
            },
        ],
        ..MediaPmDocument::default()
    };

    let mut lock = MediaLockFile::default();
    let report =
        sync_hierarchy(&paths, &document, &MachineNickelDocument::default(), &cas_root, &mut lock)
            .await
            .expect("sync hierarchy");

    assert_eq!(report.materialized_paths, 3);

    let playlist_path = paths.hierarchy_root_dir.join("library/playlists/mixed-ids.m3u8");
    let playlist_text = std::fs::read_to_string(&playlist_path).expect("read playlist file");
    assert_eq!(playlist_text, "#EXTM3U\n../music/alpha.mp3\n../music/beta.mp3\n");
}

/// Protects playlist safety by rejecting ids that do not resolve to
/// media-file hierarchy nodes.
#[tokio::test]
async fn sync_hierarchy_playlist_rejects_non_media_hierarchy_id() {
    let temp = tempfile::tempdir().expect("tempdir");
    let paths = MediaPmPaths::from_root(temp.path());
    let cas_root = paths.root_dir.join(".mediapm").join("store");
    let cas = FileSystemCas::open(&cas_root).await.expect("open cas");
    let sidecar_zip_hash =
        cas.put(zip_payload(&[("captions.en.vtt", b"sub")])).await.expect("put sidecar zip");

    let document = MediaPmDocument {
        media: BTreeMap::from([(
            "folder-only".to_string(),
            MediaSourceSpec {
                id: None,
                description: Some("local sidecars".to_string()),
                title: None,
                workflow_id: None,
                metadata: None,
                variant_hashes: BTreeMap::from([(
                    "sidecars".to_string(),
                    sidecar_zip_hash.to_string(),
                )]),
                steps: Vec::new(),
            },
        )]),
        hierarchy: vec![
            crate::config::HierarchyNode {
                path: "library/playlists/folder-only.m3u8".to_string(),
                kind: HierarchyNodeKind::Playlist,
                id: None,
                media_id: None,
                variant: None,
                variants: Vec::new(),
                rename_files: Vec::new(),
                sanitize_names: SanitizeNamesConfig::Inherit,
                format: PlaylistFormat::M3u8,
                ids: vec![PlaylistItemRef {
                    id: "folder-only".to_string(),
                    path: PlaylistEntryPathMode::Relative,
                }],
                children: Vec::new(),
            },
            crate::config::HierarchyNode {
                path: "library/sidecars".to_string(),
                kind: HierarchyNodeKind::MediaFolder,
                id: Some("folder-only".to_string()),
                media_id: Some("folder-only".to_string()),
                variant: None,
                variants: vec!["sidecars".to_string()],
                rename_files: Vec::new(),
                sanitize_names: SanitizeNamesConfig::Inherit,
                format: PlaylistFormat::M3u8,
                ids: Vec::new(),
                children: Vec::new(),
            },
        ],
        ..MediaPmDocument::default()
    };

    let mut lock = MediaLockFile::default();
    let error =
        sync_hierarchy(&paths, &document, &MachineNickelDocument::default(), &cas_root, &mut lock)
            .await
            .expect_err("playlist references to non-media hierarchy ids must fail");

    assert!(error.to_string().contains("references unknown hierarchy id 'folder-only'"));
    assert!(!paths.hierarchy_root_dir.join("library/playlists/folder-only.m3u8").exists());
}

/// Protects playlist renderer support across configured common formats.
#[test]
fn render_playlist_bytes_supports_common_formats() {
    let items = vec![
        super::RenderedPlaylistItem {
            id: "alpha-id".to_string(),
            path: "../music/alpha.mp3".to_string(),
        },
        super::RenderedPlaylistItem {
            id: "beta-id".to_string(),
            path: "/library/music/beta.mp3".to_string(),
        },
    ];

    let m3u8 = String::from_utf8(super::render_playlist_bytes(PlaylistFormat::M3u8, &items))
        .expect("m3u8 should be utf-8");
    assert!(m3u8.starts_with("#EXTM3U\n"));

    let m3u = String::from_utf8(super::render_playlist_bytes(PlaylistFormat::M3u, &items))
        .expect("m3u should be utf-8");
    assert!(m3u.starts_with("#EXTM3U\n"));

    let pls = String::from_utf8(super::render_playlist_bytes(PlaylistFormat::Pls, &items))
        .expect("pls should be utf-8");
    assert!(pls.contains("[playlist]\n"));
    assert!(pls.contains("File1=../music/alpha.mp3\n"));
    assert!(pls.contains("NumberOfEntries=2\n"));

    let xspf = String::from_utf8(super::render_playlist_bytes(PlaylistFormat::Xspf, &items))
        .expect("xspf should be utf-8");
    assert!(xspf.contains("<playlist version=\"1\""));
    assert!(xspf.contains("<title>alpha-id</title>"));

    let wpl = String::from_utf8(super::render_playlist_bytes(PlaylistFormat::Wpl, &items))
        .expect("wpl should be utf-8");
    assert!(wpl.contains("<?wpl version=\"1.0\"?>"));
    assert!(wpl.contains("<media src=\"../music/alpha.mp3\" />"));

    let asx = String::from_utf8(super::render_playlist_bytes(PlaylistFormat::Asx, &items))
        .expect("asx should be utf-8");
    assert!(asx.contains("<asx version=\"3.0\">"));
    assert!(asx.contains("<title>alpha-id</title>"));
    assert!(asx.contains("<ref href=\"../music/alpha.mp3\" />"));
}

/// Protects non-default playlist materialization by rendering configured
/// PLS output and recording playlist format provenance in the lockfile.
#[tokio::test]
async fn sync_hierarchy_generates_pls_playlist_and_records_format_label() {
    let temp = tempfile::tempdir().expect("tempdir");
    let paths = MediaPmPaths::from_root(temp.path());
    let cas_root = paths.root_dir.join(".mediapm").join("store");
    let cas = FileSystemCas::open(&cas_root).await.expect("open cas");
    let alpha_hash = cas.put(b"alpha".to_vec()).await.expect("put alpha bytes");

    let document = MediaPmDocument {
        media: BTreeMap::from([(
            "alpha-source".to_string(),
            MediaSourceSpec {
                id: None,
                description: Some("file: alpha.bin".to_string()),
                title: None,
                workflow_id: None,
                metadata: None,
                variant_hashes: BTreeMap::from([("default".to_string(), alpha_hash.to_string())]),
                steps: Vec::new(),
            },
        )]),
        hierarchy: hierarchy_nodes(BTreeMap::from([
            (
                "library/music/alpha.mp3".to_string(),
                HierarchyEntry {
                    kind: HierarchyEntryKind::Media,
                    format: PlaylistFormat::M3u8,
                    ids: Vec::new(),
                    media_id: "alpha-source".to_string(),
                    variants: vec!["default".to_string()],
                    rename_files: Vec::new(),
                    sanitize_names: SanitizeNamesConfig::Inherit,
                },
            ),
            (
                "library/playlists/demo.pls".to_string(),
                HierarchyEntry {
                    kind: HierarchyEntryKind::Playlist,
                    format: PlaylistFormat::Pls,
                    ids: vec![
                        PlaylistItemRef {
                            id: "alpha-source".to_string(),
                            path: PlaylistEntryPathMode::Relative,
                        },
                        PlaylistItemRef {
                            id: "alpha-source".to_string(),
                            path: PlaylistEntryPathMode::Relative,
                        },
                    ],
                    media_id: String::new(),
                    variants: Vec::new(),
                    rename_files: Vec::new(),
                    sanitize_names: SanitizeNamesConfig::Inherit,
                },
            ),
        ])),
        ..MediaPmDocument::default()
    };

    let mut lock = MediaLockFile::default();
    let report =
        sync_hierarchy(&paths, &document, &MachineNickelDocument::default(), &cas_root, &mut lock)
            .await
            .expect("sync hierarchy");

    assert_eq!(report.materialized_paths, 2);

    let playlist_path = paths.hierarchy_root_dir.join("library/playlists/demo.pls");
    let playlist_text = std::fs::read_to_string(&playlist_path).expect("read playlist file");
    assert!(playlist_text.starts_with("[playlist]\n"));
    assert!(playlist_text.contains("File1=../music/alpha.mp3\n"));
    assert!(playlist_text.contains("File2=../music/alpha.mp3\n"));
    assert!(playlist_text.contains("Title1=alpha-source\n"));
    assert!(playlist_text.contains("Title2=alpha-source\n"));
    assert!(playlist_text.contains("NumberOfEntries=2\n"));
    assert!(playlist_text.ends_with("Version=2\n"));

    let record =
        lock.managed_files.get("library/playlists/demo.pls").expect("playlist lock record");
    assert_eq!(record.media_id, "playlist");
    assert_eq!(record.variant, "playlist:pls");
    let playlist_hash = record.hash.parse::<Hash>().expect("playlist hash");
    let playlist_bytes_from_cas = cas.get(playlist_hash).await.expect("playlist bytes from cas");
    assert_eq!(playlist_bytes_from_cas.as_ref(), playlist_text.as_bytes());
}

/// Protects hierarchy placeholder interpolation for literal metadata values.
#[tokio::test]
async fn sync_hierarchy_interpolates_literal_media_metadata_placeholders() {
    let temp = tempfile::tempdir().expect("tempdir");
    let paths = MediaPmPaths::from_root(temp.path());
    let cas_root = paths.root_dir.join(".mediapm").join("store");
    let cas = FileSystemCas::open(&cas_root).await.expect("open cas");
    let hash = cas.put(b"abc".to_vec()).await.expect("put local bytes");

    let document = MediaPmDocument {
        media: BTreeMap::from([(
            "media-a".to_string(),
            MediaSourceSpec {
                id: None,
                description: Some("file: source.bin".to_string()),
                title: None,
                workflow_id: None,
                metadata: Some(BTreeMap::from([(
                    "title".to_string(),
                    MediaMetadataValue::Literal("Demo Title".to_string()),
                )])),
                variant_hashes: BTreeMap::from([("default".to_string(), hash.to_string())]),
                steps: Vec::new(),
            },
        )]),
        hierarchy: hierarchy_nodes(BTreeMap::from([(
            "library/${media.metadata.title}.bin".to_string(),
            HierarchyEntry {
                kind: HierarchyEntryKind::Media,
                format: PlaylistFormat::M3u8,
                ids: Vec::new(),
                media_id: "media-a".to_string(),
                variants: vec!["default".to_string()],
                rename_files: Vec::new(),
                sanitize_names: SanitizeNamesConfig::Inherit,
            },
        )])),
        ..MediaPmDocument::default()
    };

    let mut lock = MediaLockFile::default();
    let report =
        sync_hierarchy(&paths, &document, &MachineNickelDocument::default(), &cas_root, &mut lock)
            .await
            .expect("sync hierarchy");

    assert_eq!(report.materialized_paths, 1);
    assert!(paths.hierarchy_root_dir.join("library/Demo Title.bin").exists());
}

/// Protects hierarchy placeholder interpolation for variant-backed
/// metadata extraction from JSON sidecar payloads.
#[tokio::test]
async fn sync_hierarchy_interpolates_variant_backed_media_metadata_placeholders() {
    let temp = tempfile::tempdir().expect("tempdir");
    let paths = MediaPmPaths::from_root(temp.path());
    let cas_root = paths.root_dir.join(".mediapm").join("store");
    let cas = FileSystemCas::open(&cas_root).await.expect("open cas");
    let audio_hash = cas.put(b"audio-bytes".to_vec()).await.expect("put audio bytes");
    let infojson_hash =
        cas.put(br#"{"title":"Variant Title"}"#.to_vec()).await.expect("put infojson bytes");

    let document = MediaPmDocument {
        media: BTreeMap::from([(
            "media-a".to_string(),
            MediaSourceSpec {
                id: None,
                description: Some("file: source.bin".to_string()),
                title: None,
                workflow_id: None,
                metadata: Some(BTreeMap::from([(
                    "title".to_string(),
                    MediaMetadataValue::Variant(MediaMetadataVariantBinding {
                        variant: "infojson".to_string(),
                        metadata_key: "title".to_string(),
                        transform: None,
                    }),
                )])),
                variant_hashes: BTreeMap::from([
                    ("audio".to_string(), audio_hash.to_string()),
                    ("infojson".to_string(), infojson_hash.to_string()),
                ]),
                steps: Vec::new(),
            },
        )]),
        hierarchy: hierarchy_nodes(BTreeMap::from([(
            "library/${media.metadata.title}.bin".to_string(),
            HierarchyEntry {
                kind: HierarchyEntryKind::Media,
                format: PlaylistFormat::M3u8,
                ids: Vec::new(),
                media_id: "media-a".to_string(),
                variants: vec!["audio".to_string()],
                rename_files: Vec::new(),
                sanitize_names: SanitizeNamesConfig::Inherit,
            },
        )])),
        ..MediaPmDocument::default()
    };

    let mut lock = MediaLockFile::default();
    let report =
        sync_hierarchy(&paths, &document, &MachineNickelDocument::default(), &cas_root, &mut lock)
            .await
            .expect("sync hierarchy");

    assert_eq!(report.materialized_paths, 1);
    assert!(paths.hierarchy_root_dir.join("library/Variant Title.bin").exists());
    assert_eq!(
        std::fs::read(paths.hierarchy_root_dir.join("library/Variant Title.bin"))
            .expect("read output"),
        b"audio-bytes"
    );
}

/// Protects hierarchy placeholder interpolation for `${media.id}`.
#[tokio::test]
async fn sync_hierarchy_interpolates_media_id_placeholder() {
    let temp = tempfile::tempdir().expect("tempdir");
    let paths = MediaPmPaths::from_root(temp.path());
    let cas_root = paths.root_dir.join(".mediapm").join("store");
    let cas = FileSystemCas::open(&cas_root).await.expect("open cas");
    let hash = cas.put(b"abc".to_vec()).await.expect("put local bytes");

    let document = MediaPmDocument {
        media: BTreeMap::from([(
            "media-a".to_string(),
            MediaSourceSpec {
                id: None,
                description: Some("file: source.bin".to_string()),
                title: None,
                workflow_id: None,
                metadata: None,
                variant_hashes: BTreeMap::from([("default".to_string(), hash.to_string())]),
                steps: Vec::new(),
            },
        )]),
        hierarchy: hierarchy_nodes(BTreeMap::from([(
            "library/${media.id}/output.bin".to_string(),
            HierarchyEntry {
                kind: HierarchyEntryKind::Media,
                format: PlaylistFormat::M3u8,
                ids: Vec::new(),
                media_id: "media-a".to_string(),
                variants: vec!["default".to_string()],
                rename_files: Vec::new(),
                sanitize_names: SanitizeNamesConfig::Inherit,
            },
        )])),
        ..MediaPmDocument::default()
    };

    let mut lock = MediaLockFile::default();
    let report =
        sync_hierarchy(&paths, &document, &MachineNickelDocument::default(), &cas_root, &mut lock)
            .await
            .expect("sync hierarchy");

    assert_eq!(report.materialized_paths, 1);
    assert!(paths.hierarchy_root_dir.join("library/media-a/output.bin").exists());
}

/// Protects metadata extension interpolation by applying full-match regex
/// transforms with capture groups.
#[tokio::test]
async fn sync_hierarchy_interpolates_variant_metadata_with_dot_prefix() {
    let temp = tempfile::tempdir().expect("tempdir");
    let paths = MediaPmPaths::from_root(temp.path());
    let cas_root = paths.root_dir.join(".mediapm").join("store");
    let cas = FileSystemCas::open(&cas_root).await.expect("open cas");
    let audio_hash = cas.put(b"audio-bytes".to_vec()).await.expect("put audio bytes");
    let infojson_hash = cas.put(br#"{"ext":"mkv"}"#.to_vec()).await.expect("put infojson bytes");

    let document = MediaPmDocument {
        media: BTreeMap::from([(
            "media-a".to_string(),
            MediaSourceSpec {
                id: None,
                description: Some("file: source.bin".to_string()),
                title: None,
                workflow_id: None,
                metadata: Some(BTreeMap::from([(
                    "video_ext".to_string(),
                    MediaMetadataValue::Variant(MediaMetadataVariantBinding {
                        variant: "infojson".to_string(),
                        metadata_key: "ext".to_string(),
                        transform: Some(MediaMetadataRegexTransform {
                            pattern: "(.+)".to_string(),
                            replacement: ".$0".to_string(),
                        }),
                    }),
                )])),
                variant_hashes: BTreeMap::from([
                    ("audio".to_string(), audio_hash.to_string()),
                    ("infojson".to_string(), infojson_hash.to_string()),
                ]),
                steps: Vec::new(),
            },
        )]),
        hierarchy: hierarchy_nodes(BTreeMap::from([(
            "library/output${media.metadata.video_ext}".to_string(),
            HierarchyEntry {
                kind: HierarchyEntryKind::Media,
                format: PlaylistFormat::M3u8,
                ids: Vec::new(),
                media_id: "media-a".to_string(),
                variants: vec!["audio".to_string()],
                rename_files: Vec::new(),
                sanitize_names: SanitizeNamesConfig::Inherit,
            },
        )])),
        ..MediaPmDocument::default()
    };

    let mut lock = MediaLockFile::default();
    let report =
        sync_hierarchy(&paths, &document, &MachineNickelDocument::default(), &cas_root, &mut lock)
            .await
            .expect("sync hierarchy");

    assert_eq!(report.materialized_paths, 1);
    assert!(paths.hierarchy_root_dir.join("library/output.mkv").exists());
}

/// Protects optional metadata transform behavior by allowing empty values
/// to pass through unchanged when replacement omits dot-prefixing.
#[tokio::test]
async fn sync_hierarchy_interpolates_empty_variant_metadata_without_dot_prefix() {
    let temp = tempfile::tempdir().expect("tempdir");
    let paths = MediaPmPaths::from_root(temp.path());
    let cas_root = paths.root_dir.join(".mediapm").join("store");
    let cas = FileSystemCas::open(&cas_root).await.expect("open cas");
    let audio_hash = cas.put(b"audio-bytes".to_vec()).await.expect("put audio bytes");
    let infojson_hash = cas.put(br#"{"ext":""}"#.to_vec()).await.expect("put infojson bytes");

    let document = MediaPmDocument {
        media: BTreeMap::from([(
            "media-a".to_string(),
            MediaSourceSpec {
                id: None,
                description: Some("file: source.bin".to_string()),
                title: None,
                workflow_id: None,
                metadata: Some(BTreeMap::from([(
                    "video_ext".to_string(),
                    MediaMetadataValue::Variant(MediaMetadataVariantBinding {
                        variant: "infojson".to_string(),
                        metadata_key: "ext".to_string(),
                        transform: Some(MediaMetadataRegexTransform {
                            pattern: "(.*)".to_string(),
                            replacement: "$0".to_string(),
                        }),
                    }),
                )])),
                variant_hashes: BTreeMap::from([
                    ("audio".to_string(), audio_hash.to_string()),
                    ("infojson".to_string(), infojson_hash.to_string()),
                ]),
                steps: Vec::new(),
            },
        )]),
        hierarchy: hierarchy_nodes(BTreeMap::from([(
            "library/output${media.metadata.video_ext}".to_string(),
            HierarchyEntry {
                kind: HierarchyEntryKind::Media,
                format: PlaylistFormat::M3u8,
                ids: Vec::new(),
                media_id: "media-a".to_string(),
                variants: vec!["audio".to_string()],
                rename_files: Vec::new(),
                sanitize_names: SanitizeNamesConfig::Inherit,
            },
        )])),
        ..MediaPmDocument::default()
    };

    let mut lock = MediaLockFile::default();
    let report =
        sync_hierarchy(&paths, &document, &MachineNickelDocument::default(), &cas_root, &mut lock)
            .await
            .expect("sync hierarchy");

    assert_eq!(report.materialized_paths, 1);
    assert!(paths.hierarchy_root_dir.join("library/output").exists());
}

/// Protects stale-path removal for readonly managed files on Windows/Linux.
#[test]
fn remove_path_handles_readonly_files() {
    let temp = tempfile::tempdir().expect("tempdir");
    let file_path = temp.path().join("readonly.txt");
    std::fs::write(&file_path, b"x").expect("write file");

    let mut permissions = std::fs::metadata(&file_path).expect("metadata").permissions();
    permissions.set_readonly(true);
    std::fs::set_permissions(&file_path, permissions).expect("set readonly");

    super::remove_path(&file_path).expect("remove readonly file");
    assert!(!file_path.exists());
}

/// Protects online-source materialization by resolving workflow output hashes
/// from persisted orchestration state instead of writing placeholders.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
)]
async fn sync_hierarchy_materializes_online_variant_from_workflow_state() {
    let temp = tempfile::tempdir().expect("tempdir");
    let paths = MediaPmPaths::from_root(temp.path());
    let cas_root = paths.root_dir.join(".mediapm").join("store");
    let cas = FileSystemCas::open(&cas_root).await.expect("open cas");

    let media_id = "remote-a";
    let source_uri = "https://example.com/audio";
    let output_bytes = b"ID3workflow-output".to_vec();
    let output_hash = cas.put(output_bytes.clone()).await.expect("put output bytes");

    let tool_id = "mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp@latest".to_string();
    let tool_spec = ToolSpec {
        kind: ToolKindSpec::Executable {
            command: vec!["yt-dlp.exe".to_string()],
            env_vars: BTreeMap::new(),
            success_codes: vec![0],
        },
        inputs: BTreeMap::from([
            ("source_url".to_string(), ToolInputSpec::default()),
            ("leading_args".to_string(), ToolInputSpec { kind: ToolInputKind::StringList }),
            ("trailing_args".to_string(), ToolInputSpec { kind: ToolInputKind::StringList }),
        ]),
        outputs: BTreeMap::from([(
            "primary".to_string(),
            ToolOutputSpec {
                allow_empty: false,
                capture: OutputCaptureSpec::File {
                    path: "downloads/yt-dlp-output.media".to_string(),
                },
            },
        )]),
        ..ToolSpec::default()
    };

    let step_id = "0-0-yt-dlp".to_string();
    let workflow = WorkflowSpec {
        name: Some(media_id.to_string()),
        description: Some("online source".to_string()),
        steps: vec![WorkflowStepSpec {
            id: step_id.clone(),
            tool: tool_id.clone(),
            inputs: BTreeMap::from([
                ("source_url".to_string(), InputBinding::String(source_uri.to_string())),
                (
                    "leading_args".to_string(),
                    InputBinding::StringList(vec![
                        "--format".to_string(),
                        "bestaudio/best".to_string(),
                    ]),
                ),
                ("trailing_args".to_string(), InputBinding::StringList(Vec::new())),
            ]),
            depends_on: Vec::new(),
            outputs: BTreeMap::new(),
        }],
    };

    let mut machine = MachineNickelDocument::default();
    machine.tools.insert(tool_id.clone(), tool_spec.clone());
    let workflow_id = crate::conductor_bridge::managed_workflow_id_for_media(
        media_id,
        &MediaSourceSpec {
            id: None,
            description: Some("online source".to_string()),
            title: None,
            workflow_id: None,
            metadata: None,
            variant_hashes: BTreeMap::new(),
            steps: vec![MediaStep {
                tool: MediaStepTool::YtDlp,
                input_variants: Vec::new(),
                output_variants: BTreeMap::from([(
                    "normalized".to_string(),
                    yt_dlp_output_variant("primary"),
                )]),
                options: BTreeMap::from([
                    ("uri".to_string(), TransformInputValue::String(source_uri.to_string())),
                    (
                        "leading_args".to_string(),
                        TransformInputValue::String("--format bestaudio/best".to_string()),
                    ),
                ]),
            }],
        },
    );
    machine.workflows.insert(workflow_id, workflow);

    let instance_inputs = BTreeMap::from([
        (
            "source_url".to_string(),
            mediapm_conductor::ResolvedInput::from_hash(Hash::from_content(source_uri.as_bytes())),
        ),
        (
            "leading_args".to_string(),
            mediapm_conductor::ResolvedInput::from_string_list(vec![
                "--format".to_string(),
                "bestaudio/best".to_string(),
            ])
            .expect("list hash"),
        ),
        (
            "trailing_args".to_string(),
            mediapm_conductor::ResolvedInput::from_string_list(Vec::new())
                .expect("empty list hash"),
        ),
    ]);

    let state = OrchestrationState {
        version: 1,
        instances: BTreeMap::from([(
            "instance-a".to_string(),
            mediapm_conductor::ToolCallInstance {
                tool_name: tool_id.clone(),
                metadata: tool_spec,
                impure_timestamp: None,
                last_used: None,
                inputs: instance_inputs,
                outputs: BTreeMap::from([(
                    "primary".to_string(),
                    OutputRef {
                        allow_empty_capture: false,
                        hash: output_hash,
                        persistence: PersistenceFlags::default(),
                    },
                )]),
            },
        )]),
    };

    let state_blob = serde_json::to_vec(&state).expect("encode state blob");
    let state_pointer = cas.put(state_blob).await.expect("put state blob");
    let encoded_state_document = encode_state_document(StateNickelDocument {
        impure_timestamps: BTreeMap::new(),
        state_pointer: Some(state_pointer),
    })
    .expect("encode state document");

    std::fs::create_dir_all(paths.conductor_state_config.parent().expect("state parent"))
        .expect("create state parent");
    std::fs::write(&paths.conductor_state_config, encoded_state_document)
        .expect("write state document");

    let source = MediaSourceSpec {
        id: None,
        description: Some("online source".to_string()),
        title: None,
        workflow_id: None,
        metadata: None,
        variant_hashes: BTreeMap::new(),
        steps: vec![MediaStep {
            tool: MediaStepTool::YtDlp,
            input_variants: Vec::new(),
            output_variants: BTreeMap::from([(
                "normalized".to_string(),
                yt_dlp_output_variant("primary"),
            )]),
            options: BTreeMap::from([
                ("uri".to_string(), TransformInputValue::String(source_uri.to_string())),
                (
                    "leading_args".to_string(),
                    TransformInputValue::String("--format bestaudio/best".to_string()),
                ),
            ]),
        }],
    };

    let binding =
        crate::conductor_bridge::resolve_media_variant_output_binding(&source, "normalized")
            .expect("resolve variant binding")
            .expect("binding exists");
    assert_eq!(binding.step_id, step_id);
    assert_eq!(binding.output_name, "primary");

    let document = MediaPmDocument {
        media: BTreeMap::from([(media_id.to_string(), source)]),
        hierarchy: hierarchy_nodes(BTreeMap::from([(
            "demo/online.bin".to_string(),
            HierarchyEntry {
                kind: HierarchyEntryKind::Media,
                format: PlaylistFormat::M3u8,
                ids: Vec::new(),
                media_id: media_id.to_string(),
                variants: vec!["normalized".to_string()],
                rename_files: Vec::new(),
                sanitize_names: SanitizeNamesConfig::Inherit,
            },
        )])),
        ..MediaPmDocument::default()
    };

    let mut lock = MediaLockFile::default();
    let report = sync_hierarchy(&paths, &document, &machine, &cas_root, &mut lock)
        .await
        .expect("sync hierarchy");

    assert_eq!(report.materialized_paths, 1);
    assert!(report.notices.is_empty());
    assert_eq!(
        std::fs::read(paths.hierarchy_root_dir.join("demo/online.bin")).expect("read output"),
        output_bytes
    );
}

/// Protects strict local-source materialization by failing when the
/// declared CAS hash pointer is unavailable.
#[tokio::test]
async fn sync_hierarchy_fails_when_local_variant_hash_is_missing_from_cas() {
    let temp = tempfile::tempdir().expect("tempdir");
    let paths = MediaPmPaths::from_root(temp.path());
    let cas_root = paths.root_dir.join(".mediapm").join("store");

    let missing_hash = Hash::from_content(b"missing-local-payload");
    let document = MediaPmDocument {
        media: BTreeMap::from([(
            "local-missing".to_string(),
            MediaSourceSpec {
                id: None,
                description: Some("local source".to_string()),
                title: None,
                workflow_id: None,
                metadata: None,
                variant_hashes: BTreeMap::from([("default".to_string(), missing_hash.to_string())]),
                steps: Vec::new(),
            },
        )]),
        hierarchy: hierarchy_nodes(BTreeMap::from([(
            "demo/local.bin".to_string(),
            HierarchyEntry {
                kind: HierarchyEntryKind::Media,
                format: PlaylistFormat::M3u8,
                ids: Vec::new(),
                media_id: "local-missing".to_string(),
                variants: vec!["default".to_string()],
                rename_files: Vec::new(),
                sanitize_names: SanitizeNamesConfig::Inherit,
            },
        )])),
        ..MediaPmDocument::default()
    };

    let mut lock = MediaLockFile::default();
    let error =
        sync_hierarchy(&paths, &document, &MachineNickelDocument::default(), &cas_root, &mut lock)
            .await
            .expect_err("missing local CAS payload must fail materialization");

    let error_text = error.to_string();
    assert!(error_text.contains("variant 'default'"), "unexpected error: {error_text}");
    assert!(error_text.contains("missing") || error_text.contains("not found"));
    assert!(!paths.hierarchy_root_dir.join("demo/local.bin").exists());
}

/// Protects strict online-source materialization by failing when no
/// workflow output hash can be resolved from runtime state.
#[tokio::test]
async fn sync_hierarchy_fails_when_online_variant_hash_is_unresolved() {
    let temp = tempfile::tempdir().expect("tempdir");
    let paths = MediaPmPaths::from_root(temp.path());
    let cas_root = paths.root_dir.join(".mediapm").join("store");

    let source_uri = "https://example.com/audio";
    let document = MediaPmDocument {
        media: BTreeMap::from([(
            "remote-unresolved".to_string(),
            MediaSourceSpec {
                id: None,
                description: Some("online source".to_string()),
                title: None,
                workflow_id: None,
                metadata: None,
                variant_hashes: BTreeMap::new(),
                steps: vec![MediaStep {
                    tool: MediaStepTool::YtDlp,
                    input_variants: Vec::new(),
                    output_variants: BTreeMap::from([(
                        "normalized".to_string(),
                        yt_dlp_output_variant("primary"),
                    )]),
                    options: BTreeMap::from([(
                        "uri".to_string(),
                        TransformInputValue::String(source_uri.to_string()),
                    )]),
                }],
            },
        )]),
        hierarchy: hierarchy_nodes(BTreeMap::from([(
            "demo/online.bin".to_string(),
            HierarchyEntry {
                kind: HierarchyEntryKind::Media,
                format: PlaylistFormat::M3u8,
                ids: Vec::new(),
                media_id: "remote-unresolved".to_string(),
                variants: vec!["normalized".to_string()],
                rename_files: Vec::new(),
                sanitize_names: SanitizeNamesConfig::Inherit,
            },
        )])),
        ..MediaPmDocument::default()
    };

    let mut lock = MediaLockFile::default();
    let error =
        sync_hierarchy(&paths, &document, &MachineNickelDocument::default(), &cas_root, &mut lock)
            .await
            .expect_err("online source without resolved workflow output hash must fail");

    let error_text = error.to_string();
    assert!(error_text.contains(source_uri), "unexpected error: {error_text}");
    assert!(error_text.contains("runtime state") || error_text.contains("workflow"));
    assert!(!paths.hierarchy_root_dir.join("demo/online.bin").exists());
}

/// Protects runtime-instance matching by allowing extra runtime-injected
/// default inputs while still requiring all step-declared input hashes.
#[test]
fn instance_matching_allows_extra_runtime_inputs() {
    let expected_text_hash = Hash::from_content(b"hello");
    let expected = super::ExpectedStepInputs {
        resolved_hashes: BTreeMap::from([("text".to_string(), expected_text_hash)]),
        unresolved_hash_input_names: BTreeSet::new(),
    };

    let instance = ToolCallInstance {
        tool_name: "echo@1.0.0".to_string(),
        metadata: ToolSpec {
            kind: ToolKindSpec::Builtin { name: "echo".to_string(), version: "1.0.0".to_string() },
            ..ToolSpec::default()
        },
        impure_timestamp: None,
        last_used: None,
        inputs: BTreeMap::from([
            ("text".to_string(), mediapm_conductor::ResolvedInput::from_hash(expected_text_hash)),
            (
                "leading_args".to_string(),
                mediapm_conductor::ResolvedInput::from_string_list(vec!["--verbose".to_string()])
                    .expect("list hash"),
            ),
        ]),
        outputs: BTreeMap::new(),
    };

    assert!(instance_matches_expected_inputs(&instance, &expected));
}

/// Protects runtime step resolution against equivalent-call collisions by
/// requiring matched instances to expose step-declared output names.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
)]
async fn resolve_step_output_hashes_matches_instance_with_expected_output_names() {
    let temp = tempfile::tempdir().expect("tempdir");
    let cas = FileSystemCas::open(temp.path()).await.expect("open cas");

    let tool_id = "mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp@latest".to_string();
    let tool_spec = ToolSpec {
        kind: ToolKindSpec::Executable {
            command: vec!["yt-dlp.exe".to_string()],
            env_vars: BTreeMap::new(),
            success_codes: vec![0],
        },
        inputs: BTreeMap::from([("source_url".to_string(), ToolInputSpec::default())]),
        outputs: BTreeMap::from([
            (
                "content".to_string(),
                ToolOutputSpec {
                    allow_empty: false,
                    capture: OutputCaptureSpec::File {
                        path: "downloads/yt-dlp-output.media".to_string(),
                    },
                },
            ),
            (
                "yt_dlp_thumbnail_artifacts".to_string(),
                ToolOutputSpec {
                    allow_empty: false,
                    capture: OutputCaptureSpec::Folder {
                        path: "downloads".to_string(),
                        include_topmost_folder: false,
                    },
                },
            ),
        ]),
        ..ToolSpec::default()
    };

    let step_id = "step-0-primary".to_string();
    let source_url = "https://example.com/video".to_string();
    let source_url_hash = Hash::from_content(source_url.as_bytes());

    let workflow = WorkflowSpec {
        name: Some("demo".to_string()),
        description: None,
        steps: vec![WorkflowStepSpec {
            id: step_id.clone(),
            tool: tool_id.clone(),
            inputs: BTreeMap::from([(
                "source_url".to_string(),
                InputBinding::String(source_url.clone()),
            )]),
            depends_on: Vec::new(),
            outputs: BTreeMap::from([(
                "content".to_string(),
                OutputPolicy { save: Some(OutputSaveMode::Full) },
            )]),
        }],
    };

    let mut machine = MachineNickelDocument::default();
    machine.tools.insert(tool_id.clone(), tool_spec.clone());

    let state = OrchestrationState {
        version: 1,
        instances: BTreeMap::from([
            (
                "a-thumbnail-first".to_string(),
                ToolCallInstance {
                    tool_name: tool_id.clone(),
                    metadata: tool_spec.clone(),
                    impure_timestamp: None,
                    last_used: None,
                    inputs: BTreeMap::from([(
                        "source_url".to_string(),
                        mediapm_conductor::ResolvedInput::from_hash(source_url_hash),
                    )]),
                    outputs: BTreeMap::from([(
                        "yt_dlp_thumbnail_artifacts".to_string(),
                        OutputRef {
                            allow_empty_capture: false,
                            hash: Hash::from_content(b"thumb-zip"),
                            persistence: PersistenceFlags::default(),
                        },
                    )]),
                },
            ),
            (
                "z-primary-second".to_string(),
                ToolCallInstance {
                    tool_name: tool_id,
                    metadata: tool_spec,
                    impure_timestamp: None,
                    last_used: None,
                    inputs: BTreeMap::from([(
                        "source_url".to_string(),
                        mediapm_conductor::ResolvedInput::from_hash(source_url_hash),
                    )]),
                    outputs: BTreeMap::from([(
                        "content".to_string(),
                        OutputRef {
                            allow_empty_capture: false,
                            hash: Hash::from_content(b"primary-media"),
                            persistence: PersistenceFlags::default(),
                        },
                    )]),
                },
            ),
        ]),
    };

    let step_output_hashes =
        super::resolve_workflow_step_output_hashes(&cas, &machine, &state, &workflow)
            .await
            .expect("resolve step outputs")
            .expect("step outputs should resolve");

    let output_hash = step_output_hashes
        .get(&step_id)
        .and_then(|outputs| outputs.get("content"))
        .copied()
        .expect("content hash should resolve from primary instance");
    assert_eq!(output_hash, Hash::from_content(b"primary-media"));
}

/// Protects ZIP-selector materialization by skipping stale matching
/// instances whose required step outputs are missing from CAS.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
)]
async fn resolve_step_output_hashes_prefers_materializable_zip_selector_instance() {
    let temp = tempfile::tempdir().expect("tempdir");
    let cas = FileSystemCas::open(temp.path()).await.expect("open cas");

    let tagger_tool_id = "mediapm.tools.media-tagger@latest".to_string();
    let tagger_tool_spec = ToolSpec {
        kind: ToolKindSpec::Executable {
            command: vec!["media-tagger.exe".to_string()],
            env_vars: BTreeMap::new(),
            success_codes: vec![0],
        },
        inputs: BTreeMap::from([("input_content".to_string(), ToolInputSpec::default())]),
        outputs: BTreeMap::from([(
            "sandbox_artifacts".to_string(),
            ToolOutputSpec {
                allow_empty: false,
                capture: OutputCaptureSpec::Folder {
                    path: "sandbox".to_string(),
                    include_topmost_folder: false,
                },
            },
        )]),
        ..ToolSpec::default()
    };

    let apply_tool_id = "mediapm.tools.ffmpeg@latest".to_string();
    let apply_tool_spec = ToolSpec {
        kind: ToolKindSpec::Executable {
            command: vec!["ffmpeg.exe".to_string()],
            env_vars: BTreeMap::new(),
            success_codes: vec![0],
        },
        inputs: BTreeMap::from([("cover_flag".to_string(), ToolInputSpec::default())]),
        outputs: BTreeMap::from([(
            "content".to_string(),
            ToolOutputSpec {
                allow_empty: false,
                capture: OutputCaptureSpec::File { path: "output.media".to_string() },
            },
        )]),
        ..ToolSpec::default()
    };

    let step_tagger_id = "step-0-media-tagger".to_string();
    let step_apply_id = "step-1-ffmpeg".to_string();
    let input_hash = Hash::from_content(b"tagger-input");

    let required_member = "coverart-slot-0.flag";
    let member_bytes = b"coverart-present";
    let valid_zip_hash = cas
        .put(zip_payload(&[(required_member, member_bytes.as_slice())]))
        .await
        .expect("put valid zip payload");
    let missing_zip_hash = Hash::from_content(b"missing-zip-payload");

    let final_output_hash = Hash::from_content(b"final-media-output");
    let workflow = WorkflowSpec {
        name: Some("zip-selector-demo".to_string()),
        description: None,
        steps: vec![
            WorkflowStepSpec {
                id: step_tagger_id.clone(),
                tool: tagger_tool_id.clone(),
                inputs: BTreeMap::from([(
                    "input_content".to_string(),
                    InputBinding::String("tagger-input".to_string()),
                )]),
                depends_on: Vec::new(),
                outputs: BTreeMap::from([(
                    "sandbox_artifacts".to_string(),
                    OutputPolicy { save: Some(OutputSaveMode::Full) },
                )]),
            },
            WorkflowStepSpec {
                id: step_apply_id.clone(),
                tool: apply_tool_id.clone(),
                inputs: BTreeMap::from([(
                    "cover_flag".to_string(),
                    InputBinding::String(format!(
                        "${{step_output.{step_tagger_id}.sandbox_artifacts:zip({required_member})}}"
                    )),
                )]),
                depends_on: vec![step_tagger_id.clone()],
                outputs: BTreeMap::from([(
                    "content".to_string(),
                    OutputPolicy { save: Some(OutputSaveMode::Full) },
                )]),
            },
        ],
    };

    let mut machine = MachineNickelDocument::default();
    machine.tools.insert(tagger_tool_id.clone(), tagger_tool_spec.clone());
    machine.tools.insert(apply_tool_id.clone(), apply_tool_spec.clone());

    let state = OrchestrationState {
        version: 1,
        instances: BTreeMap::from([
            (
                "a-stale-missing-zip".to_string(),
                ToolCallInstance {
                    tool_name: tagger_tool_id.clone(),
                    metadata: tagger_tool_spec.clone(),
                    impure_timestamp: Some(ImpureTimestamp { epoch_seconds: 1, subsec_nanos: 0 }),
                    last_used: None,
                    inputs: BTreeMap::from([(
                        "input_content".to_string(),
                        mediapm_conductor::ResolvedInput::from_hash(input_hash),
                    )]),
                    outputs: BTreeMap::from([(
                        "sandbox_artifacts".to_string(),
                        OutputRef {
                            allow_empty_capture: false,
                            hash: missing_zip_hash,
                            persistence: PersistenceFlags::default(),
                        },
                    )]),
                },
            ),
            (
                "z-fresh-valid-zip".to_string(),
                ToolCallInstance {
                    tool_name: tagger_tool_id,
                    metadata: tagger_tool_spec,
                    impure_timestamp: Some(ImpureTimestamp { epoch_seconds: 2, subsec_nanos: 0 }),
                    last_used: None,
                    inputs: BTreeMap::from([(
                        "input_content".to_string(),
                        mediapm_conductor::ResolvedInput::from_hash(input_hash),
                    )]),
                    outputs: BTreeMap::from([(
                        "sandbox_artifacts".to_string(),
                        OutputRef {
                            allow_empty_capture: false,
                            hash: valid_zip_hash,
                            persistence: PersistenceFlags::default(),
                        },
                    )]),
                },
            ),
            (
                "apply-instance".to_string(),
                ToolCallInstance {
                    tool_name: apply_tool_id,
                    metadata: apply_tool_spec,
                    impure_timestamp: None,
                    last_used: None,
                    inputs: BTreeMap::from([(
                        "cover_flag".to_string(),
                        mediapm_conductor::ResolvedInput::from_hash(Hash::from_content(
                            member_bytes,
                        )),
                    )]),
                    outputs: BTreeMap::from([(
                        "content".to_string(),
                        OutputRef {
                            allow_empty_capture: false,
                            hash: final_output_hash,
                            persistence: PersistenceFlags::default(),
                        },
                    )]),
                },
            ),
        ]),
    };

    let step_output_hashes =
        super::resolve_workflow_step_output_hashes(&cas, &machine, &state, &workflow)
            .await
            .expect("resolve step outputs")
            .expect("step outputs should resolve");

    let tagger_output_hash = step_output_hashes
        .get(&step_tagger_id)
        .and_then(|outputs| outputs.get("sandbox_artifacts"))
        .copied()
        .expect("sandbox_artifacts hash should resolve");
    assert_eq!(tagger_output_hash, valid_zip_hash);

    let apply_output_hash = step_output_hashes
        .get(&step_apply_id)
        .and_then(|outputs| outputs.get("content"))
        .copied()
        .expect("content hash should resolve");
    assert_eq!(apply_output_hash, final_output_hash);
}

/// Protects runtime-state resolution when ZIP-selector source artifacts are
/// unavailable in persisted CAS but downstream instance inputs are still
/// present in orchestration state.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn resolve_step_output_hashes_tolerates_missing_zip_selector_source_bytes() {
    let temp = tempfile::tempdir().expect("tempdir");
    let cas = FileSystemCas::open(temp.path()).await.expect("open cas");

    let tagger_tool_id = "mediapm.tools.media-tagger@latest".to_string();
    let tagger_tool_spec = ToolSpec {
        kind: ToolKindSpec::Executable {
            command: vec!["media-tagger.exe".to_string()],
            env_vars: BTreeMap::new(),
            success_codes: vec![0],
        },
        inputs: BTreeMap::from([("input_content".to_string(), ToolInputSpec::default())]),
        outputs: BTreeMap::from([(
            "sandbox_artifacts".to_string(),
            ToolOutputSpec {
                allow_empty: false,
                capture: OutputCaptureSpec::Folder {
                    path: "sandbox".to_string(),
                    include_topmost_folder: false,
                },
            },
        )]),
        ..ToolSpec::default()
    };

    let apply_tool_id = "mediapm.tools.ffmpeg@latest".to_string();
    let apply_tool_spec = ToolSpec {
        kind: ToolKindSpec::Executable {
            command: vec!["ffmpeg.exe".to_string()],
            env_vars: BTreeMap::new(),
            success_codes: vec![0],
        },
        inputs: BTreeMap::from([("cover_flag".to_string(), ToolInputSpec::default())]),
        outputs: BTreeMap::from([(
            "content".to_string(),
            ToolOutputSpec {
                allow_empty: false,
                capture: OutputCaptureSpec::File { path: "output.media".to_string() },
            },
        )]),
        ..ToolSpec::default()
    };

    let step_tagger_id = "step-0-media-tagger".to_string();
    let step_apply_id = "step-1-ffmpeg".to_string();
    let missing_zip_hash = Hash::from_content(b"missing-zip-payload");
    let final_output_hash = Hash::from_content(b"final-media-output");

    let workflow = WorkflowSpec {
        name: Some("zip-selector-missing-source-demo".to_string()),
        description: None,
        steps: vec![
            WorkflowStepSpec {
                id: step_tagger_id.clone(),
                tool: tagger_tool_id.clone(),
                inputs: BTreeMap::from([(
                    "input_content".to_string(),
                    InputBinding::String("tagger-input".to_string()),
                )]),
                depends_on: Vec::new(),
                outputs: BTreeMap::from([(
                    "sandbox_artifacts".to_string(),
                    OutputPolicy { save: Some(OutputSaveMode::Full) },
                )]),
            },
            WorkflowStepSpec {
                id: step_apply_id.clone(),
                tool: apply_tool_id.clone(),
                inputs: BTreeMap::from([(
                    "cover_flag".to_string(),
                    InputBinding::String(format!(
                        "${{step_output.{step_tagger_id}.sandbox_artifacts:zip(coverart-slot-0.flag)}}"
                    )),
                )]),
                depends_on: vec![step_tagger_id.clone()],
                outputs: BTreeMap::from([(
                    "content".to_string(),
                    OutputPolicy { save: Some(OutputSaveMode::Full) },
                )]),
            },
        ],
    };

    let mut machine = MachineNickelDocument::default();
    machine.tools.insert(tagger_tool_id.clone(), tagger_tool_spec.clone());
    machine.tools.insert(apply_tool_id.clone(), apply_tool_spec.clone());

    let state = OrchestrationState {
        version: 1,
        instances: BTreeMap::from([
            (
                "tagger-instance".to_string(),
                ToolCallInstance {
                    tool_name: tagger_tool_id,
                    metadata: tagger_tool_spec,
                    impure_timestamp: Some(ImpureTimestamp { epoch_seconds: 2, subsec_nanos: 0 }),
                    last_used: None,
                    inputs: BTreeMap::from([(
                        "input_content".to_string(),
                        mediapm_conductor::ResolvedInput::from_hash(Hash::from_content(
                            b"tagger-input",
                        )),
                    )]),
                    outputs: BTreeMap::from([(
                        "sandbox_artifacts".to_string(),
                        OutputRef {
                            allow_empty_capture: false,
                            hash: missing_zip_hash,
                            persistence: PersistenceFlags::default(),
                        },
                    )]),
                },
            ),
            (
                "apply-instance".to_string(),
                ToolCallInstance {
                    tool_name: apply_tool_id,
                    metadata: apply_tool_spec,
                    impure_timestamp: Some(ImpureTimestamp { epoch_seconds: 3, subsec_nanos: 0 }),
                    inputs: BTreeMap::from([(
                        "cover_flag".to_string(),
                        mediapm_conductor::ResolvedInput::from_hash(Hash::from_content(
                            b"opaque-runtime-cover-flag",
                        )),
                    )]),
                    outputs: BTreeMap::from([(
                        "content".to_string(),
                        OutputRef {
                            allow_empty_capture: false,
                            hash: final_output_hash,
                            persistence: PersistenceFlags::default(),
                        },
                    )]),
                },
            ),
        ]),
    };

    let step_output_hashes =
        super::resolve_workflow_step_output_hashes(&cas, &machine, &state, &workflow)
            .await
            .expect("resolve step outputs")
            .expect("step outputs should resolve");

    let apply_output_hash = step_output_hashes
        .get(&step_apply_id)
        .and_then(|outputs| outputs.get("content"))
        .copied()
        .expect("content hash should resolve");
    assert_eq!(apply_output_hash, final_output_hash);
}
