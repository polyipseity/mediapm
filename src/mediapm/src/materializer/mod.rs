//! Direct materialization of CAS objects to final output paths for mediapm hierarchy sync.
//!
//! The materializer resolves hierarchy entries, extracts ZIP folders to a
//! temporary directory, and materializes all outputs directly to their final
//! paths under the resolved library directory.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mediapm_cas::{CasApi, FileSystemCas, Hash};
use mediapm_conductor::{MachineNickelDocument, OrchestrationState};
use pulsebar::{MultiProgress, ProgressBar};
use regex::Regex;
use unicode_normalization::UnicodeNormalization;

use crate::conductor_bridge::resolve_ffmpeg_slot_limits;
use crate::config::{
    FlattenedHierarchyEntry, HierarchyEntryKind, MediaPmDocument, PlaylistEntryPathMode,
    expand_variant_selectors, flatten_hierarchy_nodes_for_runtime,
};
use crate::config::{ManagedFileRecord, MediaPmState, ToolRegistryRecord};
use crate::error::MediaPmError;
use crate::paths::MediaPmPaths;

mod commit;
mod file_ops;
pub(crate) mod metadata;
mod playlist;
mod resolve;
#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet, HashSet};
    use std::fs;
    use std::path::Path;

    use mediapm_cas::{CasApi, FileSystemCas, Hash};
    use mediapm_conductor::model::config::ImpureTimestamp;
    use mediapm_conductor::model::config::ToolInputKind;
    use mediapm_conductor::{
        InputBinding, MachineNickelDocument, OrchestrationState, OutputCaptureSpec, OutputPolicy,
        OutputRef, OutputSaveMode, PersistenceFlags, StateNickelDocument, ToolCallInstance,
        ToolInputSpec, ToolKindSpec, ToolOutputSpec, ToolSpec, WorkflowSpec, WorkflowStepSpec,
        encode_state, encode_state_document,
    };
    use unicode_normalization::UnicodeNormalization;

    use crate::config::MediaPmState;
    use crate::config::{
        HierarchyEntry, HierarchyEntryKind, HierarchyFolderRenameRule, HierarchyNode,
        HierarchyNodeKind, HierarchyPath, MaterializationMethod, MediaMetadataRegexTransform,
        MediaMetadataValue, MediaMetadataVariantBinding, MediaPmDocument, MediaSourceSpec,
        MediaStep, MediaStepTool, PlaylistEntryPathMode, PlaylistFormat, PlaylistItemRef,
        SanitizeNamesConfig, TransformInputValue,
    };
    use crate::paths::MediaPmPaths;

    use crate::materializer::{
        instance_matches_expected_inputs, resolve_managed_ffprobe_path, sanitize_path_component,
        sync_hierarchy, validate_components,
    };

    /// Protects managed ffprobe metadata lookup by resolving relative command
    /// selectors under the active managed ffmpeg payload root.
    #[test]
    fn resolve_managed_ffprobe_path_anchors_relative_selector_to_tool_root() {
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(temp.path());

        let ffmpeg_tool_id =
            "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@demo".to_string();
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

        let mut lock = MediaPmState::default();
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

        let ffmpeg_tool_id =
            "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@demo".to_string();
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

        let mut lock = MediaPmState::default();
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
                "hierarchy file 'library/fallback.bin' materialization fell back to 'copy'"
                    .to_string()
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

    fn hierarchy_nodes(
        entries: BTreeMap<String, HierarchyEntry>,
    ) -> Vec<crate::config::HierarchyNode> {
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

                let path: HierarchyPath = HierarchyPath::from(path.as_str());
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
        let components = vec!["movies".to_string(), "Star:Wars.mkv".to_string()];
        let err = validate_components(&components).expect_err("path should fail");
        assert!(err.to_string().contains("forbidden characters"));
    }

    /// Protects NFD-only normalization policy for hierarchy path segments.
    #[test]
    fn hierarchy_path_rejects_non_nfd_segments() {
        let components = vec!["movies".to_string(), "épisode".to_string()];
        let err = validate_components(&components).expect_err("NFD should fail");
        assert!(err.to_string().contains("NFD"));
    }

    #[test]
    fn sanitize_path_component_replaces_reserved_characters() {
        let replacements = BTreeMap::from([(':', '_'), ('<', '_'), ('?', '_')]);

        assert_eq!(sanitize_path_component("Star:Wars?.mkv", &replacements), "Star_Wars_.mkv");
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
                    artist: None,
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

        drop(cas);
        let mut lock = MediaPmState::default();
        sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
            None,
            false,
        )
        .await
        .expect("sync hierarchy");

        let normalized_path =
            format!("library/{}/track.mkv", artist_name.nfd().collect::<String>());
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
                    artist: None,
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

        drop(cas);
        let mut lock = MediaPmState::default();
        sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
            None,
            false,
        )
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
        let duplicate_write =
            super::register_zip_file_entry("captions.en.vtt", &mut extracted_entries)
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
                replacement:
                    "Rick Astley - Never Gonna Give You Up [youtube.dQw4w9WgXcQ].thumbnail.$1"
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
    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn sync_hierarchy_applies_folder_rename_rules_to_zip_members() {
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
                    artist: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::from([(
                        "sidecars".to_string(),
                        zip_hash.to_string(),
                    )]),
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
                        replacement: "Rick Astley - Never Gonna Give You Up [rickroll]$1"
                            .to_string(),
                    }],
                    sanitize_names: SanitizeNamesConfig::Inherit,
                },
            )])),
            ..MediaPmDocument::default()
        };

        drop(cas);
        let mut lock = MediaPmState::default();
        let report = sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
            None,
            false,
        )
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
            lock.managed_files.contains_key(
                "library/rickroll/Rick Astley - Never Gonna Give You Up [rickroll].jpg"
            )
        );
        assert!(
            lock.managed_files.contains_key(
                "library/rickroll/Rick Astley - Never Gonna Give You Up [rickroll].url"
            )
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
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(temp.path());
        let cas_root = paths.root_dir.join(".mediapm").join("store");
        let cas = FileSystemCas::open(&cas_root).await.expect("open cas");

        let sidecar_hash = cas
            .put(zip_payload(&[("info.json", br#"{"id":"demo"}"#)]))
            .await
            .expect("put sidecar zip");
        let root_hash =
            cas.put(zip_payload(&[("thumb.webp", b"webp")])).await.expect("put root zip");

        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "media-a".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: Some("local zip variants".to_string()),
                    title: None,
                    artist: None,
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
                    path: HierarchyPath::from("library/${media.id}/sidecars"),
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
                    path: HierarchyPath::from("library/${media.id}"),
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

        drop(cas);
        let mut lock = MediaPmState::default();
        let report = sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
            None,
            false,
        )
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
            lock.managed_files
                .get("library/media-a/thumb.webp")
                .expect("thumb managed record")
                .variant,
            "root"
        );
    }

    /// Protects nested folder-merge semantics by preserving existing children
    /// when a later parent media-folder commit contributes overlapping
    /// directory names.
    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn sync_hierarchy_preserves_nested_children_on_directory_name_collision() {
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(temp.path());
        let cas_root = paths.root_dir.join(".mediapm").join("store");
        let cas = FileSystemCas::open(&cas_root).await.expect("open cas");

        let sidecar_hash = cas
            .put(zip_payload(&[("info.json", br#"{"id":"demo"}"#)]))
            .await
            .expect("put sidecar zip");
        let root_hash = cas
            .put(zip_payload(&[
                ("sidecars/links.url", b"[InternetShortcut]"),
                ("thumb.webp", b"webp"),
            ]))
            .await
            .expect("put root zip");

        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "media-a".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: Some("local zip variants".to_string()),
                    title: None,
                    artist: None,
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
                    path: HierarchyPath::from("library/${media.id}/sidecars"),
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
                    path: HierarchyPath::from("library/${media.id}"),
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

        drop(cas);
        let mut lock = MediaPmState::default();
        let report = sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
            None,
            false,
        )
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
            lock.managed_files
                .get("library/media-a/thumb.webp")
                .expect("thumb managed record")
                .variant,
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
                    artist: None,
                    workflow_id: None,
                    metadata: Some(BTreeMap::from([(
                        "title".to_string(),
                        MediaMetadataValue::Literal("Demo Title".to_string()),
                    )])),
                    variant_hashes: BTreeMap::from([(
                        "sidecars".to_string(),
                        zip_hash.to_string(),
                    )]),
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

        drop(cas);
        let mut lock = MediaPmState::default();
        let report = sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
            None,
            false,
        )
        .await
        .expect("sync hierarchy");

        assert_eq!(report.materialized_paths, 1);
        assert!(
            paths.hierarchy_root_dir.join("library/renamed/Demo Title [media-a].jpg").is_file()
        );
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
                    artist: None,
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

        drop(cas);
        let mut lock = MediaPmState::default();
        let report = sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
            None,
            false,
        )
        .await
        .expect("sync hierarchy");

        assert_eq!(report.materialized_paths, 1);
        assert!(paths.hierarchy_root_dir.join("library/media-a.bin").exists());
        let metadata = std::fs::metadata(paths.hierarchy_root_dir.join("library/media-a.bin"))
            .expect("metadata");
        assert!(metadata.permissions().readonly(), "managed file should be readonly");
        assert_eq!(
            std::fs::read(paths.hierarchy_root_dir.join("library/media-a.bin"))
                .expect("read output"),
            b"abc"
        );
        let record = lock.managed_files.get("library/media-a.bin").expect("managed record");
        assert_eq!(record.media_id, "media-a");
        assert_eq!(record.hash, hash.to_string());

        let cas = FileSystemCas::open(&cas_root).await.expect("open cas");
        let source_path = cas.object_path_for_hash(hash);
        let output_path = paths.hierarchy_root_dir.join("library/media-a.bin");
        assert_hardlinked_paths(&source_path, &output_path);
    }

    /// Protects playlist hierarchy generation by preserving declared id order,
    /// default relative path rendering, and explicit absolute-path overrides.
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn sync_hierarchy_generates_playlist_with_relative_and_absolute_entries() {
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
                        artist: None,
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
                        artist: None,
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

        drop(cas);
        let mut lock = MediaPmState::default();
        let report = sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
            None,
            false,
        )
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
        let cas = FileSystemCas::open(&cas_root).await.expect("open cas");
        let playlist_hash = record.hash.parse::<Hash>().expect("playlist hash");
        let playlist_bytes_from_cas =
            cas.get(playlist_hash).await.expect("playlist bytes from cas");
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
                        artist: None,
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
                        artist: None,
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
                    path: HierarchyPath::from("library/music/alpha.mp3"),
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
                    path: HierarchyPath::from("library/music/beta.mp3"),
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
                    path: HierarchyPath::from("library/playlists/mixed-ids.m3u8"),
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

        drop(cas);
        let mut lock = MediaPmState::default();
        let report = sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
            None,
            false,
        )
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
                    artist: None,
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
                    path: HierarchyPath::from("library/playlists/folder-only.m3u8"),
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
                    path: HierarchyPath::from("library/sidecars"),
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

        drop(cas);
        let mut lock = MediaPmState::default();
        let error = sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
            None,
            false,
        )
        .await
        .expect_err("playlist references to non-media hierarchy ids must fail");

        assert!(error.to_string().contains("references unknown hierarchy id 'folder-only'"));
        assert!(!paths.hierarchy_root_dir.join("library/playlists/folder-only.m3u8").exists());
    }

    /// Protects playlist resolution when two media entries share the same
    /// template path but reference different `media_ids` — without the fix,
    /// both playlist entries resolve to the same (last-writer-wins) path.
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn sync_hierarchy_playlist_resolves_different_media_ids_with_same_template_path() {
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(temp.path());
        let cas_root = paths.root_dir.join(".mediapm").join("store");
        let cas = FileSystemCas::open(&cas_root).await.expect("open cas");
        let alpha_hash = cas.put(b"alpha".to_vec()).await.expect("put alpha bytes");
        let beta_hash = cas.put(b"beta".to_vec()).await.expect("put beta bytes");

        let document = MediaPmDocument {
            media: BTreeMap::from([
                (
                    "song_a".to_string(),
                    MediaSourceSpec {
                        id: None,
                        description: Some("file: song_a.bin".to_string()),
                        title: None,
                        artist: None,
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
                    "song_b".to_string(),
                    MediaSourceSpec {
                        id: None,
                        description: Some("file: song_b.bin".to_string()),
                        title: None,
                        artist: None,
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
                    path: HierarchyPath::from("${media.id}.bin"),
                    kind: HierarchyNodeKind::Media,
                    id: Some("entry-a".to_string()),
                    media_id: Some("song_a".to_string()),
                    variant: Some("default".to_string()),
                    variants: Vec::new(),
                    rename_files: Vec::new(),
                    sanitize_names: SanitizeNamesConfig::Inherit,
                    format: PlaylistFormat::M3u8,
                    ids: Vec::new(),
                    children: Vec::new(),
                },
                crate::config::HierarchyNode {
                    path: HierarchyPath::from("${media.id}.bin"),
                    kind: HierarchyNodeKind::Media,
                    id: Some("entry-b".to_string()),
                    media_id: Some("song_b".to_string()),
                    variant: Some("default".to_string()),
                    variants: Vec::new(),
                    rename_files: Vec::new(),
                    sanitize_names: SanitizeNamesConfig::Inherit,
                    format: PlaylistFormat::M3u8,
                    ids: Vec::new(),
                    children: Vec::new(),
                },
                crate::config::HierarchyNode {
                    path: HierarchyPath::from("playlist.m3u8"),
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
                            id: "entry-a".to_string(),
                            path: PlaylistEntryPathMode::Relative,
                        },
                        PlaylistItemRef {
                            id: "entry-b".to_string(),
                            path: PlaylistEntryPathMode::Relative,
                        },
                    ],
                    children: Vec::new(),
                },
            ],
            ..MediaPmDocument::default()
        };

        drop(cas);
        let mut lock = MediaPmState::default();
        let report = sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
            None,
            false,
        )
        .await
        .expect("sync hierarchy");

        assert_eq!(report.materialized_paths, 3);

        let playlist_path = paths.hierarchy_root_dir.join("playlist.m3u8");
        let playlist_text = std::fs::read_to_string(&playlist_path).expect("read playlist file");
        assert_eq!(playlist_text, "#EXTM3U\nsong_a.bin\nsong_b.bin\n");
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
                    artist: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::from([(
                        "default".to_string(),
                        alpha_hash.to_string(),
                    )]),
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

        drop(cas);
        let mut lock = MediaPmState::default();
        let report = sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
            None,
            false,
        )
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
        let cas = FileSystemCas::open(&cas_root).await.expect("open cas");
        let playlist_hash = record.hash.parse::<Hash>().expect("playlist hash");
        let playlist_bytes_from_cas =
            cas.get(playlist_hash).await.expect("playlist bytes from cas");
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
                    artist: None,
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

        drop(cas);
        let mut lock = MediaPmState::default();
        let report = sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
            None,
            false,
        )
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
                    artist: None,
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

        drop(cas);
        let mut lock = MediaPmState::default();
        let report = sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
            None,
            false,
        )
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
                    artist: None,
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

        drop(cas);
        let mut lock = MediaPmState::default();
        let report = sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
            None,
            false,
        )
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
        let infojson_hash =
            cas.put(br#"{"ext":"mkv"}"#.to_vec()).await.expect("put infojson bytes");

        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "media-a".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: Some("file: source.bin".to_string()),
                    title: None,
                    artist: None,
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

        drop(cas);
        let mut lock = MediaPmState::default();
        let report = sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
            None,
            false,
        )
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
                    artist: None,
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

        drop(cas);
        let mut lock = MediaPmState::default();
        let report = sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
            None,
            false,
        )
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
                artist: None,
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
                mediapm_conductor::ResolvedInput::from_hash(Hash::from_content(
                    source_uri.as_bytes(),
                ))
                .into(),
            ),
            (
                "leading_args".to_string(),
                mediapm_conductor::ResolvedInput::from_string_list(vec![
                    "--format".to_string(),
                    "bestaudio/best".to_string(),
                ])
                .expect("list hash")
                .into(),
            ),
            (
                "trailing_args".to_string(),
                mediapm_conductor::ResolvedInput::from_string_list(Vec::new())
                    .expect("empty list hash")
                    .into(),
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
            aux: BTreeMap::new(),
            referenced_instance_keys: HashSet::new(),
        };

        let state_pointer = encode_state(&cas, state).await.expect("encode state");
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
            artist: None,
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

        drop(cas);
        let mut lock = MediaPmState::default();
        let report = sync_hierarchy(&paths, &document, &machine, &cas_root, &mut lock, None, false)
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
                    artist: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::from([(
                        "default".to_string(),
                        missing_hash.to_string(),
                    )]),
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

        let mut lock = MediaPmState::default();
        let error = sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
            None,
            false,
        )
        .await
        .expect_err("missing local CAS payload must fail materialization");

        let error_text = error.to_string();
        assert!(
            error_text.contains("materializing hierarchy file 'demo/local.bin'"),
            "unexpected error: {error_text}"
        );
        assert!(error_text.contains("not found"));
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
                    artist: None,
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

        let mut lock = MediaPmState::default();
        let error = sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
            None,
            false,
        )
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
                kind: ToolKindSpec::Builtin {
                    name: "echo".to_string(),
                    version: "1.0.0".to_string(),
                },
                ..ToolSpec::default()
            },
            impure_timestamp: None,
            inputs: BTreeMap::from([
                (
                    "text".to_string(),
                    mediapm_conductor::ResolvedInput::from_hash(expected_text_hash).into(),
                ),
                (
                    "leading_args".to_string(),
                    mediapm_conductor::ResolvedInput::from_string_list(vec![
                        "--verbose".to_string(),
                    ])
                    .expect("list hash")
                    .into(),
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
                        inputs: BTreeMap::from([(
                            "source_url".to_string(),
                            mediapm_conductor::ResolvedInput::from_hash(source_url_hash).into(),
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
                        inputs: BTreeMap::from([(
                            "source_url".to_string(),
                            mediapm_conductor::ResolvedInput::from_hash(source_url_hash).into(),
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
            aux: BTreeMap::new(),
            referenced_instance_keys: HashSet::new(),
        };

        let step_output_hashes = super::resolve_workflow_step_output_hashes(
            &cas,
            &machine,
            &state,
            &workflow,
            &BTreeMap::new(),
        )
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
                        impure_timestamp: Some(ImpureTimestamp {
                            epoch_seconds: 1,
                            subsec_nanos: 0,
                        }),
                        inputs: BTreeMap::from([(
                            "input_content".to_string(),
                            mediapm_conductor::ResolvedInput::from_hash(input_hash).into(),
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
                        impure_timestamp: Some(ImpureTimestamp {
                            epoch_seconds: 2,
                            subsec_nanos: 0,
                        }),
                        inputs: BTreeMap::from([(
                            "input_content".to_string(),
                            mediapm_conductor::ResolvedInput::from_hash(input_hash).into(),
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
                        inputs: BTreeMap::from([(
                            "cover_flag".to_string(),
                            mediapm_conductor::ResolvedInput::from_hash(Hash::from_content(
                                member_bytes,
                            ))
                            .into(),
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
            aux: BTreeMap::new(),
            referenced_instance_keys: HashSet::new(),
        };

        let step_output_hashes = super::resolve_workflow_step_output_hashes(
            &cas,
            &machine,
            &state,
            &workflow,
            &BTreeMap::new(),
        )
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
                        impure_timestamp: Some(ImpureTimestamp {
                            epoch_seconds: 2,
                            subsec_nanos: 0,
                        }),
                        inputs: BTreeMap::from([(
                            "input_content".to_string(),
                            mediapm_conductor::ResolvedInput::from_hash(Hash::from_content(
                                b"tagger-input",
                            ))
                            .into(),
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
                        impure_timestamp: Some(ImpureTimestamp {
                            epoch_seconds: 3,
                            subsec_nanos: 0,
                        }),
                        inputs: BTreeMap::from([(
                            "cover_flag".to_string(),
                            mediapm_conductor::ResolvedInput::from_hash(Hash::from_content(
                                b"opaque-runtime-cover-flag",
                            ))
                            .into(),
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
            aux: BTreeMap::new(),
            referenced_instance_keys: HashSet::new(),
        };

        let step_output_hashes = super::resolve_workflow_step_output_hashes(
            &cas,
            &machine,
            &state,
            &workflow,
            &BTreeMap::new(),
        )
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
}
mod zip;

use self::commit::{
    check_nfd_source, ensure_managed_path_readonly, now_unix_seconds, remove_path,
    sanitize_path_component, unix_epoch_millis, validate_components,
};
use self::file_ops::materialize_file_from_cas_with_order;
use self::metadata::{
    resolve_hierarchy_folder_rename_rule_replacements, resolve_hierarchy_relative_path,
    resolve_managed_ffprobe_path,
};
use self::playlist::{
    collect_media_entries_by_id, collect_playlist_media_index, join_relative_paths,
    playlist_format_label, render_absolute_playlist_path, render_playlist_bytes,
    render_relative_playlist_path, resolve_playlist_media_target_relative_path,
};
use self::resolve::{
    collect_media_source_available_variants, load_runtime_orchestration_state,
    resolve_hierarchy_source, resolve_variant_source_bytes, resolve_variant_source_hash,
};
#[cfg(test)]
use self::resolve::{instance_matches_expected_inputs, resolve_workflow_step_output_hashes};
#[cfg(test)]
use self::zip::{apply_hierarchy_folder_rename_rules, register_zip_file_entry};
use self::zip::{compile_hierarchy_folder_rename_rules, extract_zip_folder_variant_bytes};

/// Maximum concurrent hierarchy materialization workers.
const HIERARCHY_MAX_CONCURRENCY: usize = 1024;

/// Maximum number of Unicode scalar values shown in one progress filename.
const HIERARCHY_PROGRESS_MAX_FILENAME_CHARS: usize = 48;

/// One prepared hierarchy-entry materialization result.
#[derive(Debug)]
struct PreparedHierarchyEntryResult {
    /// Flat hierarchy entry path template after placeholder resolution.
    relative_path: String,
    /// Final destination path for materialized output.
    final_path: PathBuf,
    /// Managed media id to persist in lock records when materialized.
    managed_media_id: Option<String>,
    /// Managed variant table keyed by materialized relative path.
    managed_file_variants: BTreeMap<String, String>,
    /// Managed CAS hash table keyed by materialized relative path.
    managed_file_hashes: BTreeMap<String, Hash>,
    /// Desired managed paths produced when one entry is skipped.
    skipped_paths: Vec<String>,
    /// Paths whose lock timestamps should be refreshed on skip.
    refreshed_lock_paths: Vec<String>,
    /// Worker notices collected while preparing this entry.
    notices: Vec<String>,
    /// Whether this entry was skipped by hash-change detection.
    skipped_entry: bool,
}

/// Summary of one materialization pass.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MaterializeReport {
    /// Number of hierarchy entries staged and committed.
    pub materialized_paths: usize,
    /// Number of hierarchy entries whose CAS hash matched the lock record and
    /// whose final output path was confirmed present on disk — skipped.
    pub skipped_paths: usize,
    /// Number of previously managed outputs removed as stale.
    pub removed_paths: usize,
    /// Number of empty parent directories removed after stale path cleanup.
    pub removed_empty_dirs: usize,
    /// Link/copy fallback notices captured during materialization.
    pub notices: Vec<String>,
}

/// Per-workflow step output hash table (`step_id -> output_name -> CAS hash`).
type StepOutputHashes = BTreeMap<String, BTreeMap<String, Hash>>;

/// Per-workflow required step output names (`step_id -> output_name[]`).
type RequiredStepOutputNames = BTreeMap<String, BTreeSet<String>>;

/// Per-workflow required ZIP member selectors (`step_id -> output_name -> zip_member[]`).
type RequiredStepZipMembers = BTreeMap<String, BTreeMap<String, BTreeSet<String>>>;

/// Per-step expected inputs used to match runtime workflow instances.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct ExpectedStepInputs {
    /// Deterministically resolved input hashes.
    resolved_hashes: BTreeMap<String, Hash>,
    /// Input names whose hashes cannot be reconstructed from persisted CAS,
    /// but that must still exist on candidate runtime instances.
    unresolved_hash_input_names: BTreeSet<String>,
}

/// One input-binding hash resolution result.
enum InputBindingHashResolution {
    /// Fully reconstructed deterministic input hash.
    Resolved(Hash),
    /// Referenced prior step output is unavailable in the current traversal
    /// order, so this step cannot be matched yet.
    MissingPriorStepOutput,
    /// Referenced step output exists in state but cannot be reconstructed from
    /// persisted CAS bytes (for example sandbox artifacts only available in
    /// ephemeral/in-memory execution context).
    MissingMaterializedStepOutput,
}

/// One compiled hierarchy rename rule used during ZIP folder extraction.
#[derive(Debug, Clone)]
struct CompiledHierarchyFolderRenameRule {
    /// Original configured regex pattern (for diagnostics).
    pattern: String,
    /// Replacement text applied when `pattern` matches.
    replacement: String,
    /// Precompiled regex for efficient per-entry application.
    regex: Regex,
}

/// Shared lookup context for resolving materialization-time variant payloads.
///
/// The materializer repeatedly resolves variant bytes from either local CAS
/// pointers or managed workflow outputs. This context groups immutable lookup
/// dependencies so helper signatures remain compact and consistent.
#[derive(Clone)]
struct MaterializationLookupContext {
    /// Conductor CAS store used for payload reads.
    cas: Arc<FileSystemCas>,
    /// Resolved conductor machine document for tool/workflow metadata.
    machine: Arc<MachineNickelDocument>,
    /// Optional persisted orchestration state loaded from runtime pointer.
    orchestration_state: Option<Arc<OrchestrationState>>,
    /// Effective ffmpeg input-slot limit used for output-binding resolution.
    ffmpeg_max_input_slots: usize,
    /// Effective ffmpeg output-slot limit used for output-binding resolution.
    ffmpeg_max_output_slots: usize,
    /// Host-resolved managed ffprobe path derived from active managed ffmpeg.
    managed_ffprobe_path: Option<PathBuf>,
    /// Cache of resolved workflow step output hashes keyed by workflow ID.
    ///
    /// The orchestration state is immutable during `sync_hierarchy`, so
    /// repeated resolution for the same workflow yields the same result.
    /// This avoids O(steps × instances) scans for each hierarchy entry's
    /// variant resolution when many entries share the same workflow.
    step_output_hashes_cache: Arc<Mutex<HashMap<String, Option<StepOutputHashes>>>>,
    /// Persistent metadata cache keyed by BLAKE3 hex of `media_id` for resolved
    /// ffprobe/JSON metadata values. Cache is opened by `sync_hierarchy()` and
    /// shared across workers.
    metadata_cache: Option<Arc<crate::metadata_cache::MetadataCache>>,
    /// Tool registry for reverse-lookup of logical tool names from tool IDs.
    /// Populated from `MediaPmState.tool_registry` at context construction time.
    tool_registry: BTreeMap<String, ToolRegistryRecord>,
}

/// Resolved payload bytes for one materialized variant request.
#[derive(Debug, Clone, PartialEq, Eq)]
struct VariantSourceBytes {
    /// Bytes to stage for the requested variant.
    bytes: Vec<u8>,
    /// Optional fallback notice (for example variant-default fallback).
    notice: Option<String>,
    /// Optional direct source hash when staged bytes exactly match one
    /// existing CAS object (no derived ZIP-member extraction).
    source_hash: Option<Hash>,
}

/// One rendered playlist row with source identity and emitted path text.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RenderedPlaylistItem {
    /// Referenced hierarchy id used for diagnostics and title fields.
    id: String,
    /// Rendered path written to playlist payload.
    path: String,
}

/// Computes bounded hierarchy materialization worker parallelism.
#[must_use]
fn hierarchy_worker_count(total_entries: usize) -> usize {
    if total_entries == 0 {
        return 1;
    }

    let cpu_hint = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
    total_entries.min(cpu_hint).clamp(1, HIERARCHY_MAX_CONCURRENCY)
}

/// Formats one hierarchy entry kind for progress-row messages.
#[must_use]
fn hierarchy_entry_kind_label(kind: HierarchyEntryKind) -> &'static str {
    match kind {
        HierarchyEntryKind::Media => "media",
        HierarchyEntryKind::MediaFolder => "media_folder",
        HierarchyEntryKind::Playlist => "playlist",
    }
}

/// Returns the basename-oriented hierarchy label shown in worker progress.
#[must_use]
fn hierarchy_progress_filename_label(path: &str) -> String {
    let trimmed = path.trim_end_matches(['/', '\\']);
    let candidate = if trimmed.is_empty() { path } else { trimmed };
    let file_name = Path::new(candidate)
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.trim().is_empty())
        .unwrap_or(candidate);
    truncate_progress_label(file_name, HIERARCHY_PROGRESS_MAX_FILENAME_CHARS)
}

/// Truncates one progress label to a bounded character length with ellipsis.
#[must_use]
fn truncate_progress_label(value: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }

    let char_count = value.chars().count();
    if char_count <= max_chars {
        return value.to_string();
    }

    if max_chars <= 1 {
        return "…".to_string();
    }

    let prefix = value.chars().take(max_chars - 1).collect::<String>();
    format!("{prefix}…")
}

/// Prepares one hierarchy entry for materialization to its final output path.
#[expect(
    clippy::too_many_lines,
    reason = "this helper intentionally keeps per-entry materialization behavior unified for deterministic worker execution"
)]
#[expect(
    clippy::too_many_arguments,
    reason = "per-entry materialization needs explicit immutable inputs to keep worker behavior deterministic"
)]
async fn prepare_hierarchy_entry(
    paths: &MediaPmPaths,
    document: &MediaPmDocument,
    lock: &MediaPmState,
    lookup: &MaterializationLookupContext,
    materialization_methods: &[crate::config::MaterializationMethod],
    playlist_media_index: &BTreeMap<String, Vec<String>>,
    media_entries_by_id: &BTreeMap<String, crate::config::HierarchyEntry>,
    flattened_entry: &FlattenedHierarchyEntry,
    job_index: usize,
    progress_bar: &ProgressBar,
) -> Result<PreparedHierarchyEntryResult, MediaPmError> {
    let path_components = flattened_entry.path_components.as_slice();
    let entry = &flattened_entry.entry;

    progress_bar.set_position(0);
    progress_bar
        .set_message(&format!("{}: resolving filename", hierarchy_entry_kind_label(entry.kind)));

    // Pipeline: check NFD → resolve templates per component → force NFD →
    // sanitize per component → validate → join once at the end.
    check_nfd_source(path_components)?;
    let mut resolved_components =
        if matches!(entry.kind, HierarchyEntryKind::Media | HierarchyEntryKind::MediaFolder) {
            let source = resolve_hierarchy_source(document, entry)?;
            resolve_hierarchy_relative_path(path_components, entry, source, lookup).await?
        } else {
            path_components.to_vec()
        };
    for component in &mut resolved_components {
        *component = component.nfd().collect::<String>();
    }
    if entry.sanitize_names.is_enabled() {
        let runtime_replacements = document.runtime.path_sanitization_mapping_with_defaults()?;
        let effective_replacements =
            entry.sanitize_names.replacement_map_with_defaults(&runtime_replacements);
        for component in &mut resolved_components {
            *component = sanitize_path_component(component, &effective_replacements);
        }
    }
    let relative_path = validate_components(&resolved_components)?.join("/");
    progress_bar.set_message(&format!(
        "{}: {}",
        hierarchy_entry_kind_label(entry.kind),
        hierarchy_progress_filename_label(&relative_path)
    ));
    let fs_relative_path = relative_path.as_str();

    if fs_relative_path.is_empty() {
        return Err(MediaPmError::Workflow(format!(
            "hierarchy path '{relative_path}' must not resolve to an empty filesystem path"
        )));
    }

    let final_path = paths.hierarchy_root_dir.join(fs_relative_path);
    progress_bar.set_position(10);

    let mut notices = Vec::new();
    let mut skipped_paths = Vec::new();
    let mut refreshed_lock_paths = Vec::new();
    let (managed_media_id, managed_file_variants, managed_file_hashes, skipped_entry) = match entry
        .kind
    {
        HierarchyEntryKind::Media => {
            let source = resolve_hierarchy_source(document, entry)?;

            if entry.variants.is_empty() {
                return Err(MediaPmError::Workflow(format!(
                    "hierarchy path '{relative_path}' must define at least one variant"
                )));
            }

            let available_variants = collect_media_source_available_variants(source);
            let resolved_variants = expand_variant_selectors(&entry.variants, &available_variants)
                .map_err(|reason| {
                    MediaPmError::Workflow(format!(
                        "hierarchy path '{relative_path}' {reason} for media '{}'",
                        entry.media_id
                    ))
                })?;

            if resolved_variants.len() != 1 {
                return Err(MediaPmError::Workflow(format!(
                    "hierarchy file path '{relative_path}' must resolve exactly one variant"
                )));
            }
            let variant = resolved_variants
                .first()
                .expect("checked non-empty and len==1 for hierarchy file path");

            let hint_hash =
                resolve_variant_source_hash(lookup, &entry.media_id, source, variant).await?;

            if let Some(hint_hash) = hint_hash {
                let hint_hash_str = hint_hash.to_string();
                if lock.managed_files.get(&relative_path).is_some_and(|r| r.hash == hint_hash_str)
                    && fs::symlink_metadata(&final_path).is_ok()
                {
                    skipped_paths.push(relative_path.clone());
                    refreshed_lock_paths.push(relative_path.clone());
                    progress_bar.set_position(100);
                    return Ok(PreparedHierarchyEntryResult {
                        relative_path,
                        final_path,
                        managed_media_id: None,
                        managed_file_variants: BTreeMap::new(),
                        managed_file_hashes: BTreeMap::new(),
                        skipped_paths,
                        refreshed_lock_paths,
                        notices,
                        skipped_entry: true,
                    });
                }

                // Hash resolvable without loading bytes: materialize
                // directly from CAS, avoiding potentially large heap
                // copies (up to ~891 MB per video file).
                if let Some(parent) = final_path.parent() {
                    tokio::fs::create_dir_all(parent).await.map_err(|source_err| {
                        MediaPmError::Io {
                            operation: "creating final parent directory".to_string(),
                            path: parent.to_path_buf(),
                            source: source_err,
                        }
                    })?;
                }

                progress_bar.set_position(35);
                materialize_file_from_cas_with_order(
                    &lookup.cas,
                    hint_hash,
                    &final_path,
                    relative_path.as_str(),
                    materialization_methods,
                    &mut notices,
                )
                .await?;

                progress_bar.set_position(100);
                (
                    Some(entry.media_id.clone()),
                    BTreeMap::from([(relative_path.clone(), variant.clone())]),
                    BTreeMap::from([(relative_path.clone(), hint_hash)]),
                    false,
                )
            } else {
                // Hash not directly resolvable (ZIP member extraction or
                // no workflow state/variant_hashes): load bytes from CAS
                // and resolve content.
                if let Some(parent) = final_path.parent() {
                    tokio::fs::create_dir_all(parent).await.map_err(|source_err| {
                        MediaPmError::Io {
                            operation: "creating final parent directory".to_string(),
                            path: parent.to_path_buf(),
                            source: source_err,
                        }
                    })?;
                }

                progress_bar.set_position(35);
                let variant_source =
                    resolve_variant_source_bytes(lookup, &entry.media_id, source, variant).await?;
                if let Some(message) = variant_source.notice.as_deref() {
                    notices.push(message.to_string());
                }

                let file_hash = if let Some(source_hash) = variant_source.source_hash {
                    source_hash
                } else {
                    lookup.cas.put(variant_source.bytes).await.map_err(|source| {
                        MediaPmError::Workflow(format!(
                            "importing materialized file '{relative_path}' into CAS failed: {source}",
                        ))
                    })?
                };

                progress_bar.set_position(70);
                materialize_file_from_cas_with_order(
                    &lookup.cas,
                    file_hash,
                    &final_path,
                    relative_path.as_str(),
                    materialization_methods,
                    &mut notices,
                )
                .await?;

                (
                    Some(entry.media_id.clone()),
                    BTreeMap::from([(relative_path.clone(), variant.clone())]),
                    BTreeMap::from([(relative_path.clone(), file_hash)]),
                    false,
                )
            }
        }
        HierarchyEntryKind::MediaFolder => {
            let source = resolve_hierarchy_source(document, entry)?;

            if entry.variants.is_empty() {
                return Err(MediaPmError::Workflow(format!(
                    "hierarchy path '{relative_path}' must define at least one variant"
                )));
            }

            let available_variants = collect_media_source_available_variants(source);
            let resolved_variants = expand_variant_selectors(&entry.variants, &available_variants)
                .map_err(|reason| {
                    MediaPmError::Workflow(format!(
                        "hierarchy path '{relative_path}' {reason} for media '{}'",
                        entry.media_id
                    ))
                })?;

            tokio::fs::create_dir_all(&final_path).await.map_err(|source_err| {
                MediaPmError::Io {
                    operation: "creating final output directory".to_string(),
                    path: final_path.clone(),
                    source: source_err,
                }
            })?;

            let rename_replacements = if entry.sanitize_names.is_enabled() {
                let runtime_replacements =
                    document.runtime.path_sanitization_mapping_with_defaults()?;
                entry.sanitize_names.replacement_map_with_defaults(&runtime_replacements)
            } else {
                BTreeMap::new()
            };

            let resolved_rename_rules = resolve_hierarchy_folder_rename_rule_replacements(
                &entry.rename_files,
                &relative_path,
                entry,
                source,
                lookup,
                &rename_replacements,
            )
            .await?;
            let compiled_rename_rules = compile_hierarchy_folder_rename_rules(
                &resolved_rename_rules,
                &relative_path,
                &entry.media_id,
            )?;

            let extract_dir =
                paths.mediapm_tmp_dir.join(format!("extract-{}-{}", now_unix_seconds(), job_index));
            tokio::fs::create_dir_all(&extract_dir).await.map_err(|source_err| {
                MediaPmError::Io {
                    operation: "creating temp extraction directory".to_string(),
                    path: extract_dir.clone(),
                    source: source_err,
                }
            })?;

            progress_bar.set_position(30);
            let mut extracted_entries = BTreeMap::new();
            let mut extracted_entry_variants = BTreeMap::<String, String>::new();
            let entry_sanitization = document.runtime.path_sanitization_mapping_with_defaults()?;
            for variant in &resolved_variants {
                let variant_source =
                    resolve_variant_source_bytes(lookup, &entry.media_id, source, variant).await?;
                if let Some(message) = variant_source.notice.as_deref() {
                    notices.push(message.to_string());
                }

                extract_zip_folder_variant_bytes(
                    variant_source.bytes.as_slice(),
                    &extract_dir,
                    &relative_path,
                    &entry.media_id,
                    variant,
                    &compiled_rename_rules,
                    &entry_sanitization,
                    &mut extracted_entries,
                    &mut extracted_entry_variants,
                )?;
            }

            let mut managed_file_hashes = BTreeMap::new();
            let mut managed_file_variants = BTreeMap::new();
            for (entry_path, is_directory) in &extracted_entries {
                if *is_directory {
                    continue;
                }

                let managed_path = join_relative_paths(fs_relative_path, entry_path);
                let extract_file_path = extract_dir.join(entry_path);
                let extracted_bytes =
                    tokio::fs::read(&extract_file_path).await.map_err(|source_err| {
                        MediaPmError::Io {
                            operation: "reading extracted file bytes for CAS import".to_string(),
                            path: extract_file_path.clone(),
                            source: source_err,
                        }
                    })?;
                let extracted_hash = lookup.cas.put(extracted_bytes).await.map_err(|source| {
                        MediaPmError::Workflow(format!(
                            "importing materialized folder member '{managed_path}' into CAS failed: {source}",
                        ))
                    })?;
                let final_file_path = final_path.join(entry_path);
                materialize_file_from_cas_with_order(
                    &lookup.cas,
                    extracted_hash,
                    &final_file_path,
                    &managed_path,
                    materialization_methods,
                    &mut notices,
                )
                .await?;
                managed_file_hashes.insert(managed_path.clone(), extracted_hash);

                let entry_variant = extracted_entry_variants
                        .get(entry_path)
                        .cloned()
                        .ok_or_else(|| {
                            MediaPmError::Workflow(format!(
                                "missing extracted variant provenance for hierarchy path '{relative_path}' media '{}' extracted file '{entry_path}'",
                                entry.media_id
                            ))
                        })?;
                managed_file_variants.insert(managed_path, entry_variant);
            }

            // Clean up temp extraction directory.
            tokio::fs::remove_dir_all(&extract_dir).await.map_err(|source_err| {
                MediaPmError::Io {
                    operation: "removing temp extraction directory".to_string(),
                    path: extract_dir.clone(),
                    source: source_err,
                }
            })?;

            (Some(entry.media_id.clone()), managed_file_variants, managed_file_hashes, false)
        }
        HierarchyEntryKind::Playlist => {
            if relative_path.ends_with('/') || relative_path.ends_with('\\') {
                return Err(MediaPmError::Workflow(format!(
                    "hierarchy playlist path '{relative_path}' must be a file path"
                )));
            }
            if entry.ids.is_empty() {
                return Err(MediaPmError::Workflow(format!(
                    "hierarchy playlist path '{relative_path}' must define at least one playlist id"
                )));
            }

            let mut resolved_playlist_media_targets = BTreeMap::<String, String>::new();
            let mut rendered_items = Vec::with_capacity(entry.ids.len());
            for (item_index, item) in entry.ids.iter().enumerate() {
                let requested_id = item.id().trim();
                if requested_id.is_empty() {
                    return Err(MediaPmError::Workflow(format!(
                        "hierarchy playlist path '{relative_path}' ids[{item_index}] has empty id"
                    )));
                }

                let media_path_components = playlist_media_index.get(requested_id).ok_or_else(|| {
                        MediaPmError::Workflow(format!(
                            "hierarchy playlist path '{relative_path}' ids[{item_index}] references unknown hierarchy id '{requested_id}'"
                        ))
                    })?;

                let target_relative = resolve_playlist_media_target_relative_path(
                    document,
                    lookup,
                    media_path_components.as_slice(),
                    requested_id,
                    media_entries_by_id,
                    &mut resolved_playlist_media_targets,
                )
                .await?;

                let rendered_path = match item.path_mode() {
                    PlaylistEntryPathMode::Relative => {
                        render_relative_playlist_path(&relative_path, &target_relative)
                    }
                    PlaylistEntryPathMode::Absolute => {
                        render_absolute_playlist_path(paths, &target_relative)
                    }
                };

                rendered_items.push(RenderedPlaylistItem {
                    id: requested_id.to_string(),
                    path: rendered_path,
                });
            }

            if let Some(parent) = final_path.parent() {
                tokio::fs::create_dir_all(parent).await.map_err(|source_err| MediaPmError::Io {
                    operation: "creating final parent directory".to_string(),
                    path: parent.to_path_buf(),
                    source: source_err,
                })?;
            }

            progress_bar.set_position(70);
            let playlist_bytes = render_playlist_bytes(entry.format, &rendered_items);
            let playlist_hash = lookup.cas.put(playlist_bytes).await.map_err(|source| {
                MediaPmError::Workflow(format!(
                    "importing generated playlist '{relative_path}' into CAS failed: {source}",
                ))
            })?;
            materialize_file_from_cas_with_order(
                &lookup.cas,
                playlist_hash,
                &final_path,
                relative_path.as_str(),
                materialization_methods,
                &mut notices,
            )
            .await?;

            (
                Some("playlist".to_string()),
                BTreeMap::from([(
                    relative_path.clone(),
                    format!("playlist:{}", playlist_format_label(entry.format)),
                )]),
                BTreeMap::from([(relative_path.clone(), playlist_hash)]),
                false,
            )
        }
    };

    progress_bar.set_position(100);
    Ok(PreparedHierarchyEntryResult {
        relative_path,
        final_path,
        managed_media_id,
        managed_file_variants,
        managed_file_hashes,
        skipped_paths,
        refreshed_lock_paths,
        notices,
        skipped_entry,
    })
}

/// Synchronizes hierarchy entries, materializing CAS objects directly to
/// final output paths. CAS integrity is trusted without separate verification.
#[expect(
    clippy::too_many_lines,
    reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
)]
pub async fn sync_hierarchy(
    paths: &MediaPmPaths,
    document: &MediaPmDocument,
    machine: &MachineNickelDocument,
    conductor_cas_root: &Path,
    lock: &mut MediaPmState,
    current_materialized_hash: Option<Hash>,
    _verify_materialization: bool,
) -> Result<MaterializeReport, MediaPmError> {
    // Fast path: skip materialization when the orchestration state hash
    // hasn't changed since the last sync. When both are `None` there is no
    // prior materialization to skip — proceed with full materialization.
    if let Some(last) = lock.last_materialized_state_hash
        && current_materialized_hash == Some(last)
    {
        return Ok(MaterializeReport::default());
    }

    let ffmpeg_slot_limits = resolve_ffmpeg_slot_limits(&document.tools)?;
    let ffmpeg_max_input_slots = ffmpeg_slot_limits.max_input_slots;
    let ffmpeg_max_output_slots = ffmpeg_slot_limits.max_output_slots;
    let materialization_methods = document.runtime.materialization_preference_order_with_defaults();

    fs::create_dir_all(&paths.mediapm_tmp_dir).map_err(|source| MediaPmError::Io {
        operation: "creating mediapm runtime temporary directory".to_string(),
        path: paths.mediapm_tmp_dir.clone(),
        source,
    })?;
    tokio::fs::create_dir_all(&paths.hierarchy_root_dir).await.map_err(|source| {
        MediaPmError::Io {
            operation: "creating resolved library directory".to_string(),
            path: paths.hierarchy_root_dir.clone(),
            source,
        }
    })?;

    let cas = Arc::new(FileSystemCas::open(conductor_cas_root).await.map_err(|source| {
        MediaPmError::Workflow(format!(
            "opening conductor CAS store '{}' for materialization failed: {source}",
            conductor_cas_root.display()
        ))
    })?);
    let orchestration_state = load_runtime_orchestration_state(paths, &cas).await?.map(Arc::new);
    let managed_ffprobe_path = resolve_managed_ffprobe_path(paths, machine, lock);
    let metadata_cache =
        crate::metadata_cache::MetadataCache::open(&paths.workspace_mediapm_cache_dir())
            .map_err(|e| tracing::warn!("failed to open metadata cache: {e}"))
            .ok()
            .map(Arc::new);
    let lookup = MaterializationLookupContext {
        cas: Arc::clone(&cas),
        machine: Arc::new(machine.clone()),
        orchestration_state,
        ffmpeg_max_input_slots,
        ffmpeg_max_output_slots,
        managed_ffprobe_path,
        step_output_hashes_cache: Arc::new(Mutex::new(HashMap::new())),
        metadata_cache,
        tool_registry: lock.tool_registry.clone(),
    };

    let mut report = MaterializeReport::default();
    let mut desired_paths = BTreeSet::new();
    let flattened_hierarchy = flatten_hierarchy_nodes_for_runtime(&document.hierarchy)?;
    let playlist_media_index = collect_playlist_media_index(&flattened_hierarchy)?;
    let media_entries_by_id = collect_media_entries_by_id(&flattened_hierarchy);
    let worker_count = hierarchy_worker_count(flattened_hierarchy.len());

    let total_entries = flattened_hierarchy.len();
    let multi = MultiProgress::new();
    let hierarchy_progress = multi
        .add_bar(total_entries.max(1) as u64)
        .with_message(&format!("syncing hierarchy ({worker_count} concurrent workers)"))
        .with_format("{msg}  {bar}  {pos}/{total}");
    hierarchy_progress.set_position(0);
    let operation_bars = (0..worker_count)
        .map(|worker_index| {
            multi
                .add_bar(100)
                .with_message(&format!("worker#{worker_index}: queued"))
                .with_format("{msg}  [{bar:18}]  {pct}")
        })
        .collect::<Vec<_>>();

    let shared_jobs = Arc::new(tokio::sync::Mutex::new(
        flattened_hierarchy.into_iter().enumerate().collect::<VecDeque<_>>(),
    ));
    let (result_sender, mut result_receiver) = tokio::sync::mpsc::unbounded_channel::<(
        usize,
        Result<PreparedHierarchyEntryResult, MediaPmError>,
    )>();

    let shared_paths = Arc::new(paths.clone());
    let shared_document = Arc::new(document.clone());
    let shared_lookup = Arc::new(lookup);
    let shared_materialization_methods = Arc::new(materialization_methods);
    let shared_playlist_media_index = Arc::new(playlist_media_index);
    let shared_media_entries_by_id = Arc::new(media_entries_by_id);
    let shared_lock_snapshot = Arc::new(lock.clone());

    let mut worker_handles = Vec::with_capacity(worker_count);
    for (worker_index, progress_bar) in operation_bars.iter().enumerate() {
        let jobs = Arc::clone(&shared_jobs);
        let sender = result_sender.clone();
        let worker_paths = Arc::clone(&shared_paths);
        let worker_document = Arc::clone(&shared_document);
        let worker_lookup = Arc::clone(&shared_lookup);
        let worker_materialization_methods = Arc::clone(&shared_materialization_methods);
        let worker_playlist_media_index = Arc::clone(&shared_playlist_media_index);
        let worker_media_entries_by_id = Arc::clone(&shared_media_entries_by_id);
        let worker_lock_snapshot = Arc::clone(&shared_lock_snapshot);
        let worker_bar = progress_bar.clone();

        worker_handles.push(tokio::spawn(async move {
            loop {
                let next_job = {
                    let mut queue = jobs.lock().await;
                    queue.pop_front()
                };

                let Some((job_index, flattened_entry)) = next_job else {
                    break;
                };

                worker_bar.set_position(0);
                worker_bar.set_message(&format!(
                    "worker#{worker_index}: {}",
                    hierarchy_entry_kind_label(flattened_entry.entry.kind)
                ));

                let prepared = prepare_hierarchy_entry(
                    worker_paths.as_ref(),
                    worker_document.as_ref(),
                    worker_lock_snapshot.as_ref(),
                    worker_lookup.as_ref(),
                    worker_materialization_methods.as_ref(),
                    worker_playlist_media_index.as_ref(),
                    worker_media_entries_by_id.as_ref(),
                    &flattened_entry,
                    job_index,
                    &worker_bar,
                )
                .await;

                if prepared.is_err() {
                    worker_bar.set_position(100);
                    worker_bar.set_message(&format!(
                        "worker#{worker_index}: failed {}",
                        hierarchy_entry_kind_label(flattened_entry.entry.kind)
                    ));
                } else if let Ok(ref prepared_entry) = prepared {
                    worker_bar.set_message(&format!(
                        "worker#{worker_index}: {}",
                        hierarchy_progress_filename_label(&prepared_entry.relative_path)
                    ));
                }

                let _ = sender.send((job_index, prepared));
            }

            worker_bar.set_position(100);
            worker_bar.set_message(&format!("worker#{worker_index}: done"));
        }));
    }
    drop(result_sender);

    let mut prepared_results = (0..total_entries).map(|_| None).collect::<Vec<_>>();
    let mut first_prepare_error: Option<MediaPmError> = None;
    let mut completed_entries = 0usize;

    while completed_entries < total_entries {
        let Some((entry_index, prepared_result)) = result_receiver.recv().await else {
            break;
        };

        hierarchy_progress.advance(1);
        completed_entries += 1;
        hierarchy_progress.set_message(&format!(
            "syncing hierarchy ({worker_count} concurrent workers, prepared {completed_entries}/{total_entries})"
        ));

        match prepared_result {
            Ok(prepared) => {
                prepared_results[entry_index] = Some(prepared);
            }
            Err(error) => {
                if first_prepare_error.is_none() {
                    first_prepare_error = Some(error);
                }
            }
        }
    }

    for handle in worker_handles {
        handle
            .await
            .map_err(|e| MediaPmError::Workflow(format!("hierarchy worker task panicked: {e}")))?;
    }

    if let Some(error) = first_prepare_error {
        return Err(error);
    }

    for prepared in prepared_results {
        let prepared = prepared.ok_or_else(|| {
            MediaPmError::Workflow(
                "hierarchy worker channel closed before all entries were prepared".to_string(),
            )
        })?;

        report.notices.extend(prepared.notices);
        desired_paths.extend(prepared.skipped_paths.iter().cloned());
        desired_paths.extend(prepared.managed_file_hashes.keys().cloned());

        if prepared.skipped_entry {
            for managed_path in &prepared.refreshed_lock_paths {
                if let Some(record) = lock.managed_files.get_mut(managed_path) {
                    record.last_synced_unix_millis = unix_epoch_millis();
                }
            }
            report.skipped_paths += 1;
            continue;
        }

        let managed_path = prepared.final_path.clone();
        tokio::task::spawn_blocking(move || ensure_managed_path_readonly(&managed_path))
            .await
            .map_err(|e| {
            MediaPmError::Workflow(format!("readonly enforcement task panicked: {e}"))
        })??;

        let managed_media_id = prepared.managed_media_id.ok_or_else(|| {
            MediaPmError::Workflow(format!(
                "missing managed media id for prepared hierarchy path '{}'",
                prepared.relative_path
            ))
        })?;

        for (managed_file_path, managed_hash) in prepared.managed_file_hashes {
            let managed_variant = prepared
                .managed_file_variants
                .get(&managed_file_path)
                .cloned()
                .ok_or_else(|| {
                MediaPmError::Workflow(format!(
                    "missing managed variant metadata for materialized path '{managed_file_path}'"
                ))
            })?;

            lock.managed_files.insert(
                managed_file_path,
                ManagedFileRecord {
                    media_id: managed_media_id.clone(),
                    variant: managed_variant,
                    hash: managed_hash.to_string(),
                    last_synced_unix_millis: unix_epoch_millis(),
                },
            );
        }

        report.materialized_paths += 1;
    }

    let stale_paths = lock
        .managed_files
        .keys()
        .filter(|path| !desired_paths.contains(*path))
        .cloned()
        .collect::<Vec<_>>();

    for stale in &stale_paths {
        if stale.ends_with('/') || stale.ends_with('\\') {
            // Legacy lock rows from historical directory-level tracking should
            // not remove whole directories once file-level tracking is active.
            lock.managed_files.remove(stale);
            continue;
        }

        let final_path = paths.hierarchy_root_dir.join(stale);
        if tokio::fs::try_exists(&final_path).await.unwrap_or(false) {
            let owned = final_path.clone();
            tokio::task::spawn_blocking(move || remove_path(&owned)).await.map_err(|e| {
                MediaPmError::Workflow(format!("remove stale path task panicked: {e}"))
            })??;
            report.removed_paths += 1;
        }
        lock.managed_files.remove(stale);
    }

    // Remove empty parent directories after stale path cleanup.
    // Walk up from each removed path's parent, removing directories that
    // contain no files (recursively), stopping at the hierarchy root.
    let mut checked_parents = BTreeSet::new();
    for stale in &stale_paths {
        if stale.ends_with('/') || stale.ends_with('\\') {
            continue;
        }
        let mut parent = paths.hierarchy_root_dir.join(stale);
        if !parent.pop() {
            continue;
        }
        loop {
            if !checked_parents.insert(parent.clone()) {
                break;
            }
            if parent == paths.hierarchy_root_dir {
                break;
            }
            let is_empty = match tokio::fs::read_dir(&parent).await {
                Ok(mut entries) => entries.next_entry().await.unwrap_or(None).is_none(),
                Err(_) => false,
            };
            if !is_empty {
                break;
            }
            let owned = parent.clone();
            tokio::task::spawn_blocking(move || remove_path(&owned)).await.map_err(|e| {
                MediaPmError::Workflow(format!("remove empty parent dir task panicked: {e}"))
            })??;
            report.removed_empty_dirs += 1;
            if !parent.pop() {
                break;
            }
        }
    }
    hierarchy_progress.set_message("done");
    tokio::time::sleep(Duration::from_millis(75)).await;

    lock.last_materialized_state_hash = current_materialized_hash;
    Ok(report)
}
