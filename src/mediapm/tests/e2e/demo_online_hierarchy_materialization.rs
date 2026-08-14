//! Hermetic `mediapm_demo_online` hierarchy materialization using pre-seeded variant hashes.
//!
//! Asserts the materialized tree matches `demo_hierarchy_golden.json` (`online`). Expected
//! layout is documented on the `mediapm_demo_online` example module.

use std::collections::BTreeMap;

use crate::common::{make_zip, service_at};
use bytes::Bytes;
use mediapm::{
    HierarchyFolderRenameRule, HierarchyNode, HierarchyNodeKind, HierarchyPath, MediaMetadataValue,
    MediaPmDocument, MediaSourceSpec, PlaylistFormat, PlaylistItemRef, SanitizeNamesConfig,
    demo_hierarchy_spec::{
        DEMO_LIBRARY_ROOT, DEMO_METADATA_ARTIST, DEMO_METADATA_TITLE, ONLINE_DEMO_MEDIA_ID,
        ONLINE_DEMO_YT_DLP_VIDEO_ID, assert_tree_under, load_demo_hierarchy_golden_document,
        online_demo_yt_dlp_provider_title, yt_dlp_sandbox_artifact_filename,
    },
    save_mediapm_document,
};
use mediapm_cas::CasApi;

const MKV_HEADER: &[u8] = &[0x1a, 0x45, 0xdf, 0xa3, 0x00, 0x00, 0x00, 0x00];

const ONLINE_HIERARCHY_MEDIA_ROOT_TEMPLATE: &str =
    "${media.metadata.artist} - ${media.metadata.title} [${media.id}]";

const ONLINE_UNTAGGED_MEDIA_FILE_NAME: &str = "${media.metadata.artist} - ${media.metadata.title} [${media.id}].untagged${media.metadata.video_ext}";

const ONLINE_TAGGED_MEDIA_FILE_NAME: &str =
    "${media.metadata.artist} - ${media.metadata.title} [${media.id}]${media.metadata.video_ext}";

const ONLINE_EN_VTT_FILE_NAME: &str =
    "${media.metadata.artist} - ${media.metadata.title} [${media.id}].en.vtt";

const ONLINE_DESCRIPTION_FILE_NAME: &str =
    "${media.metadata.artist} - ${media.metadata.title} [${media.id}].description.txt";

const ONLINE_INFOJSON_FILE_NAME: &str =
    "${media.metadata.artist} - ${media.metadata.title} [${media.id}].info.json";

const ONLINE_THUMBNAIL_RENAME_REPLACEMENT: &str =
    "${media.metadata.artist} - ${media.metadata.title} [${media.id}].thumbnail.$1";

const ONLINE_LINK_RENAME_REPLACEMENT: &str =
    "${media.metadata.artist} - ${media.metadata.title} [${media.id}].link.$1";

fn demo_metadata_literals() -> BTreeMap<String, MediaMetadataValue> {
    BTreeMap::from([
        ("artist".to_string(), MediaMetadataValue::Literal(DEMO_METADATA_ARTIST.to_string())),
        ("title".to_string(), MediaMetadataValue::Literal(DEMO_METADATA_TITLE.to_string())),
        ("video_ext".to_string(), MediaMetadataValue::Literal(".mkv".to_string())),
        ("id".to_string(), MediaMetadataValue::Literal("dQw4w9WgXcQ".to_string())),
    ])
}

/// Builds a `HierarchyNode` with the online-demo defaults: the demo media id,
/// M3u8 format, inherited sanitization, and no explicit id, rename rules, or
/// children. An empty `path` produces the default (payload-derived) path.
fn node(
    path: &str,
    kind: HierarchyNodeKind,
    variant: Option<&str>,
    variants: Vec<&str>,
    rename_files: Vec<HierarchyFolderRenameRule>,
    children: Vec<HierarchyNode>,
) -> HierarchyNode {
    HierarchyNode {
        path: if path.is_empty() { HierarchyPath::default() } else { HierarchyPath::from(path) },
        kind,
        id: None,
        media_id: Some(ONLINE_DEMO_MEDIA_ID.to_string()),
        variant: variant.map(str::to_string),
        variants: variants.into_iter().map(str::to_string).collect(),
        rename_files,
        format: PlaylistFormat::M3u8,
        ids: Vec::new(),
        sanitize_names: SanitizeNamesConfig::Inherit,
        children,
    }
}

fn rename_rule(pattern: &str, replacement: &str) -> HierarchyFolderRenameRule {
    HierarchyFolderRenameRule { pattern: pattern.to_string(), replacement: replacement.to_string() }
}

#[expect(
    clippy::too_many_lines,
    reason = "hermetic hierarchy seed mirrors the full online demo tree"
)]
fn build_online_hierarchy() -> Vec<HierarchyNode> {
    let media_root_children = vec![
        node(
            "sidecars",
            HierarchyNodeKind::Folder,
            None,
            Vec::new(),
            Vec::new(),
            vec![
                node(
                    "subtitles",
                    HierarchyNodeKind::MediaFolder,
                    None,
                    vec!["subtitles"],
                    Vec::new(),
                    Vec::new(),
                ),
                node(
                    "subtitles.en.vtt",
                    HierarchyNodeKind::Media,
                    Some("subtitles_en"),
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                ),
                node(
                    "thumbnails",
                    HierarchyNodeKind::MediaFolder,
                    None,
                    vec!["thumbnails"],
                    Vec::new(),
                    Vec::new(),
                ),
                node(
                    "links",
                    HierarchyNodeKind::MediaFolder,
                    None,
                    vec!["links"],
                    Vec::new(),
                    Vec::new(),
                ),
                node(
                    "archive.txt",
                    HierarchyNodeKind::Media,
                    Some("archive"),
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                ),
                node(
                    "description.txt",
                    HierarchyNodeKind::Media,
                    Some("description"),
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                ),
                node(
                    "info.json",
                    HierarchyNodeKind::Media,
                    Some("infojson"),
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                ),
            ],
        ),
        node(
            ONLINE_UNTAGGED_MEDIA_FILE_NAME,
            HierarchyNodeKind::Media,
            Some("video_untagged"),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        ),
        HierarchyNode {
            id: Some(ONLINE_DEMO_MEDIA_ID.to_string()),
            ..node(
                ONLINE_TAGGED_MEDIA_FILE_NAME,
                HierarchyNodeKind::Media,
                Some("video"),
                Vec::new(),
                Vec::new(),
                Vec::new(),
            )
        },
        node(
            ONLINE_EN_VTT_FILE_NAME,
            HierarchyNodeKind::Media,
            Some("subtitles_en"),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        ),
        node(
            ONLINE_DESCRIPTION_FILE_NAME,
            HierarchyNodeKind::Media,
            Some("description"),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        ),
        node(
            ONLINE_INFOJSON_FILE_NAME,
            HierarchyNodeKind::Media,
            Some("infojson"),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        ),
        node(
            "",
            HierarchyNodeKind::MediaFolder,
            None,
            vec!["thumbnails"],
            vec![rename_rule("^.*\\.([^.]+)$", ONLINE_THUMBNAIL_RENAME_REPLACEMENT)],
            Vec::new(),
        ),
        node(
            "",
            HierarchyNodeKind::MediaFolder,
            None,
            vec!["links"],
            vec![rename_rule("^.*\\.([^.]+)$", ONLINE_LINK_RENAME_REPLACEMENT)],
            Vec::new(),
        ),
        HierarchyNode {
            id: Some(format!("{ONLINE_DEMO_MEDIA_ID}.thumbnails.folder")),
            ..node(
                "",
                HierarchyNodeKind::MediaFolder,
                None,
                vec!["thumbnails"],
                vec![rename_rule(r"^.*\.([^.]*)$", "folder.$1")],
                Vec::new(),
            )
        },
    ];

    vec![
        HierarchyNode {
            media_id: None,
            ..node(
                DEMO_LIBRARY_ROOT,
                HierarchyNodeKind::Folder,
                None,
                Vec::new(),
                Vec::new(),
                vec![HierarchyNode {
                    id: Some(format!("{ONLINE_DEMO_MEDIA_ID}.media_folder")),
                    media_id: None,
                    ..node(
                        ONLINE_HIERARCHY_MEDIA_ROOT_TEMPLATE,
                        HierarchyNodeKind::Folder,
                        None,
                        Vec::new(),
                        Vec::new(),
                        media_root_children,
                    )
                }],
            )
        },
        HierarchyNode {
            media_id: None,
            ..node(
                "playlists",
                HierarchyNodeKind::Folder,
                None,
                Vec::new(),
                Vec::new(),
                vec![HierarchyNode {
                    media_id: None,
                    ids: vec![
                        PlaylistItemRef::Object {
                            id: ONLINE_DEMO_MEDIA_ID.to_string(),
                            path: Some("relative".to_string()),
                        },
                        PlaylistItemRef::Object {
                            id: ONLINE_DEMO_MEDIA_ID.to_string(),
                            path: Some("absolute".to_string()),
                        },
                    ],
                    ..node(
                        "rickroll.m3u8",
                        HierarchyNodeKind::Playlist,
                        None,
                        Vec::new(),
                        Vec::new(),
                        Vec::new(),
                    )
                }],
            )
        },
    ]
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "golden-tree assert walks every projection family in one readable pass"
)]
async fn demo_online_hierarchy_materialization_matches_golden_tree()
-> Result<(), mediapm::MediaPmError> {
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let mut service = service_at(root.path(), Some("media")).await?;
    let cas = service.conductor().cas().clone();

    let video_hash = cas
        .put(Bytes::from_static(MKV_HEADER))
        .await
        .map_err(|e| mediapm::MediaPmError::Workflow(format!("seed video: {e}")))?;
    let subtitles_folder_hash = cas
        .put(Bytes::from(make_zip(&[("downloads/subtitle__mediapm__.en.vtt", b"WEBVTT\n")])))
        .await
        .map_err(|e| mediapm::MediaPmError::Workflow(format!("seed subtitles folder: {e}")))?;
    let subtitle_file_hash = cas
        .put(Bytes::from_static(b"WEBVTT\n"))
        .await
        .map_err(|e| mediapm::MediaPmError::Workflow(format!("seed subtitle file: {e}")))?;
    let description_hash = cas
        .put(Bytes::from_static(b"description fixture"))
        .await
        .map_err(|e| mediapm::MediaPmError::Workflow(format!("seed description: {e}")))?;
    let infojson_hash = cas
        .put(Bytes::from_static(br#"{"title":"Never Gonna Give You Up"}"#))
        .await
        .map_err(|e| mediapm::MediaPmError::Workflow(format!("seed infojson: {e}")))?;
    let archive_hash = cas
        .put(Bytes::from_static(b"archive fixture"))
        .await
        .map_err(|e| mediapm::MediaPmError::Workflow(format!("seed archive: {e}")))?;
    let thumbnail_zip = make_zip(&[
        ("downloads/poster__mediapm__.jpg", b"jpg-bytes"),
        ("downloads/wide__mediapm__.webp", b"webp-bytes"),
    ]);
    let thumbnail_hash = cas
        .put(Bytes::from(thumbnail_zip))
        .await
        .map_err(|e| mediapm::MediaPmError::Workflow(format!("seed thumbnails: {e}")))?;
    // yt-dlp-format sandbox ZIP members; materializer strips `__mediapm__` to yt-dlp public names.
    let provider_title = online_demo_yt_dlp_provider_title();
    let url_member = format!(
        "downloads/{}",
        yt_dlp_sandbox_artifact_filename(&provider_title, ONLINE_DEMO_YT_DLP_VIDEO_ID, "url")
    );
    let webloc_member = format!(
        "downloads/{}",
        yt_dlp_sandbox_artifact_filename(&provider_title, ONLINE_DEMO_YT_DLP_VIDEO_ID, "webloc")
    );
    let desktop_member = format!(
        "downloads/{}",
        yt_dlp_sandbox_artifact_filename(&provider_title, ONLINE_DEMO_YT_DLP_VIDEO_ID, "desktop")
    );
    let links_zip = make_zip(&[
        (url_member.as_str(), b"https://example.com/watch"),
        (webloc_member.as_str(), b"webloc-bytes"),
        (desktop_member.as_str(), b"desktop-bytes"),
    ]);
    let links_hash = cas
        .put(Bytes::from(links_zip))
        .await
        .map_err(|e| mediapm::MediaPmError::Workflow(format!("seed links: {e}")))?;

    let document = MediaPmDocument {
        media: BTreeMap::from([(
            ONLINE_DEMO_MEDIA_ID.to_string(),
            MediaSourceSpec {
                description: String::new(),
                title: DEMO_METADATA_TITLE.to_string(),
                artist: DEMO_METADATA_ARTIST.to_string(),
                metadata: demo_metadata_literals(),
                variant_hashes: BTreeMap::from([
                    ("video_untagged".to_string(), video_hash.to_string()),
                    ("video".to_string(), video_hash.to_string()),
                    ("subtitles_en".to_string(), subtitle_file_hash.to_string()),
                    ("description".to_string(), description_hash.to_string()),
                    ("infojson".to_string(), infojson_hash.to_string()),
                    ("archive".to_string(), archive_hash.to_string()),
                    ("subtitles".to_string(), subtitles_folder_hash.to_string()),
                    ("thumbnails".to_string(), thumbnail_hash.to_string()),
                    ("links".to_string(), links_hash.to_string()),
                ]),
                steps: Vec::new(),
            },
        )]),
        hierarchy: build_online_hierarchy(),
        ..MediaPmDocument::default()
    };

    save_mediapm_document(&service.paths().mediapm_ncl, &document)?;
    let reloaded = mediapm::load_mediapm_document(&service.paths().mediapm_ncl)?;
    let reloaded_source = reloaded.media.get(ONLINE_DEMO_MEDIA_ID).expect("reloaded media source");
    assert!(
        reloaded_source.variant_hashes.contains_key("subtitles"),
        "expected subtitles variant hash to round-trip through mediapm.ncl"
    );

    for hash in [
        video_hash,
        subtitles_folder_hash,
        subtitle_file_hash,
        description_hash,
        infojson_hash,
        archive_hash,
        thumbnail_hash,
        links_hash,
    ] {
        cas.ensure_blob_materialized(hash).await.map_err(|source| {
            mediapm::MediaPmError::Workflow(format!(
                "ensure blob materialized for '{hash}': {source}"
            ))
        })?;
    }

    let summary = service.sync_library(false).await?;
    assert!(summary.materialized_paths > 0, "expected hierarchy materialization to write files");

    let hierarchy_root = service.resolve_effective_paths()?.hierarchy_root_dir;
    let golden = load_demo_hierarchy_golden_document();
    if let Err(error) = assert_tree_under(&hierarchy_root, &golden.online) {
        panic!(
            "online hierarchy should match golden tree: {error}; sync warnings: {:?}",
            summary.warnings
        );
    }

    Ok(())
}
