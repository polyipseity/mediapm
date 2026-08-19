//! Hermetic `mediapm_demo_online` hierarchy materialization using pre-seeded variant hashes.
//!
//! Asserts the materialized tree matches `demo_hierarchy_golden.json` (`online`). Expected
//! layout is documented on the `mediapm_demo_online` example module.

use std::collections::BTreeMap;

use crate::common::{make_zip, seed_cas, service_at};
use bytes::Bytes;
use mediapm::{
    HierarchyFolderRenameRule, HierarchyNode, HierarchyNodeKind, HierarchyPath, MediaMetadataValue,
    MediaPmDocument, MediaSourceSpec, PlaylistFormat, PlaylistItemRef, SanitizeNamesConfig,
    demo_hierarchy_spec::{
        DEMO_LIBRARY_ROOT, DEMO_METADATA_ARTIST, DEMO_METADATA_TITLE, ONLINE_DEMO_MEDIA_ID,
        ONLINE_DEMO_PLAYLIST, ONLINE_DEMO_YT_DLP_VIDEO_ID, assert_tree_under,
        load_demo_hierarchy_golden_document, online_demo_root_link_filename,
        online_demo_sidecar_link_filename, online_demo_yt_dlp_provider_title,
        yt_dlp_sandbox_artifact_filename,
    },
    save_mediapm_document,
};

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

fn assert_file_bytes(path: &std::path::Path, expected: &[u8], label: &str) -> Result<(), String> {
    let actual =
        std::fs::read(path).map_err(|e| format!("{label}: read '{}': {e}", path.display()))?;
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "{label}: content mismatch at '{}': expected {} bytes, got {} bytes",
            path.display(),
            expected.len(),
            actual.len(),
        ))
    }
}

fn assert_materialized_content_matches_seeds(
    hierarchy_root: &std::path::Path,
) -> Result<(), String> {
    let media_folder =
        hierarchy_root.join(mediapm::demo_hierarchy_spec::online_demo_media_folder_relative());
    let provider_title = online_demo_yt_dlp_provider_title();
    // Hierarchy templates use `${media.id}` which resolves to the full media ID
    // (e.g., `youtube.dQw4w9WgXcQ`), not the raw yt-dlp video ID.
    let media_id = ONLINE_DEMO_MEDIA_ID;

    // Tagged video file
    let tagged_name = format!("{provider_title} [{media_id}].mkv");
    assert_file_bytes(&media_folder.join(&tagged_name), MKV_HEADER, "[video tagged]")?;

    // Untagged video file
    let untagged_name = format!("{provider_title} [{media_id}].untagged.mkv");
    assert_file_bytes(&media_folder.join(&untagged_name), MKV_HEADER, "[video untagged]")?;

    // Subtitle (root-level .en.vtt from primary variant)
    let vtt_name = format!("{provider_title} [{media_id}].en.vtt");
    assert_file_bytes(&media_folder.join(&vtt_name), b"WEBVTT\n", "[subtitle root]")?;
    // Subtitle (sidecar)
    assert_file_bytes(
        &media_folder.join("sidecars/subtitles.en.vtt"),
        b"WEBVTT\n",
        "[subtitle sidecar]",
    )?;

    // Description (root-level)
    let desc_name = format!("{provider_title} [{media_id}].description.txt");
    assert_file_bytes(
        &media_folder.join(&desc_name),
        b"description fixture",
        "[description root]",
    )?;
    // Description (sidecar)
    assert_file_bytes(
        &media_folder.join("sidecars/description.txt"),
        b"description fixture",
        "[description sidecar]",
    )?;

    // Info.json (root-level)
    let info_name = format!("{provider_title} [{media_id}].info.json");
    assert_file_bytes(
        &media_folder.join(&info_name),
        br#"{"title":"Never Gonna Give You Up"}"#,
        "[infojson root]",
    )?;
    // Info.json (sidecar)
    assert_file_bytes(
        &media_folder.join("sidecars/info.json"),
        br#"{"title":"Never Gonna Give You Up"}"#,
        "[infojson sidecar]",
    )?;

    // Archive (sidecar only)
    assert_file_bytes(
        &media_folder.join("sidecars/archive.txt"),
        b"archive fixture",
        "[archive sidecar]",
    )?;

    // Thumbnails (sidecar)
    assert_file_bytes(
        &media_folder.join("sidecars/thumbnails/poster.jpg"),
        b"jpg-bytes",
        "[thumbnail poster]",
    )?;
    assert_file_bytes(
        &media_folder.join("sidecars/thumbnails/wide.webp"),
        b"webp-bytes",
        "[thumbnail wide]",
    )?;

    // Thumbnails (root projection)
    let thumb_jpg_name = format!("{provider_title} [{media_id}].thumbnail.jpg");
    assert_file_bytes(&media_folder.join(&thumb_jpg_name), b"jpg-bytes", "[thumbnail root jpg]")?;
    let thumb_webp_name = format!("{provider_title} [{media_id}].thumbnail.webp");
    assert_file_bytes(
        &media_folder.join(&thumb_webp_name),
        b"webp-bytes",
        "[thumbnail root webp]",
    )?;

    // folder.jpg / folder.webp
    assert_file_bytes(&media_folder.join("folder.jpg"), b"jpg-bytes", "[folder.jpg]")?;
    assert_file_bytes(&media_folder.join("folder.webp"), b"webp-bytes", "[folder.webp]")?;

    // Links (sidecar)
    assert_file_bytes(
        &media_folder.join("sidecars/links").join(&online_demo_sidecar_link_filename("url")),
        b"https://example.com/watch",
        "[link sidecar url]",
    )?;
    assert_file_bytes(
        &media_folder.join("sidecars/links").join(&online_demo_sidecar_link_filename("webloc")),
        b"webloc-bytes",
        "[link sidecar webloc]",
    )?;
    assert_file_bytes(
        &media_folder.join("sidecars/links").join(&online_demo_sidecar_link_filename("desktop")),
        b"desktop-bytes",
        "[link sidecar desktop]",
    )?;

    // Links (root projection)
    assert_file_bytes(
        &media_folder.join(&online_demo_root_link_filename("url")),
        b"https://example.com/watch",
        "[link root url]",
    )?;
    assert_file_bytes(
        &media_folder.join(&online_demo_root_link_filename("webloc")),
        b"webloc-bytes",
        "[link root webloc]",
    )?;
    assert_file_bytes(
        &media_folder.join(&online_demo_root_link_filename("desktop")),
        b"desktop-bytes",
        "[link root desktop]",
    )?;

    // Playlist (structural: starts with #EXTM3U; playlist entries are resolved
    // from the media index which uses hierarchy_id as key, so the playlist
    // contains only the M3U8 header when no hierarchy_id matches the playlist path)
    let playlist_path = hierarchy_root.join("playlists").join(ONLINE_DEMO_PLAYLIST);
    let playlist_bytes = std::fs::read(&playlist_path)
        .map_err(|e| format!("[playlist] read '{}': {e}", playlist_path.display()))?;
    if !playlist_bytes.starts_with(b"#EXTM3U") {
        return Err(format!(
            "[playlist] '{}' should start with '#EXTM3U'",
            playlist_path.display()
        ));
    }

    Ok(())
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

    let video_hash = seed_cas(&service, Bytes::from_static(MKV_HEADER), "video").await?;
    let subtitles_folder_hash = seed_cas(
        &service,
        Bytes::from(make_zip(&[("downloads/subtitle__mediapm__.en.vtt", b"WEBVTT\n")])),
        "subtitles folder",
    )
    .await?;
    let subtitle_file_hash =
        seed_cas(&service, Bytes::from_static(b"WEBVTT\n"), "subtitle file").await?;
    let description_hash =
        seed_cas(&service, Bytes::from_static(b"description fixture"), "description").await?;
    let infojson_hash = seed_cas(
        &service,
        Bytes::from_static(br#"{"title":"Never Gonna Give You Up"}"#),
        "infojson",
    )
    .await?;
    let archive_hash =
        seed_cas(&service, Bytes::from_static(b"archive fixture"), "archive").await?;
    let thumbnail_zip = make_zip(&[
        ("downloads/poster__mediapm__.jpg", b"jpg-bytes"),
        ("downloads/wide__mediapm__.webp", b"webp-bytes"),
    ]);
    let thumbnail_hash = seed_cas(&service, Bytes::from(thumbnail_zip), "thumbnails").await?;
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
    let links_hash = seed_cas(&service, Bytes::from(links_zip), "links").await?;

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
        service.conductor().cas().ensure_blob_materialized(hash).await.map_err(|source| {
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
    if let Err(error) = assert_materialized_content_matches_seeds(&hierarchy_root) {
        panic!("materialized content should match CAS seeds: {error}");
    }

    Ok(())
}
