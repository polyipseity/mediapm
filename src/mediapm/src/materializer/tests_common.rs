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

/// A one-folder-entry document whose single variant resolves from
/// `variant_hash`, so a `sync_hierarchy` over it reaches the folder arm.
///
/// Kept apart from [`resolvable_media_document`] because the two arms write
/// differently. A media entry writes one file at its own hierarchy path; a
/// folder writes one file per variant under its path, so a test that occupies
/// one name inside the folder has to say which arm it is aiming at.
pub(super) fn folder_only_document(variant_hash: &str) -> MediaPmDocument {
    let mut document = folder_and_playlist_document(variant_hash);
    document.hierarchy.retain(|node| matches!(node.kind, HierarchyNodeKind::MediaFolder));
    document
}

/// A one-folder-entry document whose media source declares no output
/// variants and carries no variant hashes.
///
/// Both places a variant name can come from are emptied, so the folder's
/// selected list is empty before the loop rather than blocked inside it. No
/// name reaches the loop, so nothing in it can report having written. The
/// hash the folder fixture starts from is cleared on the way out, so the
/// document never carries a variant the materializer could resolve.
pub(super) fn folder_without_variants_document() -> MediaPmDocument {
    let mut document = folder_only_document("");
    for source in document.media.values_mut() {
        source.variant_hashes.clear();
        for step in &mut source.steps {
            step.output_variants.clear();
        }
    }
    document
}

/// A one-folder-entry document with two variants, [`BLOCKED_VARIANT`] and
/// the `default` variant [`folder_only_document`] already carries, of which
/// only `default` resolves from `good_hash`.
///
/// Both carry the same hash, so what refuses the first is the directory
/// a test puts at its path rather than its content, which is what lets one
/// test hold the resolution path constant and vary the conflict.
///
/// Two variants are what makes this fixture answer a question a
/// single-variant folder cannot. A folder is one hierarchy path however many
/// variants it holds, so an entry that wrote one file and refused another
/// belongs in exactly one counter, and the run says which.
pub(super) fn folder_with_blocked_and_good_variants_document(good_hash: &str) -> MediaPmDocument {
    let mut document = folder_only_document(good_hash);
    for source in document.media.values_mut() {
        source.variant_hashes.insert(BLOCKED_VARIANT.to_string(), good_hash.to_string());
        for step in &mut source.steps {
            step.output_variants.insert(
                BLOCKED_VARIANT.to_string(),
                OutputVariantValue::Generic(GenericOutputVariantConfig {
                    kind: "primary".to_string(),
                    ..Default::default()
                }),
            );
        }
    }
    document
}

/// A one-media-entry document and the playlist beside it, with the album node
/// of [`folder_and_playlist_document`] dropped, so the run reaches the media
/// arm and the playlist arm and nothing else.
///
/// The referenced media entry has to stay. A playlist resolves its references
/// through the media index, which holds `Media` entries and nothing else, so a
/// document holding a playlist alone could not name a path for it. That is why
/// every counter a playlist test reads also carries the media entry, and why
/// the tests say which entry each count is about.
pub(super) fn playlist_document(variant_hash: &str) -> MediaPmDocument {
    let mut document = folder_and_playlist_document(variant_hash);
    document.hierarchy.retain(|node| !matches!(node.kind, HierarchyNodeKind::MediaFolder));
    document
}

/// Variant name a test occupies with a directory so the folder arm refuses to
/// write it.
pub(super) const BLOCKED_VARIANT: &str = "blocked";

/// Hierarchy path of the folder entries these fixtures declare, which is the
/// directory the folder arm writes its members into.
pub(super) const FOLDER_PATH: &str = "album";

/// Variant name those folder fixtures give their resolvable variant, and so
/// the file name a single-variant folder writes inside [`FOLDER_PATH`].
pub(super) const FOLDER_VARIANT: &str = "default";

/// Hierarchy path of the playlist [`playlist_document`] declares.
pub(super) const PLAYLIST_PATH: &str = "playlists/rickroll.m3u8";

/// Byte the M3U8 body [`playlist_document`] renders starts with, which is how
/// a reader tells a playlist body from whatever else is on disk.
const PLAYLIST_FIRST_BYTE: u8 = b'#';

/// A workspace holding an entry a first run already wrote, kept together so a
/// re-sync can be asked for without repeating the setup each time.
///
/// One shape for both arms that resolve their content from the document rather
/// than from a single CAS object, because what a caller does with the result is
/// the same for either: disturb `output`, re-sync, read the counters.
pub(super) struct WrittenEntry {
    /// Workspace owning the tree on disk. Dropping it removes the tree, so it
    /// is held for as long as any test reads a path inside it.
    _workspace: tempfile::TempDir,
    /// Layout of that workspace, which is what `sync_hierarchy` takes.
    paths: MediaPmPaths,
    /// The file the first run wrote, which is a folder directory for a folder
    /// entry and the playlist body for a playlist entry.
    pub(super) output: PathBuf,
    /// Document every run reads, and the one whose entries the first run wrote.
    document: MediaPmDocument,
    /// Records the first run left, which a re-sync has to match.
    state: MediaPmState,
    /// Store every run reads from.
    cas: FileSystemCas,
}

impl WrittenEntry {
    /// Runs `sync_hierarchy` again over the same document and the records the
    /// first run left.
    pub(super) async fn resync(
        &mut self,
        progress_group: Option<Arc<dyn ProgressScreenApi + Send + Sync>>,
        overall_bar: Option<Arc<dyn ProgressBarApi>>,
    ) -> MaterializeReport {
        run_sync(
            &self.paths,
            &self.document,
            &mut self.state,
            &self.cas,
            progress_group,
            overall_bar,
        )
        .await
    }
}

/// A folder the first run has written from `members`, ready to be disturbed
/// and re-synced.
///
/// The payload is a stored ZIP rather than plain bytes because a plain variant
/// writes one file per variant, and a folder with one member is a case a
/// single-file check would already cover. Two members are what makes "every
/// member verifies" a question with more than one answer.
///
/// Asserts that both members landed, so a caller whose archive did not produce
/// two files finds out here rather than in an assertion about drift.
pub(super) async fn write_folder_over_archive(members: &[(&str, &[u8])]) -> WrittenEntry {
    let workspace = mediapm_utils::temp::artifact_dir().unwrap();
    let paths = MediaPmPaths::from_root(workspace.path());
    let cas = open_hierarchy_cas(&paths).await;
    let archive = cas.put(bytes::Bytes::from(zip_payload(members))).await.unwrap();
    let document = folder_only_document(&archive.to_string());
    let mut state = MediaPmState::default();

    let first = run_sync(&paths, &document, &mut state, &cas, None, None).await;
    assert_eq!(
        (first.materialized_paths, first.skipped_paths, first.missing_paths),
        (1, 0, 0),
        "the first run has nothing recorded for the folder, so it must write it: {first:?}"
    );

    let folder = paths.hierarchy_root_dir.join(FOLDER_PATH);
    for (name, payload) in members {
        assert_eq!(
            std::fs::read(folder.join(name)).unwrap(),
            *payload,
            "the archive member '{name}' has to be on disk, or the case its caller is about to \
             stage was never set up"
        );
    }

    WrittenEntry { _workspace: workspace, paths, output: folder, document, state, cas }
}

/// A playlist the first run has written, ready to be disturbed and re-synced.
///
/// Asserts the body landed and reads as a playlist, so a caller whose document
/// failed to reference anything finds out here rather than in an assertion
/// about drift.
pub(super) async fn write_playlist() -> WrittenEntry {
    let workspace = mediapm_utils::temp::artifact_dir().unwrap();
    let paths = MediaPmPaths::from_root(workspace.path());
    let cas = open_hierarchy_cas(&paths).await;
    let payload = b"playlist media payload";
    let hash = cas.put(bytes::Bytes::from_static(payload)).await.unwrap();
    let document = playlist_document(&hash.to_string());
    let mut state = MediaPmState::default();

    let first = run_sync(&paths, &document, &mut state, &cas, None, None).await;
    assert_eq!(
        (first.materialized_paths, first.skipped_paths, first.missing_paths),
        (2, 0, 0),
        "the first run has nothing recorded for either entry, so it must write both: {first:?}"
    );

    let playlist = paths.hierarchy_root_dir.join(PLAYLIST_PATH);
    let body = std::fs::read(&playlist).unwrap();
    assert_eq!(
        body.first().copied(),
        Some(PLAYLIST_FIRST_BYTE),
        "the first run has to have written a playlist body at {}, or the case about to be staged \
         was never set up: {body:?}",
        playlist.display()
    );

    WrittenEntry { _workspace: workspace, paths, output: playlist, document, state, cas }
}

/// Opens a CAS under the workspace runtime root for a `sync_hierarchy` call.
pub(super) async fn open_hierarchy_cas(paths: &MediaPmPaths) -> FileSystemCas {
    let cas_root = paths.runtime_root.join("store");
    tokio::fs::create_dir_all(&cas_root).await.unwrap();
    FileSystemCas::open(&cas_root).await.unwrap()
}

/// Runs `sync_hierarchy` over `document` with no progress output, or with the
/// tracker's own bars when one is supplied.
pub(super) async fn run_sync(
    paths: &MediaPmPaths,
    document: &MediaPmDocument,
    state: &mut MediaPmState,
    cas: &FileSystemCas,
    progress_group: Option<Arc<dyn ProgressScreenApi + Send + Sync>>,
    overall_bar: Option<Arc<dyn ProgressBarApi>>,
) -> MaterializeReport {
    sync_hierarchy(
        paths,
        document,
        state,
        cas,
        false,
        &ConductorState::new_empty(),
        &NickelDocument::default(),
        progress_group,
        overall_bar,
    )
    .await
    .expect("a run over one resolvable entry either writes it or skips it")
}

/// Clears the read-only bit the materializer sets on every managed output, so
/// a test can stand in for something outside mediapm writing into the library.
pub(super) fn clear_readonly(path: &Path) {
    let mut permissions = std::fs::metadata(path).unwrap().permissions();

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        let mode = permissions.mode();
        let writable_mode = mode | 0o200;
        if writable_mode != mode {
            permissions.set_mode(writable_mode);
        }
    }

    #[cfg(not(unix))]
    {
        #[expect(
            clippy::permissions_set_readonly_false,
            reason = "on non-Unix platforms the readonly flag blocks the external write this helper exists to stage"
        )]
        {
            permissions.set_readonly(false);
        }
    }

    std::fs::set_permissions(path, permissions).expect("the permissions are restored");
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
