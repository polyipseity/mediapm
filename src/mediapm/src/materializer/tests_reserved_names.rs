//! A variant name or a rename replacement carrying a reserved character is refused.

use crate::config::hierarchy_types::{
    HierarchyFolderRenameRule, HierarchyNodeKind, SanitizeNamesConfig,
};
use crate::config::{GenericOutputVariantConfig, OutputVariantValue};

use super::tests_common::{folder_and_playlist_document, open_hierarchy_cas, zip_payload};

use super::*;

/// Media-folder variant name carrying a reserved character, so the
/// `SanitizePolicy` rewrite half has something it would change.
const UNSAFE_VARIANT_NAME: &str = "raw<b";

/// The spelling `SanitizePolicy::Enabled` produces from
/// [`UNSAFE_VARIANT_NAME`]. The variant join uses the reject half, so
/// this path must stay absent; its presence is the silent rewrite.
const REWRITTEN_VARIANT_NAME: &str = "raw_b";

/// A `rename_files` replacement emitting a reserved character, applied to
/// the member `cover.jpg` so the produced leaf is [`REPLACED_LEAF`].
const UNSAFE_REPLACEMENT: &str = "raw<b";

/// The file name `UNSAFE_REPLACEMENT` produces on `cover.jpg`.
const REPLACED_LEAF: &str = "raw<b.jpg";

/// The spelling the configured replacement map would produce from
/// [`REPLACED_LEAF`]. Its presence means the map reached the rename chain.
const REWRITTEN_MEMBER_NAME: &str = "raw_b.jpg";

/// A media-folder variant name the sanitizer would rewrite is refused, and
/// neither the raw spelling nor the rewritten one reaches the library.
///
/// A variant name is a free-form key in the source's variant map, so it
/// reaches `target_path.join(...)` and then `tokio::fs::write` without
/// being a hierarchy component. The join parses it under
/// `SanitizePolicy::disabled()`, which is the reject half of the policy
/// rather than the rewrite half: the name is a key an upstream tool chose,
/// so committing `raw_b` would materialize a file the document never
/// spelled.
///
/// The assertion is the absence of both spellings on disk, not the error
/// alone. A refusal and a silent rewrite are told apart by the library
/// tree, because a rewritten name leaves a file where the refused one
/// leaves nothing.
#[tokio::test]
async fn a_media_folder_variant_name_the_sanitizer_would_rewrite_is_refused() {
    let root = mediapm_utils::temp::artifact_dir().unwrap();
    let paths = MediaPmPaths::from_root(root.path());
    let cas = open_hierarchy_cas(&paths).await;
    let hash = cas.put(bytes::Bytes::from_static(b"variant-name-guard-bytes")).await.unwrap();

    let mut document = folder_and_playlist_document(&hash.to_string());
    document.hierarchy.retain(|node| matches!(node.kind, HierarchyNodeKind::MediaFolder));
    let source = document.media.get_mut("src1").unwrap();
    source.steps[0].output_variants = BTreeMap::from([(
        UNSAFE_VARIANT_NAME.to_string(),
        OutputVariantValue::Generic(GenericOutputVariantConfig {
            kind: "primary".to_string(),
            ..Default::default()
        }),
    )]);

    let result = sync_hierarchy(
        &paths,
        &document,
        &mut MediaPmState::default(),
        &cas,
        true,
        &ConductorState::new_empty(),
        &NickelDocument::default(),
        None,
        None,
    )
    .await;

    let folder = root.path().join("album");
    let raw = folder.join(UNSAFE_VARIANT_NAME);
    let rewritten = folder.join(REWRITTEN_VARIANT_NAME);
    assert!(!raw.exists(), "the raw variant name was written to '{}'", raw.display());
    assert!(
        !rewritten.exists(),
        "the sanitizer rewrote '{UNSAFE_VARIANT_NAME}' to '{REWRITTEN_VARIANT_NAME}', \
             which is what this guard exists to prevent; '{}' exists",
        rewritten.display()
    );

    let error = result.err().unwrap_or_else(|| {
        panic!("a variant name carrying '{UNSAFE_VARIANT_NAME}' must not reach the join")
    });
    assert!(
        error.to_string().contains(UNSAFE_VARIANT_NAME),
        "the error must name the refused variant so the document key is identifiable; got: {error}"
    );
}

/// A `rename_files` replacement emitting a reserved character is refused
/// even when the entry configures `sanitize_names = Enabled`, and the
/// rewritten spelling never reaches the library.
///
/// `SanitizePolicy` has two halves: `Enabled` and `Custom` carry a
/// replacement map and rewrite reserved characters, `disabled` carries
/// none and rejects them. The ZIP rename chain uses the second half, so
/// the configured map has no effect on a replacement's output. That is a
/// decision rather than an omission, and `SanitizePolicy::disabled` gives
/// the reason: a member name belongs to whatever produced the archive, so
/// rewriting it would materialize a file nobody asked for.
///
/// Configuring `Enabled` on the entry is what makes this a guard. With the
/// entry left on its default the map is not in play anywhere, so the test
/// would pass against any policy. Here the document asks for the rewrite
/// and the run refuses, which fails the moment someone routes the
/// configured map into the rename chain.
///
/// The coverage-matrix row this replaces asked for the opposite: that
/// replacement strings are sanitized with the configured map. Asserting
/// that would have pinned a rewrite that does not happen. What is pinned
/// instead is the refusal, and the absence of both spellings on disk.
#[tokio::test]
async fn a_rename_rule_output_is_refused_even_when_sanitize_names_is_enabled() {
    let root = mediapm_utils::temp::artifact_dir().unwrap();
    let paths = MediaPmPaths::from_root(root.path());
    let cas = open_hierarchy_cas(&paths).await;
    let members = [("cover.jpg", &b"rename-rule-bytes"[..])];
    let hash = cas
        .put(bytes::Bytes::from(zip_payload(&members)))
        .await
        .expect("the archive enters the store");

    let mut document = folder_and_playlist_document(&hash.to_string());
    document.hierarchy.retain(|node| matches!(node.kind, HierarchyNodeKind::MediaFolder));
    let folder = document.hierarchy.first_mut().expect("the folder node is retained");
    folder.sanitize_names = Some(SanitizeNamesConfig::Enabled);
    folder.rename_files = vec![HierarchyFolderRenameRule {
        pattern: "^cover".to_string(),
        replacement: UNSAFE_REPLACEMENT.to_string(),
    }];

    let result = sync_hierarchy(
        &paths,
        &document,
        &mut MediaPmState::default(),
        &cas,
        true,
        &ConductorState::new_empty(),
        &NickelDocument::default(),
        None,
        None,
    )
    .await;

    let folder_dir = root.path().join("album");
    let rewritten = folder_dir.join(REWRITTEN_MEMBER_NAME);
    assert!(
        !rewritten.exists(),
        "'{}' was written, so the configured replacement map reached the rename chain; \
             the member name is not the user's to rewrite",
        rewritten.display()
    );

    let error = result.err().unwrap_or_else(|| {
        panic!(
            "a replacement emitting '{UNSAFE_REPLACEMENT}' must be refused under \
                 sanitize_names = Enabled, not rewritten"
        )
    });
    assert!(
        error.to_string().contains(REPLACED_LEAF),
        "the error must name the file name the rule produced; got: {error}"
    );
}
