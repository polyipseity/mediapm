//! # One piece of work, several names: a shared instance feeds every binding
//!
//! The instance key derives from inputs alone, so two media steps that ingest
//! the same payload under the same options are one piece of work with two
//! recorded names. The union rule is what makes that usable: each settlement
//! merges its captures into the entries the instance already holds, so the
//! record ends up naming both artifacts and the second sync — and the
//! materializer, which resolves variants from that record — finds both.
//!
//! Without the union, settlement replaced the record, so the instance kept only
//! the name of whichever step settled last: the other hierarchy entry resolved
//! to no hash and the two steps took turns re-executing on every sync. The
//! shape is the demo's own (one source, several captures off one execution),
//! with the builtin `import` tool so the scenario stays hermetic and downloads
//! nothing.

use std::collections::BTreeMap;

use crate::common::{seed_cas, service_with_cache, sync_library_with_test_terminal};
use bytes::Bytes;
use mediapm::{
    HierarchyNode, HierarchyNodeKind, HierarchyPath, MediaPmError, MediaPmService,
    MediaRuntimeStorage, MediaSourceSpec, MediaStep, MediaStepTool, OutputVariantValue,
    PlaylistFormat, SanitizeNamesConfig, TransformInputValue, YtDlpOutputVariantConfig,
    load_mediapm_document, media_id_from_uri, save_mediapm_document,
};
use url::Url;

/// URI of the single offline source; its id is the media id both steps publish.
const SOURCE_URI: &str = "local:capture-union-probe";

/// Output variant names the two import steps declare, one each.
const VARIANTS: [&str; 2] = ["alpha", "beta"];

/// Hierarchy paths the two variants materialize at.
const HIERARCHY_PATHS: [&str; 2] = ["union_alpha", "union_beta"];

/// Payload both steps ingest, which both captures therefore name.
const PAYLOAD: &[u8] = b"capture-union probe payload";

/// Builds one `import` step ingesting `hash` and publishing `variant`.
///
/// The two steps differ only in the name they publish: their options and their
/// `cas_hash` input are identical, which is what gives them one instance key.
fn import_step(hash: &str, variant: &str) -> MediaStep {
    MediaStep {
        tool: MediaStepTool::Import,
        input_variants: Vec::new(),
        output_variants: BTreeMap::from([(
            variant.to_string(),
            OutputVariantValue::YtDlp(YtDlpOutputVariantConfig::default()),
        )]),
        options: BTreeMap::from([
            ("kind".to_string(), TransformInputValue::String("cas_hash".to_string())),
            ("hash".to_string(), TransformInputValue::String(hash.to_string())),
        ]),
    }
}

/// Seeds the payload in CAS, one two-step import source, and the two hierarchy
/// entries that bind the two published variants.
async fn seed_union_workspace(
    service: &mut MediaPmService<mediapm_cas::FileSystemCas>,
    uri: &Url,
) -> Result<(), MediaPmError> {
    let hash = seed_cas(service, Bytes::copy_from_slice(PAYLOAD), "union payload").await?;
    service
        .add_media_source(
            &MediaSourceSpec {
                steps: VARIANTS
                    .iter()
                    .map(|variant| import_step(&hash.to_string(), variant))
                    .collect(),
                ..MediaSourceSpec::default()
            },
            media_id_from_uri(uri),
            uri,
            None,
            None,
        )
        .map_err(|source| MediaPmError::Workflow(format!("adding the union source: {source}")))?;

    let paths = service.paths().clone();
    let mut document = load_mediapm_document(&paths.mediapm_ncl)?;
    for (variant, path) in VARIANTS.iter().zip(HIERARCHY_PATHS) {
        document.hierarchy.push(HierarchyNode {
            path: HierarchyPath::simple(path),
            kind: HierarchyNodeKind::Media,
            id: None,
            media_id: Some(media_id_from_uri(uri)),
            variant: Some((*variant).to_string()),
            variants: Vec::new(),
            rename_files: Vec::new(),
            format: PlaylistFormat::M3u8,
            ids: Vec::new(),
            sanitize_names: Some(SanitizeNamesConfig::Inherit),
            children: Vec::new(),
        });
    }
    save_mediapm_document(&paths.mediapm_ncl, &document)?;
    Ok(())
}

/// Two steps over identical work end with one instance naming both artifacts,
/// both materialize, and the second sync re-executes nothing.
///
/// The user-level guarantee: declaring a second output off the same command
/// does not cost a second run per sync. The union is what the second sync
/// depends on — a record that kept one name would send the other step back to
/// the command on every run.
#[tokio::test]
async fn two_steps_over_identical_work_share_one_instance_and_both_names_materialize()
-> Result<(), MediaPmError> {
    let (mut service, root, _cache) = service_with_cache(MediaRuntimeStorage::default()).await?;

    let uri = Url::parse(SOURCE_URI).expect("source URI must parse");
    seed_union_workspace(&mut service, &uri).await?;

    let first = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(
        (first.executed_instances, first.missing_paths),
        (2, 0),
        "both steps execute once and both paths resolve from the one recorded instance: {first:?}",
    );
    assert_eq!(
        first.materialized_paths,
        HIERARCHY_PATHS.len(),
        "every hierarchy entry must materialize: {first:?}",
    );
    for path in HIERARCHY_PATHS {
        let materialized = root.path().join(path);
        let bytes = std::fs::read(&materialized).map_err(|source| MediaPmError::Io {
            operation: "reading the materialized union output".to_string(),
            path: materialized.clone(),
            source,
        })?;
        assert_eq!(bytes, PAYLOAD, "{} must hold the ingested payload", materialized.display());
    }

    let state_path = service.paths().conductor_state_config.clone();
    let state_bytes = std::fs::read(&state_path).map_err(|source| MediaPmError::Io {
        operation: "reading conductor state file".to_string(),
        path: state_path.clone(),
        source,
    })?;
    let state = mediapm_conductor::decode_state_json(&state_bytes)
        .map_err(|error| MediaPmError::Workflow(format!("decoding conductor state: {error}")))?;
    assert_eq!(
        state.tool_call_instances.len(),
        1,
        "identical work must be one instance, not one per step: {:?}",
        state.tool_call_instances.keys().collect::<Vec<_>>(),
    );
    let outputs = &state.tool_call_instances.values().next().expect("the one instance").outputs;
    for variant in VARIANTS {
        assert!(
            outputs.contains_key(variant),
            "the shared instance must record '{variant}', got {:?}",
            outputs.keys().collect::<Vec<_>>(),
        );
    }

    let second = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(
        (second.executed_instances, second.materialized_paths, second.missing_paths),
        (0, 0, 0),
        "an unchanged re-run must execute and materialize nothing: {second:?}",
    );
    assert_eq!(
        second.skipped_paths,
        HIERARCHY_PATHS.len(),
        "both materialized paths must be declined on the re-run: {second:?}",
    );

    // Release the CAS store lock before the workspace TempDir removes the tree.
    drop(service);
    Ok(())
}
