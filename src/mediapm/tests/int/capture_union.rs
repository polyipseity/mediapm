//! # Distinct output declarations produce distinct instances and both materialize
//!
//! Instance key version 8 includes the output specification (output names and
//! capture tokens) in key material, so two steps that declare different output
//! names are different instances even when they ingest the same payload under
//! the same options. Both steps still execute once, both declared artifacts
//! materialize, and a second unchanged sync re-executes and rematerializes
//! nothing.
//!
//! The scenario stays hermetic with the builtin `import` tool: one offline
//! source fans out into two hierarchy entries bound to different variants.

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

/// Two steps declaring different outputs over the same input work produce
/// distinct instances, both materialize, and the second sync re-executes
/// nothing.
///
/// The user-level guarantee: each declared output is captured and reusable on a
/// steady-state re-run even when another step ingests the same source bytes.
#[tokio::test]
async fn two_steps_over_identical_work_share_one_instance_and_both_names_materialize()
-> Result<(), MediaPmError> {
    let (mut service, root, _cache) = service_with_cache(MediaRuntimeStorage::default()).await?;

    let uri = Url::parse(SOURCE_URI).expect("source URI must parse");
    seed_union_workspace(&mut service, &uri).await?;

    let first = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(
        (first.executed_steps, first.missing_paths),
        (2, 0),
        "both steps execute once and both paths resolve from recorded instances: {first:?}",
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
        2,
        "v8 keys output specifications, so distinct output declarations must produce distinct instances: {:?}",
        state.tool_call_instances.keys().collect::<Vec<_>>(),
    );
    let recorded: std::collections::BTreeSet<_> = state
        .tool_call_instances
        .values()
        .flat_map(|instance| instance.outputs.keys().cloned())
        .collect();
    for variant in VARIANTS {
        assert!(
            recorded.contains(variant),
            "the recorded instances must include '{variant}', got {recorded:?}",
        );
    }

    let second = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(
        (second.executed_steps, second.materialized_paths, second.missing_paths),
        (0, 0, 0),
        "an unchanged re-run must execute and materialize nothing: {second:?}",
    );
    assert_eq!(
        second.skipped_paths,
        HIERARCHY_PATHS.len(),
        "both materialized paths must be declined on the re-run: {second:?}",
    );

    // Release the CAS store lock before the workspace TempDir removes the tree.
    service.shutdown().await.expect("service shutdown");
    drop(service);
    Ok(())
}
