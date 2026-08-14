//! Conductor execution end-to-end tests: `sync_library` runs the synthesized
//! managed workflows through the conductor and persists conductor state.

use std::collections::BTreeMap;
use std::path::Path;

use bytes::Bytes;
use mediapm::{
    HierarchyNode, HierarchyNodeKind, HierarchyPath, MediaPmService, MediaRuntimeStorage,
    MediaSourceSpec, MediaStep, MediaStepTool, OutputVariantValue, PlaylistFormat,
    SanitizeNamesConfig, TransformInputValue, YtDlpOutputVariantConfig, load_mediapm_document,
    load_mediapm_state_document, media_id_from_uri, save_mediapm_document,
};
use mediapm_cas::CasApi;
use url::Url;

/// Creates a filesystem service whose tool download cache lives inside the
/// workspace root, so parallel tests never contend on the OS-level cache.
async fn service_at(
    root: &Path,
) -> Result<MediaPmService<mediapm_cas::FileSystemCas>, mediapm::MediaPmError> {
    let runtime_storage = MediaRuntimeStorage {
        cache_root_override: Some(root.join("tool-cache")),
        ..MediaRuntimeStorage::default()
    };
    MediaPmService::new_fs_at_with_runtime_storage_overrides(root, runtime_storage).await
}

/// Builds a single-step `import` source spec that ingests one CAS payload
/// hash (`kind=cas_hash`, mirroring `local_source_default_steps`).
fn import_source_spec(hash: &str) -> MediaSourceSpec {
    MediaSourceSpec {
        steps: vec![MediaStep {
            tool: MediaStepTool::Import,
            input_variants: Vec::new(),
            output_variants: BTreeMap::from([(
                "primary".to_string(),
                OutputVariantValue::YtDlp(YtDlpOutputVariantConfig::default()),
            )]),
            options: BTreeMap::from([
                ("kind".to_string(), TransformInputValue::String("cas_hash".to_string())),
                ("hash".to_string(), TransformInputValue::String(hash.to_string())),
            ]),
        }],
        ..MediaSourceSpec::default()
    }
}

/// Seeds the import fixture payload into the CAS and registers an import
/// source for `uri` in the service document.
async fn seed_and_add_import_source(
    service: &mut MediaPmService<mediapm_cas::FileSystemCas>,
    uri: &Url,
    payload: &[u8],
) -> Result<(), mediapm::MediaPmError> {
    let cas = service.conductor().cas().clone();
    let hash = cas
        .put(Bytes::copy_from_slice(payload))
        .await
        .map_err(|e| mediapm::MediaPmError::Workflow(format!("seeding CAS: {e}")))?;
    service.add_media_source(
        &import_source_spec(&hash.to_string()),
        media_id_from_uri(uri),
        uri,
        None,
        None,
    )?;
    Ok(())
}

/// Decodes the conductor state file, failing with a Workflow error so `?`
/// works in test bodies.
fn read_conductor_state(
    path: &std::path::Path,
) -> Result<mediapm_conductor::ConductorState, mediapm::MediaPmError> {
    let bytes = std::fs::read(path).map_err(|source| mediapm::MediaPmError::Io {
        operation: "reading conductor state file".to_string(),
        path: path.to_path_buf(),
        source,
    })?;
    mediapm_conductor::decode_state_json(&bytes)
        .map_err(|e| mediapm::MediaPmError::Workflow(format!("decoding conductor state: {e}")))
}

/// A full sync runs the synthesized `mediapm.media.*` import workflow through
/// the conductor: the CAS hash payload is ingested, an instance key lands in
/// the persisted state file, and the generated doc registers the workflow.
#[tokio::test]
async fn sync_executes_import_workflow() -> Result<(), mediapm::MediaPmError> {
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let mut service = service_at(root.path()).await?;

    let uri = Url::parse("local:phase2-import").expect("url must parse");
    seed_and_add_import_source(&mut service, &uri, b"phase2 conductor execution fixture").await?;
    let media_id = media_id_from_uri(&uri);

    let summary = service.sync_library(false).await?;

    // The managed import workflow executed: at least one conductor instance.
    assert!(
        summary.executed_instances >= 1,
        "import workflow should execute at least one conductor instance"
    );
    assert!(
        summary.warnings.iter().all(|warning| !warning.contains("failed step")),
        "expected no failed-step warnings, got: {:?}",
        summary.warnings
    );

    // The synthesized managed workflow is persisted in the generated doc.
    let generated_bytes = std::fs::read(&service.paths().conductor_generated_ncl).map_err(|e| {
        mediapm::MediaPmError::Io {
            operation: "read generated conductor doc".to_string(),
            path: service.paths().conductor_generated_ncl.clone(),
            source: e,
        }
    })?;
    let generated_doc = mediapm_conductor::decode_document(&generated_bytes)
        .map_err(|e| mediapm::MediaPmError::Workflow(format!("decode generated doc: {e}")))?;
    assert!(
        generated_doc
            .workflows
            .iter()
            .any(|workflow| workflow.name == format!("mediapm.media.{media_id}")),
        "generated doc should contain the managed workflow for {media_id}"
    );

    // `run_workflow` persists conductor state after every run.
    let state_path = root.path().join(".mediapm").join("state.conductor.json");
    let state = read_conductor_state(&state_path)?;
    assert!(
        !state.tool_call_instances.is_empty(),
        "state file should record the executed import instance: {}",
        state_path.display()
    );

    Ok(())
}

/// Re-syncing the same workspace resumes from the persisted conductor state;
/// the impure import step re-executes (impure steps get a fresh per-run
/// instance key) and the state file keeps accumulating instances.
#[tokio::test]
async fn sync_twice_conductor_state_persists() -> Result<(), mediapm::MediaPmError> {
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");

    // First service: add the source and run a full sync.
    let first_executed;
    {
        let mut service = service_at(root.path()).await?;
        let uri = Url::parse("local:phase2-import").expect("url must parse");
        seed_and_add_import_source(&mut service, &uri, b"phase2 conductor execution fixture")
            .await?;
        let summary = service.sync_library(false).await?;
        assert!(summary.executed_instances >= 1, "first sync should execute the import workflow");
        first_executed = summary.executed_instances;
    }

    let state_path = root.path().join(".mediapm").join("state.conductor.json");
    let first_state = read_conductor_state(&state_path)?;

    // Second service on the same root: mediapm document and conductor state
    // persist across service instances; the impure import re-executes.
    let mut service = service_at(root.path()).await?;
    let second_summary = service.sync_library(false).await?;
    assert!(
        second_summary.executed_instances >= 1,
        "second sync should execute the import workflow again"
    );
    assert!(
        second_summary.warnings.iter().all(|warning| !warning.contains("failed step")),
        "second sync should not report failed steps: {:?}",
        second_summary.warnings
    );

    let second_state = read_conductor_state(&state_path)?;
    assert!(
        second_state.tool_call_instances.len() > first_state.tool_call_instances.len(),
        "state file should accumulate the second run's instance"
    );
    assert_eq!(
        first_executed, second_summary.executed_instances,
        "each sync executes exactly one import step"
    );

    Ok(())
}

/// A sync whose import step fails must still shut down cleanly. The failed
/// step exercises the graceful-failure path: the workflow runs, the step
/// fails, and the step worker exits normally while the workflow is still
/// supervised by the conductor actor.
#[tokio::test]
async fn sync_with_failed_step_then_shutdown_succeeds() -> Result<(), mediapm::MediaPmError> {
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    {
        let mut service = service_at(root.path()).await?;

        // Seed the CAS with a real payload; the import step below references
        // an invalid hash, so the step fails instead of ingesting it.
        let cas = service.conductor().cas().clone();
        let _hash = cas
            .put(Bytes::from_static(b"phase2 conductor execution fixture"))
            .await
            .map_err(|e| mediapm::MediaPmError::Workflow(format!("seeding CAS: {e}")))?;

        let uri = Url::parse("local:phase2-failed-import").expect("url must parse");
        let media_id = media_id_from_uri(&uri);
        service.add_media_source(&import_source_spec("x"), media_id.clone(), &uri, None, None)?;

        let summary = service.sync_library(false).await?;
        assert!(
            summary.warnings.iter().any(|warning| warning.contains("failed step")),
            "the invalid-hash import step should produce a failed-step warning, got: {:?}",
            summary.warnings
        );

        // Regression: the conductor actor must survive linked step-worker
        // exits during Shutdown (handle_supervisor_evt override in
        // orchestration/node.rs). Before that override, the actor self-stopped
        // when its linked step worker exited inside the Shutdown handler's
        // `coordinator.shutdown()`, so the client's `stop_and_wait` failed with
        // `MessagingErr::ChannelClosed` ("Messaging failed because channel is
        // closed").
        service.conductor().shutdown().await?;
    }

    // Sequential same-root opens must not deadlock on the CAS directory lock
    // after a clean shutdown released the first service's handle.
    let service = service_at(root.path()).await?;
    drop(service);

    Ok(())
}

/// Materialization records managed outputs in `state.json` when hierarchy
/// entries resolve workflow variant hashes from conductor state.
#[tokio::test]
async fn sync_populates_managed_files_from_conductor_state() -> Result<(), mediapm::MediaPmError> {
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let mut service = service_at(root.path()).await?;

    let uri = Url::parse("local:phase3-managed-files").expect("url must parse");
    seed_and_add_import_source(&mut service, &uri, b"phase3 materializer managed_files fixture")
        .await?;
    let media_id = media_id_from_uri(&uri);

    let paths = service.paths().clone();
    let mut document = load_mediapm_document(&paths.mediapm_ncl)?;
    document.hierarchy.push(HierarchyNode {
        path: HierarchyPath::simple("imported_fixture"),
        kind: HierarchyNodeKind::Media,
        id: None,
        media_id: Some(media_id.clone()),
        variant: Some("primary".to_string()),
        variants: vec![],
        rename_files: vec![],
        format: PlaylistFormat::M3u8,
        ids: vec![],
        sanitize_names: SanitizeNamesConfig::Inherit,
        children: vec![],
    });
    save_mediapm_document(&paths.mediapm_ncl, &document)?;

    service.sync_library(false).await?;

    let state = load_mediapm_state_document(&paths.mediapm_state_json)?;
    assert!(
        !state.managed_files.is_empty(),
        "expected managed_files after hierarchy materialization"
    );
    assert!(
        state.workflow_states.get(&media_id).is_some_and(|ws| !ws.variant_hashes.is_empty()),
        "expected workflow_states.variant_hashes for {media_id}"
    );

    Ok(())
}
