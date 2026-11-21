//! # Workflow-step incrementality guarantees (T8 C2)
//!
//! Step identity depends on step options/config and resolved inputs, not on
//! managed-tool payload bytes, and impure steps' outputs survive invalidation of
//! downstream steps so a rerun can reuse real bytes.
//!
//! These tests pin that contract on an offline three-step media
//! (`import` -> `ffmpeg` -> `ffmpeg`) provisioned through the stub upstream, so
//! the only network is the local mock and the only tool binaries are the
//! compiled stub copies the mock serves.

use std::collections::BTreeMap;

use bytes::Bytes;
use mediapm::{
    AddInsertPosition, GenericOutputVariantConfig, HierarchyNode, HierarchyNodeKind, HierarchyPath,
    MediaPmError, MediaPmPaths, MediaPmService, MediaRuntimeStorage, MediaSourceSpec, MediaStep,
    MediaStepTool, OutputSaveConfig, OutputVariantValue, PlaylistFormat, SanitizeNamesConfig,
    TransformInputValue, YtDlpOutputVariantConfig, load_mediapm_document, media_id_from_uri,
    save_mediapm_document,
};
use mediapm_cas::CasApi;
use mediapm_conductor::decode_state_json;
use url::Url;

use crate::common::{
    service_with_cache, sync_library_with_test_terminal, test_sync_progress_overrides,
};

/// Source URI for the incrementality probe media.
const SOURCE_URI: &str = "local:incrementality-probe";
/// Hierarchy path the final step's output lands at.
const HIERARCHY_PATH: &str = "incrementality_out";
/// Payload the import step ingests.
const PAYLOAD: &[u8] = b"incrementality probe payload";

/// Reads the conductor state as last written to disk.
fn conductor_state_on_disk(paths: &MediaPmPaths) -> mediapm_conductor::ConductorState {
    let bytes = std::fs::read(&paths.conductor_state_config).expect("read conductor state");
    decode_state_json(&bytes).expect("decode conductor state")
}

/// Builds the offline three-step source: `import` -> `ffmpeg` -> `ffmpeg`.
///
/// `step2_options` apply to the middle ffmpeg step (step index 1, "step 2" in
/// 1-based order). `step3_options` apply to the final ffmpeg step (step index 2,
/// "step 3"). Distinct options keep the two ffmpeg steps from collapsing to one
/// instance key when the stub copies identical bytes.
fn three_step_source_spec(
    hash: &str,
    step2_options: BTreeMap<String, TransformInputValue>,
    step3_options: BTreeMap<String, TransformInputValue>,
) -> MediaSourceSpec {
    MediaSourceSpec {
        steps: vec![
            MediaStep {
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
            },
            MediaStep {
                tool: MediaStepTool::Ffmpeg,
                input_variants: vec!["primary".to_string()],
                output_variants: BTreeMap::from([(
                    "mid".to_string(),
                    OutputVariantValue::Generic(GenericOutputVariantConfig {
                        kind: "primary".to_string(),
                        extension: "mkv".to_string(),
                        save: OutputSaveConfig::Full,
                        ..Default::default()
                    }),
                )]),
                options: step2_options,
            },
            MediaStep {
                tool: MediaStepTool::Ffmpeg,
                input_variants: vec!["mid".to_string()],
                output_variants: BTreeMap::from([(
                    "out".to_string(),
                    OutputVariantValue::Generic(GenericOutputVariantConfig {
                        kind: "primary".to_string(),
                        extension: "mkv".to_string(),
                        save: OutputSaveConfig::Full,
                        ..Default::default()
                    }),
                )]),
                options: step3_options,
            },
        ],
        ..MediaSourceSpec::default()
    }
}

/// Seeds the three-step probe workspace: payload in CAS, source, hierarchy, and
/// ffmpeg declared as a managed tool so it is provisioned via sync.
async fn seed_three_step_workspace(
    service: &mut MediaPmService<mediapm_cas::FileSystemCas>,
    step2_options: BTreeMap<String, TransformInputValue>,
    step3_options: BTreeMap<String, TransformInputValue>,
) -> Result<String, MediaPmError> {
    let uri = Url::parse(SOURCE_URI).expect("source URI must parse");
    let media_id = media_id_from_uri(&uri);

    let cas = service.conductor().cas().clone();
    let hash = cas
        .put(Bytes::copy_from_slice(PAYLOAD))
        .await
        .map_err(|e| MediaPmError::Workflow(format!("seed payload: {e}")))?;

    service.add_media_source(
        &three_step_source_spec(&hash.to_string(), step2_options, step3_options),
        media_id.clone(),
        &uri,
        None,
        None,
    )?;

    let paths = service.paths().clone();
    let mut document = load_mediapm_document(&paths.mediapm_ncl)?;
    document
        .runtime
        .tools
        .entry("ffmpeg".to_string())
        .or_insert_with(mediapm::ToolRequirement::default);
    document.hierarchy.push(HierarchyNode {
        path: HierarchyPath::simple(HIERARCHY_PATH),
        kind: HierarchyNodeKind::Media,
        id: None,
        media_id: Some(media_id.clone()),
        variant: Some("out".to_string()),
        variants: Vec::new(),
        rename_files: Vec::new(),
        format: PlaylistFormat::M3u8,
        ids: Vec::new(),
        sanitize_names: Some(SanitizeNamesConfig::Inherit),
        children: Vec::new(),
    });
    save_mediapm_document(&paths.mediapm_ncl, &document)?;
    Ok(media_id)
}

/// Installs managed-tool upstream seams pointing at a fresh mock that serves
/// the real stub binaries, so provisioned tools execute as chain copies.
async fn stub_upstreams()
-> (mediapm_test_support::MockUpstream, mediapm_test_support::SeamGuard<'static>) {
    let mock = mediapm_test_support::MockUpstream::start().await;
    let _managed = mediapm_test_support::stub_media::mount_managed_tool_upstreams(&mock).await;
    let seams = mock.all_seams(mediapm::example_isolation::env_lock());
    (mock, seams)
}

/// Runs the tool sync phase (provisioning) with a test terminal.
async fn sync_tools(
    service: &mut MediaPmService<mediapm_cas::FileSystemCas>,
) -> Result<mediapm::ToolsSyncSummary, MediaPmError> {
    service.sync_tools_with_progress_overrides(false, false, test_sync_progress_overrides()).await
}

/// Replaces the three-step source, keeping the same media identity, so the next
/// sync plans against the updated step options.
fn replace_three_step_source(
    service: &mut MediaPmService<mediapm_cas::FileSystemCas>,
    media_id: &str,
    hash: &str,
    step2_options: BTreeMap<String, TransformInputValue>,
    step3_options: BTreeMap<String, TransformInputValue>,
) -> Result<(), MediaPmError> {
    let uri = Url::parse(SOURCE_URI).expect("source URI must parse");
    service.add_media_source_with_position(
        &three_step_source_spec(hash, step2_options, step3_options),
        media_id.to_string(),
        &uri,
        None,
        None,
        AddInsertPosition::End,
        true,
    )?;
    Ok(())
}

/// Changing a step's options re-executes only that step when its output bytes
/// are unchanged.
///
/// The user-level guarantee: incrementality is content-addressed. A step's
/// identity includes its options and resolved inputs, but a re-executed step
/// that produces identical output bytes does not invalidate dependents whose
/// resolved inputs therefore remain unchanged. Editing step 2 re-executes step
/// 2 while the impure import (step 1) and downstream step 3 stay cached here
/// because the stub copies bytes verbatim and step 2's output hash is unchanged.
#[tokio::test]
async fn a_step_option_change_re_executes_only_the_changed_step_when_bytes_are_unchanged()
-> Result<(), MediaPmError> {
    let (mock, _seams) = stub_upstreams().await;
    let _ = mock; // keep mock alive
    let mut runtime = MediaRuntimeStorage::default();
    runtime.tools.insert("ffmpeg".to_string(), mediapm::ToolRequirement::default());
    let (mut service, _root, _cache) = service_with_cache(runtime).await?;
    let step2_initial =
        BTreeMap::from([("crf".to_string(), TransformInputValue::String("23".to_string()))]);
    let step3_options =
        BTreeMap::from([("crf".to_string(), TransformInputValue::String("28".to_string()))]);
    let media_id =
        seed_three_step_workspace(&mut service, step2_initial.clone(), step3_options.clone())
            .await?;

    let tools = sync_tools(&mut service).await?;
    assert!(tools.added_tools >= 1, "ffmpeg must be provisioned: {tools:?}");

    let first = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(first.executed_steps, 3, "the first sync must execute all three steps: {first:?}");

    let second = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(second.executed_steps, 0, "an unchanged rerun must be fully cached: {second:?}");
    assert_eq!(second.cached_steps, 3, "all three steps must hit cache: {second:?}");

    // Change the middle ffmpeg step's options (step 2 in 1-based order).
    let cas = service.conductor().cas().clone();
    let hash = cas
        .put(Bytes::copy_from_slice(PAYLOAD))
        .await
        .map_err(|e| MediaPmError::Workflow(format!("re-seed payload: {e}")))?;
    let step2_changed =
        BTreeMap::from([("crf".to_string(), TransformInputValue::String("18".to_string()))]);
    replace_three_step_source(
        &mut service,
        &media_id,
        &hash.to_string(),
        step2_changed,
        step3_options,
    )?;

    let third = sync_library_with_test_terminal(&mut service, false).await?;
    // The stub copies bytes verbatim, so changing step 2's options re-executes
    // step 2 but produces identical output bytes. Step 3's resolved inputs are
    // therefore unchanged and it remains cached — correct incrementality:
    // only steps whose identity or inputs changed re-execute.
    assert_eq!(
        third.executed_steps, 1,
        "changing step 2 must re-execute step 2; step 3 stays cached because step 2's output bytes are unchanged: {third:?}"
    );
    assert_eq!(
        third.cached_steps, 2,
        "the impure import (step 1) and the downstream step 3 (unchanged inputs) must remain cached: {third:?}"
    );

    drop(service);
    Ok(())
}

/// A tool payload update that the sync reports must not cause workflow steps to
/// re-execute, because step identity does not include tool payload bytes.
///
/// The workflow runs against real stub binaries first so every step is cached.
/// A second tool sync serves the same tool with a different resolved version and
/// different payload bytes, which the sync must report as an update, but the
/// cached workflow instances remain valid because their keys are derived from
/// step material, not payload.
#[tokio::test]
async fn a_tool_payload_update_does_not_re_execute_steps() -> Result<(), MediaPmError> {
    // First provision: real stub binaries so the workflow can run.
    let mock = mediapm_test_support::MockUpstream::start().await;
    let _managed = mediapm_test_support::stub_media::mount_managed_tool_upstreams(&mock).await;
    let seams = mock.all_seams(mediapm::example_isolation::env_lock());

    let mut runtime = MediaRuntimeStorage::default();
    runtime.tools.insert("ffmpeg".to_string(), mediapm::ToolRequirement::default());
    let (mut service, root, _cache) = service_with_cache(runtime.clone()).await?;
    let step2_options =
        BTreeMap::from([("crf".to_string(), TransformInputValue::String("23".to_string()))]);
    let step3_options =
        BTreeMap::from([("crf".to_string(), TransformInputValue::String("28".to_string()))]);
    let _media_id = seed_three_step_workspace(&mut service, step2_options, step3_options).await?;

    let tools_first = sync_tools(&mut service).await?;
    assert!(tools_first.added_tools >= 1, "ffmpeg must be provisioned: {tools_first:?}");

    let first = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(first.executed_steps, 3, "first run must execute all steps: {first:?}");
    let cached = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(cached.executed_steps, 0, "steps must be cached before the tool update: {cached:?}");

    drop(seams);
    drop(mock);
    drop(service);

    // Second provision: a different upstream build (new autobuild tag and macOS
    // leg id) with different payload bytes. The resolved canonical version
    // changes, so the sync reprovisions and reports an update. A fresh cache is
    // required so the metadata cache does not answer the old version.
    let mock2 = mediapm_test_support::MockUpstream::start().await;
    mount_ffmpeg_updated_build(&mock2).await;
    let _seams2 = mock2.all_seams(mediapm::example_isolation::env_lock());

    let fresh_cache = mediapm_utils::temp::cache_dir().expect("fresh cache dir");
    let mut runtime2 = runtime;
    runtime2.cache_root_override = Some(fresh_cache.path().to_path_buf());
    let mut service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), runtime2).await?;

    let tools_second = sync_tools(&mut service).await?;
    assert!(
        tools_second.updated_tools >= 1 || tools_second.added_tools >= 1,
        "a different payload must be reported as a tool update: {tools_second:?}"
    );

    let after_update = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(
        after_update.executed_steps, 0,
        "step identity must not depend on tool payload bytes, so no workflow step may re-execute: {after_update:?}"
    );
    assert_eq!(
        after_update.cached_steps, 3,
        "all three steps must remain cached after a tool payload update: {after_update:?}"
    );

    drop(service);
    drop(fresh_cache);
    drop(root);
    Ok(())
}

/// Mounts ffmpeg from a different upstream build with different payload bytes.
///
/// Only ffmpeg is declared in this test's desired tools, so no other managed
/// tool needs to be mounted.
async fn mount_ffmpeg_updated_build(mock: &mediapm_test_support::MockUpstream) {
    use mediapm_test_support::{PayloadFormat, StubAsset, StubEntry};

    // A different resolved build and different bytes.
    const UPDATED_AUTOBUILD_TAG: &str = "autobuild-2026-02-01-12-00";
    const UPDATED_MARTIN_RIEDL_BUILD_ID: &str = "1867225600_N-127999-gdeadbeef";
    const UPDATED_COMMIT_SHA: &str = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
    const JELLYFIN_LINE: &str = "latest-10.x";
    const JELLYFIN_VERSION: &str = "7.1.1";

    fn slot_exec(tool: &str, slot: &str) -> String {
        if slot == "windows" { format!("{tool}.exe") } else { tool.to_string() }
    }
    fn host_arch() -> &'static str {
        match std::env::consts::ARCH {
            "aarch64" => "aarch64",
            _ => "x86_64",
        }
    }

    mock.mount_github_release_list("FFmpeg-Builds", "FFmpeg-Builds", &[UPDATED_AUTOBUILD_TAG])
        .await;

    let mut assets = Vec::new();
    for (os, name, format) in [
        ("windows", "ffmpeg-master-latest-win64-gpl-shared.zip", PayloadFormat::Zip),
        ("windows", "ffmpeg-master-latest-win64-gpl.zip", PayloadFormat::Zip),
        ("linux", "ffmpeg-master-latest-linux64-gpl-shared.tar.xz", PayloadFormat::TarXz),
        ("linux", "ffmpeg-master-latest-linux64-gpl.tar.xz", PayloadFormat::TarXz),
    ] {
        let body = format!("updated ffmpeg payload for {os}\n").into_bytes();
        let entries = vec![StubEntry::executable(slot_exec("ffmpeg", os), body)];
        assets.push(StubAsset { name: name.to_string(), bytes: format.pack(&entries) });
    }
    mock.mount_github_release(
        "FFmpeg-Builds",
        "FFmpeg-Builds",
        UPDATED_AUTOBUILD_TAG,
        UPDATED_COMMIT_SHA,
        &assets,
    )
    .await;

    let arch = host_arch();
    for (canonical, site_label) in [("aarch64", "arm64"), ("x86_64", "amd64")] {
        let body = if canonical == arch {
            b"updated ffmpeg macos payload\n".to_vec()
        } else {
            format!("updated ffmpeg payload for {canonical}\n").into_bytes()
        };
        let entries = vec![StubEntry::executable(slot_exec("ffmpeg", canonical), body)];
        let payload = PayloadFormat::Zip.pack(&entries);
        for channel in ["snapshot", "release"] {
            mock.mount_martin_riedl(site_label, channel, UPDATED_MARTIN_RIEDL_BUILD_ID, &payload)
                .await;
        }
    }

    for (canonical, site_dir, site_token) in
        [("aarch64", "arm64", "macarm64"), ("x86_64", "x86_64", "mac64")]
    {
        let archive =
            format!("jellyfin-ffmpeg_{JELLYFIN_VERSION}_portable_{site_token}-gpl.tar.xz");
        let body = format!("updated jellyfin ffmpeg for {canonical}\n").into_bytes();
        let entries = vec![StubEntry::executable(slot_exec("ffmpeg", canonical), body)];
        let payload = PayloadFormat::TarXz.pack(&entries);
        mock.mount_jellyfin(JELLYFIN_LINE, site_dir, site_token, &archive, &payload).await;
    }
}

/// Invalidating the last step must not make the impure import step's output
/// unreadable after GC.
///
/// The impure `import` step (step 1) produces bytes the workflow needs; its
/// recorded output must remain CAS-readable even when a later step is
/// invalidated and GC runs, because a cached impure step's output is still a
/// live dependency of the workflow state.
#[tokio::test]
async fn invalidating_the_last_step_preserves_the_impure_import_output_in_cas()
-> Result<(), MediaPmError> {
    let (_mock, _seams) = stub_upstreams().await;
    let mut runtime = MediaRuntimeStorage::default();
    runtime.tools.insert("ffmpeg".to_string(), mediapm::ToolRequirement::default());
    let (mut service, _root, _cache) = service_with_cache(runtime).await?;
    let step2_options =
        BTreeMap::from([("crf".to_string(), TransformInputValue::String("23".to_string()))]);
    let step3_options =
        BTreeMap::from([("crf".to_string(), TransformInputValue::String("28".to_string()))]);
    let media_id = seed_three_step_workspace(&mut service, step2_options, step3_options).await?;

    let tools = sync_tools(&mut service).await?;
    assert!(tools.added_tools >= 1, "ffmpeg must be provisioned: {tools:?}");

    let first = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(first.executed_steps, 3, "first run must execute all steps: {first:?}");
    let cached = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(cached.executed_steps, 0, "steps must be cached before invalidation: {cached:?}");

    let state_before = conductor_state_on_disk(service.paths());
    let import_output_hash = state_before
        .tool_call_instances
        .values()
        .find_map(|instance| {
            let serves_import = instance
                .step_ids_nk
                .iter()
                .any(|s| s.step_id.contains("import") && s.step_id.contains("primary"));
            if !serves_import {
                return None;
            }
            instance.outputs.values().find_map(|o| o.hash_nk)
        })
        .expect("import step must have recorded an output hash");

    // Invalidate the last ffmpeg step (step index 2, 0-based).
    let invalidation = service.invalidate_media_step_tool_calls(&media_id, 2).await?;
    assert!(
        invalidation.changed_instances >= 1,
        "invalidating the last step must mark its instance: {invalidation:?}"
    );

    let rerun = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(
        rerun.executed_steps, 1,
        "only the invalidated last step must re-execute: {rerun:?}"
    );
    assert_eq!(
        rerun.cached_steps, 2,
        "the impure import and the unchanged middle step must remain cached: {rerun:?}"
    );

    // GC must not reclaim the impure import step's output while it remains
    // reachable as a recorded workflow dependency.
    service.conductor().run_gc().await?;

    let cas = service.conductor().cas().clone();
    let readable = cas.get(import_output_hash).await.is_ok();
    assert!(
        readable,
        "the impure import output ({import_output_hash}) must remain CAS-readable after GC"
    );

    // Sanity: the import instance is still present in state.
    let state_after = conductor_state_on_disk(service.paths());
    assert!(
        state_after.tool_call_instances.values().any(|instance| {
            instance
                .step_ids_nk
                .iter()
                .any(|s| s.step_id.contains("import") && s.step_id.contains("primary"))
        }),
        "the impure import instance must still be recorded after rerun and GC"
    );

    drop(service);
    Ok(())
}

/// Changing the upstream payload bytes re-executes the changed step and its
/// dependents, and the materializer resolves to the newly produced instances.
///
/// A re-execution that produces different bytes creates a new instance record.
/// The conductor records the current instance in `step_instances`, so the
/// materializer must select it rather than refuse between disagreeing records.
#[tokio::test]
async fn changing_upstream_bytes_re_executes_dependents_and_resolves_the_new_instances()
-> Result<(), MediaPmError> {
    let (_mock, _seams) = stub_upstreams().await;
    let mut runtime = MediaRuntimeStorage::default();
    runtime.tools.insert("ffmpeg".to_string(), mediapm::ToolRequirement::default());
    let (mut service, _root, _cache) = service_with_cache(runtime).await?;
    let step2_options =
        BTreeMap::from([("crf".to_string(), TransformInputValue::String("23".to_string()))]);
    let step3_options =
        BTreeMap::from([("crf".to_string(), TransformInputValue::String("28".to_string()))]);
    let media_id =
        seed_three_step_workspace(&mut service, step2_options.clone(), step3_options.clone())
            .await?;

    let tools = sync_tools(&mut service).await?;
    assert!(tools.added_tools >= 1, "ffmpeg must be provisioned: {tools:?}");

    let first = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(first.executed_steps, 3, "first run must execute all steps: {first:?}");
    let cached = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(cached.executed_steps, 0, "steps must be cached before the change: {cached:?}");

    // Replace the ingested payload with different bytes so the import step's
    // output changes and every dependent step's resolved input changes with it.
    let cas = service.conductor().cas().clone();
    let new_hash = cas
        .put(Bytes::from_static(b"incrementality probe payload, changed"))
        .await
        .map_err(|e| MediaPmError::Workflow(format!("seed changed payload: {e}")))?;
    replace_three_step_source(
        &mut service,
        &media_id,
        &new_hash.to_string(),
        step2_options,
        step3_options,
    )?;

    let rerun = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(
        rerun.executed_steps, 3,
        "changed upstream bytes must re-execute the import and every dependent step: {rerun:?}"
    );
    assert!(
        rerun.missing_paths == 0,
        "the re-executed workflow must materialize successfully: {rerun:?}"
    );

    drop(service);
    Ok(())
}
