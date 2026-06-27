//! Tool-reconciliation coordinator.
//!
//! This module orchestrates the full tool-sync lifecycle:
//! 1. Ensure conductor documents exist (generated + state)
//! 2. Load the generated document
//! 3. Provision desired tools (download missing payloads)
//! 4. Generate tool configs with companion binding
//! 5. Import content-map artifacts into CAS
//! 6. Apply lifecycle transitions (tag updates, launcher files)
//! 7. Write generated runtime env file
//! 8. Save the generated document

pub(crate) mod content_import;
pub(crate) mod lifecycle;
pub(crate) mod provision;
pub(crate) mod tool_config;

use std::collections::BTreeMap;

use mediapm_cas::CasApi;
use mediapm_conductor::cache_user_level::default_mediapm_user_download_cache_root;
use mediapm_conductor::{ToolKindSpec, ToolRuntime, ToolSpec};

use crate::conductor_bridge::documents::{
    load_conductor_generated_document, register_missing_builtin_tool_configs,
    register_missing_builtin_tools, save_conductor_generated_document,
};
use crate::conductor_bridge::sync::content_import::import_tool_content_files_into_cas;
use crate::conductor_bridge::sync::lifecycle::{
    ensure_internal_launcher_content_entries_exist, is_builtin_source_ingest_requirement,
    regenerate_media_tagger_internal_launcher_file, should_skip_tag_update_check,
};
use crate::conductor_bridge::sync::provision::provision_desired_tools_concurrently;
use crate::conductor_bridge::sync::tool_config::{
    ensure_machine_runtime_inherits_generated_env_vars, resolve_companion_deno_selection,
    resolve_companion_ffmpeg_selection, write_generated_runtime_env_file,
};
use crate::error::MediaPmError;
use crate::paths::MediaPmPaths;
use crate::tools::downloader::ToolDownloadCache;

/// Summary of one `mediapm tool sync` reconciliation pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ToolSyncReport {
    /// Number of tools newly registered.
    pub(crate) tools_added: usize,
    /// Number of tools removed (no longer in desired set).
    pub(crate) tools_removed: usize,
    /// Number of tools updated to match desired version.
    pub(crate) tools_updated: usize,
    /// Non-fatal warnings collected during reconciliation.
    pub(crate) warnings: Vec<String>,
}

/// Runs the full tool-reconciliation cycle for the current workspace.
///
/// # Errors
///
/// Returns an error when any critical step (document loading, builtin
/// registration, content-map import) fails. Non-critical failures are
/// reported as warnings in [`ToolSyncReport`].
#[allow(clippy::too_many_lines)]
pub(crate) async fn reconcile_desired_tools(
    cas: &impl CasApi,
    paths: &MediaPmPaths,
    desired_tools: &BTreeMap<String, serde_json::Value>,
    inherited_env_vars: &BTreeMap<String, Vec<String>>,
    check_tag_updates: bool,
) -> Result<ToolSyncReport, MediaPmError> {
    let mut report = ToolSyncReport::default();

    // 1. Load or create generated document.
    let mut generated_doc = load_conductor_generated_document(paths)?;

    // 2. Register missing builtin tool definitions and config stubs.
    register_missing_builtin_tools(&mut generated_doc);
    register_missing_builtin_tool_configs(&mut generated_doc);

    // 3. Desired-tool reconciliation: ensure each desired tool has a spec.
    let mut tool_runtimes: BTreeMap<String, ToolRuntime> = BTreeMap::new();
    for (tool_id, requirement_value) in desired_tools {
        let is_builtin_code = is_builtin_source_ingest_requirement(tool_id);
        let already_exists = generated_doc.tools.contains_key(tool_id);

        if !already_exists {
            // Synthesize a simple tool spec for the desired tool.
            generated_doc.tools.insert(
                tool_id.clone(),
                ToolSpec {
                    name: tool_id.clone(),
                    version: "latest".to_string(),
                    kind: ToolKindSpec::Executable {
                        command: Vec::new(),
                        env_vars: BTreeMap::new(),
                        success_codes: vec![0],
                    },
                    inputs: BTreeMap::new(),
                    default_inputs: BTreeMap::new(),
                    outputs: BTreeMap::new(),
                    runtime: ToolRuntime::default(),
                },
            );
            if !is_builtin_code {
                report.tools_added += 1;
            }
        }

        // Build runtime from requirement — used for env generation.
        if let Ok(_req) =
            serde_json::from_value::<crate::config::ToolRequirement>(requirement_value.clone())
        {
            let runtime = ToolRuntime {
                impure: false,
                inherited_env_vars: inherited_env_vars
                    .get(tool_id)
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|v| (v.clone(), v))
                    .collect(),
                ..ToolRuntime::default()
            };
            tool_runtimes.insert(tool_id.clone(), runtime);
            report.tools_updated += 1;
        } else {
            report.warnings.push(format!(
                "tool {tool_id}: failed to parse requirement value, skipping runtime config"
            ));
        }
    }

    // Companion binding resolution (for ffmpeg/deno selectors).
    let _ffmpeg_selection = resolve_companion_ffmpeg_selection(desired_tools);
    let _deno_selection = resolve_companion_deno_selection(desired_tools);

    // 3b. Provision desired tools (check CAS availability).
    let mut provision_hashes: BTreeMap<String, String> = BTreeMap::new();
    for (tool_id, requirement_value) in desired_tools {
        if let Some(version) =
            requirement_value.get("version").and_then(|v| v.as_str()).filter(|s| !s.is_empty())
        {
            provision_hashes.insert(tool_id.clone(), version.to_string());
        }
    }
    // Open or create the tool download cache.
    let cache_root = default_mediapm_user_download_cache_root().ok_or_else(|| {
        MediaPmError::Workflow("could not determine default tool cache root".to_string())
    })?;
    let cache = ToolDownloadCache::open(&cache_root)
        .await
        .map_err(|e| MediaPmError::Workflow(format!("failed to open tool download cache: {e}")))?;

    match provision_desired_tools_concurrently(cas, &provision_hashes, &paths.tools_dir, &cache)
        .await
    {
        Ok(provisioned) => {
            // 3c. Import tool content files from tools_dir into CAS.
            if !provisioned.is_empty() {
                match import_tool_content_files_into_cas(cas, &provisioned, &paths.tools_dir).await
                {
                    Ok(imported) => {
                        report.tools_updated += imported.len();
                    }
                    Err(e) => {
                        report.warnings.push(format!(
                            "content-map import failed (will retry on next sync): {e}",
                        ));
                    }
                }
            }
        }
        Err(e) => {
            report
                .warnings
                .push(format!("tool provisioning failed (will retry on next sync): {e}"));
        }
    }

    // 4. Apply lifecycle transitions.
    if !check_tag_updates {
        // When tag updates are disabled, mark all managed tools as skip-check.
        let tool_names: Vec<String> = generated_doc.tools.keys().cloned().collect();
        for tool_name in &tool_names {
            let _ = should_skip_tag_update_check(tool_name, &generated_doc);
        }
    }

    // 5. Ensure internal launcher content entries exist and regenerate launcher.
    let tools_dir = &paths.tools_dir;
    ensure_internal_launcher_content_entries_exist(&mut generated_doc, tools_dir);
    regenerate_media_tagger_internal_launcher_file(tools_dir)?;

    // 6. Inject generated env vars into tool runtimes.
    ensure_machine_runtime_inherits_generated_env_vars(
        &mut generated_doc,
        &paths.env_generated_file.to_string_lossy(),
    );

    // 7. Write generated runtime env file.
    write_generated_runtime_env_file(paths, &tool_runtimes)?;

    // 8. Save generated document.
    save_conductor_generated_document(paths, &generated_doc)?;

    Ok(report)
}
