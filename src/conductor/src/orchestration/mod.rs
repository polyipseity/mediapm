//! Actor-backed orchestration runtime.

pub(crate) mod actors;
pub mod config;
mod coordinator;
mod node;
mod profiler;
mod protocol;

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use mediapm_cas::CasApi;
use mediapm_cas::CasMaintenanceApi;
use tokio::sync::OnceCell;

use crate::api::{
    ConductorApi, RunSummary, RunWorkflowOptions, RuntimeDiagnostics, StateMutationOptions,
    export_nickel_config_schemas,
};
use crate::error::ConductorError;
use crate::model::state::OrchestrationState;
use mediapm_cas::Hash;

pub use node::ConductorActorClient;
pub use node::spawn_conductor_actor;
pub use profiler::print_profile_timing;

/// Public conductor API facade backed by a lazily spawned ractor node.
pub struct SimpleConductor<C>
where
    C: CasApi + CasMaintenanceApi + Send + Sync + 'static,
{
    cas: Arc<C>,
    actor_client: OnceCell<ConductorActorClient>,
}

impl<C> SimpleConductor<C>
where
    C: CasApi + CasMaintenanceApi + Send + Sync + 'static,
{
    /// Creates an actor-backed conductor facade.
    #[must_use]
    pub fn new(cas: C) -> Self {
        Self { cas: Arc::new(cas), actor_client: OnceCell::new() }
    }

    async fn actor_client(&self) -> Result<&ConductorActorClient, ConductorError> {
        self.actor_client
            .get_or_try_init(|| async { node::spawn_conductor_actor(self.cas.clone()).await })
            .await
    }

    /// Runs instance GC with an optional TTL override.
    ///
    /// When `ttl_override` is `None`, the state store's configured TTL is used;
    /// if neither is set the call is a no-op.
    ///
    /// # Errors
    ///
    /// Delegates to the conductor actor; returns an error when RPC delivery
    /// or GC/persistence fails in the state store.
    pub async fn run_gc(&self, ttl_override: Option<u64>) -> Result<(), ConductorError> {
        let client = self.actor_client().await?;
        client.run_gc(ttl_override).await
    }
}

#[async_trait]
impl<C> ConductorApi for SimpleConductor<C>
where
    C: CasApi + CasMaintenanceApi + Send + Sync + 'static,
{
    async fn run_workflow_with_options(
        &self,
        user_ncl: &Path,
        machine_ncl: &Path,
        options: RunWorkflowOptions,
    ) -> Result<RunSummary, ConductorError> {
        // Clone state-relevant paths before consuming `options` in submit.
        let reload_storage_paths = options.runtime_storage_paths.clone();
        let reload_inherited_env_vars = options.runtime_inherited_env_vars.clone();
        let resolved_runtime_paths = reload_storage_paths.resolve_for(user_ncl, machine_ncl);
        export_nickel_config_schemas(&resolved_runtime_paths.conductor_schema_dir)?;
        let client = self.actor_client().await?;
        let handle_id = client.submit_workflow(user_ncl, machine_ncl, options).await?;
        let summary = client.wait_workflow(handle_id).await?;
        // Reload state into the main coordinator so subsequent get_state()/run_gc()
        // calls reflect the completed workflow's state changes.  This may fail
        // if the state file was cleaned up between persist and reload; log a
        // warning but don't fail the overall run.
        let state_options = StateMutationOptions {
            runtime_storage_paths: reload_storage_paths,
            runtime_inherited_env_vars: reload_inherited_env_vars,
        };
        if let Err(e) = client.load_resolved_state(user_ncl, machine_ncl, state_options).await {
            tracing::warn!("failed to reload coordinator state after workflow: {e}");
        }
        Ok(summary)
    }

    async fn submit_workflow(
        &self,
        user_ncl: &Path,
        machine_ncl: &Path,
        options: RunWorkflowOptions,
    ) -> Result<u64, ConductorError> {
        let resolved_runtime_paths =
            options.runtime_storage_paths.resolve_for(user_ncl, machine_ncl);
        export_nickel_config_schemas(&resolved_runtime_paths.conductor_schema_dir)?;
        let client = self.actor_client().await?;
        client.submit_workflow(user_ncl, machine_ncl, options).await
    }

    async fn poll_workflow(
        &self,
        handle_id: u64,
    ) -> Result<Option<Result<RunSummary, ConductorError>>, ConductorError> {
        let client = self.actor_client().await?;
        client.poll_workflow(handle_id).await
    }

    async fn get_state(&self) -> Result<OrchestrationState, ConductorError> {
        let client = self.actor_client().await?;
        client.get_state().await
    }

    async fn load_resolved_state(
        &self,
        user_ncl: &Path,
        machine_ncl: &Path,
        options: StateMutationOptions,
    ) -> Result<OrchestrationState, ConductorError> {
        let resolved_runtime_paths =
            options.runtime_storage_paths.resolve_for(user_ncl, machine_ncl);
        export_nickel_config_schemas(&resolved_runtime_paths.conductor_schema_dir)?;
        let client = self.actor_client().await?;
        client.load_resolved_state(user_ncl, machine_ncl, options).await
    }

    async fn replace_resolved_state(
        &self,
        user_ncl: &Path,
        machine_ncl: &Path,
        state: OrchestrationState,
        options: StateMutationOptions,
    ) -> Result<Hash, ConductorError> {
        let resolved_runtime_paths =
            options.runtime_storage_paths.resolve_for(user_ncl, machine_ncl);
        export_nickel_config_schemas(&resolved_runtime_paths.conductor_schema_dir)?;
        let client = self.actor_client().await?;
        client.replace_resolved_state(user_ncl, machine_ncl, state, options).await
    }

    async fn get_runtime_diagnostics(&self) -> Result<RuntimeDiagnostics, ConductorError> {
        let client = self.actor_client().await?;
        client.get_runtime_diagnostics().await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet, HashSet};

    use mediapm_cas::InMemoryCas;
    use tempfile::tempdir;

    use crate::api::{ConductorApi, StateMutationOptions};
    use crate::model::config::{
        OutputCaptureSpec, ToolKindSpec, ToolOutputSpec, ToolSpec, UserNickelDocument,
        encode_state_document, encode_user_document,
    };
    use crate::model::state::{OrchestrationState, OutputRef, PersistenceFlags, ToolCallInstance};

    use super::SimpleConductor;

    /// Ensures API-driven workflow execution exports conductor schemas to the
    /// resolved runtime root even when callers bypass the CLI entrypoint.
    #[tokio::test]
    async fn run_workflow_exports_schemas_for_default_runtime_root() {
        let root = tempdir().expect("tempdir");
        let conductor = SimpleConductor::new(InMemoryCas::new());
        let user_ncl = root.path().join("conductor.ncl");
        let machine_ncl = root.path().join("conductor.machine.ncl");

        let _summary = conductor.run_workflow(&user_ncl, &machine_ncl).await.expect("run");

        let schema_root = root.path().join(".conductor").join("config").join("conductor");
        assert!(schema_root.join("mod.ncl").exists());
        assert!(schema_root.join("v1.ncl").exists());
    }

    /// Ensures API state replacement updates volatile pointer + CAS state blob
    /// and can be loaded back through resolved-state APIs.
    #[tokio::test]
    async fn replace_and_load_resolved_state_roundtrip_via_public_api() {
        let root = tempdir().expect("tempdir");
        let conductor = SimpleConductor::new(InMemoryCas::new());
        let user_ncl = root.path().join("conductor.ncl");
        let machine_ncl = root.path().join("conductor.machine.ncl");
        let state_ncl = root.path().join(".conductor").join("state.ncl");

        let user_document = UserNickelDocument {
            tools: BTreeMap::from([(
                "echo@1.0.0".to_string(),
                ToolSpec {
                    is_impure: false,
                    inputs: BTreeMap::new(),
                    kind: ToolKindSpec::Builtin {
                        name: "echo".to_string(),
                        version: "1.0.0".to_string(),
                    },
                    outputs: BTreeMap::from([(
                        "result".to_string(),
                        ToolOutputSpec {
                            capture: OutputCaptureSpec::Stdout {},
                            allow_empty: false,
                        },
                    )]),
                },
            )]),
            ..UserNickelDocument::default()
        };
        std::fs::write(&user_ncl, encode_user_document(user_document).expect("encode user"))
            .expect("write user");
        std::fs::create_dir_all(state_ncl.parent().expect("state parent"))
            .expect("create state parent");
        std::fs::write(
            &state_ncl,
            encode_state_document(crate::model::config::StateNickelDocument::default())
                .expect("encode state"),
        )
        .expect("write state");

        let next_state = OrchestrationState {
            version: OrchestrationState::default().version,
            external_data: BTreeMap::new(),
            instances: BTreeMap::from([(
                "instance-a".to_string(),
                ToolCallInstance {
                    tool_name: "echo@1.0.0".to_string(),
                    metadata: ToolSpec {
                        is_impure: false,
                        inputs: BTreeMap::new(),
                        kind: ToolKindSpec::Builtin {
                            name: "echo".to_string(),
                            version: "1.0.0".to_string(),
                        },
                        outputs: BTreeMap::new(),
                    },
                    impure_timestamp: None,
                    inputs: BTreeMap::new(),
                    outputs: BTreeMap::from([(
                        "result".to_string(),
                        OutputRef {
                            hash: mediapm_cas::Hash::from_content(b"api-roundtrip"),
                            persistence: PersistenceFlags::default(),
                            allow_empty_capture: false,
                        },
                    )]),
                },
            )]),
            aux: BTreeMap::new(),
            instance_blob_hashes: BTreeSet::new(),
            referenced_instance_keys: HashSet::new(),
        };

        let pointer = conductor
            .replace_resolved_state(
                &user_ncl,
                &machine_ncl,
                next_state.clone(),
                StateMutationOptions::default(),
            )
            .await
            .expect("replace state");
        assert!(pointer.to_string().starts_with("blake3:"));

        let loaded = conductor
            .load_resolved_state(&user_ncl, &machine_ncl, StateMutationOptions::default())
            .await
            .expect("load state");
        // Only assert instance-round-trip fields — the background GC fires
        // concurrently and mutates `aux` (sets `last_unreachable`), so ignore
        // it for equality.
        assert_eq!(loaded.version, next_state.version, "version mismatch");
        assert_eq!(loaded.instances, next_state.instances, "instances mismatch");
        assert_eq!(
            loaded.referenced_instance_keys, next_state.referenced_instance_keys,
            "referenced_instance_keys mismatch",
        );
    }

    /// Ensures public API state replacement validates instances against merged
    /// tool catalog and rejects unknown tool references.
    #[tokio::test]
    async fn replace_resolved_state_rejects_unknown_tool_via_public_api() {
        let root = tempdir().expect("tempdir");
        let conductor = SimpleConductor::new(InMemoryCas::new());
        let user_ncl = root.path().join("conductor.ncl");
        let machine_ncl = root.path().join("conductor.machine.ncl");

        std::fs::write(
            &user_ncl,
            encode_user_document(UserNickelDocument::default()).expect("encode user"),
        )
        .expect("write user");

        let invalid = OrchestrationState {
            version: OrchestrationState::default().version,
            external_data: BTreeMap::new(),
            instances: BTreeMap::from([(
                "instance-a".to_string(),
                ToolCallInstance {
                    tool_name: "missing@1.0.0".to_string(),
                    metadata: ToolSpec::default(),
                    impure_timestamp: None,
                    inputs: BTreeMap::new(),
                    outputs: BTreeMap::new(),
                },
            )]),
            aux: BTreeMap::new(),
            instance_blob_hashes: BTreeSet::new(),
            referenced_instance_keys: HashSet::new(),
        };

        let error = conductor
            .replace_resolved_state(
                &user_ncl,
                &machine_ncl,
                invalid,
                StateMutationOptions::default(),
            )
            .await
            .expect_err("unknown tool should fail validation");
        assert!(error.to_string().contains("references unknown tool"));
    }
}
