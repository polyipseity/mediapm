//! Actor-backed orchestration runtime.

mod actors;
pub mod config;
mod coordinator;
mod node;
mod profiler;
mod protocol;

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use mediapm_cas::CasApi;
use tokio::sync::OnceCell;

use crate::api::{
    ConductorApi, RunSummary, RunWorkflowOptions, RuntimeDiagnostics, export_nickel_config_schemas,
    resolve_runtime_storage_paths,
};
use crate::error::ConductorError;
use crate::model::state::OrchestrationState;

pub use node::ConductorActorClient;
pub use node::spawn_conductor_actor;

/// Public conductor API facade backed by a lazily spawned ractor node.
pub struct SimpleConductor<C>
where
    C: CasApi + Send + Sync + 'static,
{
    cas: Arc<C>,
    actor_client: OnceCell<ConductorActorClient>,
}

impl<C> SimpleConductor<C>
where
    C: CasApi + Send + Sync + 'static,
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
}

#[async_trait]
impl<C> ConductorApi for SimpleConductor<C>
where
    C: CasApi + Send + Sync + 'static,
{
    async fn run_workflow_with_options(
        &self,
        user_ncl: &Path,
        machine_ncl: &Path,
        options: RunWorkflowOptions,
    ) -> Result<RunSummary, ConductorError> {
        let resolved_runtime_paths =
            resolve_runtime_storage_paths(user_ncl, machine_ncl, &options.runtime_storage_paths);
        export_nickel_config_schemas(&resolved_runtime_paths.conductor_schema_dir)?;
        let client = self.actor_client().await?;
        client.run_workflow(user_ncl, machine_ncl, options).await
    }

    async fn get_state(&self) -> Result<OrchestrationState, ConductorError> {
        let client = self.actor_client().await?;
        client.get_state().await
    }

    async fn get_runtime_diagnostics(&self) -> Result<RuntimeDiagnostics, ConductorError> {
        let client = self.actor_client().await?;
        client.get_runtime_diagnostics().await
    }
}

#[cfg(test)]
mod tests {
    use mediapm_cas::InMemoryCas;
    use tempfile::tempdir;

    use crate::api::ConductorApi;

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
}
