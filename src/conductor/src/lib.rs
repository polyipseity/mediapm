//! Phase 2 Conductor orchestration contracts.
//!
//! This crate models deterministic workflow execution state and the merge logic
//! for persistence flags. It is designed as the functional orchestration layer
//! that coordinates tool call instances over CAS-managed content.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use async_trait::async_trait;
use mediapm_cas::{CasApi, Hash};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// User/machine merged persistence flags for one output.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistenceFlags {
    /// When false for all references, output can be dropped from CAS.
    pub save: bool,
    /// When true for any reference, output should remain full-data preferred.
    pub force_full: bool,
}

impl Default for PersistenceFlags {
    fn default() -> Self {
        Self { save: true, force_full: false }
    }
}

/// Merges persistence flags from multiple equivalent tool-call references.
pub fn merge_persistence_flags(
    flags: impl IntoIterator<Item = PersistenceFlags>,
) -> PersistenceFlags {
    let mut merged = PersistenceFlags::default();

    for flag in flags {
        merged.save = merged.save && flag.save;
        merged.force_full = merged.force_full || flag.force_full;
    }

    merged
}

/// Minimal tool metadata participating in instance-key derivation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolMetadata {
    /// Logical tool name (for example `ffmpeg`).
    pub name: String,
    /// Versioned tool identity (for example `ffmpeg@<commit>`).
    pub version: String,
    /// Whether tool behavior is impure and may require timestamp injection.
    pub is_impure: bool,
}

/// Fully resolved input vector for deterministic instance keys.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedInput {
    /// Plain content used by the tool invocation.
    pub plain_content: Vec<u8>,
    /// Optional source hash if input references pre-existing CAS content.
    pub source_hash: Option<Hash>,
}

/// Output map entry for an executed instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputRef {
    /// CAS hash for this output value.
    pub hash: Hash,
    /// Effective merged persistence policy for this output.
    pub persistence: PersistenceFlags,
}

/// State record for one deterministic tool call instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCallInstance {
    /// Metadata used to identify tool code/semantics.
    pub metadata: ToolMetadata,
    /// Resolved inputs participating in cache identity.
    pub inputs: HashMap<String, ResolvedInput>,
    /// Captured output CAS refs and effective persistence policies.
    pub outputs: HashMap<String, OutputRef>,
}

/// Immutable-orchestration-state payload pointer target.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrchestrationState {
    /// Deterministic instance table keyed by derived instance key.
    pub instances: HashMap<String, ToolCallInstance>,
}

/// Summary of one workflow run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunSummary {
    /// Number of instances that were executed or newly materialized.
    pub executed_instances: usize,
}

/// Conductor-level error category.
#[derive(Debug, Error)]
pub enum ConductorError {
    /// Workflow references could not be loaded or interpreted.
    #[error("workflow file error: {0}")]
    Workflow(String),
    /// CAS operation failed while materializing state.
    #[error("cas operation failed: {0}")]
    Cas(String),
    /// Internal synchronization error.
    #[error("internal conductor error: {0}")]
    Internal(String),
}

/// Async API contract for Phase 2 conductor.
#[async_trait]
pub trait ConductorApi: Send + Sync {
    /// Executes a workflow using user and machine config inputs.
    async fn run_workflow(
        &self,
        user_cue: &Path,
        machine_cue: &Path,
    ) -> Result<RunSummary, ConductorError>;

    /// Returns the current orchestration state snapshot.
    async fn get_state(&self) -> Result<OrchestrationState, ConductorError>;
}

/// Minimal stateful conductor that demonstrates deterministic keying and state writes.
pub struct SimpleConductor<C>
where
    C: CasApi,
{
    cas: C,
    state: RwLock<OrchestrationState>,
}

impl<C> SimpleConductor<C>
where
    C: CasApi,
{
    /// Creates a conductor backed by the provided CAS implementation.
    pub fn new(cas: C) -> Self {
        Self { cas, state: RwLock::new(OrchestrationState::default()) }
    }

    fn derive_instance_key(
        metadata: &ToolMetadata,
        inputs: &HashMap<String, ResolvedInput>,
    ) -> String {
        let mut hasher = blake3::Hasher::new();
        hasher.update(metadata.name.as_bytes());
        hasher.update(metadata.version.as_bytes());
        hasher.update(&[metadata.is_impure as u8]);

        let mut keys: Vec<&String> = inputs.keys().collect();
        keys.sort();

        for key in keys {
            hasher.update(key.as_bytes());
            if let Some(input) = inputs.get(key) {
                hasher.update(&input.plain_content);
                if let Some(source_hash) = input.source_hash {
                    hasher.update(source_hash.as_bytes());
                }
            }
        }

        hasher.finalize().to_hex().to_string()
    }
}

#[async_trait]
impl<C> ConductorApi for SimpleConductor<C>
where
    C: CasApi,
{
    async fn run_workflow(
        &self,
        user_cue: &Path,
        machine_cue: &Path,
    ) -> Result<RunSummary, ConductorError> {
        if user_cue.as_os_str().is_empty() || machine_cue.as_os_str().is_empty() {
            return Err(ConductorError::Workflow(
                "user.cue and machine.cue paths must be non-empty".to_string(),
            ));
        }

        let metadata = ToolMetadata {
            name: "workflow-placeholder".to_string(),
            version: "0.1.0".to_string(),
            is_impure: false,
        };

        let mut inputs = HashMap::new();
        inputs.insert(
            "workflow_hint".to_string(),
            ResolvedInput {
                plain_content: user_cue.as_os_str().to_string_lossy().as_bytes().to_vec(),
                source_hash: None,
            },
        );

        let key = Self::derive_instance_key(&metadata, &inputs);
        let output_hash = self
            .cas
            .put(user_cue.as_os_str().to_string_lossy().as_bytes().to_vec().into())
            .await
            .map_err(|err| ConductorError::Cas(err.to_string()))?;

        let mut state =
            self.state.write().map_err(|err| ConductorError::Internal(err.to_string()))?;
        state.instances.insert(
            key,
            ToolCallInstance {
                metadata,
                inputs,
                outputs: HashMap::from([(
                    "result".to_string(),
                    OutputRef { hash: output_hash, persistence: PersistenceFlags::default() },
                )]),
            },
        );

        Ok(RunSummary { executed_instances: 1 })
    }

    async fn get_state(&self) -> Result<OrchestrationState, ConductorError> {
        let state = self.state.read().map_err(|err| ConductorError::Internal(err.to_string()))?;
        Ok(state.clone())
    }
}

/// Canonical default state paths for Phase 2.
pub fn default_state_paths() -> (PathBuf, PathBuf) {
    (PathBuf::from("user.cue"), PathBuf::from("machine.cue"))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use mediapm_cas::InMemoryCas;

    use crate::{
        ConductorApi, PersistenceFlags, ResolvedInput, SimpleConductor, ToolMetadata,
        merge_persistence_flags,
    };

    #[test]
    fn persistence_flags_follow_intersection_and_union_rules() {
        let merged = merge_persistence_flags([
            PersistenceFlags { save: true, force_full: false },
            PersistenceFlags { save: false, force_full: false },
            PersistenceFlags { save: true, force_full: true },
        ]);

        assert!(!merged.save);
        assert!(merged.force_full);
    }

    #[tokio::test]
    async fn workflow_execution_populates_state() {
        let conductor = SimpleConductor::new(InMemoryCas::new());
        let summary = conductor
            .run_workflow("user.cue".as_ref(), "machine.cue".as_ref())
            .await
            .expect("workflow should execute");

        assert_eq!(summary.executed_instances, 1);

        let state = conductor.get_state().await.expect("state should load");
        assert_eq!(state.instances.len(), 1);
    }

    #[test]
    fn derived_keys_are_deterministic() {
        let metadata =
            ToolMetadata { name: "tool".to_string(), version: "v1".to_string(), is_impure: false };
        let inputs = HashMap::from([(
            "input".to_string(),
            ResolvedInput { plain_content: b"abc".to_vec(), source_hash: None },
        )]);

        let key_a = super::SimpleConductor::<InMemoryCas>::derive_instance_key(&metadata, &inputs);
        let key_b = super::SimpleConductor::<InMemoryCas>::derive_instance_key(&metadata, &inputs);

        assert_eq!(key_a, key_b);
    }
}
