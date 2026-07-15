//! Actor-backed step execution.
//!
//! Step workers own the expensive, side-effecting portion of orchestration:
//! resolving inputs, materializing sandbox files, running processes, and
//! capturing declared outputs.  The coordinator interacts with them through
//! deterministic request/response messages while state merging stays separate.

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use mediapm_cas::CasApi;
use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort};

use crate::error::ConductorError;
use crate::orchestration::protocol::{StepExecutionBundle, StepExecutionRequest};

mod cache;
mod capture;
mod executor;
mod inputs;
mod process;
mod sandbox;
pub(crate) mod template;

// ---------------------------------------------------------------------------
// Message types
// ---------------------------------------------------------------------------

/// Messages sent to step-worker actors.
#[derive(Debug)]
pub(crate) enum StepWorkerMessage {
    /// Execute one planned step request and return the merge-ready bundle.
    ExecuteStep(
        Box<StepExecutionRequest>,
        RpcReplyPort<Result<StepExecutionBundle, ConductorError>>,
    ),
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default timeout for external executable subprocesses (15 minutes).
const DEFAULT_EXECUTABLE_TIMEOUT: Duration = Duration::from_mins(15);

/// Environment variable override for executable timeout in seconds.
const EXECUTABLE_TIMEOUT_ENV_VAR: &str = "MEDIAPM_CONDUCTOR_EXECUTABLE_TIMEOUT_SECS";

/// Returns the effective executable timeout from env or default.
fn executable_timeout() -> Duration {
    let secs = std::env::var(EXECUTABLE_TIMEOUT_ENV_VAR)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    if secs > 0 { Duration::from_secs(secs) } else { DEFAULT_EXECUTABLE_TIMEOUT }
}

// ---------------------------------------------------------------------------
// Actor types
// ---------------------------------------------------------------------------

/// Actor marker for one step worker.
#[derive(Debug, Clone, Copy)]
struct StepWorkerActor<C>(PhantomData<C>);

impl<C> Default for StepWorkerActor<C> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

/// Runtime state for one worker actor.
struct StepWorkerState<C: CasApi + Send + Sync> {
    /// Shared CAS handle for content I/O.
    cas: Arc<C>,
}

impl<C: CasApi + Send + Sync + 'static> Actor for StepWorkerActor<C> {
    type Msg = StepWorkerMessage;
    type State = StepWorkerState<C>;
    type Arguments = StepWorkerState<C>;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(args)
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            StepWorkerMessage::ExecuteStep(request, reply) => {
                let result = executor::execute_step(&*state.cas, *request).await;
                let _ = reply.send(result);
            }
        }
        Ok(())
    }
}

/// Spawns a pool of step-worker actors.
pub(crate) async fn spawn_step_worker_pool<C: CasApi + Send + Sync + 'static>(
    cas: Arc<C>,
    pool_size: usize,
) -> Result<Vec<ActorRef<StepWorkerMessage>>, ConductorError> {
    let mut workers = Vec::with_capacity(pool_size);
    for _ in 0..pool_size {
        let state = StepWorkerState { cas: cas.clone() };
        let (actor_ref, _handle) = ractor::spawn::<StepWorkerActor<C>>(state)
            .await
            .map_err(|e| ConductorError::Internal(format!("failed to spawn step worker: {e}")))?;
        workers.push(actor_ref);
    }
    Ok(workers)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ImpureTimestamp;
    use crate::orchestration::protocol::{OrchestrationState, StepOutputs};
    use crate::state::{OutputRef, OutputSaveMode, ResolvedInput, ToolCallInstance};
    use mediapm_cas::Hash;
    use std::collections::{BTreeMap, BTreeSet};

    /// Verifies `resolve_step_output_refs` resolves valid references.
    #[test]
    fn resolve_step_output_refs_resolves_references() {
        let mut step_outputs = StepOutputs::new();
        step_outputs.insert("step-1".to_string(), {
            let mut m = BTreeMap::new();
            m.insert("result".to_string(), Hash::from_content(b"hello"));
            m
        });

        let result =
            inputs::resolve_step_output_refs("${step_output.step-1.result}", &step_outputs)
                .expect("should resolve");

        let expected = Hash::from_content(b"hello").to_string();
        assert_eq!(result, expected);
    }

    /// Verifies `resolve_step_output_refs` errors on missing reference.
    #[test]
    fn resolve_step_output_refs_errors_on_missing() {
        let step_outputs = StepOutputs::new();
        let result =
            inputs::resolve_step_output_refs("${step_output.missing-step.output}", &step_outputs);
        assert!(result.is_err());
    }

    /// Verifies `derive_instance_key` produces deterministic keys.
    #[test]
    fn derive_instance_key_is_deterministic() {
        let inputs = vec![ResolvedInput { key: "message".to_string(), value: "hello".to_string() }];

        let key1 = cache::derive_instance_key("test_tool", &inputs, None);
        let key2 = cache::derive_instance_key("test_tool", &inputs, None);
        assert_eq!(key1, key2);
    }

    /// Verifies `derive_instance_key` varies with impure timestamp.
    #[test]
    fn derive_instance_key_varies_with_impure_timestamp() {
        let inputs = vec![ResolvedInput { key: "message".to_string(), value: "hello".to_string() }];

        let ts1 = ImpureTimestamp::from_unix_nanos(1000);
        let ts2 = ImpureTimestamp::from_unix_nanos(2000);
        let key1 = cache::derive_instance_key("test_tool", &inputs, Some(ts1));
        let key2 = cache::derive_instance_key("test_tool", &inputs, Some(ts2));
        assert_ne!(key1, key2);
    }

    /// Verifies `probe_cache` finds cached instances.
    #[test]
    fn probe_cache_finds_existing_instance() {
        let instance_key = "test-key".to_string();
        let instance = ToolCallInstance {
            instance_key: instance_key.clone(),
            tool_id: "echo@v1".to_string(),
            inputs: Vec::new(),
            outputs: Vec::new(),
            worker_index: 0,
            executed: true,
            rematerialized: false,
            conductor_gc_last_referenced_at: ImpureTimestamp::default(),
        };

        let mut state = OrchestrationState::new_empty();
        state.tool_call_instances.insert(instance_key.clone(), instance);

        let (hit, _) = cache::probe_cache(&instance_key, &state, &BTreeSet::new());
        assert!(hit);
    }

    /// Verifies `probe_cache` misses on unknown key.
    #[test]
    fn probe_cache_misses_on_unknown_key() {
        let state = OrchestrationState::new_empty();
        let (hit, _) = cache::probe_cache("unknown-key", &state, &BTreeSet::new());
        assert!(!hit);
    }

    /// Verifies `probe_cache` checks required outputs.
    #[test]
    fn probe_cache_checks_required_outputs() {
        let instance_key = "test-key".to_string();
        let instance = ToolCallInstance {
            instance_key: instance_key.clone(),
            tool_id: "echo@v1".to_string(),
            inputs: Vec::new(),
            outputs: vec![OutputRef {
                name: "result".to_string(),
                hash: Hash::from_content(b"data"),
                save_mode: OutputSaveMode::Saved,
            }],
            worker_index: 0,
            executed: true,
            rematerialized: false,
            conductor_gc_last_referenced_at: ImpureTimestamp::default(),
        };

        let mut state = OrchestrationState::new_empty();
        state.tool_call_instances.insert(instance_key.clone(), instance);

        // Required output exists.
        let required: BTreeSet<String> = ["result".to_string()].into();
        let (hit, _) = cache::probe_cache(&instance_key, &state, &required);
        assert!(hit);

        // Required output missing.
        let missing: BTreeSet<String> = ["nonexistent".to_string()].into();
        let (hit, _) = cache::probe_cache(&instance_key, &state, &missing);
        assert!(!hit);
    }

    /// Verifies `sanitize_for_path` replaces special characters.
    #[test]
    fn sanitize_for_path_replaces_special_chars() {
        assert_eq!(sandbox::sanitize_for_path("hello/world:test"), "hello_world_test");
        assert_eq!(sandbox::sanitize_for_path("simple"), "simple");
    }

    /// Verifies `executable_timeout` returns default when env is unset.
    #[test]
    fn executable_timeout_default_when_unset() {
        // Safety: not thread-safe but fine in single-threaded test.
        unsafe {
            std::env::remove_var(EXECUTABLE_TIMEOUT_ENV_VAR);
        }
        let timeout = executable_timeout();
        assert_eq!(timeout, DEFAULT_EXECUTABLE_TIMEOUT);
    }
}
