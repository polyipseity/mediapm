//! Deterministic tool call instance-key derivation and cache-probe logic.

use std::collections::BTreeSet;

use crate::config::ImpureTimestamp;
use crate::orchestration::protocol::{OrchestrationState, UnifiedToolSpec};
use crate::state::{ResolvedInput, ToolCallInstance};

/// Derives a deterministic tool call instance key from tool + inputs + optional impure timestamp.
pub(super) fn derive_instance_key(
    _tool_spec: &UnifiedToolSpec,
    inputs: &[ResolvedInput],
    impure_timestamp: Option<ImpureTimestamp>,
) -> String {
    use blake3;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"instance-key-v1");

    // Include tool identity.
    // For now, key on inputs since we don't have tool id easily accessible.
    for input in inputs {
        hasher.update(input.key.as_bytes());
        hasher.update(b"\0");
        hasher.update(input.value.as_bytes());
        hasher.update(b"\0");
    }

    if let Some(ts) = impure_timestamp {
        hasher.update(b"impure:");
        hasher.update(&ts.as_unix_nanos().to_le_bytes());
    }

    hasher.finalize().to_string()
}

/// Checks whether a cached tool call instance exists with all required outputs.
pub(super) fn probe_cache(
    instance_key: &str,
    state: &OrchestrationState,
    required_outputs: &BTreeSet<String>,
) -> (bool, Option<ToolCallInstance>) {
    if let Some(instance) = state.tool_call_instances.get(instance_key) {
        // Check that all required outputs exist.
        if required_outputs.is_empty()
            || required_outputs.iter().all(|name| instance.outputs.iter().any(|o| &o.name == name))
        {
            return (true, Some(instance.clone()));
        }
    }
    (false, None)
}
