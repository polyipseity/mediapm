//! Tool-call instance-key helpers shared by step execution and mediapm
//! materialization matching.

use std::collections::BTreeMap;

use mediapm_cas::Hash;

use crate::config::{InputBinding, ToolSpec};
use crate::orchestration::step_worker::template::is_deterministic_command_part;
use crate::state::{ResolvedInput, ToolCallInstance};

/// Returns whether a binding source participates in instance-key material,
/// matching step-worker `is_deterministic_command_part` semantics.
#[must_use]
pub fn is_deterministic_binding_source(raw_binding_source: &str) -> bool {
    is_deterministic_command_part(raw_binding_source)
}

/// Encodes one tool default binding as the string form used in step inputs.
#[must_use]
pub fn input_binding_to_string(binding: &InputBinding) -> String {
    match binding {
        InputBinding::String(value) => value.clone(),
        InputBinding::Vec(values) => serde_json::to_string(values).unwrap_or_default(),
    }
}

/// Unions step-declared bindings with the tool spec contract, matching
/// conductor `resolve_step_inputs` key collection.
#[must_use]
pub fn merge_step_input_bindings(
    step_inputs: &BTreeMap<String, String>,
    tool_spec: &ToolSpec,
) -> BTreeMap<String, String> {
    let mut merged = BTreeMap::new();
    for key in tool_spec.inputs.keys() {
        let value = step_inputs
            .get(key)
            .cloned()
            .or_else(|| tool_spec.default_inputs.get(key).map(input_binding_to_string))
            .unwrap_or_default();
        merged.insert(key.clone(), value);
    }
    for (key, value) in step_inputs {
        merged.insert(key.clone(), value.clone());
    }
    merged
}

/// Resolved value bytes for a hash-backed binding (`step_output` / `external_data`
/// with CAS unavailable during step-input resolution).
#[must_use]
pub fn instance_key_resolved_value_bytes_for_hash_reference(hash: Hash) -> Vec<u8> {
    hash.to_string().into_bytes()
}

/// Resolved value bytes for a literal binding (no template expansion).
#[must_use]
pub fn instance_key_resolved_value_bytes_for_literal_binding(raw_binding_source: &str) -> Vec<u8> {
    raw_binding_source.as_bytes().to_vec()
}

/// Maps one resolved binding value to its instance-key hash when the raw
/// binding source is deterministic.
#[must_use]
pub fn instance_key_hash_from_resolved_value_bytes(
    raw_binding_source: &str,
    resolved_value: &[u8],
) -> Option<Hash> {
    is_deterministic_binding_source(raw_binding_source)
        .then_some(Hash::from_content(resolved_value))
}

/// Collects deterministic input hashes from resolved step inputs, matching
/// step-worker instance-key construction.
#[must_use]
pub fn deterministic_input_hashes_from_resolved_inputs(
    resolved_inputs: &[ResolvedInput],
    step_inputs: &BTreeMap<String, String>,
    default_inputs: &BTreeMap<String, InputBinding>,
) -> Vec<Hash> {
    let mut input_hashes = Vec::new();
    for resolved_input in resolved_inputs {
        let raw_source = step_inputs
            .get(&resolved_input.key)
            .cloned()
            .or_else(|| default_inputs.get(&resolved_input.key).map(input_binding_to_string))
            .unwrap_or_default();
        if let Some(hash) = instance_key_hash_from_resolved_value_bytes(
            &raw_source,
            resolved_input.value.as_bytes(),
        ) {
            input_hashes.push(hash);
        }
    }
    input_hashes
}

/// Collects deterministic input hashes from merged bindings and resolved value
/// bytes (mediapm materializer path).
#[must_use]
pub fn deterministic_input_hashes_from_resolved_value_bytes(
    merged_bindings: &BTreeMap<String, String>,
    resolved_values: &BTreeMap<String, Vec<u8>>,
) -> Vec<Hash> {
    let mut input_hashes = Vec::new();
    for (key, raw_source) in merged_bindings {
        let Some(value_bytes) = resolved_values.get(key) else {
            continue;
        };
        if let Some(hash) = instance_key_hash_from_resolved_value_bytes(raw_source, value_bytes) {
            input_hashes.push(hash);
        }
    }
    input_hashes
}

/// Returns materialized-input hashes in tool content-map key order.
#[must_use]
pub fn deterministic_materialized_input_hashes(instance: &ToolCallInstance) -> Vec<Hash> {
    instance.materialized_inputs.values().map(|record| record.hash).collect()
}

/// Returns true when `instance.instance_key` matches re-derived key material.
#[must_use]
pub fn instance_matches_stored_key(
    instance: &ToolCallInstance,
    deterministic_input_hashes: &[Hash],
    deterministic_materialized_input_hashes: &[Hash],
) -> bool {
    let executed_at_nanos = if instance.impure { instance.executed_at.as_unix_nanos() } else { 0 };
    let derived = crate::state::derive_tool_call_instance_key(
        &instance.tool_call_id,
        instance.impure,
        executed_at_nanos,
        deterministic_input_hashes,
        &[],
        deterministic_materialized_input_hashes,
    );
    derived == instance.instance_key
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_reference_binding_uses_hash_string_bytes() {
        let hash = Hash::from_content(b"referenced-bytes");
        let bytes = instance_key_resolved_value_bytes_for_hash_reference(hash);
        assert_eq!(bytes, hash.to_string().as_bytes());
        let key_hash =
            instance_key_hash_from_resolved_value_bytes("${step_output.step-0.primary}", &bytes)
                .expect("hash reference binding is deterministic");
        assert_eq!(key_hash, Hash::from_content(hash.to_string().as_bytes()));
    }

    #[test]
    fn literal_binding_uses_raw_source_bytes() {
        let raw = "mp4";
        let bytes = instance_key_resolved_value_bytes_for_literal_binding(raw);
        let key_hash = instance_key_hash_from_resolved_value_bytes(raw, &bytes)
            .expect("literal is deterministic");
        assert_eq!(key_hash, Hash::from_content(raw.as_bytes()));
    }

    #[test]
    fn env_binding_is_not_instance_key_material() {
        assert!(!is_deterministic_binding_source("${env.HOME}"));
        assert!(instance_key_hash_from_resolved_value_bytes("${env.HOME}", b"/tmp").is_none());
    }
}
