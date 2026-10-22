//! Tool-call instance-key helpers shared by step execution and mediapm
//! materialization matching.
//!
//! This module owns the instance-key scheme: its integer version, the domain
//! separator that version renders, and the derivation itself. Instance
//! identity is runtime data, so it does not live in `state/versions/`, whose
//! modules own wire formats alone.

use std::collections::BTreeMap;

use mediapm_cas::Hash;

use crate::config::{InputBinding, ToolSpec};
use crate::orchestration::step_worker::template::is_deterministic_command_part;
use crate::state::{ResolvedInput, ToolCallInstance};

/// Version of the tool-call instance key scheme.
///
/// The scheme is versioned by an integer and only ever moves forward:
/// `v1 -> v2 -> v3 -> v4`. A change to the key material bumps this number; it
/// never appends a revision such as `v4.1`, because the point of the bump is to
/// be visible. Old persisted keys simply miss once and re-execute.
pub const INSTANCE_KEY_SCHEME: u32 = 4;

/// Domain separator for tool-call instance keys, carrying the scheme version.
#[must_use]
pub fn instance_key_domain() -> String {
    format!("mediapm/conductor/tool call instances/v{INSTANCE_KEY_SCHEME}")
}

/// Derives the content-addressed key of one tool-call instance.
///
/// The key answers whether this work has been done before, from material any
/// party can re-derive: the tool call identity, the deterministic argument
/// hashes and the materialized tool-content hashes. Captures are **not** key
/// material. They are records addressed by name, so two steps that run the same
/// work share one key however differently they name what they record, and each
/// step reads the names it binds out of the instance it found. An entry records
/// the spec that produced it, which is what lets a step tell whether the bytes
/// it finds were produced the way it would produce them; the key cannot answer
/// that question and does not try.
///
/// The composite is exactly those four terms, in this order: the scheme domain
/// separator ([`instance_key_domain`]), the tool call id, the command-arg
/// hashes in argv order, and the materialized-input hashes in
/// tool-content-map path order. Nothing here sorts, so the order is key
/// material and the caller owes it:
/// [`deterministic_materialized_input_hashes`] reads a `BTreeMap` and is
/// path-ordered already.
///
/// The environment term earlier schemes carried is gone. The executor records
/// every env value as nondeterministic and therefore passes no env hashes, so
/// that term was empty by construction on every path and keyed nothing; it
/// existed only to keep two derivations looking symmetric.
///
/// What this key omits is answered by the records it addresses, not by the key.
/// A binding whose source is excluded as nondeterministic — anything reading
/// `${env.…}`, `${context.…}`, `:file(…)` or `:folder(…)` — can resolve to
/// different bytes for the same key, so two executions of one instance may
/// disagree. That is deliberate and errs toward reuse: the stored entry carries
/// its capture spec, and the settlement decides whether a differing record is
/// divergence or the expected output of impure work. Adding such a binding to
/// the key instead would make every value it can take a new instance, so a
/// single tool would re-execute once per environment it is run under.
#[must_use]
pub fn derive_instance_key(
    tool_call_id: &str,
    deterministic_command_args: &[Hash],
    deterministic_materialized_inputs: &[Hash],
) -> Hash {
    let mut parts = Vec::with_capacity(
        2 + deterministic_command_args.len() + deterministic_materialized_inputs.len(),
    );
    parts.push(Hash::from_content(instance_key_domain().as_bytes()));
    parts.push(Hash::from_content(tool_call_id.as_bytes()));
    parts.extend_from_slice(deterministic_command_args);
    parts.extend_from_slice(deterministic_materialized_inputs);
    Hash::composite(&parts)
}

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

/// Maps one resolved binding to its instance-key hash when the raw binding
/// source is deterministic.
///
/// The hash covers the input **name** as well as the resolved bytes: the name
/// decides which slot the value occupies in the command, so two bindings that
/// resolve to the same bytes under different names must land in different keys.
/// Names are stable per step — they come from the tool's declared input
/// contract unioned with the step's own bindings — so keying them cannot split
/// one step's own cache across runs.
#[must_use]
pub fn instance_key_hash_from_resolved_value_bytes(
    key: &str,
    raw_binding_source: &str,
    resolved_value: &[u8],
) -> Option<Hash> {
    is_deterministic_binding_source(raw_binding_source).then(|| {
        Hash::composite(&[Hash::from_content(key.as_bytes()), Hash::from_content(resolved_value)])
    })
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
            &resolved_input.key,
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
        if let Some(hash) =
            instance_key_hash_from_resolved_value_bytes(key, raw_source, value_bytes)
        {
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
///
/// The derivation is content-only (tool id, deterministic input hashes,
/// materialized-input hashes), so an impure instance's stored key keeps
/// matching no matter how much wall-clock time separates the recording from the
/// check, and a step's capture declarations cannot move it.
#[must_use]
pub fn instance_matches_stored_key(
    instance: &ToolCallInstance,
    deterministic_input_hashes: &[Hash],
    deterministic_materialized_input_hashes: &[Hash],
) -> bool {
    let derived = crate::state::derive_tool_call_instance_key(
        &instance.tool_call_id,
        deterministic_input_hashes,
        deterministic_materialized_input_hashes,
    );
    derived == instance.instance_key
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deriving twice from identical material yields identical keys.
    #[test]
    fn derive_instance_key_is_deterministic() {
        let a = derive_instance_key("echo@v1", &[], &[]);
        let b = derive_instance_key("echo@v1", &[], &[]);
        assert_eq!(a, b);
    }

    /// Time never participates in any instance key, impure or pure.
    ///
    /// Earlier key schemes embedded `executed_at` for impure steps, which made
    /// every run mint a fresh instance and re-execute the step. The timestamp
    /// parameter is gone from the signature, so time-independence is
    /// compile-enforced at this level; the behavioral pin is
    /// `an_immediate_second_sync_executes_nothing_and_rematerializes_nothing`.
    #[test]
    fn impure_keys_ignore_time() {
        let args = [Hash::from_content(b"arg")];
        assert_eq!(
            derive_instance_key("tool", &args, &[]),
            derive_instance_key("tool", &args, &[]),
            "identical material must derive identical keys regardless of wall-clock time",
        );
    }

    /// The versioned domain prefix participates in the key.
    ///
    /// The expected key is composed by hand, `instance_key_domain()`'s bytes
    /// included, and the same composite without that term must differ, so the
    /// test fails if the domain term is dropped or its bytes change.
    #[test]
    fn derive_instance_key_domain_prefix() {
        let args = [Hash::from_content(b"arg")];
        let with_domain = derive_instance_key("tool", &args, &[]);
        let expected = Hash::composite(&[
            Hash::from_content(instance_key_domain().as_bytes()),
            Hash::from_content(b"tool"),
            args[0],
        ]);
        let without_domain = Hash::composite(&[Hash::from_content(b"tool"), args[0]]);
        assert_eq!(
            with_domain, expected,
            "the key must be the composition that includes the domain"
        );
        assert_ne!(with_domain, without_domain, "dropping the domain term must move the key");
    }

    /// Argument order matters, and materialized-input hashes participate.
    #[test]
    fn derive_instance_key_args_and_inputs_ordered() {
        let a = Hash::from_content(b"a");
        let b = Hash::from_content(b"b");
        assert_ne!(
            derive_instance_key("t", &[a, b], &[]),
            derive_instance_key("t", &[b, a], &[]),
            "command arg order must matter",
        );
        assert_ne!(
            derive_instance_key("t", &[], &[]),
            derive_instance_key("t", &[], &[b]),
            "deterministic materialized inputs must participate",
        );
    }

    /// The key is exactly `composite(domain, id, args, inputs)`.
    ///
    /// Pinning the exact component list guards all of it: adding a component,
    /// reintroducing a capture or env term, or reintroducing a time term breaks
    /// this test. Captures in particular must never come back — a key that
    /// covers them makes identical work derive different keys, which is the
    /// split the shared-instance model exists to remove.
    #[test]
    fn derive_instance_key_composite_components_exact() {
        let arg = Hash::from_content(b"arg");
        let key = derive_instance_key("t", &[arg], &[]);
        let expected = Hash::composite(&[
            Hash::from_content(instance_key_domain().as_bytes()),
            Hash::from_content(b"t"),
            arg,
        ]);
        assert_eq!(key, expected);
    }

    /// Pins that a hash-backed binding resolves to the referenced hash's
    /// canonical string bytes and that the instance hash covers the input name
    /// as well as those bytes.
    #[test]
    fn hash_reference_binding_uses_hash_string_bytes() {
        let hash = Hash::from_content(b"referenced-bytes");
        let bytes = instance_key_resolved_value_bytes_for_hash_reference(hash);
        assert_eq!(bytes, hash.to_string().as_bytes());
        let key_hash = instance_key_hash_from_resolved_value_bytes(
            "primary",
            "${step_output.step-0.primary}",
            &bytes,
        )
        .expect("hash reference binding is deterministic");
        assert_eq!(
            key_hash,
            Hash::composite(&[
                Hash::from_content(b"primary"),
                Hash::from_content(hash.to_string().as_bytes()),
            ])
        );
    }

    /// Pins that a literal binding's value material is its raw source bytes
    /// (no template expansion) and that the input name participates in the
    /// instance hash.
    #[test]
    fn literal_binding_uses_raw_source_bytes() {
        let raw = "mp4";
        let bytes = instance_key_resolved_value_bytes_for_literal_binding(raw);
        let key_hash = instance_key_hash_from_resolved_value_bytes("format", raw, &bytes)
            .expect("literal is deterministic");
        assert_eq!(
            key_hash,
            Hash::composite(&[Hash::from_content(b"format"), Hash::from_content(raw.as_bytes()),])
        );
    }

    /// Pins that a non-deterministic binding source (`${env.HOME}`, which the
    /// host environment can change between runs) stays out of the instance-key
    /// material entirely, so an env-dependent run never reuses a stale
    /// instance.
    #[test]
    fn env_binding_is_not_instance_key_material() {
        assert!(!is_deterministic_binding_source("${env.HOME}"));
        assert!(
            instance_key_hash_from_resolved_value_bytes("home", "${env.HOME}", b"/tmp").is_none()
        );
    }

    /// The input name is key material: it decides which command slot the value
    /// lands in, so two bindings that resolve to the same bytes under different
    /// names must not share an instance key. Sharing one would let the first
    /// step's recorded instance be probed and reused for a differently-named
    /// (and therefore differently-shaped) command.
    #[test]
    fn same_bytes_under_different_names_produce_different_hashes() {
        let left = instance_key_hash_from_resolved_value_bytes("message", "same", b"same")
            .expect("a literal binding is deterministic");
        let right = instance_key_hash_from_resolved_value_bytes("text", "same", b"same")
            .expect("a literal binding is deterministic");
        assert_ne!(left, right, "the input name is part of the instance-key material");

        let again = instance_key_hash_from_resolved_value_bytes("message", "same", b"same")
            .expect("a literal binding is deterministic");
        assert_eq!(left, again, "and the derivation stays deterministic");
    }

    /// The scheme version is an integer and the domain states it verbatim.
    ///
    /// Version sequences in this repository move `v1 -> v2 -> v3 -> v4`; a
    /// dotted revision such as `v4.1` hides that the scheme changed and is
    /// banned. The rendered value is pinned literally, so changing the key
    /// material without bumping the scheme fails here — the oracle in
    /// `mediapm::materializer::resolve` pins the same literal independently.
    #[test]
    fn the_instance_key_domain_carries_an_integer_version() {
        let domain = instance_key_domain();
        assert_eq!(domain, "mediapm/conductor/tool call instances/v4");
        assert_eq!(domain, format!("mediapm/conductor/tool call instances/v{INSTANCE_KEY_SCHEME}"));
        assert!(!domain.contains('.'), "dotted scheme revision in {domain}");
    }
}
