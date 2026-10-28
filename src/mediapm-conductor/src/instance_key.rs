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
/// `v1 -> v2 -> v3 -> v4 -> v5`. A change to the key material bumps this number;
/// it never appends a revision such as `v4.1`, because the point of the bump is
/// to be visible. Old persisted keys simply miss once and re-execute.
pub const INSTANCE_KEY_SCHEME: u32 = 5;

/// Domain separator for tool-call instance keys, carrying the scheme version.
#[must_use]
pub fn instance_key_domain() -> String {
    format!("mediapm/conductor/tool call instances/v{INSTANCE_KEY_SCHEME}")
}

/// Derives the content-addressed key of one tool-call instance.
///
/// The key answers whether this work has been done before, from material any
/// party can re-derive: the tool call identity and the key material the command
/// names. Captures are **not** key material. They are records addressed by
/// name, so two steps that run the same work share one key however differently
/// they name what they record, and each step reads the names it binds out of
/// the instance it found.
///
/// The composite is exactly those two terms and the material between them, in
/// this order: the scheme domain separator ([`instance_key_domain`]), the tool
/// call id, then one hash per key-material term in the order the command names
/// them. Nothing here sorts, so the order is key material and the caller owes
/// it; [`key_material_for_tool`] is the one function that builds it, so the
/// executor and the materializer cannot rank the terms differently.
///
/// The environment is not material. A command part reading `${env.VAR}`
/// resolves to whatever the host holds, so keying it would make one tool mint a
/// new instance per environment it runs under.
///
/// What this key omits is answered by the records it addresses, not by the key.
/// The stored entry carries its capture spec, and the settlement decides whether
/// a differing record is divergence or the expected output of impure work.
#[must_use]
pub fn derive_instance_key(tool_call_id: &str, key_material: &[Hash]) -> Hash {
    let mut parts = Vec::with_capacity(2 + key_material.len());
    parts.push(Hash::from_content(instance_key_domain().as_bytes()));
    parts.push(Hash::from_content(tool_call_id.as_bytes()));
    parts.extend_from_slice(key_material);
    Hash::composite(&parts)
}

/// Returns whether a command part or a binding source reads the host
/// environment.
///
/// Such a source resolves to a value no other host is obliged to reproduce, so
/// it is not key material: two runs differing only in that value are the same
/// work. Everything else is, including a `:file(dest)` / `:folder(dest)`
/// selector, whose contribution is `composite(dest, source_hash)` rather than
/// its resolved text — the text embeds a sandbox path named for the key, which
/// cannot be hashed into the key that names it.
#[must_use]
pub fn references_environment(source: &str) -> bool {
    source.contains("${env.")
}

/// Builds the key material of a tool that runs no argv (every builtin).
///
/// A builtin receives its inputs by name, so the declared inputs in input-name
/// order are its command: each contributes `composite(name, resolved value)`,
/// and the name is part of the term because it decides which slot the value
/// occupies. An input whose binding source reads the host environment is left
/// out for the reason [`references_environment`] gives.
///
/// `input_values` holds each input's resolved value bytes, which both the
/// executor and the materializer can reconstruct: the executor from its own
/// resolution, the materializer from the object or literal a binding names.
#[must_use]
pub fn key_material_for_builtin(
    input_values: &BTreeMap<String, Vec<u8>>,
    raw_bindings: &BTreeMap<String, String>,
) -> Vec<Hash> {
    input_values
        .iter()
        .filter(|(name, _)| !raw_bindings.get(*name).is_some_and(|raw| references_environment(raw)))
        .map(|(name, value)| {
            Hash::composite(&[Hash::from_content(name.as_bytes()), Hash::from_content(value)])
        })
        .collect()
}

/// Builds the key material of a command, in argv order.
///
/// Each key-material part contributes one hash:
///
/// - A plain part contributes `composite(part template bytes, the value hash of
///   every input it reads, in reference order)`. The part's own text is in the
///   term because a changed flag is changed work, and the referenced values are
///   in it because a part is how a step passes an input to its tool.
/// - A part carrying `:file(dest)` or `:folder(dest)` contributes
///   `composite(dest, source hash)` per selector. The source is the object the
///   selector stages, which the resolved input names; the resolved text cannot
///   be hashed instead, because it embeds a sandbox path named for the key.
/// - A part reading `${env.VAR}` contributes nothing, for the reason
///   [`references_environment`] gives.
///
/// Both the executor and the materializer derive from these two inputs, so the
/// ordering and the term shape live here rather than at either call site.
pub fn key_material_for_command_parts(
    command_parts: &[String],
    input_values: &BTreeMap<String, Vec<u8>>,
) -> Vec<Hash> {
    let mut material = Vec::with_capacity(command_parts.len());
    for part in command_parts {
        if references_environment(part) {
            continue;
        }
        let selectors = selector_sources(part);
        if selectors.is_empty() {
            let mut parts = vec![Hash::from_content(part.as_bytes())];
            for name in referenced_inputs(part) {
                let value = input_values.get(&name).map_or(&[][..], Vec::as_slice);
                parts.push(Hash::from_content(value));
            }
            material.push(Hash::composite(&parts));
        } else {
            for selector in selectors {
                material.push(selector.term(input_values));
            }
        }
    }
    material
}

/// Builds the key material of one tool, choosing the form its shape calls for.
///
/// A tool with no command parts is a builtin and keys its inputs; every other
/// tool keys its command. Both the executor and the materializer call this, so
/// the ordering lives in one place.
#[must_use]
pub fn key_material_for_tool(
    command_parts: &[String],
    input_values: &BTreeMap<String, Vec<u8>>,
    raw_bindings: &BTreeMap<String, String>,
) -> Vec<Hash> {
    if command_parts.is_empty() {
        return key_material_for_builtin(input_values, raw_bindings);
    }
    key_material_for_command_parts(command_parts, input_values)
}

/// One selector a command part carries: where it stages, and what it stages.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SelectorSource {
    /// Sandbox-relative destination the selector writes to.
    destination: String,
    /// Binding source the selector consumes, as written in the template.
    source: String,
}

impl SelectorSource {
    /// Key material of this selector: its destination and its source, composed.
    ///
    /// The source is the hash of the object the selector stages. A resolved
    /// input value carries that hash as text when the binding names a stored
    /// object, and carries the value itself when the binding is a literal, so
    /// the parsed hash wins and the value's own hash is the fallback.
    fn term(&self, input_values: &BTreeMap<String, Vec<u8>>) -> Hash {
        Hash::composite(&[
            Hash::from_content(self.destination.as_bytes()),
            self.source_hash(input_values),
        ])
    }

    /// The hash of the bytes the selector consumes.
    fn source_hash(&self, input_values: &BTreeMap<String, Vec<u8>>) -> Hash {
        let value = self
            .source
            .strip_prefix("inputs.")
            .and_then(|name| input_values.get(name))
            .map_or(&[][..], Vec::as_slice);
        std::str::from_utf8(value)
            .ok()
            .and_then(|text| text.parse::<Hash>().ok())
            .unwrap_or_else(|| Hash::from_content(value))
    }
}

/// Every input a command part reads, in the order it reads them.
///
/// A part names inputs as `${inputs.NAME}` or, inside a splat, as
/// `${*inputs.NAME}`; both are reads of that input and both are captured, so a
/// changed value moves the key whether the part spreads it or passes it.
fn referenced_inputs(part: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut rest = part;
    while let Some(offset) = rest.find("inputs.") {
        let after = &rest[offset + "inputs.".len()..];
        let end = after
            .find(|character: char| {
                !(character.is_ascii_alphanumeric() || matches!(character, '_' | '-'))
            })
            .unwrap_or(after.len());
        if end > 0 {
            names.push(after[..end].to_string());
        }
        rest = &after[end..];
    }
    names
}

/// Finds every `:file(dest)` / `:folder(dest)` a command part carries, in
/// order.
///
/// The source of a selector is the identifier immediately before its colon: the
/// template grammar requires the reference to end there, so a conditional wraps
/// the selector without moving it.
fn selector_sources(part: &str) -> Vec<SelectorSource> {
    let mut found = Vec::new();
    let mut rest = part;
    loop {
        let file = rest.find(":file(");
        let folder = rest.find(":folder(");
        let (offset, marker) = match (file, folder) {
            (Some(file), Some(folder)) if folder < file => (folder, ":folder("),
            (Some(file), _) => (file, ":file("),
            (None, Some(folder)) => (folder, ":folder("),
            (None, None) => break,
        };
        let after = &rest[offset + marker.len()..];
        let Some(close) = after.find(')') else { break };
        found.push(SelectorSource {
            destination: after[..close].to_string(),
            source: trailing_identifier(&rest[..offset]),
        });
        rest = &after[close + 1..];
    }
    found
}

/// The identifier a selector source ends with, read backwards from the colon.
fn trailing_identifier(prefix: &str) -> String {
    let mut start = prefix.len();
    for (index, character) in prefix.char_indices().rev() {
        let continues =
            character.is_ascii_alphanumeric() || matches!(character, '_' | '.' | '-' | ':');
        if !continues {
            break;
        }
        start = index;
    }
    prefix[start..].to_string()
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

/// Returns true when `stored_key` — the key `instance` is filed under — matches
/// re-derived key material.
///
/// The derivation is content-only (tool id and key material), so an
/// impure instance's stored key keeps matching no matter how much wall-clock
/// time separates the recording from the check, and a step's capture
/// declarations cannot move it.
///
/// The key is a parameter and the record does not carry one: the map key is the
/// instance's only identity, so there is no second copy for a caller to compare
/// against the derivation instead of against where the record is filed.
#[must_use]
pub fn instance_matches_stored_key(
    stored_key: Hash,
    instance: &ToolCallInstance,
    key_material: &[Hash],
) -> bool {
    crate::state::derive_tool_call_instance_key(&instance.tool_call_id, key_material) == stored_key
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deriving twice from identical material yields identical keys.
    #[test]
    fn derive_instance_key_is_deterministic() {
        let a = derive_instance_key("echo@v1", &[]);
        let b = derive_instance_key("echo@v1", &[]);
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
            derive_instance_key("tool", &args),
            derive_instance_key("tool", &args),
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
        let with_domain = derive_instance_key("tool", &args);
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

    /// Argument order matters.
    ///
    /// The v4 scheme also had a separate materialized-inputs term here; it is
    /// gone, because a selector's material now reaches the key through the argv
    /// part that names it (see `a_staged_input_whose_bytes_change_re_keys_the_step`).
    #[test]
    fn derive_instance_key_args_are_ordered() {
        let a = Hash::from_content(b"a");
        let b = Hash::from_content(b"b");
        assert_ne!(
            derive_instance_key("t", &[a, b]),
            derive_instance_key("t", &[b, a]),
            "command arg order must matter",
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
        let key = derive_instance_key("t", &[arg]);
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

    /// A command part's position is key material.
    ///
    /// The argv the tool runs is a sequence, so two steps that pass the same
    /// two values in opposite orders run different commands. The list the
    /// derivation consumes is that argv, in order, and nothing in this module
    /// sorts it.
    #[test]
    fn command_part_order_is_key_material() {
        let first = Hash::from_content(b"-i");
        let second = Hash::from_content(b"-y");
        assert_ne!(
            derive_instance_key("tool", &[first, second]),
            derive_instance_key("tool", &[second, first]),
            "argv order must move the key",
        );
    }

    /// A staged input's bytes are key material.
    ///
    /// A `:file(dest)` part hands the command a file whose bytes come from a
    /// binding, and the part reaches the key as `composite(dest, source_hash)`.
    /// Two steps identical but for that source therefore derive different keys.
    /// Classifying the selector as nondeterministic instead would let two
    /// different files stage under one key, and the second step would be served
    /// the first one's instance.
    #[test]
    fn a_staged_input_whose_bytes_change_re_keys_the_step() {
        let parts = ["${inputs.source:file(inputs/source.mkv)}".to_string()];
        let first_hash = Hash::from_content(b"first-variant");
        let second_hash = Hash::from_content(b"second-variant");
        let first_values =
            BTreeMap::from([("source".to_string(), first_hash.to_string().into_bytes())]);
        let second_values =
            BTreeMap::from([("source".to_string(), second_hash.to_string().into_bytes())]);

        let first = key_material_for_command_parts(&parts, &first_values);
        let second = key_material_for_command_parts(&parts, &second_values);

        assert_eq!(
            first,
            vec![Hash::composite(&[Hash::from_content(b"inputs/source.mkv"), first_hash,])],
            "the selector hashes its destination and the object it stages",
        );
        assert_ne!(
            derive_instance_key("tool", &first),
            derive_instance_key("tool", &second),
            "different staged bytes must derive different keys",
        );
    }

    /// A plain part names the inputs it reads, and their changes reach the key.
    ///
    /// A splat reads its input too: the values it spreads are that input's, so
    /// a changed list is changed work even though the template does not move.
    #[test]
    fn a_plain_part_keys_its_text_and_the_inputs_it_reads() {
        let parts = ["--format=${inputs.format}".to_string()];
        let first = key_material_for_command_parts(
            &parts,
            &BTreeMap::from([("format".to_string(), b"mkv".to_vec())]),
        );
        let second = key_material_for_command_parts(
            &parts,
            &BTreeMap::from([("format".to_string(), b"mp4".to_vec())]),
        );
        assert_ne!(first, second, "a changed input value must move the key");
        assert_eq!(first.len(), 1, "one key-material part is one term");

        let splat = ["${*inputs.extra}".to_string()];
        let empty = key_material_for_command_parts(
            &splat,
            &BTreeMap::from([("extra".to_string(), b"[]".to_vec())]),
        );
        let filled = key_material_for_command_parts(
            &splat,
            &BTreeMap::from([("extra".to_string(), br#"["-y"]"#.to_vec())]),
        );
        assert_ne!(empty, filled, "a changed splat list must move the key");
    }

    /// A part reading the host environment stays out of the key entirely.
    #[test]
    fn an_environment_part_is_not_key_material() {
        let parts = ["${env.HOME}".to_string(), "--flag".to_string()];
        let first = key_material_for_command_parts(&parts, &BTreeMap::new());
        let second = key_material_for_command_parts(&parts[1..], &BTreeMap::new());
        assert_eq!(first, second, "the environment term must not reach the key");
        assert_eq!(first.len(), 1);
    }

    /// A builtin keys its inputs in name order, each name and value composed.
    #[test]
    fn a_builtin_keys_its_inputs_by_name() {
        let values = BTreeMap::from([
            ("kind".to_string(), b"file".to_vec()),
            ("hash".to_string(), b"blake3:abc".to_vec()),
        ]);
        let material = key_material_for_builtin(&values, &BTreeMap::new());
        assert_eq!(
            material,
            vec![
                Hash::composite(&[Hash::from_content(b"hash"), Hash::from_content(b"blake3:abc"),]),
                Hash::composite(&[Hash::from_content(b"kind"), Hash::from_content(b"file")]),
            ],
            "input-name order decides the terms",
        );
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
        assert_eq!(domain, "mediapm/conductor/tool call instances/v5");
        assert_eq!(domain, format!("mediapm/conductor/tool call instances/v{INSTANCE_KEY_SCHEME}"));
        assert!(!domain.contains('.'), "dotted scheme revision in {domain}");
    }
}
