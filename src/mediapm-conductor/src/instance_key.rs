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
use crate::state::ToolCallInstance;

/// Version of the tool-call instance key scheme.
///
/// The scheme is versioned by an integer and only ever moves forward:
/// `v1 -> v2 -> v3 -> v4 -> v5 -> v6 -> v7`. A change to the key material bumps this
/// number; it never appends a revision such as `v5.1`, because the point of the
/// bump is to be visible. Old persisted keys simply miss once and re-execute.
///
/// Scheme 6 changed one term. Schemes 4 and 5 hashed every input by its resolved
/// value, so a binding reading `${context.os}` — `js_runtimes`, in the managed
/// tools — produced a different term on each host. Scheme 6 hashes the binding's
/// own text when the binding reads nothing but the host or the environment, and
/// keeps resolved bytes for a binding that reads a step output, external data or
/// another input.
///
/// Scheme 7 applies the provenance rule to the environment. A name the document
/// fixes with a non-empty `env_vars` literal is deterministic: the literal is
/// inlined into the part or binding before hashing, so editing it in the
/// document moves the key. A name the document does not fix — inherited from
/// the host, selected by platform, or given an empty literal — is recorded but
/// never keyed: its reference keeps its own text, so no host's value can move
/// the key.
pub const INSTANCE_KEY_SCHEME: u32 = 7;

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
/// What this key omits is answered by the records it addresses, not by the key.
/// The stored entry carries its capture spec, and the settlement decides whether
/// a differing record is divergence or the expected output of impure work.
///
/// The host's environment is not material. A command part reading `${env.VAR}`
/// whose name the document gives no literal resolves to whatever the host
/// holds, so keying it would make one tool mint a new instance per environment
/// it runs under; a name the document does fix is inlined into the part text
/// before hashing, and that value is the document's own rather than the host's.
#[must_use]
pub fn derive_instance_key(tool_call_id: &str, key_material: &[Hash]) -> Hash {
    let mut parts = Vec::with_capacity(2 + key_material.len());
    parts.push(Hash::from_content(instance_key_domain().as_bytes()));
    parts.push(Hash::from_content(tool_call_id.as_bytes()));
    parts.extend_from_slice(key_material);
    Hash::composite(&parts)
}

/// Returns whether a source still reads the environment after deterministic
/// literals were inlined.
///
/// A reference no literal covers is inherited: it resolves to a value no other
/// host is obliged to reproduce, so it contributes nothing to the key and two
/// runs differing only in that value are the same work. What survives
/// `inline_literals` is exactly that inherited remainder, which is why this
/// test runs on the inlined text rather than the raw template. Everything else
/// contributes, including a `:file(dest)` /
/// `:folder(dest)` selector, whose term is `composite(dest, source_hash)` rather
/// than its resolved text — the text embeds a sandbox path named for the key,
/// which cannot be hashed into the key that names it.
///
/// An input *binding* that reads `${env.…}` is handled by
/// `input_value_term_bytes` instead: it contributes its own text, so the input
/// still occupies its slot and two different variables stay distinct.
///
/// A literal whose value itself contains the literal text `${env.` is the one
/// shape where this check and the test oracle disagree: production inlines the
/// value and then reads the result as still reading the environment, so the
/// part is skipped and the binding contributes its own text (the inherited
/// remainder), while `oracle_inline_env`
/// (`src/mediapm/src/materializer/resolve.rs:1919`) returns `Some` and keys the
/// inlined text. Both production sides share this helper, so no user-visible
/// split exists today, and `demo_fixture_instance_keys_match_independent_spec_oracle`
/// fails loudly when a fixture ever carries such a value.
#[must_use]
pub fn references_environment(source: &str) -> bool {
    source.contains("${env.")
}

/// Substitutes every `${env.NAME}` whose `NAME` a deterministic literal covers.
///
/// The scan mirrors the template's own reference shape: `${env.` opens the
/// reference, the name runs over identifier characters, and a covered reference
/// is only the `${env.NAME}` form — one carrying a `:file(…)`-style selector is
/// left untouched, because a selector over an environment value stages what the
/// host would supply, which is inherited by the rule above. A reference whose
/// name the map does not cover is copied verbatim, so the caller can tell the
/// inherited remainder apart by testing the result for `${env.`: what survives
/// this function is exactly what the host still supplies.
fn inline_literals(source: &str, literals: &BTreeMap<String, String>) -> String {
    if !source.contains("${env.") {
        return source.to_string();
    }
    let marker = "${env.";
    let mut inlined = String::with_capacity(source.len());
    let mut rest = source;
    while let Some(offset) = rest.find(marker) {
        let after = &rest[offset + marker.len()..];
        let end = after
            .find(|character: char| {
                !(character.is_ascii_alphanumeric() || matches!(character, '_' | '-'))
            })
            .unwrap_or(after.len());
        let covered = literals.get(&after[..end]).filter(|_| after[end..].starts_with('}'));
        if let Some(value) = covered {
            inlined.push_str(&rest[..offset]);
            inlined.push_str(value);
            rest = &after[end + 1..];
        } else {
            inlined.push_str(&rest[..offset + marker.len() + end]);
            rest = &after[end..];
        }
    }
    inlined.push_str(rest);
    inlined
}

/// Returns whether a binding source names work rather than the environment it
/// runs in.
///
/// A binding that reads a step output, external data or another input names
/// bytes the document fixes, and those bytes are the work. A binding that reads
/// nothing but `${context.…}` or `${env.…}` selects where provisioned tooling
/// lives, which is environment: `js_runtimes` is the live case, and keying the
/// path it selects would split one deployment across hosts. A literal reads
/// neither, and its text already equals its value.
///
/// The test is a substring test, and that is its limit: a reference sitting in
/// an unreachable branch, or the same text inside a literal, classifies as
/// work. Nothing splits between the worker and the materializer, because both
/// read the same binding text, so the cost of a false positive is that the
/// binding is keyed by its resolved bytes, which for a literal is the literal
/// itself.
fn binding_names_work(raw_binding_source: &str) -> bool {
    raw_binding_source.contains("step_output.")
        || raw_binding_source.contains("external_data.")
        || raw_binding_source.contains("inputs.")
}

/// The bytes one input contributes to an instance key.
///
/// This is the whole of the scheme-7 rule: resolved bytes when the binding names
/// work, the binding's own text when it names only the host or the environment —
/// except a binding that reads no `${context.…}` whose `${env.…}` references the
/// document all fixes as literals, which contributes its resolved value, because
/// that value is the document's own and editing it is changed work. A binding
/// that mixes a context selection with fixed literals contributes its text
/// instead: the resolved bytes name the branch the host selected, so keying them
/// would split one document across hosts, and the literal half's staleness is
/// carried by the whole-map term [`key_material_for_tool`] joins. An input with
/// no recorded binding contributes its resolved bytes, because the caller that
/// submits a value without a source has nothing else to hash.
fn input_value_term_bytes<'a>(
    input_values: &'a BTreeMap<String, Vec<u8>>,
    raw_bindings: &'a BTreeMap<String, String>,
    name: &str,
    literal_env: &BTreeMap<String, String>,
) -> &'a [u8] {
    match raw_bindings.get(name) {
        Some(raw) if !binding_names_work(raw) => {
            // A deterministic env binding resolves to the document's literal,
            // so its value bytes are keyed; a binding that also selects on the
            // host keys its text, because the resolved bytes are the host's
            // branch, and an inherited remainder keeps the binding text, exactly
            // as a binding with no env reference does.
            if raw.contains("${env.")
                && !raw.contains("${context.")
                && !references_environment(&inline_literals(raw, literal_env))
            {
                input_values.get(name).map_or(&[][..], Vec::as_slice)
            } else {
                instance_key_value_bytes_for_context_only_binding(raw)
            }
        }
        _ => input_values.get(name).map_or(&[][..], Vec::as_slice),
    }
}

/// Builds the key material of a tool that runs no argv (every builtin).
///
/// A builtin receives its inputs by name, so the declared inputs in input-name
/// order are its command: each contributes `composite(name, value)`, and the
/// name is part of the term because it decides which slot the value occupies.
/// Every declared input keeps its slot, because dropping one would let two
/// inputs with distinct names collide when the remaining terms coincide.
///
/// `input_values` holds each input's resolved value bytes, which both the
/// executor and the materializer can reconstruct: the executor from its own
/// resolution, the materializer from the object or literal a binding names. The
/// value bytes are chosen by `input_value_term_bytes`, so a builtin keys a
/// context-only binding by its text exactly as a command tool does, and a
/// binding whose env references the document fixes as literals by those value
/// bytes.
#[must_use]
pub fn key_material_for_builtin(
    input_values: &BTreeMap<String, Vec<u8>>,
    raw_bindings: &BTreeMap<String, String>,
    literal_env: &BTreeMap<String, String>,
) -> Vec<Hash> {
    input_values
        .keys()
        .map(|name| {
            let value = input_value_term_bytes(input_values, raw_bindings, name, literal_env);
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
///   in it because a part is how a step passes an input to its tool. The value
///   hash follows `input_value_term_bytes`: a binding naming work contributes
///   its resolved bytes, a binding naming only the host contributes its text.
/// - A part carrying `:file(dest)` or `:folder(dest)` contributes
///   `composite(dest, source hash)` per selector. The source is the object the
///   selector stages, which the resolved input names; the resolved text cannot
///   be hashed instead, because it embeds a sandbox path named for the key.
/// - A part reading `${env.VAR}` whose name the document fixes with a literal
///   contributes the part text with that literal inlined, so editing the value
///   moves the key; a part whose env reference is inherited contributes
///   nothing, for the reason [`references_environment`] gives.
///
/// Both the executor and the materializer derive from these two inputs, so the
/// ordering and the term shape live here rather than at either call site.
#[must_use]
pub fn key_material_for_command_parts(
    command_parts: &[String],
    input_values: &BTreeMap<String, Vec<u8>>,
    raw_bindings: &BTreeMap<String, String>,
    literal_env: &BTreeMap<String, String>,
) -> Vec<Hash> {
    let mut material = Vec::with_capacity(command_parts.len());
    for raw_part in command_parts {
        let inlined;
        let part = if raw_part.contains("${env.") {
            inlined = inline_literals(raw_part, literal_env);
            if references_environment(&inlined) {
                continue;
            }
            inlined.as_str()
        } else {
            raw_part.as_str()
        };
        let selectors = selector_sources(part);
        if selectors.is_empty() {
            let mut parts = vec![Hash::from_content(part.as_bytes())];
            for name in referenced_inputs(part) {
                let value = input_value_term_bytes(input_values, raw_bindings, &name, literal_env);
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
/// the ordering lives in one place. `literal_env` is the document's non-empty
/// `env_vars` for this tool — the deterministic half of the environment, which
/// inlines into parts and bindings — and the two callers owe the same map, or
/// the keys they derive separate.
///
/// The map itself joins the material after the argv terms as one
/// `composite(name, value)` per entry in name order, the same shape a builtin's
/// declared inputs take (reusing [`key_material_for_builtin`]). The whole map is
/// the staleness channel for every literal the document fixes, not only the
/// ones a part or binding reads, so editing an unreferenced value re-keys the
/// step too.
#[must_use]
pub fn key_material_for_tool(
    command_parts: &[String],
    input_values: &BTreeMap<String, Vec<u8>>,
    raw_bindings: &BTreeMap<String, String>,
    literal_env: &BTreeMap<String, String>,
) -> Vec<Hash> {
    let mut material = if command_parts.is_empty() {
        key_material_for_builtin(input_values, raw_bindings, literal_env)
    } else {
        key_material_for_command_parts(command_parts, input_values, raw_bindings, literal_env)
    };
    let literal_inputs: BTreeMap<String, Vec<u8>> = literal_env
        .iter()
        .map(|(name, value)| (name.clone(), value.clone().into_bytes()))
        .collect();
    material.extend(key_material_for_builtin(&literal_inputs, &BTreeMap::new(), &BTreeMap::new()));
    material
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
///
/// Crate-private: the only reader is the hash helper below, and a second reader
/// outside this module would be a caller deciding key material by a rule that
/// lives here.
fn is_deterministic_binding_source(raw_binding_source: &str) -> bool {
    is_deterministic_command_part(raw_binding_source)
}

/// Encodes one tool default binding as the string form used in step inputs.
///
/// A list binding becomes a JSON array, which is what a step's own binding means
/// when it spells one out. The encoding is written here rather than delegated to
/// `serde_json`, because serializing a list of strings cannot fail and a
/// serializer's error arm would have to answer with something: an empty string
/// would read as "this binding names no work", which is a different fact and a
/// silently wrong key. Escaping follows JSON for the characters a string can
/// carry, and a test pins this output against `serde_json`'s.
#[must_use]
pub fn input_binding_to_string(binding: &InputBinding) -> String {
    match binding {
        InputBinding::String(value) => value.clone(),
        InputBinding::Vec(values) => {
            let mut encoded = String::from("[");
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    encoded.push(',');
                }
                encoded.push('"');
                for character in value.chars() {
                    match character {
                        '"' => encoded.push_str("\\\""),
                        '\\' => encoded.push_str("\\\\"),
                        '\n' => encoded.push_str("\\n"),
                        '\r' => encoded.push_str("\\r"),
                        '\t' => encoded.push_str("\\t"),
                        control if control.is_control() => {
                            // Lowercase hex, four digits, without a formatter: a
                            // `write!` to a `String` cannot fail, and its
                            // `Result` would have to be discarded to say so.
                            encoded.push_str("\\u");
                            let code = control as u32;
                            for shift in [12, 8, 4, 0] {
                                let nibble = ((code >> shift) & 0xF) as u8;
                                let digit =
                                    if nibble < 10 { b'0' + nibble } else { b'a' + (nibble - 10) };
                                encoded.push(char::from(digit));
                            }
                        }
                        other => encoded.push(other),
                    }
                }
                encoded.push('"');
            }
            encoded.push(']');
            encoded
        }
    }
}

#[cfg(test)]
mod binding_string_tests {
    use super::*;

    /// The list encoding the module writes itself agrees with `serde_json`.
    ///
    /// The encoding exists because a serializer's error arm would have to answer
    /// with a value, and any value it chose would be a wrong key. Writing it by
    /// hand is only safe while it matches the format every reader already
    /// expects, so this pins the hand-written output against the serializer's
    /// across the characters that need escaping.
    #[test]
    fn a_list_binding_encodes_as_the_serializer_would() {
        let cases: [&[&str]; 6] = [
            &[],
            &["plain"],
            &["two", "entries"],
            &["quote\"inside"],
            &["back\\slash"],
            &["new\nline", "tab\there", "carriage\rreturn", "control\u{1}char"],
        ];
        for case in cases {
            let values: Vec<String> = case.iter().map(|value| (*value).to_string()).collect();
            let encoded = input_binding_to_string(&InputBinding::Vec(values.clone()));
            let expected = serde_json::to_string(&values).expect("serialize a list of strings");
            assert_eq!(
                encoded, expected,
                "the hand-written encoding must match the serializer for {values:?}",
            );
        }
    }

    /// A string binding passes through unchanged.
    #[test]
    fn a_string_binding_is_its_own_text() {
        let binding = InputBinding::String("${env.HOME}/bin".to_string());
        assert_eq!(input_binding_to_string(&binding), "${env.HOME}/bin");
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

/// Bytes a binding contributes when it names only the host or the environment.
///
/// The binding's own text stands in for the value it selects. Two hosts running
/// the same document therefore derive one key for work whose only difference is
/// where the provisioned payload lives, and editing the template is still a
/// change to the key.
#[must_use]
pub fn instance_key_value_bytes_for_context_only_binding(raw_binding_source: &str) -> &[u8] {
    raw_binding_source.as_bytes()
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

    /// Pins that a binding naming no work contributes its source bytes, and that
    /// the input name participates in the instance hash.
    #[test]
    fn literal_binding_uses_raw_source_bytes() {
        let raw = "mp4";
        let bytes = instance_key_value_bytes_for_context_only_binding(raw);
        let key_hash = instance_key_hash_from_resolved_value_bytes("format", raw, bytes)
            .expect("literal is deterministic");
        assert_eq!(
            key_hash,
            Hash::composite(&[Hash::from_content(b"format"), Hash::from_content(raw.as_bytes()),])
        );
    }

    /// A binding that reads only `${env.…}` contributes its own text, not the
    /// value the host supplies for it.
    ///
    /// The live case is a step that passes a provisioned path held in the
    /// environment: keying the value would split one deployment across hosts
    /// that happen to hold different values for the same variable, while two
    /// different variables stay two jobs because their text differs.
    #[test]
    fn an_environment_binding_contributes_its_text() {
        let parts = ["--output".to_string(), "${inputs.out}".to_string()];
        let binding = "${env.MEDIAPM_OUT}";
        let bindings = BTreeMap::from([("out".to_string(), binding.to_string())]);
        let first = BTreeMap::from([("out".to_string(), b"/tmp/one".to_vec())]);
        let second = BTreeMap::from([("out".to_string(), b"/tmp/two".to_vec())]);

        let from_first = key_material_for_tool(&parts, &first, &bindings, &BTreeMap::new());
        assert_eq!(
            from_first,
            key_material_for_tool(&parts, &second, &bindings, &BTreeMap::new()),
            "the value the host holds is not work, so it must not move the key",
        );
        assert_eq!(
            from_first[1],
            Hash::composite(&[
                Hash::from_content(parts[1].as_bytes()),
                Hash::from_content(binding.as_bytes()),
            ]),
            "the term is the binding's own text",
        );

        let other_variable = BTreeMap::from([("out".to_string(), "${env.OTHER_OUT}".to_string())]);
        assert_ne!(
            key_material_for_tool(&parts, &first, &other_variable, &BTreeMap::new()),
            from_first,
            "two variables are two jobs, so their bindings must not collapse into one key",
        );
    }

    /// A deterministic env value is key material; an inherited reference is not.
    ///
    /// Two literal spellings of the same name must derive different keys, so
    /// editing a literal in the document is a miss and a re-run (the key is the
    /// staleness channel for literals). With no literal the reference is
    /// inherited, which contributes nothing: its material equals the material
    /// of the command with that part dropped, so a host's value can never move
    /// the key.
    #[test]
    fn a_literal_env_value_is_key_material() {
        let parts = vec!["echo".to_string(), "--out=${env.OUT}".to_string()];
        let literals_a = BTreeMap::from([("OUT".to_string(), "/tmp/a".to_string())]);
        let literals_b = BTreeMap::from([("OUT".to_string(), "/tmp/b".to_string())]);
        let empty = BTreeMap::new();
        let a = key_material_for_tool(&parts, &BTreeMap::new(), &BTreeMap::new(), &literals_a);
        let b = key_material_for_tool(&parts, &BTreeMap::new(), &BTreeMap::new(), &literals_b);
        let inherited = key_material_for_tool(&parts, &BTreeMap::new(), &BTreeMap::new(), &empty);
        assert_ne!(a, b, "a deterministic value changes the key");
        // The brief's `assert_eq!(a, inherited)` cannot hold beside
        // `assert_ne!(a, b)` (the inherited case has no literal, so no rule can
        // make it equal the literal-a case while literals move the key); both
        // facts are pinned here instead.
        assert_ne!(a, inherited, "an inherited reference contributes no value");
        assert_eq!(
            inherited,
            key_material_for_tool(&parts[..1], &BTreeMap::new(), &BTreeMap::new(), &empty),
            "the inherited reference contributes nothing, so its part is dropped like any other",
        );
    }

    /// A part mixing a fixed literal with an input keys its inlined text and
    /// that input's value.
    ///
    /// This is the structural half of scheme 7's part rule, where the vacuous
    /// guard lives: `a_literal_env_value_is_key_material` proves the key moves,
    /// this proves where — the term's own bytes contain the substituted value,
    /// so a derivation that skipped the part instead of inlining it cannot
    /// reproduce this term. The input read beside the literal still carries its
    /// bytes too, which is the collision class: part participation is decided
    /// before `referenced_inputs` runs, so a skip would drop the part text and
    /// its referenced-input term together and two documents differing only in
    /// that input would share one key.
    #[test]
    fn a_mixed_env_and_input_part_keys_the_inlined_text_and_the_input_value() {
        let parts = vec!["--tag=${env.FIXED}-${inputs.y}".to_string()];
        let literals = BTreeMap::from([("FIXED".to_string(), "v1".to_string())]);
        let empty_bindings = BTreeMap::new();
        let alpha = BTreeMap::from([("y".to_string(), b"alpha".to_vec())]);
        let beta = BTreeMap::from([("y".to_string(), b"beta".to_vec())]);

        let alpha_material = key_material_for_tool(&parts, &alpha, &empty_bindings, &literals);
        assert_eq!(
            alpha_material,
            vec![
                Hash::composite(&[
                    Hash::from_content(b"--tag=v1-${inputs.y}".as_slice()),
                    Hash::from_content(b"alpha".as_slice()),
                ]),
                Hash::composite(&[
                    Hash::from_content(b"FIXED".as_slice()),
                    Hash::from_content(b"v1".as_slice()),
                ]),
            ],
            "the part contributes its inlined text with the input's value, then the whole map joins",
        );
        assert_ne!(
            alpha_material,
            key_material_for_tool(&parts, &beta, &empty_bindings, &literals),
            "the input beside the fixed literal still decides the key",
        );
    }

    /// A literal no part or binding reads still moves the key.
    ///
    /// The whole `env_vars` map joins the key material, so it is the
    /// staleness channel for every literal the document fixes and not only for
    /// the ones the command spells: editing a value no part or binding
    /// references must re-key the step, or the record would keep serving work
    /// under a document that no longer describes it.
    #[test]
    fn an_unreferenced_literal_env_value_re_keys_the_step() {
        let parts = vec!["echo".to_string(), "hello".to_string()];
        let literals_a = BTreeMap::from([("UNUSED_TAG".to_string(), "one".to_string())]);
        let literals_b = BTreeMap::from([("UNUSED_TAG".to_string(), "two".to_string())]);

        let a = key_material_for_tool(&parts, &BTreeMap::new(), &BTreeMap::new(), &literals_a);
        let b = key_material_for_tool(&parts, &BTreeMap::new(), &BTreeMap::new(), &literals_b);
        assert_ne!(
            a, b,
            "a literal the command never reads is still a value the document fixes, so it must re-key the step",
        );
    }

    /// A binding mixing `${context.…}` with a fixed literal keys its text.
    ///
    /// The resolved bytes name the branch the host selected, so keying them
    /// would split one document across hosts. The literal half's staleness is
    /// carried by the whole-map composite above, so the text loses nothing and
    /// no host's bytes enter the key.
    #[test]
    fn a_mixed_context_and_literal_env_binding_keys_its_text() {
        let parts = ["--js-runtimes".to_string(), "${inputs.js_runtimes}".to_string()];
        let binding =
            "deno:${context.os == \"macos\" ? deps/macos/deno : deps/linux/deno}#${env.T3_TAG}";
        let bindings = BTreeMap::from([("js_runtimes".to_string(), binding.to_string())]);
        let literals = BTreeMap::from([("T3_TAG".to_string(), "v1".to_string())]);
        let on_macos =
            BTreeMap::from([("js_runtimes".to_string(), b"deno:deps/macos/deno#v1".to_vec())]);
        let on_linux =
            BTreeMap::from([("js_runtimes".to_string(), b"deno:deps/linux/deno#v1".to_vec())]);

        let macos = key_material_for_tool(&parts, &on_macos, &bindings, &literals);
        assert_eq!(
            macos,
            key_material_for_tool(&parts, &on_linux, &bindings, &literals),
            "the branch the host selects is environment, so one binding text must derive one key on both hosts",
        );
        assert_eq!(
            macos[1],
            Hash::composite(&[
                Hash::from_content(parts[1].as_bytes()),
                Hash::from_content(binding.as_bytes()),
            ]),
            "the term is the binding's text with the fixed literal still spelled in it",
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

        let bindings =
            BTreeMap::from([("source".to_string(), "${step_output.step-0.primary}".to_string())]);
        let first =
            key_material_for_command_parts(&parts, &first_values, &bindings, &BTreeMap::new());
        let second =
            key_material_for_command_parts(&parts, &second_values, &bindings, &BTreeMap::new());

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
            &BTreeMap::from([("format".to_string(), "mkv".to_string())]),
            &BTreeMap::new(),
        );
        let second = key_material_for_command_parts(
            &parts,
            &BTreeMap::from([("format".to_string(), b"mp4".to_vec())]),
            &BTreeMap::from([("format".to_string(), "mp4".to_string())]),
            &BTreeMap::new(),
        );
        assert_ne!(first, second, "a changed input value must move the key");
        assert_eq!(first.len(), 1, "one key-material part is one term");

        let splat = ["${*inputs.extra}".to_string()];
        let empty = key_material_for_command_parts(
            &splat,
            &BTreeMap::from([("extra".to_string(), b"[]".to_vec())]),
            &BTreeMap::new(),
            &BTreeMap::new(),
        );
        let filled = key_material_for_command_parts(
            &splat,
            &BTreeMap::from([("extra".to_string(), br#"["-y"]"#.to_vec())]),
            &BTreeMap::new(),
            &BTreeMap::new(),
        );
        assert_ne!(empty, filled, "a changed splat list must move the key");
    }

    /// A part reading the host environment stays out of the key entirely.
    #[test]
    fn an_environment_part_is_not_key_material() {
        let parts = ["${env.HOME}".to_string(), "--flag".to_string()];
        let first = key_material_for_command_parts(
            &parts,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
        );
        let second = key_material_for_command_parts(
            &parts[1..],
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
        );
        assert_eq!(first, second, "the environment term must not reach the key");
        assert_eq!(first.len(), 1);
    }

    /// A binding that reads only the host contributes its own text, so the path
    /// it selects does not split the key.
    ///
    /// `js_runtimes` in the demo's tools is
    /// `deno:${context.os == "macos" ? … }`: the branch picks where the provisioned
    /// Deno payload lives, which is environment rather than work. Keying the
    /// resolved path would make one deployment on two hosts look like two
    /// different jobs, and a workspace carried between them would redo every step
    /// for a path change. The template text is what names the job.
    #[test]
    fn a_binding_that_reads_only_the_host_does_not_split_the_key() {
        let parts = ["--js-runtimes".to_string(), "${inputs.js_runtimes}".to_string()];
        let binding = "deno:${context.os == \"macos\" ? deps/macos/deno : deps/linux/deno}";
        let bindings = BTreeMap::from([("js_runtimes".to_string(), binding.to_string())]);
        let on_macos =
            BTreeMap::from([("js_runtimes".to_string(), b"deno:deps/macos/deno".to_vec())]);
        let on_linux =
            BTreeMap::from([("js_runtimes".to_string(), b"deno:deps/linux/deno".to_vec())]);

        let macos = key_material_for_tool(&parts, &on_macos, &bindings, &BTreeMap::new());
        let linux = key_material_for_tool(&parts, &on_linux, &bindings, &BTreeMap::new());
        assert_eq!(
            macos, linux,
            "a binding that reads only the host names no work, so the branch it selects must not move the key",
        );
        assert_eq!(
            macos[1],
            Hash::composite(&[
                Hash::from_content(parts[1].as_bytes()),
                Hash::from_content(binding.as_bytes()),
            ]),
            "the term is the binding's text, composed with the part that reads it",
        );

        let edited = BTreeMap::from([(
            "js_runtimes".to_string(),
            "deno:${context.os == \"macos\" ? deps/macos/other : deps/linux/other}".to_string(),
        )]);
        assert_ne!(
            key_material_for_tool(&parts, &on_macos, &edited, &BTreeMap::new()),
            macos,
            "editing the template is editing what the step asks for",
        );
    }

    /// A context-only binding reaches a builtin's key as its text too.
    ///
    /// A builtin takes its inputs by name, so the same rule has to hold there:
    /// otherwise one host would split the key for a builtin while the identical
    /// binding left a command tool alone.
    #[test]
    fn a_builtin_keys_a_context_only_binding_by_its_text() {
        let binding = "${context.os == \"macos\" ? payload/macos : payload/linux}";
        let bindings = BTreeMap::from([("runtime".to_string(), binding.to_string())]);
        let on_macos = BTreeMap::from([("runtime".to_string(), b"payload/macos".to_vec())]);
        let on_linux = BTreeMap::from([("runtime".to_string(), b"payload/linux".to_vec())]);

        assert_eq!(
            key_material_for_builtin(&on_macos, &bindings, &BTreeMap::new()),
            key_material_for_builtin(&on_linux, &bindings, &BTreeMap::new()),
            "the host branch a builtin input selects is environment, not work",
        );
    }

    /// A literal value still reaches the key, unchanged by the rule above.
    ///
    /// A literal reads neither an input nor a step output, and its text and its
    /// resolved value are the same bytes, so `"23"` and `"24"` stay two different
    /// jobs. The pin exists because the rule now reads the binding, and a version
    /// of it that skipped literals would silently collapse every literal input
    /// into one key.
    #[test]
    fn a_literal_binding_value_reaches_the_key() {
        let parts = ["-crf".to_string(), "${inputs.crf}".to_string()];
        let low = BTreeMap::from([("crf".to_string(), b"23".to_vec())]);
        let high = BTreeMap::from([("crf".to_string(), b"24".to_vec())]);
        let bindings_low = BTreeMap::from([("crf".to_string(), "23".to_string())]);
        let bindings_high = BTreeMap::from([("crf".to_string(), "24".to_string())]);

        assert_ne!(
            key_material_for_tool(&parts, &low, &bindings_low, &BTreeMap::new()),
            key_material_for_tool(&parts, &high, &bindings_high, &BTreeMap::new()),
            "a changed literal is changed work",
        );
    }

    /// A binding that reads a step output reaches the key through that output's
    /// hash, so a rerun producing different bytes re-keys the step.
    #[test]
    fn a_step_output_binding_reaches_the_key_through_its_hash() {
        let parts = ["-i".to_string(), "${inputs.source}".to_string()];
        let binding = "${step_output.step-1.primary}";
        let bindings = BTreeMap::from([("source".to_string(), binding.to_string())]);
        let first = Hash::from_content(b"first");
        let second = Hash::from_content(b"second");
        let first_values = BTreeMap::from([("source".to_string(), first.to_string().into_bytes())]);
        let second_values =
            BTreeMap::from([("source".to_string(), second.to_string().into_bytes())]);

        assert_ne!(
            key_material_for_tool(&parts, &first_values, &bindings, &BTreeMap::new()),
            key_material_for_tool(&parts, &second_values, &bindings, &BTreeMap::new()),
            "an input read from another step is work, so its bytes belong in the key",
        );
    }

    /// A binding that reads declared external data is work, and its own clause
    /// is what says so.
    ///
    /// A `${external_data.<hash>}` binding names bytes the document declares, so
    /// two runs over different declared bytes are different work. The step-output
    /// clause beside it does not cover this shape: the text contains neither
    /// `step_output.` nor `inputs.`, so without its own clause the binding would
    /// be read as host-only and keyed by its own text, and every declared value
    /// would collapse into one key.
    #[test]
    fn an_external_data_binding_reaches_the_key_through_its_bytes() {
        let parts = ["-i".to_string(), "${inputs.source}".to_string()];
        let binding = "${external_data.blake3:abc}";
        let bindings = BTreeMap::from([("source".to_string(), binding.to_string())]);
        let first = BTreeMap::from([("source".to_string(), b"first".to_vec())]);
        let second = BTreeMap::from([("source".to_string(), b"second".to_vec())]);

        let from_first = key_material_for_tool(&parts, &first, &bindings, &BTreeMap::new());
        assert_ne!(
            from_first,
            key_material_for_tool(&parts, &second, &bindings, &BTreeMap::new()),
            "declared bytes are work, so a different declared value must move the key",
        );
        assert_eq!(
            from_first[1],
            Hash::composite(&[
                Hash::from_content(parts[1].as_bytes()),
                Hash::from_content(b"first"),
            ]),
            "the term is the declared bytes, not the binding text that names them",
        );
    }

    /// A builtin keys its inputs in name order, each name and value composed.
    #[test]
    fn a_builtin_keys_its_inputs_by_name() {
        let values = BTreeMap::from([
            ("kind".to_string(), b"file".to_vec()),
            ("hash".to_string(), b"blake3:abc".to_vec()),
        ]);
        let material = key_material_for_builtin(&values, &BTreeMap::new(), &BTreeMap::new());
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
    /// Version sequences in this repository move `v1 -> v2 -> v3 -> v4 -> v5 -> v6 -> v7`;
    /// a
    /// dotted revision such as `v4.1` hides that the scheme changed and is
    /// banned. The rendered value is pinned literally, so changing the key
    /// material without bumping the scheme fails here — the oracle in
    /// `mediapm::materializer::resolve` pins the same literal independently.
    #[test]
    fn the_instance_key_domain_carries_an_integer_version() {
        let domain = instance_key_domain();
        assert_eq!(domain, "mediapm/conductor/tool call instances/v7");
        assert_eq!(domain, format!("mediapm/conductor/tool call instances/v{INSTANCE_KEY_SCHEME}"));
        assert!(!domain.contains('.'), "dotted scheme revision in {domain}");
    }
}
