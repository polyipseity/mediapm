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
use crate::state::{CapturedOutput, ToolCallInstance};

/// The latest tool-call instance key version.
///
/// The scheme is versioned by an integer and only ever moves forward:
/// `v1 -> ... -> v7 -> v8`. A change to the key material bumps this number; it
/// never appends a revision such as `v7.1`, because the point of the bump is to
/// be visible. Old persisted keys simply miss once and re-execute.
///
/// Scheme 6 changed one term. Schemes 4 and 5 hashed every input by its
/// resolved value, so a binding reading `${context.os}` produced a different
/// term on each host. Scheme 6 hashes the binding's own text when the binding
/// reads nothing but the host or the environment, and keeps resolved bytes for
/// a binding that reads a step output, external data or another input.
///
/// Scheme 7 applies the provenance rule to the environment. A name the document
/// fixes with a non-empty `env_vars` literal is deterministic: the literal is
/// inlined into the part or binding before hashing, so editing it in the
/// document moves the key. A name the document does not fix — inherited from
/// the host, selected by platform, or given an empty literal — is recorded but
/// never keyed.
///
/// Scheme 8 keys the output specification and the staged inputs: sorted output
/// names with their capture tokens, then `materialized_inputs` as one composite
/// per entry. A captured output hash never enters.
pub const INSTANCE_KEY_VERSION: u32 = 8;

/// The v7 tool-call instance key version.
pub const V7_INSTANCE_KEY_VERSION: u32 = 7;

/// Domain separator for tool-call instance keys, carrying the scheme version.
#[must_use]
pub fn instance_key_domain(version: u32) -> String {
    format!("mediapm/conductor/tool call instances/v{version}")
}

/// Extra key material required to derive a v8 instance key.
///
/// v8 adds the output specification and the staged inputs. Output names and
/// their capture tokens are key material; captured output hashes are results
/// and never enter. Staged inputs are keyed again on their own as one
/// composite per entry, in destination order, even though selectors already
/// reach `command_args`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct V8KeyMaterial<'a> {
    /// Capture token per output name. Derivation reads these in name order.
    pub output_tokens: &'a BTreeMap<String, String>,
    /// Staged inputs: sandbox-relative destination to staged hash.
    pub materialized_inputs: &'a BTreeMap<String, Hash>,
}

/// Returns the extra key terms for `version`.
///
/// - v7: no extra terms.
/// - v8: one `composite(hash(name), hash(capture token))` per output name, in
///   name order, then one `composite(destination, staged hash)` per staged
///   input, in destination order.
/// - any other version: `None`, because the derivation cannot be evaluated.
#[must_use]
fn extra_terms(version: u32, material: Option<&V8KeyMaterial<'_>>) -> Option<Vec<Hash>> {
    match version {
        V7_INSTANCE_KEY_VERSION => Some(Vec::new()),
        INSTANCE_KEY_VERSION => {
            let material = material?;
            let mut terms = Vec::with_capacity(
                material.output_tokens.len() + material.materialized_inputs.len(),
            );
            for (name, token) in material.output_tokens {
                terms.push(Hash::composite(&[
                    Hash::from_content(name.as_bytes()),
                    Hash::from_content(token.as_bytes()),
                ]));
            }
            for (destination, staged_hash) in material.materialized_inputs {
                terms.push(Hash::composite(&[
                    Hash::from_content(destination.as_bytes()),
                    *staged_hash,
                ]));
            }
            Some(terms)
        }
        _ => None,
    }
}

/// Derives the content-addressed key of one tool-call instance for `version`.
///
/// The key answers whether this work has been done before, from material any
/// party can re-derive: the tool call identity and the key material the command
/// names. Captures are **not** key material except for their specification in
/// v8 (names and capture tokens), never their produced hashes.
///
/// The composite is, in order: the versioned domain separator
/// ([`instance_key_domain`]), the tool call id, then `command_args` (the
/// key-material term list) in order, then the version's extra terms. Nothing
/// here sorts `command_args`; [`key_material_for_tool`] is the one function
/// that builds it.
///
/// Returns `None` when `version` is unknown or when v8 derivation lacks its
/// required extra material.
#[must_use]
pub fn derive_instance_key_for_version(
    version: u32,
    tool_call_id: &str,
    command_args: &[Hash],
    v8: Option<&V8KeyMaterial<'_>>,
) -> Option<Hash> {
    let extra = extra_terms(version, v8)?;
    let mut parts = Vec::with_capacity(2 + command_args.len() + extra.len());
    parts.push(Hash::from_content(instance_key_domain(version).as_bytes()));
    parts.push(Hash::from_content(tool_call_id.as_bytes()));
    parts.extend_from_slice(command_args);
    parts.extend(extra);
    Some(Hash::composite(&parts))
}

/// Derives a v7 instance key from `command_args`.
///
/// v7 is reproducible from `command_args` alone.
#[must_use]
pub fn derive_instance_key_v7(tool_call_id: &str, command_args: &[Hash]) -> Hash {
    derive_instance_key_for_version(V7_INSTANCE_KEY_VERSION, tool_call_id, command_args, None)
        .expect("v7 derivation requires no extra material")
}

/// Derives a v8 instance key from `command_args` and the output/staged-input
/// specification material.
#[must_use]
pub fn derive_instance_key_v8(
    tool_call_id: &str,
    command_args: &[Hash],
    output_tokens: &BTreeMap<String, String>,
    materialized_inputs: &BTreeMap<String, Hash>,
) -> Hash {
    let material = V8KeyMaterial { output_tokens, materialized_inputs };
    derive_instance_key_for_version(
        INSTANCE_KEY_VERSION,
        tool_call_id,
        command_args,
        Some(&material),
    )
    .expect("v8 derivation is defined for complete material")
}

/// Derives the content-addressed key of one tool-call instance.
///
/// This compatibility helper derives a v7 key, preserving pre-T5 runtime
/// behavior while the versioned lookup/migration logic is introduced. New code
/// that needs an explicit version should use [`derive_instance_key_for_version`],
/// [`derive_instance_key_v7`], or [`derive_instance_key_v8`].
#[must_use]
pub fn derive_instance_key(tool_call_id: &str, command_args: &[Hash]) -> Hash {
    derive_instance_key_v7(tool_call_id, command_args)
}

/// Material produced by migrating instance-key terms between versions.
///
/// For `(v7, v8)` the command arguments are unchanged and v8's extra terms come
/// from the record's outputs and materialized inputs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigratedKeyMaterial {
    /// Key-material term list (command args) for the migrated derivation.
    pub command_args: Vec<Hash>,
    /// Capture token per output name for v8 derivation.
    pub output_tokens: BTreeMap<String, String>,
    /// Staged inputs for v8 derivation.
    pub materialized_inputs: BTreeMap<String, Hash>,
}

/// Extracts output capture tokens from a recorded instance.
///
/// v6 stores the capture token string directly on each output.
#[must_use]
pub fn output_tokens_from_outputs(
    outputs: &BTreeMap<String, CapturedOutput>,
) -> Option<BTreeMap<String, String>> {
    Some(outputs.iter().map(|(name, output)| (name.clone(), output.capture.clone())).collect())
}

/// Migrates instance-key material from `from_version` to `to_version`.
///
/// The registry currently supports `(v7, v8)`, whose transform is the record
/// itself: `command_args` are unchanged and v8's extra terms come from the
/// record's outputs and materialized inputs. An unregistered pair or missing
/// data returns `None`.
#[must_use]
pub fn migrate_terms(
    from_version: u32,
    to_version: u32,
    command_args: &[Hash],
    outputs: &BTreeMap<String, CapturedOutput>,
    materialized_inputs: &BTreeMap<String, Hash>,
) -> Option<MigratedKeyMaterial> {
    match (from_version, to_version) {
        (V7_INSTANCE_KEY_VERSION, INSTANCE_KEY_VERSION) => Some(MigratedKeyMaterial {
            command_args: command_args.to_vec(),
            output_tokens: output_tokens_from_outputs(outputs)?,
            materialized_inputs: materialized_inputs.clone(),
        }),
        _ => None,
    }
}

/// Derives a v8 key from migrated material.
#[must_use]
pub fn derive_migrated_v8_key(tool_call_id: &str, migrated: &MigratedKeyMaterial) -> Hash {
    derive_instance_key_v8(
        tool_call_id,
        &migrated.command_args,
        &migrated.output_tokens,
        &migrated.materialized_inputs,
    )
}

/// Extracts output capture tokens from a tool-call instance record.
///
/// Returns `None` when the record cannot supply the output-spec terms v8 needs.
#[must_use]
pub fn instance_output_tokens(instance: &ToolCallInstance) -> Option<BTreeMap<String, String>> {
    output_tokens_from_outputs(&instance.outputs)
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

/// Returns the predicted `materialized_inputs` map for v8 key derivation from
/// command parts: each selector's sandbox-relative destination mapped to the
/// hash of the bytes it would stage, in destination order.
///
/// Staging records `Hash::from_content` of the bytes written, so the predicted
/// hash is the content hash of the resolved source bytes available from
/// `input_values` when the selector sources an input; selectors whose source is
/// not an `inputs.*` reference cannot be predicted from input values alone and
/// are omitted (callers must fall back to recorded material where needed).
#[must_use]
pub fn predicted_materialized_inputs(
    command_parts: &[String],
    input_values: &BTreeMap<String, Vec<u8>>,
    literal_env: &BTreeMap<String, String>,
) -> BTreeMap<String, Hash> {
    let mut predicted = BTreeMap::new();
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
        for selector in selector_sources(part) {
            if let Some(name) = selector.source.strip_prefix("inputs.") {
                if let Some(value) = input_values.get(name) {
                    predicted.insert(selector.destination, Hash::from_content(value));
                }
            }
        }
    }
    predicted
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
    #[test]
    fn derive_instance_key_domain_prefix() {
        let args = [Hash::from_content(b"arg")];
        let with_domain = derive_instance_key_v7("tool", &args);
        let expected = Hash::composite(&[
            Hash::from_content(instance_key_domain(V7_INSTANCE_KEY_VERSION).as_bytes()),
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

    /// The key is exactly `composite(domain, id, args, inputs)` for v7.
    #[test]
    fn derive_instance_key_composite_components_exact() {
        let arg = Hash::from_content(b"arg");
        let key = derive_instance_key_v7("t", &[arg]);
        let expected = Hash::composite(&[
            Hash::from_content(instance_key_domain(V7_INSTANCE_KEY_VERSION).as_bytes()),
            Hash::from_content(b"t"),
            arg,
        ]);
        assert_eq!(key, expected);
    }

    /// A v7 key is reproducible from `command_args` alone.
    #[test]
    fn v7_reproduces_a_stored_key_from_command_args() {
        let command_args = vec![Hash::from_content(b"part-1"), Hash::from_content(b"part-2")];
        let stored = derive_instance_key_v7("tool@v1", &command_args);
        let reproduced = derive_instance_key_for_version(
            V7_INSTANCE_KEY_VERSION,
            "tool@v1",
            &command_args,
            None,
        )
        .expect("v7 derivation needs no extra material");
        assert_eq!(stored, reproduced);
    }

    /// A v8 key moves with an output name or token and does not move with a
    /// captured hash.
    #[test]
    fn v8_moves_with_output_spec_and_not_with_captured_hash() {
        let command_args = vec![Hash::from_content(b"part")];
        let materialized_inputs =
            BTreeMap::from([("inputs/a.mkv".to_string(), Hash::from_content(b"staged"))]);
        let base_outputs = BTreeMap::from([("video".to_string(), "file:out.mkv".to_string())]);

        let base =
            derive_instance_key_v8("tool@v1", &command_args, &base_outputs, &materialized_inputs);

        let renamed_output = BTreeMap::from([("clip".to_string(), "file:out.mkv".to_string())]);
        assert_ne!(
            base,
            derive_instance_key_v8("tool@v1", &command_args, &renamed_output, &materialized_inputs),
            "an output name is key material in v8",
        );

        let retokened_output =
            BTreeMap::from([("video".to_string(), "file:other.mkv".to_string())]);
        assert_ne!(
            base,
            derive_instance_key_v8(
                "tool@v1",
                &command_args,
                &retokened_output,
                &materialized_inputs
            ),
            "an output capture token is key material in v8",
        );

        let different_staged =
            BTreeMap::from([("inputs/a.mkv".to_string(), Hash::from_content(b"different-staged"))]);
        assert_ne!(
            base,
            derive_instance_key_v8("tool@v1", &command_args, &base_outputs, &different_staged),
            "staged input hashes are key material in v8",
        );

        // Captured output hashes are results; v8 material carries names/tokens
        // only, so two records with different produced hashes but identical
        // capture tokens derive the same key.
        let outputs_a = BTreeMap::from([(
            "video".to_string(),
            CapturedOutput {
                capture: "file:out.mkv".to_string(),
                hash_nk: Some(Hash::from_content(b"bytes-a")),
                saved_nk: true,
            },
        )]);
        let outputs_b = BTreeMap::from([(
            "video".to_string(),
            CapturedOutput {
                capture: "file:out.mkv".to_string(),
                hash_nk: Some(Hash::from_content(b"bytes-b")),
                saved_nk: false,
            },
        )]);
        let tokens_a = output_tokens_from_outputs(&outputs_a).expect("tokens");
        let tokens_b = output_tokens_from_outputs(&outputs_b).expect("tokens");
        assert_eq!(tokens_a, tokens_b);
        assert_eq!(
            derive_instance_key_v8("tool@v1", &command_args, &tokens_a, &materialized_inputs),
            derive_instance_key_v8("tool@v1", &command_args, &tokens_b, &materialized_inputs),
            "captured output hashes and non-key record facts must not move the v8 key",
        );
    }

    /// An unknown version fails to derive.
    #[test]
    fn an_unknown_version_fails_to_derive() {
        let command_args = [Hash::from_content(b"arg")];
        assert!(
            derive_instance_key_for_version(0, "tool@v1", &command_args, None).is_none(),
            "version 0 is not a supported derivation",
        );
        assert!(
            derive_instance_key_for_version(6, "tool@v1", &command_args, None).is_none(),
            "an older unregistered version cannot be derived",
        );
        assert!(
            derive_instance_key_for_version(9, "tool@v1", &command_args, None).is_none(),
            "a newer unknown version cannot be derived",
        );
        assert!(
            derive_instance_key_for_version(99, "tool@v1", &command_args, None).is_none(),
            "an unknown future version cannot be derived",
        );
    }

    /// The domain string carries the requested key version.
    #[test]
    fn instance_key_domain_carries_the_requested_version() {
        assert_eq!(
            instance_key_domain(V7_INSTANCE_KEY_VERSION),
            "mediapm/conductor/tool call instances/v7"
        );
        assert_eq!(
            instance_key_domain(INSTANCE_KEY_VERSION),
            "mediapm/conductor/tool call instances/v8"
        );
    }

    /// `(v7, v8)` migration reproduces v8 terms from a v7 record.
    #[test]
    fn migrate_terms_v7_to_v8_uses_record_material() {
        let command_args = vec![Hash::from_content(b"argv")];
        let materialized_inputs =
            BTreeMap::from([("inputs/x".to_string(), Hash::from_content(b"staged"))]);
        let outputs = BTreeMap::from([(
            "video".to_string(),
            CapturedOutput {
                capture: "file:out.mkv".to_string(),
                hash_nk: Some(Hash::from_content(b"produced-hash")),
                saved_nk: true,
            },
        )]);

        let migrated = migrate_terms(
            V7_INSTANCE_KEY_VERSION,
            INSTANCE_KEY_VERSION,
            &command_args,
            &outputs,
            &materialized_inputs,
        )
        .expect("v7->v8 migration is registered and record has capture specs");
        assert_eq!(migrated.command_args, command_args);
        assert_eq!(
            migrated.output_tokens,
            BTreeMap::from([("video".to_string(), "file:out.mkv".to_string())])
        );
        assert_eq!(migrated.materialized_inputs, materialized_inputs);
        let v8 = derive_migrated_v8_key("tool@v1", &migrated);
        assert_eq!(
            v8,
            derive_instance_key_v8(
                "tool@v1",
                &command_args,
                &migrated.output_tokens,
                &materialized_inputs
            )
        );
    }

    /// Migration fails for unregistered pairs.
    #[test]
    fn migrate_terms_rejects_unregistered_pairs() {
        let command_args = [Hash::from_content(b"argv")];
        let materialized_inputs = BTreeMap::new();
        let complete_outputs = BTreeMap::from([(
            "video".to_string(),
            CapturedOutput {
                capture: "stdout".to_string(),
                hash_nk: Some(Hash::from_content(b"produced-hash")),
                saved_nk: true,
            },
        )]);
        assert!(
            migrate_terms(
                6,
                INSTANCE_KEY_VERSION,
                &command_args,
                &complete_outputs,
                &materialized_inputs
            )
            .is_none()
        );
        assert!(
            migrate_terms(
                V7_INSTANCE_KEY_VERSION,
                9,
                &command_args,
                &complete_outputs,
                &materialized_inputs
            )
            .is_none()
        );
        assert!(
            migrate_terms(
                INSTANCE_KEY_VERSION,
                V7_INSTANCE_KEY_VERSION,
                &command_args,
                &complete_outputs,
                &materialized_inputs
            )
            .is_none()
        );
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
        assert_ne!(a, inherited, "an inherited reference contributes no value");
        assert_eq!(
            inherited,
            key_material_for_tool(&parts[..1], &BTreeMap::new(), &BTreeMap::new(), &empty),
            "the inherited reference contributes nothing, so its part is dropped like any other",
        );
    }

    /// A part mixing a fixed literal with an input keys its inlined text and
    /// that input's value.
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

    /// The input name is key material.
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

    /// A staged input's bytes are key material in command-part derivation.
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

    /// A binding that reads only the host contributes its own text.
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
    /// hash.
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
            "a binding that names work keys the hash its source resolves to, not the binding text",
        );
    }

    /// A literal value the document fixes in an env-only binding reaches the
    /// key.
    #[test]
    fn a_deterministic_env_only_binding_reaches_the_key_through_its_value() {
        let parts = ["--out".to_string(), "${inputs.out}".to_string()];
        let bindings = BTreeMap::from([("out".to_string(), "${env.OUT}".to_string())]);
        let literals_a = BTreeMap::from([("OUT".to_string(), "/tmp/a".to_string())]);
        let literals_b = BTreeMap::from([("OUT".to_string(), "/tmp/b".to_string())]);
        let values_a = BTreeMap::from([("out".to_string(), b"/tmp/a".to_vec())]);
        let values_b = BTreeMap::from([("out".to_string(), b"/tmp/b".to_vec())]);

        assert_ne!(
            key_material_for_tool(&parts, &values_a, &bindings, &literals_a),
            key_material_for_tool(&parts, &values_b, &bindings, &literals_b),
            "editing a literal the document fixes is changed work",
        );
    }
}
