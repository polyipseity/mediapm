//! Tool-call instance-key helpers shared by step execution and mediapm
//! materialization matching.
//!
//! This module owns the instance-key scheme: its integer version, the domain
//! separator that version renders, and the derivation itself. Instance
//! identity is runtime data, so it does not live in `state/versions/`, whose
//! modules own wire formats alone.

use std::collections::BTreeMap;

use mediapm_cas::Hash;

use crate::config::{InputBinding, OutputCaptureSpec, SaveMode, ToolSpec};
use crate::orchestration::step_worker::template::is_deterministic_command_part;
use crate::state::{ResolvedInput, ToolCallInstance};

/// Version of the tool-call instance key scheme.
///
/// The scheme is versioned by an integer and only ever moves forward:
/// `v1 -> v2 -> v3`. A change to the key material bumps this number; it never
/// appends a revision such as `v3.1`, because the point of the bump is to be
/// visible. Old persisted keys simply miss once and re-execute.
pub const INSTANCE_KEY_SCHEME: u32 = 3;

/// Domain separator for tool-call instance keys, carrying the scheme version.
#[must_use]
pub fn instance_key_domain() -> String {
    format!("mediapm/conductor/tool call instances/v{INSTANCE_KEY_SCHEME}")
}

/// Derives the content-addressed key of one tool-call instance.
///
/// The key answers whether this work has been done before, from material the
/// caller can re-derive: the tool call identity, the step's capture set, the
/// deterministic argument hashes, the deterministic environment hashes and the
/// materialized tool-content hashes. Time and impurity never participate; an
/// impure instance's recorded timestamp is provenance.
///
/// The composite is exactly those six terms, in this order: the scheme domain
/// separator ([`instance_key_domain`]), the tool call id, the capture-set hash
/// ([`step_output_specs_hash`]), the command-arg hashes in argv order, the
/// environment hashes sorted by name, and the materialized-input hashes in
/// tool-content-map path order. Nothing here sorts, so the order is key
/// material and the caller owes it: [`deterministic_materialized_input_hashes`]
/// reads a `BTreeMap` and is path-ordered already, while env hashes must be
/// collected in name order. The executor passes no env hashes at all — every
/// env record is recorded as nondeterministic — so that term is empty on the
/// production paths.
#[must_use]
pub fn derive_instance_key(
    tool_call_id: &str,
    output_specs_hash: Hash,
    deterministic_command_args: &[Hash],
    deterministic_env_vars: &[Hash],
    deterministic_materialized_inputs: &[Hash],
) -> Hash {
    let mut parts = Vec::with_capacity(
        3 + deterministic_command_args.len()
            + deterministic_env_vars.len()
            + deterministic_materialized_inputs.len(),
    );
    parts.push(Hash::from_content(instance_key_domain().as_bytes()));
    parts.push(Hash::from_content(tool_call_id.as_bytes()));
    parts.push(output_specs_hash);
    parts.extend_from_slice(deterministic_command_args);
    parts.extend_from_slice(deterministic_env_vars);
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

/// Appends one field to the capture-set byte string, prefixed by its length.
///
/// The prefix is a fixed 8-byte little-endian `u64` on every target. A
/// `usize::to_le_bytes` prefix would be 4 bytes on a 32-bit host, so the same
/// capture set would derive a different key on a different architecture and a
/// library synced on one machine would re-execute everything on another. The
/// length is a widening cast: `usize` is at most 64 bits on every supported
/// target.
fn push_capture_field(bytes: &mut Vec<u8>, field: &[u8]) {
    bytes.extend_from_slice(&(field.len() as u64).to_le_bytes());
    bytes.extend_from_slice(field);
}

/// Returns the stable tag a [`SaveMode`] contributes to capture hashing.
///
/// Spelled out rather than derived from `Debug` or the serde rename: the tags
/// are key material, so they must not move when a diagnostic format or a
/// serialization attribute does.
const fn save_mode_tag(save: SaveMode) -> &'static str {
    match save {
        SaveMode::False => "false",
        SaveMode::True => "true",
        SaveMode::Full => "full",
    }
}

/// Hashes a step's declared capture set so the instance key covers the files
/// the step records, not only the command that produced them.
///
/// An instance claims the outputs it captured, so the capture set is key
/// material: two steps that run the same tool with the same inputs but record
/// different files must not share a key, or the second step reuses the first
/// step's captures and never records its own artifact.
///
/// The caller passes the step's own `outputs` map, which is exactly what the
/// capture phase records: the conductor never bridges a tool's default
/// captures into execution, so a tool-level `outputs` block in the document is
/// inert here. The `to_unified` construction in `config::documents` carries
/// the reason. Making those defaults live is the Series 2 change, and it
/// removes this term from the key in the same bump: the v4 key is input-only,
/// so a step locates an instance by the work it does and reads the names it
/// binds out of what the instance recorded. Growing this term with the
/// defaults instead — the other way to keep the current key shape — would keep
/// the key output-derived and reintroduce the executor/materializer
/// divergence the term exists to end.
///
/// The encoding is canonical and infallible: the byte string is built
/// directly, so there is no serialization path that can fail or reorder. For
/// each entry in `BTreeMap` order it pushes the map key, `name`, `capture` and
/// the `save` tag as length-prefixed fields, then `allow_empty` and
/// `include_topmost_folder` as single bytes. An empty map is encoded as zero
/// bytes, giving every step that declares no captures one fixed hash.
#[must_use]
pub fn step_output_specs_hash(outputs: &BTreeMap<String, OutputCaptureSpec>) -> Hash {
    let mut bytes = Vec::new();
    for (key, spec) in outputs {
        push_capture_field(&mut bytes, key.as_bytes());
        push_capture_field(&mut bytes, spec.name.as_bytes());
        push_capture_field(&mut bytes, spec.capture.as_bytes());
        push_capture_field(&mut bytes, save_mode_tag(spec.save).as_bytes());
        bytes.push(u8::from(spec.allow_empty));
        bytes.push(u8::from(spec.include_topmost_folder));
    }
    Hash::from_content(&bytes)
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
/// The derivation is content-only (tool id, the step's capture set,
/// deterministic input hashes, materialized-input hashes), so an impure
/// instance's stored key keeps matching no matter how much wall-clock time
/// separates the recording from the check.
///
/// `output_specs_hash` is caller-supplied — [`step_output_specs_hash`] of the
/// step's declared captures, which is the set the capture phase records —
/// because the instance carries the captures it recorded, not the spec that
/// selected them: the caller re-derives the spec from the workflow document it
/// is matching against.
#[must_use]
pub fn instance_matches_stored_key(
    instance: &ToolCallInstance,
    output_specs_hash: Hash,
    deterministic_input_hashes: &[Hash],
    deterministic_materialized_input_hashes: &[Hash],
) -> bool {
    let derived = crate::state::derive_tool_call_instance_key(
        &instance.tool_call_id,
        output_specs_hash,
        deterministic_input_hashes,
        &[],
        deterministic_materialized_input_hashes,
    );
    derived == instance.instance_key
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Returns a capture spec that captures `capture` under `name`, with every
    /// optional field left at its documented default so a test moves one field
    /// at a time.
    fn capture_spec(name: &str, capture: &str) -> OutputCaptureSpec {
        OutputCaptureSpec {
            name: name.to_string(),
            capture: capture.to_string(),
            ..OutputCaptureSpec::default()
        }
    }

    /// Stands in for a step's capture-set hash in the composition tests.
    ///
    /// The value only has to be stable: what these tests isolate is the
    /// composite around the capture term, and the term's own encoding is
    /// pinned by the capture tests below.
    fn capture_term() -> Hash {
        Hash::from_content(b"capture-set")
    }

    /// Deriving twice from identical material yields identical keys.
    #[test]
    fn derive_instance_key_is_deterministic() {
        let captures = capture_term();
        let a = derive_instance_key("echo@v1", captures, &[], &[], &[]);
        let b = derive_instance_key("echo@v1", captures, &[], &[], &[]);
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
        let captures = capture_term();
        assert_eq!(
            derive_instance_key("tool", captures, &args, &[], &[]),
            derive_instance_key("tool", captures, &args, &[], &[]),
            "identical material must derive identical keys regardless of wall-clock time",
        );
    }

    /// The versioned domain prefix participates in the key.
    ///
    /// The expected key is composed by hand, domain literal included, and the
    /// same composite without that term must differ. The comparison is against
    /// the full composition rather than a keyless-looking stand-in, so the test
    /// fails if the domain term is dropped or its bytes change; a stand-in that
    /// omitted the capture and argument terms would pass either way.
    #[test]
    fn derive_instance_key_domain_prefix() {
        let captures = capture_term();
        let args = [Hash::from_content(b"arg")];
        let with_domain = derive_instance_key("tool", captures, &args, &[], &[]);
        let expected = Hash::composite(&[
            Hash::from_content(instance_key_domain().as_bytes()),
            Hash::from_content(b"tool"),
            captures,
            args[0],
        ]);
        let without_domain = Hash::composite(&[Hash::from_content(b"tool"), captures, args[0]]);
        assert_eq!(
            with_domain, expected,
            "the key must be the composition that includes the domain"
        );
        assert_ne!(with_domain, without_domain, "dropping the domain term must move the key");
    }

    /// Argument order matters, and deterministic env and materialized-input
    /// hashes participate.
    #[test]
    fn derive_instance_key_args_env_inputs_ordered() {
        let a = Hash::from_content(b"a");
        let b = Hash::from_content(b"b");
        let captures = capture_term();
        assert_ne!(
            derive_instance_key("t", captures, &[a, b], &[], &[]),
            derive_instance_key("t", captures, &[b, a], &[], &[]),
            "command arg order must matter",
        );
        assert_ne!(
            derive_instance_key("t", captures, &[], &[], &[]),
            derive_instance_key("t", captures, &[], &[a], &[]),
            "deterministic env records must participate",
        );
        assert_ne!(
            derive_instance_key("t", captures, &[], &[], &[]),
            derive_instance_key("t", captures, &[], &[], &[b]),
            "deterministic materialized inputs must participate",
        );
    }

    /// The key is exactly `composite(domain, id, captures, args, env, inputs)`.
    ///
    /// Env is excluded in practice because the executor always passes an empty
    /// env slice. The capture term is included because an instance claims the
    /// outputs it captured. Pinning the exact component list guards all of it:
    /// adding a component, dropping the capture term, keying env, or
    /// reintroducing a time term breaks this test.
    #[test]
    fn derive_instance_key_composite_components_exact() {
        let arg = Hash::from_content(b"arg");
        let captures = capture_term();
        let key = derive_instance_key("t", captures, &[arg], &[], &[]);
        let expected = Hash::composite(&[
            Hash::from_content(instance_key_domain().as_bytes()),
            Hash::from_content(b"t"),
            captures,
            arg,
        ]);
        assert_eq!(key, expected);
    }

    /// Two steps that run the same tool with the same inputs but capture
    /// different files must not share an instance key.
    ///
    /// An instance claims the outputs it captured, so a shared key serves one
    /// step's captures to the other and the loser's artifact is never
    /// recorded. The online demo's `links` and `thumbnails` steps are exactly
    /// this pair: identical merged yt-dlp inputs, different captures.
    #[test]
    fn two_steps_that_differ_only_in_their_output_captures_get_distinct_keys() {
        let args = [Hash::from_content(b"arg")];
        let links = BTreeMap::from([(
            "yt_dlp_link_artifacts".to_string(),
            capture_spec("yt_dlp_link_artifacts", "folder_regex:^downloads/.+[.](?:url|webloc)$"),
        )]);
        let thumbnails = BTreeMap::from([(
            "yt_dlp_thumbnail_artifacts".to_string(),
            capture_spec(
                "yt_dlp_thumbnail_artifacts",
                "folder_regex:^downloads/.+[.](?:jpg|webp)$",
            ),
        )]);

        assert_ne!(
            crate::state::derive_tool_call_instance_key(
                "yt-dlp@blake3:aa",
                step_output_specs_hash(&links),
                &args,
                &[],
                &[],
            ),
            crate::state::derive_tool_call_instance_key(
                "yt-dlp@blake3:aa",
                step_output_specs_hash(&thumbnails),
                &args,
                &[],
                &[],
            ),
            "steps that capture different files must not share an instance key",
        );
    }

    /// Identical captures keep deduplicating: two steps with the same tool,
    /// inputs and captures derive the same key, so one execution serves both.
    #[test]
    fn identical_output_captures_still_deduplicate() {
        let args = [Hash::from_content(b"arg")];
        let captures = BTreeMap::from([("result".to_string(), capture_spec("result", "stdout"))]);
        let specs_hash = step_output_specs_hash(&captures);

        assert_eq!(
            crate::state::derive_tool_call_instance_key("echo@v1", specs_hash, &args, &[], &[]),
            crate::state::derive_tool_call_instance_key("echo@v1", specs_hash, &args, &[], &[]),
        );
    }

    /// Every capture field participates: a changed folder-naming rule or save
    /// mode changes what the instance claims, so it must change the key.
    #[test]
    fn step_output_specs_hash_changes_with_every_capture_field() {
        let base = capture_spec("out", "file:downloads/a.mkv");
        let base_hash =
            step_output_specs_hash(&BTreeMap::from([("out".to_string(), base.clone())]));
        let moved = [
            ("name", OutputCaptureSpec { name: "other".to_string(), ..base.clone() }),
            (
                "capture",
                OutputCaptureSpec { capture: "file:downloads/b.mkv".to_string(), ..base.clone() },
            ),
            ("save", OutputCaptureSpec { save: SaveMode::Full, ..base.clone() }),
            ("allow_empty", OutputCaptureSpec { allow_empty: !base.allow_empty, ..base.clone() }),
            (
                "include_topmost_folder",
                OutputCaptureSpec {
                    include_topmost_folder: !base.include_topmost_folder,
                    ..base.clone()
                },
            ),
        ];
        for (field, spec) in moved {
            assert_ne!(
                step_output_specs_hash(&BTreeMap::from([("out".to_string(), spec)])),
                base_hash,
                "changing `{field}` must change the capture hash",
            );
        }
        assert_ne!(
            step_output_specs_hash(&BTreeMap::from([("renamed".to_string(), base)])),
            base_hash,
            "changing the output key must change the capture hash",
        );
    }

    /// A step that declares no captures hashes stably and distinctly from one
    /// that captures something.
    #[test]
    fn empty_output_capture_map_hashes_stably() {
        let empty = BTreeMap::new();
        assert_eq!(
            step_output_specs_hash(&empty),
            step_output_specs_hash(&empty),
            "the empty capture set must hash to one fixed value",
        );
        assert_ne!(
            step_output_specs_hash(&empty),
            step_output_specs_hash(&BTreeMap::from([(
                "out".to_string(),
                capture_spec("out", "stdout")
            )])),
        );
    }

    /// Pins the capture byte string field by field: an 8-byte little-endian
    /// length, the field, and the two flag bytes at the end.
    ///
    /// The prefix is deliberately a `u64`. `usize::to_le_bytes` writes 4 bytes
    /// on a 32-bit host, which would derive a different key there and make a
    /// library synced on one architecture re-execute everything on the other.
    /// The pinned bytes below catch a changed field order, a varint or a
    /// missing flag; the width itself is only observable off a 64-bit host,
    /// where the two spellings coincide, so that half rests on the code
    /// carrying an explicit `u64` rather than on this assertion.
    #[test]
    fn capture_encoding_prefixes_every_field_with_a_fixed_eight_byte_length() {
        let captures = BTreeMap::from([("k".to_string(), capture_spec("n", "stdout"))]);
        let mut expected = Vec::new();
        for field in ["k", "n", "stdout", "true"] {
            expected.extend_from_slice(&(field.len() as u64).to_le_bytes());
            expected.extend_from_slice(field.as_bytes());
        }
        // The two flag bytes, at the spec's documented defaults: `allow_empty`
        // false and `include_topmost_folder` true.
        expected.push(0);
        expected.push(1);
        assert_eq!(
            step_output_specs_hash(&captures),
            Hash::from_content(&expected),
            "each capture field must carry an 8-byte little-endian length",
        );
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
    /// Version sequences in this repository move `v1 -> v2 -> v3`; a dotted
    /// revision such as `v2.2` hides that the scheme changed and is banned.
    #[test]
    fn the_instance_key_domain_carries_an_integer_version() {
        let domain = instance_key_domain();
        assert_eq!(domain, format!("mediapm/conductor/tool call instances/v{INSTANCE_KEY_SCHEME}"));
        assert!(!domain.contains('.'), "dotted scheme revision in {domain}");
    }
}
