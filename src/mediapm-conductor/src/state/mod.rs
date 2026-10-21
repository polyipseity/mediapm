//! Runtime orchestration state (volatile + CAS-persisted).
//!
//! The orchestration state tracks every tool-call instance, the captured
//! outputs each instance recorded together with the capture spec that produced
//! them, and the auxiliary metadata needed for GC and diagnostics.

use std::collections::{BTreeMap, BTreeSet};

use mediapm_cas::Hash;
use mediapm_utils::Timestamp;
use serde::{Deserialize, Serialize};

use crate::error::ConductorError;

pub mod versions;

/// Current orchestration-state schema version.
///
/// Bumped when the persisted JSON layout changes in a way older or newer
/// readers cannot absorb. An additive, serde-defaulted provenance field is
/// not such a change: an older file decodes with an empty value and a newer
/// file still decodes in an older reader, so the version stays put. That
/// forward readability holds only because no state wire type denies unknown
/// fields; a wire type that gained `#[serde(deny_unknown_fields)]` would end
/// the carve-out.
///
/// Readability is all the carve-out buys, so it is not round-trip safety: a
/// reader that predates `step_ids` decodes it as an unknown field, drops it
/// when it next writes the document, and leaves a state file whose schema
/// version still says this value while its provenance is gone. A build that
/// may rewrite this document therefore has to know every field it carries,
/// and a change an older writer would lose that way is what a bump is for.
/// Backward compatibility is handled via the `state/versions/` module.
pub(crate) const STATE_VERSION: u32 = 3;

/// Persistence policy for a CAS-referenced blob declared as external data.
///
/// Captured outputs carry their persistence decision on the capture spec
/// recorded beside them, so this mode speaks only for blobs the config names
/// directly: it says how a hash that is not part of any tool content map is
/// retained and persisted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OutputSaveMode {
    /// The blob is not persisted to CAS.
    Unsaved,
    /// The blob is persisted to CAS.
    Saved,
    /// The blob is persisted with full-data preference.
    Full,
}

/// A resolved input key-value pair for a tool-call instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedInput {
    /// Input key name.
    pub key: String,
    /// Resolved string value (may be a CAS hash reference or literal).
    pub value: String,
}

/// A content-addressed value with its determinism classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HashedValueRecord {
    /// CAS content hash of the recorded value.
    pub hash: Hash,
    /// Whether the value is deterministic (participates in instance keys).
    pub deterministic: bool,
}

/// One captured output: its content hash and the spec that produced it.
///
/// The spec travels with the bytes because an output is addressed by name, and
/// the name alone does not say what was captured: `content` may be a file read
/// or a folder archive, strict or tolerant of emptiness, and a reusing step
/// must be able to tell whether the bytes it finds were produced the way it
/// would produce them. `None` is the migration's "recorded before specs were
/// stored" sentinel. No declared [`crate::config::OutputCaptureSpec`] equals
/// `None`, so an entry in that state never satisfies a capture request that
/// names a spec — the step-worker probe compares each required name's recorded
/// spec and treats `None` as a miss — and the step re-executes and re-records
/// it once.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapturedOutput {
    /// CAS content hash of the captured bytes.
    pub hash: Hash,
    /// The capture spec that produced this entry, or `None` when the record
    /// predates spec storage.
    pub capture: Option<crate::config::OutputCaptureSpec>,
}

/// A completed tool-call instance: the runtime artifact of one tool call.
///
/// Records the effective command argv, execution environment, materialized
/// inputs, and captured outputs. Values are content-addressed (never stored
/// literally); only deterministic records participate in the instance key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolCallInstance {
    /// Content-addressed instance key (see
    /// [`crate::instance_key::derive_instance_key`]).
    pub instance_key: Hash,
    /// Tool call identifier: the unified tools-catalog map key (e.g.
    /// `"ffmpeg@v1"`).
    pub tool_call_id: String,
    /// Whether the tool is impure.
    ///
    /// Stored as provenance: it says how the tool was classified when the
    /// instance was recorded. Nothing reads it to decide behaviour — the retry
    /// gate consults the tool spec's `is_impure`, not this field — and it never
    /// alters the instance key, because a key derives from content alone.
    pub impure: bool,
    /// Execution timestamp recorded at run time. Provenance only; it is never
    /// part of any instance key, impure or pure.
    pub executed_at: Timestamp,
    /// Steps that have served this instance, by step id.
    ///
    /// Provenance only, never key material. It names the step that recorded
    /// the instance and every later step that reused it, which is what lets
    /// the demo fixture be re-keyed without re-deriving a historical key
    /// scheme: regeneration pairs an instance with every step that claims it —
    /// the one that executed the work and each later one that reused it.
    /// A cache hit records the reusing step here, and an id the set already
    /// holds is left as it is, so a steady-state sync persists the same
    /// record.
    #[serde(default)]
    pub step_ids: BTreeSet<String>,
    /// Effective command argv recorded at execution time, in order. Values
    /// are content-addressed; deterministic records participate in the key.
    pub command_args: Vec<HashedValueRecord>,
    /// Execution environment variables (minimal set: non-empty config values
    /// only). Values are hashed, never stored literally.
    pub env_vars: BTreeMap<String, HashedValueRecord>,
    /// Materialized tool-content inputs keyed by sandbox-relative path.
    pub materialized_inputs: BTreeMap<String, HashedValueRecord>,
    /// Captured output records keyed by output name, each carrying the spec
    /// that produced it.
    pub outputs: BTreeMap<String, CapturedOutput>,
}

/// Per-instance auxiliary metadata, kept off the instance record so cache
/// probes and key derivation stay lean.
///
/// Persistence is not here: a save mode belongs to the capture spec recorded
/// with the entry it describes, not to the instance, so replacing one output
/// never speaks for another.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct InstanceAux {
    /// Conductor GC last-reference clock: refreshed whenever the instance is
    /// referenced during step execution or retained by GC.
    pub last_referenced_at: Timestamp,
}

/// Auxiliary metadata attached to the orchestration state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AuxData {
    /// Monotonic tool call instance counter.
    pub tool_call_instance_counter: u64,
    /// Conductor GC reference clock — updated to [`Timestamp::now()`] on every
    /// state commit. Used by `run_conductor_gc()` for grace-period comparisons
    /// and CAS blob reclamation. Distinct from CAS GC.
    pub conductor_gc_epoch: Timestamp,
    /// Per-instance aux metadata keyed by instance key.
    pub instances: BTreeMap<Hash, InstanceAux>,
}

/// Full orchestration state snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConductorState {
    /// Schema version marker.
    pub version: u32,
    /// Declared tool call instance store (`instance_key` → instance).
    pub tool_call_instances: BTreeMap<Hash, ToolCallInstance>,
    /// Auxiliary metadata.
    pub aux: AuxData,
}

impl ConductorState {
    /// Creates an empty initial state with the current version marker.
    #[must_use]
    pub fn new_empty() -> Self {
        Self {
            version: STATE_VERSION,
            tool_call_instances: BTreeMap::new(),
            aux: AuxData::default(),
        }
    }

    /// Runs conductor GC on instances: refreshes
    /// `aux.instances[key].last_referenced_at` for referenced instances,
    /// evicts unreferenced instances past the TTL grace period, then updates
    /// `conductor_gc_epoch`.
    ///
    /// This is CONDUCTOR GC — prunes stale tool call instances and reclaims
    /// unreachable CAS blobs. Distinct from CAS GC which is a separate
    /// mechanism.
    pub fn run_conductor_gc(&mut self, referenced_keys: &BTreeSet<Hash>, ttl_seconds: u64) {
        let ttl_nanos = ttl_seconds.saturating_mul(1_000_000_000);
        let epoch = self.aux.conductor_gc_epoch;
        let epoch_nanos = epoch.as_unix_nanos();
        let aux = &mut self.aux;

        self.tool_call_instances.retain(|key, _instance| {
            if referenced_keys.contains(key) {
                // Referenced instances survive and get their last-reference
                // clock refreshed to the current GC epoch.
                if let Some(entry) = aux.instances.get_mut(key) {
                    entry.last_referenced_at = epoch;
                }
                true
            } else {
                let last_ref = aux
                    .instances
                    .get(key)
                    .map_or(0, |entry| entry.last_referenced_at.as_unix_nanos());
                // Evict if last reference was more than TTL ago.
                let deadline = last_ref.saturating_add(ttl_nanos);
                deadline >= epoch_nanos
            }
        });
        // Drop aux entries for evicted instances so the map never grows stale
        // keys.
        let live: BTreeSet<Hash> = self.tool_call_instances.keys().copied().collect();
        aux.instances.retain(|key, _| live.contains(key));
        aux.conductor_gc_epoch = Timestamp::now();
    }
}

impl Default for ConductorState {
    fn default() -> Self {
        Self::new_empty()
    }
}

/// Re-derives a tool-call instance key from the same components recorded at
/// execution time.
///
/// Builtin tools store resolved deterministic input hashes in the key rather
/// than `command_args`, so mediapm materialization uses this helper to match
/// persisted instances. Every step's key derives from content alone: the
/// recorded `executed_at` timestamp never participates, so an impure step's
/// stored instance keeps matching across runs until its key material changes
/// (or an explicit `force_rerun` bypasses the cache for one run).
///
/// Captures are not parameters: an instance's captured outputs are records
/// addressed by name, and a step reads the names it binds out of the instance
/// it found. Two steps running the same work therefore share this key however
/// differently they declare what they record, which is what lets one execution
/// serve both.
#[must_use]
pub fn derive_tool_call_instance_key(
    tool_call_id: &str,
    deterministic_input_hashes: &[Hash],
    deterministic_materialized_input_hashes: &[Hash],
) -> Hash {
    crate::instance_key::derive_instance_key(
        tool_call_id,
        deterministic_input_hashes,
        deterministic_materialized_input_hashes,
    )
}

/// Merges one execution's captured outputs into the entries an instance
/// already holds.
///
/// This is a union, not a replacement, because one instance is shared by every
/// step that runs the same work. Two such steps may declare different names —
/// each step only ever sees its own declarations — so a settlement that kept
/// only its own bundle would evict the names another settlement recorded. The
/// pair would then alternate: each sync re-executes the step whose name was
/// dropped, and each successful execution drops the other's name again. Nothing
/// about that loop looks like an error, which is why it has to be prevented
/// here rather than diagnosed later.
///
/// The rules, in the order they are applied:
///
/// - A stored entry whose name this execution declared, under the same capture
///   identity, and then did not produce is dropped. A capture that matches
///   nothing records nothing — a zero-match `file:` or `file_regex:` capture is
///   absent from `fresh` rather than empty — so without this the entry a
///   previous execution recorded under that name would outlive the artifact it
///   named, and a dependent would be handed bytes this execution did not
///   produce. Only an entry whose identity the declaration matches is dropped,
///   and only for a name the declaration carries: anything else is another
///   settlement's capture and belongs to the union below. A capture the config
///   stops declaring altogether is therefore never dropped either, because no
///   later execution is told it exists.
/// - A name the stored map does not hold is inserted. This is the union itself,
///   and it is what lets a step-only declaration survive a sibling's settlement.
/// - A name recorded under the *same* capture identity must hold the same bytes.
///   Equal bytes are a no-op that keeps the stored entry, spec and `save`
///   included, so a `save`-only difference between two steps never rewrites the
///   record.
/// - Different bytes under one identity are a hard error naming the output and
///   both hashes, with one boundary: a capture that reads `stdout` or `stderr`
///   keeps the stored entry instead. Byte identity is a promise about the
///   artifacts the work produces, and a capture reading a process stream cannot
///   promise byte reproducibility: those streams are progress lines, timings and
///   cache-hit notices, they vary per execution by construction, and any
///   execution of that work is an equivalent source for its stdout. The
///   exemption is keyed on the *selector*, not on the name, so a capture named
///   `content` that reads `stdout` is exempt while a capture named `stdout` that
///   reads `file:stdout` is not — what is read decides, and the shipped presets
///   rely on exactly that (`content` is captured from `stdout` in echo, import,
///   fs, export and sd). `process_code` and every file or folder capture stay
///   strictly identical-or-error, because a different exit code or a different
///   file for identical work is real divergence rather than presentation.
///   Silently keeping the later bytes there is how a non-deterministic tool — or
///   two distinct executions collapsed under one key — would hide.
/// - A name recorded under a *different* capture identity is replaced by the
///   incoming entry. That is the "a changed capture spec under the same name
///   re-executes and replaces only that entry" case. Identity outranks the
///   identical-bytes no-op on purpose: keeping the old entry would leave the
///   probe comparing against a spec the step no longer declares, so it would
///   miss on every later sync and re-execute forever.
///
/// `declared` is the name-to-spec map the executing step declared it could
/// produce this run — the tool defaults merged under its declarations, plus the
/// implicit stream captures — and `fresh` is what the capture phase actually
/// recorded from that execution. Neither is the whole instance: a settlement
/// speaks only for the names its execution was responsible for.
///
/// # Errors
///
/// Returns [`ConductorError::Workflow`] when one name is recorded under equal
/// capture identities with different content hashes, unless the capture reads
/// `stdout` or `stderr` (see the boundary above).
pub(crate) fn merge_captured_outputs(
    stored: &BTreeMap<String, CapturedOutput>,
    declared: &BTreeMap<String, crate::config::OutputCaptureSpec>,
    fresh: BTreeMap<String, CapturedOutput>,
) -> Result<BTreeMap<String, CapturedOutput>, ConductorError> {
    let mut merged = stored.clone();
    for (name, spec) in declared {
        if fresh.contains_key(name) {
            continue;
        }
        let same_identity = merged.get(name).is_some_and(|stored| {
            same_recorded_capture_identity(stored.capture.as_ref(), Some(spec))
        });
        if same_identity {
            merged.remove(name);
        }
    }
    for (name, incoming) in fresh {
        let Some(existing) = merged.get(&name) else {
            merged.insert(name, incoming);
            continue;
        };
        if !same_recorded_capture_identity(existing.capture.as_ref(), incoming.capture.as_ref()) {
            merged.insert(name, incoming);
            continue;
        }
        // The two questions are asked together so the exemption cannot come
        // before the byte check: identical bytes are always a no-op, differing
        // diagnostic chatter keeps the stored entry, and anything else that
        // differs is divergence.
        match (
            existing.hash != incoming.hash,
            is_diagnostic_stream(existing.capture.as_ref().map(|spec| spec.capture.as_str())),
        ) {
            // Same bytes: the stored entry already says everything, `save`
            // included — persistence is the executing step's decision and
            // changes no bytes. Differing diagnostic chatter keeps the stored
            // entry too: those streams are the work's presentation, not one of
            // its artifacts, and the stored bytes are as good as the fresh
            // ones.
            (false, _) | (true, true) => {}
            (true, false) => {
                return Err(ConductorError::Workflow(format!(
                    "captured output '{name}' was recorded as {} and a re-execution of the same \
                     work produced {}; identical work must produce identical artifacts, so a \
                     differing record means the tool is non-deterministic or two different \
                     executions were recorded under one instance key",
                    existing.hash, incoming.hash,
                )));
            }
        }
    }
    Ok(merged)
}

/// Returns whether a capture selector reads one of the two diagnostic streams.
///
/// The selector is what decides, never the output name: `stdout` and `stderr`
/// are the streams every execution produces, and their bytes are presentation —
/// progress lines, timings, cache-hit notices — so a capture that reads one
/// cannot promise byte reproducibility and [`merge_captured_outputs`] treats a
/// differing hash there as no divergence. A name is not evidence either way: a
/// capture called `content` that reads `stdout` is exempt, and one called
/// `stdout` that reads `file:stdout` is not. `process_code` is deliberately
/// absent: an exit code is an outcome, and two different outcomes for identical
/// work are exactly what the byte-identity rule exists to surface. `None` is
/// the migration's unknown spec and is not exempt either, because nothing
/// recorded says the bytes came from a stream.
fn is_diagnostic_stream(selector: Option<&str>) -> bool {
    matches!(selector, Some("stdout" | "stderr"))
}

/// Returns whether two recorded capture identities name the same artifact.
///
/// A record from before specs were stored carries `None` and never matches a
/// declared spec; two such records match each other, because the migration
/// gives every entry of one instance the same unknown state and there is no
/// declared identity for either to contradict.
fn same_recorded_capture_identity(
    existing: Option<&crate::config::OutputCaptureSpec>,
    incoming: Option<&crate::config::OutputCaptureSpec>,
) -> bool {
    match (existing, incoming) {
        (Some(existing), Some(incoming)) => existing.same_capture_identity(incoming),
        (None, None) => true,
        (Some(_), None) | (None, Some(_)) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::{CapturedOutput, merge_captured_outputs};
    use crate::config::{OutputCaptureSpec, SaveMode};
    use mediapm_cas::Hash;
    use std::collections::BTreeMap;

    /// Builds a capture spec for `name` reading `selector`, with `save` the
    /// only field a caller varies.
    fn spec(name: &str, selector: &str, save: SaveMode) -> OutputCaptureSpec {
        OutputCaptureSpec {
            name: name.to_string(),
            capture: selector.to_string(),
            save,
            ..OutputCaptureSpec::default()
        }
    }

    /// Builds a captured entry for `name` under `selector`, hashing `bytes`.
    fn captured(name: &str, selector: &str, bytes: &[u8]) -> CapturedOutput {
        CapturedOutput {
            hash: Hash::from_content(bytes),
            capture: Some(spec(name, selector, SaveMode::True)),
        }
    }

    /// Builds the declared name-to-spec map of an execution that produced every
    /// entry of `fresh`.
    ///
    /// Most tests exercise the union and the byte rules rather than the
    /// declared-but-absent case, and for those the declaration is exactly the
    /// produced set: nothing is declared and missing, so nothing is dropped.
    fn declared_of(
        fresh: &BTreeMap<String, CapturedOutput>,
    ) -> BTreeMap<String, OutputCaptureSpec> {
        fresh
            .iter()
            .filter_map(|(name, output)| output.capture.clone().map(|spec| (name.clone(), spec)))
            .collect()
    }

    /// A name the stored map does not hold is inserted beside the entries it
    /// does hold. This is the union's whole point: two steps that run the same
    /// work and bind different names end with one instance naming both.
    ///
    /// The stored map deliberately carries a name the fresh map does not, so a
    /// wholesale replacement fails this test: it would return the fresh keys
    /// alone and evict `sibling_only`.
    #[test]
    fn a_fresh_name_is_inserted_beside_the_stored_entries() {
        let stored = BTreeMap::from([
            ("content".to_string(), captured("content", "stdout", b"A")),
            ("sibling_only".to_string(), captured("sibling_only", "stdout", b"S")),
        ]);
        let fresh = BTreeMap::from([
            ("content".to_string(), captured("content", "stdout", b"A")),
            ("thumbnail".to_string(), captured("thumbnail", "stderr", b"B")),
        ]);

        let merged = merge_captured_outputs(&stored, &declared_of(&fresh), fresh)
            .expect("identical bytes are not a conflict");

        assert_eq!(
            merged.keys().cloned().collect::<Vec<_>>(),
            vec!["content".to_string(), "sibling_only".to_string(), "thumbnail".to_string()],
            "the union must keep every stored name and add the fresh one",
        );
        assert_eq!(merged["thumbnail"].hash, Hash::from_content(b"B"));
        assert_eq!(
            merged["sibling_only"], stored["sibling_only"],
            "a stored name the fresh map does not carry must survive untouched",
        );
    }

    /// A name recorded again with identical bytes is a no-op: the entry the
    /// instance already holds is kept, spec included.
    ///
    /// The stored map carries a name the fresh map does not, so a wholesale
    /// replacement fails this test — it would return the fresh map and drop
    /// `sibling_only` — while the no-op keeps the stored record byte for byte.
    #[test]
    fn an_identical_record_is_a_no_op() {
        let stored = BTreeMap::from([
            ("content".to_string(), captured("content", "stdout", b"A")),
            ("sibling_only".to_string(), captured("sibling_only", "stdout", b"S")),
        ]);
        let fresh = BTreeMap::from([("content".to_string(), captured("content", "stdout", b"A"))]);
        let merged = merge_captured_outputs(&stored, &declared_of(&fresh), fresh)
            .expect("an identical record cannot conflict");

        assert_eq!(
            merged, stored,
            "re-capturing the same bytes must leave the whole record untouched",
        );
    }

    /// A `save`-only difference keeps the stored entry as it is.
    ///
    /// `save` is a persistence instruction rather than a property of the
    /// bytes, so it is not part of capture identity. Two steps sharing one
    /// instance may legitimately disagree about it, and letting the later
    /// settlement rewrite the entry would make persisted provenance depend on
    /// settlement order.
    #[test]
    fn a_save_only_change_keeps_the_stored_entry() {
        let mut stored_entry = captured("content", "stdout", b"A");
        let stored = BTreeMap::from([("content".to_string(), stored_entry.clone())]);
        stored_entry.capture = Some(spec("content", "stdout", SaveMode::False));

        let merged = merge_captured_outputs(
            &stored,
            &declared_of(&BTreeMap::from([("content".to_string(), stored_entry.clone())])),
            BTreeMap::from([("content".to_string(), stored_entry)]),
        )
        .expect("a save-mode difference is not a conflict");

        assert_eq!(
            merged, stored,
            "the entry, spec and save mode included, must stay the one the instance already holds",
        );
    }

    /// A differing `stdout` or `stderr` record keeps the stored entry and is not
    /// an error.
    ///
    /// These two streams are diagnostic chatter: progress lines, timings, a
    /// cache-hit notice. Their bytes vary per execution by construction, so two
    /// executions of identical work differing here is not divergence about the
    /// work — it is what the streams are. Keeping the stored entry makes the
    /// union's result depend on the first settlement rather than on who finished
    /// last, and any execution of that work is an equivalent source for its
    /// stdout.
    #[test]
    fn a_differing_diagnostic_stream_keeps_the_stored_entry() {
        let stored = BTreeMap::from([
            ("stdout".to_string(), captured("stdout", "stdout", b"first run chatter")),
            ("stderr".to_string(), captured("stderr", "stderr", b"first run warnings")),
        ]);
        let fresh = BTreeMap::from([
            ("stdout".to_string(), captured("stdout", "stdout", b"second run chatter")),
            ("stderr".to_string(), captured("stderr", "stderr", b"second run warnings")),
        ]);

        let merged = merge_captured_outputs(&stored, &declared_of(&fresh), fresh)
            .expect("diagnostic chatter cannot contradict the work");

        assert_eq!(merged, stored, "the first recorded diagnostics must be the ones kept");
    }

    /// A differing `process_code` is still a hard error.
    ///
    /// An exit code is an outcome of the work, not presentation: the same work
    /// cannot succeed once and fail once, and a record that hid that would let a
    /// later probe reuse a failure's artifacts as a success's.
    #[test]
    fn different_process_codes_under_one_identity_are_rejected() {
        let stored = BTreeMap::from([(
            "process_code".to_string(),
            captured("process_code", "process_code", b"0"),
        )]);
        let fresh = BTreeMap::from([(
            "process_code".to_string(),
            captured("process_code", "process_code", b"1"),
        )]);

        let error = merge_captured_outputs(&stored, &declared_of(&fresh), fresh)
            .expect_err("an exit code is an outcome, so a change is divergence");

        assert!(
            error.to_string().contains("'process_code'"),
            "the message must name the output: {error}",
        );
    }

    /// An entry recorded without a spec is replaced by a fresh one, whatever
    /// the bytes.
    ///
    /// A `None` spec is the v2 migration's "spec unknown": the record cannot
    /// say which bytes the name holds beyond the hash it stored, so it can
    /// neither equal a declared spec nor vouch for the fresh entry's identity.
    /// Replacing it is what heals a migrated instance on its first re-execution
    /// instead of leaving it permanently unmatchable.
    #[test]
    fn a_fresh_entry_replaces_a_record_without_a_spec() {
        let stored = BTreeMap::from([(
            "content".to_string(),
            CapturedOutput { hash: Hash::from_content(b"A"), capture: None },
        )]);
        let fresh = BTreeMap::from([("content".to_string(), captured("content", "stdout", b"A"))]);

        let merged = merge_captured_outputs(&stored, &declared_of(&fresh), fresh.clone())
            .expect("an unknown recorded spec cannot be a byte conflict");

        assert_eq!(
            merged, fresh,
            "a spec-less record must be replaced by the entry that now says what it captured",
        );
    }

    /// Two spec-less records of the same bytes agree, because neither claims an
    /// identity the other contradicts.
    ///
    /// This only arises while merging migrated provenance; a fresh execution
    /// always records the spec it used, so the mixed case above is the one that
    /// runs in production.
    #[test]
    fn two_spec_less_records_of_the_same_bytes_are_a_no_op() {
        let stored = BTreeMap::from([(
            "content".to_string(),
            CapturedOutput { hash: Hash::from_content(b"A"), capture: None },
        )]);

        let merged = merge_captured_outputs(&stored, &declared_of(&stored), stored.clone())
            .expect("two unknown specs cannot contradict each other");

        assert_eq!(merged, stored);
    }

    /// A changed capture identity replaces that one entry and leaves the rest.
    ///
    /// This is the "a changed capture spec under the same name re-executes and
    /// replaces only that entry" case: the fresh bytes are what the new
    /// selector reads, so keeping the old entry would make every later probe
    /// miss forever. Identity outranks the identical-bytes no-op for that
    /// reason; here the bytes differ too, which is the ordinary case.
    #[test]
    fn a_changed_capture_identity_replaces_only_that_entry() {
        let stored = BTreeMap::from([
            ("content".to_string(), captured("content", "stdout", b"A")),
            ("thumbnail".to_string(), captured("thumbnail", "stdout", b"B")),
        ]);
        let fresh = BTreeMap::from([("content".to_string(), captured("content", "stderr", b"C"))]);

        let merged = merge_captured_outputs(&stored, &declared_of(&fresh), fresh)
            .expect("an identity change is allowed");

        assert_eq!(
            merged["content"].capture.as_ref().map(|spec| spec.capture.as_str()),
            Some("stderr"),
            "the fresh capture spec must replace the recorded one",
        );
        assert_eq!(merged["content"].hash, Hash::from_content(b"C"));
        assert_eq!(
            merged["thumbnail"], stored["thumbnail"],
            "an identity change under one name must not disturb another entry",
        );
    }

    /// Different bytes under one capture identity are a hard error naming the
    /// name and both hashes.
    ///
    /// Identical work must produce identical bytes. Overwriting silently is how
    /// a non-deterministic tool, or two different executions collapsed under
    /// one instance key, would hide: the instance would keep whichever
    /// settlement landed last and every dependent would read those bytes
    /// believing they came from its own inputs.
    #[test]
    fn different_bytes_under_one_identity_are_rejected() {
        let stored =
            BTreeMap::from([("content".to_string(), captured("content", "file:a.bin", b"A"))]);
        let fresh =
            BTreeMap::from([("content".to_string(), captured("content", "file:a.bin", b"C"))]);

        let error = merge_captured_outputs(&stored, &declared_of(&fresh), fresh)
            .expect_err("identical work that produced different bytes must be rejected");
        let message = error.to_string();

        assert!(message.contains("'content'"), "the message must name the output: {message}");
        assert!(
            message.contains(&Hash::from_content(b"A").to_string()),
            "the message must name the recorded hash: {message}",
        );
        assert!(
            message.contains(&Hash::from_content(b"C").to_string()),
            "the message must name the fresh hash: {message}",
        );
    }

    /// Nothing fresh leaves the stored map untouched, so a settlement that
    /// captured no output cannot erase what an earlier one recorded.
    #[test]
    fn an_empty_fresh_map_leaves_the_stored_map_untouched() {
        let stored = BTreeMap::from([("content".to_string(), captured("content", "stdout", b"A"))]);

        let merged = merge_captured_outputs(&stored, &BTreeMap::new(), BTreeMap::new())
            .expect("an empty capture set cannot conflict");

        assert_eq!(merged, stored);
    }

    /// Identical bytes under a changed capture identity are replaced, not a
    /// no-op.
    ///
    /// Identity outranks the byte comparison, and this is the case that proves
    /// the ordering rather than merely stating it: the bytes are equal, so an
    /// implementation that checked bytes first would keep the stored spec and
    /// leave the probe comparing against a declaration the step no longer makes
    /// — a miss on every later sync, and a replacement every time.
    #[test]
    fn identical_bytes_under_a_changed_identity_are_still_replaced() {
        let stored = BTreeMap::from([("content".to_string(), captured("content", "stdout", b"A"))]);
        let fresh = BTreeMap::from([("content".to_string(), captured("content", "stderr", b"A"))]);

        let merged = merge_captured_outputs(&stored, &declared_of(&fresh), fresh.clone())
            .expect("a changed capture identity is always allowed");

        assert_eq!(
            merged, fresh,
            "the fresh entry must replace the stored one although the bytes are equal",
        );
        assert_eq!(
            merged["content"].capture.as_ref().map(|spec| spec.capture.as_str()),
            Some("stderr"),
            "the recorded spec must be the one this execution declared",
        );
    }

    /// A name the executing step declared but this execution produced nothing
    /// under is dropped from the stored record.
    ///
    /// A zero-match `file:` capture records nothing rather than an empty entry,
    /// so the name is simply absent from `fresh`. Keeping the stored entry would
    /// outlive the artifact it named and hand a dependent bytes this execution
    /// did not produce — the hazard the stdout-alias removal closed on the
    /// materializer side.
    #[test]
    fn a_declared_name_the_execution_did_not_produce_is_dropped() {
        let stored = BTreeMap::from([("gamma".to_string(), captured("gamma", "file:g.txt", b"G"))]);
        let declared =
            BTreeMap::from([("gamma".to_string(), spec("gamma", "file:g.txt", SaveMode::True))]);

        let merged = merge_captured_outputs(&stored, &declared, BTreeMap::new())
            .expect("a capture that matched nothing is not a conflict");

        assert!(merged.is_empty(), "the entry must not outlive the artifact: {merged:?}");
    }

    /// A stored entry under a different capture identity survives the drop.
    ///
    /// The drop speaks only for a name this execution declared, and only for an
    /// entry recorded the way this execution would have recorded it. A differing
    /// identity is another settlement's capture — the sibling that shares the
    /// instance — so it belongs to the union and the drop leaves it alone.
    #[test]
    fn a_sibling_entry_under_a_different_identity_survives_the_drop() {
        let stored = BTreeMap::from([("beta".to_string(), captured("beta", "stdout", b"B"))]);
        let declared =
            BTreeMap::from([("beta".to_string(), spec("beta", "stderr", SaveMode::True))]);

        let merged = merge_captured_outputs(&stored, &declared, BTreeMap::new())
            .expect("a name nothing produced is not a conflict");

        assert_eq!(merged, stored, "another identity's record is not this execution's to drop");
    }

    /// A name the executing step never declares is never dropped.
    ///
    /// The drop is driven by this run's declarations, so a name only a sibling
    /// records has no declaration to remove it: that is what makes the union
    /// survive repeated re-executions of one step in a shared instance.
    #[test]
    fn a_name_the_execution_never_declares_is_never_dropped() {
        let stored = BTreeMap::from([
            ("alpha".to_string(), captured("alpha", "stdout", b"A")),
            ("sibling_only".to_string(), captured("sibling_only", "stdout", b"S")),
        ]);
        let declared =
            BTreeMap::from([("alpha".to_string(), spec("alpha", "stdout", SaveMode::True))]);
        let fresh = BTreeMap::from([("alpha".to_string(), captured("alpha", "stdout", b"A"))]);

        let merged = merge_captured_outputs(&stored, &declared, fresh)
            .expect("an undeclared name cannot be a conflict");

        assert_eq!(
            merged, stored,
            "an undeclared name is the sibling's to keep, whatever this execution produced",
        );
    }
}
