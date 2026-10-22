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
/// readers cannot absorb. Every V4 wire type that can carry a field denies
/// unknown ones (`#[serde(deny_unknown_fields)]`; a fieldless enum and a
/// newtype cannot carry the attribute), so an older reader handed a newer
/// document fails on the field it cannot name instead of decoding it away and
/// then writing the document back without it. That makes the unsupported
/// direction a loud error rather than a silent loss, while the direction that
/// matters still holds: every field a V4 document may omit is
/// `#[serde(default)]`, so a document this version wrote always decodes here,
/// and so does one an earlier version wrote, whose fields are a subset. A
/// field a V4 document must carry stays required: `capture` is the example,
/// because no reader has ever written a record without one.
///
/// Backward compatibility is handled via the `state/versions/` module.
pub(crate) const STATE_VERSION: u32 = 4;

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
    /// Two readers consult it, in two different places: the retry gate reads
    /// the tool spec's `is_impure` for the execution in hand, and settlement
    /// reads this recorded flag to decide whether a differing artifact under
    /// one capture identity is the expected output of unreproducible work or
    /// divergence. It is never part of any instance key, impure or pure, so a
    /// reclassification changes what a settlement accepts without invalidating
    /// the entries already recorded.
    pub impure: bool,
    /// When an operator last marked this instance's recorded bytes as not to be
    /// reused, or `None` while they may be.
    ///
    /// This is the whole of explicit invalidation. The two commands that set it
    /// (`conductor state invalidate-tool-call`, `mediapm media invalidate`) and
    /// the one reader on the execution path are deliberately asymmetric: only
    /// `step_worker::cache::probe_cache` consults it, so every decision about
    /// reusing recorded work passes through the single place that cannot forget
    /// to check it. Materialization is the one reader that ignores it: it runs
    /// no work, and refusing a marked record would remove bytes from the
    /// library before the re-execution that replaces them exists, which is the
    /// destruction this marker exists to avoid.
    ///
    /// A re-execution clears it by rewriting the record: the fresh instance is
    /// built with no marker. Without that, a re-execution of unchanged inputs —
    /// which derives the same key — would miss forever. A cache hit never
    /// clears it, because a marked instance cannot be a hit.
    ///
    /// Provenance for the GC's purposes and never key material. The marker does
    /// not pin the record: retention is decided by reachability, so an
    /// invalidated instance that nothing references ages out like any other.
    #[serde(default)]
    pub invalidated_at: Option<Timestamp>,
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
    /// Instance keys each workflow's most recent successful run produced or
    /// hit, keyed by workflow id.
    ///
    /// The seed of the GC reachability walk: a key in this set, or reachable
    /// from one through the inputs it consumed, is live, and only an instance
    /// outside that closure can age out. Replaced wholesale by a successful
    /// run and left untouched by a failed one, so a failure never shortens a
    /// record's life. A workflow with no entry here contributes nothing to the
    /// closure, so a pass with no seeds has nothing to protect and evicts
    /// nothing.
    #[serde(default)]
    pub live_instances: BTreeMap<String, BTreeSet<Hash>>,
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

/// The step and work a settlement is merging for.
///
/// Carried as one value because both fields are diagnostics-or-policy for the
/// same decision: whether a differing artifact is divergence (the step is
/// named in the error) or the expected output of unreproducible work (the
/// `impure` flag decides). Passing them together keeps the call site from
/// having to remember which is which.
#[derive(Clone, Copy)]
pub(crate) struct SettlingStep<'a> {
    /// Id of the step whose settlement is merging, named in divergence
    /// diagnostics so a reader knows which step to look at.
    pub(crate) step_id: &'a str,
    /// Whether the work is impure.
    ///
    /// A settlement sees two records that each claim a classification: the
    /// stored instance's flag and the executing tool's. The work counts as
    /// impure when either says so, because the byte-identity error only means
    /// anything when both records agree the work is reproducible.
    pub(crate) impure: bool,
}

/// What one fresh capture does to the entry the instance already holds.
///
/// Named rather than inlined into the loop so the order of the rules is visible
/// in one place: identity first (a changed spec replaces), then bytes (equal is
/// a no-op), then the diagnostic-stream exemption, then the impurity boundary,
/// and only what is left is divergence.
enum MergeDecision {
    /// The instance holds no entry under this name, so the fresh one is added.
    Insert,
    /// The recorded entry cannot describe these bytes or this work, so the
    /// fresh one takes its place.
    Replace,
    /// The recorded entry already says what the fresh one would say.
    Keep,
    /// A pure tool produced different artifacts for one input set.
    Diverge {
        /// Hash the instance recorded.
        stored: Hash,
        /// Hash the re-execution produced.
        produced: Hash,
    },
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
/// - A name recorded under a *different* capture identity is replaced by the
///   incoming entry. That is the "a changed capture spec under the same name
///   re-executes and replaces only that entry" case. Identity outranks the
///   identical-bytes no-op on purpose: keeping the old entry would leave the
///   probe comparing against a spec the step no longer declares, so it would
///   miss on every later sync and re-execute forever.
/// - A name recorded under the *same* capture identity must hold the same bytes.
///   Equal bytes are a no-op that keeps the stored entry, spec and `save`
///   included, so a `save`-only difference between two steps never rewrites the
///   record.
/// - Different bytes under one identity are a hard error naming the step, the
///   output and both hashes, with two boundaries.
///
///   The first is the diagnostic streams: a capture that reads `stdout` or
///   `stderr` keeps the stored entry instead. Byte identity is a promise about
///   the artifacts the work produces, and a capture reading a process stream
///   cannot promise byte reproducibility: those streams are progress lines,
///   timings and cache-hit notices, they vary per execution by construction,
///   and any execution of that work is an equivalent source for its stdout. The
///   exemption is keyed on the *selector*, not on the name, so a capture named
///   `content` that reads `stdout` is exempt while a capture named `stdout` that
///   reads `file:stdout` is not — what is read decides, and the shipped presets
///   rely on exactly that (`content` is captured from `stdout` in echo, import,
///   fs, export and sd). For a pure instance, `process_code` and every file or
///   folder capture stay strictly identical-or-error, because a different exit
///   code or a different file for identical work is real divergence rather
///   than presentation.
///   Silently keeping the later bytes there is how a non-deterministic tool — or
///   two distinct executions collapsed under one key — would hide.
///
///   The second is impurity: an instance the state records as impure replaces
///   the entry instead. Impure work cannot promise the same bytes twice — it
///   fetches, renders, or reads a service — so a difference is the expected
///   outcome rather than a claim about identical work, and the newest execution
///   is the record. Erroring here also had no in-band repair: every supported
///   way of forcing a re-execution (`force_rerun`, a blob deleted behind the
///   store's back, a sibling declaring a name the instance lacks, a changed
///   declaration elsewhere on the same instance) lands in this settlement, and
///   rejecting it left the run failing identically on every retry. The strict
///   rule stays for pure work, where a differing artifact for one input set is
///   either non-determinism or a key collision, which is what the error exists
///   to surface.
///
///   Impurity is not the only way two different executions can share a key, and
///   it is the only one this rule forgives: a command part the key derivation
///   cannot carry (see `is_deterministic_command_part`) leaves two executions
///   that differ only there under one key, so for an impure instance they
///   replace each other with nothing recorded about the substitution. That gap
///   predates this rule, and stating it keeps it from being read as something
///   impurity covered.
///
/// Capture identity is one comparison for every capture kind:
/// [`crate::config::OutputCaptureSpec::same_capture_identity`] compares the
/// selector and the two settings that change which bytes it reads. That means
/// `include_topmost_folder` is compared even for a `file:` capture, which
/// cannot honour it. The alternative — a per-kind field list — would have to be
/// kept in step with the capture phase's own reading of each selector, and a
/// mismatch there is a miss on every sync. A needless field in the comparison
/// only costs a re-execution of the rare declaration that sets it on a
/// non-folder capture.
///
/// `declared` is the name-to-spec map the executing step declared it could
/// produce this run — the tool defaults merged under its declarations, plus the
/// implicit stream captures — and `fresh` is what the capture phase actually
/// recorded from that execution. Neither is the whole instance: a settlement
/// speaks only for the names its execution was responsible for. `settling` says
/// whose settlement this is: the step the diagnostics name, and whether the
/// work is impure, in which case a differing artifact replaces instead of
/// failing.
///
/// # Errors
///
/// Returns [`ConductorError::Workflow`] when a *pure* instance records one name
/// under equal capture identities with different content hashes, unless the
/// capture reads `stdout` or `stderr` (see the boundaries above).
pub(crate) fn merge_captured_outputs(
    stored: &BTreeMap<String, CapturedOutput>,
    declared: &BTreeMap<String, crate::config::OutputCaptureSpec>,
    fresh: BTreeMap<String, CapturedOutput>,
    settling: SettlingStep<'_>,
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
        // The decision is taken from an immutable view and acted on afterwards,
        // because replacing an entry needs `&mut` and the question is about the
        // entry it replaces.
        let decision = match merged.get(&name) {
            None => MergeDecision::Insert,
            Some(existing) => {
                if !same_recorded_capture_identity(
                    existing.capture.as_ref(),
                    incoming.capture.as_ref(),
                ) {
                    MergeDecision::Replace
                } else if existing.hash == incoming.hash
                    || is_diagnostic_stream(
                        existing.capture.as_ref().map(|spec| spec.capture.as_str()),
                    )
                {
                    // Same bytes: the stored entry already says everything,
                    // `save` included — persistence is the executing step's
                    // decision and changes no bytes. Differing diagnostic
                    // chatter keeps the stored entry too: those streams are the
                    // work's presentation, not one of its artifacts, and the
                    // stored bytes are as good as the fresh ones.
                    MergeDecision::Keep
                } else if settling.impure {
                    MergeDecision::Replace
                } else {
                    MergeDecision::Diverge { stored: existing.hash, produced: incoming.hash }
                }
            }
        };
        match decision {
            MergeDecision::Insert | MergeDecision::Replace => {
                merged.insert(name, incoming);
            }
            MergeDecision::Keep => {}
            MergeDecision::Diverge { stored, produced } => {
                return Err(ConductorError::Workflow(format!(
                    "step '{}' captured output '{name}' as {stored} and a re-execution of the \
                     same work produced {produced}; identical work must produce identical \
                     artifacts, so a differing record means the tool is non-deterministic or two \
                     different executions were recorded under one instance key",
                    settling.step_id,
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
    use super::{CapturedOutput, SettlingStep, merge_captured_outputs};
    use crate::config::{OutputCaptureSpec, SaveMode};
    use mediapm_cas::Hash;
    use std::collections::BTreeMap;

    /// The settlement context these tests run under unless a test varies it: a
    /// pure step named `s1`.
    ///
    /// Byte identity is the default question — whether identical work produced
    /// identical artifacts — so a test that cares about impurity says so
    /// explicitly instead of every other test inheriting it.
    fn pure_settlement() -> SettlingStep<'static> {
        SettlingStep { step_id: "s1", impure: false }
    }

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

        let merged =
            merge_captured_outputs(&stored, &declared_of(&fresh), fresh, pure_settlement())
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
        let merged =
            merge_captured_outputs(&stored, &declared_of(&fresh), fresh, pure_settlement())
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
            pure_settlement(),
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

        let merged =
            merge_captured_outputs(&stored, &declared_of(&fresh), fresh, pure_settlement())
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

        let error = merge_captured_outputs(&stored, &declared_of(&fresh), fresh, pure_settlement())
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

        let merged =
            merge_captured_outputs(&stored, &declared_of(&fresh), fresh.clone(), pure_settlement())
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

        let merged = merge_captured_outputs(
            &stored,
            &declared_of(&stored),
            stored.clone(),
            pure_settlement(),
        )
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

        let merged =
            merge_captured_outputs(&stored, &declared_of(&fresh), fresh, pure_settlement())
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

        let error = merge_captured_outputs(&stored, &declared_of(&fresh), fresh, pure_settlement())
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

        let merged =
            merge_captured_outputs(&stored, &BTreeMap::new(), BTreeMap::new(), pure_settlement())
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

        let merged =
            merge_captured_outputs(&stored, &declared_of(&fresh), fresh.clone(), pure_settlement())
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

        let merged = merge_captured_outputs(&stored, &declared, BTreeMap::new(), pure_settlement())
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

        let merged = merge_captured_outputs(&stored, &declared, BTreeMap::new(), pure_settlement())
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

        let merged = merge_captured_outputs(&stored, &declared, fresh, pure_settlement())
            .expect("an undeclared name cannot be a conflict");

        assert_eq!(
            merged, stored,
            "an undeclared name is the sibling's to keep, whatever this execution produced",
        );
    }

    /// An impure re-execution replaces a differing artifact instead of failing
    /// the settlement.
    ///
    /// An impure tool's bytes are unreproducible by construction — a fetch, a
    /// render, a network lookup — so two executions of the same work are
    /// expected to differ, and the record has to move to the newest one. Every
    /// supported way of forcing a re-execution (`force_rerun`, a blob deleted
    /// behind the store's back, a changed sibling declaration) lands here.
    /// Erroring instead blamed the tool for the impurity the state already
    /// declares and left no in-band repair: `force_rerun` bypasses the probe,
    /// so the next attempt failed the same way.
    #[test]
    fn an_impure_re_execution_replaces_a_differing_artifact() {
        let stored =
            BTreeMap::from([("content".to_string(), captured("content", "file:out.bin", b"A"))]);
        let fresh =
            BTreeMap::from([("content".to_string(), captured("content", "file:out.bin", b"C"))]);

        let merged = merge_captured_outputs(
            &stored,
            &declared_of(&fresh),
            fresh.clone(),
            SettlingStep { step_id: "render", impure: true },
        )
        .expect("an impure tool's re-execution cannot contradict its own bytes");

        assert_eq!(
            merged, fresh,
            "the newest execution must be the record for unreproducible work",
        );
    }

    /// An impure re-execution that produced the same bytes is still a no-op.
    ///
    /// Impurity permits a difference; it does not require one. Keeping the
    /// stored entry here preserves the first settlement's provenance — `save`
    /// and any sibling name — instead of rewriting the record for nothing.
    #[test]
    fn an_impure_re_execution_of_identical_bytes_is_a_no_op() {
        let stored = BTreeMap::from([
            ("content".to_string(), captured("content", "file:out.bin", b"A")),
            ("sibling_only".to_string(), captured("sibling_only", "stdout", b"S")),
        ]);
        let fresh =
            BTreeMap::from([("content".to_string(), captured("content", "file:out.bin", b"A"))]);

        let merged = merge_captured_outputs(
            &stored,
            &declared_of(&fresh),
            fresh,
            SettlingStep { step_id: "render", impure: true },
        )
        .expect("identical bytes cannot conflict, impure or not");

        assert_eq!(merged, stored, "an unchanging artifact must leave the record untouched");
    }

    /// The divergence error names the step whose settlement found it.
    ///
    /// A shared instance can be settled by several steps, so a message naming
    /// only the output leaves the reader to work out which declaration to look
    /// at. The sibling-conflict error beside it already names both steps.
    #[test]
    fn the_divergence_error_names_the_settling_step() {
        let stored =
            BTreeMap::from([("content".to_string(), captured("content", "file:a.bin", b"A"))]);
        let fresh =
            BTreeMap::from([("content".to_string(), captured("content", "file:a.bin", b"C"))]);

        let error = merge_captured_outputs(
            &stored,
            &declared_of(&fresh),
            fresh,
            SettlingStep { step_id: "transcode", impure: false },
        )
        .expect_err("a pure tool cannot produce two different artifacts for one input set");

        assert!(
            error.to_string().contains("'transcode'"),
            "the message must name the step: {error}",
        );
    }
}
