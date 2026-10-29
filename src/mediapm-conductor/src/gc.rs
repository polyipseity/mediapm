//! Garbage collection orchestration for conductor.
//!
//! Conductor GC covers state-instance pruning, CAS orphan reclamation, and CAS
//! metadata maintenance. It is distinct from CAS-internal GC.
//!
//! # One cycle, two triggers
//!
//! `run_conductor_gc` is the whole cycle, and both triggers run it: the explicit
//! `conductor gc` command and the background maintenance loop.
//!
//! Deleting a blob assumes every live byte is accounted for, and the two
//! triggers establish that differently. An operator invoking `conductor gc`
//! asserts nothing is in flight. A timer has to read the account off the
//! document instead, which is what the root set is built from: every hash a
//! document names is declared in `external_data`
//! (`NickelDocument::validate_external_data_invariant` rejects a document that
//! names one without declaring it), and every byte a run produced
//! is a recorded instance output. The invariant is what makes the cycle safe
//! unattended, and it is enforced at load rather than discovered here: a
//! document that forgot a declaration fails when it is read, before a pass can
//! act on it.
//!
//! # What may be evicted
//!
//! Retention is decided by reachability, not by the clock alone. A run records
//! the instances it produced or hit as its seed
//! ([`crate::state::AuxData::live_instances`]), and the instances a live
//! workflow can still reach are found by walking that seed outward along the
//! edges the records already carry: a consumer's `materialized_inputs` name a
//! hash, and a producer's `outputs` name the hash it wrote, so the producer of
//! every input a reached instance binds is itself reached. Sharing widens the
//! closure and never narrows it.
//!
//! The grace period then applies only to what the walk did not reach, so a
//! superseded chain keeps its records and its blobs while a live consumer still
//! lists its bytes, and starts aging the moment that consumer re-runs onto
//! different inputs.
//!
//! The walk decides how long a record lives; the mode its capture spec recorded
//! decides whether the bytes outlive the producing instance. An output captured
//! with [`SaveMode::False`] is not a root once its producer leaves the closure,
//! so the next pass reclaims the blob while the record waits out its grace
//! period. While the producer is still reachable those bytes stay rooted: the
//! loop runs independently of the application, and a pass landing between one
//! step's capture and the next step's read would otherwise delete bytes the
//! consumer is about to open. A record that carries no capture spec roots, and
//! an abstained pass keeps every output, because a spec or a walk that cannot
//! answer is not evidence that the bytes are disposable.
//!
//! Two rules keep a GC pass from destroying live work. A workflow declared in
//! the merged document but carrying no seed makes the whole pass abstain —
//! refreshing and evicting nothing, logged once — because a closure computed
//! from an absent seed would evict the entire store. And closure membership
//! outranks `invalidated_at`: a marked instance is still what some live consumer
//! reads today, so the marker must not become a deletion before the
//! re-execution that replaces it has run.

use std::collections::{BTreeMap, BTreeSet};

use mediapm_cas::{CasApi, CasMaintenanceApi, Hash, PruneReport};

use crate::config::SaveMode;
use crate::error::ConductorError;
use crate::orchestration::protocol::UnifiedNickelDocument;

use crate::state::{CapturedOutput, ConductorState};

/// What one GC pass decided, as distinct from what it counted.
///
/// The counts in [`ConductorGcReport`] are all zero in two unrelated
/// situations: a pass that declined to walk, and a pass that walked a library
/// holding no instance. Only the first means the pass judged nothing, so the
/// two cannot be told apart by the numbers and this field says which happened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GcClassification {
    /// The walk ran, so the counts describe it.
    ///
    /// An empty library and a walked library whose every record is outside the
    /// closure both land here, and the counts separate them: no instances at
    /// all gives zero reachable and zero superseded, while a closure that
    /// missed everything gives zero reachable and a superseded count above
    /// zero.
    Classified,
    /// The pass refused to judge, because it could not trust the walk.
    ///
    /// Nothing was classified and nothing was evicted. Every output stayed
    /// rooted, because a walk the pass declined to use is no evidence that
    /// bytes are disposable.
    Abstained {
        /// What the pass could not vouch for, in words an operator can act on.
        reason: String,
    },
}

/// Aggregate report from one full conductor GC cycle.
///
/// There is no `Default`: a zeroed report is the report a walk that found
/// nothing produces, and handing one to a caller that never ran a walk would
/// state that as a fact.
#[derive(Debug, Clone)]
pub struct ConductorGcReport {
    /// Number of tool-call instances evicted by the TTL grace period.
    pub instances_removed: usize,
    /// Number of instances the reachability walk kept, whose
    /// `last_referenced_at` this pass refreshed.
    ///
    /// Zero when the pass abstains, because nothing was classified: the count
    /// is a statement about the walk, not about how many instances happen to
    /// exist. [`Self::classification`] is what tells the two apart.
    pub reachable_instances: usize,
    /// Number of recorded instances the walk did not reach.
    ///
    /// A superseded instance is not removed unless its grace period has
    /// expired, so this count can exceed [`Self::instances_removed`]. Zero when
    /// the pass abstains.
    pub superseded_instances: usize,
    /// What the pass decided, which the counts alone cannot express.
    pub classification: GcClassification,
    /// Number of orphan CAS blobs deleted.
    pub orphans_removed: usize,
    /// Result of the final CAS maintenance / constraint-pruning step.
    pub prune_report: PruneReport,
}

/// Computes the set of recorded instances a live workflow can still reach.
///
/// The seed is the union of every workflow's `live_instances` entry, restricted
/// to instances the store still holds — a seed naming an instance that has
/// already been evicted reaches nothing and is not counted. The walk then
/// follows, for every reached instance, each `materialized_inputs` hash to
/// every instance that recorded it as an output. Several producers of one hash
/// are all reached: the closure answers "who could a consumer be reading",
/// so a shared blob keeps every producer alive.
///
/// `invalidated_at` is not consulted. A marked instance is what a live consumer
/// reads today, so its record and its blobs stay until that consumer moves on
/// and the seven-day clock starts.
#[must_use]
pub(crate) fn reachability_closure(state: &ConductorState) -> BTreeSet<Hash> {
    let mut producers: BTreeMap<Hash, Vec<Hash>> = BTreeMap::new();
    for (instance_key, instance) in &state.tool_call_instances {
        for record in instance.outputs.values() {
            producers.entry(record.hash).or_default().push(*instance_key);
        }
    }

    let mut reached: BTreeSet<Hash> = state
        .aux
        .live_instances
        .values()
        .flatten()
        .filter(|key| state.tool_call_instances.contains_key(*key))
        .copied()
        .collect();
    let mut pending: Vec<Hash> = reached.iter().copied().collect();
    while let Some(instance_key) = pending.pop() {
        let Some(instance) = state.tool_call_instances.get(&instance_key) else {
            continue;
        };
        for input in instance.materialized_inputs.values() {
            let Some(instance_keys) = producers.get(input) else {
                continue;
            };
            for producer in instance_keys {
                if reached.insert(*producer) {
                    pending.push(*producer);
                }
            }
        }
    }
    reached
}

/// Drops seed entries for workflows the merged document no longer declares.
///
/// A deleted workflow cannot run again, so its seed would otherwise protect its
/// instances for as long as the state file lives. Removing the entry makes those
/// instances unreachable, which is what starts their grace period.
fn prune_seeds_for_removed_workflows(state: &mut ConductorState, unified: &UnifiedNickelDocument) {
    let workflows = &unified.workflows;
    state.aux.live_instances.retain(|workflow, _| workflows.contains_key(workflow));
}

/// Earliest phase of the cycle: evict the instance records the reachability walk
/// did not keep, and return what it decided.
///
/// This is the whole of what a background pass may do to the state, and it
/// touches no bytes: evicting a record leaves its blobs alone, which is why a
/// pass that runs while the application is working cannot destroy the
/// application's work.
///
/// # Phases
///
/// 1. Seed pruning — an entry whose workflow the merged document no longer
///    declares is dropped, so a deleted workflow's instances become
///    unreachable.
/// 2. Closure — the seed keys, plus every instance that recorded, as an output,
///    a hash some reached instance names in `materialized_inputs`.
/// 3. Reachability refresh — every key in the closure keeps its record and its
///    clock is refreshed to the GC epoch.
/// 4. TTL sweep — an instance outside the closure is evicted once its clock is
///    past the TTL.
///
/// Two abstentions stop the walk from deciding more than it knows. A merged
/// document that declares no workflow is treated as unreadable rather than as
/// permission to sweep, and a declared workflow with no recorded seed makes the
/// pass refresh and evict nothing. Pruning waits until the pass is otherwise
/// proceeding, because a generated document can be legitimately incomplete for
/// one run.
pub(crate) fn run_instance_gc(
    state: &mut ConductorState,
    unified: &UnifiedNickelDocument,
    ttl_seconds: u64,
) -> InstanceGcCounts {
    let unseeded: Vec<String> = unified
        .workflows
        .keys()
        .filter(|workflow| !state.aux.live_instances.contains_key(*workflow))
        .cloned()
        .collect();

    let document_declares_nothing =
        unified.workflows.is_empty() && !state.tool_call_instances.is_empty();
    let unseeded_reason = if document_declares_nothing {
        Some("the merged document declares no workflow".to_string())
    } else if unseeded.is_empty() {
        None
    } else {
        Some(format!("workflow(s) without a recorded instance seed: {}", unseeded.join(", ")))
    };

    let Some(reason) = unseeded_reason else {
        // Pruning is skipped while abstaining: a seed for a workflow the
        // document does not declare is only evidence of deletion when the
        // document is otherwise describing the same world.
        prune_seeds_for_removed_workflows(state, unified);
        let reachable = reachability_closure(state);
        let reachable_instances = reachable.len();
        let superseded_instances =
            state.tool_call_instances.len().saturating_sub(reachable_instances);
        tracing::info!(
            "conductor GC phase 1/3: instance pruning (ttl={ttl_seconds}s, \
             reachable={reachable_instances}, superseded={superseded_instances})"
        );
        let instance_count_before = state.tool_call_instances.len();
        state.run_conductor_gc(&reachable, ttl_seconds);
        return InstanceGcCounts {
            instances_removed: instance_count_before
                .saturating_sub(state.tool_call_instances.len()),
            reachable_instances,
            superseded_instances,
            abstention: None,
        };
    };

    tracing::info!("conductor GC phase 1/3: {reason}, so this pass refreshes and evicts nothing");
    InstanceGcCounts { abstention: Some(reason), ..InstanceGcCounts::default() }
}

/// Whether a captured output recorded itself as not worth keeping.
///
/// Only `false` is unsaved: `true` and `full` both ask for the bytes. A record
/// with no capture spec is saved, because the spec is what records the mode and
/// a record too old to carry one is no evidence that its bytes are disposable.
fn output_is_unsaved(record: &CapturedOutput) -> bool {
    record.capture.as_ref().is_some_and(|spec| spec.save == SaveMode::False)
}

/// Whether an instance's captured output is a root for this pass.
///
/// A saved output is always a root. An unsaved one is a root while its producer
/// can still be reached, and it is a root when `reachable` is `None`: an
/// abstained pass does not know which instances a live workflow reads, and the
/// doubt that stops it evicting records stops it reclaiming their bytes.
fn keeps_output(
    record: &CapturedOutput,
    instance_key: &Hash,
    reachable: Option<&BTreeSet<Hash>>,
) -> bool {
    if !output_is_unsaved(record) {
        return true;
    }
    reachable.is_none_or(|reached| reached.contains(instance_key))
}

/// Runs full conductor garbage collection: instance pruning, CAS orphan
/// deletion, and CAS maintenance.
///
/// Both triggers run this: the operator path (`conductor gc`, and the
/// `Conductor::run_gc` facade method) and the background maintenance loop.
/// Deleting orphans assumes every live byte is named by the root set, which is
/// built from the surviving instances' outputs, the merged document's
/// `external_data` keys, and its tool content hashes. An operator invoking
/// `conductor gc` adds the stronger assertion that nothing is in flight; the
/// timer relies on the document being complete, which
/// `NickelDocument::validate_external_data_invariant` enforces at load.
///
/// # Phases
///
/// 1. [`run_instance_gc`] — evict the records the walk did not keep.
/// 2. Root-set / orphan reclamation — collect referenced CAS hashes from
///    surviving instances and the unified config, then delete orphans. An
///    instance's outputs are roots except the ones it recorded as unsaved, and
///    those are roots only while the instance is still reachable.
/// 3. CAS maintenance — delegate to [`run_cas_gc_sweep`] for constraint
///    pruning and WAL consumption.
///
/// Phase 2 runs on an abstained pass too. Its root set is the outputs of the
/// surviving instances, so an input blob whose producing instance is not in the
/// state at all — or was already evicted — is not in it and would be deleted
/// while a surviving consumer still names it. That is harmless only because a
/// cache hit replays the recorded outputs and never opens the inputs it lists:
/// the consumer's own bytes are roots, and its inputs are provenance. The claim
/// this phase can make is therefore about what re-execution needs, not about
/// what a record mentions. What it says about bytes no record mentions is what
/// the document declares: a byte an application supplies through a step input
/// has an `external_data` entry, which is a root, and a byte nothing declares is
/// an orphan by definition. An application that stages bytes before the document
/// declaring them exists is the one case where that definition costs it work:
/// the pass reclaims the staged bytes, and the next run imports them again.
///
/// # Errors
///
/// Returns [`ConductorError::Cas`] on any CAS operation failure.
pub(crate) async fn run_conductor_gc<C>(
    cas: &C,
    state: &mut ConductorState,
    unified: &UnifiedNickelDocument,
    ttl_seconds: u64,
) -> Result<ConductorGcReport, ConductorError>
where
    C: CasApi + CasMaintenanceApi,
{
    let counts = run_instance_gc(state, unified, ttl_seconds);

    // ---- Phase 2: Root-set collection + CAS orphan deletion ----
    tracing::info!("conductor GC phase 2/3: root-set collection and orphan reclamation");
    let mut root_set: BTreeSet<Hash> = BTreeSet::new();

    // Hashes referenced by surviving tool-call instance outputs, minus the ones
    // the run recorded as unsaved whose producer nothing live can reach. The walk
    // is recomputed rather than carried out of phase 1 because phase 1 abstains
    // without computing one, and an abstention is exactly the case where this
    // pass must keep every output.
    let reachable = counts.abstention.is_none().then(|| reachability_closure(state));
    for (instance_key, instance) in &state.tool_call_instances {
        for record in instance.outputs.values() {
            if keeps_output(record, instance_key, reachable.as_ref()) {
                root_set.insert(record.hash);
            }
        }
    }

    // External-data policy keys are CAS hashes that must be preserved.
    root_set.extend(unified.external_data_policies.keys().copied());

    // Tool-content-map hashes referenced by the config.
    root_set.extend(unified.tool_content_hashes.iter().copied());

    let all_hashes: BTreeSet<Hash> =
        cas.list_hashes().await.map_err(ConductorError::Cas)?.into_iter().collect();

    let orphans: Vec<&Hash> = all_hashes.difference(&root_set).collect();
    let orphans_removed = orphans.len();

    for hash in &orphans {
        // CONDUCTOR GC: deleting orphan CAS blob {hash}
        cas.delete(**hash).await.map_err(ConductorError::Cas)?;
    }

    // Flush pending WAL entries (deletes) so the maintenance step sees a
    // consistent state.
    cas.flush().await.map_err(ConductorError::Cas)?;

    // ---- Phase 3: CAS maintenance (optimize + prune constraints) ----
    tracing::info!("conductor GC phase 3/3: CAS maintenance");
    let prune_report = run_cas_gc_sweep(cas).await?;

    Ok(ConductorGcReport {
        instances_removed: counts.instances_removed,
        reachable_instances: counts.reachable_instances,
        superseded_instances: counts.superseded_instances,
        classification: counts
            .abstention
            .map_or(GcClassification::Classified, |reason| GcClassification::Abstained { reason }),
        orphans_removed,
        prune_report,
    })
}

/// What one instance-only pass decided.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct InstanceGcCounts {
    /// Records evicted because their grace period had expired.
    pub instances_removed: usize,
    /// Records the walk kept, whose clocks this pass refreshed.
    pub reachable_instances: usize,
    /// Records the walk did not reach, whether or not they were old enough to
    /// evict.
    pub superseded_instances: usize,
    /// Why the pass refused to decide, or `None` when it walked.
    ///
    /// The reason travels with the counts rather than only into a log line,
    /// because the caller that prints the pass result needs to say what the
    /// pass could not vouch for. An abstained pass classifies nothing, so the
    /// counts above stay zero and the caller must not read their absence as
    /// "nothing is reachable": no instance may lose its bytes on the strength
    /// of a walk the pass declined to use.
    pub abstention: Option<String>,
}

/// Runs full CAS maintenance: optimize, prune constraints.
///
/// Does not touch instance GC or orphan reclamation; callers run
/// `run_conductor_gc` for the full cycle.
///
/// # Errors
///
/// Returns [`ConductorError::Cas`] when any maintenance operation fails.
pub async fn run_cas_gc_sweep<C>(cas: &C) -> Result<PruneReport, ConductorError>
where
    C: CasApi + CasMaintenanceApi,
{
    tracing::info!("CAS GC phase 1/2: run_maintenance_cycle");
    cas.run_maintenance_cycle().await.map_err(ConductorError::Cas)?;
    tracing::info!("CAS GC phase 2/2: prune_constraints");
    let report = cas.prune_constraints().await.map_err(ConductorError::Cas)?;
    tracing::info!(
        "CAS GC phase 2/2: complete (targets={}, bases={}, markers={})",
        report.targets_removed,
        report.bases_removed,
        report.markers_removed,
    );
    Ok(report)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use mediapm_cas::{CasApi, InMemoryCas};
    use mediapm_utils::Timestamp;

    use super::*;
    use crate::config::{ConductorRuntimeConfig, SaveMode, WorkflowSpec};
    use std::ops::{Deref, DerefMut};

    use crate::state::{AuxData, CapturedOutput, ToolCallInstance};

    /// A grace period longer than any test run, so no record is evicted and the
    /// root set alone decides which bytes survive.
    const A_GRACE_PERIOD_THAT_OUTLASTS_THE_TEST: u64 = 86_400;

    /// One instance with the given inputs and outputs, keyed by `b"instance-{n}"`.
    ///
    /// Inputs are recorded under `in-{i}` and outputs under `out-{i}`, which is
    /// all the reachability walk reads: names are the step's business, hashes
    /// are the graph's.
    fn instance(label: &str, inputs: &[Hash], outputs: &[Hash]) -> FixtureInstance {
        FixtureInstance {
            key: Hash::from_content(label.as_bytes()),
            instance: recorded_instance(inputs, outputs),
        }
    }

    /// One fixture instance with the key it is filed under.
    ///
    /// Held as a pair because the record no longer carries its own key: the map
    /// key is the identity, so a fixture has to state it beside the record the
    /// way the store does. Deref keeps the tests reading fields off the record
    /// itself.
    struct FixtureInstance {
        /// The key the instance is filed under.
        key: Hash,
        /// The record itself.
        instance: ToolCallInstance,
    }

    impl Deref for FixtureInstance {
        type Target = ToolCallInstance;

        fn deref(&self) -> &Self::Target {
            &self.instance
        }
    }

    impl DerefMut for FixtureInstance {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.instance
        }
    }

    /// The record half of a fixture instance: inputs under `in-{i}`, outputs
    /// under `out-{i}`.
    fn recorded_instance(inputs: &[Hash], outputs: &[Hash]) -> ToolCallInstance {
        ToolCallInstance {
            tool_call_id: "echo@v1".to_string(),
            impure: false,
            invalidated_at: None,
            executed_at: Timestamp::default(),
            last_referenced_at: Timestamp::from_unix_nanos(0),
            step_ids: BTreeSet::new(),
            command_args: Vec::new(),
            env_vars: BTreeMap::new(),
            materialized_inputs: inputs
                .iter()
                .enumerate()
                .map(|(index, hash)| (format!("in-{index}"), *hash))
                .collect(),
            outputs: outputs
                .iter()
                .enumerate()
                .map(|(index, hash)| {
                    (format!("out-{index}"), CapturedOutput { hash: *hash, capture: None })
                })
                .collect(),
        }
    }

    /// A state holding `instances` with every `last_referenced_at` at the Unix
    /// epoch, so a zero-second TTL puts all of them past their grace period.
    fn state_with(
        instances: Vec<FixtureInstance>,
        seeds: BTreeMap<String, BTreeSet<Hash>>,
    ) -> ConductorState {
        let mut state = ConductorState::new_empty();
        state.tool_call_instances =
            instances.into_iter().map(|fixture| (fixture.key, fixture.instance)).collect();
        state.aux = AuxData { conductor_gc_epoch: Timestamp::now(), live_instances: seeds };
        state
    }

    /// `recorded` with every output recording `mode` as the capture spec it ran
    /// under, which is the fact the root set reads.
    fn instance_saving(recorded: FixtureInstance, mode: SaveMode) -> FixtureInstance {
        let mut saved = recorded;
        for output in saved.instance.outputs.values_mut() {
            output.capture = Some(crate::config::OutputCaptureSpec {
                name: String::new(),
                capture: "stdout".to_string(),
                save: mode,
                ..crate::config::OutputCaptureSpec::default()
            });
        }
        saved
    }

    /// Stamps every recorded instance's last reference at the present, so a pass
    /// inside the grace period evicts none of them.
    ///
    /// The eviction branch reads the epoch as "never referenced", which is what
    /// [`state_with`] installs, so a test that wants a record to survive the walk
    /// has to say when it was last read.
    fn keep_records_within_their_grace_period(state: &mut ConductorState) {
        let now = Timestamp::now();
        for instance in state.tool_call_instances.values_mut() {
            instance.last_referenced_at = now;
        }
    }

    /// A merged document declaring exactly the given workflow names.
    fn unified_with(workflow_names: &[&str]) -> UnifiedNickelDocument {
        UnifiedNickelDocument {
            tools: BTreeMap::new(),
            workflows: workflow_names
                .iter()
                .map(|name| ((*name).to_string(), WorkflowSpec::default()))
                .collect(),
            tool_content_hashes: BTreeSet::new(),
            external_data_policies: BTreeMap::new(),
            runtime: ConductorRuntimeConfig::default(),
        }
    }

    /// A document that declares no workflow is treated as unreadable, not as
    /// authorisation to sweep.
    ///
    /// Config discovery skips missing files on purpose and an empty merge is
    /// legal, so an empty workflow map is what a checkout with its config moved
    /// away looks like. The empty seed set that follows would have made the
    /// closure empty, and an empty closure evicts every instance past TTL — the
    /// wipe this rule exists to prevent.
    #[tokio::test]
    async fn a_document_that_declares_no_workflow_evicts_nothing() {
        let cas = InMemoryCas::new();
        let orphan = Hash::from_content(b"orphan");
        let mut state = state_with(vec![instance("orphan", &[], &[])], BTreeMap::new());

        run_conductor_gc(&cas, &mut state, &unified_with(&[]), 0).await.expect("gc");

        assert!(
            state.tool_call_instances.contains_key(&orphan),
            "an empty document must abstain, not sweep the store",
        );
    }

    /// A pass that declined to walk is a different outcome from a pass that
    /// walked an empty library, and the report has to say which happened.
    ///
    /// The walk's counts are zero in both cases, so the counts cannot carry the
    /// distinction on their own. An operator who reads zero as "nothing is
    /// reachable" believes the pass kept less than it did, and the empty-library
    /// side has to be asserted too: an empty library is the one case where the
    /// zero counts are the whole truth rather than a redaction.
    #[tokio::test]
    async fn an_abstained_pass_reports_a_different_outcome_than_an_empty_library() {
        let mut abstaining_state = state_with(vec![instance("orphan", &[], &[])], BTreeMap::new());
        let abstained = run_conductor_gc(
            &InMemoryCas::new(),
            &mut abstaining_state,
            &unified_with(&["declared"]),
            0,
        )
        .await
        .expect("gc");

        let mut empty_state =
            state_with(Vec::new(), BTreeMap::from([("declared".to_string(), BTreeSet::new())]));
        let emptied = run_conductor_gc(
            &InMemoryCas::new(),
            &mut empty_state,
            &unified_with(&["declared"]),
            0,
        )
        .await
        .expect("gc");

        let GcClassification::Abstained { reason } = &abstained.classification else {
            panic!("an unseeded workflow must abstain, got {:?}", abstained.classification);
        };
        assert!(
            reason.contains("declared"),
            "the reason must name the workflow with no seed, got {reason:?}",
        );

        assert_eq!(
            emptied.classification,
            GcClassification::Classified,
            "a walk over an empty library judged an empty library",
        );
        assert_eq!(
            emptied.reachable_instances, 0,
            "there was nothing to reach, and the walk says so",
        );
        assert_eq!(emptied.superseded_instances, 0, "an empty library supersedes nothing");
        assert_eq!(emptied.instances_removed, 0, "and evicts nothing");
        assert_eq!(emptied.orphans_removed, 0, "and reclaims no orphan blob");
    }

    /// An abstained report carries the reason, not only the refusal.
    ///
    /// The reason is what an operator acts on: a workflow with no recorded seed
    /// means a workflow that never ran, while an empty workflow map means a
    /// config discovery that found nothing. The two need different fixes.
    #[tokio::test]
    async fn an_abstained_report_names_what_it_could_not_vouch_for() {
        let mut state = state_with(vec![instance("orphan", &[], &[])], BTreeMap::new());

        let report =
            run_conductor_gc(&InMemoryCas::new(), &mut state, &unified_with(&["declared"]), 0)
                .await
                .expect("gc");

        let GcClassification::Abstained { reason } = &report.classification else {
            panic!("an unseeded workflow must abstain, got {:?}", report.classification);
        };
        assert!(
            reason.contains("declared"),
            "the reason must name the workflow with no seed, got {reason:?}",
        );
    }

    /// An empty workflow map and a missing seed are refusals with different
    /// reasons, so one message cannot serve both.
    #[tokio::test]
    async fn an_empty_workflow_map_abstains_for_its_own_reason() {
        let mut state = state_with(vec![instance("orphan", &[], &[])], BTreeMap::new());

        let report = run_conductor_gc(&InMemoryCas::new(), &mut state, &unified_with(&[]), 0)
            .await
            .expect("gc");

        let GcClassification::Abstained { reason } = &report.classification else {
            panic!("an empty workflow map must abstain, got {:?}", report.classification);
        };
        assert!(
            reason.contains("declares no workflow"),
            "the reason must name the missing document, got {reason:?}",
        );
    }

    /// A pass that walked a library and reached nothing is classified, not
    /// abstained, and the counts say which of the two classified shapes it is.
    ///
    /// Zero reachable instances with a superseded count above zero means the
    /// walk ran and the closure missed every record. A pass over an empty
    /// library also reports zero reachable, with nothing to supersede.
    #[tokio::test]
    async fn a_walk_that_reaches_nothing_is_classified_rather_than_abstained() {
        let seeded = Hash::from_content(b"seeded");
        let mut state = state_with(
            vec![instance("orphan", &[], &[seeded])],
            BTreeMap::from([("declared".to_string(), BTreeSet::from([seeded]))]),
        );
        keep_records_within_their_grace_period(&mut state);

        let report =
            run_conductor_gc(&InMemoryCas::new(), &mut state, &unified_with(&["declared"]), 0)
                .await
                .expect("gc");

        assert_eq!(
            report.classification,
            GcClassification::Classified,
            "the walk ran, so no refusal happened",
        );
        assert_eq!(report.reachable_instances, 0, "the seed names a hash, not the record");
        assert_eq!(report.superseded_instances, 1, "the record is outside the closure");
        assert_eq!(report.instances_removed, 0, "and inside its grace period");
    }

    /// An abstaining pass prunes no seed.
    ///
    /// The document can be legitimately incomplete for one run — a generated
    /// document is rewritten each sync — so a missing workflow is only evidence
    /// of deletion once the pass is otherwise describing the same world.
    #[tokio::test]
    async fn an_abstaining_pass_keeps_seeds_it_cannot_vouch_for() {
        let cas = InMemoryCas::new();
        let seeded = Hash::from_content(b"seeded");
        let kept = instance("kept", &[], &[seeded]);
        let mut state = state_with(
            vec![kept],
            BTreeMap::from([("removed".to_string(), BTreeSet::from([seeded]))]),
        );

        // "declared" has no seed, so this pass abstains.
        run_conductor_gc(&cas, &mut state, &unified_with(&["declared"]), 0).await.expect("gc");

        assert!(
            state.aux.live_instances.contains_key("removed"),
            "an abstaining pass must not prune a seed it cannot replace",
        );
        assert!(
            state.tool_call_instances.contains_key(&Hash::from_content(b"kept")),
            "and evicts nothing either",
        );
    }

    /// Two instances that consume each other's outputs terminate.
    ///
    /// The walk guards on membership before pushing, so a cycle closes instead
    /// of spinning; this pins the guard against a future "always push the
    /// producers" rewrite.
    #[test]
    fn a_hash_cycle_terminates() {
        let first_output = Hash::from_content(b"first");
        let second_output = Hash::from_content(b"second");
        let state = state_with(
            vec![
                instance("first", &[second_output], &[first_output]),
                instance("second", &[first_output], &[second_output]),
            ],
            BTreeMap::from([("loop".to_string(), BTreeSet::from([Hash::from_content(b"first")]))]),
        );

        let reached = reachability_closure(&state);

        assert_eq!(
            reached,
            BTreeSet::from([Hash::from_content(b"first"), Hash::from_content(b"second")]),
            "both members of the cycle are reachable and the walk stops",
        );
    }

    /// A producer stays alive while a live consumer's inputs name its blob.
    ///
    /// The user-level guarantee: garbage collection never removes the bytes a
    /// workflow still reads. The producer is absent from the seed — only the
    /// consumer ran and was recorded — so its survival proves the edge, not a
    /// seed entry.
    #[tokio::test]
    async fn a_producer_stays_reachable_while_a_live_consumer_lists_its_blob() {
        let cas = InMemoryCas::new();
        let blob = cas.put(bytes::Bytes::from_static(b"producer payload")).await.expect("put");
        let producer = instance("producer", &[], &[blob]);
        let consumer = instance("consumer", &[blob], &[]);
        let producer_key = producer.key;
        let consumer_key = consumer.key;
        let mut state = state_with(
            vec![producer, consumer],
            BTreeMap::from([("default".to_string(), BTreeSet::from([consumer_key]))]),
        );
        let unified = unified_with(&["default"]);

        let report = run_conductor_gc(&cas, &mut state, &unified, 0).await.expect("gc");

        assert!(
            state.tool_call_instances.contains_key(&producer_key),
            "the consumer lists the producer's blob, so the producer is reachable"
        );
        assert_eq!(report.reachable_instances, 2, "both ends of the edge are in the closure");
        assert_eq!(report.superseded_instances, 0, "nothing is outside the closure");
        assert_eq!(report.instances_removed, 0, "a reachable chain is never evicted");
    }

    /// The edge disappears when the consumer re-runs onto different inputs.
    ///
    /// The user-level guarantee: grace starts when the last reader moves on,
    /// not when the producer stops running. Both passes are asserted so the
    /// test cannot pass by evicting everything: the first keeps the producer,
    /// the second evicts it and reclaims its blob.
    #[tokio::test]
    async fn a_producer_becomes_unreachable_once_the_consumer_moves_on() {
        let cas = InMemoryCas::new();
        let blob = cas.put(bytes::Bytes::from_static(b"producer payload")).await.expect("put");
        let producer = instance("producer", &[], &[blob]);
        let consumer = instance("consumer", &[blob], &[]);
        let producer_key = producer.key;
        let consumer_key = consumer.key;
        let mut state = state_with(
            vec![producer, consumer],
            BTreeMap::from([("default".to_string(), BTreeSet::from([consumer_key]))]),
        );
        let unified = unified_with(&["default"]);

        run_conductor_gc(&cas, &mut state, &unified, 0).await.expect("first gc");
        assert!(
            state.tool_call_instances.contains_key(&producer_key),
            "the first pass must keep the producer, or the second proves nothing"
        );

        let moved_on = cas.put(bytes::Bytes::from_static(b"new inputs")).await.expect("put");
        state
            .tool_call_instances
            .get_mut(&consumer_key)
            .expect("consumer")
            .materialized_inputs
            .insert("in-0".to_string(), moved_on);

        let report = run_conductor_gc(&cas, &mut state, &unified, 0).await.expect("second gc");

        assert!(
            !state.tool_call_instances.contains_key(&producer_key),
            "nothing reads the producer's blob any more, so it ages out"
        );
        assert_eq!(report.superseded_instances, 1, "the walk classifies it as superseded");
        assert_eq!(report.instances_removed, 1, "past a zero-second grace, superseded means gone");
        assert!(
            cas.get(blob).await.is_err(),
            "the blob is reclaimed once no surviving record names it"
        );
        assert!(
            state.tool_call_instances.contains_key(&consumer_key),
            "the live consumer itself is seeded and stays"
        );
    }

    /// A workflow declared but never run makes the pass abstain.
    ///
    /// The user-level guarantee: the mistake is "kept too long", never "deleted
    /// in use". A closure computed from an absent seed is empty, and an empty
    /// closure past its grace period would empty the store.
    #[tokio::test]
    async fn a_workflow_without_a_seed_evicts_nothing() {
        let cas = InMemoryCas::new();
        let orphan_of_opportunity = instance("unseeded", &[], &[]);
        let instance_key = orphan_of_opportunity.key;
        let mut state = state_with(vec![orphan_of_opportunity], BTreeMap::new());
        let unified = unified_with(&["default"]);

        let report = run_conductor_gc(&cas, &mut state, &unified, 0).await.expect("gc");

        assert!(
            state.tool_call_instances.contains_key(&instance_key),
            "a workflow with no seed protects every recorded instance"
        );
        assert_eq!(report.instances_removed, 0, "an abstaining pass removes nothing");
        assert_eq!(report.reachable_instances, 0, "an abstaining pass classifies nothing");
        assert_eq!(report.superseded_instances, 0, "an abstaining pass classifies nothing");
    }

    /// An invalidated instance that a live workflow still reads keeps its bytes.
    ///
    /// The user-level guarantee: invalidation asks for a re-execution, it does
    /// not delete library content. The marker is cleared only by the run that
    /// replaces the bytes, so the record and its blobs must outlive the marker.
    #[tokio::test]
    async fn a_reachable_marked_instance_is_not_evicted() {
        let cas = InMemoryCas::new();
        let blob = cas.put(bytes::Bytes::from_static(b"marked payload")).await.expect("put");
        let mut producer = instance("producer", &[], &[blob]);
        producer.invalidated_at = Some(Timestamp::now());
        let producer_key = producer.key;
        let consumer = instance("consumer", &[blob], &[]);
        let consumer_key = consumer.key;
        let mut state = state_with(
            vec![producer, consumer],
            BTreeMap::from([("default".to_string(), BTreeSet::from([consumer_key]))]),
        );
        let unified = unified_with(&["default"]);

        let report = run_conductor_gc(&cas, &mut state, &unified, 0).await.expect("gc");

        assert!(
            state.tool_call_instances.contains_key(&producer_key),
            "closure membership outranks the invalidation marker"
        );
        assert!(
            state
                .tool_call_instances
                .get(&producer_key)
                .expect("producer")
                .invalidated_at
                .is_some(),
            "GC must not clear the marker; only a re-execution does that"
        );
        assert_eq!(report.reachable_instances, 2, "the marked producer is inside the closure");
        assert!(cas.get(blob).await.is_ok(), "its blob survives with it");
    }

    /// A seed for a workflow the document no longer declares is dropped.
    ///
    /// The user-level guarantee: deleting a workflow does not pin its instances
    /// forever. Their entry goes, they become unreachable, and the ordinary
    /// grace period takes over.
    #[tokio::test]
    async fn a_seed_for_a_removed_workflow_is_pruned_and_its_instances_age_out() {
        let cas = InMemoryCas::new();
        let abandoned = instance("abandoned", &[], &[]);
        let abandoned_key = abandoned.key;
        let mut state = state_with(
            vec![abandoned],
            BTreeMap::from([
                ("default".to_string(), BTreeSet::new()),
                ("gone".to_string(), BTreeSet::from([abandoned_key])),
            ]),
        );
        let unified = unified_with(&["default"]);

        let report = run_conductor_gc(&cas, &mut state, &unified, 0).await.expect("gc");

        assert!(
            !state.aux.live_instances.contains_key("gone"),
            "a workflow the document no longer declares must not keep its seed"
        );
        assert!(
            !state.tool_call_instances.contains_key(&abandoned_key),
            "with the seed gone the instance is unreachable and ages out"
        );
        assert_eq!(report.instances_removed, 1, "the abandoned instance is the one removed");
    }

    /// An output the run recorded as unsaved stops rooting once its producer is
    /// superseded.
    ///
    /// The user-level guarantee: saving has consequences. The record outlives the
    /// bytes, because the grace period is about the record, so the assertion that
    /// the record is still present is what keeps this from passing by eviction.
    #[tokio::test]
    async fn an_unsaved_output_of_a_superseded_instance_is_reclaimed() {
        let cas = InMemoryCas::new();
        let blob = cas.put(bytes::Bytes::from_static(b"intermediate payload")).await.expect("put");
        let producer = instance_saving(instance("producer", &[], &[blob]), SaveMode::False);
        let producer_key = producer.key;
        let mut state =
            state_with(vec![producer], BTreeMap::from([("default".to_string(), BTreeSet::new())]));
        keep_records_within_their_grace_period(&mut state);

        let report = run_conductor_gc(
            &cas,
            &mut state,
            &unified_with(&["default"]),
            A_GRACE_PERIOD_THAT_OUTLASTS_THE_TEST,
        )
        .await
        .expect("gc");

        assert!(
            state.tool_call_instances.contains_key(&producer_key),
            "the grace period is about the record, so the record stays while its bytes go"
        );
        assert!(
            cas.get(blob).await.is_err(),
            "an unsaved output is not a root once its producer is outside the closure"
        );
        assert_eq!(report.orphans_removed, 1, "the only unreachable blob is the unsaved output");
    }

    /// An unsaved output stays rooted while its producer is reachable.
    ///
    /// The user-level guarantee: a pass landing between one step's capture and the
    /// next step's read must not delete the bytes the consumer is about to open.
    #[tokio::test]
    async fn an_unsaved_output_of_a_reachable_instance_survives_the_pass() {
        let cas = InMemoryCas::new();
        let blob = cas.put(bytes::Bytes::from_static(b"intermediate payload")).await.expect("put");
        let producer = instance_saving(instance("producer", &[], &[blob]), SaveMode::False);
        let producer_key = producer.key;
        let mut state = state_with(
            vec![producer],
            BTreeMap::from([("default".to_string(), BTreeSet::from([producer_key]))]),
        );
        keep_records_within_their_grace_period(&mut state);

        let report = run_conductor_gc(
            &cas,
            &mut state,
            &unified_with(&["default"]),
            A_GRACE_PERIOD_THAT_OUTLASTS_THE_TEST,
        )
        .await
        .expect("gc");

        assert!(
            cas.get(blob).await.is_ok(),
            "a reachable producer's unsaved output is still being read by the run in flight"
        );
        assert_eq!(report.orphans_removed, 0, "nothing is reclaimable while the producer is live");
    }

    /// An unsaved output survives while a live consumer's inputs name it.
    ///
    /// The user-level guarantee: the mid-run guard follows the graph, not the seed.
    /// A producer that is not itself seeded is still reached through the consumer
    /// that lists its blob, so the bytes a running workflow is about to read outlive
    /// the pass even though the run recorded them as unsaved.
    #[tokio::test]
    async fn an_unsaved_output_a_live_consumer_reads_survives_the_pass() {
        let cas = InMemoryCas::new();
        let blob = cas.put(bytes::Bytes::from_static(b"intermediate payload")).await.expect("put");
        let producer = instance_saving(instance("producer", &[], &[blob]), SaveMode::False);
        let consumer = instance("consumer", &[blob], &[]);
        let consumer_key = consumer.key;
        let mut state = state_with(
            vec![producer, consumer],
            BTreeMap::from([("default".to_string(), BTreeSet::from([consumer_key]))]),
        );
        keep_records_within_their_grace_period(&mut state);

        run_conductor_gc(
            &cas,
            &mut state,
            &unified_with(&["default"]),
            A_GRACE_PERIOD_THAT_OUTLASTS_THE_TEST,
        )
        .await
        .expect("gc");

        assert!(
            cas.get(blob).await.is_ok(),
            "the live consumer lists the blob, so its producer is reachable and the bytes stay"
        );
    }

    /// Retention reads the record, so a `save`-only config change cannot revoke
    /// bytes the run recorded as kept.
    ///
    /// The user-level guarantee: what the document says now does not decide what a
    /// past run promised. `same_capture_identity` excludes `save`, so a step that
    /// now declares `false` still hits the instance that ran under `true`, and that
    /// instance's record is what this pass reads. Both kept modes are asserted so
    /// the test cannot pass by treating every spec as saved.
    #[tokio::test]
    async fn a_save_only_change_does_not_retroactively_change_retention() {
        let cas = InMemoryCas::new();
        let saved = cas.put(bytes::Bytes::from_static(b"saved payload")).await.expect("put");
        let full = cas.put(bytes::Bytes::from_static(b"full payload")).await.expect("put");
        let producer = instance("producer", &[], &[saved, full]);
        let mut producer = instance_saving(producer, SaveMode::True);
        producer.outputs.get_mut("out-1").expect("second output").capture =
            Some(crate::config::OutputCaptureSpec {
                save: SaveMode::Full,
                ..crate::config::OutputCaptureSpec::default()
            });
        let mut state =
            state_with(vec![producer], BTreeMap::from([("default".to_string(), BTreeSet::new())]));
        keep_records_within_their_grace_period(&mut state);

        run_conductor_gc(
            &cas,
            &mut state,
            &unified_with(&["default"]),
            A_GRACE_PERIOD_THAT_OUTLASTS_THE_TEST,
        )
        .await
        .expect("gc");

        assert!(cas.get(saved).await.is_ok(), "a recorded `true` output stays a root");
        assert!(cas.get(full).await.is_ok(), "a recorded `full` output stays a root");
    }

    /// A record too old to carry a capture spec still roots its output.
    ///
    /// The user-level guarantee: a missing spec is not evidence of unsavedness, so
    /// upgrading a state file must not turn its outputs into orphans.
    #[tokio::test]
    async fn a_record_without_a_capture_spec_still_roots_its_output() {
        let cas = InMemoryCas::new();
        let blob = cas.put(bytes::Bytes::from_static(b"legacy payload")).await.expect("put");
        let producer = instance("producer", &[], &[blob]);
        let mut state =
            state_with(vec![producer], BTreeMap::from([("default".to_string(), BTreeSet::new())]));
        keep_records_within_their_grace_period(&mut state);

        run_conductor_gc(
            &cas,
            &mut state,
            &unified_with(&["default"]),
            A_GRACE_PERIOD_THAT_OUTLASTS_THE_TEST,
        )
        .await
        .expect("gc");

        assert!(
            cas.get(blob).await.is_ok(),
            "a record with no capture spec roots by default, because it cannot say otherwise"
        );
    }

    /// An abstained pass keeps an unsaved output rooted.
    ///
    /// The user-level guarantee: a pass that cannot tell which instances are live
    /// must not act on its doubt. The document declares a workflow that recorded
    /// no seed, so the closure says nothing about the producer, and reclaiming its
    /// bytes on that basis would be the same mistake as evicting its record.
    #[tokio::test]
    async fn an_abstaining_pass_keeps_an_unsaved_output_rooted() {
        let cas = InMemoryCas::new();
        let blob = cas.put(bytes::Bytes::from_static(b"intermediate payload")).await.expect("put");
        let producer = instance_saving(instance("producer", &[], &[blob]), SaveMode::False);
        let mut state = state_with(vec![producer], BTreeMap::new());
        keep_records_within_their_grace_period(&mut state);

        let report = run_conductor_gc(
            &cas,
            &mut state,
            &unified_with(&["default"]),
            A_GRACE_PERIOD_THAT_OUTLASTS_THE_TEST,
        )
        .await
        .expect("gc");

        assert!(
            cas.get(blob).await.is_ok(),
            "an abstained pass does not know the producer is disposable, so it keeps its bytes"
        );
        assert_eq!(report.orphans_removed, 0, "an abstained pass reclaims nothing");
    }
}
