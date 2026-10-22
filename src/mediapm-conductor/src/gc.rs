//! Garbage collection orchestration for conductor.
//!
//! Conductor GC covers state-instance pruning, CAS orphan reclamation, and CAS
//! metadata maintenance. It is distinct from CAS-internal GC.
//!
//! # Two entry points, on purpose
//!
//! `run_conductor_gc` is the full cycle, and only the explicit `conductor gc`
//! command runs it. It deletes blobs, so it assumes what an operator invoking it
//! assumes: nothing is in flight, and every byte the store holds is either
//! recorded in the state or described by the merged document.
//!
//! `run_record_maintenance` is what the background loop runs. It evicts
//! instance records and maintains the CAS metadata, and it deletes no payload.
//! A timer cannot assume the full cycle's premise: an application sharing the
//! store can hold bytes it has staged but not yet recorded anywhere, and no root
//! set computed from the conductor's own state and document can name them. The
//! cost is that unattended maintenance never frees payload bytes; the explicit
//! command does.
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
//! Two rules keep a GC pass from destroying live work. A workflow declared in
//! the merged document but carrying no seed makes the whole pass abstain —
//! refreshing and evicting nothing, logged once — because a closure computed
//! from an absent seed would evict the entire store. And closure membership
//! outranks `invalidated_at`: a marked instance is still what some live consumer
//! reads today, so the marker must not become a deletion before the
//! re-execution that replaces it has run.

use std::collections::{BTreeMap, BTreeSet};

use mediapm_cas::{CasApi, CasMaintenanceApi, Hash, PruneReport};

use crate::error::ConductorError;
use crate::orchestration::protocol::UnifiedNickelDocument;

use crate::state::ConductorState;

/// Aggregate report from one full conductor GC cycle.
#[derive(Debug, Clone, Default)]
pub struct ConductorGcReport {
    /// Number of tool-call instances evicted by the TTL grace period.
    pub instances_removed: usize,
    /// Number of instances the reachability walk kept, whose
    /// `last_referenced_at` this pass refreshed.
    ///
    /// Zero when the pass abstains for want of a seed, because nothing was
    /// classified: the count is a statement about the walk, not about how many
    /// instances happen to exist.
    pub reachable_instances: usize,
    /// Number of recorded instances the walk did not reach.
    ///
    /// A superseded instance is not removed unless its grace period has
    /// expired, so this count can exceed [`Self::instances_removed`]. Zero when
    /// the pass abstains.
    pub superseded_instances: usize,
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
            let Some(instance_keys) = producers.get(&input.hash) else {
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
        };
    };

    tracing::info!("conductor GC phase 1/3: {reason}, so this pass refreshes and evicts nothing");
    InstanceGcCounts::default()
}

/// Runs the background maintenance cycle: instance eviction, then CAS metadata
/// maintenance, and never a payload deletion.
///
/// [`run_conductor_gc`] without its orphan phase. The metadata half —
/// [`run_cas_gc_sweep`] — rewrites how existing objects are encoded and drops
/// constraint entries whose targets or bases no longer exist; neither can remove
/// an object's bytes, which is what makes it safe on a timer.
///
/// # Errors
///
/// Returns [`ConductorError::Cas`] on any CAS operation failure.
pub(crate) async fn run_record_maintenance<C>(
    cas: &C,
    state: &mut ConductorState,
    unified: &UnifiedNickelDocument,
    ttl_seconds: u64,
) -> Result<ConductorGcReport, ConductorError>
where
    C: CasApi + CasMaintenanceApi,
{
    let counts = run_instance_gc(state, unified, ttl_seconds);
    tracing::info!("conductor maintenance: CAS metadata maintenance (no orphan deletion)");
    let prune_report = run_cas_gc_sweep(cas).await?;
    Ok(ConductorGcReport {
        instances_removed: counts.instances_removed,
        reachable_instances: counts.reachable_instances,
        superseded_instances: counts.superseded_instances,
        orphans_removed: 0,
        prune_report,
    })
}

/// Runs full conductor garbage collection: instance pruning, CAS orphan
/// deletion, and CAS maintenance.
///
/// This is the operator path (`conductor gc`, and the `Conductor::run_gc`
/// facade method). Deleting orphans assumes the caller chose the moment:
/// nothing is in flight, and every live byte is either recorded in the state or
/// described by the merged document. A background timer may not make that
/// assumption — see [`run_record_maintenance`], which is the cycle the timer
/// runs.
///
/// # Phases
///
/// 1. [`run_instance_gc`] — evict the records the walk did not keep.
/// 2. Root-set / orphan reclamation — collect referenced CAS hashes from
///    surviving instances and the unified config, then delete orphans.
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
/// what a record mentions. It says nothing at all about bytes no record mentions
/// yet — staged inputs, for instance — which is why the phase is not on a timer.
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

    // Hashes referenced by surviving tool-call instance outputs.
    for instance in state.tool_call_instances.values() {
        for record in instance.outputs.values() {
            root_set.insert(record.hash);
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
        orphans_removed,
        prune_report,
    })
}

/// What one instance-only pass decided.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct InstanceGcCounts {
    /// Records evicted because their grace period had expired.
    pub instances_removed: usize,
    /// Records the walk kept, whose clocks this pass refreshed.
    pub reachable_instances: usize,
    /// Records the walk did not reach, whether or not they were old enough to
    /// evict.
    pub superseded_instances: usize,
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
    tracing::info!("CAS GC phase 2/2: complete (removed={})", report.removed);
    Ok(report)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use mediapm_cas::{CasApi, InMemoryCas};
    use mediapm_utils::Timestamp;

    use super::*;
    use crate::config::{ConductorRuntimeConfig, WorkflowSpec};
    use crate::state::{AuxData, CapturedOutput, HashedValueRecord, InstanceAux, ToolCallInstance};

    /// One instance with the given inputs and outputs, keyed by `b"instance-{n}"`.
    ///
    /// Inputs are recorded under `in-{i}` and outputs under `out-{i}`, which is
    /// all the reachability walk reads: names are the step's business, hashes
    /// are the graph's.
    fn instance(label: &str, inputs: &[Hash], outputs: &[Hash]) -> ToolCallInstance {
        let hash = |text: &str| Hash::from_content(text.as_bytes());
        ToolCallInstance {
            instance_key: hash(label),
            tool_call_id: "echo@v1".to_string(),
            impure: false,
            invalidated_at: None,
            executed_at: Timestamp::default(),
            step_ids: BTreeSet::new(),
            command_args: Vec::new(),
            env_vars: BTreeMap::new(),
            materialized_inputs: inputs
                .iter()
                .enumerate()
                .map(|(index, hash)| {
                    (format!("in-{index}"), HashedValueRecord { hash: *hash, deterministic: true })
                })
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
        instances: Vec<ToolCallInstance>,
        seeds: BTreeMap<String, BTreeSet<Hash>>,
    ) -> ConductorState {
        let mut state = ConductorState::new_empty();
        state.tool_call_instances =
            instances.into_iter().map(|instance| (instance.instance_key, instance)).collect();
        state.aux = AuxData {
            tool_call_instance_counter: 0,
            conductor_gc_epoch: Timestamp::now(),
            instances: state
                .tool_call_instances
                .keys()
                .map(|key| {
                    (*key, InstanceAux { last_referenced_at: Timestamp::from_unix_nanos(0) })
                })
                .collect(),
            live_instances: seeds,
        };
        state
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
        let producer_key = producer.instance_key;
        let consumer_key = consumer.instance_key;
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
        let producer_key = producer.instance_key;
        let consumer_key = consumer.instance_key;
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
            .insert("in-0".to_string(), HashedValueRecord { hash: moved_on, deterministic: true });

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
        let instance_key = orphan_of_opportunity.instance_key;
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
        let producer_key = producer.instance_key;
        let consumer = instance("consumer", &[blob], &[]);
        let consumer_key = consumer.instance_key;
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
        let abandoned_key = abandoned.instance_key;
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
}
