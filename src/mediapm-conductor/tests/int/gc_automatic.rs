//! Automatic (background) conductor GC coverage.
//!
//! The conductor's background loop runs `gc::run_conductor_gc` on a timer, so
//! these scenarios pin what a pass does without an operator asking for one:
//! what it keeps (live records, declared bytes), what it refuses to judge
//! (an abstaining pass), and what it reclaims (bytes no declaration roots).
//!
//! This module also owns [`backdate`], the shared test helper the GC scenarios
//! use to age a record past a grace period without sleeping through it: seven
//! days is not a test duration, so the record's own clock is moved instead.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use bytes::Bytes;
use mediapm_cas::{CasApi, Hash, InMemoryCas};
use mediapm_conductor::gc::GcClassification;
use mediapm_conductor::{
    CapturedOutput, Conductor, ConductorState, ExternalDataEntry, OutputCaptureSpec,
    OutputSaveMode, RunSummary, RunWorkflowOptions, RuntimeStoragePaths, WorkflowStepSpec,
    decode_document,
};
use mediapm_utils::Timestamp;

use super::gc::sample_instance;
use crate::{
    doc_with_workflows, echo_step, echo_tool, workflow_with_steps, write_conductor_config,
};

/// Rewrites one instance's `last_referenced_at` to `secs_ago` seconds before
/// now, so a pass judges it as that old.
///
/// The instance must already exist in `state`; `key` is its hex content hash
/// (as `Hash::to_hex` prints it), because that is the map key it is filed
/// under. This only moves the record's own clock  -  seeds, the GC epoch, and
/// every other record are untouched, so the next pass measures exactly the
/// age this helper wrote against the epoch that pass finds.
///
/// # Panics
///
/// Panics when `key` is not a parseable hash or names no recorded instance:
/// a backdate that silently did nothing would let a TTL assertion pass
/// against a record that never aged.
pub(crate) fn backdate(state: &mut ConductorState, key: &str, secs_ago: u64) {
    let key: mediapm_cas::Hash = key.parse().expect("backdate key must be a content hash in hex");
    let age_nanos = secs_ago.saturating_mul(1_000_000_000);
    let backdated = Timestamp::now().as_unix_nanos().saturating_sub(age_nanos);
    let instance = state
        .tool_call_instances
        .get_mut(&key)
        .expect("backdate target must be a recorded instance");
    instance.last_referenced_at = Timestamp::from_unix_nanos(backdated);
}

/// The grace period every fixture in this module configures: one day. It is
/// what the scenarios age records past, so an aged record is evictable under
/// the settings while a fresh one is not.
const TTL_SECONDS: u64 = 86_400;

/// How far a scenario ages an unreachable record: past this module's
/// configured TTL and past the built-in seven-day default as well, so the
/// record is evictable under either grace period. `cli_gc` reuses it for the
/// record the spawned binary judges.
pub(crate) const BACKDATE_SECONDS: u64 = 700_000;

/// A producer step whose stdout is captured as `result`.
///
/// Workflows `a` and `b` both run this step with identical key material, so
/// they share one instance record; that sharing is what the keep-claim in
/// [`the_background_pass_keeps_what_is_live_and_what_is_declared`] asserts
/// over.
fn producer_step(text: &str) -> WorkflowStepSpec {
    WorkflowStepSpec {
        id: "producer".to_string(),
        tool: "echo@v1".to_string(),
        inputs: BTreeMap::from([("text".to_string(), text.to_string())]),
        outputs: BTreeMap::from([(
            "result".to_string(),
            OutputCaptureSpec {
                name: "result".to_string(),
                capture: "stdout".to_string(),
                ..OutputCaptureSpec::default()
            },
        )]),
        max_retries: 0,
        depends_on: Vec::new(),
    }
}

/// A consumer step reading the shared producer's output, so workflow `b`
/// reaches the record workflow `a` created instead of running a copy of its
/// own.
fn consumer_step() -> WorkflowStepSpec {
    WorkflowStepSpec {
        id: "consumer".to_string(),
        tool: "echo@v1".to_string(),
        inputs: BTreeMap::from([(
            "text".to_string(),
            "${step_output.producer.result}".to_string(),
        )]),
        outputs: BTreeMap::from([(
            "result".to_string(),
            OutputCaptureSpec {
                name: "result".to_string(),
                capture: "stdout".to_string(),
                ..OutputCaptureSpec::default()
            },
        )]),
        max_retries: 0,
        depends_on: vec!["producer".to_string()],
    }
}

/// One shared fixture for this module's scenarios: the document below, its
/// facade built with `with_gc_settings`, and the hash the document declares.
struct AutomaticGc {
    /// Tempdir owning the workspace root, so the config, state file and store
    /// live exactly as long as the facade reading them.
    dir: mediapm_utils::temp::ManagedTempDir,
    /// The facade under test, carrying the scenario's GC settings.
    conductor: Conductor<InMemoryCas>,
    /// The hash the document declares in `external_data`.
    declared: Hash,
}

/// Builds the shared fixture.
///
/// The document declares three workflows  -  `a` (the producer), `b` (the
/// producer again plus a consumer, so the two share one record) and `w`  -
/// plus one `external_data` declaration whose bytes are staged at
/// construction. `w` is what makes abstention testable: while it stays unrun
/// every pass abstains, and a scenario that needs a judged pass runs it first.
///
/// `background_interval_secs` is the scenario's to choose: the loop-timing
/// scenario ticks every second because the loop is its subject, and the
/// manual-pass scenarios use a day so the loop's single first pass fires
/// once, during the first workflow run, and then sleeps out of their way.
///
/// # Panics
///
/// Panics when the tempdir, the config write, the facade or the staging of
/// the declared bytes fails: a fixture that half-built would make every
/// assertion in a scenario vacuous.
async fn complicated(background_interval_secs: u64) -> AutomaticGc {
    let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    let declared_bytes: &'static [u8] = b"external bytes every scenario in this module must keep";
    let declared = Hash::from_content(declared_bytes);
    let mut document = doc_with_workflows(
        BTreeMap::from([("echo@v1".to_string(), echo_tool("echo@v1"))]),
        vec![
            workflow_with_steps("a", vec![producer_step("payload")]),
            workflow_with_steps("b", vec![producer_step("payload"), consumer_step()]),
            workflow_with_steps(
                "w",
                vec![echo_step("echo@v1", "w runs only when a scenario asks")],
            ),
        ],
    );
    document.external_data.insert(
        declared,
        ExternalDataEntry { description: None, save_mode: OutputSaveMode::Saved },
    );
    write_conductor_config(dir.path(), document);

    let conductor = Conductor::new(RuntimeStoragePaths::new(dir.path()), InMemoryCas::new())
        .with_gc_settings(TTL_SECONDS, background_interval_secs);
    conductor
        .cas()
        .put(Bytes::from_static(declared_bytes))
        .await
        .expect("stage the declared bytes");
    AutomaticGc { dir, conductor, declared }
}

impl AutomaticGc {
    /// Runs one declared workflow to completion and returns its summary.
    ///
    /// # Panics
    ///
    /// Panics when the run fails: a scenario asserting over a failed run
    /// would be asserting over a fixture that never finished building.
    async fn run(&self, workflow: &str) -> RunSummary {
        self.conductor
            .run_workflow(workflow, RunWorkflowOptions::default())
            .await
            .expect("workflow run")
    }

    /// Snapshots the recorded instance keys and every output blob hash they
    /// name, so a scenario compares exact sets rather than counts.
    fn snapshot(&self) -> (BTreeSet<Hash>, BTreeSet<Hash>) {
        let state = self.conductor.get_state().expect("state");
        let keys = state.tool_call_instances.keys().copied().collect();
        let blobs = state
            .tool_call_instances
            .values()
            .flat_map(|instance| instance.outputs.values().map(|output| output.hash))
            .collect();
        (keys, blobs)
    }

    /// Inserts an unreachable record with one staged output blob and ages it
    /// past every grace period in play, all before the first pass exists to
    /// race the insertion.
    ///
    /// No seed names the record, so it is garbage from the moment it exists;
    /// the backdate is what makes it evictable rather than merely garbage.
    ///
    /// # Panics
    ///
    /// Panics when staging or insertion fails: a scenario whose orphan never
    /// entered the store would assert over nothing.
    async fn insert_aged_orphan(&self, bytes: &'static [u8], marker: &str) -> (Hash, Hash) {
        let key = Hash::from_content(marker.as_bytes());
        let blob = self
            .conductor
            .cas()
            .put(Bytes::from_static(bytes))
            .await
            .expect("stage the orphan's output");
        let mut state = self.conductor.get_state().expect("state");
        let mut instance = sample_instance(key);
        instance.outputs =
            BTreeMap::from([("result".to_string(), CapturedOutput { hash: blob, capture: None })]);
        state.tool_call_instances.insert(key, instance);
        backdate(&mut state, &key.to_hex(), BACKDATE_SECONDS);
        self.conductor.replace_resolved_state(state).expect("insert the orphan");
        (key, blob)
    }

    /// Whether one blob still resolves in the store.
    async fn holds(&self, blob: Hash) -> bool {
        self.conductor.cas().stat(blob).await.is_ok()
    }

    /// Declares `hash` in the fixture's document, the way a sync that
    /// imported the bytes would.
    ///
    /// The file is re-read and re-written whole, so a scenario can add a
    /// declaration after construction without holding the document around.
    ///
    /// # Panics
    ///
    /// Panics when the document cannot be read, decoded or written: a
    /// declaration that silently failed to land would leave the next pass
    /// reclaiming bytes the scenario believes it declared.
    fn declare(&self, hash: Hash) {
        let path = self.dir.path().join("conductor.ncl");
        let bytes = std::fs::read(&path).expect("read the fixture document");
        let mut document = decode_document(&bytes).expect("decode the fixture document");
        document.external_data.insert(
            hash,
            ExternalDataEntry { description: None, save_mode: OutputSaveMode::Saved },
        );
        write_conductor_config(self.dir.path(), document);
    }
}

/// The background pass keeps what is live and what is declared.
///
/// The user-level guarantee: with the loop ticking on its own, an
/// unreachable record past the configured TTL is reclaimed, while the
/// records a seed reaches, the blobs those records produced and the bytes
/// the document declares in `external_data` all stay  -  so the automatic
/// pass applies the same contract as an operator's, and a repeat run still
/// reuses what it kept.
///
/// The fixture inserts the unreachable record before the first workflow run,
/// because the loop's first pass starts with that run and a later insertion
/// could race it.
#[tokio::test]
async fn the_background_pass_keeps_what_is_live_and_what_is_declared() {
    let fixture = complicated(1).await;
    let (superseded, superseded_blob) = fixture
        .insert_aged_orphan(b"bytes only the superseded record names", "superseded record")
        .await;

    fixture.run("a").await;
    fixture.run("b").await;
    fixture.run("w").await;

    let (baseline_keys, baseline_blobs) = fixture.snapshot();
    assert!(
        baseline_keys.contains(&superseded),
        "the insert and the three runs must both be in the state a pass reads"
    );

    // The loop ticks once a second: one classified pass starts the GC clock
    // and the next judges the record against it, so eviction lands within a
    // couple of ticks. The deadline fails the test rather than waiting the
    // configured grace period.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let state = fixture.conductor.get_state().expect("state");
        if !state.tool_call_instances.contains_key(&superseded) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the 1s background tick must reclaim a record past its TTL; the record is still recorded"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let (after_keys, _) = fixture.snapshot();
    let mut expected_keys = baseline_keys;
    assert!(expected_keys.remove(&superseded), "the baseline held the record");
    assert_eq!(
        after_keys, expected_keys,
        "exactly the unreachable record goes; no seeded record may disappear with it"
    );
    let mut expected_blobs = baseline_blobs;
    assert!(expected_blobs.remove(&superseded_blob), "the baseline held the blob");
    for blob in &expected_blobs {
        assert!(fixture.holds(*blob).await, "a kept record's output blob must stay readable");
    }
    assert!(
        fixture.holds(fixture.declared).await,
        "declared external bytes must stay rooted through the automatic pass"
    );
    assert!(
        !fixture.holds(superseded_blob).await,
        "the evicted record's blob must be reclaimed with it"
    );

    // What the pass kept still serves a repeat run.
    let again = fixture.run("b").await;
    assert_eq!(
        (again.executed_steps, again.cached_steps),
        (0, 2),
        "a repeat run must reuse both kept records: {again:?}"
    );

    // A restarted facade passes again over the same persisted library and
    // finds nothing left to remove: the sweep is idempotent across ticks and
    // across restarts.
    fixture.conductor.shutdown().await.expect("shutdown");
    let reopened = Conductor::new(RuntimeStoragePaths::new(fixture.dir.path()), InMemoryCas::new())
        .with_gc_settings(TTL_SECONDS, 1);
    reopened.ensure_persisted_state_loaded().await.expect("load persisted state");
    let second_pass = reopened.run_gc().await.expect("a pass over the restarted facade");
    assert!(
        matches!(&second_pass.classification, GcClassification::Classified),
        "the seeds persisted, so the restarted pass judges: {:?}",
        second_pass.classification
    );
    assert_eq!(
        second_pass.instances_removed, 0,
        "a pass over an already-swept library must remove nothing extra"
    );
    let reopened_state = reopened.get_state().expect("state");
    let reopened_keys: BTreeSet<Hash> =
        reopened_state.tool_call_instances.keys().copied().collect();
    assert_eq!(
        reopened_keys, after_keys,
        "the second pass must leave exactly the records the sweep left"
    );
    reopened.shutdown().await.expect("shutdown");
}

/// An abstaining pass freezes the library until the declared workflow runs.
///
/// The user-level guarantee: a document declaring a workflow that has never
/// run makes every pass abstain  -  an unreachable record and its blob survive
/// no matter how old they are, because a walk the pass cannot trust is no
/// evidence bytes are disposable. Once the declared workflow runs the pass
/// judges, and a record past the TTL goes with its blob.
///
/// The one-day interval keeps the loop quiet after its own first pass (which
/// abstains too, `w` being unrun at that point), so every report here is the
/// manual one this scenario drives.
#[tokio::test]
async fn an_abstaining_pass_freezes_until_the_declared_workflow_runs() {
    let fixture = complicated(86_400).await;
    let (aged, aged_blob) =
        fixture.insert_aged_orphan(b"bytes only the aged record names", "aged orphan").await;

    fixture.run("a").await;
    fixture.run("b").await;
    // `w` stays declared and unrun: that is the abstention.

    let frozen = fixture.conductor.run_gc().await.expect("a pass over the abstaining library");
    match &frozen.classification {
        GcClassification::Abstained { reason } => assert!(
            reason.contains("seed: w"),
            "the reason must name the workflow that never ran: {reason}"
        ),
        other @ GcClassification::Classified => {
            panic!("an unseeded declared workflow must abstain, got {other:?}")
        }
    }
    assert!(
        fixture.conductor.get_state().expect("state").tool_call_instances.contains_key(&aged),
        "an abstaining pass must not evict, however old the record is"
    );
    assert!(
        fixture.holds(aged_blob).await,
        "an abstaining pass must leave the aged record's blob alone"
    );

    // The declared workflow runs, so the walk is complete from here. The
    // first classified pass only starts the GC clock (the epoch contract in
    // `ConductorState::run_conductor_gc`), and the pass after it judges  -
    // two passes at most, because the loop's own first pass may already have
    // started the clock.
    fixture.run("w").await;
    let judged = fixture.conductor.run_gc().await.expect("a judged pass");
    assert!(
        matches!(&judged.classification, GcClassification::Classified),
        "every declared workflow now has a seed: {:?}",
        judged.classification
    );
    let swept = fixture.conductor.run_gc().await.expect("the judging pass");
    assert!(
        matches!(&swept.classification, GcClassification::Classified),
        "every declared workflow still has a seed: {:?}",
        swept.classification
    );
    assert!(
        !fixture.conductor.get_state().expect("state").tool_call_instances.contains_key(&aged),
        "once judged, a record past its TTL must be evicted"
    );
    assert!(!fixture.holds(aged_blob).await, "the evicted record's blob goes with it");
}

/// Bytes no declaration names are reclaimed, and the next import repairs them.
///
/// The user-level guarantee (`gc.rs` module docs, "a stale tool content
/// entry"): an application that stages bytes before the document declaring
/// them exists has those bytes reclaimed by the next pass; re-importing them
/// restores the store, and declaring them keeps them. The declared blob
/// staged beside them is the control  -  the same pass keeps what the document
/// roots.
///
/// The one-day interval again: the loop's single first pass fires during the
/// first workflow run, before these bytes exist, so the pass whose report is
/// asserted here is the manual one.
#[tokio::test]
async fn staged_bytes_without_a_declaration_are_reclaimed_and_repaired() {
    let fixture = complicated(86_400).await;
    fixture.run("a").await;
    fixture.run("b").await;
    fixture.run("w").await;

    let (baseline_keys, _) = fixture.snapshot();
    let staged_bytes: &'static [u8] = b"bytes no document declares yet";
    let staged = fixture
        .conductor
        .cas()
        .put(Bytes::from_static(staged_bytes))
        .await
        .expect("stage the undeclared bytes");

    let report = fixture.conductor.run_gc().await.expect("a classified pass");
    assert!(
        matches!(&report.classification, GcClassification::Classified),
        "every declared workflow has a seed: {:?}",
        report.classification
    );
    assert!(
        report.orphans_removed >= 1,
        "bytes outside every root set must count as reclaimed: {report:?}"
    );
    assert!(!fixture.holds(staged).await, "the pass must reclaim bytes no declaration names");
    assert!(
        fixture.holds(fixture.declared).await,
        "the declared control must survive the same pass"
    );
    let (after_keys, _) = fixture.snapshot();
    assert_eq!(after_keys, baseline_keys, "blob reclamation must not touch instance records");

    // The repair: the next sync re-imports the bytes, and content addressing
    // gives them the same hash back.
    let restored = fixture
        .conductor
        .cas()
        .put(Bytes::from_static(staged_bytes))
        .await
        .expect("re-import the bytes");
    assert_eq!(restored, staged, "a re-import of the same bytes hashes the same");
    assert!(fixture.holds(staged).await, "the re-import restores the bytes");

    // Declaring them is what ends the loop: the next pass roots them.
    fixture.declare(staged);
    let declared_pass = fixture.conductor.run_gc().await.expect("the pass after the declaration");
    assert!(
        matches!(&declared_pass.classification, GcClassification::Classified),
        "the declaration changes no seed: {:?}",
        declared_pass.classification
    );
    assert!(
        fixture.holds(staged).await,
        "declared bytes must stay rooted through the pass after their declaration"
    );
}
