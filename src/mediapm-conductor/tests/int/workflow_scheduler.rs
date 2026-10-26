//! Ready-queue scheduler tests: dependency-counter dispatch over a free
//! worker pool, with no level barrier, and dependency-scoped request views.
//!
//! Ordering is observed through the test-only `slow@v1` builtin, which
//! appends `start <label>` / `finish <label>` lines to a per-test event log
//! and (optionally) holds until the log contains an awaited line. The hold
//! makes every asserted ordering a fact of the schedule rather than a race
//! against wall-clock time: a step that must outlive another step's start
//! waits for a line that step's builtin appends, so the assertion holds on
//! any machine speed. Under the old level-barrier dispatch these tests fail
//! cleanly: the holder's wait exhausts its timeout, the step errors, and the
//! run reports a failed step.
//!
//! Request visibility (what a step's `step_outputs` / `state_snapshot`
//! carry) is observed through template resolution: an echo producer step
//! declares a `result` output, and a probe step's input carries a
//! `${step_output.<id>.<name>}` reference. Input resolution runs before the
//! step's builtin, so a reference inside the step's transitive-dependency
//! closure resolves (the step runs and logs its start line) while a
//! reference outside it fails the step during resolution — the absence of a
//! start line in the event log is the observable proof that the output was
//! NOT visible, and the always-captured `process_code` output lets probes
//! target non-echo producers too.
//!
//! Tests need no feature of their own: the doubles they dispatch to
//! (`slow@v1` and `flaky@v1`) come from the `test-builtins` feature, and the
//! integration harness refuses to build without it.
//!
//! Retry release is observed structurally, not through the event log: the
//! retry tests use the `flaky@v1` builtin (fails its first N invocations
//! per key, then succeeds) and pin the property by making a run whose
//! retry arm failed to return its worker to the free pool unable to
//! complete at all.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::{TestConductor, doc_with_workflows};
use mediapm_conductor::api::RunWorkflowOptions;
use mediapm_conductor::{
    OutputCaptureSpec, ToolInputKind, ToolInputSpec, ToolKindSpec, ToolRuntime, ToolSpec,
    WorkflowSpec, WorkflowStepSpec,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Pins the worker pool size so scheduler assertions are deterministic.
///
/// # Safety
///
/// Sets a process-wide env var; only safe when all concurrent tests agree on
/// the same value.
fn fix_worker_pool_size() {
    // SAFETY: all tests in this module expect pool_size = 2.
    unsafe {
        std::env::set_var("MEDIAPM_CONDUCTOR_WORKER_POOL_SIZE", "2");
    }
}

/// Creates a `slow@v1` `ToolSpec` (test-only builtin: event log + hold).
fn slow_tool(name: &str) -> ToolSpec {
    ToolSpec {
        version: None,
        kind: ToolKindSpec::Builtin { builtin_id: "slow@v1".to_string() },
        name: name.into(),
        inputs: BTreeMap::from([
            ("log".into(), ToolInputSpec { kind: ToolInputKind::String, required: true }),
            ("label".into(), ToolInputSpec { kind: ToolInputKind::String, required: true }),
            ("hold_until".into(), ToolInputSpec { kind: ToolInputKind::String, required: false }),
            (
                "hold_timeout_ms".into(),
                ToolInputSpec { kind: ToolInputKind::String, required: false },
            ),
        ]),
        default_inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        runtime: ToolRuntime::default(),
    }
}

/// Creates a `slow@v1` `WorkflowStepSpec` writing to `log` with the given
/// label, optionally holding until the log contains `hold_until`.
fn slow_step(
    id: &str,
    log: &Path,
    label: &str,
    hold_until: Option<&str>,
    depends_on: &[&str],
) -> WorkflowStepSpec {
    let mut inputs = BTreeMap::from([
        ("log".into(), log.display().to_string()),
        ("label".into(), label.to_string()),
    ]);
    if let Some(line) = hold_until {
        inputs.insert("hold_until".into(), line.to_string());
    }
    WorkflowStepSpec {
        id: id.into(),
        tool: "slow@v1".into(),
        inputs,
        outputs: BTreeMap::new(),
        max_retries: 0,
        depends_on: depends_on.iter().map(ToString::to_string).collect(),
    }
}

/// Creates a `slow@v1` `ToolSpec` with a `max_concurrent_calls` cap (`0` =
/// unlimited), mirroring [`slow_tool`].
fn capped_slow_tool(name: &str, cap: usize) -> ToolSpec {
    ToolSpec {
        runtime: ToolRuntime { max_concurrent_calls: cap, ..ToolRuntime::default() },
        ..slow_tool(name)
    }
}

/// Creates a `slow@v1` step routed at a named tool variant: the cap tests
/// register several `slow@v1`-backed tools under distinct names, and
/// `step.tool` — not the tool map key — is what the scheduler resolves
/// and caps on.
fn slow_step_for_tool(
    id: &str,
    tool: &str,
    log: &Path,
    label: &str,
    hold_until: Option<&str>,
) -> WorkflowStepSpec {
    let mut step = slow_step(id, log, label, hold_until, &[]);
    step.tool = tool.into();
    step
}

/// Runs the named workflow with default options, asserting a clean run.
async fn run_clean(tc: &TestConductor, workflow_name: &str, expected_steps: usize) {
    let summary = tc
        .conductor()
        .run_workflow(workflow_name, RunWorkflowOptions::default())
        .await
        .expect("workflow");
    assert_eq!(summary.total_steps, expected_steps);
    assert_eq!(summary.executed_steps, expected_steps, "every step executes once");
    assert_eq!(summary.failed_steps, 0, "no step fails: {summary:?}");
}

/// Reads the per-test event log, panicking when it is missing.
fn read_event_log(log: &PathBuf) -> Vec<String> {
    match std::fs::read_to_string(log) {
        Ok(contents) => contents.lines().map(str::to_string).collect(),
        Err(e) => panic!("event log {} missing after the run: {e}", log.display()),
    }
}

/// Position of an exact event line in the log, panicking when absent.
fn pos(events: &[String], line: &str) -> usize {
    events
        .iter()
        .position(|event| event == line)
        .unwrap_or_else(|| panic!("missing event '{line}' in {events:?}"))
}

/// Creates a `flaky@v1` `WorkflowStepSpec`: fails its first `failures`
/// invocations (per-`key` attempt counter inside the builtin), then
/// succeeds. `max_retries` bounds how many times the coordinator may
/// re-dispatch the step after a failure. `key` must be distinct per step
/// in a workflow — the builtin's counter is keyed by it, so two steps
/// sharing a key would advance one counter and the second step's first
/// invocation would already be past its failure budget.
fn flaky_step(id: &str, failures: usize, max_retries: usize, key: &str) -> WorkflowStepSpec {
    WorkflowStepSpec {
        id: id.into(),
        tool: "flaky@v1".into(),
        inputs: BTreeMap::from([
            ("failures".into(), failures.to_string()),
            ("key".into(), key.into()),
        ]),
        outputs: BTreeMap::new(),
        max_retries,
        depends_on: Vec::new(),
    }
}

/// Creates an `echo@v1` producer step: the echo builtin's JSON stdout is
/// captured as the declared `result` output, so a downstream
/// `${step_output.<id>.result}` reference resolves to it whenever the
/// producer is inside the referencing step's transitive-dependency closure.
/// Distinct `text` values give distinct instance keys, so two identical
/// producers are told apart by their inputs.
fn echo_producer_step(id: &str, text: &str, depends_on: &[&str]) -> WorkflowStepSpec {
    WorkflowStepSpec {
        id: id.into(),
        tool: "echo@v1".into(),
        inputs: BTreeMap::from([("text".into(), text.into())]),
        outputs: BTreeMap::from([(
            "result".to_string(),
            OutputCaptureSpec {
                name: "result".to_string(),
                capture: "stdout".to_string(),
                ..OutputCaptureSpec::default()
            },
        )]),
        max_retries: 0,
        depends_on: depends_on.iter().map(ToString::to_string).collect(),
    }
}

/// Creates a `slow@v1` probe step: an ordinary slow step whose `probe`
/// input carries a `${step_output...}` template reference. Input resolution
/// runs before the builtin does, so a reference inside the step's dependency
/// closure resolves and the step logs its start line, while a reference
/// outside it fails the step during resolution — before the builtin ever
/// runs, so no start line appears in the event log. The slow builtin
/// ignores the extra `probe` argument.
fn probe_step(id: &str, log: &Path, depends_on: &[&str], probe_ref: &str) -> WorkflowStepSpec {
    let mut step = slow_step(id, log, id, None, depends_on);
    step.inputs.insert("probe".to_string(), probe_ref.to_string());
    step
}

/// Registers the `slow@v1` and `echo@v1` tools for scoping tests.
fn scoping_tools() -> BTreeMap<String, ToolSpec> {
    BTreeMap::from([
        ("slow@v1".to_string(), slow_tool("slow@v1")),
        ("echo@v1".to_string(), crate::echo_tool("echo@v1")),
    ])
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// A step whose own dependencies have finished starts while an unrelated
/// branch is still running: the level barrier is gone.
///
/// DAG: `a → (b, c)`, `b → d`. `c` holds until the log records `finish d`,
/// so the run can only complete when `c` observed `d` start — impossible
/// while dispatch waits on whole levels, because `d` sits one level below
/// `c`'s own level. The asserted orderings are schedule facts, not timing
/// guesses: a `start` line is appended by the step's builtin once the
/// coordinator dispatched it, and the coordinator dispatches a step only
/// after every dependency's `finish` line was appended and the step
/// settled.
#[tokio::test]
async fn ready_queue_dispatches_a_step_before_an_unrelated_branch_finishes() {
    fix_worker_pool_size();
    let tc = TestConductor::new();
    let log = tc.path().join("events.log");
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("slow@v1".into(), slow_tool("slow@v1"))]),
        vec![WorkflowSpec {
            name: "default".into(),
            display_name: None,
            description: None,
            impure: false,
            steps: vec![
                slow_step("a", &log, "a", None, &[]),
                slow_step("b", &log, "b", None, &["a"]),
                slow_step("c", &log, "c", Some("finish d"), &["a"]),
                slow_step("d", &log, "d", None, &["b"]),
            ],
        }],
    ));

    run_clean(&tc, "default", 4).await;

    let events = read_event_log(&log);
    // Happens-after for every dependency edge: a step never starts before
    // all of its own dependencies completed.
    assert!(
        pos(&events, "finish a") < pos(&events, "start b"),
        "b starts only after a completed: {events:?}"
    );
    assert!(
        pos(&events, "finish a") < pos(&events, "start c"),
        "c starts only after a completed: {events:?}"
    );
    assert!(
        pos(&events, "finish b") < pos(&events, "start d"),
        "d starts only after b completed: {events:?}"
    );
    // The barrier is gone: d starts while c — a step on an unrelated branch,
    // held open on purpose — is still running.
    assert!(
        pos(&events, "start d") < pos(&events, "finish c"),
        "d must start before the held, unrelated c finishes: {events:?}"
    );
    tc.shutdown().await;
}

/// A dependent step is dispatched onto a freed worker while another branch
/// still holds its own worker: the pool hands out workers, it does not pin
/// them per branch.
///
/// `s1` holds until `finish s3`; `s3` depends only on `s2`. The run proves
/// `s3` started while `s1` was still running: under a level barrier `s3`
/// could not dispatch before `s1` — a level-1 step — completed.
#[tokio::test]
async fn free_pool_dispatches_a_dependent_while_another_branch_holds_a_worker() {
    fix_worker_pool_size();
    let tc = TestConductor::new();
    let log = tc.path().join("events.log");
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("slow@v1".into(), slow_tool("slow@v1"))]),
        vec![WorkflowSpec {
            name: "default".into(),
            display_name: None,
            description: None,
            impure: false,
            steps: vec![
                slow_step("s1", &log, "s1", Some("finish s3"), &[]),
                slow_step("s2", &log, "s2", None, &[]),
                slow_step("s3", &log, "s3", None, &["s2"]),
            ],
        }],
    ));

    run_clean(&tc, "default", 3).await;

    let events = read_event_log(&log);
    assert!(
        pos(&events, "finish s2") < pos(&events, "start s3"),
        "s3 starts only after its dependency s2 completed: {events:?}"
    );
    // The pool property: s3 ran on the worker s2 freed while s1 still held
    // the other one, so s3's start precedes s1's finish.
    assert!(
        pos(&events, "start s3") < pos(&events, "finish s1"),
        "s3 must start while the held s1 still runs: {events:?}"
    );
    tc.shutdown().await;
}

/// A diamond join releases only when every branch feeding it has settled:
/// the per-step dependency counter reaches zero for ALL declared
/// dependencies, not for a subset and not via a level barrier.
///
/// DAG: `a → (b, c)`, `(b, c) → d`. `c` holds until the log records
/// `finish b`, which makes `c` deterministically the last of the two
/// branches to finish. `d` starting after `finish c` therefore proves it
/// waited for both branches: a join that fired on the first-completed
/// dependency would start `d` between `finish b` and `finish c` and fail
/// the last assertion. `a` precedes both branches, pinning the diamond's
/// root edges. All four orderings are schedule facts, not timing guesses.
#[tokio::test]
async fn ready_queue_releases_a_diamond_join_only_after_both_branches_finish() {
    fix_worker_pool_size();
    let tc = TestConductor::new();
    let log = tc.path().join("events.log");
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("slow@v1".into(), slow_tool("slow@v1"))]),
        vec![WorkflowSpec {
            name: "default".into(),
            display_name: None,
            description: None,
            impure: false,
            steps: vec![
                slow_step("a", &log, "a", None, &[]),
                slow_step("b", &log, "b", None, &["a"]),
                slow_step("c", &log, "c", Some("finish b"), &["a"]),
                slow_step("d", &log, "d", None, &["b", "c"]),
            ],
        }],
    ));

    run_clean(&tc, "default", 4).await;

    let events = read_event_log(&log);
    // The diamond's root edges: neither branch starts before a completed.
    assert!(
        pos(&events, "finish a") < pos(&events, "start b"),
        "b starts only after a completed: {events:?}"
    );
    assert!(
        pos(&events, "finish a") < pos(&events, "start c"),
        "c starts only after a completed: {events:?}"
    );
    // The join edges: d starts after BOTH branches finished. c held on
    // finish b, so c is the last branch to finish; d starting after finish c
    // proves the counter waits for every dependency, not just one.
    assert!(
        pos(&events, "finish b") < pos(&events, "start d"),
        "d starts only after b completed: {events:?}"
    );
    assert!(
        pos(&events, "finish c") < pos(&events, "start d"),
        "d starts only after the last branch c completed: {events:?}"
    );
    tc.shutdown().await;
}

/// An independent branch completes while an unrelated step still occupies a
/// worker: overlap is the steady state, and the fast branch's progress is
/// deterministic regardless of the slow branch's state.
///
/// DAG: `a → b`, plus an unrelated held root `c` (holds until
/// `finish b`). `a` and `c` dispatch together onto the 2-worker pool; `c`
/// stays in flight — started, not finished — across b's entire execution,
/// and b's completion is what releases it. The asserted interleaving is a
/// schedule fact: c's hold guarantees `finish b` strictly precedes
/// `finish c`, so "b completed while c was in flight" holds on any machine
/// speed.
#[tokio::test]
async fn fast_branch_completes_while_an_unrelated_held_step_is_in_flight() {
    fix_worker_pool_size();
    let tc = TestConductor::new();
    let log = tc.path().join("events.log");
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("slow@v1".into(), slow_tool("slow@v1"))]),
        vec![WorkflowSpec {
            name: "default".into(),
            display_name: None,
            description: None,
            impure: false,
            steps: vec![
                slow_step("a", &log, "a", None, &[]),
                slow_step("b", &log, "b", None, &["a"]),
                slow_step("c", &log, "c", Some("finish b"), &[]),
            ],
        }],
    ));

    run_clean(&tc, "default", 3).await;

    let events = read_event_log(&log);
    assert!(
        pos(&events, "finish a") < pos(&events, "start b"),
        "b starts only after a completed: {events:?}"
    );
    // c is in flight across b's whole execution: it started before b
    // completed and — held on finish b — can only finish after it.
    assert!(
        pos(&events, "start c") < pos(&events, "finish b"),
        "c must have started before b completed: {events:?}"
    );
    assert!(
        pos(&events, "finish b") < pos(&events, "finish c"),
        "b completed while the held, unrelated c was still in flight: {events:?}"
    );
    tc.shutdown().await;
}

/// A retry does not hold its worker while pending: the attempt's
/// settlement returns the worker to the free pool, so the re-dispatched
/// attempt can take a worker and the run completes.
///
/// The release is pinned structurally rather than by observing which
/// worker ran what: two independent fail-once steps at pool size 2 make
/// both initial attempts occupy both workers, so if a retry arm failed to
/// release its worker, the first retry would park in the ready queue with
/// nothing in flight and the scheduler would abort with `scheduler
/// stalled` instead of completing. Each step uses its own flaky key so
/// both counters start at one and both steps actually fail once and
/// retry — a shared key would let the second step's first invocation
/// succeed without ever exercising the retry arm.
#[tokio::test]
async fn retry_releases_the_worker_for_the_re_dispatched_attempt() {
    fix_worker_pool_size();
    let tc = TestConductor::new();
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("flaky@v1".into(), crate::flaky_tool("flaky@v1"))]),
        vec![WorkflowSpec {
            name: "default".into(),
            display_name: None,
            description: None,
            impure: false,
            steps: vec![
                flaky_step("s1", 1, 1, "scheduler_retry_releases_s1"),
                flaky_step("s2", 1, 1, "scheduler_retry_releases_s2"),
            ],
        }],
    ));

    let summary = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("workflow");
    assert_eq!(summary.total_steps, 2);
    assert_eq!(
        summary.executed_steps, 2,
        "both steps execute once more on their retry: {summary:?}"
    );
    assert_eq!(summary.failed_steps, 0, "no step fails: {summary:?}");
    assert_eq!(
        summary.retried_steps, 2,
        "both steps succeed only after one retry, which requires the worker released: {summary:?}"
    );
    tc.shutdown().await;
}

/// A step's request carries exactly its own branch's outputs: a sibling
/// branch is invisible even though it is fast, settles early, and shares
/// the pool.
///
/// DAG: `a → b`, `c → d`. The four probe leaves split each branch's
/// visibility in two: `p_b_a` / `p_d_c` probe their own branch's producer
/// and must succeed (they log a start line), while `p_b_c` / `p_d_a` probe
/// the sibling branch and must fail during input resolution (no start
/// line). A cumulative-outputs implementation lets `p_b_c` pass — both
/// producers are fast roots that settled long before the probes dispatched
/// — so the failed-steps count discriminates this test from a regression.
#[tokio::test]
async fn dependency_scoped_outputs_isolate_sibling_branches() {
    fix_worker_pool_size();
    let tc = TestConductor::new();
    let log = tc.path().join("events.log");
    tc.write_config(doc_with_workflows(
        scoping_tools(),
        vec![WorkflowSpec {
            name: "default".into(),
            display_name: None,
            description: None,
            impure: false,
            steps: vec![
                echo_producer_step("a", "from-a", &[]),
                echo_producer_step("c", "from-c", &[]),
                slow_step("b", &log, "b", None, &["a"]),
                slow_step("d", &log, "d", None, &["c"]),
                probe_step("p_b_a", &log, &["b"], "${step_output.a.result}"),
                probe_step("p_b_c", &log, &["b"], "${step_output.c.result}"),
                probe_step("p_d_c", &log, &["d"], "${step_output.c.result}"),
                probe_step("p_d_a", &log, &["d"], "${step_output.a.result}"),
            ],
        }],
    ));

    let summary = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("workflow");
    assert_eq!(summary.total_steps, 8);
    assert_eq!(
        summary.executed_steps, 6,
        "both producers and all four-branch chain steps run, plus the two own-branch probes: {summary:?}"
    );
    assert_eq!(
        summary.failed_steps, 2,
        "exactly the two cross-branch probes fail on the missing output: {summary:?}"
    );

    let events = read_event_log(&log);
    assert!(
        events.iter().any(|event| event == "start p_b_a"),
        "b's branch must see a's outputs: {events:?}"
    );
    assert!(
        events.iter().any(|event| event == "start p_d_c"),
        "d's branch must see c's outputs: {events:?}"
    );
    assert!(
        !events.iter().any(|event| event == "start p_b_c"),
        "b must not see the sibling c's outputs: {events:?}"
    );
    assert!(
        !events.iter().any(|event| event == "start p_d_a"),
        "d must not see the sibling a's outputs: {events:?}"
    );
    tc.shutdown().await;
}

/// A diamond join sees its whole closure — root and both branches — and
/// nothing outside it.
///
/// DAG: `a → (b, c)`, `(b, c) → d`, plus an unrelated echo root `x`. The
/// join leaf `p_join` probes the root's `result` output and both branches'
/// `process_code` outputs in one step; all are ancestors, so it must start.
/// The leaf `p_x` probes the unrelated root and must fail during resolution:
/// `x` settled at the very start of the run, so a cumulative-outputs
/// implementation would resolve it and report a clean run.
#[tokio::test]
async fn dependency_scoped_outputs_cover_the_whole_diamond_closure() {
    fix_worker_pool_size();
    let tc = TestConductor::new();
    let log = tc.path().join("events.log");
    tc.write_config(doc_with_workflows(
        scoping_tools(),
        vec![WorkflowSpec {
            name: "default".into(),
            display_name: None,
            description: None,
            impure: false,
            steps: vec![
                echo_producer_step("a", "from-a", &[]),
                echo_producer_step("x", "from-x", &[]),
                slow_step("b", &log, "b", None, &["a"]),
                slow_step("c", &log, "c", None, &["a"]),
                slow_step("d", &log, "d", None, &["b", "c"]),
                probe_step(
                    "p_join",
                    &log,
                    &["d"],
                    "${step_output.a.result} ${step_output.b.process_code} \
                     ${step_output.c.process_code}",
                ),
                probe_step("p_x", &log, &["d"], "${step_output.x.process_code}"),
            ],
        }],
    ));

    let summary = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("workflow");
    assert_eq!(summary.total_steps, 7);
    assert_eq!(
        summary.executed_steps, 6,
        "the whole diamond plus the join probe run; only the unrelated probe fails: {summary:?}"
    );
    assert_eq!(
        summary.failed_steps, 1,
        "exactly the probe of the unrelated root fails: {summary:?}"
    );

    let events = read_event_log(&log);
    assert!(
        events.iter().any(|event| event == "start p_join"),
        "the join must see all three of its ancestors' outputs: {events:?}"
    );
    assert!(
        !events.iter().any(|event| event == "start p_x"),
        "the join must not see the unrelated root's outputs: {events:?}"
    );
    tc.shutdown().await;
}

/// An in-flight step's outputs never leak into a later step's view: the
/// closure excludes a step that has not settled yet, even though its
/// output shape is exactly what a settled step provides.
///
/// DAG: `b → d → (p_b, p_c)`; an independent chain `h → c` where `h` holds
/// until `finish p_b`, so `c` — the only step that could contribute the
/// probed `process_code` output — cannot possibly have settled when the
/// probes dispatch (both probes leave the ready queue in the same batch as
/// `d`'s settlement, and `c` cannot even start before `h` finishes after
/// `finish p_b`). `p_b` succeeding against the same implicit
/// `process_code` output of its own ancestor `b` proves the probe's
/// failure in `p_c` is the closure exclusion, not a missing output. The
/// asserted exclusion itself holds under every interleaving; the hold only
/// makes "c unsettled at dispatch" the schedule's intent rather than a
/// wall-clock hope.
#[tokio::test]
async fn dependency_scoped_outputs_exclude_an_unsettled_step() {
    fix_worker_pool_size();
    let tc = TestConductor::new();
    let log = tc.path().join("events.log");
    tc.write_config(doc_with_workflows(
        scoping_tools(),
        vec![WorkflowSpec {
            name: "default".into(),
            display_name: None,
            description: None,
            impure: false,
            steps: vec![
                echo_producer_step("b", "from-b", &[]),
                slow_step("d", &log, "d", None, &["b"]),
                slow_step("h", &log, "h", Some("finish p_b"), &[]),
                slow_step("c", &log, "c", None, &["h"]),
                probe_step("p_b", &log, &["d"], "${step_output.b.process_code}"),
                probe_step("p_c", &log, &["d"], "${step_output.c.process_code}"),
            ],
        }],
    ));

    let summary = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("workflow");
    assert_eq!(summary.total_steps, 6);
    assert_eq!(
        summary.executed_steps, 5,
        "everything runs except the probe of the unsettled step: {summary:?}"
    );
    assert_eq!(
        summary.failed_steps, 1,
        "exactly the probe of the unsettled step fails: {summary:?}"
    );

    let events = read_event_log(&log);
    assert!(
        events.iter().any(|event| event == "start p_b"),
        "the in-closure probe must succeed: {events:?}"
    );
    assert!(
        !events.iter().any(|event| event == "start p_c"),
        "the unsettled step's outputs must not be visible: {events:?}"
    );
    // The hold is what keeps c unsettled across the probes' dispatch: h can
    // only finish after p_b finished, and c starts only after h finished.
    assert!(
        pos(&events, "finish p_b") < pos(&events, "start c"),
        "c must still have been in flight when the probes dispatched: {events:?}"
    );
    tc.shutdown().await;
}

/// A step that already settled does not leak into a later unrelated step's
/// view either: exclusion is the closure rule, not a function of timing.
///
/// DAG: `b` and `c` are independent roots; `b2` chains onto `b` and holds
/// until `finish c`, so nothing downstream of `b2` can dispatch before
/// `c` finished — and under the current-thread test harness, before `c`'s
/// completion was processed into the run state (c's response enters the
/// completion stream when it writes the line `b2` observes, strictly
/// before `b2` can return). `p_c` probes
/// `${step_output.c.process_code}` and must STILL fail: `c` is unrelated
/// to the `b → b2` chain, so under a cumulative-outputs implementation
/// `p_c` resolves here (c settled long before), making this polarity the
/// regression discriminator a settled-but-unrelated step stays invisible.
/// `p_b` succeeding against the same implicit `process_code` output pins
/// the positive direction and proves the probe shape is sound.
#[tokio::test]
async fn dependency_scoped_outputs_exclude_a_step_that_already_settled() {
    fix_worker_pool_size();
    let tc = TestConductor::new();
    let log = tc.path().join("events.log");
    tc.write_config(doc_with_workflows(
        scoping_tools(),
        vec![WorkflowSpec {
            name: "default".into(),
            display_name: None,
            description: None,
            impure: false,
            steps: vec![
                slow_step("c", &log, "c", None, &[]),
                slow_step("b", &log, "b", None, &[]),
                slow_step("b2", &log, "b2", Some("finish c"), &["b"]),
                probe_step("p_b", &log, &["b2"], "${step_output.b.process_code}"),
                probe_step("p_c", &log, &["b2"], "${step_output.c.process_code}"),
            ],
        }],
    ));

    let summary = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("workflow");
    assert_eq!(summary.total_steps, 5);
    assert_eq!(
        summary.executed_steps, 4,
        "everything runs except the probe of the settled unrelated step: {summary:?}"
    );
    assert_eq!(
        summary.failed_steps, 1,
        "exactly the probe of the settled unrelated step fails: {summary:?}"
    );

    let events = read_event_log(&log);
    // The hold is what pins the ordering: b2 can only finish after the log
    // recorded c's finish, so every probe below dispatched strictly after
    // c completed.
    assert!(
        pos(&events, "finish c") < pos(&events, "finish b2"),
        "b2 must have held until c finished: {events:?}"
    );
    assert!(
        events.iter().any(|event| event == "start p_b"),
        "the in-closure probe must succeed: {events:?}"
    );
    assert!(
        !events.iter().any(|event| event == "start p_c"),
        "the settled unrelated step's outputs must not be visible: {events:?}"
    );
    tc.shutdown().await;
}

/// A step's state snapshot carries only its ancestors' tool-call
/// instances: an identical call made by an ancestor is a cache hit, while
/// an identical call made by an unrelated branch is not.
///
/// `a` and `x` are independent echo roots; `b` is byte-identical to `a`
/// (same tool, same inputs → same instance key) and depends on it, so the
/// ancestor-scoped state lets `b` hit `a`'s fresh instance instead of
/// re-executing. `y` is also byte-identical to `a` but depends only on
/// `x`, so `a`'s instance is outside `y`'s closure and `y` must execute
/// fresh. An implementation that hands out the run-entry state only never
/// produces the cache hit; one that merges the live state leaks `a`'s
/// instance to `y`.
#[tokio::test]
async fn dependency_scoped_state_exposes_only_ancestor_instances() {
    fix_worker_pool_size();
    let tc = TestConductor::new();
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("echo@v1".to_string(), crate::echo_tool("echo@v1"))]),
        vec![WorkflowSpec {
            name: "default".into(),
            display_name: None,
            description: None,
            impure: false,
            steps: vec![
                echo_producer_step("a", "shared", &[]),
                echo_producer_step("x", "other", &[]),
                echo_producer_step("b", "shared", &["a"]),
                echo_producer_step("y", "shared", &["x"]),
            ],
        }],
    ));

    let summary = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("workflow");
    assert_eq!(summary.total_steps, 4);
    assert_eq!(
        summary.cached_steps, 1,
        "b must hit a's fresh instance through the ancestor-scoped state: {summary:?}"
    );
    assert_eq!(
        summary.executed_steps, 3,
        "a, x, and y execute — y must not see a's instance: {summary:?}"
    );
    assert_eq!(summary.failed_steps, 0, "no step fails: {summary:?}");
    tc.shutdown().await;
}

// ---------------------------------------------------------------------------
// Tool concurrency cap (`max_concurrent_calls`)
// ---------------------------------------------------------------------------

/// A tool's `max_concurrent_calls` cap serializes that tool's steps: with
/// cap=1 and two independent steps of one tool on a 2-worker pool, the
/// second step can only start after the first finished — even though the
/// pool holds a free worker throughout.
///
/// This is the yt-dlp regression test: every step of a capped tool used to
/// dispatch concurrently (the scheduler never read the cap), so two
/// yt-dlp steps overlapped and saturated the tool.
#[tokio::test]
async fn tool_cap_serializes_a_single_tool() {
    fix_worker_pool_size();
    let tc = TestConductor::new();
    let log = tc.path().join("events.log");
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("slow-cap@v1".into(), capped_slow_tool("slow-cap@v1", 1))]),
        vec![WorkflowSpec {
            name: "default".into(),
            display_name: None,
            description: None,
            impure: false,
            steps: vec![
                slow_step_for_tool("a", "slow-cap@v1", &log, "a", None),
                slow_step_for_tool("b", "slow-cap@v1", &log, "b", None),
            ],
        }],
    ));

    run_clean(&tc, "default", 2).await;

    let events = read_event_log(&log);
    // Serialization under cap=1: b starts strictly after a finished, so the
    // two calls never overlap on the tool.
    assert!(
        pos(&events, "finish a") < pos(&events, "start b"),
        "b must not start before a finished under cap=1: {events:?}"
    );
    tc.shutdown().await;
}

/// Parking behind a cap is per-tool: tool X (cap=1) serializes its two
/// steps while tool Y (uncapped) starts and finishes in between — X's
/// saturation never blocks Y's dispatch.
///
/// `x1` holds until the log records `finish y`, so y provably started and
/// finished while x1 was still in flight and x2 still parked. Under the
/// old uncapped dispatch x1 and x2 would occupy both workers, and y could
/// only start after one of them finished — the last assertion is the
/// discriminator.
#[tokio::test]
async fn tool_cap_does_not_block_other_tools() {
    fix_worker_pool_size();
    let tc = TestConductor::new();
    let log = tc.path().join("events.log");
    tc.write_config(doc_with_workflows(
        BTreeMap::from([
            ("slow-x@v1".into(), capped_slow_tool("slow-x@v1", 1)),
            ("slow-y@v1".into(), capped_slow_tool("slow-y@v1", 0)),
        ]),
        vec![WorkflowSpec {
            name: "default".into(),
            display_name: None,
            description: None,
            impure: false,
            steps: vec![
                slow_step_for_tool("x1", "slow-x@v1", &log, "x1", Some("finish y")),
                slow_step_for_tool("x2", "slow-x@v1", &log, "x2", None),
                slow_step_for_tool("y", "slow-y@v1", &log, "y", None),
            ],
        }],
    ));

    run_clean(&tc, "default", 3).await;

    let events = read_event_log(&log);
    // y started while the capped tool's x1 was still in flight (held) …
    assert!(
        pos(&events, "start y") < pos(&events, "finish x1"),
        "y must start while the capped tool's x1 is still in flight: {events:?}"
    );
    // … and finished before x1, i.e. X's saturation never blocked it.
    assert!(
        pos(&events, "finish y") < pos(&events, "finish x1"),
        "y must finish while capped-tool x1 is still running: {events:?}"
    );
    // x2 was parked behind the cap and dispatches only after x1's slot frees.
    assert!(
        pos(&events, "finish x1") < pos(&events, "start x2"),
        "x2 must stay parked until x1 releases the tool slot: {events:?}"
    );
    tc.shutdown().await;
}

/// `max_concurrent_calls = 0` means unlimited: three independent steps of
/// one uncapped tool dispatch together on the 2-worker pool instead of
/// serializing.
///
/// `a` holds until the log records `start b`: any implicit serialization
/// (e.g. treating 0 as a cap of 1) would make a's hold time out and the
/// step fail, while a "never run" reading of 0 parks everything and trips
/// the scheduler's stall guard — either regression fails loudly.
#[tokio::test]
async fn tool_cap_zero_runs_all_steps() {
    fix_worker_pool_size();
    let tc = TestConductor::new();
    let log = tc.path().join("events.log");
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("slow-cap0@v1".into(), capped_slow_tool("slow-cap0@v1", 0))]),
        vec![WorkflowSpec {
            name: "default".into(),
            display_name: None,
            description: None,
            impure: false,
            steps: vec![
                slow_step_for_tool("a", "slow-cap0@v1", &log, "a", Some("start b")),
                slow_step_for_tool("b", "slow-cap0@v1", &log, "b", None),
                slow_step_for_tool("c", "slow-cap0@v1", &log, "c", None),
            ],
        }],
    ));

    run_clean(&tc, "default", 3).await;

    let events = read_event_log(&log);
    // a and b overlapped: a's hold on `start b` resolved while a was still
    // running, which is only possible if both dispatched in the same pass.
    assert!(pos(&events, "start a") < pos(&events, "finish a"), "a must run: {events:?}");
    assert!(
        pos(&events, "start b") < pos(&events, "finish a"),
        "b must start while the uncapped a still runs: {events:?}"
    );
    tc.shutdown().await;
}

/// A settle releases the capped tool's slot to a parked step: with cap=1
/// and two steps, the parked step dispatches as soon as the running one
/// finishes, and the run completes without stalling.
#[tokio::test]
async fn tool_cap_settle_releases_a_parked_step() {
    fix_worker_pool_size();
    let tc = TestConductor::new();
    let log = tc.path().join("events.log");
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("slow-cap@v1".into(), capped_slow_tool("slow-cap@v1", 1))]),
        vec![WorkflowSpec {
            name: "default".into(),
            display_name: None,
            description: None,
            impure: false,
            steps: vec![
                slow_step_for_tool("a", "slow-cap@v1", &log, "a", None),
                slow_step_for_tool("b", "slow-cap@v1", &log, "b", None),
            ],
        }],
    ));

    // No stall: the parked step re-admits on a's settlement instead of
    // wedging the scheduler with everything parked and nothing in flight.
    run_clean(&tc, "default", 2).await;

    let events = read_event_log(&log);
    assert!(
        pos(&events, "finish a") < pos(&events, "start b"),
        "the parked step must dispatch once a releases the tool slot: {events:?}"
    );
    tc.shutdown().await;
}

/// Parking never livelocks: with cap=1 and three independent steps, each
/// parked step re-admits as the slot frees and the run completes. If the
/// cap accounting ever left every remaining step parked with nothing in
/// flight, the scheduler's stall guard fails loudly instead of hanging.
#[tokio::test]
async fn tool_cap_park_does_not_livelock() {
    fix_worker_pool_size();
    let tc = TestConductor::new();
    let log = tc.path().join("events.log");
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("slow-cap@v1".into(), capped_slow_tool("slow-cap@v1", 1))]),
        vec![WorkflowSpec {
            name: "default".into(),
            display_name: None,
            description: None,
            impure: false,
            steps: vec![
                slow_step_for_tool("a", "slow-cap@v1", &log, "a", None),
                slow_step_for_tool("b", "slow-cap@v1", &log, "b", None),
                slow_step_for_tool("c", "slow-cap@v1", &log, "c", None),
            ],
        }],
    ));

    run_clean(&tc, "default", 3).await;

    let events = read_event_log(&log);
    // The three calls serialize in queue order: each parked step follows
    // the previous one's finish, so no call ever overlaps another.
    assert!(
        pos(&events, "finish a") < pos(&events, "start b"),
        "b must follow a under cap=1: {events:?}"
    );
    assert!(
        pos(&events, "finish b") < pos(&events, "start c"),
        "c must follow b under cap=1: {events:?}"
    );
    tc.shutdown().await;
}
