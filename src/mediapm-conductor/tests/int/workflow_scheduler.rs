//! Ready-queue scheduler tests: dependency-counter dispatch over a free
//! worker pool, with no level barrier.
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
//! Tests are gated on `#[cfg(feature = "progress")]` for parity with the
//! `slow@v1` builtin, which (like `flaky@v1`) is compiled in under
//! `any(test, feature = "progress")`.
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
    ToolInputKind, ToolInputSpec, ToolKindSpec, ToolRuntime, ToolSpec, WorkflowSpec,
    WorkflowStepSpec,
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
}
