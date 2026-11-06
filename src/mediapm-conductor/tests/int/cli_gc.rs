//! The spawned conductor binary's `gc` subcommand runs the cycle and prints
//! what it decided.
//!
//! [`crate::gc`] and [`crate::gc_automatic`] pin the pass in process; these
//! tests pin the path an operator takes: `conductor gc` opens the workspace's
//! state file and store itself, runs the cycle, and reports through the one
//! line `gc_summary_line` renders. The binary is spawned because that path
//! starts in `run_from_env`, which an in-process test cannot set up.
//!
//! Both scenarios build their inputs directly - the config document, the
//! staged blobs, and the state file through the same `encode_state_json` the
//! conductor saves with - so no in-process conductor exists and no background
//! pass can race the fixture. The state file is the contract between the two
//! processes, which is exactly what it is for an operator too.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use bytes::Bytes;
use mediapm_cas::{CasApi, FileSystemCas, Hash};
use mediapm_conductor::{
    CapturedOutput, ConductorState, RuntimeStoragePaths, decode_state_json, encode_state_json,
};

use super::gc::sample_instance;
use super::gc_automatic::BACKDATE_SECONDS;
use crate::{
    doc_with_workflows, echo_step, echo_tool, run_binary, workflow_with_steps,
    write_conductor_config,
};

/// Writes the state file a previous run would have left: one unreachable
/// record aged past the default grace period, plus - when `live_workflow` is
/// given - a seeded live instance whose staged output the seed's
/// reachability roots.
///
/// The GC clock stays at its zero default on purpose: a fresh state's clock
/// has not run, so the first judged pass starts it and the next one judges,
/// which is the two-pass shape an operator's first `conductor gc` pair
/// takes. Returns the live record's `(key, blob)` when one was staged, then
/// the aged key and the aged blob.
async fn write_starting_state(
    path: &Path,
    live_workflow: Option<&str>,
    store: &FileSystemCas,
) -> (Option<(Hash, Hash)>, Hash, Hash) {
    let mut state = ConductorState::new_empty();

    let live = match live_workflow {
        Some(workflow) => {
            let key = Hash::from_content(workflow.as_bytes());
            let blob = store
                .put(Bytes::from_static(b"cli gc live record output"))
                .await
                .expect("stage the live record's output");
            let mut instance = sample_instance(key);
            instance.outputs = BTreeMap::from([(
                "result".to_string(),
                CapturedOutput { hash: blob, capture: None },
            )]);
            state.tool_call_instances.insert(key, instance);
            state.aux.live_instances.insert(workflow.to_string(), BTreeSet::from([key]));
            Some((key, blob))
        }
        None => None,
    };

    let aged = Hash::from_content(b"cli gc aged record");
    let aged_blob = store
        .put(Bytes::from_static(b"cli gc aged record output"))
        .await
        .expect("stage the aged record's output");
    let mut instance = sample_instance(aged);
    instance.outputs =
        BTreeMap::from([("result".to_string(), CapturedOutput { hash: aged_blob, capture: None })]);
    state.tool_call_instances.insert(aged, instance);
    super::gc_automatic::backdate(&mut state, &aged.to_hex(), BACKDATE_SECONDS);

    store.flush().await.expect("stage the blobs");
    std::fs::write(path, encode_state_json(&state).expect("encode the state"))
        .expect("write the state file");
    (live, aged, aged_blob)
}

/// Reads the state file back through the decoder the conductor loads with.
fn read_state(path: &Path) -> ConductorState {
    let bytes = std::fs::read(path).expect("read the state file");
    decode_state_json(&bytes).expect("decode the state file")
}

/// The spawned `gc` subcommand starts the clock on one pass and reclaims an
/// aged record on the next, printing the completed line for both.
///
/// The user-level guarantee: the first `conductor gc` on a state whose clock
/// never ran judges and persists the running clock, and the next invocation
/// evicts the unreachable record past its grace period along with its blob,
/// while the record a seed reaches and its blob survive both. Exit status,
/// the printed decision, the state file and the store are all asserted,
/// because the line alone would not say the library changed.
#[tokio::test]
async fn the_spawned_gc_pass_starts_the_clock_then_reclaims_the_aged_record() {
    let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    write_conductor_config(
        dir.path(),
        doc_with_workflows(
            BTreeMap::from([("echo@v1".to_string(), echo_tool("echo@v1"))]),
            vec![workflow_with_steps("w", vec![echo_step("echo@v1", "seeded work")])],
        ),
    );

    let paths = RuntimeStoragePaths::new(dir.path());
    let store_path = paths.cas_store_dir.clone();
    let state_file = paths.state_file_path.clone();

    let store = FileSystemCas::open(&store_path).await.expect("open the store");
    let (live, aged, aged_blob) = write_starting_state(&state_file, Some("w"), &store).await;
    let (live_key, live_blob) = live.expect("test 1 stages a live record");
    drop(store); // the spawned process opens this store; its lock must be free

    let dir_arg = dir.path().to_str().expect("tempdir path is utf-8");

    // Pass one starts the clock. Whether it also reclaims is not asserted
    // here: one invocation runs the awaited pass and the background loop's
    // own first pass, and whichever judges second sees the clock running, so
    // the record may go in this process or the next. The clock contract
    // itself is pinned in process by `gc.rs`; what this path must show is
    // that the invocation judges and persists the clock.
    let first = run_binary(&["--conductor-dir", dir_arg, "gc"]);
    assert!(
        first.success,
        "the first `conductor gc` must exit successfully; stderr: {}",
        first.stderr
    );
    assert!(
        first.stdout.contains("Garbage collection completed"),
        "every declared workflow has a seed, so the pass judges; stdout: {}",
        first.stdout
    );
    let after_first = read_state(&state_file);
    assert!(
        after_first.aux.conductor_gc_epoch.as_unix_nanos() > 0,
        "the first judged pass must leave the clock running"
    );

    // Pass two: the clock is running by now, so the record's age counts and
    // it goes with its blob, while the seeded record stays.
    let second = run_binary(&["--conductor-dir", dir_arg, "gc"]);
    assert!(
        second.success,
        "the second `conductor gc` must exit successfully; stderr: {}",
        second.stderr
    );
    assert!(
        second.stdout.contains("Garbage collection completed"),
        "the second pass judges too; stdout: {}",
        second.stdout
    );
    let after_second = read_state(&state_file);
    assert!(
        !after_second.tool_call_instances.contains_key(&aged),
        "the record past its TTL must be gone from the state file"
    );
    assert!(
        after_second.tool_call_instances.contains_key(&live_key),
        "the seeded record must survive the same pass"
    );

    let store = FileSystemCas::open(&store_path).await.expect("reopen the store");
    assert!(
        store.stat(aged_blob).await.is_err(),
        "the aged record's blob must be reclaimed with it"
    );
    assert!(store.stat(live_blob).await.is_ok(), "the live record's blob must stay readable");
}

/// The spawned `gc` subcommand abstains on a declared workflow that never
/// ran, and the freeze it reports is the freeze the library gets.
///
/// The user-level guarantee: an operator who declared a workflow and has not
/// run it yet gets an abstention - a successful exit, the reason naming that
/// workflow, no eviction however old the recorded orphan is, no clock
/// started (nothing was judged), and no blob touched.
#[tokio::test]
async fn the_spawned_gc_pass_abstains_on_a_workflow_that_never_ran() {
    let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    write_conductor_config(
        dir.path(),
        doc_with_workflows(
            BTreeMap::from([("echo@v1".to_string(), echo_tool("echo@v1"))]),
            vec![workflow_with_steps(
                "never",
                vec![echo_step("echo@v1", "declared but never run")],
            )],
        ),
    );

    let paths = RuntimeStoragePaths::new(dir.path());
    let store_path = paths.cas_store_dir.clone();
    let state_file = paths.state_file_path.clone();

    let store = FileSystemCas::open(&store_path).await.expect("open the store");
    let (no_live, aged, aged_blob) = write_starting_state(&state_file, None, &store).await;
    assert!(no_live.is_none(), "test 2 stages no live record");
    drop(store);

    let dir_arg = dir.path().to_str().expect("tempdir path is utf-8");
    let run = run_binary(&["--conductor-dir", dir_arg, "gc"]);
    assert!(
        run.success,
        "an abstention is a legitimate outcome, so the command exits successfully; stderr: {}",
        run.stderr
    );
    assert!(
        run.stdout.contains("Garbage collection abstained"),
        "a declared workflow with no seed must abstain; stdout: {}",
        run.stdout
    );
    assert!(
        run.stdout.contains("seed: never"),
        "the line must name the workflow that never ran; stdout: {}",
        run.stdout
    );

    let after = read_state(&state_file);
    assert!(
        after.tool_call_instances.contains_key(&aged),
        "an abstaining pass must not evict, however old the record is"
    );
    assert_eq!(
        after.aux.conductor_gc_epoch.as_unix_nanos(),
        0,
        "an abstaining pass records no clock: nothing was judged"
    );
    let store = FileSystemCas::open(&store_path).await.expect("reopen the store");
    assert!(
        store.stat(aged_blob).await.is_ok(),
        "an abstaining pass leaves the record's blob alone"
    );
}
