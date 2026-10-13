//! Dependency-aware job pool for parallel tool provisioning.
//!
//! Tool sync provisions managed tools concurrently, and some tools declare
//! **same-step dependencies** on others: a tool's canonical version folds in
//! its `SameStep` deps' resolved versions, and its content map inlines the deps'
//! payload maps. Those two facts must cross from a dep's job to the requirer's
//! job *while both are still running* — waiting for the dep job to finish first
//! would serialize the pool.
//!
//! The pool models those DAG edges with one [`tokio::sync::watch`] channel pair
//! per job index:
//!
//! - **DAG edges** — [`PoolJob::same_step_deps`] lists, per job, the indices of
//!   the jobs whose published data it consumes. [`run_pool`] builds the
//!   channels up front; each job receives a [`DepGates`] with cloned receivers
//!   for its declared deps and a [`DepPublisher`] carrying its own senders. A
//!   job with no declared edges never waits: `dep_version`/`dep_own_map` only
//!   exist for declared indices, and no other await point gates the job.
//! - **Publication points** — the caller publishes `publish_version` after the
//!   dep's *resolve* phase produces its canonical version (so dependents can
//!   fold it into their own composite version) and `publish_own_map` after the
//!   dep's *process* phase produces its pre-inline payload map (so dependents
//!   can inline it). A dependent awaiting a channel whose dep completes without
//!   publishing observes a closed channel and proceeds with `None`.
//! - **Fail-fast abort** — the first job to return `Err` ends the pool with
//!   [`PoolFailure`]; the remaining jobs are not waited on.
//! - **Cancellation on drop** — jobs run inline on the driving future (via
//!   `buffer_unordered`), never on spawned tasks, so returning on abort drops
//!   the stream and with it every in-flight job future, cancelling each at its
//!   next await point (drop guards inside a future run at that moment).
//! - **Index-sorted apply order** — on success [`run_pool`] returns outcomes
//!   sorted by [`PoolJob::index`], so the caller applies results in a
//!   deterministic order regardless of completion order.

// The pool is additive in this change: the tool-sync provisioning loop becomes
// its first production consumer in the follow-up wiring, so nothing in the
// non-test build calls into this module yet. The expectation is removed by
// that wiring.
#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the pool is additive in this change; the tool-sync provisioning loop becomes its first production consumer in the follow-up wiring"
    )
)]

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::future::Future;

use futures_util::stream;
use futures_util::stream::StreamExt;
use tokio::sync::watch;

/// One unit of pool work: a job index, the indices of its same-step
/// dependencies (the DAG edges), and the caller's input handed to the run
/// closure.
pub(crate) struct PoolJob<I> {
    /// Position of this job in the caller's job list; the pool keys every
    /// publication channel by it and returns outcomes sorted by it. Indices
    /// must be unique within one [`run_pool`] call.
    pub index: usize,
    /// Indices of the jobs this job depends on (same-step dependency edges).
    /// Only these deps gate this job; an unknown index is treated as an
    /// undeclared edge and never blocks.
    pub same_step_deps: Vec<usize>,
    /// Opaque per-job input passed through to the run closure.
    pub input: I,
}

/// The read side of one job's DAG edges: cloned watch receivers for every
/// dependency the job declared, keyed by dependency job index.
///
/// The receivers are cloned from the dep jobs' channels before anything runs,
/// so a dependent starts waiting the moment it awaits and observes the dep's
/// publication as soon as the dep publishes it.
pub(crate) struct DepGates {
    /// Version channels of the declared deps, keyed by dep job index.
    version_rx: HashMap<usize, watch::Receiver<Option<String>>>,
    /// Payload-map channels of the declared deps, keyed by dep job index.
    own_map_rx: HashMap<usize, watch::Receiver<Option<BTreeMap<String, String>>>>,
}

impl DepGates {
    /// Waits until the dependency job at `index` publishes its resolved
    /// canonical version and returns it.
    ///
    /// Returns `None` when this job declared no edge to `index`, or when the
    /// dep's channels closed without a publication (the dep's future was
    /// dropped before publishing); either way the caller proceeds ungated.
    pub(crate) async fn dep_version(&self, index: usize) -> Option<String> {
        let receiver = self.version_rx.get(&index)?;
        let mut receiver = receiver.clone();
        match receiver.wait_for(Option::is_some).await {
            Ok(value) => (*value).clone(),
            Err(_) => None,
        }
    }

    /// Waits until the dependency job at `index` publishes its processed
    /// payload map (the pre-inline own map) and returns it.
    ///
    /// Returns `None` under the same conditions as [`Self::dep_version`].
    pub(crate) async fn dep_own_map(&self, index: usize) -> Option<BTreeMap<String, String>> {
        let receiver = self.own_map_rx.get(&index)?;
        let mut receiver = receiver.clone();
        match receiver.wait_for(Option::is_some).await {
            Ok(value) => (*value).clone(),
            Err(_) => None,
        }
    }
}

/// The write side of one job's own DAG edges: the watch senders for the job's
/// publications.
///
/// The senders move into the job's future, so dependents observe a closed
/// channel the moment the job future is dropped (completion or pool abort).
pub(crate) struct DepPublisher {
    /// Channel carrying this job's resolved canonical version.
    version_tx: watch::Sender<Option<String>>,
    /// Channel carrying this job's processed payload map.
    own_map_tx: watch::Sender<Option<BTreeMap<String, String>>>,
}

impl DepPublisher {
    /// Publishes this job's resolved canonical version to its dependents.
    ///
    /// Succeeds even when no job declared an edge to this one.
    pub(crate) fn publish_version(&mut self, version: String) {
        self.version_tx.send_replace(Some(version));
    }

    /// Publishes this job's processed payload map to its dependents.
    ///
    /// Succeeds even when no job declared an edge to this one.
    pub(crate) fn publish_own_map(&mut self, map: BTreeMap<String, String>) {
        self.own_map_tx.send_replace(Some(map));
    }
}

/// The fail-fast result of [`run_pool`]: the index and error of the first job
/// that returned `Err`. Derives `Debug` so callers can wrap it in their own
/// error types and test harnesses can assert on it.
#[derive(Debug)]
pub(crate) struct PoolFailure<E> {
    /// Index of the job that failed (its [`PoolJob::index`]).
    pub index: usize,
    /// The error the failing job returned.
    pub source: E,
}

/// Runs `jobs` through `run` with at most `concurrency` jobs in flight, wiring
/// DAG-edge channels between jobs that declare same-step dependencies.
///
/// Every job gets a version channel and a payload-map channel keyed by its
/// [`PoolJob::index`]; `run` receives the job, a [`DepGates`] holding cloned
/// receivers for the job's declared deps, and the job's own [`DepPublisher`].
/// Publishing (see the module docs) is the caller's responsibility inside
/// `run`.
///
/// The first `Err` aborts the pool: `run_pool` returns that job's
/// [`PoolFailure`] and the remaining in-flight job futures are dropped
/// (cancelled). On success the collected `(index, outcome)` pairs are sorted
/// by index, so the caller applies outcomes in job order regardless of
/// completion order.
///
/// `concurrency` is clamped up to 1. Duplicate `PoolJob::index` values are a
/// caller bug and panic.
///
/// # Errors
///
/// Returns the first job error as [`PoolFailure`], carrying the failing job's
/// index and its source error.
pub(crate) async fn run_pool<I, O, E, F, Fut>(
    jobs: Vec<PoolJob<I>>,
    concurrency: usize,
    run: F,
) -> Result<Vec<(usize, O)>, PoolFailure<E>>
where
    I: Send,
    O: Send,
    E: Send,
    F: Fn(PoolJob<I>, DepGates, DepPublisher) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<O, E>> + Send,
{
    // One channel pair per job index; senders move into the jobs' publishers,
    // receivers are cloned into dependents' gates.
    let mut version_senders = HashMap::new();
    let mut own_map_senders = HashMap::new();
    let mut version_receivers = HashMap::new();
    let mut own_map_receivers = HashMap::new();
    for job in &jobs {
        let (version_tx, version_rx) = watch::channel::<Option<String>>(None);
        let (own_map_tx, own_map_rx) = watch::channel::<Option<BTreeMap<String, String>>>(None);
        version_senders.insert(job.index, version_tx);
        version_receivers.insert(job.index, version_rx);
        own_map_senders.insert(job.index, own_map_tx);
        own_map_receivers.insert(job.index, own_map_rx);
    }

    // Pair each job with its gates (cloned dep receivers) and its publisher
    // (its own senders) before anything runs, so the senders travel into the
    // job futures and close when those futures are dropped.
    let prepared: Vec<(DepGates, DepPublisher)> = jobs
        .iter()
        .map(|job| {
            let gates = DepGates {
                version_rx: job
                    .same_step_deps
                    .iter()
                    .filter_map(|&dep| {
                        version_receivers.get(&dep).map(|receiver| (dep, receiver.clone()))
                    })
                    .collect(),
                own_map_rx: job
                    .same_step_deps
                    .iter()
                    .filter_map(|&dep| {
                        own_map_receivers.get(&dep).map(|receiver| (dep, receiver.clone()))
                    })
                    .collect(),
            };
            // Every index was inserted in the pass above, so a miss here means
            // the index was declared twice — a caller bug; fail fast.
            let job_index = job.index;
            let Some(version_tx) = version_senders.remove(&job_index) else {
                panic!("duplicate pool job index {job_index}");
            };
            let Some(own_map_tx) = own_map_senders.remove(&job_index) else {
                panic!("duplicate pool job index {job_index}");
            };
            let publisher = DepPublisher { version_tx, own_map_tx };
            (gates, publisher)
        })
        .collect();

    // buffer_unordered polls the futures inline (no spawn), so dropping the
    // stream on abort drops every in-flight job future.
    let mut tasks =
        stream::iter(jobs.into_iter().zip(prepared).map(|(job, (gates, publisher))| {
            let index = job.index;
            let run = run.clone();
            async move { (index, run(job, gates, publisher).await) }
        }))
        .buffer_unordered(concurrency.max(1));

    let mut outcomes = Vec::new();
    while let Some((index, result)) = tasks.next().await {
        match result {
            Ok(outcome) => outcomes.push((index, outcome)),
            // Returning drops `tasks`, dropping every in-flight job future.
            Err(source) => return Err(PoolFailure { index, source }),
        }
    }
    outcomes.sort_unstable_by_key(|(index, _)| *index);
    Ok(outcomes)
}

#[cfg(test)]
mod tests {
    //! Unit tests pinning the pool's contract: index-sorted outcomes,
    //! dependency gating through the watch channels, no gating without
    //! declared edges, fail-fast abort with in-flight cancellation, the
    //! concurrency bound, and the empty-pool edge case.

    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use super::*;

    /// Outcomes must come back sorted by job index even when jobs complete in
    /// a different order, so callers can apply results deterministically.
    #[tokio::test(start_paused = true)]
    async fn pool_returns_outcomes_in_index_order_regardless_of_completion_order() {
        let jobs = vec![
            PoolJob { index: 0, same_step_deps: vec![], input: Duration::from_millis(30) },
            PoolJob { index: 1, same_step_deps: vec![], input: Duration::from_millis(10) },
            PoolJob { index: 2, same_step_deps: vec![], input: Duration::from_millis(20) },
        ];
        let outcomes = run_pool(jobs, 4, |job, _gates, _publisher| async move {
            tokio::time::sleep(job.input).await;
            Ok::<_, ()>(job.index)
        })
        .await
        .expect("all jobs succeed");
        assert_eq!(outcomes, vec![(0, 0), (1, 1), (2, 2)]);
    }

    /// A dependent must start its independent work immediately and only await
    /// its declared dep's publications at the gated stage; it must not be
    /// serialized behind the dep's full run.
    #[tokio::test(start_paused = true)]
    async fn pool_lets_a_dependent_start_before_its_dep_finishes() {
        // The event log records the interleaving: the dependent's start must
        // precede the dep's completion, and the dependent's gated stage must
        // follow the dep's own-map publication.
        let log = Arc::new(Mutex::new(Vec::<&'static str>::new()));
        let log_dep = log.clone();
        let log_ind = log.clone();
        let log_gate = log.clone();
        let jobs = vec![
            PoolJob { index: 0, same_step_deps: vec![], input: () },
            PoolJob { index: 1, same_step_deps: vec![0], input: () },
        ];
        let outcomes = run_pool(jobs, 4, move |job, gates, mut publisher| {
            let log = if job.index == 0 { log_dep.clone() } else { log_ind.clone() };
            let log_gate = log_gate.clone();
            async move {
                if job.index == 0 {
                    log.lock().unwrap().push("dep-resolve");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    publisher.publish_version("v0".to_string());
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    publisher.publish_own_map(BTreeMap::new());
                    log.lock().unwrap().push("dep-done");
                } else {
                    log.lock().unwrap().push("dependent-start");
                    gates.dep_version(0).await.expect("dep version");
                    gates.dep_own_map(0).await.expect("dep own map");
                    log_gate.lock().unwrap().push("dependent-gated");
                }
                Ok::<_, ()>(())
            }
        })
        .await
        .expect("both jobs succeed");
        assert_eq!(outcomes.len(), 2);
        let log = log.lock().unwrap();
        let start =
            log.iter().position(|entry| *entry == "dependent-start").expect("dependent started");
        let dep_done = log.iter().position(|entry| *entry == "dep-done").expect("dep finished");
        let gated =
            log.iter().position(|entry| *entry == "dependent-gated").expect("gated stage ran");
        assert!(start < dep_done, "dependent must start before its dep finishes: {log:?}");
        assert!(dep_done < gated, "gated stage must run after the dep published its map: {log:?}");
    }

    /// A job with empty `same_step_deps` must never wait for an unrelated job:
    /// with the paused clock, a job wrongly gated on the long sleeper would
    /// hang past the timeout instead of completing.
    #[tokio::test(start_paused = true)]
    async fn pool_does_not_gate_a_job_with_no_declared_edges() {
        let jobs = vec![
            PoolJob { index: 0, same_step_deps: vec![], input: Duration::from_mins(1) },
            PoolJob { index: 1, same_step_deps: vec![], input: Duration::from_millis(1) },
        ];
        // The success path waits for every job, so the short timeout must
        // exceed the longest job's virtual sleep; a gating bug that made job 1
        // wait for a channel that never resolves would never return at all.
        let outcomes = tokio::time::timeout(
            Duration::from_mins(2),
            run_pool(jobs, 2, |job, _gates, _publisher| async move {
                tokio::time::sleep(job.input).await;
                Ok::<_, ()>(job.index)
            }),
        )
        .await
        .expect("ungated job must not wait for an unrelated job")
        .expect("both jobs succeed");
        assert_eq!(outcomes, vec![(0, 0), (1, 1)]);
    }

    /// The first failure must abort the pool: the error propagates immediately
    /// and every in-flight job future is dropped (cancellation on drop).
    #[tokio::test(start_paused = true)]
    async fn pool_aborts_and_cancels_in_flight_jobs_on_the_first_failure() {
        /// Drop guard that flags cancellation when the holding job future is
        /// dropped by the pool's abort.
        struct CancelProbe(Arc<AtomicBool>);

        impl Drop for CancelProbe {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let dropped = Arc::new(AtomicBool::new(false));
        let probe = dropped.clone();
        let jobs = vec![
            PoolJob { index: 0, same_step_deps: vec![], input: () },
            PoolJob { index: 1, same_step_deps: vec![], input: () },
        ];
        let failure = run_pool(jobs, 2, move |job, _gates, _publisher| {
            let guard = (job.index == 0).then(|| CancelProbe(probe.clone()));
            async move {
                if let Some(_guard) = guard {
                    tokio::time::sleep(Duration::from_hours(1)).await;
                    Ok::<_, &'static str>(0)
                } else {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    Err("boom")
                }
            }
        })
        .await
        .expect_err("failure must propagate");
        assert_eq!(failure.index, 1);
        assert_eq!(failure.source, "boom");
        // One yield so a drop deferred by the abort runs before the assertion.
        tokio::task::yield_now().await;
        assert!(dropped.load(Ordering::SeqCst), "in-flight job must be cancelled");
    }

    /// The pool must never run more jobs than the requested concurrency width.
    #[tokio::test(start_paused = true)]
    async fn pool_bounds_concurrency_to_the_requested_width() {
        let active = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));
        let jobs: Vec<PoolJob<()>> =
            (0..6).map(|index| PoolJob { index, same_step_deps: vec![], input: () }).collect();
        let outcomes = run_pool(jobs, 2, {
            let active = active.clone();
            let max_seen = max_seen.clone();
            move |_job, _gates, _publisher| {
                let active = active.clone();
                let max_seen = max_seen.clone();
                async move {
                    let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                    max_seen.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    active.fetch_sub(1, Ordering::SeqCst);
                    Ok::<_, ()>(())
                }
            }
        })
        .await
        .expect("all jobs succeed");
        assert_eq!(outcomes.len(), 6);
        assert_eq!(max_seen.load(Ordering::SeqCst), 2, "never more than `concurrency` in flight");
    }

    /// An empty job list is an immediate success with no outcomes.
    #[tokio::test(start_paused = true)]
    async fn pool_with_no_jobs_returns_empty_success() {
        let jobs: Vec<PoolJob<()>> = vec![];
        let outcomes: Vec<(usize, ())> =
            run_pool(jobs, 4, |_job, _gates, _publisher| async { Ok::<(), ()>(()) })
                .await
                .expect("empty pool succeeds");
        assert!(outcomes.is_empty());
    }
}
