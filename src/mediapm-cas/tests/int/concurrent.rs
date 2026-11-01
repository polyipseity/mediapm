//! Tests for concurrent access patterns and advanced lifecycle scenarios.
//!
//! Covers:
//! - Write → Delete → Write re-creation cycles
//! - Concurrent put/get/delete on many hashes
//! - In-flight dedup concurrent stress test
//! - Clone-sharing across concurrent tasks
//! - `BackgroundEngine` cancellation safety
//! - Two concurrent puts that share one fan-out directory, repeated in-process

use std::collections::{BTreeSet, HashMap};

use bytes::Bytes;

use mediapm_cas::Hash;
use mediapm_cas::api::{CasApi, CasMaintenanceApi, ConstraintApi};
use mediapm_cas::new_in_memory_cas;

use crate::common::{open_file_cas, put_static};

/// Write → Delete → Write for the same content must work.
///
/// After deleting an object, putting the same content again must make it
/// retrievable — the new journal entry takes precedence in reverse scan.
#[tokio::test]
async fn write_delete_write_recreation() {
    let cas = new_in_memory_cas();
    let data = Bytes::from_static(b"recreate me");

    // First put.
    let hash = cas.put(data.clone()).await.unwrap();
    assert!(cas.get(hash).await.is_ok());

    // Delete.
    cas.delete(hash).await.unwrap();
    assert!(cas.get(hash).await.is_err());

    // Put same content again.
    let hash2 = cas.put(data.clone()).await.unwrap();
    assert_eq!(hash, hash2, "same content must produce same hash");

    // Must be retrievable after re-creation.
    let retrieved = cas.get(hash).await.unwrap();
    assert_eq!(retrieved, data);
}

/// Multiple write-delete-write cycles on the same content.
///
/// Exercises the WAL reverse-scan ordering across many tombstone →
/// re-creation transitions.
#[tokio::test]
async fn write_delete_write_cycle_multiple() {
    let cas = new_in_memory_cas();
    let data = Bytes::from_static(b"cycle content");

    for i in 0..5 {
        let hash = cas.put(data.clone()).await.unwrap();
        assert!(cas.get(hash).await.is_ok(), "cycle {i}: object should be present after put");
        cas.delete(hash).await.unwrap();
        assert!(cas.get(hash).await.is_err(), "cycle {i}: object should be absent after delete");
    }

    // Final put after all cycles — still works.
    let hash = cas.put(data).await.unwrap();
    assert!(cas.get(hash).await.is_ok());
}

/// Concurrent put/get/delete on many unique hashes.
///
/// Each task operates on its own independent hash; all share the same
/// `CasStore` instance. This exercises `DashMap`, WAL append, and the
/// read-view cache under contention.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_operations_many_hashes() {
    let cas = new_in_memory_cas();
    let task_count = 20;

    let mut handles = Vec::with_capacity(task_count);
    for i in 0..task_count {
        let cas_ref = cas.clone();
        handles.push(tokio::spawn(async move {
            let data = Bytes::from(format!("task-{i}"));
            let hash = cas_ref.put(data.clone()).await.unwrap();

            // Verify get after put.
            let retrieved = cas_ref.get(hash).await.unwrap();
            assert_eq!(retrieved, data, "task {i}: get after put mismatch");

            // Verify stat.
            let meta = cas_ref.stat(hash).await.unwrap();
            assert_eq!(meta.len, data.len() as u64, "task {i}: stat payload_len mismatch");

            // Delete and verify.
            cas_ref.delete(hash).await.unwrap();
            assert!(cas_ref.get(hash).await.is_err(), "task {i}: get after delete should fail");
        }));
    }

    for (i, handle) in handles.into_iter().enumerate() {
        handle.await.unwrap_or_else(|_| panic!("task {i} panicked"));
    }
}

/// Concurrent constraint operations on shared hashes.
///
/// Multiple tasks simultaneously set, patch, and get constraints on the
/// same target hash. Exercises `MetadataIndex` concurrent access patterns.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_constraint_operations() {
    let cas = new_in_memory_cas();

    // Prepare objects for constraint targets and bases.
    let target = cas.put(Bytes::from_static(b"target")).await.unwrap();
    let mut bases = Vec::with_capacity(20);
    for i in 0..20 {
        let data = Bytes::from(format!("base-{i}"));
        bases.push(cas.put(data).await.unwrap());
    }

    // Concurrently set constraints referencing different base subsets.
    let mut handles = Vec::new();
    for i in 0..10 {
        let cas_ref = cas.clone();
        let t = target;
        let b = bases[i * 2..(i + 1) * 2].to_vec();
        handles.push(tokio::spawn(async move {
            let bases_set: BTreeSet<_> = b.into_iter().collect();
            cas_ref.set_constraint(t, bases_set).await.unwrap();
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // Verify constraint exists (last concurrent set wins).
    let bases = cas.get_constraint(target).await.unwrap();
    assert!(!bases.is_empty(), "constraint should have bases");
}

/// Clone-sharing across concurrent tasks.
///
/// Two clones of the same `CasStore` must share state: an object put through
/// one clone is visible through the other, even when accessed concurrently.
#[tokio::test(flavor = "multi_thread")]
async fn cas_clone_concurrent_access() {
    const TASKS_PER_CLONE: usize = 10;

    let cas_a = new_in_memory_cas();
    let cas_b = cas_a.clone();

    // Phase 1: put objects through both clones concurrently.
    let mut puts_a = Vec::with_capacity(TASKS_PER_CLONE);
    let mut puts_b = Vec::with_capacity(TASKS_PER_CLONE);

    for i in 0..TASKS_PER_CLONE {
        let cas = cas_a.clone();
        puts_a.push(tokio::spawn(async move {
            let data = Bytes::from(format!("from-a-{i}"));
            cas.put(data).await.unwrap()
        }));
    }
    for i in 0..TASKS_PER_CLONE {
        let cas = cas_b.clone();
        puts_b.push(tokio::spawn(async move {
            let data = Bytes::from(format!("from-b-{i}"));
            cas.put(data).await.unwrap()
        }));
    }

    // Collect all hashes.
    let mut hashes = Vec::with_capacity(TASKS_PER_CLONE * 2);
    for handle in puts_a.into_iter().chain(puts_b.into_iter()) {
        hashes.push(handle.await.unwrap());
    }

    // Phase 2: verify all objects visible from both clones.
    for (i, hash) in hashes.iter().enumerate() {
        let retrieved_a = cas_a.get(*hash).await.unwrap();
        let retrieved_b = cas_b.get(*hash).await.unwrap();
        assert_eq!(retrieved_a, retrieved_b, "object {i} differs between clones");
    }
}

/// `BackgroundEngine` cancellation does not corrupt state.
///
/// Requesting cancellation causes maintenance (optimizer + constraint
/// pruning) to exit early without errors. State remains consistent.
#[tokio::test]
async fn bg_engine_cancellation_graceful() {
    let cas = new_in_memory_cas();

    let keep = put_static(&cas, b"keep").await;
    cas.set_constraint(keep, BTreeSet::new()).await.unwrap();

    // Drain WAL and run maintenance normally.
    cas.run_maintenance_cycle().await.unwrap();
    assert!(cas.get(keep).await.is_ok());

    // Cancel before further maintenance.
    cas.bg_engine().request_cancel();
    assert!(cas.bg_engine().is_cancelled());

    // Run run_maintenance_cycle again — WAL empty, maintenance exits early.
    let report = cas.run_maintenance_cycle().await.unwrap();
    assert_eq!(report.wal_entries_consumed, 0);
    assert!(!report.maintenance_done);

    // State consistent.
    assert!(cas.get(keep).await.is_ok());

    // prune_constraints works independently.
    let sweep = cas.prune_constraints().await.unwrap();
    assert_eq!(sweep.targets_removed, 0);
}

/// In-flight dedup concurrent stress test.
///
/// Many tasks simultaneously put the *same* content (same bytes → same
/// hash), then concurrently get and delete it. This exercises the CAS
/// dedup path under contention: WAL append, read-view cache updates,
/// and the in-memory object store's write-once semantics must all handle
/// concurrent identical puts without data loss or corruption.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_dedup_same_content() {
    let cas = new_in_memory_cas();
    let task_count = 15;

    // Shared content payload that all tasks will put concurrently.
    let content_small = Bytes::from_static(b"small shared content");
    let content_large = Bytes::from(vec![0xABu8; 4096]);

    // Phase 1: concurrent puts of same small content.
    let mut handles = Vec::with_capacity(task_count);
    for i in 0..task_count {
        let cas_ref = cas.clone();
        let data = content_small.clone();
        handles.push(tokio::spawn(async move {
            let hash = cas_ref.put(data).await.unwrap();
            (i, hash)
        }));
    }

    let mut hashes: Vec<(usize, Hash)> = Vec::with_capacity(task_count);
    for handle in handles {
        hashes.push(handle.await.unwrap());
    }

    // All tasks must agree on the same hash (content-addressable identity).
    let expected = hashes[0].1;
    for (i, hash) in &hashes[1..] {
        assert_eq!(*hash, expected, "task {i}: hash must match for same content");
    }

    // Object retrievable after all puts.
    let retrieved = cas.get(expected).await.unwrap();
    assert_eq!(retrieved, content_small);

    // Phase 2: concurrent puts of same large content.
    let mut handles = Vec::with_capacity(task_count);
    for _ in 0..task_count {
        let cas_ref = cas.clone();
        let data = content_large.clone();
        handles.push(tokio::spawn(async move { cas_ref.put(data).await.unwrap() }));
    }

    let mut large_hashes = Vec::with_capacity(task_count);
    for handle in handles {
        large_hashes.push(handle.await.unwrap());
    }

    let expected_large = large_hashes[0];
    for (i, hash) in large_hashes[1..].iter().enumerate() {
        assert_eq!(*hash, expected_large, "large content task {i}: hash mismatch");
    }

    let retrieved_large = cas.get(expected_large).await.unwrap();
    assert_eq!(retrieved_large, content_large);

    // Phase 3: concurrent deletes and re-puts of same content.
    let mut handles = Vec::with_capacity(task_count * 2);
    // Half of tasks try to delete then re-put, half just get.
    for i in 0..task_count {
        let cas_ref = cas.clone();
        let data = content_small.clone();
        let h = expected;
        if i % 2 == 0 {
            handles.push(tokio::spawn(async move {
                // Delete (may race) then put same content.
                let _ = cas_ref.delete(h).await;
                let hash = cas_ref.put(data).await.unwrap();
                assert_eq!(hash, h, "re-put after delete must produce same hash");
            }));
        } else {
            handles.push(tokio::spawn(async move {
                // Just get — might succeed or fail depending on race.
                let _ = cas_ref.get(h).await;
            }));
        }
    }

    for (i, handle) in handles.into_iter().enumerate() {
        handle.await.unwrap_or_else(|_| panic!("dedup stress task {i} panicked"));
    }

    // At least one successful re-put must have happened, so the object
    // should be retrievable eventually.
    // Re-put one more time to guarantee it's present.
    let final_hash = cas.put(content_small.clone()).await.unwrap();
    assert_eq!(final_hash, expected);
    let final_data = cas.get(final_hash).await.unwrap();
    assert_eq!(final_data, content_small);
}

/// `PendingOps` read dedup — many concurrent `get()` on the same hash.
///
/// Verifies that when N tasks simultaneously read the same hash, all
/// receive the same bytes. This exercises the in-flight read dedup
/// (`PendingOps`) where only one task performs the actual lookup while
/// others wait for its result.
#[tokio::test(flavor = "multi_thread")]
async fn pending_ops_deduplicates_concurrent_gets() {
    let cas = new_in_memory_cas();

    // Put a single object.
    let data = Bytes::from_static(b"shared dedup target");
    let hash = cas.put(data.clone()).await.unwrap();

    // Spawn N tasks all getting the same hash concurrently.
    let task_count = 15;
    let mut handles = Vec::with_capacity(task_count);
    for _ in 0..task_count {
        let cas_ref = cas.clone();
        let h = hash;
        handles.push(tokio::spawn(async move { cas_ref.get(h).await }));
    }

    // All must return the same data.
    for (i, handle) in handles.into_iter().enumerate() {
        let result = handle.await.unwrap().unwrap_or_else(|_| panic!("get task {i} failed"));
        assert_eq!(result, data, "task {i}: data mismatch");
    }
}

/// Concurrent put safety for `FileSystemCas`.
///
/// Exercises the real WAL journal, flock, and on-disk metadata under
/// contention — the `InMemory` CAS path is already covered by
/// `concurrent_operations_many_hashes` and `concurrent_dedup_same_content`.
#[tokio::test(flavor = "multi_thread")]
async fn file_system_cas_concurrent_puts_are_safe() {
    use mediapm_cas::api::CasMaintenanceApi;

    let cas = open_file_cas().await;
    let task_count = 16;

    // Phase 1: concurrent puts of distinct payloads.
    let mut handles = Vec::with_capacity(task_count);
    for i in 0..task_count {
        let cas_ref = cas.clone();
        handles.push(tokio::spawn(async move {
            let data = Bytes::from(format!("unique-payload-{i}"));
            let hash = cas_ref.put(data.clone()).await.unwrap();
            let retrieved = cas_ref.get(hash).await.unwrap();
            assert_eq!(retrieved, data, "phase 1 task {i}: content mismatch");
            let meta = cas_ref.stat(hash).await.unwrap();
            assert_eq!(meta.len, data.len() as u64, "phase 1 task {i}: stat len mismatch");
            hash
        }));
    }
    let mut phase1_hashes = Vec::with_capacity(task_count);
    for (i, handle) in handles.into_iter().enumerate() {
        phase1_hashes.push(handle.await.unwrap_or_else(|_| panic!("phase 1 task {i} panicked")));
    }
    // All 16 hashes must be distinct.
    let unique: std::collections::HashSet<_> = phase1_hashes.iter().copied().collect();
    assert_eq!(unique.len(), task_count, "all phase-1 hashes must be distinct");

    // Phase 2: concurrent puts of identical payloads.
    let shared_content = Bytes::from_static(b"shared-identical-content");
    let mut handles = Vec::with_capacity(task_count);
    for _ in 0..task_count {
        let cas_ref = cas.clone();
        let data = shared_content.clone();
        handles.push(tokio::spawn(async move { cas_ref.put(data).await.unwrap() }));
    }
    let mut phase2_hashes = Vec::with_capacity(task_count);
    for handle in handles {
        phase2_hashes.push(handle.await.unwrap());
    }
    let expected_phase2 = phase2_hashes[0];
    for (i, hash) in phase2_hashes[1..].iter().enumerate() {
        assert_eq!(*hash, expected_phase2, "phase 2 task {i}: hash must match");
    }
    let retrieved = cas.get(expected_phase2).await.unwrap();
    assert_eq!(retrieved, shared_content);
    // Phase-2 hash must differ from all phase-1 hashes.
    assert!(!phase1_hashes.contains(&expected_phase2), "phase-2 hash must differ from phase-1");

    // Phase 3: WAL durability — flush then re-read.
    cas.run_maintenance_cycle().await.unwrap();
    let re_read_phase1 = cas.get(phase1_hashes[0]).await.unwrap();
    assert_eq!(re_read_phase1, Bytes::from("unique-payload-0"));
    let re_read_phase2 = cas.get(expected_phase2).await.unwrap();
    assert_eq!(re_read_phase2, shared_content);
}

/// Rounds the shared-directory pair is repeated for.
///
/// The failure this covers appeared once in five isolated runs, so one round per
/// process is the shape that hid it: repetition in one process is what turns
/// "seen once" into a count.
const FAN_OUT_ROUNDS: usize = 50;

/// Length of each probe payload: one byte over the WAL inline limit.
///
/// The length is load-bearing. A `put` at or below the limit only appends a
/// `Put` entry and leaves the blob and its fan-out snapshot to the WAL consumer,
/// so a pair of small payloads would never reach the writes that share the
/// directory. Deriving the length from the limit rather than repeating `1 MiB`
/// keeps the probe on the eager path if the limit moves.
#[expect(
    clippy::cast_possible_truncation,
    reason = "the inline limit is 1 MiB, far below the smallest usize of any target this workspace \
              builds for"
)]
const FAN_OUT_PAYLOAD_LEN: usize = mediapm_cas::defaults::WAL_INLINE_LIMIT as usize + 1;

/// Width of the per-probe index suffix, in bytes.
const PROBE_INDEX_WIDTH: usize = std::mem::size_of::<u32>();

/// Byte every probe body is filled with.
const PROBE_FILL: u8 = 0x5a;

/// Number of hex digits that name a fan-out directory.
const FAN_OUT_PREFIX_LEN: usize = 4;

/// How many probe payloads the search may hash before it gives up.
///
/// A leaf directory is named by four hex digits, so two probes collide with
/// probability 1/65536 per pair and the birthday bound puts the first collision
/// within a few hundred probes. This bound leaves a wide margin.
const FAN_OUT_PROBES: u32 = 4096;

/// Builds probe payload `index`: a filled body whose last four bytes are the
/// index.
fn fan_out_probe(index: u32) -> Bytes {
    let mut payload = vec![PROBE_FILL; FAN_OUT_PAYLOAD_LEN];
    let suffix = FAN_OUT_PAYLOAD_LEN - PROBE_INDEX_WIDTH;
    payload[suffix..].copy_from_slice(&index.to_be_bytes());
    Bytes::from(payload)
}

/// The four hex digits that name `payload`'s fan-out directory.
fn fan_out_prefix(payload: &Bytes) -> String {
    let hex = Hash::from_content(payload).to_hex();
    hex[..FAN_OUT_PREFIX_LEN].to_string()
}

/// Two payloads whose hashes share a fan-out directory.
///
/// The blob store lays a blob out under `v1/blake3/<hex[0..2]>/<hex[2..4]>/`
/// (`storage::blob_store::versions::v1::hash_to_path`) and the metadata store
/// snapshots one file per `to_hex()[..4]` prefix
/// (`FileSystemMetadataStore::flush_dir`), so agreement on the first four hex
/// digits is what puts both payloads in one directory. The search is bounded and
/// fails loudly: a pair that did not share a directory would leave the test
/// above exercising nothing.
fn shared_fan_out_payloads() -> (Bytes, Bytes) {
    // Every probe shares its body, so the body is hashed once and the chaining
    // state is cloned per candidate: a body is a mebibyte, and re-hashing it for
    // every index would cost seconds in an unoptimized build for no more
    // information. `blake3`'s streaming digest is the same value as its one-shot
    // digest over the same bytes; the assertions below pin that for the pair
    // this returns, because the store derives paths with the one-shot form.
    let body = vec![PROBE_FILL; FAN_OUT_PAYLOAD_LEN - PROBE_INDEX_WIDTH];
    let mut shared = blake3::Hasher::new();
    shared.update(&body);

    let mut seen: HashMap<String, u32> = HashMap::new();
    for index in 0..FAN_OUT_PROBES {
        let mut hasher = shared.clone();
        hasher.update(&index.to_be_bytes());
        let hex = Hash::from_bytes(*hasher.finalize().as_bytes()).to_hex();
        let prefix = &hex[..FAN_OUT_PREFIX_LEN];
        if let Some(&twin) = seen.get(prefix) {
            let pair = (fan_out_probe(twin), fan_out_probe(index));
            for payload in [&pair.0, &pair.1] {
                assert_eq!(
                    fan_out_prefix(payload),
                    prefix,
                    "the incremental search must agree with Hash::from_content",
                );
            }
            return pair;
        }
        seen.insert(prefix.to_string(), index);
    }
    panic!(
        "no two of {FAN_OUT_PROBES} probes shared a fan-out directory; the pair is not constructed"
    );
}

/// Two puts whose fan-out directory is shared both land and both read back.
///
/// The pair is repeated in one process because the failure this covers appeared
/// once in five isolated runs: a single round is the shape that hid it. The
/// directory is shared by construction — see [`shared_fan_out_payloads`]. The
/// payloads exceed the WAL inline limit so both puts write the directory now:
/// the leaf's `create_dir_all`, the two blobs staged and renamed beside each
/// other, and the one snapshot file the directory holds.
///
/// The cause is unproven: fifty rounds in one process did not reproduce the
/// failure seen once in five isolated runs, and neither did five such runs nor
/// a single run of five hundred rounds, so this pins the property that must
/// hold rather than a mechanism that breaks it. It also guards the property
/// directly: each round asserts the two blobs share a parent directory and are
/// on disk, so a store that stopped writing either eagerly would fail here
/// instead of passing while covering nothing.
#[tokio::test(flavor = "multi_thread")]
async fn two_puts_into_one_fan_out_directory_both_land() {
    let fixture = open_file_cas().await;
    let cas = (*fixture).clone();

    let (left, right) = shared_fan_out_payloads();
    for round in 0..FAN_OUT_ROUNDS {
        let left_cas = cas.clone();
        let right_cas = cas.clone();
        // Each task owns its payload; `Bytes` is not `Copy`, so the round keeps
        // its own handle for the read-back below.
        let left_data = left.clone();
        let right_data = right.clone();
        let (left_hash, right_hash) = tokio::join!(
            tokio::spawn(async move { left_cas.put(left_data).await }),
            tokio::spawn(async move { right_cas.put(right_data).await }),
        );
        let left_hash = left_hash.expect("left task must not panic").expect("left put must land");
        let right_hash =
            right_hash.expect("right task must not panic").expect("right put must land");

        // The claim this test covers is that the two puts write ONE directory,
        // and that they write it at all: a store that deferred a mebibyte to the
        // WAL consumer would leave the pair landing in one directory in name
        // only.
        let left_path =
            cas.object_path_for_hash(left_hash).expect("a file-system store exposes object paths");
        let right_path =
            cas.object_path_for_hash(right_hash).expect("a file-system store exposes object paths");
        assert_eq!(
            left_path.parent(),
            right_path.parent(),
            "round {round}: the pair must share a fan-out directory",
        );
        assert!(
            left_path.is_file() && right_path.is_file(),
            "round {round}: both puts must have written their blob, not deferred it to the WAL \
             consumer",
        );

        assert_eq!(
            cas.get(left_hash).await.expect("left object must read back"),
            left,
            "round {round}: the left object must survive its neighbour",
        );
        assert_eq!(
            cas.get(right_hash).await.expect("right object must read back"),
            right,
            "round {round}: the right object must survive its neighbour",
        );
    }
}
