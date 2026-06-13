//! Tests for concurrent access patterns and advanced lifecycle scenarios.
//!
//! Covers:
//! - Write → Delete → Write re-creation cycles
//! - Concurrent put/get/delete on many hashes
//! - In-flight dedup concurrent stress test
//! - Clone-sharing across concurrent tasks
//! - BackgroundEngine cancellation safety

use std::collections::BTreeSet;

use bytes::Bytes;

use mediapm_cas::Hash;
use mediapm_cas::api::{CasApi, CasMaintenanceApi, ConstraintApi};
use mediapm_cas::new_in_memory_cas;

/// Write → Delete → Write for the same content must work.
///
/// After deleting an object, putting the same content again must make it
/// retrievable — the new journal entry takes precedence in reverse scan,
/// and the read-view cache is refreshed by `hint_state_change`.
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
/// CasStore instance. This exercises DashMap, WAL append, and the
/// read-view cache under contention.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_operations_many_hashes() {
    let cas = new_in_memory_cas();
    let task_count = 50;

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
/// same target hash. Exercises MetadataIndex concurrent access patterns.
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

    // Verify constraint exists.
    let retrieved = cas.get_constraint(target).await.unwrap();
    assert!(retrieved.is_some(), "constraint should exist");
    let bases = retrieved.unwrap();
    // At least some bases should be present (last concurrent set wins).
    assert!(!bases.is_empty(), "constraint should have bases");
}

/// Clone-sharing across concurrent tasks.
///
/// Two clones of the same CasStore must share state: an object put through
/// one clone is visible through the other, even when accessed concurrently.
#[tokio::test(flavor = "multi_thread")]
async fn cas_clone_concurrent_access() {
    let cas_a = new_in_memory_cas();
    let cas_b = cas_a.clone();

    const TASKS_PER_CLONE: usize = 20;

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

/// BackgroundEngine cancellation does not corrupt state.
///
/// Requesting cancellation causes maintenance (optimizer + constraint
/// pruning) to exit early without errors. State remains consistent.
#[tokio::test]
async fn bg_engine_cancellation_graceful() {
    let cas = new_in_memory_cas();

    let keep = cas.put(Bytes::from_static(b"keep")).await.unwrap();
    cas.set_constraint(keep, BTreeSet::new()).await.unwrap();

    // Drain WAL and run maintenance normally.
    cas.optimize_once().await.unwrap();
    assert!(cas.get(keep).await.is_ok());

    // Cancel before further maintenance.
    cas.bg_engine().request_cancel();
    assert!(cas.bg_engine().is_cancelled());

    // Run optimize_once again — WAL empty, maintenance exits early.
    let report = cas.optimize_once().await.unwrap();
    assert_eq!(report.wal_entries_consumed, 0);
    assert!(!report.maintenance_done);

    // State consistent.
    assert!(cas.get(keep).await.is_ok());

    // prune_constraints works independently.
    let sweep = cas.prune_constraints().await.unwrap();
    assert_eq!(sweep.removed, 0);
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
    let task_count = 30;

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
