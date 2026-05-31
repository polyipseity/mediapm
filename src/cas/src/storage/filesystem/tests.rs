use std::collections::BTreeSet;
use std::time::Duration;

use bytes::Bytes;
use futures_util::StreamExt;
use tempfile::{TempDir, tempdir};
use tokio::io::AsyncReadExt;

use super::FileSystemCas;
use crate::{
    CasApi, CasError, CasMaintenanceApi, Constraint, ConstraintPatch, FileSystemRecoveryOptions,
    Hash, HashAlgorithm, IndexRecoveryMode, ObjectMeta, OptimizeOptions, empty_content_hash,
};

async fn open_temp_filesystem_cas() -> (TempDir, FileSystemCas) {
    let dir = tempdir().expect("tempdir");
    let cas = FileSystemCas::open_for_tests(dir.path()).await.expect("cas should open");
    (dir, cas)
}

fn count_backup_snapshots(root: &std::path::Path) -> usize {
    let backup_root = root.join("index-backups");
    let Ok(entries) = std::fs::read_dir(backup_root) else {
        return 0;
    };

    entries
        .flatten()
        .filter_map(|entry| {
            entry
                .file_name()
                .to_str()
                .map(|name| name.starts_with("index-backup-") && name.ends_with(".postcard"))
        })
        .filter(|is_backup| *is_backup)
        .count()
}

#[tokio::test]
async fn constructors_expose_explicit_drop_grace_policies() {
    let dir = tempdir().expect("tempdir");

    let production = FileSystemCas::open(dir.path()).await.expect("open production cas");
    assert_eq!(production.drop_grace_period, Duration::from_secs(2));
    drop(production);

    let test = FileSystemCas::open_for_tests(dir.path()).await.expect("open test cas");
    assert_eq!(test.drop_grace_period, Duration::from_millis(25));
}

#[tokio::test]
async fn open_bootstraps_index_and_empty_object() {
    let (dir, cas) = open_temp_filesystem_cas().await;

    assert!(dir.path().join("index.redb").is_file());
    let empty_path = cas.object_path_for_hash(empty_content_hash());
    assert!(empty_path.is_file());

    let empty_payload = cas.get(empty_content_hash()).await.expect("get empty payload");
    assert!(empty_payload.is_empty(), "empty content hash must resolve to empty bytes");
}

#[tokio::test]
async fn filesystem_backup_snapshot_interval_batches_mutation_backups() {
    let dir = tempdir().expect("tempdir");
    let recovery = FileSystemRecoveryOptions {
        mode: IndexRecoveryMode::Recover,
        max_backup_snapshots: 16,
        backup_snapshot_interval_ops: 3,
    };
    let cas = FileSystemCas::open_with_alpha_and_recovery_for_tests(dir.path(), 4, recovery)
        .await
        .expect("open fs cas with interval recovery settings");

    let initial = count_backup_snapshots(dir.path());
    assert_eq!(initial, 1, "open should persist one initial backup snapshot");

    cas.put(Bytes::from_static(b"interval-a")).await.expect("put #1");
    assert_eq!(count_backup_snapshots(dir.path()), initial);

    cas.put(Bytes::from_static(b"interval-b")).await.expect("put #2");
    assert_eq!(count_backup_snapshots(dir.path()), initial);

    cas.put(Bytes::from_static(b"interval-c")).await.expect("put #3");
    assert_eq!(count_backup_snapshots(dir.path()), initial + 1);
}

#[test]
fn localized_depth_recompute_updates_transitive_descendants() {
    let base = Hash::from_content(b"depth-base");
    let pivot = Hash::from_content(b"depth-pivot");
    let child = Hash::from_content(b"depth-child");
    let grandchild = Hash::from_content(b"depth-grandchild");

    let mut index = crate::IndexState::default();
    crate::ensure_empty_record(&mut index);
    index.objects.insert(base, ObjectMeta::full(4, 4, 1));
    index.objects.insert(pivot, ObjectMeta::full(4, 4, 1));
    index.objects.insert(child, ObjectMeta::delta(2, 4, 2, pivot));
    index.objects.insert(grandchild, ObjectMeta::delta(2, 4, 3, child));
    index.rebuild_delta_reverse();

    let next_pivot = ObjectMeta::delta(2, 4, 2, base);
    let previous_pivot = index.objects.insert(pivot, next_pivot);
    super::FileSystemState::sync_delta_reverse_for_meta_update(
        &mut index,
        pivot,
        previous_pivot,
        next_pivot,
    );

    let roots = BTreeSet::from([pivot]);
    super::FileSystemState::recompute_descendant_depths_with_fallback(&mut index, &roots)
        .expect("localized depth recompute should succeed");

    assert_eq!(index.objects.get(&pivot).expect("pivot meta").depth(), 2);
    assert_eq!(index.objects.get(&child).expect("child meta").depth(), 3);
    assert_eq!(index.objects.get(&grandchild).expect("grandchild meta").depth(), 4);
}

#[tokio::test]
async fn filesystem_put_get_delete_lifecycle() {
    let (_dir, cas) = open_temp_filesystem_cas().await;

    let payload = Bytes::from_static(b"fs-delete");
    let hash = cas.put(payload.clone()).await.expect("put payload");
    assert_eq!(cas.get(hash).await.expect("get payload"), payload);

    cas.delete(hash).await.expect("delete payload");
    let missing = cas.get(hash).await;
    assert!(missing.is_err(), "deleted payload should not be retrievable");
}

/// Protects CAS filesystem hygiene by pruning empty fanout directories after
/// object deletion while keeping shared `v1/tmp` staging intact.
#[tokio::test]
async fn filesystem_delete_prunes_empty_fanout_directories_but_keeps_tmp() {
    let (dir, cas) = open_temp_filesystem_cas().await;

    let hash = cas.put(Bytes::from_static(b"fanout-prune-target")).await.expect("put payload");
    let object_path = cas.object_path_for_hash(hash);
    let leaf_dir = object_path.parent().expect("leaf dir").to_path_buf();
    let branch_dir = leaf_dir.parent().expect("branch dir").to_path_buf();
    let storage_root = dir.path().join(super::STORAGE_VERSION);
    let shared_tmp = storage_root.join("tmp");

    assert!(leaf_dir.exists(), "put should materialize object fanout leaf directory");
    assert!(shared_tmp.exists(), "put should materialize shared staging tmp directory");

    cas.delete(hash).await.expect("delete payload");

    assert!(cas.get(hash).await.is_err(), "deleted payload should not be retrievable");
    assert!(!leaf_dir.exists(), "deleting last object in fanout leaf should prune leaf directory");
    assert!(
        !branch_dir.exists(),
        "deleting last object in fanout branch should prune branch directory"
    );
    assert!(shared_tmp.exists(), "shared staging tmp directory must remain for atomic CAS writes");
}

#[tokio::test]
async fn filesystem_uses_single_shared_staging_tmp_directory() {
    let (dir, cas) = open_temp_filesystem_cas().await;

    cas.put(Bytes::from_static(b"staging tmp regression payload")).await.expect("put payload");

    let storage_root = dir.path().join(super::STORAGE_VERSION);
    let expected_tmp = storage_root.join("tmp");
    assert!(expected_tmp.exists(), "shared staging tmp should exist under storage root");

    let mut stack = vec![storage_root.clone()];
    let mut tmp_dirs = Vec::new();
    while let Some(path) = stack.pop() {
        for entry in std::fs::read_dir(&path).expect("read_dir") {
            let entry = entry.expect("dir entry");
            let child = entry.path();
            if child.is_dir() {
                if child.file_name().is_some_and(|name| name == "tmp") {
                    tmp_dirs.push(child.clone());
                }
                stack.push(child);
            }
        }
    }

    assert_eq!(tmp_dirs, vec![expected_tmp]);
}

#[tokio::test]
async fn filesystem_set_constraint_rejects_missing_base() {
    let (_dir, cas) = open_temp_filesystem_cas().await;

    let target = cas.put(Bytes::from_static(b"target")).await.expect("target store should succeed");
    let missing = Hash::from_content(b"missing");
    let result = cas
        .set_constraint(Constraint {
            target_hash: target,
            potential_bases: BTreeSet::from([missing]),
        })
        .await;

    assert!(result.is_err(), "missing-base constraints must be rejected");
    assert_ne!(empty_content_hash(), target);
}

#[tokio::test]
async fn filesystem_delete_preserves_delta_descendants_via_rewrite() {
    let (_dir, cas) = open_temp_filesystem_cas().await;

    let base_payload = vec![b'a'; 8 * 1024];
    let mut target_payload = base_payload.clone();
    target_payload[2_048] = b'z';

    let base_hash = cas.put(Bytes::from(base_payload)).await.expect("put base");
    let target_hash = Hash::from_content(&target_payload);
    let stored_target = cas.put(Bytes::from(target_payload.clone())).await.expect("put target");
    assert_eq!(stored_target, target_hash);

    cas.set_constraint(Constraint { target_hash, potential_bases: BTreeSet::from([base_hash]) })
        .await
        .expect("set target constraint");

    cas.delete(base_hash).await.expect("delete base while preserving descendants");

    assert!(cas.get(base_hash).await.is_err());
    assert_eq!(
        cas.get(target_hash).await.expect("target must remain reconstructible"),
        Bytes::from(target_payload)
    );

    let target_constraint =
        cas.get_constraint(target_hash).await.expect("get target constraint after base delete");
    if let Some(constraint) = target_constraint {
        assert!(
            !constraint.potential_bases.contains(&base_hash),
            "rewritten descendant constraint must not retain deleted base"
        );
    }
}

#[tokio::test]
async fn filesystem_exists_many_returns_ordered_bitset() {
    let (_dir, cas) = open_temp_filesystem_cas().await;

    let a = cas.put(Bytes::from_static(b"a")).await.expect("put a");
    let b = cas.put(Bytes::from_static(b"b")).await.expect("put b");
    let missing = Hash::from_content(b"missing");

    let flags = cas.exists_many(vec![a, missing, b]).await.expect("exists_many");

    assert_eq!(flags.iter().by_vals().collect::<Vec<_>>(), vec![true, false, true]);
}

#[tokio::test]
async fn filesystem_patch_constraint_add_remove_and_clear() {
    let (_dir, cas) = open_temp_filesystem_cas().await;

    let target = cas.put(Bytes::from_static(b"target")).await.expect("put target");
    let base_a = cas.put(Bytes::from_static(b"base-a")).await.expect("put base_a");
    let base_b = cas.put(Bytes::from_static(b"base-b")).await.expect("put base_b");

    cas.set_constraint(Constraint {
        target_hash: target,
        potential_bases: BTreeSet::from([base_a]),
    })
    .await
    .expect("set initial constraint");

    let patched = cas
        .patch_constraint(
            target,
            ConstraintPatch {
                add_bases: BTreeSet::from([base_b]),
                remove_bases: BTreeSet::from([base_a]),
                clear_existing: false,
            },
        )
        .await
        .expect("patch constraint");

    assert_eq!(
        patched,
        Some(Constraint { target_hash: target, potential_bases: BTreeSet::from([base_b]) })
    );

    let cleared = cas
        .patch_constraint(
            target,
            ConstraintPatch {
                add_bases: BTreeSet::new(),
                remove_bases: BTreeSet::new(),
                clear_existing: true,
            },
        )
        .await
        .expect("clear explicit constraint");

    assert!(cleared.is_none());
    assert!(cas.get_constraint(target).await.expect("get constraint").is_none());
}

#[tokio::test]
async fn filesystem_delete_respects_patched_constraint_reverse_links() {
    let (_dir, cas) = open_temp_filesystem_cas().await;

    let base_a = cas.put(Bytes::from_static(b"base-a")).await.expect("put base_a");
    let base_b = cas.put(Bytes::from_static(b"base-b")).await.expect("put base_b");
    let target_a = cas.put(Bytes::from_static(b"target-a")).await.expect("put target_a");
    let target_b = cas.put(Bytes::from_static(b"target-b")).await.expect("put target_b");

    cas.set_constraint(Constraint {
        target_hash: target_a,
        potential_bases: BTreeSet::from([base_a]),
    })
    .await
    .expect("set target_a constraint");
    cas.set_constraint(Constraint {
        target_hash: target_b,
        potential_bases: BTreeSet::from([base_a]),
    })
    .await
    .expect("set target_b constraint");

    cas.patch_constraint(
        target_a,
        ConstraintPatch {
            add_bases: BTreeSet::from([base_b]),
            remove_bases: BTreeSet::from([base_a]),
            clear_existing: false,
        },
    )
    .await
    .expect("patch target_a constraint");

    cas.delete(base_a).await.expect("delete base_a");

    assert_eq!(
        cas.get_constraint(target_a).await.expect("target_a constraint"),
        Some(Constraint { target_hash: target_a, potential_bases: BTreeSet::from([base_b]) })
    );
    assert!(
        cas.get_constraint(target_b).await.expect("target_b constraint").is_none(),
        "target_b should become unconstrained after deleting its only base"
    );
}

#[tokio::test]
async fn filesystem_put_stream_with_constraints_sets_constraint() {
    let (_dir, cas) = open_temp_filesystem_cas().await;
    let base = cas.put(Bytes::from_static(b"stream-base")).await.expect("put base");

    let reader = Box::new(tokio::io::repeat(b'y').take(5));
    let hash = cas
        .put_stream_with_constraints(reader, BTreeSet::from([base]))
        .await
        .expect("put_stream_with_constraints");

    let bytes = cas.get(hash).await.expect("get streamed object");
    assert_eq!(bytes, Bytes::from_static(b"yyyyy"));
    assert_eq!(
        cas.get_constraint(hash).await.expect("get constraint row"),
        Some(Constraint { target_hash: hash, potential_bases: BTreeSet::from([base]) })
    );
}

#[tokio::test]
async fn filesystem_put_stream_hash_matches_multihash_identity() {
    let (_dir, cas) = open_temp_filesystem_cas().await;

    let len = (super::FILESYSTEM_STREAM_READ_CHUNK_BYTES * 2) + 257;
    let payload = vec![b'z'; len];
    let expected = Hash::from_content_with_algorithm(HashAlgorithm::Blake3, &payload);

    let reader = Box::new(tokio::io::repeat(b'z').take(len as u64));
    let hash = cas.put_stream(reader).await.expect("put_stream");

    assert_eq!(hash, expected);
    assert_eq!(cas.get(hash).await.expect("get streamed payload").len(), len);
}

#[tokio::test]
async fn filesystem_info_many_returns_ordered_metadata() {
    let (_dir, cas) = open_temp_filesystem_cas().await;

    let a = cas.put(Bytes::from_static(b"aa")).await.expect("put a");
    let b = cas.put(Bytes::from_static(b"bbb")).await.expect("put b");

    let infos = cas.info_many(vec![b, a]).await.expect("info_many");

    assert_eq!(infos.len(), 2);
    assert_eq!(infos[0].0, b);
    assert_eq!(infos[0].1.content_len, 3);
    assert_eq!(infos[1].0, a);
    assert_eq!(infos[1].1.content_len, 2);
}

#[tokio::test]
async fn filesystem_get_constraint_many_returns_ordered_rows() {
    let (_dir, cas) = open_temp_filesystem_cas().await;

    let base = cas.put(Bytes::from_static(b"base")).await.expect("put base");
    let constrained = cas.put(Bytes::from_static(b"constrained")).await.expect("put constrained");
    let unrestricted = cas.put(Bytes::from_static(b"free")).await.expect("put unrestricted");

    cas.set_constraint(Constraint {
        target_hash: constrained,
        potential_bases: BTreeSet::from([base]),
    })
    .await
    .expect("set constraint");

    let rows = cas
        .get_constraint_many(vec![unrestricted, constrained])
        .await
        .expect("get_constraint_many");

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (unrestricted, None));
    assert_eq!(
        rows[1],
        (
            constrained,
            Some(Constraint { target_hash: constrained, potential_bases: BTreeSet::from([base]) }),
        )
    );
}

#[tokio::test]
async fn filesystem_delete_many_removes_all_hashes() {
    let (_dir, cas) = open_temp_filesystem_cas().await;

    let a = cas.put(Bytes::from_static(b"a")).await.expect("put a");
    let b = cas.put(Bytes::from_static(b"b")).await.expect("put b");

    cas.delete_many(vec![a, b]).await.expect("delete_many");

    assert!(cas.get(a).await.is_err());
    assert!(cas.get(b).await.is_err());
}

#[tokio::test]
async fn filesystem_put_defaults_to_full_hot_path() {
    let (_dir, cas) = open_temp_filesystem_cas().await;

    let hash =
        cas.put(Bytes::from_static(b"hot-path-full-object")).await.expect("put object on hot path");

    assert!(cas.object_path_for_hash(hash).is_file());
    assert!(!cas.diff_path_for_hash(hash).exists());
}

/// Protects readonly enforcement for persisted full-object payload files.
#[tokio::test]
async fn filesystem_put_marks_object_file_readonly() {
    let (_dir, cas) = open_temp_filesystem_cas().await;

    let hash = cas.put(Bytes::from_static(b"readonly-object")).await.expect("put object");
    let object_path = cas.object_path_for_hash(hash);
    let metadata = std::fs::metadata(&object_path).expect("object metadata");

    assert!(metadata.permissions().readonly());
}

#[tokio::test]
async fn filesystem_optimize_once_rewrites_unconstrained_objects() {
    let (_dir, cas) = open_temp_filesystem_cas().await;

    let base = cas
        .put(Bytes::from_static(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaBBBBBBBBBBBBBBBB"))
        .await
        .expect("put base");
    let target = cas
        .put(Bytes::from_static(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaCCCCCCCCCCCCCCCC"))
        .await
        .expect("put target");

    let report = cas
        .optimize_once(OptimizeOptions::default())
        .await
        .expect("optimize unconstrained objects");

    assert!(report.rewritten_objects >= 1, "expected at least one rewrite from optimizer");
    assert_eq!(cas.get(base).await.expect("reconstruct base").len(), 48);
    assert_eq!(cas.get(target).await.expect("reconstruct target").len(), 48);
}

#[tokio::test]
async fn filesystem_get_stream_round_trips_large_full_payload() {
    let (_dir, cas) = open_temp_filesystem_cas().await;

    let mmap_min_bytes = usize::try_from(super::FILESYSTEM_MMAP_MIN_BYTES).unwrap_or(64 * 1024);
    let payload = vec![b'm'; mmap_min_bytes + 4096];
    let hash = cas.put(Bytes::from(payload.clone())).await.expect("put large payload");

    let mut stream = cas.get_stream(hash).await.expect("get_stream large full payload");
    let mut restored = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.expect("stream chunk");
        restored.extend_from_slice(&chunk);
    }

    assert_eq!(restored, payload);
}

#[tokio::test]
async fn filesystem_get_detects_payload_corruption() {
    let (_dir, cas) = open_temp_filesystem_cas().await;

    let original = Bytes::from_static(b"integrity-check-target");
    let hash = cas.put(original).await.expect("put payload");

    let object_path = cas.object_path_for_hash(hash);
    super::util::clear_file_readonly_if_set(&object_path)
        .expect("clear readonly flag before intentional corruption");
    tokio::fs::write(&object_path, b"mutated-bytes").await.expect("mutate object bytes");

    let err = cas.get(hash).await.expect_err("corrupt payload must fail verification");
    assert!(matches!(err, CasError::CorruptObject(_)));
}

#[tokio::test]
async fn filesystem_metrics_expose_cache_hits_and_optimizer_runtime() {
    let (_dir, cas) = open_temp_filesystem_cas().await;

    let base = cas.put(Bytes::from_static(b"aaaaaaaaaaaaaaaaaaaaaaaa")).await.expect("put base");
    let target =
        cas.put(Bytes::from_static(b"aaaaaaaaaaaaaaaabbbbbbbb")).await.expect("put target");

    cas.set_constraint(Constraint { target_hash: target, potential_bases: BTreeSet::from([base]) })
        .await
        .expect("set optimization constraint");

    cas.optimize_once(OptimizeOptions::default()).await.expect("optimize #1");
    cas.optimize_once(OptimizeOptions::default()).await.expect("optimize #2");

    let metrics = cas.metrics().await.expect("read metrics snapshot");
    assert!(metrics.cache_hits > 0);
    assert!(metrics.optimizer_runtime_ms > 0);
    assert!(metrics.delta_compression_ratio.is_finite());
}

#[tokio::test]
async fn filesystem_topology_snapshot_captures_nodes_delta_edges_and_constraints() {
    let (_dir, cas) = open_temp_filesystem_cas().await;

    let base = cas
        .put(Bytes::from_static(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaBBBBBBBBBBBBBBBB"))
        .await
        .expect("put base");
    let target = cas
        .put(Bytes::from_static(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaCCCCCCCCCCCCCCCC"))
        .await
        .expect("put target");

    cas.set_constraint(Constraint { target_hash: target, potential_bases: BTreeSet::from([base]) })
        .await
        .expect("set explicit constraint");
    cas.optimize_once(OptimizeOptions::default()).await.expect("optimize");

    let snapshot = cas.topology_snapshot(false).await.expect("topology snapshot");
    assert!(snapshot.nodes.iter().all(|node| node.hash != empty_content_hash()));

    let target_node =
        snapshot.nodes.iter().find(|node| node.hash == target).expect("target node must exist");
    match target_node.encoding {
        crate::storage::CasTopologyEncoding::Delta { base_hash } => assert_eq!(base_hash, base),
        crate::storage::CasTopologyEncoding::Full => {
            panic!("expected optimized target to be encoded as delta")
        }
    }

    let row = snapshot
        .constraints
        .iter()
        .find(|row| row.target_hash == target)
        .expect("constraint row for target");
    assert_eq!(row.bases, vec![base]);
}

#[tokio::test]
async fn filesystem_visualize_mermaid_emits_graph_nodes_and_edges() {
    let (_dir, cas) = open_temp_filesystem_cas().await;

    let base = cas
        .put(Bytes::from_static(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaBBBBBBBBBBBBBBBB"))
        .await
        .expect("put base");
    let target = cas
        .put(Bytes::from_static(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaCCCCCCCCCCCCCCCC"))
        .await
        .expect("put target");

    cas.set_constraint(Constraint { target_hash: target, potential_bases: BTreeSet::from([base]) })
        .await
        .expect("set explicit constraint");
    cas.optimize_once(OptimizeOptions::default()).await.expect("optimize");

    let mermaid = cas.visualize_mermaid(false).await.expect("render mermaid");
    assert!(mermaid.contains("flowchart TD"));
    assert!(mermaid.contains(&format!("n{}", target.to_hex())));
    assert!(mermaid.contains(&format!("n{}", base.to_hex())));
    assert!(mermaid.contains("-->|base|"));
    assert!(mermaid.contains("-.->|allowed|"));
}

#[tokio::test]
async fn filesystem_visualize_mermaid_neighborhood_limits_distance() {
    let (_dir, cas) = open_temp_filesystem_cas().await;

    let base = cas
        .put(Bytes::from_static(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaBBBBBBBBBBBBBBBB"))
        .await
        .expect("put base");
    let mid = cas
        .put(Bytes::from_static(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaCCCCCCCCCCCCCCCC"))
        .await
        .expect("put mid");
    let tip = cas
        .put(Bytes::from_static(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaDDDDDDDDDDDDDDDD"))
        .await
        .expect("put tip");

    cas.set_constraint(Constraint { target_hash: mid, potential_bases: BTreeSet::from([base]) })
        .await
        .expect("set constraint mid");
    cas.set_constraint(Constraint { target_hash: tip, potential_bases: BTreeSet::from([mid]) })
        .await
        .expect("set constraint tip");

    cas.optimize_once(OptimizeOptions::default()).await.expect("optimize neighborhood topology");

    let limited =
        cas.visualize_mermaid_neighborhood(base, 1, false).await.expect("render neighborhood");

    assert!(limited.contains(&format!("n{}", base.to_hex())));
    assert!(limited.contains(&format!("n{}", mid.to_hex())));
    assert!(
        !limited.contains(&format!("n{}", tip.to_hex())),
        "depth-1 neighborhood should exclude distance-2 nodes"
    );
}
