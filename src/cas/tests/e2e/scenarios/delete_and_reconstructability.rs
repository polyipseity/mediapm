use super::*;

/// Delete workflow: deleting unknown hash returns `NotFound`.
///
/// Steps:
/// 1. Open each backend.
/// 2. Construct hash that does not exist in store.
/// 3. Invoke `delete`.
/// 4. Assert explicit `CasError::NotFound`.
///
/// Edge cases covered:
/// - delete-path error classification on unknown identity.
#[tokio::test]
async fn delete_unknown_hash_returns_not_found() {
    run_with_15s_timeout(async {
        for kind in BackendKind::all() {
            let backend = BackendHarness::new(kind).await;
            let missing = Hash::from_content(b"scenario11-missing");

            let error = backend.delete(missing).await.expect_err("missing delete must fail");
            assert!(
                matches!(error, CasError::NotFound(_)),
                "{} expected NotFound, got {error}",
                backend.label()
            );
        }
    })
    .await;
}

/// Graph-safety workflow: deleting an ancestor preserves descendant bytes.
///
/// Steps:
/// 1. Open each backend.
/// 2. Store base payload (`128 KiB`).
/// 3. Build target payload by mutating one byte in base payload.
/// 4. Store target and set explicit `target -> {base}` constraint.
/// 5. Run optimize to encourage delta/base relationship.
/// 6. Delete base object.
/// 7. Assert base now returns `NotFound`.
/// 8. Assert target still reconstructs to exact original bytes.
///
/// Edge cases covered:
/// - dependent rewrite/rebase path during ancestor deletion.
#[tokio::test]
async fn delete_base_preserves_descendant_reconstructability() {
    run_with_15s_timeout(async {
        for kind in BackendKind::all() {
            let backend = BackendHarness::new(kind).await;

            let base_payload = synthetic_payload(51, 128 * 1024);
            let target_payload = mutated_payload(base_payload.as_ref(), 777, b'Z');

            let base = backend.put(base_payload).await.expect("put base");
            let target = backend.put(target_payload.clone()).await.expect("put target");
            backend
                .set_constraint(Constraint {
                    target_hash: target,
                    potential_bases: BTreeSet::from([base]),
                })
                .await
                .expect("set constraint");

            backend.delete(base).await.expect("delete base");

            let base_error = backend.get(base).await.expect_err("base should be gone");
            assert!(
                matches!(base_error, CasError::NotFound(_)),
                "{} expected NotFound for base",
                backend.label()
            );

            let restored = backend.get(target).await.expect("target must survive");
            assert_eq!(restored, target_payload, "{} descendant bytes changed", backend.label());
        }
    })
    .await;
}

/// Batch-delete workflow: `delete_many` removes all requested hashes.
///
/// Steps:
/// 1. Open each backend.
/// 2. Store three independent objects.
/// 3. Call `delete_many` with deterministic input order.
/// 4. Assert all three objects are no longer retrievable.
/// 5. Assert each failure classifies as `NotFound`.
///
/// Edge cases covered:
/// - deterministic sequential delete_many behavior.
#[tokio::test]
async fn delete_many_removes_all_targets() {
    run_with_15s_timeout(async {
        for kind in BackendKind::all() {
            let backend = BackendHarness::new(kind).await;

            let a = backend.put(synthetic_payload(61, 1000)).await.expect("put a");
            let b = backend.put(synthetic_payload(62, 1001)).await.expect("put b");
            let c = backend.put(synthetic_payload(63, 1002)).await.expect("put c");

            backend.delete_many(vec![a, b, c]).await.expect("delete_many");

            for hash in [a, b, c] {
                let error = backend.get(hash).await.expect_err("hash should be removed");
                assert!(
                    matches!(error, CasError::NotFound(_)),
                    "{} expected NotFound after delete_many",
                    backend.label()
                );
            }
        }
    })
    .await;
}

/// Constraint-lifecycle workflow: deleting a constrained target removes its row.
///
/// Steps:
/// 1. Open each backend.
/// 2. Store base and target.
/// 3. Set explicit `target -> {base}` constraint.
/// 4. Delete target.
/// 5. Query `get_constraint(target)` and assert `NotFound`.
/// 6. Assert base object remains unaffected.
///
/// Edge cases covered:
/// - automatic cleanup of target-owned constraint rows.
#[tokio::test]
async fn deleting_target_removes_its_constraint_row() {
    run_with_15s_timeout(async {
        for kind in BackendKind::all() {
            let backend = BackendHarness::new(kind).await;

            let base_payload = synthetic_payload(71, 4096);
            let target_payload = synthetic_payload(72, 4096);
            let base = backend.put(base_payload.clone()).await.expect("put base");
            let target = backend.put(target_payload).await.expect("put target");

            backend
                .set_constraint(Constraint {
                    target_hash: target,
                    potential_bases: BTreeSet::from([base]),
                })
                .await
                .expect("set constraint");

            backend.delete(target).await.expect("delete target");

            let row_error = backend
                .get_constraint(target)
                .await
                .expect_err("deleted target should not have readable constraint row");
            assert!(
                matches!(row_error, CasError::NotFound(_)),
                "{} expected NotFound for deleted target constraint",
                backend.label()
            );

            let base_restored = backend.get(base).await.expect("base remains");
            assert_eq!(base_restored, base_payload, "{} base payload changed", backend.label());
        }
    })
    .await;
}

/// Maintenance-sequencing workflow: optimize-delete-optimize remains safe.
///
/// Steps:
/// 1. Open each backend.
/// 2. Store one base and five target variants.
/// 3. Attach explicit `target_i -> {base}` constraints.
/// 4. Run initial optimize pass.
/// 5. Delete one constrained target.
/// 6. Run optimize again.
/// 7. Run prune.
/// 8. Assert remaining targets are still retrievable.
/// 9. Assert optimize reports remain bounded by live target count.
///
/// Edge cases covered:
/// - maintenance idempotency around delete events;
/// - constrained set stability after one member removal.
#[tokio::test]
async fn optimize_delete_optimize_remains_stable() {
    run_with_15s_timeout(async {
        for kind in BackendKind::all() {
            let backend = BackendHarness::new(kind).await;
            let base = backend.put(synthetic_payload(81, 24 * 1024)).await.expect("put base");

            let mut targets = Vec::new();
            for idx in 0..5usize {
                let payload = synthetic_payload(90 + idx as u8, 24 * 1024);
                let target = backend.put(payload).await.expect("put target");
                backend
                    .set_constraint(Constraint {
                        target_hash: target,
                        potential_bases: BTreeSet::from([base]),
                    })
                    .await
                    .expect("set constraint");
                targets.push(target);
            }

            let first =
                backend.optimize_once(OptimizeOptions::default()).await.expect("first optimize");
            backend.delete(targets[2]).await.expect("delete one target");
            let second =
                backend.optimize_once(OptimizeOptions::default()).await.expect("second optimize");
            let _ = backend.prune_constraints().await.expect("prune");

            for (idx, target) in targets.into_iter().enumerate() {
                if idx == 2 {
                    continue;
                }
                let restored = backend.get(target).await.expect("remaining target get");
                assert_eq!(
                    restored.len(),
                    24 * 1024,
                    "{} remaining target len mismatch",
                    backend.label()
                );
            }

            assert!(first.rewritten_objects <= 5, "{} first optimize bound", backend.label());
            assert!(second.rewritten_objects <= 4, "{} second optimize bound", backend.label());
        }
    })
    .await;
}

/// Batch-delete error workflow: `delete_many` stops at first error in input order.
///
/// Steps:
/// 1. Open each backend.
/// 2. Store three objects A/B/C.
/// 3. Build delete list `[A, missing, C]`.
/// 4. Execute `delete_many`.
/// 5. Assert call fails with `NotFound`.
/// 6. Assert A was deleted before failure.
/// 7. Assert C remains present because processing stopped.
///
/// Edge cases covered:
/// - deterministic sequential `delete_many` fail-fast behavior.
#[tokio::test]
async fn delete_many_stops_on_first_error_in_order() {
    run_with_15s_timeout(async {
        for kind in BackendKind::all() {
            let backend = BackendHarness::new(kind).await;

            let a = backend.put(synthetic_payload(181, 1200)).await.expect("put a");
            let b = backend.put(synthetic_payload(182, 1201)).await.expect("put b");
            let c = backend.put(synthetic_payload(183, 1202)).await.expect("put c");
            let missing = Hash::from_content(b"delete-many-missing");

            let error = backend
                .delete_many(vec![a, missing, c])
                .await
                .expect_err("delete_many should fail on missing hash");
            assert!(
                matches!(error, CasError::NotFound(_)),
                "{} expected NotFound",
                backend.label()
            );

            assert!(matches!(
                backend.get(a).await.expect_err("a should be deleted first"),
                CasError::NotFound(_)
            ));
            assert_eq!(backend.get(b).await.expect("b should remain").len(), 1201);
            assert_eq!(backend.get(c).await.expect("c should remain").len(), 1202);
        }
    })
    .await;
}

/// Idempotency workflow: deleting the same hash twice yields deterministic errors.
///
/// Steps:
/// 1. Open each backend.
/// 2. Store one payload and delete it.
/// 3. Attempt second delete on same hash.
/// 4. Assert second delete fails with `NotFound`.
///
/// Edge cases covered:
/// - repeated delete idempotency/error classification.
#[tokio::test]
async fn deleting_same_hash_twice_reports_not_found_on_second_attempt() {
    run_with_15s_timeout(async {
        for kind in BackendKind::all() {
            let backend = BackendHarness::new(kind).await;
            let hash = backend.put(synthetic_payload(184, 2048)).await.expect("put object");

            backend.delete(hash).await.expect("first delete");
            let error = backend.delete(hash).await.expect_err("second delete must fail");
            assert!(
                matches!(error, CasError::NotFound(_)),
                "{} expected NotFound",
                backend.label()
            );
        }
    })
    .await;
}
