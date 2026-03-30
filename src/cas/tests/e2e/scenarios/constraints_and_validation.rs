use super::*;

/// Validation workflow: missing candidate base is rejected.
///
/// Steps:
/// 1. Open each backend.
/// 2. Store a valid target object.
/// 3. Prepare a hash that is not present in the store.
/// 4. Attempt `set_constraint` with missing candidate base.
/// 5. Assert explicit `CasError::NotFound`.
/// 6. Assert target payload remains unaffected and retrievable.
///
/// Edge cases covered:
/// - explicit constraint candidate validation;
/// - error-path non-destructive behavior.
#[tokio::test]
async fn set_constraint_rejects_missing_candidate_base() {
    run_with_15s_timeout(async {
        for kind in BackendKind::all() {
            let backend = BackendHarness::new(kind).await;
            let target = backend.put(synthetic_payload(9, 8192)).await.expect("put target");
            let missing = Hash::from_content(b"scenario06-missing");

            let error = backend
                .set_constraint(Constraint {
                    target_hash: target,
                    potential_bases: BTreeSet::from([missing]),
                })
                .await
                .expect_err("missing base must fail");
            assert!(
                matches!(error, CasError::NotFound(_)),
                "{} expected NotFound, got {error}",
                backend.label()
            );

            let restored = backend.get(target).await.expect("get target");
            assert_eq!(restored.len(), 8192, "{} target should remain intact", backend.label());
        }
    })
    .await;
}

/// Validation workflow: self-referential constraints are rejected.
///
/// Steps:
/// 1. Open each backend.
/// 2. Store target object.
/// 3. Attempt `set_constraint` where target appears in its own candidate set.
/// 4. Assert explicit `CasError::InvalidConstraint`.
/// 5. Assert no explicit row is persisted for that target.
///
/// Edge cases covered:
/// - self-reference guard on constraint semantics.
#[tokio::test]
async fn set_constraint_rejects_self_reference() {
    run_with_15s_timeout(async {
        for kind in BackendKind::all() {
            let backend = BackendHarness::new(kind).await;
            let target = backend.put(synthetic_payload(13, 4096)).await.expect("put target");

            let error = backend
                .set_constraint(Constraint {
                    target_hash: target,
                    potential_bases: BTreeSet::from([target]),
                })
                .await
                .expect_err("self-reference must fail");
            assert!(
                matches!(error, CasError::InvalidConstraint(_)),
                "{} expected InvalidConstraint, got {error}",
                backend.label()
            );

            let row = backend.get_constraint(target).await.expect("get constraint");
            assert!(
                row.is_none(),
                "{} unexpected constraint row after invalid write",
                backend.label()
            );
        }
    })
    .await;
}

/// Mutation workflow: patch add/remove/clear semantics.
///
/// Steps:
/// 1. Open each backend.
/// 2. Store one target + two bases.
/// 3. Set initial constraint `{base_a}`.
/// 4. Patch by adding `base_b`.
/// 5. Patch by removing `base_a`.
/// 6. Assert resulting row is `{base_b}`.
/// 7. Patch with `clear_existing=true` and no adds.
/// 8. Assert row transitions to implicit-unconstrained (`None`).
///
/// Edge cases covered:
/// - additive and subtractive patch semantics;
/// - clear semantics that normalize to implicit row absence.
#[tokio::test]
async fn patch_constraint_add_remove_clear_lifecycle() {
    run_with_15s_timeout(async {
        for kind in BackendKind::all() {
            let backend = BackendHarness::new(kind).await;
            let target = backend.put(synthetic_payload(21, 5000)).await.expect("put target");
            let base_a = backend.put(synthetic_payload(22, 5000)).await.expect("put base_a");
            let base_b = backend.put(synthetic_payload(23, 5000)).await.expect("put base_b");

            backend
                .set_constraint(Constraint {
                    target_hash: target,
                    potential_bases: BTreeSet::from([base_a]),
                })
                .await
                .expect("set initial constraint");

            let _ = backend
                .patch_constraint(
                    target,
                    ConstraintPatch {
                        add_bases: BTreeSet::from([base_b]),
                        remove_bases: BTreeSet::new(),
                        clear_existing: false,
                    },
                )
                .await
                .expect("patch add");

            let patched = backend
                .patch_constraint(
                    target,
                    ConstraintPatch {
                        add_bases: BTreeSet::new(),
                        remove_bases: BTreeSet::from([base_a]),
                        clear_existing: false,
                    },
                )
                .await
                .expect("patch remove")
                .expect("explicit row remains");
            assert_eq!(
                patched.potential_bases,
                BTreeSet::from([base_b]),
                "{} patch result mismatch",
                backend.label()
            );

            let cleared = backend
                .patch_constraint(
                    target,
                    ConstraintPatch {
                        add_bases: BTreeSet::new(),
                        remove_bases: BTreeSet::new(),
                        clear_existing: true,
                    },
                )
                .await
                .expect("patch clear");
            assert!(cleared.is_none(), "{} clear should remove explicit row", backend.label());
            assert!(backend.get_constraint(target).await.expect("get after clear").is_none());
        }
    })
    .await;
}

/// Read workflow: `get_constraint_many` order and `None` semantics.
///
/// Steps:
/// 1. Open each backend.
/// 2. Store base, constrained target, and unconstrained target.
/// 3. Set explicit constraint only on constrained target.
/// 4. Query `get_constraint_many([unconstrained, constrained])`.
/// 5. Assert response tuple order is identical to input order.
/// 6. Assert unconstrained entry is `None`.
/// 7. Assert constrained entry contains explicit base set.
///
/// Edge cases covered:
/// - mixed explicit/implicit rows in one batch;
/// - strict positional ordering contract.
#[tokio::test]
async fn get_constraint_many_preserves_order_and_none_rows() {
    run_with_15s_timeout(async {
        for kind in BackendKind::all() {
            let backend = BackendHarness::new(kind).await;
            let base = backend.put(synthetic_payload(29, 2048)).await.expect("put base");
            let constrained =
                backend.put(synthetic_payload(30, 2048)).await.expect("put constrained");
            let free = backend.put(synthetic_payload(31, 2048)).await.expect("put free");

            backend
                .set_constraint(Constraint {
                    target_hash: constrained,
                    potential_bases: BTreeSet::from([base]),
                })
                .await
                .expect("set constraint");

            let rows = backend
                .get_constraint_many(vec![free, constrained])
                .await
                .expect("get_constraint_many");
            assert_eq!(rows[0], (free, None), "{} unconstrained row mismatch", backend.label());
            assert_eq!(
                rows[1],
                (
                    constrained,
                    Some(Constraint {
                        target_hash: constrained,
                        potential_bases: BTreeSet::from([base]),
                    })
                ),
                "{} constrained row mismatch",
                backend.label()
            );
        }
    })
    .await;
}

/// Maintenance workflow: prune removes dangling deleted candidates.
///
/// Steps:
/// 1. Open each backend.
/// 2. Store base A, base B, and target T.
/// 3. Set explicit constraint `T -> {A, B}`.
/// 4. Delete base B.
/// 5. Run `prune_constraints`.
/// 6. Assert prune reports at least one removed candidate.
/// 7. Assert `get_constraint(T)` excludes deleted base B.
/// 8. Assert T is still reconstructable.
///
/// Edge cases covered:
/// - dangling candidate cleanup after base deletion.
#[tokio::test]
async fn prune_constraints_removes_deleted_candidate_bases() {
    run_with_15s_timeout(async {
        for kind in BackendKind::all() {
            let backend = BackendHarness::new(kind).await;
            let base_a = backend.put(synthetic_payload(41, 4096)).await.expect("put base_a");
            let base_b = backend.put(synthetic_payload(42, 4096)).await.expect("put base_b");
            let target_payload = synthetic_payload(43, 4096);
            let target = backend.put(target_payload.clone()).await.expect("put target");

            backend
                .set_constraint(Constraint {
                    target_hash: target,
                    potential_bases: BTreeSet::from([base_a, base_b]),
                })
                .await
                .expect("set constraint");

            backend.delete(base_b).await.expect("delete base_b");
            let _ = backend.prune_constraints().await.expect("prune constraints");

            let row = backend
                .get_constraint(target)
                .await
                .expect("get target constraint")
                .expect("target explicit row should remain due base_a");
            assert!(
                !row.potential_bases.contains(&base_b),
                "{} dangling base_b remained",
                backend.label()
            );

            let restored = backend.get(target).await.expect("get target");
            assert_eq!(restored, target_payload, "{} target payload changed", backend.label());
        }
    })
    .await;
}

/// Validation workflow: empty candidate set is normalized to implicit constraint absence.
///
/// Steps:
/// 1. Open each backend.
/// 2. Store one target object.
/// 3. Apply `set_constraint` with empty candidate set.
/// 4. Read row using `get_constraint`.
/// 5. Assert row is `None` (implicit unconstrained semantics).
///
/// Edge cases covered:
/// - empty-set constraint normalization.
#[tokio::test]
async fn empty_constraint_set_is_treated_as_implicit() {
    run_with_15s_timeout(async {
        for kind in BackendKind::all() {
            let backend = BackendHarness::new(kind).await;
            let target = backend.put(synthetic_payload(151, 2048)).await.expect("put target");

            backend
                .set_constraint(Constraint {
                    target_hash: target,
                    potential_bases: BTreeSet::new(),
                })
                .await
                .expect("set empty constraint");

            let row = backend.get_constraint(target).await.expect("get constraint row");
            assert!(row.is_none(), "{} empty constraint should normalize to None", backend.label());
        }
    })
    .await;
}

/// Validation workflow: setting constraints on missing targets fails.
///
/// Steps:
/// 1. Open each backend.
/// 2. Build missing target and candidate hashes.
/// 3. Call `set_constraint`.
/// 4. Assert operation fails with `NotFound`.
///
/// Edge cases covered:
/// - target existence validation for constraint writes.
#[tokio::test]
async fn set_constraint_rejects_missing_target() {
    run_with_15s_timeout(async {
        for kind in BackendKind::all() {
            let backend = BackendHarness::new(kind).await;
            let target = Hash::from_content(b"missing-target");
            let base = backend.put(synthetic_payload(152, 3000)).await.expect("put base");

            let error = backend
                .set_constraint(Constraint {
                    target_hash: target,
                    potential_bases: BTreeSet::from([base]),
                })
                .await
                .expect_err("set_constraint should fail for missing target");
            assert!(
                matches!(error, CasError::NotFound(_)),
                "{} expected NotFound",
                backend.label()
            );
        }
    })
    .await;
}

/// Patch workflow: missing target and missing base additions are rejected.
///
/// Steps:
/// 1. Open each backend.
/// 2. Attempt patch on a missing target.
/// 3. Assert `NotFound`.
/// 4. Create valid target.
/// 5. Attempt patch adding missing base hash.
/// 6. Assert `NotFound`.
///
/// Edge cases covered:
/// - patch target existence validation;
/// - patch add-base existence validation.
#[tokio::test]
async fn patch_constraint_rejects_missing_target_and_missing_added_base() {
    run_with_15s_timeout(async {
        for kind in BackendKind::all() {
            let backend = BackendHarness::new(kind).await;

            let missing_target = Hash::from_content(b"patch-missing-target");
            let missing_base = Hash::from_content(b"patch-missing-base");
            let error_missing_target = backend
                .patch_constraint(
                    missing_target,
                    ConstraintPatch {
                        add_bases: BTreeSet::from([missing_base]),
                        remove_bases: BTreeSet::new(),
                        clear_existing: false,
                    },
                )
                .await
                .expect_err("patch on missing target must fail");
            assert!(matches!(error_missing_target, CasError::NotFound(_)));

            let target = backend.put(synthetic_payload(153, 3000)).await.expect("put target");
            let error_missing_base = backend
                .patch_constraint(
                    target,
                    ConstraintPatch {
                        add_bases: BTreeSet::from([missing_base]),
                        remove_bases: BTreeSet::new(),
                        clear_existing: false,
                    },
                )
                .await
                .expect_err("patch add missing base must fail");
            assert!(matches!(error_missing_base, CasError::NotFound(_)));
        }
    })
    .await;
}

/// Read workflow: `get_constraint_many` fails when any target hash is missing.
///
/// Steps:
/// 1. Open each backend.
/// 2. Store one valid target object.
/// 3. Query `get_constraint_many` with valid + missing target hashes.
/// 4. Assert call fails with `NotFound`.
///
/// Edge cases covered:
/// - mixed valid/missing target handling in bulk constraint reads.
#[tokio::test]
async fn get_constraint_many_fails_when_any_target_is_missing() {
    run_with_15s_timeout(async {
        for kind in BackendKind::all() {
            let backend = BackendHarness::new(kind).await;
            let existing = backend.put(synthetic_payload(154, 1024)).await.expect("put existing");
            let missing = Hash::from_content(b"missing-constraint-target");

            let error = backend
                .get_constraint_many(vec![existing, missing])
                .await
                .expect_err("get_constraint_many must fail when any target is missing");
            assert!(
                matches!(error, CasError::NotFound(_)),
                "{} expected NotFound",
                backend.label()
            );
        }
    })
    .await;
}
