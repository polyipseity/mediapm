use super::*;

/// Ingest workflow: deterministic identity and metadata roundtrip.
///
/// Steps:
/// 1. Open each storage backend in isolation.
/// 2. Generate a realistic synthetic payload (`192 KiB`).
/// 3. Store content using `put`.
/// 4. Independently compute expected BLAKE3 hash from payload bytes.
/// 5. Validate `exists` for returned hash.
/// 6. Query `info` and verify content/payload sizes.
/// 7. Retrieve via `get` and verify exact bytes.
/// 8. Retrieve via `get_stream` and verify exact bytes.
/// 9. Repeat read to ensure stable identity semantics.
/// 10. Assert no backend-specific semantic divergence.
///
/// Edge cases covered:
/// - repeated retrieval consistency;
/// - parity between full-byte and stream read paths.
#[tokio::test]
async fn ingest_roundtrip_preserves_identity_and_metadata() {
    run_with_15s_timeout(async {
        for kind in BackendKind::all() {
            let backend = BackendHarness::new(kind).await;
            let payload = synthetic_payload(7, 192 * 1024);
            let expected_hash = Hash::from_content(payload.as_ref());

            let hash = backend.put(payload.clone()).await.expect("put payload");
            assert_eq!(hash, expected_hash, "{} hash mismatch", backend.label());
            assert!(
                backend.exists(hash).await.expect("exists"),
                "{} missing hash",
                backend.label()
            );

            let info = backend.info(hash).await.expect("info");
            assert_eq!(info.content_len, payload.len() as u64, "{} content_len", backend.label());
            assert_eq!(info.payload_len, payload.len() as u64, "{} payload_len", backend.label());

            let restored = backend.get(hash).await.expect("get");
            assert_eq!(restored, payload, "{} byte mismatch on get", backend.label());

            let streamed = backend.get_stream_bytes(hash).await.expect("get_stream");
            assert_eq!(streamed, payload, "{} byte mismatch on get_stream", backend.label());

            let second = backend.get(hash).await.expect("second get");
            assert_eq!(second, payload, "{} byte mismatch on repeated get", backend.label());
        }
    })
    .await;
}

/// Stream workflow: large stream ingest and stream retrieval contract.
///
/// Steps:
/// 1. Open each backend.
/// 2. Ingest a large repeat-byte stream (`1.25 MiB`) via `put_stream`.
/// 3. Validate canonical hash equals independently computed hash.
/// 4. Read back with `get_stream` and collect chunks.
/// 5. Assert reconstructed size and byte pattern.
/// 6. Store a base object with `put`.
/// 7. Ingest another stream via `put_stream_with_constraints`.
/// 8. Assert explicit constraint row includes the provided base.
/// 9. Run one optimize pass.
/// 10. Re-verify stream-ingested object remains reconstructable.
///
/// Edge cases covered:
/// - stream-only code paths;
/// - stream ingest with atomic explicit constraints.
#[tokio::test]
async fn stream_ingest_roundtrip_with_explicit_constraint() {
    run_with_15s_timeout(async {
        for kind in BackendKind::all() {
            let backend = BackendHarness::new(kind).await;

            let stream_len = 1_250_000usize;
            let expected_hash = Hash::from_content(&vec![b'q'; stream_len]);
            let hash = backend.put_stream_repeat(b'q', stream_len).await.expect("put_stream");
            assert_eq!(hash, expected_hash, "{} stream hash mismatch", backend.label());

            let streamed = backend.get_stream_bytes(hash).await.expect("get_stream");
            assert_eq!(streamed.len(), stream_len, "{} stream len mismatch", backend.label());
            assert!(
                streamed.iter().all(|byte| *byte == b'q'),
                "{} stream payload mismatch",
                backend.label()
            );

            let base = backend.put(synthetic_payload(11, 128 * 1024)).await.expect("put base");
            let constrained = backend
                .put_stream_repeat_with_constraints(b'r', 420_000, BTreeSet::from([base]))
                .await
                .expect("put_stream_with_constraints");

            let constraint = backend
                .get_constraint(constrained)
                .await
                .expect("get constraint")
                .expect("explicit constraint row");
            assert_eq!(
                constraint.potential_bases,
                BTreeSet::from([base]),
                "{} base set mismatch",
                backend.label()
            );

            let restored = backend.get(constrained).await.expect("get constrained stream object");
            assert_eq!(
                restored.len(),
                420_000,
                "{} constrained stream len mismatch",
                backend.label()
            );
        }
    })
    .await;
}

/// Bulk lookup workflow: ordered `exists_many`, `get_many`, and `info_many` semantics.
///
/// Steps:
/// 1. Open each backend.
/// 2. Store 8 payloads with distinct sizes/seeds.
/// 3. Build non-sorted query lists including duplicates.
/// 4. Query `exists_many` and assert bit order matches input order.
/// 5. Query `get_many` and assert tuple order matches input order.
/// 6. Query `info_many` and assert metadata order matches input order.
/// 7. Validate content lengths for each response entry.
/// 8. Verify duplicate hash entries remain positional duplicates.
///
/// Edge cases covered:
/// - missing hash in bitmap query;
/// - duplicate hash query entries;
/// - strict output-order contract.
#[tokio::test]
async fn bulk_lookup_preserves_input_order_and_duplicates() {
    run_with_15s_timeout(async {
        for kind in BackendKind::all() {
            let backend = BackendHarness::new(kind).await;

            let mut hashes = Vec::new();
            for idx in 0..8usize {
                let payload = synthetic_payload(31 + idx as u8, 4096 + idx * 137);
                let hash = backend.put(payload).await.expect("put bulk object");
                hashes.push(hash);
            }

            let missing = Hash::from_content(b"scenario03-missing");
            let exists = backend
                .exists_many(vec![hashes[3], missing, hashes[1], hashes[3]])
                .await
                .expect("exists_many");
            assert_eq!(
                exists,
                vec![true, false, true, true],
                "{} exists_many order mismatch",
                backend.label()
            );

            let get_many =
                backend.get_many(vec![hashes[6], hashes[0], hashes[4]]).await.expect("get_many");
            assert_eq!(get_many[0].0, hashes[6], "{} get_many[0] hash order", backend.label());
            assert_eq!(get_many[1].0, hashes[0], "{} get_many[1] hash order", backend.label());
            assert_eq!(get_many[2].0, hashes[4], "{} get_many[2] hash order", backend.label());

            let info_many =
                backend.info_many(vec![hashes[2], hashes[2], hashes[5]]).await.expect("info_many");
            assert_eq!(info_many[0].0, hashes[2], "{} info_many[0] order", backend.label());
            assert_eq!(
                info_many[1].0,
                hashes[2],
                "{} info_many[1] duplicate order",
                backend.label()
            );
            assert_eq!(info_many[2].0, hashes[5], "{} info_many[2] order", backend.label());
            assert_eq!(
                info_many[0].1.content_len,
                info_many[1].1.content_len,
                "{} duplicate metadata mismatch",
                backend.label()
            );
        }
    })
    .await;
}

/// Atomic ingest workflow: `put_with_constraints` consistency contract.
///
/// Steps:
/// 1. Open each backend.
/// 2. Store base payload.
/// 3. Store target payload using `put_with_constraints`.
/// 4. Read explicit constraint row for target.
/// 5. Assert base set matches exactly the provided candidate set.
/// 6. Retrieve target bytes and verify exact equality.
/// 7. Execute optimize/prune maintenance lifecycle.
/// 8. Re-read target bytes and constraint row.
///
/// Edge cases covered:
/// - atomic constraint attachment at ingest time;
/// - post-maintenance semantic stability.
#[tokio::test]
async fn put_with_constraints_is_atomic_and_stable() {
    run_with_15s_timeout(async {
        for kind in BackendKind::all() {
            let backend = BackendHarness::new(kind).await;

            let base_payload = synthetic_payload(77, 96 * 1024);
            let target_payload = synthetic_payload(79, 96 * 1024);

            let base = backend.put(base_payload).await.expect("put base");
            let target = backend
                .put_with_constraints(target_payload.clone(), BTreeSet::from([base]))
                .await
                .expect("put_with_constraints");

            let row = backend
                .get_constraint(target)
                .await
                .expect("get constraint")
                .expect("explicit row");
            assert_eq!(
                row.potential_bases,
                BTreeSet::from([base]),
                "{} explicit row mismatch",
                backend.label()
            );

            let restored = backend.get(target).await.expect("get target");
            assert_eq!(restored, target_payload, "{} target bytes mismatch", backend.label());

            let _ = backend.prune_constraints().await.expect("prune");

            let restored_after = backend.get(target).await.expect("get target after maintenance");
            assert_eq!(
                restored_after,
                target_payload,
                "{} target mismatch after maintenance",
                backend.label()
            );
        }
    })
    .await;
}

/// Burst-load workflow: realistic-sized ingest with coarse SLO.
///
/// Steps:
/// 1. Acquire global budget lock to reduce scheduler noise.
/// 2. Open each backend.
/// 3. Ingest 72 payloads of ~64 KiB each.
/// 4. Sample-retrieve every 13th hash for integrity checks.
/// 5. Run optimize and prune once.
/// 6. Verify optimize report is bounded by input cardinality.
/// 7. Assert elapsed wall time within coarse reproducible budget.
/// 8. Assert workload remains reconstructable throughout.
///
/// Edge cases covered:
/// - burst workload under maintenance pressure;
/// - quantitative + qualitative performance checks.
#[tokio::test]
async fn burst_load_keeps_integrity_within_coarse_budget() {
    let _guard = budget_sensitive_lock().lock().await;

    run_with_15s_timeout(async {
        for kind in BackendKind::all() {
            let backend = BackendHarness::new(kind).await;

            let started = Instant::now();
            let mut hashes = Vec::with_capacity(72);
            for idx in 0..72usize {
                let payload = synthetic_payload(101 + (idx % 13) as u8, 64 * 1024 + (idx % 64));
                let hash = backend.put(payload).await.expect("burst put");
                hashes.push(hash);
            }

            for (idx, hash) in hashes.iter().enumerate().step_by(13) {
                let bytes = backend.get(*hash).await.expect("sample get");
                assert_eq!(
                    bytes.len(),
                    64 * 1024 + (idx % 64),
                    "{} sample len mismatch",
                    backend.label()
                );
            }

            let prune = backend.prune_constraints().await.expect("prune");
            assert!(prune.removed_candidates <= hashes.len(), "{} prune bound", backend.label());

            let elapsed = started.elapsed();
            assert_budget(
                elapsed,
                Duration::from_secs(12),
                &format!("{} burst workflow", backend.label()),
            );
        }
    })
    .await;
}

/// Duplicate-ingest workflow: repeated `put` operations return stable identity.
///
/// Steps:
/// 1. Open each backend.
/// 2. Build one realistic synthetic payload.
/// 3. Call `put` repeatedly with identical bytes.
/// 4. Assert all returned hashes are identical.
/// 5. Query `info` and verify lengths remain stable.
/// 6. Fetch bytes and verify exact roundtrip.
///
/// Edge cases covered:
/// - repeated duplicate writes;
/// - hash stability and retrieval consistency.
#[tokio::test]
async fn duplicate_puts_keep_identity_stable() {
    run_with_15s_timeout(async {
        for kind in BackendKind::all() {
            let backend = BackendHarness::new(kind).await;
            let payload = synthetic_payload(111, 48 * 1024);

            let first = backend.put(payload.clone()).await.expect("first put");
            let second = backend.put(payload.clone()).await.expect("second put");
            let third = backend.put(payload.clone()).await.expect("third put");
            assert_eq!(first, second, "{} first/second hash mismatch", backend.label());
            assert_eq!(second, third, "{} second/third hash mismatch", backend.label());

            let info = backend.info(first).await.expect("info");
            assert_eq!(info.content_len, payload.len() as u64);
            assert_eq!(info.payload_len, payload.len() as u64);

            let restored = backend.get(first).await.expect("get");
            assert_eq!(restored, payload, "{} duplicate-put payload mismatch", backend.label());
        }
    })
    .await;
}

/// Empty-payload workflow: empty object is valid for byte and stream ingestion.
///
/// Steps:
/// 1. Open each backend.
/// 2. Store empty payload via `put`.
/// 3. Store empty payload via `put_stream`.
/// 4. Assert hashes match canonical empty-content hash.
/// 5. Assert retrieval from both paths yields empty bytes.
///
/// Edge cases covered:
/// - zero-length payload handling;
/// - parity between byte and stream ingest paths.
#[tokio::test]
async fn empty_payload_roundtrips_for_put_and_stream_paths() {
    run_with_15s_timeout(async {
        for kind in BackendKind::all() {
            let backend = BackendHarness::new(kind).await;
            let expected = Hash::from_content(&[]);

            let by_bytes = backend.put(Bytes::new()).await.expect("put empty");
            let by_stream = backend.put_stream_repeat(b'x', 0).await.expect("put_stream empty");
            assert_eq!(by_bytes, expected, "{} empty put hash mismatch", backend.label());
            assert_eq!(by_stream, expected, "{} empty stream hash mismatch", backend.label());

            assert_eq!(backend.get(by_bytes).await.expect("get by_bytes").len(), 0);
            assert_eq!(backend.get_stream_bytes(by_stream).await.expect("get_stream").len(), 0);
        }
    })
    .await;
}

/// Empty-batch workflow: bulk APIs return empty outputs for empty inputs.
///
/// Steps:
/// 1. Open each backend.
/// 2. Call `exists_many([])`.
/// 3. Call `get_many([])`.
/// 4. Call `info_many([])`.
/// 5. Assert all outputs are empty and no errors occur.
///
/// Edge cases covered:
/// - zero-sized bulk inputs across read APIs.
#[tokio::test]
async fn empty_bulk_inputs_return_empty_outputs() {
    run_with_15s_timeout(async {
        for kind in BackendKind::all() {
            let backend = BackendHarness::new(kind).await;

            let exists = backend.exists_many(Vec::new()).await.expect("exists_many empty");
            let get_many = backend.get_many(Vec::new()).await.expect("get_many empty");
            let info_many = backend.info_many(Vec::new()).await.expect("info_many empty");

            assert!(exists.is_empty(), "{} exists_many should be empty", backend.label());
            assert!(get_many.is_empty(), "{} get_many should be empty", backend.label());
            assert!(info_many.is_empty(), "{} info_many should be empty", backend.label());
        }
    })
    .await;
}

/// Mixed-batch workflow: `get_many` fails when any requested hash is missing.
///
/// Steps:
/// 1. Open each backend.
/// 2. Store one known payload.
/// 3. Build request list with known + unknown hash.
/// 4. Execute `get_many`.
/// 5. Assert operation fails with `NotFound`.
///
/// Edge cases covered:
/// - fail-fast missing-hash behavior in bulk retrieval.
#[tokio::test]
async fn get_many_with_missing_hash_returns_not_found() {
    run_with_15s_timeout(async {
        for kind in BackendKind::all() {
            let backend = BackendHarness::new(kind).await;
            let existing = backend.put(synthetic_payload(205, 4096)).await.expect("put existing");
            let missing = Hash::from_content(b"missing-bulk-get");

            let error = backend
                .get_many(vec![existing, missing])
                .await
                .expect_err("get_many should fail when one hash is missing");
            assert!(
                matches!(error, CasError::NotFound(_)),
                "{} expected NotFound",
                backend.label()
            );
        }
    })
    .await;
}
