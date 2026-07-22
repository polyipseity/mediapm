//! Large-object and streaming-path integration tests.
//!
//! These tests exercise the streaming `put`/`get_to_writer` paths and verify
//! behavior at the WAL-inline threshold boundary (1 MiB).  Test data is:
//!
//! - 1 MiB for streaming-path correctness (well below threshold).
//! - 65 MiB for threshold-exceeded paths (above
//!   [`WAL_INLINE_THRESHOLD`](mediapm_cas::defaults::WAL_INLINE_THRESHOLD)).

use mediapm_cas::api::CasApi;
use mediapm_cas::hash::Hash;

#[cfg(feature = "large-tests")]
use bytes::Bytes;
#[cfg(feature = "large-tests")]
use mediapm_cas::CasError;
#[cfg(feature = "large-tests")]
use tempfile::tempdir;

/// Size of a 1 MiB payload for streaming correctness tests.
const SIZE_1MIB: u64 = 1024 * 1024;
/// Size of a 65 MiB payload (> old [`WAL_INLINE_THRESHOLD`]) for `TooLarge` tests.
#[cfg(feature = "large-tests")]
const SIZE_65MIB: u64 = 65 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Streaming put/get_to_writer round-trips (any store backend)
// ---------------------------------------------------------------------------

/// `put_stream` with a 1 MiB payload propagates the correct length through
/// metadata so that `stat` returns the real size.
#[tokio::test]
async fn put_stream_content_len_propagated() {
    let cas = new_in_memory_cas_for_large_tests();
    #[allow(clippy::cast_possible_truncation)]
    let data = vec![0xABu8; SIZE_1MIB as usize];
    let expected_hash = Hash::from_content(&data);

    let hash = cas.put_stream(&data[..]).await.unwrap();
    assert_eq!(hash, expected_hash, "hash must match content");

    let meta = cas.stat(hash).await.unwrap();
    assert_eq!(meta.len, SIZE_1MIB, "stat().len must equal the number of bytes streamed");
}

/// `put_stream` + `get_to_writer` round-trip with a 1 MiB payload: data
/// written via the streaming path must be recoverable.
#[tokio::test]
async fn put_stream_get_to_writer_roundtrip() {
    let cas = new_in_memory_cas_for_large_tests();
    #[allow(clippy::cast_possible_truncation)]
    let data = vec![0xCDu8; SIZE_1MIB as usize];
    let expected_hash = Hash::from_content(&data);

    let hash = cas.put_stream(&data[..]).await.unwrap();
    assert_eq!(hash, expected_hash);

    #[allow(clippy::cast_possible_truncation)]
    let mut buf = Vec::with_capacity(SIZE_1MIB as usize);
    cas.get_to_writer(hash, &mut buf).await.unwrap();
    assert_eq!(buf.len() as u64, SIZE_1MIB, "output length must match");
    assert_eq!(buf.as_slice(), &data[..], "output content must match");
}

// ---------------------------------------------------------------------------
// InMemoryCas: large-object behaviour (FileSystemCas covered below)
// ---------------------------------------------------------------------------

/// `InMemoryCas` `get()` returns [`CasError::TooLarge`] when the object
/// exceeds [`WAL_INLINE_THRESHOLD`].
#[cfg(feature = "large-tests")]
#[tokio::test]
async fn in_memory_large_object_get_returns_too_large() {
    let cas = new_in_memory_cas_for_large_tests();
    #[allow(clippy::cast_possible_truncation)]
    let data = Bytes::from(vec![0u8; SIZE_65MIB as usize]);

    let hash = cas.put(data).await.unwrap();
    let meta = cas.stat(hash).await.unwrap();
    assert_eq!(meta.len, SIZE_65MIB);

    let result = cas.get(hash).await;
    assert!(matches!(&result, Err(CasError::TooLarge { .. })), "expected TooLarge, got {result:?}");
    if let Err(CasError::TooLarge { hash: h, size, limit }) = &result {
        assert_eq!(*h, hash);
        assert_eq!(*size, SIZE_65MIB);
        assert!(*size > *limit);
    }
}

/// `InMemoryCas` `get_to_writer()` succeeds for objects that exceed
/// [`WAL_INLINE_THRESHOLD`] (streaming read path).
#[cfg(feature = "large-tests")]
#[tokio::test]
async fn in_memory_large_object_get_to_writer_works() {
    let cas = new_in_memory_cas_for_large_tests();
    #[allow(clippy::cast_possible_truncation)]
    let data = vec![0xEFu8; SIZE_65MIB as usize];
    let expected_hash = Hash::from_content(&data);
    let chunk = Bytes::from(data.clone());

    let hash = cas.put(chunk).await.unwrap();
    assert_eq!(hash, expected_hash);

    #[allow(clippy::cast_possible_truncation)]
    let mut buf = Vec::with_capacity(SIZE_65MIB as usize);
    cas.get_to_writer(hash, &mut buf).await.unwrap();
    assert_eq!(buf.len() as u64, SIZE_65MIB);
    assert_eq!(buf.as_slice(), &data[..]);
}

// ---------------------------------------------------------------------------
// FileSystemCas: large-object behaviour (disk-backed store)
// ---------------------------------------------------------------------------

/// `FileSystemCas` `get()` returns [`CasError::TooLarge`] for objects
/// exceeding [`WAL_INLINE_THRESHOLD`].
#[cfg(feature = "large-tests")]
#[tokio::test]
async fn filesystem_large_object_get_returns_too_large() {
    let dir = tempdir().unwrap();
    let cas = mediapm_cas::FileSystemCas::open(dir.path()).await.unwrap();
    #[allow(clippy::cast_possible_truncation)]
    let data = Bytes::from(vec![0xFFu8; SIZE_65MIB as usize]);

    let hash = cas.put(data).await.unwrap();
    let result = cas.get(hash).await;
    assert!(matches!(&result, Err(CasError::TooLarge { .. })), "expected TooLarge, got {result:?}");
}

/// `FileSystemCas` `get_to_writer()` succeeds for objects exceeding
/// [`WAL_INLINE_THRESHOLD`].
#[cfg(feature = "large-tests")]
#[tokio::test]
async fn filesystem_large_object_get_to_writer_works() {
    let dir = tempdir().unwrap();
    let cas = mediapm_cas::FileSystemCas::open(dir.path()).await.unwrap();
    #[allow(clippy::cast_possible_truncation)]
    let data = vec![0xFEu8; SIZE_65MIB as usize];
    let expected_hash = Hash::from_content(&data);
    let chunk = Bytes::from(data.clone());

    let hash = cas.put(chunk).await.unwrap();
    assert_eq!(hash, expected_hash);

    #[allow(clippy::cast_possible_truncation)]
    let mut buf = Vec::with_capacity(SIZE_65MIB as usize);
    cas.get_to_writer(hash, &mut buf).await.unwrap();
    assert_eq!(buf.len() as u64, SIZE_65MIB);
    assert_eq!(buf.as_slice(), &data[..]);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Create an `InMemoryCas` with large-enough limits for multi-MiB payloads.
fn new_in_memory_cas_for_large_tests() -> mediapm_cas::InMemoryCas {
    // The default InMemoryCas is unlimited and works fine for large data.
    mediapm_cas::new_in_memory_cas()
}
