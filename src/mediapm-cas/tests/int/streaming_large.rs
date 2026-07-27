//! Large-object and streaming-path integration tests.
//!
//! These tests exercise:
//!
//! - Streaming `put`/`get_to_writer` paths at 1 MiB and 65 MiB.
//! - `get()` above [`WAL_INLINE_LIMIT`] (regression: `get()` delegates
//!   to `get_to_writer()` internally and resolves objects of any size).
//!
//! Test data sizes:
//!
//! - 1 MiB for streaming-path correctness (below [`WAL_INLINE_LIMIT`]).
//! - 2 MiB for `get()` above inline limit (small, no feature gate).
//! - 65 MiB for `get_to_writer` and `get()` large-payload correctness
//!   (requires `large-tests` feature).

use mediapm_cas::api::CasApi;
use mediapm_cas::hash::Hash;

use bytes::Bytes;
#[cfg(feature = "large-tests")]
use tempfile::tempdir;

/// Size of a 1 MiB payload for streaming correctness tests.
const SIZE_1MIB: u64 = 1024 * 1024;
/// Size of a 65 MiB payload for get_to_writer streaming tests.
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

/// `InMemoryCas` `get()` succeeds for objects > [`WAL_INLINE_LIMIT`]
/// (regression: `get()` no longer returns `TooLarge`).
#[tokio::test]
async fn in_memory_get_succeeds_above_wal_inline_limit() {
    let cas = new_in_memory_cas_for_large_tests();
    let data = vec![0xABu8; 2 * 1024 * 1024]; // 2 MiB
    let expected_hash = Hash::from_content(&data);
    let hash = cas.put(Bytes::from(data.clone())).await.unwrap();
    assert_eq!(hash, expected_hash);
    let retrieved = cas.get(hash).await.unwrap();
    assert_eq!(retrieved.to_vec(), data);
}

// ---------------------------------------------------------------------------
// InMemoryCas: large-object behaviour (FileSystemCas covered below)
// ---------------------------------------------------------------------------

/// `InMemoryCas` `get_to_writer()` succeeds for objects exceeding
/// [`WAL_INLINE_LIMIT`] (streaming read path).
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

/// `FileSystemCas` `get_to_writer()` succeeds for objects exceeding
/// [`WAL_INLINE_LIMIT`].
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

/// `FileSystemCas` `get()` succeeds for objects > [`WAL_INLINE_LIMIT`].
#[cfg(feature = "large-tests")]
#[tokio::test]
async fn filesystem_get_succeeds_above_wal_inline_limit() {
    let dir = tempdir().unwrap();
    let cas = mediapm_cas::FileSystemCas::open(dir.path()).await.unwrap();
    let data = vec![0xFEu8; SIZE_65MIB as usize];
    let expected_hash = Hash::from_content(&data);
    let hash = cas.put(Bytes::from(data.clone())).await.unwrap();
    assert_eq!(hash, expected_hash);
    let retrieved = cas.get(hash).await.unwrap();
    assert_eq!(retrieved.to_vec(), data);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Create an `InMemoryCas` with large-enough limits for multi-MiB payloads.
fn new_in_memory_cas_for_large_tests() -> mediapm_cas::InMemoryCas {
    // The default InMemoryCas is unlimited and works fine for large data.
    mediapm_cas::new_in_memory_cas()
}
