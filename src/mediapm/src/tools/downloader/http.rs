//! Async HTTP helpers used by release metadata lookup and payload transfer.
//!
//! The downloader intentionally avoids one hard global request timeout. Instead,
//! timeout policy is split between:
//! - a strict connect timeout (failing fast when hosts are unreachable), and
//! - a much larger transfer timeout that can be tuned by environment variable.
//!
//! Compared with a short fixed timeout, this keeps slow-but-progressing large
//! downloads viable while still bounding hung transfers.

use std::sync::Arc;

use reqwest::{Client, Response};

use crate::error::MediaPmError;
use crate::http_client::shared_http_client;

use super::DownloadProgressCallback;
use super::DownloadProgressSnapshot;
use super::ToolDownloadCache;
/// Downloads bytes from URL candidates in-order until one succeeds.
pub(super) async fn fetch_bytes_from_candidates(
    urls: &[String],
    progress_callback: Option<DownloadProgressCallback>,
    download_cache: Option<Arc<ToolDownloadCache>>,
    cache_key: String,
) -> Result<(Vec<u8>, String), MediaPmError> {
    if let Some(cache) = &download_cache
        && let Some(bytes) = cache.lookup_bytes(&cache_key).await
    {
        let total_bytes = bytes.len() as u64;
        emit_download_progress(
            progress_callback.as_ref(),
            DownloadProgressSnapshot { downloaded_bytes: 0, total_bytes: Some(total_bytes) },
        );
        emit_download_progress(
            progress_callback.as_ref(),
            DownloadProgressSnapshot {
                downloaded_bytes: total_bytes,
                total_bytes: Some(total_bytes),
            },
        );
        return Ok((bytes, "cache://shared-user".to_string()));
    }

    let client = build_http_client()?;
    let mut errors = Vec::new();

    for url in urls {
        let response = match client.get(url).send().await {
            Ok(response) => response,
            Err(source) => {
                errors.push(format!("{url}: {source}"));
                continue;
            }
        };

        if !response.status().is_success() {
            errors.push(format!("{url}: HTTP {}", response.status()));
            continue;
        }

        let total_bytes = response.content_length().filter(|value| *value > 0);
        emit_download_progress(
            progress_callback.as_ref(),
            candidate_progress_snapshot(0, total_bytes),
        );

        match read_response_bytes(url, response, total_bytes, progress_callback.as_ref()).await {
            Ok(bytes) => {
                if let Some(cache) = &download_cache {
                    cache.store_bytes(&cache_key, &bytes).await;
                }
                return Ok((bytes, url.clone()));
            }
            Err(error) => {
                errors.push(error.to_string());
            }
        }
    }

    Err(MediaPmError::Workflow(format!(
        "all tool download URL candidates failed: {}",
        errors.join("; ")
    )))
}

/// Reads one response body as bytes while emitting incremental progress.
async fn read_response_bytes(
    url: &str,
    mut response: Response,
    total_bytes: Option<u64>,
    progress_callback: Option<&DownloadProgressCallback>,
) -> Result<Vec<u8>, MediaPmError> {
    let mut bytes = Vec::new();
    let mut downloaded_bytes = 0_u64;

    while let Some(chunk) = response.chunk().await.map_err(|source| {
        MediaPmError::Workflow(format!("reading response body failed for '{url}': {source}"))
    })? {
        bytes.extend_from_slice(&chunk);
        downloaded_bytes = downloaded_bytes.saturating_add(chunk.len() as u64);
        emit_download_progress(
            progress_callback,
            candidate_progress_snapshot(downloaded_bytes, total_bytes),
        );
    }

    Ok(bytes)
}

/// Best-effort probe for payload size across URL candidates.
///
/// This helper tries candidate URLs in-order and returns the first discovered
/// `Content-Length` value. Probe failures are intentionally non-fatal because
/// download flow can still proceed with indeterminate totals.
#[must_use]
pub(super) async fn probe_content_length_from_candidates(urls: &[String]) -> Option<u64> {
    let client = build_http_client().ok()?;

    for url in urls {
        if let Ok(response) = client.head(url).send().await
            && response.status().is_success()
            && response.content_length().is_some_and(|value| value > 0)
        {
            return response.content_length();
        }

        if let Ok(response) = client.get(url).send().await
            && response.status().is_success()
            && response.content_length().is_some_and(|value| value > 0)
        {
            return response.content_length();
        }
    }

    None
}

/// Builds one candidate-local progress snapshot.
///
/// Fallback attempts intentionally restart transfer progress from zero instead
/// of summing bytes across failed candidates. This keeps total-size reporting
/// stable and avoids sudden denominator jumps when mirrors are retried.
#[must_use]
fn candidate_progress_snapshot(
    downloaded_bytes: u64,
    total_bytes: Option<u64>,
) -> DownloadProgressSnapshot {
    DownloadProgressSnapshot { downloaded_bytes, total_bytes }
}

/// Emits one transfer-progress snapshot when a callback is configured.
fn emit_download_progress(
    callback: Option<&DownloadProgressCallback>,
    snapshot: DownloadProgressSnapshot,
) {
    if let Some(callback) = callback {
        callback(snapshot);
    }
}

/// Creates one async HTTP client with stall-aware timeout policy.
pub(super) fn build_http_client() -> Result<&'static Client, MediaPmError> {
    shared_http_client()
}

#[cfg(test)]
mod tests {
    use super::candidate_progress_snapshot;

    /// Protects retry UX by ensuring each fallback candidate starts with its
    /// own independent byte counters.
    #[test]
    fn candidate_progress_snapshot_is_candidate_local() {
        let first_attempt = candidate_progress_snapshot(600, Some(1_000));
        assert_eq!(first_attempt.downloaded_bytes, 600);
        assert_eq!(first_attempt.total_bytes, Some(1_000));

        let second_attempt = candidate_progress_snapshot(0, Some(500));
        assert_eq!(second_attempt.downloaded_bytes, 0);
        assert_eq!(second_attempt.total_bytes, Some(500));
    }

    /// Protects snapshot integrity when one failed candidate omits
    /// `Content-Length`; rendered totals must remain indeterminate.
    #[test]
    fn candidate_progress_snapshot_supports_unknown_totals() {
        let snapshot = candidate_progress_snapshot(64, None);
        assert_eq!(snapshot.downloaded_bytes, 64);
        assert_eq!(snapshot.total_bytes, None);
    }

    /// Protects downloader progress from endpoints that report
    /// `Content-Length: 0` by ensuring callers normalize to unknown totals.
    #[test]
    fn candidate_progress_snapshot_can_represent_zero_totals() {
        let snapshot = candidate_progress_snapshot(32, Some(0));
        assert_eq!(snapshot.downloaded_bytes, 32);
        assert_eq!(snapshot.total_bytes, Some(0));
    }
}
