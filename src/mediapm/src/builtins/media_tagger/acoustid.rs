//! AcoustID fingerprint lookup helpers.

use std::path::Path;

use anyhow::{Context, bail};
use chromaprint::{Algorithm, fingerprint_audio};
use serde::{Deserialize, Serialize};
use tokio::process::Command;

use crate::http_client::shared_http_client;

use super::util::{normalize_optional_text, resolve_ffmpeg_executable};
use super::{ACOUSTID_API_KEY_ENV, FINGERPRINT_CHANNELS, FINGERPRINT_SAMPLE_RATE};

/// Decoded fingerprint payload plus AcoustID request duration hint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct FingerprintPayload {
    /// Encoded Chromaprint fingerprint string used by AcoustID lookup.
    pub(super) fingerprint: String,
    /// Rounded audio duration in whole seconds.
    pub(super) duration_seconds: u32,
}

/// Resolves AcoustID API key from CLI override or environment.
pub(super) fn resolve_acoustid_api_key(override_value: Option<&str>) -> Option<String> {
    if let Some(override_value) = override_value {
        if let Some(value) = normalize_optional_text(Some(override_value)) {
            return Some(value);
        }

        // An explicit blank override intentionally disables environment
        // fallback so callers can force the "missing-key" flow.
        return None;
    }

    if let Some(value) = std::env::var_os(ACOUSTID_API_KEY_ENV)
        .and_then(|value| value.into_string().ok())
        .and_then(|value| normalize_optional_text(Some(&value)))
    {
        return Some(value);
    }

    None
}

/// Enforces that AcoustID autodetection has a non-empty API key.
///
/// The lookup path is entered only when callers omit `recording_mbid`.
/// At that point, missing credentials represent a configuration error:
/// runtime would otherwise silently disable autodetection and surprise users.
///
/// In `mediapm sync` executions, the internal media-tagger runs as a managed
/// executable tool under conductor. In that mode, operators typically need to
/// include `ACOUSTID_API_KEY` in `runtime.inherited_env_vars` so the tool
/// subprocess can read it from environment.
pub(super) fn require_acoustid_api_key_for_lookup(
    resolved_key: Option<String>,
) -> anyhow::Result<String> {
    let Some(api_key) = resolved_key else {
        bail!(
            "AcoustID lookup requires a non-empty API key; set --acoustid-api-key or {ACOUSTID_API_KEY_ENV} (for mediapm sync workflows, ensure runtime.inherited_env_vars includes {ACOUSTID_API_KEY_ENV})"
        );
    };

    Ok(api_key)
}

/// Runs `ffmpeg` decode + Chromaprint fingerprint generation for one media file.
pub(super) async fn decode_and_fingerprint_audio(
    input_path: &Path,
) -> anyhow::Result<FingerprintPayload> {
    let ffmpeg_executable = resolve_ffmpeg_executable();
    let output = Command::new(&ffmpeg_executable)
        .arg("-v")
        .arg("error")
        .arg("-probesize")
        .arg("32k")
        .arg("-analyzeduration")
        .arg("0")
        .arg("-i")
        .arg(input_path)
        .arg("-vn")
        .arg("-f")
        .arg("s16le")
        .arg("-ac")
        .arg(FINGERPRINT_CHANNELS.to_string())
        .arg("-ar")
        .arg(FINGERPRINT_SAMPLE_RATE.to_string())
        .arg("-")
        .output()
        .await
        .with_context(|| {
            format!("running '{ffmpeg_executable}' to decode '{}'", input_path.display())
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "ffmpeg decode failed for '{}' with status {}: {stderr}",
            input_path.display(),
            output.status
        );
    }

    if output.stdout.is_empty() {
        bail!("ffmpeg decode for '{}' produced no PCM bytes", input_path.display());
    }

    let samples = output
        .stdout
        .chunks_exact(2)
        .map(|chunk| i16::from_le_bytes([chunk[0], chunk[1]]))
        .collect::<Vec<_>>();

    if samples.is_empty() {
        bail!("decoded PCM sample stream is empty");
    }

    let fingerprint = fingerprint_audio(
        &samples,
        FINGERPRINT_SAMPLE_RATE,
        FINGERPRINT_CHANNELS,
        Algorithm::default(),
    )
    .context("computing chromaprint fingerprint")?;

    let encoded_fingerprint = fingerprint.encoded().to_string();
    let sample_count = u128::try_from(samples.len()).unwrap_or(u128::MAX);
    let samples_per_second = u128::from(FINGERPRINT_SAMPLE_RATE) * u128::from(FINGERPRINT_CHANNELS);
    let rounded_seconds = ((sample_count + (samples_per_second / 2)) / samples_per_second).max(1);
    let duration_seconds = u32::try_from(rounded_seconds).unwrap_or(u32::MAX);

    Ok(FingerprintPayload { fingerprint: encoded_fingerprint, duration_seconds })
}

/// AcoustID API response payload.
#[derive(Debug, Deserialize)]
pub(super) struct AcoustIdLookupResponse {
    /// API status string (`ok` on success).
    status: String,
    /// Candidate matches returned by AcoustID.
    #[serde(default)]
    results: Vec<AcoustIdResult>,
}

/// Wire-format query parameters for AcoustID lookup.
#[derive(Debug, Serialize)]
pub(super) struct AcoustIdLookupQuery<'a> {
    /// API key issued by AcoustID.
    client: &'a str,
    /// Requested lookup metadata projections.
    meta: &'a str,
    /// Rounded clip duration in seconds.
    duration: u32,
    /// Encoded Chromaprint fingerprint text.
    fingerprint: &'a str,
    /// Output response format.
    format: &'a str,
}

/// One AcoustID candidate match.
#[derive(Debug, Deserialize)]
pub(super) struct AcoustIdResult {
    /// Match confidence score in `[0, 1]`.
    score: Option<f64>,
    /// Candidate recordings for this fingerprint match.
    #[serde(default)]
    recordings: Vec<AcoustIdRecording>,
}

/// Recording payload returned in AcoustID lookup metadata.
#[derive(Debug, Deserialize)]
pub(super) struct AcoustIdRecording {
    /// MusicBrainz recording identifier.
    id: String,
    /// Optional release candidates linked to this recording.
    #[serde(default)]
    releases: Vec<AcoustIdRelease>,
}

/// Release payload returned in AcoustID lookup metadata.
#[derive(Debug, Deserialize)]
pub(super) struct AcoustIdRelease {
    /// MusicBrainz release identifier.
    id: String,
}

/// Selected AcoustID match values used for downstream fetches.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct AcoustIdMatch {
    /// Selected recording MBID, when available.
    pub(super) recording_mbid: Option<String>,
    /// Selected release MBID, when available.
    pub(super) release_mbid: Option<String>,
}

/// Calls AcoustID lookup API and returns highest-scoring MBID pair.
pub(super) async fn lookup_acoustid_match(
    endpoint: &str,
    api_key: &str,
    fingerprint: &str,
    duration_seconds: u32,
) -> anyhow::Result<AcoustIdMatch> {
    let http_client = shared_http_client()
        .map_err(|error| anyhow::anyhow!("initializing shared HTTP client failed: {error}"))?;

    let payload = http_client
        .get(endpoint)
        .query(&AcoustIdLookupQuery {
            client: api_key,
            meta: "recordings releases releasegroups tracks compress usermeta sources",
            duration: duration_seconds,
            fingerprint,
            format: "json",
        })
        .send()
        .await
        .context("sending AcoustID lookup request")?
        .error_for_status()
        .context("AcoustID lookup returned non-success status")?
        .text()
        .await
        .context("reading AcoustID lookup response body")?;

    let response = serde_json::from_str::<AcoustIdLookupResponse>(&payload)
        .context("decoding AcoustID lookup response JSON")?;

    if !response.status.eq_ignore_ascii_case("ok") {
        bail!("AcoustID lookup returned status '{}'", response.status);
    }

    let mut ordered_results = response.results;
    ordered_results.sort_by(|left, right| {
        right
            .score
            .unwrap_or_default()
            .partial_cmp(&left.score.unwrap_or_default())
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let Some(best) = ordered_results.into_iter().next() else {
        return Ok(AcoustIdMatch::default());
    };

    let Some(recording) = best.recordings.into_iter().find(|recording| !recording.id.is_empty())
    else {
        return Ok(AcoustIdMatch::default());
    };

    let release_mbid: Option<String> = recording
        .releases
        .into_iter()
        .map(|release| release.id)
        .find(|value| !value.trim().is_empty());

    Ok(AcoustIdMatch { recording_mbid: Some(recording.id), release_mbid })
}
