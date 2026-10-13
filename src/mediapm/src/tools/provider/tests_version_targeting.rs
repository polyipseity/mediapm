//! Hermetic tests for version-targeted tool fetches (Task 1, versioned
//! source chains).
//!
//! These protect the plan's user-visible guarantees: an `Exact { tag }` spec
//! on a GitHub-release tool resolves that tag's release *and* fetches the
//! tag-pinned asset URLs (never `releases/latest`), a release missing a
//! platform's asset names the slot it could not fill, and a pin on `BtbN`'s
//! rolling ffmpeg legs is refused with the retention reason instead of
//! silently fetching the wrong bytes.
//!
//! HTTP is served by a local `wiremock` server through the
//! `MEDIAPM_GITHUB_API_BASE` test seam (read per call, never a `OnceLock`),
//! so no test touches the real network. The seam is process-global, so every
//! test here holds `lock_process_env()` for its mutation scope.

use super::*;
use crate::example_isolation::lock_process_env;
use mediapm_conductor::tools::provider::VersionSpecFields;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Builds an `Exact` version spec pinning one git tag.
fn exact_tag(tag: &str) -> VersionSpec {
    VersionSpec::Exact(VersionSpecFields {
        vcs_hash: None,
        version: None,
        tag: Some(tag.to_string()),
    })
}

/// Mocks the two GitHub API endpoints a lightweight (non-annotated) tag
/// resolution needs: the release-by-tag document with its asset list, and the
/// tag ref whose object carries the commit SHA.
async fn mock_lightweight_tag_release(
    server: &MockServer,
    owner: &str,
    repo: &str,
    tag: &str,
    assets: &[&str],
) {
    let asset_json: Vec<serde_json::Value> =
        assets.iter().map(|a| serde_json::json!({ "name": a })).collect();
    Mock::given(method("GET"))
        .and(path(format!("/repos/{owner}/{repo}/releases/tags/{tag}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "tag_name": tag,
            "assets": asset_json,
        })))
        .expect(1)
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/repos/{owner}/{repo}/git/refs/tags/{tag}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "object": { "type": "commit", "sha": "d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e2" },
        })))
        .expect(1)
        .mount(server)
        .await;
}

/// An `Exact { tag }` on yt-dlp resolves that tag's release through the API
/// and builds `releases/download/{tag}/…` URLs — the pinned bytes, not
/// whatever `latest` currently points at — with the resolved commit hash as
/// the canonical version.
#[tokio::test]
#[expect(
    clippy::await_holding_lock,
    reason = "the env lock must serialize the whole set-seam -> await -> unset-seam resolve; the guard is Send and this is the documented MEDIAPM_* test-seam mutation-scope rule"
)]
async fn exact_tag_fetches_release_assets_and_builds_pinned_urls() {
    let server = MockServer::start().await;
    mock_lightweight_tag_release(
        &server,
        "yt-dlp",
        "yt-dlp",
        "2025.07.15",
        &["yt-dlp.exe", "yt-dlp_macos", "yt-dlp_linux"],
    )
    .await;

    let env_lock = lock_process_env();
    // SAFETY: the env lock the guard holds scopes this process-global seam
    // to this test, per the documented MEDIAPM_* mutation-scope rule.
    unsafe {
        std::env::set_var(GITHUB_API_BASE_ENV, server.uri());
    }

    let result =
        resolve_tool_fetch("yt-dlp", &exact_tag("2025.07.15"), None, RecheckPolicy::UseCached)
            .await;

    unsafe {
        std::env::remove_var(GITHUB_API_BASE_ENV);
    }
    drop(env_lock);

    let (fetch, metadata) = result.unwrap_or_else(|e| panic!("pinned resolve failed: {e}"));
    let urls: Vec<&str> = fetch
        .sources
        .iter()
        .filter_map(|s| match &s.producer {
            SourceProducer::Fetch { urls } => urls.first().map(String::as_str),
            SourceProducer::GenerateLauncher { .. } => None,
        })
        .collect();
    assert_eq!(urls.len(), 3, "expected one URL per OS, got {urls:?}");
    for url in &urls {
        assert!(
            url.contains("/releases/download/2025.07.15/"),
            "pinned URL must address the tag, got {url}"
        );
        assert!(!url.contains("/latest/"), "pinned URL must not use the latest alias, got {url}");
    }
    assert_eq!(
        metadata.canonical_version, "d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e2",
        "canonical version is the tag's commit hash"
    );
    assert_eq!(metadata.resolved_tag.as_deref(), Some("2025.07.15"));
    assert_eq!(
        metadata.resolved_vcs_hash.as_deref(),
        Some("d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e2")
    );
    assert!(!metadata.metadata_cached, "an API-served resolution is not a cache hit");
}

/// A pinned release lacking one platform's asset fails naming the tool, the
/// tag, the missing pattern, and the OS slot — so a pin typo or an upstream
/// asset rename is diagnosable without opening the API by hand.
#[tokio::test]
#[expect(
    clippy::await_holding_lock,
    reason = "the env lock must serialize the whole set-seam -> await -> unset-seam resolve; the guard is Send and this is the documented MEDIAPM_* test-seam mutation-scope rule"
)]
async fn exact_tag_missing_asset_names_the_unfilled_slot() {
    let server = MockServer::start().await;
    // The linux asset is deliberately absent.
    mock_lightweight_tag_release(&server, "yt-dlp", "yt-dlp", "2099.01.01", &["yt-dlp.exe"]).await;

    let env_lock = lock_process_env();
    // SAFETY: the env lock the guard holds scopes this process-global seam
    // to this test, per the documented MEDIAPM_* mutation-scope rule.
    unsafe {
        std::env::set_var(GITHUB_API_BASE_ENV, server.uri());
    }

    let result =
        resolve_tool_fetch("yt-dlp", &exact_tag("2099.01.01"), None, RecheckPolicy::UseCached)
            .await;

    unsafe {
        std::env::remove_var(GITHUB_API_BASE_ENV);
    }
    drop(env_lock);

    let Err(error) = result else {
        panic!("missing asset must fail the pinned resolve");
    };
    let message = error.to_string();
    assert!(message.contains("yt-dlp"), "error names the tool: {message}");
    assert!(message.contains("2099.01.01"), "error names the tag: {message}");
    assert!(message.contains("yt-dlp_macos"), "error names the missing pattern: {message}");
    assert!(message.contains("macos"), "error names the unfilled OS slot: {message}");
}

/// Pinning ffmpeg is refused outright because every ffmpeg leg resolves from
/// `BtbN`'s single rolling release, whose assets are replaced on every build —
/// a pin resolved against `latest` would fetch bytes other than the ones it
/// names. The refusal happens at resolve, before any HTTP.
#[tokio::test]
async fn btbn_pinned_ffmpeg_is_refused_with_the_retention_reason() {
    let result = resolve_tool_fetch(
        "ffmpeg",
        &exact_tag("autobuild-2025-07-15-12-00"),
        None,
        RecheckPolicy::UseCached,
    )
    .await;

    let Err(error) = result else {
        panic!("a pinned ffmpeg resolve must fail");
    };
    let message = error.to_string();
    assert!(
        message.contains("BtbN does not retain historical builds"),
        "refusal states the retention reason: {message}"
    );
}

/// Deno's per-OS slots carry exactly one candidate each — never a
/// cross-arch fallback list. The macOS and Linux slots follow the host
/// architecture; the Windows slot stays `x86_64` because upstream publishes
/// no aarch64 Windows build there, so its single candidate *is* the
/// selection.
#[test]
fn deno_sources_select_one_host_arch_candidate_per_os() {
    let host = host_arch_label();
    let fetch = deno::sources();
    for source in &fetch.sources {
        let SourceProducer::Fetch { urls } = &source.producer else {
            continue;
        };
        assert_eq!(urls.len(), 1, "exactly one candidate for os {}", source.os);
        let expected_arch =
            if source.os == "windows" { "x86_64".to_string() } else { host.clone() };
        assert!(
            urls[0].contains(&format!("-{expected_arch}-")),
            "candidate must be the {expected_arch} asset, got {}",
            urls[0]
        );
        assert_eq!(source.arch, expected_arch, "source arch label matches the asset");
    }
}

/// sd's per-OS slots are single-candidate selections, same guarantee as
/// deno — kept as a separate assertion because each tool's asset-name
/// pattern is authored independently. Upstream publishes aarch64 for macOS
/// only, so the macOS slot follows the host arch and the Linux/Windows slots
/// pin the sole published `x86_64` build; one candidate means the selection
/// is explicit either way.
#[test]
fn sd_sources_select_one_host_arch_candidate_per_os() {
    let host = host_arch_label();
    let fetch = sd::sources();
    for source in &fetch.sources {
        let SourceProducer::Fetch { urls } = &source.producer else {
            continue;
        };
        assert_eq!(urls.len(), 1, "exactly one candidate for os {}", source.os);
        let expected_arch = if source.os == "macos" { host.clone() } else { "x86_64".to_string() };
        assert!(
            urls[0].contains(&format!("-{expected_arch}-")),
            "candidate must be the {expected_arch} asset, got {}",
            urls[0]
        );
        assert_eq!(source.arch, expected_arch, "source arch label matches the asset");
    }
}
