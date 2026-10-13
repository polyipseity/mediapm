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
//! The pure matcher tests at the bottom pin the per-tool asset selection
//! itself (exact-name for yt-dlp/deno, version-embedded suffix for
//! rsgain/sd) against synthetic asset lists, with no HTTP at all.
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

/// Pinning ffmpeg is refused outright, in every `Exact` flavor, because every
/// ffmpeg leg resolves from `BtbN`'s single rolling release, whose assets are
/// replaced on every build — a pin resolved against `latest` would fetch bytes
/// other than the ones it names. The refusal happens at resolve, before any
/// HTTP, so all three flavors assert the same retention reason hermetically.
#[tokio::test]
async fn btbn_pinned_ffmpeg_is_refused_with_the_retention_reason() {
    let pinned_flavors = [
        ("tag", exact_tag("autobuild-2025-07-15-12-00")),
        (
            "version",
            VersionSpec::Exact(VersionSpecFields {
                vcs_hash: None,
                version: Some("7.1.1".to_string()),
                tag: None,
            }),
        ),
        (
            "vcs_hash",
            VersionSpec::Exact(VersionSpecFields {
                vcs_hash: Some("0123456789abcdef0123456789abcdef01234567".to_string()),
                version: None,
                tag: None,
            }),
        ),
    ];
    for (flavor, target) in &pinned_flavors {
        let result = resolve_tool_fetch("ffmpeg", target, None, RecheckPolicy::UseCached).await;

        let Err(error) = result else {
            panic!("a pinned ffmpeg resolve must fail for the {flavor} flavor");
        };
        let message = error.to_string();
        assert!(
            message.contains("BtbN does not retain historical builds"),
            "{flavor} pin refusal states the retention reason: {message}"
        );
    }
}

/// A pinned tag whose ref carries no dereferenceable SHA (here: an object
/// of type `tree`) is refused with the dereference reason — the folded
/// review fix. A `Latest` resolve keeps its legacy fallback (the tag string
/// stands in as the identity), but a pin that stored the tag as its
/// "commit hash" could never match a real one, so the pinned path errors
/// instead. The release document itself is served, isolating the refusal to
/// the ref dereference.
#[tokio::test]
#[expect(
    clippy::await_holding_lock,
    reason = "the env lock must serialize the whole set-seam -> await -> unset-seam resolve; the guard is Send and this is the documented MEDIAPM_* test-seam mutation-scope rule"
)]
async fn pinned_tag_with_a_non_dereferenceable_ref_is_refused() {
    let server = MockServer::start().await;
    let tag = "2025.07.15";
    Mock::given(method("GET"))
        .and(path(format!("/repos/yt-dlp/yt-dlp/releases/tags/{tag}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "tag_name": tag,
            "assets": [
                { "name": "yt-dlp.exe" },
                { "name": "yt-dlp_macos" },
                { "name": "yt-dlp_linux" },
            ],
        })))
        .mount(&server)
        .await;
    // The ref answers, but its object is a `tree`, not a commit or annotated
    // tag — the dereference falls back to the tag string, which the pinned
    // path must refuse.
    Mock::given(method("GET"))
        .and(path(format!("/repos/yt-dlp/yt-dlp/git/refs/tags/{tag}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "object": { "type": "tree", "sha": "1111111111111111111111111111111111111111" },
        })))
        .mount(&server)
        .await;

    let env_lock = lock_process_env();
    // SAFETY: the env lock the guard holds scopes this process-global seam
    // to this test, per the documented MEDIAPM_* mutation-scope rule.
    unsafe {
        std::env::set_var(GITHUB_API_BASE_ENV, server.uri());
    }

    let result =
        resolve_tool_fetch("yt-dlp", &exact_tag(tag), None, RecheckPolicy::UseCached).await;

    // SAFETY: same env-lock scope as the set above.
    unsafe {
        std::env::remove_var(GITHUB_API_BASE_ENV);
    }
    drop(env_lock);

    let error = result.expect_err("a non-dereferenceable pinned ref must be refused");
    let message = error.to_string();
    assert!(
        message.contains(&format!("ref for tag {tag} did not dereference to a commit")),
        "the refusal names the dereference failure: {message}"
    );
}

/// A stale cache entry from the pre-dereferencing era — whose stored "hash"
/// is the tag string itself (`{tag}\n{tag}`) — must NOT be trusted as a
/// commit. The `hash != tag` guard falls through to a re-resolve, which
/// hits the ref endpoint and, finding a non-dereferenceable ref, refuses.
/// The mock's `expect(1)` on both endpoints is the proof: had the cache been
/// served, neither would have been contacted.
#[tokio::test]
#[expect(
    clippy::await_holding_lock,
    reason = "the env lock must serialize the whole set-seam -> await -> unset-seam resolve; the guard is Send and this is the documented MEDIAPM_* test-seam mutation-scope rule"
)]
async fn stale_tag_as_hash_cache_entry_triggers_reresolve_and_refusal() {
    let server = MockServer::start().await;
    let tag = "2025.07.15";
    // The release document is served, isolating the outcome to the ref
    // dereference (which falls back to the tag and is refused).
    Mock::given(method("GET"))
        .and(path(format!("/repos/yt-dlp/yt-dlp/releases/tags/{tag}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "tag_name": tag,
            "assets": [
                { "name": "yt-dlp.exe" },
                { "name": "yt-dlp_macos" },
                { "name": "yt-dlp_linux" },
            ],
        })))
        .expect(1)
        .mount(&server)
        .await;
    // A `tree` ref is neither a commit nor an annotated tag, so the
    // dereference falls back to the tag string and the pinned path refuses.
    Mock::given(method("GET"))
        .and(path(format!("/repos/yt-dlp/yt-dlp/git/refs/tags/{tag}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "object": { "type": "tree", "sha": "2222222222222222222222222222222222222222" },
        })))
        .expect(1)
        .mount(&server)
        .await;

    let temp_dir = mediapm_utils::temp::cache_dir().unwrap();
    let cache = ToolDownloadCache::open(temp_dir.path(), "test_metadata.json", 3600).await.unwrap();

    let env_lock = lock_process_env();
    // SAFETY: the env lock the guard holds scopes this process-global seam
    // to this test, per the documented MEDIAPM_* mutation-scope rule.
    unsafe {
        std::env::set_var(GITHUB_API_BASE_ENV, server.uri());
    }

    // Seed the stale pre-dereferencing entry under the seam-resolved key:
    // the "hash" column is the tag string itself.
    let api_url = format!("{}/repos/yt-dlp/yt-dlp/releases/tags/{tag}", server.uri());
    cache.store_bytes("default", api_url.as_str(), format!("{tag}\n{tag}").as_bytes()).await;

    let result = resolve_tool_fetch(
        "yt-dlp",
        &exact_tag(tag),
        Some((&cache, "default")),
        RecheckPolicy::UseCached,
    )
    .await;

    // SAFETY: same env-lock scope as the set above.
    unsafe {
        std::env::remove_var(GITHUB_API_BASE_ENV);
    }
    drop(env_lock);

    let error = result.expect_err("a stale tag-as-hash entry must not be trusted as a commit");
    let message = error.to_string();
    assert!(
        message.contains(&format!("ref for tag {tag} did not dereference to a commit")),
        "the re-resolved refusal surfaces: {message}"
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

/// The exact-name matchers (yt-dlp and deno) select a platform slot's asset
/// by exact name — a prefix-sharing decoy in the release must never be
/// selected — and the pinned URL embeds the matched name under
/// `releases/download/{tag}/`, so the fetch addresses the asset the matcher
/// actually chose.
#[test]
fn exact_name_matchers_pin_the_matched_asset_url() {
    let tag = "2025.07.15";

    // yt-dlp: exact names, plus a prefix-sharing decoy that must not match.
    let assets: Vec<String> = ["yt-dlp.exe", "yt-dlp_macos", "yt-dlp_linux", "yt-dlp_macos_arm64"]
        .iter()
        .map(|name| (*name).to_string())
        .collect();
    let fetch = yt_dlp::sources_for_release(tag, &assets).expect("all three slots match");
    for (os, expected) in
        [("windows", "yt-dlp.exe"), ("macos", "yt-dlp_macos"), ("linux", "yt-dlp_linux")]
    {
        let source = fetch.sources.iter().find(|s| s.os == os).expect("os slot present");
        let SourceProducer::Fetch { urls } = &source.producer else {
            panic!("{os} slot must be a fetch source");
        };
        assert_eq!(urls.len(), 1, "{os} slot carries one candidate");
        assert!(
            urls[0].ends_with(&format!("/releases/download/{tag}/{expected}")),
            "{os} slot must pin the exact {expected} asset, got {}",
            urls[0]
        );
    }

    // deno: same exact-name shape; the macOS/Linux names follow the host
    // arch, and the decoy is the *other* arch so it is wrong on every host.
    let host = host_arch_label();
    let other_arch = if host == "x86_64" { "aarch64" } else { "x86_64" };
    let assets: Vec<String> = vec![
        "deno-x86_64-pc-windows-msvc.zip".to_string(),
        format!("deno-{host}-apple-darwin.zip"),
        format!("deno-{host}-unknown-linux-gnu.zip"),
        format!("deno-{other_arch}-apple-darwin.zip"),
    ];
    let fetch = deno::sources_for_release(tag, &assets).expect("all three slots match");
    for (os, expected) in [
        ("windows", "deno-x86_64-pc-windows-msvc.zip".to_string()),
        ("macos", format!("deno-{host}-apple-darwin.zip")),
        ("linux", format!("deno-{host}-unknown-linux-gnu.zip")),
    ] {
        let source = fetch.sources.iter().find(|s| s.os == os).expect("os slot present");
        let SourceProducer::Fetch { urls } = &source.producer else {
            panic!("{os} slot must be a fetch source");
        };
        assert!(
            urls[0].ends_with(&format!("/releases/download/{tag}/{expected}")),
            "{os} slot must pin the exact {expected} asset, got {}",
            urls[0]
        );
    }
}

/// The rsgain matcher selects each slot's asset by `rsgain-*{suffix}` (the
/// version-embedded name), ignoring prefix- and suffix-adjacent decoys, and
/// the pinned URL embeds the matched name. A release whose assets carry only
/// wrong suffixes refuses naming the tool, the tag, the missing pattern, and
/// the OS slot.
#[test]
fn rsgain_suffix_matcher_pins_the_versioned_asset_and_names_a_miss() {
    let tag = "v3.7";
    let assets: Vec<String> = [
        "rsgain-3.7-win64.zip",
        "rsgain-3.7-macOS-x86_64.zip",
        "rsgain-3.7-Linux.tar.xz",
        // Decoys: a checksum sibling and a wrong-suffix name must not match.
        "rsgain-3.7-win64.zip.sha256",
        "rsgain-3.7-win32.zip",
    ]
    .iter()
    .map(|name| (*name).to_string())
    .collect();
    let fetch = rsgain::sources_for_release(tag, &assets).expect("all three slots match");
    for (os, expected) in [
        ("windows", "rsgain-3.7-win64.zip"),
        ("macos", "rsgain-3.7-macOS-x86_64.zip"),
        ("linux", "rsgain-3.7-Linux.tar.xz"),
    ] {
        let source = fetch.sources.iter().find(|s| s.os == os).expect("os slot present");
        let SourceProducer::Fetch { urls } = &source.producer else {
            panic!("{os} slot must be a fetch source");
        };
        assert!(
            urls[0].ends_with(&format!("/releases/download/{tag}/{expected}")),
            "{os} slot must pin the matched {expected} asset, got {}",
            urls[0]
        );
    }

    // Wrong-suffix-only list: the windows slot finds nothing and refuses.
    let miss: Vec<String> =
        ["rsgain-3.7-win32.zip", "rsgain-3.7-macOS-arm64.zip", "rsgain-3.7-Debian.tar.xz"]
            .iter()
            .map(|name| (*name).to_string())
            .collect();
    let Err(error) = rsgain::sources_for_release(tag, &miss) else {
        panic!("a release with no -win64.zip asset must refuse the windows slot");
    };
    let message = error.to_string();
    assert!(message.contains("rsgain"), "error names the tool: {message}");
    assert!(message.contains(tag), "error names the tag: {message}");
    assert!(message.contains("rsgain-*-win64.zip"), "error names the missing pattern: {message}");
    assert!(message.contains("windows"), "error names the OS slot: {message}");
}

/// The sd matcher selects each slot's asset by `sd-*{suffix}` (the
/// version-embedded name), ignoring a wrong-suffix decoy, and the pinned URL
/// embeds the matched name. A release whose assets carry only wrong suffixes
/// refuses naming the tool, the tag, the missing pattern, and the OS slot.
#[test]
fn sd_suffix_matcher_pins_the_versioned_asset_and_names_a_miss() {
    let tag = "v1.1.0";
    let host = host_arch_label();
    let assets: Vec<String> = vec![
        "sd-v1.1.0-x86_64-pc-windows-msvc.zip".to_string(),
        format!("sd-v1.1.0-{host}-apple-darwin.tar.gz"),
        "sd-v1.1.0-x86_64-unknown-linux-gnu.tar.gz".to_string(),
        // Decoy: same tag, wrong suffix — must not match any slot.
        "sd-v1.1.0-win64.zip".to_string(),
    ];
    let fetch = sd::sources_for_release(tag, &assets).expect("all three slots match");
    for (os, expected) in [
        ("windows", "sd-v1.1.0-x86_64-pc-windows-msvc.zip".to_string()),
        ("macos", format!("sd-v1.1.0-{host}-apple-darwin.tar.gz")),
        ("linux", "sd-v1.1.0-x86_64-unknown-linux-gnu.tar.gz".to_string()),
    ] {
        let source = fetch.sources.iter().find(|s| s.os == os).expect("os slot present");
        let SourceProducer::Fetch { urls } = &source.producer else {
            panic!("{os} slot must be a fetch source");
        };
        assert!(
            urls[0].ends_with(&format!("/releases/download/{tag}/{expected}")),
            "{os} slot must pin the matched {expected} asset, got {}",
            urls[0]
        );
    }

    // Wrong-suffix-only list: the windows slot finds nothing and refuses.
    let miss: Vec<String> =
        ["sd-v1.1.0-win64.zip", "sd-v1.1.0-macOS.tar.gz", "sd-v1.1.0-linux.tar.gz"]
            .iter()
            .map(|name| (*name).to_string())
            .collect();
    let Err(error) = sd::sources_for_release(tag, &miss) else {
        panic!("a release with no windows-msvc asset must refuse the windows slot");
    };
    let message = error.to_string();
    assert!(message.contains("sd"), "error names the tool: {message}");
    assert!(message.contains(tag), "error names the tag: {message}");
    assert!(
        message.contains("sd-*-x86_64-pc-windows-msvc.zip"),
        "error names the missing pattern: {message}"
    );
    assert!(message.contains("windows"), "error names the OS slot: {message}");
}
