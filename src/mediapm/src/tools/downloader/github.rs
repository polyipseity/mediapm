//! GitHub release API helpers used by download-plan resolution.

use serde_json::Value;
use url::Url;

use crate::error::MediaPmError;

use super::http::build_http_client;
use super::models::GITHUB_API_BASE;

/// Returns one concise release description extracted from GitHub metadata.
pub(super) fn github_release_description(release: &Value) -> Option<String> {
    release
        .get("name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .or_else(|| {
            release
                .get("body")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|value| value.lines().next().unwrap_or(value).trim().to_string())
        })
}

/// Resolves one immutable commit hash for a release when metadata allows it.
///
/// Resolution precedence:
/// 1. `target_commitish` if it already looks like a hash,
/// 2. `commits/<tag_name>` API lookup,
/// 3. `commits/<target_commitish>` API lookup.
pub(super) async fn github_release_resolved_commit_hash(
    repo: &str,
    release: &Value,
) -> Option<String> {
    if let Some(hash) = release
        .get("target_commitish")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| looks_like_commit_hash(value))
    {
        return Some(hash.to_ascii_lowercase());
    }

    let mut refs = Vec::new();
    if let Some(tag) = release.get("tag_name").and_then(Value::as_str).map(str::trim)
        && !tag.is_empty()
    {
        refs.push(tag.to_string());
    }
    if let Some(target) = release.get("target_commitish").and_then(Value::as_str).map(str::trim)
        && !target.is_empty()
        && refs.iter().all(|candidate| candidate != target)
    {
        refs.push(target.to_string());
    }

    for git_ref in refs {
        if let Ok(hash) = github_commit_hash_for_ref(repo, &git_ref).await {
            return Some(hash);
        }
    }

    None
}

/// Selects one ZIP release asset URL using marker-score ranking.
pub(super) fn github_release_zip_asset_url_from_release(
    release: &Value,
    markers: &[&str],
) -> Result<String, MediaPmError> {
    github_release_asset_url_by_markers_from_release(release, markers, true)
}

/// Selects one release asset URL using marker-score ranking.
pub(super) fn github_release_asset_url_by_markers_from_release(
    release: &Value,
    markers: &[&str],
    require_zip: bool,
) -> Result<String, MediaPmError> {
    let assets = release_assets(release)?;
    let marker_lowers =
        markers.iter().map(|marker| marker.to_ascii_lowercase()).collect::<Vec<_>>();

    let mut best: Option<(usize, String)> = None;
    for (name, url) in &assets {
        let lower = name.to_ascii_lowercase();
        if lower.contains("source code") {
            continue;
        }
        if require_zip
            && !std::path::Path::new(&lower)
                .extension()
                .and_then(std::ffi::OsStr::to_str)
                .is_some_and(|extension| extension.eq_ignore_ascii_case("zip"))
        {
            continue;
        }

        let score = marker_lowers.iter().filter(|marker| lower.contains(marker.as_str())).count();

        if score == 0 {
            continue;
        }

        match &best {
            Some((best_score, _)) if *best_score >= score => {}
            _ => best = Some((score, url.clone())),
        }
    }

    if let Some((_, url)) = best {
        return Ok(url);
    }

    if require_zip
        && let Some((_, url)) = assets.iter().find(|(name, _)| {
            std::path::Path::new(name)
                .extension()
                .and_then(std::ffi::OsStr::to_str)
                .is_some_and(|extension| extension.eq_ignore_ascii_case("zip"))
        })
    {
        return Ok(url.clone());
    }

    Err(MediaPmError::Workflow(format!(
        "could not find GitHub release asset matching markers: {}",
        markers.join(", ")
    )))
}

/// Fetches latest GitHub release JSON payload for one repository.
pub(super) async fn github_latest_release_json(repo: &str) -> Result<Value, MediaPmError> {
    let url = github_api_url_with_segments(repo, &["releases", "latest"])?;
    let client = build_http_client()?;
    let response =
        client.get(&url).header("Accept", "application/vnd.github+json").send().await.map_err(
            |source| {
                MediaPmError::Workflow(format!(
                    "querying latest GitHub release metadata for '{repo}' failed: {source}"
                ))
            },
        )?;

    if !response.status().is_success() {
        return Err(MediaPmError::Workflow(format!(
            "latest GitHub release metadata request failed for '{repo}' with HTTP {}",
            response.status()
        )));
    }

    let body = response.text().await.map_err(|source| {
        MediaPmError::Workflow(format!(
            "reading latest GitHub release metadata body for '{repo}' failed: {source}"
        ))
    })?;

    serde_json::from_str::<Value>(&body).map_err(|source| {
        MediaPmError::Workflow(format!(
            "decoding latest GitHub release metadata for '{repo}' failed: {source}"
        ))
    })
}

/// Fetches one GitHub release JSON payload by tag.
pub(super) async fn github_release_by_tag_json(
    repo: &str,
    tag: &str,
) -> Result<Value, MediaPmError> {
    let url = github_api_url_with_segments(repo, &["releases", "tags", tag])?;
    let client = build_http_client()?;
    let response =
        client.get(&url).header("Accept", "application/vnd.github+json").send().await.map_err(
            |source| {
                MediaPmError::Workflow(format!(
                    "querying GitHub release metadata for '{repo}' tag '{tag}' failed: {source}"
                ))
            },
        )?;

    if !response.status().is_success() {
        return Err(MediaPmError::Workflow(format!(
            "GitHub release metadata request failed for '{repo}' tag '{tag}' with HTTP {}",
            response.status()
        )));
    }

    let body = response.text().await.map_err(|source| {
        MediaPmError::Workflow(format!(
            "reading GitHub release metadata body for '{repo}' tag '{tag}' failed: {source}"
        ))
    })?;

    serde_json::from_str::<Value>(&body).map_err(|source| {
        MediaPmError::Workflow(format!(
            "decoding GitHub release metadata for '{repo}' tag '{tag}' failed: {source}"
        ))
    })
}

/// Fetches recent GitHub release list JSON for version/tag fallback matching.
pub(super) async fn github_release_list_json(repo: &str) -> Result<Vec<Value>, MediaPmError> {
    let mut url = Url::parse(&format!("{GITHUB_API_BASE}/{repo}/releases")).map_err(|source| {
        MediaPmError::Workflow(format!(
            "building GitHub release list URL for '{repo}' failed: {source}"
        ))
    })?;
    url.query_pairs_mut().append_pair("per_page", "50");
    let url = url.to_string();
    let client = build_http_client()?;
    let response =
        client.get(&url).header("Accept", "application/vnd.github+json").send().await.map_err(
            |source| {
                MediaPmError::Workflow(format!(
                    "querying GitHub release list metadata for '{repo}' failed: {source}"
                ))
            },
        )?;

    if !response.status().is_success() {
        return Err(MediaPmError::Workflow(format!(
            "GitHub release list request failed for '{repo}' with HTTP {}",
            response.status()
        )));
    }

    let body = response.text().await.map_err(|source| {
        MediaPmError::Workflow(format!(
            "reading GitHub release list body for '{repo}' failed: {source}"
        ))
    })?;

    serde_json::from_str::<Vec<Value>>(&body).map_err(|source| {
        MediaPmError::Workflow(format!(
            "decoding GitHub release list metadata for '{repo}' failed: {source}"
        ))
    })
}

/// Returns true when one text value looks like a commit hash.
fn looks_like_commit_hash(value: &str) -> bool {
    value.len() >= 7 && value.chars().all(|ch| ch.is_ascii_hexdigit())
}

/// Resolves one commit hash by GitHub commit-reference API lookup.
async fn github_commit_hash_for_ref(repo: &str, git_ref: &str) -> Result<String, MediaPmError> {
    let url = github_api_url_with_segments(repo, &["commits", git_ref])?;
    let client = build_http_client()?;

    let response =
        client.get(&url).header("Accept", "application/vnd.github+json").send().await.map_err(
            |source| {
                MediaPmError::Workflow(format!(
                    "resolving commit hash for '{repo}' ref '{git_ref}' failed: {source}"
                ))
            },
        )?;

    if !response.status().is_success() {
        return Err(MediaPmError::Workflow(format!(
            "commit lookup failed for '{repo}' ref '{git_ref}' with HTTP {}",
            response.status()
        )));
    }

    let body = response.text().await.map_err(|source| {
        MediaPmError::Workflow(format!(
            "reading commit lookup payload for '{repo}' ref '{git_ref}' failed: {source}"
        ))
    })?;

    let value = serde_json::from_str::<Value>(&body).map_err(|source| {
        MediaPmError::Workflow(format!(
            "decoding commit lookup payload for '{repo}' ref '{git_ref}' failed: {source}"
        ))
    })?;

    let hash = value
        .get("sha")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|candidate| looks_like_commit_hash(candidate))
        .ok_or_else(|| {
            MediaPmError::Workflow(format!(
                "commit lookup payload for '{repo}' ref '{git_ref}' did not include sha"
            ))
        })?;

    Ok(hash.to_ascii_lowercase())
}

/// Builds one GitHub API URL from path segments with safe segment encoding.
fn github_api_url_with_segments(repo: &str, segments: &[&str]) -> Result<String, MediaPmError> {
    let mut url = Url::parse(&format!("{GITHUB_API_BASE}/{repo}/")).map_err(|source| {
        MediaPmError::Workflow(format!("building GitHub API URL for '{repo}' failed: {source}"))
    })?;

    {
        let mut path_segments = url.path_segments_mut().map_err(|()| {
            MediaPmError::Workflow(format!(
                "building GitHub API URL for '{repo}' failed: path is not a base"
            ))
        })?;
        path_segments.pop_if_empty();
        for segment in segments {
            path_segments.push(segment);
        }
    }

    Ok(url.to_string())
}

/// Extracts `(asset_name, browser_download_url)` tuples from release payload.
fn release_assets(release: &Value) -> Result<Vec<(String, String)>, MediaPmError> {
    let assets = release.get("assets").and_then(Value::as_array).ok_or_else(|| {
        MediaPmError::Workflow("GitHub release payload is missing 'assets' array".to_string())
    })?;

    Ok(assets
        .iter()
        .filter_map(|asset| {
            let name = asset.get("name")?.as_str()?.trim();
            let url = asset.get("browser_download_url")?.as_str()?.trim();
            if name.is_empty() || url.is_empty() {
                None
            } else {
                Some((name.to_string(), url.to_string()))
            }
        })
        .collect())
}
