//! `sd` common executable tool preset implementation.

use std::ffi::OsStr;
use std::io::Read;

use crate::error::ConductorError;

use super::CommonExecutablePayload;

/// Canonical logical tool name used in runtime machine configuration.
pub const LOGICAL_TOOL_NAME: &str = "mediapm-conductor.tools.sd";

/// Canonical executable basename used for host-installed `sd` binaries.
const EXECUTABLE_BASENAME: &str = "sd";

/// GitHub API endpoint used to resolve the latest published `sd` release.
const SD_LATEST_RELEASE_API_URL: &str = "https://api.github.com/repos/chmln/sd/releases/latest";

/// User-Agent header value used for GitHub release API and asset requests.
const SD_DOWNLOAD_USER_AGENT: &str = "mediapm-conductor";

/// Cross-platform executable suffix used by downloaded common tools.
#[cfg(windows)]
const EXECUTABLE_SUFFIX: &str = ".exe";

/// Cross-platform executable suffix used by downloaded common tools.
#[cfg(not(windows))]
const EXECUTABLE_SUFFIX: &str = "";

/// Returns the expected executable file name produced by installation.
#[must_use]
pub fn executable_file_name() -> String {
    format!("{EXECUTABLE_BASENAME}{EXECUTABLE_SUFFIX}")
}

/// Supported archive kinds for one selected release asset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReleaseArchiveKind {
    /// ZIP archive payload.
    Zip,
    /// TAR.GZ archive payload.
    TarGz,
}

/// Selected release asset metadata for the current host platform.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ReleaseAssetSelection {
    /// Direct browser-download URL for the selected release asset.
    download_url: String,
    /// Archive format used by the selected payload URL.
    archive_kind: ReleaseArchiveKind,
}

/// Returns ordered release-asset suffix markers for the current host target.
#[must_use]
fn host_release_asset_markers() -> &'static [&'static str] {
    #[cfg(all(target_os = "windows", target_arch = "x86_64"))]
    {
        &["x86_64-pc-windows-msvc.zip", "x86_64-pc-windows-gnu.zip"]
    }

    #[cfg(all(target_os = "windows", target_arch = "aarch64"))]
    {
        &["aarch64-pc-windows-msvc.zip"]
    }

    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    {
        &["x86_64-unknown-linux-gnu.tar.gz", "x86_64-unknown-linux-musl.tar.gz"]
    }

    #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
    {
        &["aarch64-unknown-linux-gnu.tar.gz", "aarch64-unknown-linux-musl.tar.gz"]
    }

    #[cfg(all(target_os = "macos", target_arch = "x86_64"))]
    {
        &["x86_64-apple-darwin.tar.gz"]
    }

    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    {
        &["aarch64-apple-darwin.tar.gz"]
    }

    #[cfg(not(any(
        all(target_os = "windows", target_arch = "x86_64"),
        all(target_os = "windows", target_arch = "aarch64"),
        all(target_os = "linux", target_arch = "x86_64"),
        all(target_os = "linux", target_arch = "aarch64"),
        all(target_os = "macos", target_arch = "x86_64"),
        all(target_os = "macos", target_arch = "aarch64")
    )))]
    {
        &[]
    }
}

/// Fetches one JSON value from the latest `sd` release endpoint.
fn fetch_latest_release_json() -> Result<serde_json::Value, ConductorError> {
    let response = ureq::AgentBuilder::new()
        .timeout(std::time::Duration::from_secs(60))
        .build()
        .get(SD_LATEST_RELEASE_API_URL)
        .set("User-Agent", SD_DOWNLOAD_USER_AGENT)
        .call()
        .map_err(|source| {
            ConductorError::Workflow(format!(
                "querying latest sd release metadata from '{SD_LATEST_RELEASE_API_URL}' failed: {source}"
            ))
        })?;

    let mut reader = response.into_reader();
    let mut payload = Vec::new();
    reader.read_to_end(&mut payload).map_err(|source| ConductorError::Io {
        operation: "reading latest sd release metadata response".to_string(),
        path: std::env::temp_dir(),
        source,
    })?;

    serde_json::from_slice::<serde_json::Value>(&payload).map_err(|source| {
        ConductorError::Workflow(format!(
            "decoding latest sd release metadata as JSON failed: {source}"
        ))
    })
}

/// Selects one release asset URL + archive kind for the current host.
fn select_host_release_asset(
    release_json: &serde_json::Value,
) -> Result<ReleaseAssetSelection, ConductorError> {
    let markers = host_release_asset_markers();
    if markers.is_empty() {
        return Err(ConductorError::Workflow(format!(
            "sd common tool has no release-asset mapping for target {}-{}",
            std::env::consts::OS,
            std::env::consts::ARCH
        )));
    }

    let assets =
        release_json.get("assets").and_then(serde_json::Value::as_array).ok_or_else(|| {
            ConductorError::Workflow(
                "latest sd release metadata missing array field 'assets'".to_string(),
            )
        })?;

    for marker in markers {
        let Some(asset) = assets.iter().find(|asset| {
            asset
                .get("name")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|name| name.contains(marker))
        }) else {
            continue;
        };

        let name = asset.get("name").and_then(serde_json::Value::as_str).ok_or_else(|| {
            ConductorError::Workflow(
                "latest sd release asset missing string field 'name'".to_string(),
            )
        })?;

        let download_url = asset
            .get("browser_download_url")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                ConductorError::Workflow(format!(
                    "latest sd release asset '{name}' missing string field 'browser_download_url'"
                ))
            })?;

        let archive_kind = if std::path::Path::new(name)
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("zip"))
        {
            ReleaseArchiveKind::Zip
        } else if name.ends_with(".tar.gz") {
            ReleaseArchiveKind::TarGz
        } else {
            return Err(ConductorError::Workflow(format!(
                "latest sd release asset '{name}' uses unsupported archive suffix"
            )));
        };

        return Ok(ReleaseAssetSelection { download_url: download_url.to_string(), archive_kind });
    }

    Err(ConductorError::Workflow(format!(
        "latest sd release metadata did not include expected target asset markers: {}",
        markers.join(", ")
    )))
}

/// Downloads one release asset payload as raw bytes.
fn download_release_asset(download_url: &str) -> Result<Vec<u8>, ConductorError> {
    let response = ureq::AgentBuilder::new()
        .timeout(std::time::Duration::from_secs(300))
        .build()
        .get(download_url)
        .set("User-Agent", SD_DOWNLOAD_USER_AGENT)
        .call()
        .map_err(|source| {
            ConductorError::Workflow(format!(
                "downloading sd release asset from '{download_url}' failed: {source}"
            ))
        })?;

    let mut reader = response.into_reader();
    let mut payload = Vec::new();
    reader.read_to_end(&mut payload).map_err(|source| ConductorError::Io {
        operation: "reading sd release asset response body".to_string(),
        path: std::env::temp_dir(),
        source,
    })?;

    if payload.is_empty() {
        return Err(ConductorError::Workflow(format!(
            "downloaded sd release asset from '{download_url}' was empty"
        )));
    }

    Ok(payload)
}

/// Extracts `sd` executable bytes from one release-archive payload.
fn extract_release_executable_bytes(
    archive_payload: &[u8],
    archive_kind: ReleaseArchiveKind,
) -> Result<Vec<u8>, ConductorError> {
    let executable_name = executable_file_name();

    match archive_kind {
        ReleaseArchiveKind::Zip => {
            let reader = std::io::Cursor::new(archive_payload);
            let mut archive = zip::ZipArchive::new(reader).map_err(|source| {
                ConductorError::Workflow(format!("decoding sd ZIP release asset failed: {source}"))
            })?;

            for index in 0..archive.len() {
                let mut entry = archive.by_index(index).map_err(|source| {
                    ConductorError::Workflow(format!(
                        "reading sd ZIP release entry at index {index} failed: {source}"
                    ))
                })?;

                let Some(file_name) = entry
                    .enclosed_name()
                    .and_then(|path| path.file_name().map(OsStr::to_os_string))
                else {
                    continue;
                };
                if file_name != OsStr::new(&executable_name) {
                    continue;
                }

                let mut executable_bytes = Vec::new();
                entry.read_to_end(&mut executable_bytes).map_err(|source| {
                    ConductorError::Workflow(format!(
                        "reading sd executable bytes from ZIP release asset failed: {source}"
                    ))
                })?;

                if executable_bytes.is_empty() {
                    return Err(ConductorError::Workflow(
                        "sd executable extracted from ZIP release asset was empty".to_string(),
                    ));
                }

                return Ok(executable_bytes);
            }
        }
        ReleaseArchiveKind::TarGz => {
            let reader = std::io::Cursor::new(archive_payload);
            let decompressor = flate2::read::GzDecoder::new(reader);
            let mut archive = tar::Archive::new(decompressor);

            let entries = archive.entries().map_err(|source| {
                ConductorError::Workflow(format!(
                    "reading sd TAR.GZ release entries failed: {source}"
                ))
            })?;

            for entry_result in entries {
                let mut entry = entry_result.map_err(|source| {
                    ConductorError::Workflow(format!(
                        "decoding one sd TAR.GZ release entry failed: {source}"
                    ))
                })?;

                let path = entry.path().map_err(|source| {
                    ConductorError::Workflow(format!(
                        "reading sd TAR.GZ release entry path failed: {source}"
                    ))
                })?;

                if path.file_name() != Some(OsStr::new(&executable_name)) {
                    continue;
                }

                let mut executable_bytes = Vec::new();
                entry.read_to_end(&mut executable_bytes).map_err(|source| {
                    ConductorError::Workflow(format!(
                        "reading sd executable bytes from TAR.GZ release asset failed: {source}"
                    ))
                })?;

                if executable_bytes.is_empty() {
                    return Err(ConductorError::Workflow(
                        "sd executable extracted from TAR.GZ release asset was empty".to_string(),
                    ));
                }

                return Ok(executable_bytes);
            }
        }
    }

    Err(ConductorError::Workflow(format!(
        "sd executable '{executable_name}' not found in downloaded release asset"
    )))
}

/// Downloads the latest host-specific `sd` release and returns executable
/// payload bytes.
///
/// # Errors
///
/// Returns [`ConductorError`] when release metadata cannot be queried, no
/// matching host asset exists, download fails, or executable extraction fails.
pub fn fetch_payload() -> Result<CommonExecutablePayload, ConductorError> {
    let release_json = fetch_latest_release_json()?;
    let selection = select_host_release_asset(&release_json)?;
    let archive_payload = download_release_asset(&selection.download_url)?;
    let executable_file_name = executable_file_name();
    let executable_bytes =
        extract_release_executable_bytes(&archive_payload, selection.archive_kind)?;

    Ok(CommonExecutablePayload { executable_file_name, executable_bytes })
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use serde_json::json;

    use super::{LOGICAL_TOOL_NAME, executable_file_name, select_host_release_asset};

    #[test]
    fn executable_name_matches_sd_with_platform_suffix() {
        let file_name = executable_file_name();
        assert!(file_name.starts_with("sd"));

        #[cfg(windows)]
        assert!(
            Path::new(&file_name)
                .extension()
                .is_some_and(|extension| extension.eq_ignore_ascii_case("exe"))
        );

        #[cfg(not(windows))]
        assert!(!file_name.ends_with(".exe"));

        assert_eq!(LOGICAL_TOOL_NAME, "mediapm-conductor.tools.sd");
    }

    #[test]
    fn host_release_asset_selection_prefers_supported_suffix() {
        let release = json!({
            "assets": [
                {
                    "name": "sd-v1.0.0-unrelated.txt",
                    "browser_download_url": "https://example.invalid/unrelated.txt"
                },
                {
                    "name": "sd-v1.0.0-x86_64-pc-windows-msvc.zip",
                    "browser_download_url": "https://example.invalid/sd.zip"
                },
                {
                    "name": "sd-v1.0.0-x86_64-unknown-linux-gnu.tar.gz",
                    "browser_download_url": "https://example.invalid/sd.tar.gz"
                }
            ]
        });

        let selection = select_host_release_asset(&release);

        #[cfg(any(
            all(target_os = "windows", target_arch = "x86_64"),
            all(target_os = "windows", target_arch = "aarch64"),
            all(target_os = "linux", target_arch = "x86_64"),
            all(target_os = "linux", target_arch = "aarch64"),
            all(target_os = "macos", target_arch = "x86_64"),
            all(target_os = "macos", target_arch = "aarch64")
        ))]
        {
            assert!(selection.is_ok(), "expected host asset selection to succeed");
        }

        #[cfg(not(any(
            all(target_os = "windows", target_arch = "x86_64"),
            all(target_os = "windows", target_arch = "aarch64"),
            all(target_os = "linux", target_arch = "x86_64"),
            all(target_os = "linux", target_arch = "aarch64"),
            all(target_os = "macos", target_arch = "x86_64"),
            all(target_os = "macos", target_arch = "aarch64")
        )))]
        {
            assert!(selection.is_err(), "unsupported targets should fail clearly");
        }
    }
}
