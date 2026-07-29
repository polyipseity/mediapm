//! Runtime `.env` management for conductor execution contexts.
//!
//! Conductor supports loading zero or more dotenv files in specified order.
//! The default convention uses two colocated files under the resolved
//! `runtime_storage_paths.conductor_dir` root:
//! - `.env`: user-authored environment variables,
//! - `.env.generated`: machine-generated runtime variables.
//!
//! Loaded variable names are returned so callers can inherit them into
//! executable tool environments.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;
use std::fs;
use std::path::{Path, PathBuf};

use crate::ToolRuntime;
use crate::error::ConductorError;

/// User-authored runtime dotenv file name under one conductor directory root.
pub const RUNTIME_DOTENV_FILE_NAME: &str = ".env";
/// Machine-generated runtime dotenv file name under one conductor directory root.
pub const RUNTIME_DOTENV_GENERATED_FILE_NAME: &str = ".env.generated";

/// Canonical generated `.env` template for conductor runtime roots.
const RUNTIME_DOTENV_TEMPLATE: &str = concat!(
    "# conductor runtime environment variables\n",
    "#\n",
    "# User-authored values go here. This file is loaded automatically by\n",
    "# conductor before state/workflow operations.\n",
    "#\n",
    "# Core mediapm/conductor runtime knobs:\n",
    "# MEDIAPM_CONDUCTOR_EXECUTABLE_TIMEOUT_SECS=120\n",
    "# MEDIAPM_CONDUCTOR_RPC_TIMEOUT_SECONDS=300\n",
    "# MEDIAPM_HTTP_TIMEOUT_SECONDS=180\n",
    "#\n",
    "# Media metadata enrichment (AcoustID -> MusicBrainz):\n",
    "# ACOUSTID_API_KEY=replace-me\n",
    "#\n",
    "# Optional demo/example timeout override:\n",
    "# MEDIAPM_DEMO_ONLINE_TIMEOUT_SECS=180\n",
    "#\n",
    "# Optional manual override for internal media-tagger ffmpeg path\n",
    "# (normally generated automatically into .env.generated):\n",
    "# MEDIAPM_MEDIA_TAGGER_FFMPEG_BIN=\n",
    "#\n",
    "# Optional manual override for yt-dlp companion paths\n",
    "# (normally generated automatically into .env.generated):\n",
    "# MEDIAPM_YT_DLP_FFMPEG_LOCATION=\n",
    "# MEDIAPM_YT_DLP_JS_RUNTIMES=\n",
    "#\n",
    "# Optional proxy passthrough for downloader tools:\n",
    "# HTTP_PROXY=http://127.0.0.1:7890\n",
    "# HTTPS_PROXY=http://127.0.0.1:7890\n",
    "# NO_PROXY=localhost,127.0.0.1\n",
);

/// Canonical generated `.env.generated` template for conductor runtime roots.
const RUNTIME_DOTENV_GENERATED_TEMPLATE: &str = concat!(
    "# conductor generated runtime variables\n",
    "#\n",
    "# This file is managed by tooling. Manual edits may be overwritten.\n",
);

/// Returns the canonical `.env` path for one conductor runtime root.
#[must_use]
pub fn runtime_dotenv_path(conductor_dir: &Path) -> PathBuf {
    conductor_dir.join(RUNTIME_DOTENV_FILE_NAME)
}

/// Returns the canonical `.env.generated` path for one conductor runtime root.
#[must_use]
pub fn runtime_generated_dotenv_path(conductor_dir: &Path) -> PathBuf {
    conductor_dir.join(RUNTIME_DOTENV_GENERATED_FILE_NAME)
}

/// Ensures runtime dotenv template files exist.
///
/// # Errors
///
/// Returns [`ConductorError`] when directory creation or file writes fail.
pub fn ensure_runtime_env_files(conductor_dir: &Path) -> Result<(), ConductorError> {
    fs::create_dir_all(conductor_dir).map_err(|source| ConductorError::Io {
        operation: "creating conductor runtime environment directory".to_string(),
        path: conductor_dir.to_path_buf(),
        source,
    })?;

    let dotenv_path = runtime_dotenv_path(conductor_dir);
    if !dotenv_path.exists() {
        fs::write(&dotenv_path, RUNTIME_DOTENV_TEMPLATE.as_bytes()).map_err(|source| {
            ConductorError::Io {
                operation: "writing conductor runtime .env template".to_string(),
                path: dotenv_path.clone(),
                source,
            }
        })?;
    }

    let generated_dotenv_path = runtime_generated_dotenv_path(conductor_dir);
    if !generated_dotenv_path.exists() {
        fs::write(&generated_dotenv_path, RUNTIME_DOTENV_GENERATED_TEMPLATE.as_bytes()).map_err(
            |source| ConductorError::Io {
                operation: "writing conductor runtime .env.generated template".to_string(),
                path: generated_dotenv_path.clone(),
                source,
            },
        )?;
    }

    Ok(())
}

/// Loads conductor runtime dotenv files in specified order.
///
/// Each file name is resolved relative to `conductor_dir`. Later files
/// override earlier values. An empty list loads nothing (returns empty names).
/// Returned names preserve declaration order with case-insensitive
/// de-duplication.
///
/// # Errors
///
/// Returns [`ConductorError`] when any dotenv file cannot be read or parsed.
pub fn load_runtime_env_files(
    conductor_dir: &Path,
    file_names: &[&str],
) -> Result<Vec<String>, ConductorError> {
    let mut inherited_names = Vec::new();

    for file_name in file_names {
        let path = conductor_dir.join(file_name);
        let names = read_dotenv_variable_names(&path)?;
        for name in &names {
            append_unique_env_var_name(&mut inherited_names, name);
        }

        if path.exists() {
            dotenvy::from_path_override(&path).map_err(|source| {
                ConductorError::Workflow(format!(
                    "loading conductor runtime dotenv file '{}' failed: {source}",
                    path.display()
                ))
            })?;
        }
    }

    Ok(inherited_names)
}

/// Returns the default env file names in load order.
#[must_use]
pub fn default_runtime_env_file_names() -> &'static [&'static str] {
    &[RUNTIME_DOTENV_FILE_NAME, RUNTIME_DOTENV_GENERATED_FILE_NAME]
}

/// Reads dotenv variable names from one file without mutating process env.
fn read_dotenv_variable_names(path: &Path) -> Result<Vec<String>, ConductorError> {
    if !path.exists() {
        return Ok(Vec::new());
    }

    let mut names = Vec::new();
    let iter = dotenvy::from_path_iter(path).map_err(|source| {
        ConductorError::Workflow(format!(
            "parsing conductor runtime dotenv file '{}' failed: {source}",
            path.display()
        ))
    })?;

    for entry in iter {
        let (name, _value) = entry.map_err(|source| {
            ConductorError::Workflow(format!(
                "parsing conductor runtime dotenv assignment in '{}' failed: {source}",
                path.display()
            ))
        })?;
        append_unique_env_var_name(&mut names, &name);
    }

    Ok(names)
}

/// Appends one env-var name with trimming and case-insensitive de-duplication.
fn append_unique_env_var_name(target: &mut Vec<String>, raw_name: &str) {
    let trimmed = raw_name.trim();
    if trimmed.is_empty() {
        return;
    }

    if target.iter().any(|existing| existing.eq_ignore_ascii_case(trimmed)) {
        return;
    }

    target.push(trimmed.to_string());
}

/// Canonical conductor `.gitignore` content for the runtime root.
///
/// Created at startup to keep generated dotenv files out of version control.
pub const CONDUCTOR_GITIGNORE_CONTENT: &str = concat!("/.env\n", "/.env.generated\n");

/// Canonical `# @generated` header for `.env.generated` files.
///
/// Used by `write_generated_dotenv()` when overwriting the generated env file.
pub const RUNTIME_DOTENV_GENERATED_HEADER: &str = concat!(
    "# @generated\n",
    "# Managed runtime environment variables.\n",
    "# Do not edit manually; values may be rewritten during sync.\n\n",
);

/// Helper: strips the `@hash` suffix from a tool key, returning the plain tool id.
#[must_use]
fn strip_tool_key_hash(tool_key: &str) -> &str {
    tool_key.split('@').next().unwrap_or(tool_key)
}

/// Derives the binary-entry env var name from an already-uppercased tool id
/// and a content-map key.
///
/// Returns `(env_var_name, key)` tuple.
#[must_use]
fn content_key_to_env_name<'a>(tool_id_upper: &str, key: &'a str) -> (String, &'a str) {
    let mut parts = key.splitn(2, '/');
    let os = parts.next().unwrap_or("");
    let rest = parts.next().unwrap_or("");
    let os_upper = os.to_uppercase();
    if rest.is_empty() {
        (format!("MEDIAPM_{tool_id_upper}_{os_upper}_DIR"), key)
    } else {
        (format!("MEDIAPM_{tool_id_upper}_{os_upper}"), key)
    }
}

/// Renders one dotenv value as a double-quoted literal with escapes.
#[must_use]
fn render_dotenv_quoted_value(value: &str) -> String {
    let escaped = value
        .replace("\\\\", "\\\\\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t");
    format!("\"{escaped}\"")
}

/// Overwrites `.env.generated` with the canonical header and tool binary path
/// entries derived from tool runtimes' content maps.
///
/// For each tool runtime entry, iterates content-map keys. Always emits a
/// `_DIR` env var pointing to the OS directory. When the key is a binary
/// entry (has a filename part), additionally emits the non-`_DIR` binary
/// env var. A dedup set prevents duplicate `_DIR` entries for the same
/// tool+OS.
///
/// Env var names derive from the plain tool id (any `@hash` suffix stripped).
/// Path values point to `<tools_base_dir>/<plain_tool_id>/payload/<key>`.
/// All paths are resolved to absolute before writing.
///
/// # Errors
///
/// Returns [`ConductorError`] when path resolution or file writes fail.
pub fn write_generated_dotenv(
    conductor_dir: &Path,
    tools_base_dir: &Path,
    tool_runtimes: &BTreeMap<String, ToolRuntime>,
) -> Result<(), ConductorError> {
    let generated_path = runtime_generated_dotenv_path(conductor_dir);
    let mut content = String::from(RUNTIME_DOTENV_GENERATED_HEADER);

    for (tool_id, runtime) in tool_runtimes {
        let plain_tool_id = strip_tool_key_hash(tool_id);
        let tool_id_upper = plain_tool_id.to_uppercase().replace(['-', '.'], "_");
        let mut emitted_dirs: BTreeSet<&str> = BTreeSet::new();

        for key in runtime.content_map.keys() {
            let mut parts = key.splitn(2, '/');
            let os = parts.next().unwrap_or("");

            // Always emit the _DIR entry pointing to the OS directory.
            let dir_env_name = format!("MEDIAPM_{}_{}_DIR", tool_id_upper, os.to_uppercase());
            if emitted_dirs.insert(os) {
                let dir_key = format!("{os}/");
                let dir_path = tools_base_dir.join(plain_tool_id).join("payload").join(&dir_key);
                let dir_value = std::path::absolute(&dir_path)
                    .map_err(|e| ConductorError::Io {
                        operation: "resolving absolute path for .env.generated dir entry".into(),
                        path: dir_path,
                        source: e,
                    })?
                    .to_string_lossy()
                    .to_string();
                let _ =
                    writeln!(content, "{dir_env_name}={}", render_dotenv_quoted_value(&dir_value));
            }

            // If this is a binary entry, also emit the binary env var.
            let rest = parts.next().unwrap_or("");
            if !rest.is_empty() {
                let (env_name, _env_key) = content_key_to_env_name(&tool_id_upper, key);
                let bin_path = tools_base_dir.join(plain_tool_id).join("payload").join(key);
                let env_value = std::path::absolute(&bin_path)
                    .map_err(|e| ConductorError::Io {
                        operation: "resolving absolute path for .env.generated binary entry".into(),
                        path: bin_path,
                        source: e,
                    })?
                    .to_string_lossy()
                    .to_string();
                let _ = writeln!(content, "{env_name}={}", render_dotenv_quoted_value(&env_value));
            }
        }
    }

    fs::write(&generated_path, content.as_bytes()).map_err(|source| ConductorError::Io {
        operation: "writing generated runtime dotenv values".to_string(),
        path: generated_path,
        source,
    })
}

/// Ensures a `.gitignore` exists in the conductor runtime root.
///
/// Creates the file with [`CONDUCTOR_GITIGNORE_CONTENT`] if it does not
/// already exist. Existing files are never overwritten.
///
/// # Errors
///
/// Returns [`ConductorError`] when directory creation or file writes fail.
pub fn ensure_runtime_gitignore(conductor_dir: &Path) -> Result<(), ConductorError> {
    let gitignore_path = conductor_dir.join(".gitignore");
    if gitignore_path.exists() {
        return Ok(());
    }
    fs::write(&gitignore_path, CONDUCTOR_GITIGNORE_CONTENT.as_bytes()).map_err(|source| {
        ConductorError::Io {
            operation: "writing conductor runtime .gitignore".to_string(),
            path: gitignore_path,
            source,
        }
    })?;
    Ok(())
}

/// Appends extra entries to the conductor runtime root `.gitignore`.
///
/// Skips entries already present in the file (simple string-contains dedup).
/// The `extra` string is appended as-is if not already contained.
///
/// # Errors
///
/// Returns [`ConductorError`] when file reads or writes fail.
pub fn extend_runtime_gitignore(conductor_dir: &Path, extra: &str) -> Result<(), ConductorError> {
    let gitignore_path = conductor_dir.join(".gitignore");
    // Ensure the base file exists first.
    ensure_runtime_gitignore(conductor_dir)?;
    let content = fs::read_to_string(&gitignore_path).map_err(|source| ConductorError::Io {
        operation: "reading conductor runtime .gitignore".to_string(),
        path: gitignore_path.clone(),
        source,
    })?;
    if content.contains(extra) {
        return Ok(());
    }
    let new_content = format!("{content}{extra}");
    fs::write(&gitignore_path, new_content.as_bytes()).map_err(|source| ConductorError::Io {
        operation: "extending conductor runtime .gitignore".to_string(),
        path: gitignore_path,
        source,
    })?;
    Ok(())
}

/// Discovers the project root by walking up from the current working directory.
///
/// Looks for a `conductor.ncl` (or `mediapm.ncl`) file to determine the
/// project boundary. Falls back to the current working directory when no
/// marker is found.
///
/// # Errors
///
/// Returns [`ConductorError`] when the current working directory cannot be
/// resolved.
pub fn discover_project_root() -> Result<PathBuf, ConductorError> {
    let cwd = std::env::current_dir().map_err(|source| ConductorError::Io {
        operation: "resolving current working directory".to_string(),
        path: PathBuf::from("."),
        source,
    })?;

    let mut current = Some(cwd.as_path());
    while let Some(dir) = current {
        if dir.join("conductor.ncl").exists() || dir.join("mediapm.ncl").exists() {
            return Ok(dir.to_path_buf());
        }
        current = dir.parent();
    }

    // Fallback: return the original cwd
    Ok(cwd)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn ensure_runtime_gitignore_creates_file() {
        let dir = tempdir().expect("tempdir");
        let conductor_dir = dir.path().join(".conductor");
        fs::create_dir_all(&conductor_dir).expect("create conductor dir");

        ensure_runtime_gitignore(&conductor_dir).expect("ensure_runtime_gitignore");
        let gitignore_path = conductor_dir.join(".gitignore");
        assert!(gitignore_path.exists(), ".gitignore should exist");
        let content = fs::read_to_string(&gitignore_path).expect("read .gitignore");
        assert!(content.contains("/.env"), ".gitignore should contain /.env\ncontent:\n{content}");
        assert!(
            content.contains("/.env.generated"),
            ".gitignore should contain /.env.generated\ncontent:\n{content}"
        );
    }

    #[test]
    fn ensure_runtime_gitignore_no_overwrite() {
        let dir = tempdir().expect("tempdir");
        let conductor_dir = dir.path().join(".conductor");
        fs::create_dir_all(&conductor_dir).expect("create conductor dir");
        let gitignore_path = conductor_dir.join(".gitignore");
        fs::write(&gitignore_path, "# custom\n").expect("write custom .gitignore");

        ensure_runtime_gitignore(&conductor_dir).expect("ensure_runtime_gitignore");
        let content = fs::read_to_string(&gitignore_path).expect("read .gitignore");
        assert_eq!(content, "# custom\n", "custom .gitignore should be preserved");
    }

    #[test]
    fn extend_runtime_gitignore_appends() {
        let dir = tempdir().expect("tempdir");
        let conductor_dir = dir.path().join(".conductor");
        fs::create_dir_all(&conductor_dir).expect("create conductor dir");

        // First create the base gitignore.
        ensure_runtime_gitignore(&conductor_dir).expect("ensure_runtime_gitignore");
        // Then extend with extra entries.
        extend_runtime_gitignore(&conductor_dir, "/cache/\n/tools/\n")
            .expect("extend_runtime_gitignore");

        let content =
            fs::read_to_string(&conductor_dir.join(".gitignore")).expect("read .gitignore");
        assert!(
            content.contains("/cache/"),
            ".gitignore should contain /cache/\ncontent:\n{content}"
        );
        assert!(
            content.contains("/tools/"),
            ".gitignore should contain /tools/\ncontent:\n{content}"
        );
        assert!(
            content.contains("/.env"),
            ".gitignore should still contain original entries\ncontent:\n{content}"
        );
    }

    #[test]
    fn extend_runtime_gitignore_no_duplicate() {
        let dir = tempdir().expect("tempdir");
        let conductor_dir = dir.path().join(".conductor");
        fs::create_dir_all(&conductor_dir).expect("create conductor dir");

        ensure_runtime_gitignore(&conductor_dir).expect("ensure_runtime_gitignore");
        extend_runtime_gitignore(&conductor_dir, "/cache/\n/tools/\n")
            .expect("extend_runtime_gitignore first");
        extend_runtime_gitignore(&conductor_dir, "/cache/\n/tools/\n")
            .expect("extend_runtime_gitignore second");

        let content =
            fs::read_to_string(&conductor_dir.join(".gitignore")).expect("read .gitignore");
        // /cache/ and /tools/ should appear exactly once each.
        let cache_count = content.matches("/cache/").count();
        let tools_count = content.matches("/tools/").count();
        assert_eq!(
            cache_count, 1,
            "/cache/ should appear exactly once, got {cache_count}\ncontent:\n{content}"
        );
        assert_eq!(
            tools_count, 1,
            "/tools/ should appear exactly once, got {tools_count}\ncontent:\n{content}"
        );
    }

    #[test]
    fn ensure_runtime_gitignore_wired_in_cli() {
        // This test validates that the CLI startup path creates the .gitignore.
        // We simulate the startup by calling the same functions ensure_conductor calls.
        let dir = tempdir().expect("tempdir");
        let conductor_dir = dir.path().join(".conductor");
        fs::create_dir_all(&conductor_dir).expect("create conductor dir");

        // Call the same sequence as ensure_conductor.
        ensure_runtime_env_files(&conductor_dir).expect("ensure_runtime_env_files");
        ensure_runtime_gitignore(&conductor_dir).expect("ensure_runtime_gitignore");

        let gitignore_path = conductor_dir.join(".gitignore");
        assert!(gitignore_path.exists(), ".gitignore should exist after startup sequence");
        let content = fs::read_to_string(&gitignore_path).expect("read .gitignore");
        assert!(content.contains("/.env"), ".gitignore should contain /.env\ncontent:\n{content}");
        assert!(
            content.contains("/.env.generated"),
            ".gitignore should contain /.env.generated\ncontent:\n{content}"
        );

        // Ensure the .env files exist too.
        assert!(
            conductor_dir.join(".env").exists(),
            ".env should exist after ensure_runtime_env_files"
        );
        assert!(
            conductor_dir.join(".env.generated").exists(),
            ".env.generated should exist after ensure_runtime_env_files"
        );
    }

    #[test]
    fn runtime_dotenv_generated_header_is_valid() {
        let header = RUNTIME_DOTENV_GENERATED_HEADER;
        assert!(
            header.starts_with("# @generated"),
            "header should start with # @generated\ngot:\n{header}"
        );
        assert!(!header.is_empty(), "header should not be empty");
    }

    // --- write_generated_dotenv tests ---

    /// Asserts that all non-comment, non-empty lines in the env file have
    /// absolute paths (start with `/` on unix, drive letter on Windows).
    fn assert_env_lines_have_absolute_paths(content: &str) {
        for line in content.lines() {
            if line.starts_with('#') || line.trim().is_empty() {
                continue;
            }
            if let Some((_name, value)) = line.split_once('=') {
                let raw = value.trim_matches('"');
                assert!(
                    raw.starts_with('/')
                        || raw.starts_with("\\\\")
                        || raw.chars().nth(1) == Some(':'),
                    "path must be absolute: {raw}"
                );
            }
        }
    }

    #[test]
    fn content_key_to_env_name_binary() {
        assert_eq!(
            content_key_to_env_name("YT_DLP", "linux/yt-dlp"),
            ("MEDIAPM_YT_DLP_LINUX".to_string(), "linux/yt-dlp"),
        );
    }

    #[test]
    fn content_key_to_env_name_dir() {
        assert_eq!(
            content_key_to_env_name("FFMPEG", "linux/"),
            ("MEDIAPM_FFMPEG_LINUX_DIR".to_string(), "linux/"),
        );
    }

    #[test]
    fn strip_tool_key_hash_removes_suffix() {
        assert_eq!(strip_tool_key_hash("yt-dlp@abc123"), "yt-dlp");
        assert_eq!(strip_tool_key_hash("ffmpeg@deadbeef"), "ffmpeg");
    }

    #[test]
    fn strip_tool_key_hash_no_hash() {
        assert_eq!(strip_tool_key_hash("yt-dlp"), "yt-dlp");
        assert_eq!(strip_tool_key_hash("media-tagger"), "media-tagger");
    }

    #[test]
    fn content_key_to_env_name_strips_hash() {
        let plain = strip_tool_key_hash("yt-dlp@abc123");
        let tool_id_upper = plain.to_uppercase().replace(['-', '.'], "_");
        let (name, key) = content_key_to_env_name(&tool_id_upper, "linux/yt-dlp");
        assert_eq!(name, "MEDIAPM_YT_DLP_LINUX");
        assert!(!name.contains('@'), "env var name must not contain @");
        assert_eq!(key, "linux/yt-dlp");
    }

    #[test]
    fn write_generated_dotenv_header_only() {
        let dir = tempfile::tempdir().expect("tempdir");
        let runtimes = BTreeMap::new();
        write_generated_dotenv(dir.path(), dir.path(), &runtimes).expect("write should succeed");
        let content = std::fs::read_to_string(dir.path().join(".env.generated"))
            .expect("env file should be readable");
        assert_eq!(content, RUNTIME_DOTENV_GENERATED_HEADER);
    }

    #[test]
    fn write_generated_dotenv_binary_produces_dir_and_binary() {
        let dir = tempfile::tempdir().expect("tempdir");
        let tools_dir = dir.path().join("tools");
        let mut content_map = BTreeMap::new();
        content_map.insert("linux/yt-dlp".to_string(), "hash".to_string());
        let mut runtimes = BTreeMap::new();
        runtimes.insert(
            "yt-dlp".to_string(),
            ToolRuntime { content_map: content_map.into(), ..ToolRuntime::default() },
        );
        write_generated_dotenv(dir.path(), &tools_dir, &runtimes).expect("write should succeed");
        let content = std::fs::read_to_string(dir.path().join(".env.generated"))
            .expect("env file should be readable");

        let tools_dir_str = tools_dir.to_string_lossy();
        let dir_line =
            format!("MEDIAPM_YT_DLP_LINUX_DIR=\"{tools_dir_str}/yt-dlp/payload/linux/\"");
        let bin_line =
            format!("MEDIAPM_YT_DLP_LINUX=\"{tools_dir_str}/yt-dlp/payload/linux/yt-dlp\"");

        assert!(content.contains(&dir_line), "env must contain _DIR entry\ncontent:\n{content}");
        assert!(content.contains(&bin_line), "env must contain binary entry\ncontent:\n{content}");
        assert_env_lines_have_absolute_paths(&content);
    }

    #[test]
    fn write_generated_dotenv_dir_produces_dir_only() {
        let dir = tempfile::tempdir().expect("tempdir");
        let tools_dir = dir.path().join("tools");
        let mut content_map = BTreeMap::new();
        content_map.insert("linux/".to_string(), "hash".to_string());
        let mut runtimes = BTreeMap::new();
        runtimes.insert(
            "ffmpeg".to_string(),
            ToolRuntime { content_map: content_map.into(), ..ToolRuntime::default() },
        );
        write_generated_dotenv(dir.path(), &tools_dir, &runtimes).expect("write should succeed");
        let content = std::fs::read_to_string(dir.path().join(".env.generated"))
            .expect("env file should be readable");

        let tools_dir_str = tools_dir.to_string_lossy();
        let dir_line =
            format!("MEDIAPM_FFMPEG_LINUX_DIR=\"{tools_dir_str}/ffmpeg/payload/linux/\"");
        let bin_var = "MEDIAPM_FFMPEG_LINUX=";

        assert!(content.contains(&dir_line), "env must contain _DIR entry\ncontent:\n{content}");
        assert!(
            !content.contains(bin_var),
            "env must not contain binary env var\ncontent:\n{content}"
        );
        assert_env_lines_have_absolute_paths(&content);
    }

    #[test]
    fn write_generated_dotenv_mixed_os_produces_no_duplicate_dirs() {
        let dir = tempfile::tempdir().expect("tempdir");
        let tools_dir = dir.path().join("tools");
        let mut content_map = BTreeMap::new();
        content_map.insert("linux/yt-dlp".to_string(), "h1".to_string());
        content_map.insert("macos/yt-dlp".to_string(), "h2".to_string());
        let mut runtimes = BTreeMap::new();
        runtimes.insert(
            "yt-dlp".to_string(),
            ToolRuntime { content_map: content_map.into(), ..ToolRuntime::default() },
        );
        write_generated_dotenv(dir.path(), &tools_dir, &runtimes).expect("write should succeed");
        let content = std::fs::read_to_string(dir.path().join(".env.generated"))
            .expect("env file should be readable");

        let tools_dir_str = tools_dir.to_string_lossy();

        assert!(
            content.contains(&format!(
                "MEDIAPM_YT_DLP_LINUX_DIR=\"{tools_dir_str}/yt-dlp/payload/linux/\""
            )),
            "missing linux _DIR entry"
        );
        assert!(
            content.contains(&format!(
                "MEDIAPM_YT_DLP_MACOS_DIR=\"{tools_dir_str}/yt-dlp/payload/macos/\""
            )),
            "missing macos _DIR entry"
        );
        assert!(
            content.contains(&format!(
                "MEDIAPM_YT_DLP_LINUX=\"{tools_dir_str}/yt-dlp/payload/linux/yt-dlp\""
            )),
            "missing linux binary entry"
        );
        assert!(
            content.contains(&format!(
                "MEDIAPM_YT_DLP_MACOS=\"{tools_dir_str}/yt-dlp/payload/macos/yt-dlp\""
            )),
            "missing macos binary entry"
        );

        let dir_count = content.matches("_DIR=").count();
        assert_eq!(dir_count, 2, "expected exactly 2 _DIR entries, got {dir_count}");
        assert_env_lines_have_absolute_paths(&content);
    }

    #[test]
    fn write_generated_dotenv_uses_absolute_paths() {
        let dir = tempfile::tempdir().expect("tempdir");
        let tools_dir = dir.path().join("tools");
        let cwd = std::env::current_dir().expect("current dir");
        std::env::set_current_dir(dir.path()).expect("cd to tempdir");
        let content = {
            let mut content_map = BTreeMap::new();
            content_map.insert("linux/yt-dlp".to_string(), "hash".to_string());
            let mut runtimes = BTreeMap::new();
            runtimes.insert(
                "yt-dlp".to_string(),
                ToolRuntime { content_map: content_map.into(), ..ToolRuntime::default() },
            );
            write_generated_dotenv(dir.path(), &tools_dir, &runtimes)
                .expect("write should succeed");
            std::fs::read_to_string(dir.path().join(".env.generated")).expect("read .env.generated")
        };
        std::env::set_current_dir(&cwd).expect("restore cwd");
        assert_env_lines_have_absolute_paths(&content);
    }
}
