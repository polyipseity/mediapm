//! Path validation, readonly enforcement, and filesystem helpers.
//!
//! Two concerns live here. The first is the hierarchy **path-component
//! validation chain** ([`PathComponent::parse`] and
//! [`sanitize_and_validate_components`]): NFD normalization,
//! reserved-character sanitization, and strict per-component validation
//! including `.`/`..` traversal rejection. Its production entry point is
//! [`crate::materializer::sanitize_and_validate_hierarchy_paths`], which the
//! materializer runs over every resolved hierarchy entry before any path is
//! staged or written, and the ZIP extraction path in
//! [`crate::materializer::zip`]. The second is **readonly enforcement** for
//! managed outputs ([`ensure_managed_path_readonly`]) and **stale-path
//! removal** ([`remove_path`]).
//!
//! ## Why the [`PathComponent`] newtype
//!
//! Validation a caller can forget is not validation. Before this type, the
//! two untrusted-input paths — resolved hierarchy components and extracted
//! ZIP member names — each carried their own `Vec<String>` and each relied on
//! the caller having invoked the right checks in the right order. A ZIP member
//! named `../evil.txt` reached `Path::join` with the `..` still live.
//!
//! [`PathComponent`] closes that gap structurally: it is constructible **only**
//! through [`PathComponent::parse`], and it exposes no `Deref`, no
//! `AsRef<str>`, no public field, and no `From<String>`. The only way to hold
//! one is to have passed untrusted text through the parser.
//!
//! ## Known seam
//!
//! [`FlattenedHierarchyEntry::path_components`] is declared `Vec<String>` in
//! the `config` module, which is outside this migration's scope, so
//! [`components_to_strings`] re-serializes the parsed values back into that
//! field. Every string it emits came from a successful parse, so the write-back
//! is validated by construction; it is a storage-shape compromise, not a
//! second validation entry point.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use unicode_normalization::UnicodeNormalization;

use crate::config::hierarchy_types::SanitizeNamesConfig;
use crate::error::MediaPmError;

/// Removes one path recursively when it is a directory, or as one file otherwise.
pub(super) fn remove_path(path: &Path) -> Result<(), MediaPmError> {
    clear_path_readonly_recursively(path)?;

    // On Unix, removing a child entry requires write permission on the parent
    // directory. The parent directory is usually already writable, but may be
    // read-only when it is itself a managed output that was marked read-only by
    // `ensure_managed_path_readonly()` during a previous materialization cycle
    // (e.g. a hierarchy folder node containing stale file entries).
    clear_directory_writable(path)?;

    let metadata = fs::symlink_metadata(path).map_err(|source| MediaPmError::Io {
        operation: "reading path metadata before removal".to_string(),
        path: path.to_path_buf(),
        source,
    })?;

    if metadata.is_dir() {
        fs::remove_dir_all(path).map_err(|source| MediaPmError::Io {
            operation: "removing stale directory".to_string(),
            path: path.to_path_buf(),
            source,
        })
    } else {
        fs::remove_file(path).map_err(|source| MediaPmError::Io {
            operation: "removing stale file".to_string(),
            path: path.to_path_buf(),
            source,
        })
    }
}

/// Marks one managed output path as read-only after successful materialization.
///
/// For directory outputs, this recursively marks descendant files/directories
/// read-only. For symlinks, permissions are applied to the resolved target.
pub(super) fn ensure_managed_path_readonly(path: &Path) -> Result<(), MediaPmError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| MediaPmError::Io {
        operation: "reading managed output metadata before readonly enforcement".to_string(),
        path: path.to_path_buf(),
        source,
    })?;

    if metadata.is_dir() {
        for entry in fs::read_dir(path).map_err(|source| MediaPmError::Io {
            operation: "reading managed output directory before readonly enforcement".to_string(),
            path: path.to_path_buf(),
            source,
        })? {
            let entry = entry.map_err(|source| MediaPmError::Io {
                operation: "iterating managed output directory before readonly enforcement"
                    .to_string(),
                path: path.to_path_buf(),
                source,
            })?;
            ensure_managed_path_readonly(&entry.path())?;
        }
    }

    let mut permissions = fs::metadata(path)
        .map_err(|source| MediaPmError::Io {
            operation: "reading managed output permissions before readonly enforcement".to_string(),
            path: path.to_path_buf(),
            source,
        })?
        .permissions();
    if !permissions.readonly() {
        permissions.set_readonly(true);
        fs::set_permissions(path, permissions).map_err(|source| MediaPmError::Io {
            operation: "marking managed output path readonly".to_string(),
            path: path.to_path_buf(),
            source,
        })?;
    }

    Ok(())
}

/// Clears read-only bit recursively so stale managed paths can be removed.
///
/// On BSD platforms (macOS, FreeBSD, etc.) this also clears the user/system
/// immutable flags (`UF_IMMUTABLE` / `SF_IMMUTABLE` / `uchg` / `schg`) which
/// prevent file deletion independently of Unix permission bits.
fn clear_path_readonly_recursively(path: &Path) -> Result<(), MediaPmError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| MediaPmError::Io {
        operation: "reading path metadata before readonly clear".to_string(),
        path: path.to_path_buf(),
        source,
    })?;

    if metadata.is_dir() {
        for entry in fs::read_dir(path).map_err(|source| MediaPmError::Io {
            operation: "reading directory before readonly clear".to_string(),
            path: path.to_path_buf(),
            source,
        })? {
            let entry = entry.map_err(|source| MediaPmError::Io {
                operation: "iterating directory before readonly clear".to_string(),
                path: path.to_path_buf(),
                source,
            })?;
            clear_path_readonly_recursively(&entry.path())?;
        }
    }

    // On BSD platforms (macOS, FreeBSD, etc.), clear immutable file flags
    // that prevent deletion independently of Unix permission bits.
    // These flags can be inherited from tool outputs, set by backup software,
    // or applied manually by the user.
    #[cfg(any(
        target_os = "macos",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd"
    ))]
    {
        clear_bsd_immutable_flags(path)?;
    }

    let mut permissions = fs::metadata(path)
        .map_err(|source| MediaPmError::Io {
            operation: "reading path permissions before readonly clear".to_string(),
            path: path.to_path_buf(),
            source,
        })?
        .permissions();
    if permissions.readonly() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mode = permissions.mode();
            let writable_mode = mode | 0o200;
            if writable_mode != mode {
                permissions.set_mode(writable_mode);
            }
        }

        #[cfg(not(unix))]
        {
            #[expect(
                clippy::permissions_set_readonly_false,
                reason = "on non-Unix platforms we must clear the readonly flag before managed overwrite/delete operations can succeed"
            )]
            {
                permissions.set_readonly(false);
            }
        }

        fs::set_permissions(path, permissions).map_err(|source| MediaPmError::Io {
            operation: "clearing readonly bit before managed-path removal".to_string(),
            path: path.to_path_buf(),
            source,
        })?;
    }

    Ok(())
}

/// Clears BSD immutable flags (`UF_IMMUTABLE` / `SF_IMMUTABLE`) on the given
/// path so the file can be removed.
///
/// Uses `stat` + `chflags` (both following symlinks) for consistency with the
/// `fs::metadata` / `fs::set_permissions` calls elsewhere in this function.
/// If `stat` fails (e.g. the path no longer exists), this is treated as a
/// no-op and the subsequent permission check will surface any relevant error.
#[cfg(any(
    target_os = "macos",
    target_os = "freebsd",
    target_os = "netbsd",
    target_os = "openbsd"
))]
fn clear_bsd_immutable_flags(path: &Path) -> Result<(), MediaPmError> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| MediaPmError::Workflow("path contains null byte for chflags".to_string()))?;

    // Read current flags via stat (follows symlinks, matching fs::metadata behavior).
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    if unsafe { libc::stat(c_path.as_ptr(), &raw mut st) } != 0 {
        // Path may not exist; let the caller's fs::metadata surface the error.
        return Ok(());
    }

    let immutable_mask = libc::UF_IMMUTABLE | libc::SF_IMMUTABLE;
    if (st.st_flags as u32) & immutable_mask != 0 {
        let new_flags = (st.st_flags as u32) & !immutable_mask;
        if unsafe { libc::chflags(c_path.as_ptr(), new_flags) } != 0 {
            let err = std::io::Error::last_os_error();
            return Err(MediaPmError::Io {
                operation: "clearing immutable flags before managed-path removal".to_string(),
                path: path.to_path_buf(),
                source: err,
            });
        }
    }

    Ok(())
}

/// Ensures the parent directory of `path` is writable so the child entry can
/// be removed.
///
/// On Unix, unlinking or renaming a child requires write permission on the
/// containing directory. Managed directory outputs may be read-only when they
/// are themselves part of a previously materialized hierarchy tree. This
/// helper clears the readonly bit and BSD immutable flags on the parent
/// (without recursing into sibling entries).
fn clear_directory_writable(path: &Path) -> Result<(), MediaPmError> {
    let Some(parent) = path.parent() else { return Ok(()) };

    #[cfg(any(
        target_os = "macos",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd"
    ))]
    {
        clear_bsd_immutable_flags(parent)?;
    }

    let Ok(metadata) = fs::metadata(parent) else { return Ok(()) };
    let mut permissions = metadata.permissions();
    if permissions.readonly() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mode = permissions.mode();
            let writable_mode = mode | 0o200;
            if writable_mode != mode {
                permissions.set_mode(writable_mode);
            }
        }

        #[cfg(not(unix))]
        {
            #[expect(
                clippy::permissions_set_readonly_false,
                reason = "on non-Unix platforms we must clear the readonly flag before managed delete operations can succeed"
            )]
            {
                permissions.set_readonly(false);
            }
        }

        fs::set_permissions(parent, permissions).map_err(|source| MediaPmError::Io {
            operation: "clearing readonly bit on parent directory before removal".to_string(),
            path: parent.to_path_buf(),
            source,
        })?;
    }

    Ok(())
}

/// Reserved characters rewritten by [`default_sanitize_replacements`].
///
/// Mirrors the character set rejected by `is_rejected_char`, so
/// [`SanitizeNamesConfig::Enabled`] rewrites exactly the characters that
/// would otherwise fail validation.
pub(super) const SANITIZED_RESERVED_CHARS: [char; 9] =
    ['<', '>', ':', '"', '|', '?', '*', '/', '\\'];

/// Builds the default replacement map applied under
/// [`SanitizeNamesConfig::Enabled`]: every reserved character listed in
/// [`SANITIZED_RESERVED_CHARS`] becomes `_`.
///
/// This is the concrete meaning of the `Enabled` variant's documented
/// "reserved chars to `_`" policy. Per-entry [`SanitizeNamesConfig::Custom`]
/// maps are layered on top of this map by
/// [`sanitize_and_validate_components`].
#[must_use]
pub(super) fn default_sanitize_replacements() -> BTreeMap<char, char> {
    SANITIZED_RESERVED_CHARS.into_iter().map(|ch| (ch, '_')).collect()
}

/// Applies a reserved-character replacement map to a single path component.
///
/// This operates on individual characters within one path component, not on
/// a joined path string, so `/` and `\` within a component are properly
/// replaced rather than consumed as structural separators. Characters absent
/// from `replacements` pass through unchanged.
#[must_use]
pub(super) fn sanitize_path_component(
    component: &str,
    replacements: &BTreeMap<char, char>,
) -> String {
    component.chars().map(|ch| replacements.get(&ch).copied().unwrap_or(ch)).collect()
}

/// Fix-versus-reject policy applied by [`PathComponent::parse`].
///
/// The policy governs only the violation class that has a fix: reserved
/// characters listed in [`SANITIZED_RESERVED_CHARS`] are rewritten to their
/// replacement when the policy carries one, and rejected when it does not.
/// NFD normalization is unconditional and therefore not policy-governed. The
/// unfixable classes — empty, `.`, `..`, path separators, and control
/// characters — are rejected under every policy.
#[derive(Debug, Clone, Default)]
pub(super) struct SanitizePolicy {
    /// Reserved-character replacements. An empty map means "reject, do not
    /// rewrite", which is what [`SanitizeNamesConfig::Disabled`] selects.
    replacements: BTreeMap<char, char>,
}

impl SanitizePolicy {
    /// Returns the policy that rejects every reserved character without
    /// rewriting it. This is the policy the ZIP extraction path uses: an
    /// archive's member names are not the user's to rename, so silently
    /// rewriting one would materialize a file the user never asked for.
    pub(super) fn disabled() -> Self {
        Self { replacements: BTreeMap::new() }
    }

    /// Returns the policy that rewrites reserved characters, layering
    /// `custom` over `default_replacements`.
    pub(super) fn with_replacements(
        default_replacements: &BTreeMap<char, char>,
        custom: Option<&BTreeMap<char, char>>,
    ) -> Self {
        let mut replacements = default_replacements.clone();
        if let Some(custom) = custom {
            replacements.extend(custom.iter().map(|(k, v)| (*k, *v)));
        }
        Self { replacements }
    }

    /// Reports whether this policy rewrites reserved characters rather than
    /// rejecting them.
    fn rewrites_reserved_chars(&self) -> bool {
        !self.replacements.is_empty()
    }
}

/// Builds the [`SanitizePolicy`] a [`SanitizeNamesConfig`] selects.
///
/// `Disabled` and `Inherit` both yield [`SanitizePolicy::disabled`]:
/// `Inherit` is resolved to its effective value before the materializer runs
/// this chain, and a value that reaches here unresolved falls back to the
/// no-rewrite behavior rather than silently sanitizing. `Enabled` yields the
/// default replacement map alone; `Custom` layers the per-entry map on top.
pub(super) fn sanitize_policy_for(
    sanitize_names: &SanitizeNamesConfig,
    default_replacements: &BTreeMap<char, char>,
) -> SanitizePolicy {
    match sanitize_names {
        SanitizeNamesConfig::Enabled => {
            SanitizePolicy::with_replacements(default_replacements, None)
        }
        SanitizeNamesConfig::Custom(custom) => {
            SanitizePolicy::with_replacements(default_replacements, Some(custom))
        }
        SanitizeNamesConfig::Disabled | SanitizeNamesConfig::Inherit => SanitizePolicy::disabled(),
    }
}

/// One validated filesystem path component.
///
/// Constructed **only** by [`PathComponent::parse`]. There is deliberately no
/// `Deref`, no `AsRef<str>`, no public field, and no `From<String>`, so raw
/// untrusted text cannot reach a [`PathBuf::join`] without passing the parser
/// first. The two untrusted-input paths that feed filesystem joins — resolved
/// hierarchy components and extracted ZIP member names — both go through it.
///
/// The parser:
/// 1. NFD-normalizes the input (unconditional; mediapm commits NFD-only names),
/// 2. applies `policy` to the reserved characters, rewriting or rejecting,
/// 3. rejects the unfixable classes: empty, `.`, `..`, embedded separators,
///    and control characters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PathComponent(String);

impl PathComponent {
    /// Parses one untrusted component under `policy`.
    ///
    /// # Errors
    ///
    /// Returns the first [`MediaPmError::Workflow`] from the invariant checks,
    /// naming the offending component and the violated rule.
    pub(super) fn parse(raw: &str, policy: &SanitizePolicy) -> Result<Self, MediaPmError> {
        let normalized = raw.nfd().collect::<String>();
        let sanitized = if policy.rewrites_reserved_chars() {
            sanitize_path_component(&normalized, &policy.replacements)
        } else {
            normalized
        };
        check_component(&sanitized)?;
        Ok(Self(sanitized))
    }
}

/// Joins validated components into one relative [`PathBuf`].
///
/// # Panics
///
/// Never: each component is a single validated path segment, so joining them
/// cannot produce a separator or a traversal component.
pub(super) fn join_path_components(components: &[PathComponent]) -> PathBuf {
    components.iter().map(|component| PathBuf::from(&component.0)).collect()
}

/// Parses every component of one untrusted relative path under `policy`.
///
/// Both `/` and `\` are treated as structural separators, so a Windows-style
/// `..\..\evil.txt` is split rather than smuggled through as one opaque
/// component. Separators left after splitting would still be rejected by
/// [`PathComponent::parse`], so the split narrows the input, never widens it.
///
/// # Errors
///
/// Propagates the first component failure from [`PathComponent::parse`].
pub(super) fn parse_relative_path_components(
    path: &Path,
    policy: &SanitizePolicy,
) -> Result<Vec<PathComponent>, MediaPmError> {
    path.to_string_lossy()
        .split(['/', '\\'])
        .filter(|component| !component.is_empty())
        .map(|component| PathComponent::parse(component, policy))
        .collect()
}

/// Serializes validated components back into `Vec<String>`.
///
/// Sole consumer is the
/// [`FlattenedHierarchyEntry::path_components`] write-back in
/// [`crate::materializer::sanitize_and_validate_hierarchy_paths`]. That field
/// is declared `Vec<String>` in the `config` module, which is outside this
/// migration's scope. Every string emitted here came from a successful
/// [`PathComponent::parse`], so the result is validated by construction; this
/// is a storage-shape seam, not a second validation entry point.
pub(super) fn components_to_strings(components: &[PathComponent]) -> Vec<String> {
    components.iter().map(|component| component.0.clone()).collect()
}

/// Applies NFD normalization, optional reserved-character sanitization, and
/// strict validation to resolved hierarchy path components.
///
/// Every component is parsed through [`PathComponent::parse`] under the
/// [`SanitizePolicy`] that `sanitize_names` selects, so the returned values
/// cannot exist without having passed the parser.
///
/// # Errors
///
/// Propagates the first component failure, naming the offending component.
///
/// Production entry point of the chain: the materializer calls this for every
/// flattened hierarchy entry, after metadata interpolation and before any
/// path is staged, verified, or committed (see
/// [`crate::materializer::sanitize_and_validate_hierarchy_paths`]).
pub(super) fn sanitize_and_validate_components(
    components: &[String],
    sanitize_names: &SanitizeNamesConfig,
    default_replacements: &BTreeMap<char, char>,
) -> Result<Vec<PathComponent>, MediaPmError> {
    let policy = sanitize_policy_for(sanitize_names, default_replacements);
    components.iter().map(|component| PathComponent::parse(component, &policy)).collect()
}

/// Checks one already-normalized component against the invariants that have
/// no fix: non-empty, not `.` or `..`, no forbidden characters, and NFD.
///
/// Config-declared components are a separate, earlier stage: they are rejected
/// for non-NFD spelling by
/// [`crate::config::hierarchy_types::check_nfd_source`], because a user can fix
/// a declaration but not the metadata interpolated into it later. A component
/// reaching [`PathComponent::parse`] has already been NFD-normalized, so the
/// NFD check here is the second of the two stages and never rejects on its
/// own.
fn check_component(component: &str) -> Result<(), MediaPmError> {
    if component.is_empty() {
        return Err(MediaPmError::Workflow(
            "hierarchy path component must not be empty".to_string(),
        ));
    }
    if component == "." || component == ".." {
        return Err(MediaPmError::Workflow(format!(
            "hierarchy path component '{component}' must not be '.' or '..'"
        )));
    }
    if component.chars().any(is_rejected_char) {
        return Err(MediaPmError::Workflow(format!(
            "hierarchy path component '{component}' contains forbidden characters"
        )));
    }
    if component.chars().any(is_control_char) {
        return Err(MediaPmError::Workflow(format!(
            "hierarchy path component '{component}' contains control characters"
        )));
    }
    let component_nfd = component.nfd().collect::<String>();
    if component_nfd != component {
        return Err(MediaPmError::Workflow(format!(
            "hierarchy path component '{component}' is not NFD-normalized"
        )));
    }
    Ok(())
}

/// Returns whether one character is forbidden by cross-platform filename rules.
///
/// The set doubles as the traversal guard: `/` and `\\` are rejected, so a
/// component that smuggled a separator (for example an artist tag `AC/DC`)
/// can never re-split into extra path components, and an absolute component
/// can never anchor outside the hierarchy root. `.` and `..` are rejected by
/// [`check_component`] before this predicate is consulted.
///
/// `pub(crate)` because it is the **single** definition of the set: the
/// config-level media-id rule in
/// `crate::config::hierarchy_types::validate_media_id` calls it rather than
/// restating the characters, so a second copy can never drift from the
/// sanitizer that would have rewritten them.
///
/// The set is deliberately **platform-independent**. `<`, `>`, `:`, `"`, `|`,
/// `?`, and `*` are legal filename characters on Linux and macOS, but they are
/// illegal on Windows, and `SANITIZED_RESERVED_CHARS` rewrites them on *every*
/// platform. A platform-dependent rule would therefore agree with the
/// filesystem on the host running it and disagree with the sanitizer that
/// actually produces the on-disk spelling — which is the identity split the
/// media-id rule exists to prevent. Matching the sanitizer is the priority.
pub(crate) fn is_rejected_char(ch: char) -> bool {
    matches!(ch, '<' | '>' | ':' | '"' | '|' | '?' | '*' | '/' | '\\')
}

/// Returns whether one character is a control character.
///
/// Rejected under every [`SanitizePolicy`]: there is no fix, and a control
/// character in a filename is a filesystem-portability hazard on every
/// platform rather than a single-tool artifact. This check is new in
/// [`PathComponent::parse`] — the pre-existing chain did not cover it.
fn is_control_char(ch: char) -> bool {
    ch.is_control()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_path_component() {
        let parsed = PathComponent::parse("normal", &SanitizePolicy::disabled()).unwrap();
        assert_eq!(components_to_strings(&[parsed]), vec!["normal".to_string()]);
    }

    #[test]
    fn parse_empty_component() {
        let err = PathComponent::parse("", &SanitizePolicy::disabled()).unwrap_err();
        assert!(err.to_string().contains("must not be empty"));
    }

    #[test]
    fn parse_dot_component() {
        let err = PathComponent::parse(".", &SanitizePolicy::disabled()).unwrap_err();
        assert!(err.to_string().contains("must not be '.' or '..'"));
    }

    #[test]
    fn parse_dotdot_component() {
        let err = PathComponent::parse("..", &SanitizePolicy::disabled()).unwrap_err();
        assert!(err.to_string().contains("must not be '.' or '..'"));
    }

    #[test]
    fn parse_component_with_separator() {
        // The traversal guard: a component that smuggled a separator would
        // re-split into extra path components under `Path::join`.
        let err = PathComponent::parse("AC/DC", &SanitizePolicy::disabled()).unwrap_err();
        assert!(err.to_string().contains("forbidden characters"));
    }

    #[test]
    fn parse_reserved_less_than() {
        let err = PathComponent::parse("a<b", &SanitizePolicy::disabled()).unwrap_err();
        assert!(err.to_string().contains("forbidden characters"));
    }

    #[test]
    fn parse_reserved_question() {
        let err = PathComponent::parse("a?b", &SanitizePolicy::disabled()).unwrap_err();
        assert!(err.to_string().contains("forbidden characters"));
    }

    #[test]
    fn parse_control_character_is_rejected_under_every_policy() {
        let err = PathComponent::parse("a\u{7}b", &SanitizePolicy::disabled()).unwrap_err();
        assert!(err.to_string().contains("control characters"));
        let err = PathComponent::parse(
            "a\u{7}b",
            &SanitizePolicy::with_replacements(&BTreeMap::from([('<', '_')]), None),
        )
        .unwrap_err();
        assert!(err.to_string().contains("control characters"));
    }

    #[test]
    fn parse_normalizes_nfc_input_to_nfd() {
        // "café" in NFC (`e` + U+00E9 precomposed) is normalized rather than
        // rejected: the parser is the fix stage for metadata the user cannot
        // edit, so it rewrites instead of refusing.
        let parsed = PathComponent::parse("caf\u{00e9}", &SanitizePolicy::disabled()).unwrap();
        assert_eq!(components_to_strings(&[parsed]), vec!["cafe\u{301}".to_string()]);
    }

    #[test]
    fn parse_relative_path_splits_backslash_form() {
        let parsed = parse_relative_path_components(
            Path::new("..\\..\\evil.txt"),
            &SanitizePolicy::disabled(),
        )
        .unwrap_err();
        assert!(parsed.to_string().contains("must not be '.' or '..'"));
    }

    #[test]
    fn parse_relative_path_joins_nested_members() {
        let parsed =
            parse_relative_path_components(Path::new("a/b/c.txt"), &SanitizePolicy::disabled())
                .unwrap();
        assert_eq!(join_path_components(&parsed), PathBuf::from("a/b/c.txt"));
    }

    #[test]
    fn sanitize_path_component_replaces_reserved() {
        let replacements = BTreeMap::from([('<', '_'), ('>', '_')]);
        let result = sanitize_path_component("a<b>c", &replacements);
        assert_eq!(result, "a_b_c");
    }

    #[test]
    fn sanitize_path_component_passes_through_normal() {
        let result = sanitize_path_component("hello", &BTreeMap::new());
        assert_eq!(result, "hello");
    }

    #[test]
    fn sanitize_and_validate_components_disabled() {
        let components = vec!["a<b".to_string()];
        let err = sanitize_and_validate_components(
            &components,
            &SanitizeNamesConfig::Disabled,
            &BTreeMap::new(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("forbidden characters"));
    }

    #[test]
    fn sanitize_and_validate_components_enabled() {
        let components = vec!["a<b".to_string()];
        let replacements = BTreeMap::from([('<', '_')]);
        let result = sanitize_and_validate_components(
            &components,
            &SanitizeNamesConfig::Enabled,
            &replacements,
        )
        .unwrap();
        assert_eq!(components_to_strings(&result), vec!["a_b".to_string()]);
    }

    #[test]
    fn sanitize_and_validate_components_custom_layers_over_defaults() {
        let components = vec!["a<b:c".to_string()];
        let defaults = BTreeMap::from([('<', '_'), (':', '#')]);
        let custom = BTreeMap::from([(':', '%')]);
        let result = sanitize_and_validate_components(
            &components,
            &SanitizeNamesConfig::Custom(custom),
            &defaults,
        )
        .unwrap();
        assert_eq!(components_to_strings(&result), vec!["a_b%c".to_string()]);
    }

    #[test]
    fn readonly_file() {
        let dir = mediapm_utils::temp::artifact_dir().unwrap();
        let file_path = dir.path().join("test.txt");
        std::fs::write(&file_path, b"data").unwrap();

        ensure_managed_path_readonly(&file_path).unwrap();

        assert!(file_path.metadata().unwrap().permissions().readonly());
    }

    #[test]
    fn readonly_directory() {
        let dir = mediapm_utils::temp::artifact_dir().unwrap();
        let sub = dir.path().join("subdir");
        std::fs::create_dir(&sub).unwrap();
        let child = sub.join("child.txt");
        std::fs::write(&child, b"child").unwrap();

        ensure_managed_path_readonly(&sub).unwrap();

        assert!(sub.metadata().unwrap().permissions().readonly());
        assert!(child.metadata().unwrap().permissions().readonly());

        // The `TempDir` guard's plain `remove_dir_all` silently fails on the
        // readonly-marked subtree (on Unix, unlinking `child.txt` requires
        // write on its parent `sub`), which would leak the tempdir from
        // `$TMPDIR`. Clear the bits via the retry helper so the guard has
        // nothing left to remove.
        mediapm_utils::temp::remove_dir_all_with_retry(dir.path())
            .expect("cleanup of readonly-marked tree");
    }

    #[test]
    fn remove_file() {
        let dir = mediapm_utils::temp::artifact_dir().unwrap();
        let file_path = dir.path().join("toremove.txt");
        std::fs::write(&file_path, b"data").unwrap();

        remove_path(&file_path).unwrap();

        assert!(!file_path.exists());
    }

    #[test]
    fn remove_dir() {
        let dir = mediapm_utils::temp::artifact_dir().unwrap();
        let sub = dir.path().join("subdir");
        std::fs::create_dir(&sub).unwrap();
        std::fs::write(sub.join("a.txt"), b"a").unwrap();
        std::fs::write(sub.join("b.txt"), b"b").unwrap();

        remove_path(&sub).unwrap();

        assert!(!sub.exists());
    }

    #[test]
    fn remove_readonly_file() {
        let dir = mediapm_utils::temp::artifact_dir().unwrap();
        let file_path = dir.path().join("readonly_remove.txt");
        std::fs::write(&file_path, b"data").unwrap();

        // Mark as readonly first.
        let mut perms = file_path.metadata().unwrap().permissions();
        perms.set_readonly(true);
        std::fs::set_permissions(&file_path, perms).unwrap();

        // Should still be removable.
        remove_path(&file_path).unwrap();
        assert!(!file_path.exists());
    }
}
