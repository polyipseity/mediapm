//! One validated filesystem path component, and the policy its parser applies.
//!
//! `PathComponent` is the crate's only way to hold a single component of a
//! library path that has been checked against the rules a committed file name
//! has to satisfy: NFD spelling, no traversal, no separator, no control
//! character. It lives at the crate root because two unrelated callers need
//! it. `config::hierarchy_types` keeps parsed components on
//! `FlattenedHierarchyEntry`, and `materializer::commit` and
//! `materializer::zip_reader` parse new ones out of untrusted text. Putting it in
//! either of those modules would invert the layering, since
//! `materializer::commit` sits downstream of `config`.
//!
//! The type has one constructor, [`PathComponent::parse`], and no `Deref`, no
//! `AsRef<str>`, no public field, and no `From<String>`. Holding one means the
//! text went through the parser. Reading the value back is possible through
//! [`Display`](std::fmt::Display) and nothing else.
//!
//! The type exists because validation a caller can forget is not validation.
//! The two untrusted-input paths, resolved hierarchy components and extracted
//! ZIP member names, each used to carry a `Vec<String>` and each relied on the
//! caller running the right checks in the right order. A ZIP member named
//! `../evil.txt` reached `Path::join` with the `..` still live.
//!
//! `SanitizePolicy` covers the one violation class that has a fix: the
//! reserved characters that `materializer::commit` rewrites under
//! `SanitizeNamesConfig::Enabled` and refuses under `Disabled`. Every other
//! class is rejected under every policy.

use std::collections::BTreeMap;
use std::fmt;

use unicode_normalization::UnicodeNormalization;

use crate::error::MediaPmError;

/// Applies a reserved-character replacement map to a single path component.
///
/// This operates on individual characters within one path component, not on a
/// joined path string, so `/` and `\` within a component are properly
/// replaced rather than consumed as structural separators. Characters absent
/// from `replacements` pass through unchanged.
#[must_use]
pub(crate) fn sanitize_path_component(
    component: &str,
    replacements: &BTreeMap<char, char>,
) -> String {
    component.chars().map(|ch| replacements.get(&ch).copied().unwrap_or(ch)).collect()
}

/// Fix-versus-reject policy applied by [`PathComponent::parse`].
///
/// The policy governs only the violation class that has a fix: reserved
/// characters are rewritten to their replacement when the policy carries one,
/// and rejected when it does not. NFD normalization is unconditional and
/// therefore not policy-governed. The unfixable classes — empty, `.`, `..`,
/// path separators, and control characters — are rejected under every policy.
#[derive(Debug, Clone, Default)]
pub(crate) struct SanitizePolicy {
    /// Reserved-character replacements. An empty map means "reject, do not
    /// rewrite", which is what `SanitizeNamesConfig::Disabled` selects.
    replacements: BTreeMap<char, char>,
}

impl SanitizePolicy {
    /// Returns the policy that rejects every reserved character without
    /// rewriting it. This is the policy the ZIP extraction path uses: an
    /// archive's member names are not the user's to rename, so silently
    /// rewriting one would materialize a file the user never asked for.
    pub(crate) fn disabled() -> Self {
        Self { replacements: BTreeMap::new() }
    }

    /// Returns the policy that rewrites reserved characters, layering
    /// `custom` over `default_replacements`.
    pub(crate) fn with_replacements(
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

/// One validated filesystem path component.
///
/// Constructed **only** by [`PathComponent::parse`]. There is deliberately no
/// `Deref`, no `AsRef<str>`, no public field, and no `From<String>`, so raw
/// untrusted text cannot reach a `PathBuf::join` without passing the parser
/// first. The untrusted-input paths that feed filesystem joins — resolved
/// hierarchy components and extracted ZIP member names — both go through it.
///
/// The parser:
/// 1. NFD-normalizes the input (unconditional; mediapm commits NFD-only names),
/// 2. applies `policy` to the reserved characters, rewriting or rejecting,
/// 3. rejects the unfixable classes: empty, `.`, `..`, embedded separators,
///    and control characters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PathComponent(String);

impl PathComponent {
    /// Parses one untrusted component under `policy`.
    ///
    /// # Errors
    ///
    /// Returns the first [`MediaPmError::Workflow`] from the invariant checks,
    /// naming the offending component and the violated rule.
    pub(crate) fn parse(raw: &str, policy: &SanitizePolicy) -> Result<Self, MediaPmError> {
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

/// Writes the stored, already-validated component.
///
/// This is the one way to read the inner value back into text. It is a
/// rendering, not a conversion: the result is a component that came out of
/// [`PathComponent::parse`], so it is still safe to join into a path.
impl fmt::Display for PathComponent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Renders validated components as one `/`-separated relative path.
///
/// Display text for progress labels, managed-file keys, the stale-path set,
/// and playlist bodies. The argument type is the point: there is no
/// `&str`-accepting form, so the only values a caller can render are
/// components that met [`PathComponent::parse`], and the rendered string
/// describes the same safe path the components do.
///
/// The materializer builds the path it writes to disk from the components
/// themselves (`materializer::commit::join_path_components`). Rendering a
/// validated component cannot reintroduce a separator, because a component
/// holding one could not have been parsed in the first place.
#[must_use]
pub(crate) fn render_relative_path(components: &[PathComponent]) -> String {
    components.iter().map(ToString::to_string).collect::<Vec<String>>().join("/")
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
/// The set doubles as the traversal guard: `/` and `\` are rejected, so a
/// component that smuggled a separator (for example an artist tag `AC/DC`)
/// can never re-split into extra path components, and an absolute component
/// can never anchor outside the hierarchy root. `.` and `..` are rejected by
/// [`check_component`] before this predicate is consulted.
///
/// `pub(crate)` because it is the **shared** definition of the set: the
/// config-level rules in `crate::config::hierarchy_types` — the media-id rule
/// and the hierarchy-component rule — call it rather than restating the
/// characters, so neither can drift from the sanitizer that would have
/// rewritten them. `materializer::commit` carries the same nine characters
/// as `SANITIZED_RESERVED_CHARS` for the rewrite map; a unit test there pins
/// the pair so neither copy can move alone.
///
/// The set is deliberately **platform-independent**. `<`, `>`, `:`, `"`, `|`,
/// `?`, and `*` are legal filename characters on Linux and macOS, but they are
/// illegal on Windows, and the rewrite map rewrites them on *every* platform.
/// A platform-dependent rule would therefore agree with the filesystem on the
/// host running it and disagree with the sanitizer that actually produces the
/// on-disk spelling — which is the identity split the media-id rule exists to
/// prevent. Matching the sanitizer is the priority.
pub(crate) fn is_rejected_char(ch: char) -> bool {
    matches!(ch, '<' | '>' | ':' | '"' | '|' | '?' | '*' | '/' | '\\')
}

/// Returns whether one character is a control character.
///
/// Rejected under every [`SanitizePolicy`]: there is no fix, and a control
/// character in a filename is a filesystem-portability hazard on every
/// platform rather than a single-tool artifact.
fn is_control_char(ch: char) -> bool {
    ch.is_control()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A component with no reserved character survives the parser unchanged,
    /// and [`Display`] renders it back to the same text.
    #[test]
    fn parse_valid_path_component() {
        let parsed = PathComponent::parse("normal", &SanitizePolicy::disabled()).unwrap();
        assert_eq!(parsed.to_string(), "normal");
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

    /// NFC input is normalized rather than rejected. The parser is the fix
    /// stage for metadata the user cannot edit, so it rewrites instead of
    /// refusing.
    #[test]
    fn parse_normalizes_nfc_input_to_nfd() {
        let parsed = PathComponent::parse("caf\u{00e9}", &SanitizePolicy::disabled()).unwrap();
        assert_eq!(parsed.to_string(), "cafe\u{301}");
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
}
