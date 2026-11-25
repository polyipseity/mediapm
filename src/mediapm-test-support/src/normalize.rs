//! Shared normalization for comparisons that must hold across workspaces and
//! across checkouts.
//!
//! The conductor's generated document names the directory each tool payload
//! was materialized into, and that path starts with the workspace root the
//! run used. Two runs in different workspaces therefore produce different
//! bytes for reasons that have nothing to do with tools, so both tests set
//! the absolute path aside before comparing. The closure that did it was
//! written twice; it lives here once because one of the callers is an
//! integration test, which cannot reach a helper compiled only under
//! `cfg(test)` inside the library.
//!
//! Line endings are the other cross-host spelling difference: `.gitattributes`
//! pins `eol` for markdown and shell scripts only, so JSON, TOML, and Nickel
//! working-tree copies carry the host's line ending. A test that compares
//! repository text against generated text normalizes through
//! [`normalize_line_endings`] instead of asserting the checkout.

use std::path::Path;

/// Placeholder every workspace root is replaced with.
///
/// Named so the assertion that the replacement happened, and the comparison
/// that follows it, agree on one spelling.
pub const WORKSPACE_PLACEHOLDER: &str = "<workspace>";

/// Replaces every spelling of `root` in `document` with the placeholder.
///
/// Three spellings are replaced because a generated Nickel document can name
/// the root exactly as the host writes it, with forward slashes, or with the
/// backslashes JSON escapes doubled. The forward-slash and doubled-backslash
/// passes are skipped when they are identical to the plain one, so a POSIX
/// root is not rewritten three times.
///
/// # Panics
///
/// Panics when `document` is not UTF-8, or when the result does not contain
/// the placeholder because the document never named its root. The missing-root
/// case is a caller error: letting it reach `assert_eq!` would report a
/// bytes-differ message instead of the real cause.
#[must_use]
pub fn normalize_workspace_root(document: &[u8], root: &Path) -> String {
    let text = String::from_utf8(document.to_vec()).expect("the generated document is UTF-8");
    let root_text = root.to_string_lossy();
    let mut normalized = text.replace(root_text.as_ref(), WORKSPACE_PLACEHOLDER);
    let forward = root_text.replace('\\', "/");
    if forward != root_text.as_ref() {
        normalized = normalized.replace(&forward, WORKSPACE_PLACEHOLDER);
    }
    let json_escaped = root_text.replace('\\', "\\\\");
    if json_escaped != root_text.as_ref() {
        normalized = normalized.replace(&json_escaped, WORKSPACE_PLACEHOLDER);
    }
    assert!(
        normalized.contains(WORKSPACE_PLACEHOLDER),
        "the generated document has to name the workspace root it was written in",
    );
    normalized
}

/// Collapses CRLF to LF so a comparison can hold across checkouts.
///
/// `.gitattributes` pins `eol` for markdown and shell scripts only, so a
/// Windows checkout gives JSON, TOML, and Nickel files CRLF endings while a
/// POSIX checkout gives LF. A test that compares repository text against
/// generated text must normalize first, or it asserts the checkout's line
/// endings instead of the content.
#[must_use]
pub fn normalize_line_endings(text: &str) -> String {
    text.replace("\r\n", "\n")
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{WORKSPACE_PLACEHOLDER, normalize_line_endings, normalize_workspace_root};

    /// Both checkouts of the same file normalize to the same string, and a
    /// bare CR survives, since it is content rather than a line ending.
    #[test]
    fn normalize_line_endings_collapses_only_crlf() {
        assert_eq!(normalize_line_endings("a\r\nb\r\n"), "a\nb\n");
        assert_eq!(normalize_line_endings("a\nb\n"), "a\nb\n");
        assert_eq!(
            normalize_line_endings("a\rb"),
            "a\rb",
            "a lone carriage return is content, not a line ending"
        );
        assert_eq!(
            normalize_line_endings("a\r\nb\nc\r\n"),
            "a\nb\nc\n",
            "a mixed file normalizes every CRLF pair it holds"
        );
    }

    /// The plain spelling is the one every platform produces, so it is the
    /// case that has to work everywhere.
    #[test]
    fn the_host_spelling_is_replaced() {
        let document = br#"root = "/repo/work""#;
        assert_eq!(
            normalize_workspace_root(document, Path::new("/repo/work")),
            format!(r#"root = "{WORKSPACE_PLACEHOLDER}""#)
        );
    }

    /// A Windows root reaches the document in both the backslash spelling and
    /// the forward-slash one, and a comparison that normalized only the first
    /// would pass on Linux and fail on Windows. The test runs on every host
    /// because the root is a constructed path, not a real one.
    #[test]
    fn both_windows_spellings_are_replaced() {
        let document = br#"a = "C:\\ws\\x"
b = "C:/ws/x"
"#;
        let got = normalize_workspace_root(document, Path::new("C:\\ws\\x"));
        assert!(!got.contains("C:"), "no spelling of the root may survive: {got}");
        assert_eq!(got.matches(WORKSPACE_PLACEHOLDER).count(), 2, "both spellings: {got}");
    }

    /// JSON escapes a backslash, so the same root can appear doubled. A pass
    /// that stopped at the single-backslash spelling would leave it behind.
    ///
    /// The literal carries two backslashes per separator, which is what
    /// `root.replace('\\', "\\\\")` produces for a root with one backslash
    /// per separator; writing four here would test a document the helper is
    /// never handed.
    #[test]
    fn the_json_escaped_spelling_is_replaced() {
        let document = br#"{"root":"C:\\ws\\x"}"#;
        let got = normalize_workspace_root(document, Path::new("C:\\ws\\x"));
        assert!(!got.contains("C:"), "the JSON-escaped spelling may not survive: {got}");
    }

    /// A document that never names its root is a caller error, not a
    /// comparison result, so it panics with a message naming the cause.
    #[test]
    #[should_panic(expected = "has to name the workspace root")]
    fn a_document_without_the_root_panics() {
        let _ = normalize_workspace_root(b"root = \"somewhere else\"", Path::new("/repo/work"));
    }
}
