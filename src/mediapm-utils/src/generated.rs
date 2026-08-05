//! Shared generated-file banner for machine-written configuration documents.
//!
//! Every document that mediapm (or standalone mediapm-conductor) rewrites
//! programmatically is stamped with the same banner so users can identify
//! machine-owned artifacts at a glance and know that manual edits are
//! overwritten on the next write. The banner is a sequence of `#` comment
//! lines, which Nickel's evaluator ignores, so prepending it never affects
//! document decoding and `encode → decode → encode` stays byte-stable.

/// The banner prepended to every machine-generated configuration document.
///
/// Written as `#` comment lines so it is inert for the Nickel evaluator.
/// Keep this constant single-sourced: every encode path delegates to
/// [`prepend_banner`], which uses it verbatim, so the stamped text is
/// identical across all consumers.
pub const GENERATED_FILE_BANNER: &str = "\
# ===========================================================================
# GENERATED FILE - DO NOT EDIT
#
# This document is machine-generated and rewritten in full on every sync.
# Manual edits are overwritten and lost.
#
# User-owned configuration belongs in your hand-edited document
# (`mediapm.conductor.ncl` in mediapm workspaces, `conductor.ncl` in
# standalone mediapm-conductor).
# ===========================================================================
";

/// Prepends the shared generated-file banner to `bytes`.
///
/// Idempotent: when `bytes` already starts with [`GENERATED_FILE_BANNER`],
/// it is returned unchanged, so re-stamping an already-bannered document
/// does not duplicate the banner.
#[must_use]
pub fn prepend_banner(bytes: &[u8]) -> Vec<u8> {
    if bytes.starts_with(GENERATED_FILE_BANNER.as_bytes()) {
        return bytes.to_vec();
    }
    let mut out = Vec::with_capacity(GENERATED_FILE_BANNER.len() + bytes.len());
    out.extend_from_slice(GENERATED_FILE_BANNER.as_bytes());
    out.extend_from_slice(bytes);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn banner_is_comment_lines_terminated_by_newline() {
        assert!(GENERATED_FILE_BANNER.starts_with("# "), "banner must be `#` comment lines");
        assert!(GENERATED_FILE_BANNER.ends_with('\n'), "banner must end with a newline");
    }

    #[test]
    fn prepend_banner_stamps_empty_input() {
        assert_eq!(prepend_banner(b""), GENERATED_FILE_BANNER.as_bytes().to_vec());
    }

    #[test]
    fn prepend_banner_stamps_before_content() {
        let content = b"version = 2\n";
        let stamped = prepend_banner(content);
        assert!(stamped.starts_with(GENERATED_FILE_BANNER.as_bytes()));
        assert!(stamped.ends_with(content));
        assert_eq!(stamped.len(), GENERATED_FILE_BANNER.len() + content.len());
    }

    #[test]
    fn prepend_banner_is_idempotent() {
        let once = prepend_banner(b"version = 2\n");
        let twice = prepend_banner(&once);
        assert_eq!(once, twice, "re-stamping must not duplicate the banner");
    }
}
