#[allow(unused_imports)]
use super::super::inner::*;
#[allow(unused_imports)]
use super::*;

// ---- ANSI-safe truncation helpers (Phase 1) -------------------------

#[test]
fn strip_ansi_empty() {
    assert_eq!(super::super::inner::strip_ansi(""), "");
}

#[test]
fn strip_ansi_no_escapes() {
    assert_eq!(super::super::inner::strip_ansi("hello world"), "hello world");
}

#[test]
fn strip_ansi_reset() {
    assert_eq!(super::super::inner::strip_ansi("\x1b[0m"), "");
}

#[test]
fn strip_ansi_multiple() {
    assert_eq!(super::super::inner::strip_ansi("\x1b[31mfoo\x1b[0m"), "foo");
    assert_eq!(super::super::inner::strip_ansi("\x1b[33m[F]\x1b[0m bar"), "[F] bar");
}

#[test]
fn strip_ansi_non_sgr_ignored() {
    // Non-SGR escape sequences (not ending with 'm') are passed through.
    assert_eq!(super::super::inner::strip_ansi("\x1b[2J"), "\x1b[2J");
    assert_eq!(super::super::inner::strip_ansi("a\x1b[Kb"), "a\x1b[Kb");
}

#[test]
fn visible_width_empty() {
    assert_eq!(super::super::inner::visible_width(""), 0);
}

#[test]
fn visible_width_no_ansi() {
    assert_eq!(super::super::inner::visible_width("hello"), 5);
}

#[test]
fn visible_width_with_ansi() {
    assert_eq!(super::super::inner::visible_width("\x1b[31mhello\x1b[0m"), 5);
    assert_eq!(super::super::inner::visible_width("\x1b[33m[F]\x1b[0m wget"), 8);
}

#[test]
fn visible_width_ansi_only() {
    assert_eq!(super::super::inner::visible_width("\x1b[0m"), 0);
    assert_eq!(super::super::inner::visible_width("\x1b[31m\x1b[33m"), 0);
}

#[test]
fn max_prefix_width_returns_constant() {
    assert_eq!(super::super::inner::max_prefix_width(), 40);
}

#[test]
fn max_suffix_width_returns_constant() {
    assert_eq!(super::super::inner::max_suffix_width(), 50);
}
