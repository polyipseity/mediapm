//! The ANSI reset that leads a client-truncated prefix, and the clipped label that follows it.

use super::*;
use std::sync::Arc;

/// A client label that clips itself to the width the renderer grants,
/// standing in for a real client label whose field layout the renderer
/// never sees.
struct ClippedLabel {
    /// The label as the client renders it when width is not a constraint.
    text: String,
}

impl crate::progress::BarLabelTruncation for ClippedLabel {
    fn truncate_prefix(&self, max_width: usize) -> String {
        self.text.chars().take(max_width).collect()
    }

    /// Returns nothing: this test is about the prefix half.
    fn truncate_suffix(&self, _max_width: usize, _suffix: &SuffixComponents) -> String {
        String::new()
    }
}

/// The reset leads a client-truncated prefix, and what follows it is the
/// truncated label.
///
/// Two halves, because either alone is satisfiable by a broken wrapping: a
/// reset in front of the whole label would pass a check for the escape
/// while the label stayed unclipped, which is the state that overflows its
/// slot. So the wide case pins that the reset is there and the narrow case
/// pins that the text after it is the clipped one. The label is the test's
/// own, so each expectation is that label's text behind the escape and no
/// bar fill or spinner glyph is typed by hand.
#[test]
fn client_truncated_prefix_leads_with_the_reset() {
    let label: Arc<dyn crate::progress::BarLabelTruncation> =
        Arc::new(ClippedLabel { text: "hello world".to_owned() });

    // Width the label fits inside: the reset is the only thing added.
    assert_eq!(client_truncated_prefix(&label, 64), "\x1b[0mhello world");

    // Width that cuts the label: the reset precedes the clipped text, and
    // the clipped text is what the renderer gets, not the full label.
    assert_eq!(client_truncated_prefix(&label, 5), "\x1b[0mhello");
}
