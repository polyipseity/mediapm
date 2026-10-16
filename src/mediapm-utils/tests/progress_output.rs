// The `progress` fixtures keep the handles that own a bar's screen slot bound
// under an underscore prefix so a bar stays attached until the frame it pins is
// asserted, and Task 6's opt-in means those bindings now make calls
// (`.set_animating(true)`). The lint only exists to catch a read of a binding
// meant to be dropped; here the read is the point.
#![allow(clippy::used_underscore_binding)]
#[path = "progress_output/mod.rs"]
mod progress_output;
