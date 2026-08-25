//! Graphical progress bar internals (feature `progress` only).
//!
//! This module groups the always-feature-gated rendering machinery: the terminal
//! wrapper, dimension/time sources, prefix/suffix components, the tracked-handle
//! state model, and the combined [`ProgressGroup`].

mod components;
mod debug;
mod group;
mod renderer;
mod sources;

pub use components::{
    DebugSlotState, DebugTickSnapshot, PrefixComponents, ProgressDebugSink, SuffixComponents,
};
pub use debug::{
    DimensionSource, RealTerminalSource, RealTimeSource, TestDimensionSource, TestTimeSource,
    TimeSource,
};
pub use group::{HasOverall, NoOverall, ProgressGroup, ProgressGroupBuilder};
pub use renderer::{ProgressRenderer, TrackSnapshot, TrackStatus, TrackedHandle};

pub(crate) use debug::{BufferGuard, detect_progress_debug_env};
pub(crate) use sources::BufferedTerm;
// `strip_ansi`/`visible_width` are re-exported so `progress::tests` can reach
// them via the `super::inner::strip_ansi` path (the `components` submodule is
// private, so the path only resolves through this re-export). Path-based access
// does not satisfy the `unused_imports` lint, hence the allow.
#[allow(unused_imports)]
pub(crate) use components::{
    MAX_SLOTS, apply_bar_style, apply_done_bar_style, apply_failed_bar_style,
    apply_overall_bar_style, bar_color_code, blank_bar_style, format_count, format_elapsed,
    format_eta, format_rate, max_prefix_width, max_suffix_width, prefix_components_from_str,
    render_prefix_components, render_suffix_components, semantic_truncate_prefix,
    semantic_truncate_suffix, strip_ansi, visible_width,
};
pub(crate) use renderer::SharedState;
