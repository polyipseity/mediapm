//! Graphical progress bar internals (feature `progress` only).
//!
//! This module groups the always-feature-gated rendering machinery: the terminal
//! wrapper, dimension/time sources, prefix/suffix components, the tracked-handle
//! state model, and the combined [`ProgressScreen`].

mod components;
mod debug;
mod gate;
mod plan;
mod renderer;
mod screen;
mod sources;
mod terminal;
#[cfg(test)]
mod tests_single_writer;
mod writer;

#[allow(unused_imports)]
pub use components::{
    DebugSlotState, DebugTickSnapshot, PrefixComponents, ProgressDebugSink, StatusCount,
    SuffixComponents,
};
pub use debug::{
    DimensionSource, RealTerminalSource, RealTimeSource, TestDimensionSource, TestTimeSource,
    TimeSource,
};
pub use renderer::{ProgressBarHandle, ProgressRenderer, TrackSnapshot, TrackStatus};
pub use screen::{HasOverall, NoOverall};
pub use terminal::{ProgressScreen, ProgressTerminal, ProgressTerminalBuilder};

pub(crate) use debug::detect_progress_debug_env;
// `renderer.rs` names `WriteGate` through the `super::WriteGate` path, so it is
// re-exported here.
pub(crate) use gate::WriteGate;
// `strip_ansi`/`visible_width` are re-exported so `progress::tests` can reach
// them via the `super::inner::strip_ansi` path (the `components` submodule is
// private, so the path only resolves through this re-export). Path-based access
// does not satisfy the `unused_imports` lint, hence the allow.
#[allow(unused_imports)]
pub(crate) use components::{
    FRAME_OVERHEAD_COLUMNS, MAX_PREFIX_WIDTH, MAX_SLOTS, MAX_SUFFIX_WIDTH, MIN_BAR_FILL,
    MIN_PREFIX_WIDTH, MIN_SUFFIX_WIDTH, PARKED_GLYPH, SPINNER_COLUMN, TICK_CHARS, bar_color_code,
    close_sgr_state, format_count, format_elapsed, format_eta, format_rate, max_prefix_width,
    max_suffix_width, prefix_components_from_str, render_prefix_components,
    render_suffix_components, semantic_truncate_prefix, semantic_truncate_suffix, strip_ansi,
    visible_width,
};
pub(crate) use renderer::SharedState;
// `FramePlan` and its row types are re-exported so renderer code and tests can
// reach them through the `inner::FramePlan` path without naming the private
// `plan` submodule. The allow mirrors the `components` re-export block above:
// the types are consumed by path in later tasks, which does not satisfy the
// `unused_imports` lint.
#[allow(unused_imports)]
pub(crate) use plan::{FramePlan, PlanRow, RenderStyle, RowAnimation};
