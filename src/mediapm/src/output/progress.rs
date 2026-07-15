//! Progress bar rendering for long-running operations.
//!
//! This module re-exports types from `mediapm_utils::progress` to maintain
//! the existing `crate::output::progress::*` import paths within the mediapm
//! crate.  See `mediapm_utils::progress` for full documentation.

#[doc(inline)]
pub use mediapm_utils::progress::{
    DimensionSource, ProgressBarApi, ProgressGroup, ProgressGroupApi, TestDimensionSource,
    TestTimeSource, TimeSource, TrackedHandle,
};
