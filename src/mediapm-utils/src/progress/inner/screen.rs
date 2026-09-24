//! Screen builder state types.
//!
//! [`NoOverall`] and [`HasOverall`] are marker types tracking whether a
//! builder has been given an overall bar yet.  They live in their own module
//! so both [`ProgressTerminal`](super::terminal::ProgressTerminal) and its
//! screen builder can name them without a module cycle.

/// Marker: the builder has no overall bar configured yet.
pub struct NoOverall;

/// Marker: the builder has an overall bar configured.
pub struct HasOverall;
