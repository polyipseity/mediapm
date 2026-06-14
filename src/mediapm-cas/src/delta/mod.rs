//! CAS codec layer for delta and object-wire encoding.
//!
//! The codec layer keeps object payload representation concerns isolated from
//! storage orchestration logic:
//! - [`patch`] handles VCDIFF patch bytes used by delta-object payloads.
//! - [`object`] handles `.diff` envelope framing for persisted delta objects.
//!
//! ## DO NOT REMOVE: external versions boundary guard
//!
//! Files outside `delta/versions/` must interact with versioned envelopes only
//! through `delta::versions` (`versions/mod.rs`) APIs, never via
//! `delta::versions::vX` imports.

pub(crate) mod object;
pub(crate) mod patch;
mod versions;
