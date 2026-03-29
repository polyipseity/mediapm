//! CAS codec layer for delta and object-wire encoding.
//!
//! The codec layer keeps object payload representation concerns isolated from
//! storage orchestration logic:
//! - [`delta`] handles VCDIFF patch bytes used by delta-object payloads.
//! - [`object`] handles `.diff` envelope framing for persisted delta objects.
//!
//! ## DO NOT REMOVE: external versions boundary guard
//!
//! Files outside `codec/versions/` must interact with versioned envelopes only
//! through `codec::versions` (`versions/mod.rs`) APIs, never via
//! `codec::versions::vX` imports.

mod delta;
mod object;
mod versions;

pub(crate) use delta::DeltaPatch;
pub(crate) use object::StoredObject;
