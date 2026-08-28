//! CAS codec layer for delta and object-wire encoding.
//!
//! Keeps payload representation isolated from storage orchestration: [`patch`]
//! handles VCDIFF patch bytes, [`object`] handles `.diff` envelope framing.
//! Files outside `delta/versions/` must use versioned envelopes only through
//! `delta::versions` APIs, never `delta::versions::vX` imports.

pub(crate) mod object;
pub(crate) mod patch;
mod versions;
