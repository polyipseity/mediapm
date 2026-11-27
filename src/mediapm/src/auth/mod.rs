//! Unified credential storage and auth services for mediapm.
//!
//! One versioned `auth.json` holds every service credential. The versioned
//! wire records live in the private [`versions`] module; the unversioned
//! domain types and atomic file IO live in [`store`].

pub mod acoustid;
pub mod error;
pub mod musicbrainz;
pub mod prompt;
pub mod service;
pub mod store;
mod versions;
