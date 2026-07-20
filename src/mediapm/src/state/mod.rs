//! State persistence and migration.
//!
//! Public serialization API is in [`ser`] (thin delegation layer over
//! [`versions`] submodules). Version-specific wire formats for V1 and V2
//! live in [`versions::v1`] and [`versions::v2`].

pub mod ser;
pub mod versions;
