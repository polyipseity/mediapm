//! State persistence and migration.
//!
//! Public serialization API is in [`ser`], a thin delegation layer over
//! [`versions`]. `versions/` owns version dispatch and the per-version wire
//! formats; callers outside it never name a version module directly.

pub mod ser;
pub mod versions;
