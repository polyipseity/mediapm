//! State serialization and migration.
//!
//! This module handles all JSON serialization of [`MediaPmState`], including
//! V2 wire format encoding, V1 migration decoding, and Nickel state file
//! migration.

pub mod ser;
