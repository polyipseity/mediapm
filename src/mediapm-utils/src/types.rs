//! Shared type aliases for mediapm builtin crates.

use std::collections::BTreeMap;

/// Canonical string-map payload used by builtin API and CLI contracts.
///
/// All argument values are strings; binary payloads use [`BinaryInputMap`].
pub type StringMap = BTreeMap<String, String>;

/// Canonical binary-input payload map used by builtin API execution.
///
/// Conductor runtime may provide binary payloads (file contents, archive bytes)
/// as a map keyed by input name.
pub type BinaryInputMap = BTreeMap<String, Vec<u8>>;
