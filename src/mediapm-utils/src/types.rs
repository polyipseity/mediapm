//! Shared type aliases for mediapm builtin crates.

use std::collections::BTreeMap;

/// Canonical string-map payload for builtin API and CLI contracts.
pub type StringMap = BTreeMap<String, String>;

/// Canonical binary-input payload map for builtin API execution.
pub type BinaryInputMap = BTreeMap<String, Vec<u8>>;
