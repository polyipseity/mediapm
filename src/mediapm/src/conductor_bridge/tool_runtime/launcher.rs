//! media-tagger launcher binary path resolution.
//!
//! media-tagger requires platform-specific launcher scripts generated from
//! internal templates. This module provides the path resolution helpers used
//! during lifecycle management.

#![allow(dead_code)]
// TODO: Stream A stubs — wired when provisioning pipeline is complete.

use std::path::{Path, PathBuf};

/// Resolves the path to a media-tagger launcher script under the given
/// content-tools directory.
#[must_use]
pub(crate) fn resolve_media_tagger_launcher_binary_path(tools_dir: &Path) -> PathBuf {
    tools_dir.join("media-tagger-launcher")
}

/// Resolves a profile-adjacent `mediapm` binary path for tool discovery.
#[must_use]
pub(crate) fn resolve_profile_adjacent_mediapm_binary(current_exe: &Path) -> Option<PathBuf> {
    let exe_name = current_exe.file_name()?;

    if let Some(parent) = current_exe.parent() {
        // Check sibling directory (same profile).
        let sibling = parent.join(exe_name);
        if sibling.exists() {
            return Some(sibling);
        }

        // Check parent directory (profile-adjacent).
        if let Some(grandparent) = parent.parent() {
            let adjacent = grandparent.join(exe_name);
            if adjacent.exists() {
                return Some(adjacent);
            }
        }
    }

    None
}
