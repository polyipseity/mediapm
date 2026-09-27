//! The unified `mediapm.ncl` document model.
//!
//! [`MediaPmDocument`] is the single type every `mediapm.ncl` on disk decodes
//! into, for every schema version: the Nickel ladder in `v1.ncl`/`v2.ncl`
//! migrates a document *up* to the active version first, and only then does
//! this type see it. It therefore carries a version marker and a
//! `runtime` field holding the active `*Latest` boundary value, which is why it
//! lives here rather than in `config/mod.rs`: naming that boundary type is
//! exactly what `config/mod.rs` is forbidden to do.
//!
//! The name itself is unversioned, so callers outside `versions/` may name
//! `MediaPmDocument` freely — through the `pub use` in `config/mod.rs`, which
//! is the crate's usual facade for its config types. What they may not do is
//! reach the boundary value hanging off [`MediaPmDocument::runtime`], which is
//! reached through the unversioned entry points in the parent module.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - This file declares no resolved (`option`-free) type. Everything it
//!   declares is wire-shaped, which is the placement rule the whole
//!   `versions/` directory exists to enforce.
//! - Do not add a `decode`/`migrate_to` mirror of the Nickel ladder here.
//!   Version dispatch is Nickel; two implementations of the same migrations
//!   would drift, and the Nickel one is the one every on-disk document has
//!   actually passed through.
//! - Do not re-export anything from this file. Callers reach
//!   [`MediaPmDocument`] through `super`'s `pub use`, so a second route out
//!   would only widen the boundary.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::v_latest::MediaRuntimeStorageLatest;
use crate::config::{
    ConfigVersionSpec, MediaPmState, ToolRequirement, defaults, hierarchy_types, source_types,
};

/// Top-level mediapm document deserialized from `mediapm.ncl`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaPmDocument {
    /// Schema version marker.
    #[serde(default = "defaults::default_mediapm_document_version")]
    pub version: u32,
    /// Media source entries keyed by unique id.
    #[serde(default)]
    pub media: BTreeMap<String, source_types::MediaSourceSpec>,
    /// Hierarchy declaration.
    #[serde(default)]
    pub hierarchy: Vec<hierarchy_types::HierarchyNode>,
    /// Managed tool requirement declarations keyed by tool id.
    #[serde(default)]
    pub tools: BTreeMap<String, ToolRequirement>,
    /// Runtime configuration overrides.
    #[serde(default)]
    pub runtime: MediaRuntimeStorageLatest,
    /// Legacy `state` payload accepted for V1 documents.
    ///
    /// State is managed separately via `state.json`; the V2 schema drops this
    /// field, so it is accepted on read for legacy documents and never
    /// emitted on V2 writes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<MediaPmState>,
}

impl Default for MediaPmDocument {
    fn default() -> Self {
        Self {
            version: defaults::MEDIAPM_DOCUMENT_VERSION,
            media: BTreeMap::new(),
            hierarchy: Vec::new(),
            tools: BTreeMap::new(),
            runtime: MediaRuntimeStorageLatest::default(),
            state: None,
        }
    }
}

impl MediaPmDocument {
    /// Normalizes string fields (trimming whitespace).
    pub fn normalize(&mut self) {
        for source in self.media.values_mut() {
            let trimmed = source.description.trim().to_string();
            source.description = trimmed;
            let trimmed = source.title.trim().to_string();
            source.title = trimmed;
            let trimmed = source.artist.trim().to_string();
            source.artist = trimmed;
        }
        // Remove tool entries that are Latest with no explicit dependencies.
        self.tools.retain(|_, tool_req| {
            tool_req.version_spec != ConfigVersionSpec::Latest || !tool_req.dependencies.is_empty()
        });
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MediaSourceSpec;

    /// Builds a minimal source whose description, title, and artist carry
    /// surrounding whitespace, so `normalize` has something to trim.
    fn padded_source() -> MediaSourceSpec {
        MediaSourceSpec {
            description: "  a description  ".to_string(),
            title: "\tthe title\n".to_string(),
            artist: "  the artist  ".to_string(),
            ..MediaSourceSpec::default()
        }
    }

    /// Verifies `normalize` trims the user-visible text fields.
    ///
    /// Untrimmed values reach the hierarchy as metadata substitutions and as
    /// literal path components, so whitespace that survives load time becomes
    /// part of the materialized path.
    #[test]
    fn normalize_trims_source_text_fields() {
        let mut document = MediaPmDocument {
            media: BTreeMap::from([("m1".to_string(), padded_source())]),
            ..MediaPmDocument::default()
        };
        document.normalize();

        let source = &document.media["m1"];
        assert_eq!(source.description, "a description");
        assert_eq!(source.title, "the title");
        assert_eq!(source.artist, "the artist");
    }

    /// Verifies `normalize` drops a bare-`latest` tool entry but keeps one
    /// that declares dependencies.
    ///
    /// A tool requirement with no version and no dependencies carries no
    /// information, so keeping it would pin an entry that says nothing; one
    /// that declares dependencies says something and must survive.
    #[test]
    fn normalize_drops_bare_latest_tools_but_keeps_declared_dependencies() {
        let bare = ToolRequirement {
            version_spec: ConfigVersionSpec::Latest,
            dependencies: BTreeMap::new(),
            recheck_seconds: 0,
            max_input_slots: 16,
            max_output_slots: 4,
        };
        let with_deps = ToolRequirement {
            dependencies: BTreeMap::from([("ffmpeg".to_string(), ConfigVersionSpec::Latest)]),
            ..bare.clone()
        };
        let mut document = MediaPmDocument {
            tools: BTreeMap::from([
                ("bare".to_string(), bare),
                ("with_deps".to_string(), with_deps),
            ]),
            ..MediaPmDocument::default()
        };
        document.normalize();

        assert!(!document.tools.contains_key("bare"), "a bare latest entry carries no information");
        assert!(document.tools.contains_key("with_deps"), "a declared dependency must survive");
    }
}
