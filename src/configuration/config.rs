//! Declarative configuration schema and IO.
//!
//! # Why configuration is a first-class layer
//!
//! `mediapm` is built around reconciliation, so users describe *intent* rather
//! than issuing imperative file operations. This file is the contract between
//! user intent and the planner.
//!
//! The current MVP uses JSON (`mediapm.json`) as the canonical user-facing
//! format. A compatibility path for `.ncl` exists only as a migration-friendly
//! stepping stone toward richer config evaluation in future iterations.

use std::{collections::BTreeMap, fs, path::Path};

use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Default filename searched by CLI commands.
pub const DEFAULT_CONFIG_FILE: &str = "mediapm.json";

/// Root declarative configuration model.
///
/// This type is intentionally stable and explicit because it is the durable
/// shape that planners, formatters, and tests all depend on.
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct AppConfig {
    /// Source declarations to import into the content-addressed store.
    #[serde(default)]
    pub sources: Vec<SourceDecl>,
    /// Explicit, user-declared desired links.
    #[serde(default)]
    pub links: Vec<LinkDecl>,
    /// Per-URI metadata overlays merged into normalized metadata.
    #[serde(default)]
    pub metadata_overrides: BTreeMap<String, Value>,
    /// Operational policies for linking and reconciliation.
    #[serde(default)]
    pub policies: Policies,
}

/// A source media declaration.
///
/// Sources represent identity roots for import operations. The same source URI
/// can later participate in multiple `links` declarations without duplicating
/// content in the object store.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceDecl {
    /// Source URI or path-like input.
    pub uri: String,
    /// Optional freeform user tags attached at declaration time.
    #[serde(default)]
    pub tags: BTreeMap<String, Value>,
}

/// A declared link from a URI to a filesystem path.
///
/// Links represent desired materialized views of stored media. Keeping links
/// explicit (instead of inferred) makes reconciliation deterministic and easy
/// to audit.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LinkDecl {
    /// Destination path where a materialized link-like artifact should exist.
    pub path: String,
    /// Source media URI/path declaration.
    pub from_uri: String,
    /// Variant selection strategy.
    #[serde(default)]
    pub select: VariantSelection,
}

/// Rules for choosing which variant to materialize.
///
/// Selection allows users to choose between policy-based targeting
/// (`prefer=...`) and exact identity targeting (`variant_hash=...`).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct VariantSelection {
    /// Generic preference strategy.
    #[serde(default)]
    pub prefer: SelectionPreference,
    /// Explicit variant hash override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variant_hash: Option<String>,
}

impl Default for VariantSelection {
    fn default() -> Self {
        Self { prefer: SelectionPreference::Latest, variant_hash: None }
    }
}

/// Built-in variant preference options.
///
/// These preferences exist so workflows can stay declarative even when exact
/// variant hashes are unknown or intentionally abstracted away.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum SelectionPreference {
    /// Pick the most recently known variant.
    #[default]
    Latest,
    /// Prefer latest non-lossy container, fallback to latest.
    LatestNonLossy,
}

/// Operational policy knobs.
///
/// Policies are intentionally narrow in MVP scope: enough to control how links
/// materialize and leave room for stricter or provider-aware behavior later.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Policies {
    /// Ordered link-materialization method preference.
    #[serde(default = "default_link_methods")]
    pub link_methods: Vec<LinkMethod>,
    /// Placeholder policy flag for strict rehash behaviors.
    #[serde(default)]
    pub strict_rehash: bool,
    /// Provider integration switch.
    #[serde(default)]
    pub musicbrainz_enabled: bool,
}

impl Default for Policies {
    fn default() -> Self {
        Self {
            link_methods: default_link_methods(),
            strict_rehash: false,
            musicbrainz_enabled: false,
        }
    }
}

fn default_link_methods() -> Vec<LinkMethod> {
    vec![LinkMethod::Symlink, LinkMethod::Hardlink, LinkMethod::Copy]
}

/// Supported link materialization methods.
///
/// The ordered list in `Policies::link_methods` is interpreted as a fallback
/// chain. This keeps cross-platform behavior explicit and predictable.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LinkMethod {
    /// Native symbolic link.
    Symlink,
    /// Native hard link.
    Hardlink,
    /// Byte copy fallback.
    Copy,
}

/// Load an [`AppConfig`] from disk.
///
/// Extension-based parsing is explicit on purpose; unclear extension handling
/// tends to hide configuration errors and make support/debugging harder.
pub fn load_config(config_path: &Path) -> Result<AppConfig> {
    let bytes = fs::read(config_path)?;

    match config_path.extension().and_then(|value| value.to_str()) {
        Some("json") => Ok(serde_json::from_slice(&bytes)?),
        Some("ncl") => serde_json::from_slice(&bytes).map_err(|_| {
            anyhow!(
                "Nickel support is planned but not integrated yet; use JSON config in {} for now",
                config_path.display()
            )
        }),
        _ => Err(anyhow!(
            "unsupported config extension for {} (supported: .json, .ncl [JSON-compatible subset only])",
            config_path.display()
        )),
    }
}

/// Write a pretty-printed JSON config to disk.
///
/// Formatting output deterministically is valuable for review workflows and for
/// making generated diffs stable in CI.
pub fn save_config_pretty(config_path: &Path, config: &AppConfig) -> Result<()> {
    if let Some(parent) = config_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut output = serde_json::to_vec_pretty(config)?;
    output.push(b'\n');
    fs::write(config_path, output)?;

    Ok(())
}
