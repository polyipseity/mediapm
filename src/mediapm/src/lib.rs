//! Phase 3 `mediapm` media orchestration facade.
//!
//! This crate composes the CAS and Conductor layers into a media-oriented API.
//! The implementation is intentionally lightweight but keeps the runtime and
//! type contracts stable so deeper pipeline/materialization logic can be added
//! incrementally.

use std::path::Path;

use async_trait::async_trait;
use mediapm_cas::InMemoryCas;
use mediapm_conductor::{ConductorApi, ConductorError, SimpleConductor};
use thiserror::Error;
use url::Url;

/// Media package descriptor returned by source processing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaPackage {
    /// Canonical source URI that produced this package.
    pub source_uri: Url,
    /// Whether permanent transcode mode was requested.
    pub permanent: bool,
}

/// Summary of a sync run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncSummary {
    /// Number of conductor instances executed during sync.
    pub executed_instances: usize,
}

/// Error category for phase 3 orchestration.
#[derive(Debug, Error)]
pub enum MediaPmError {
    /// Source URI does not satisfy scheme requirements.
    #[error("invalid source URI: {0}")]
    InvalidSource(String),
    /// Error propagated from phase 2 conductor.
    #[error("conductor error: {0}")]
    Conductor(#[from] ConductorError),
}

/// Async API contract for media source processing and sync.
#[async_trait]
pub trait MediaPmApi: Send + Sync {
    /// Processes a single source URI using the configured media pipeline policy.
    async fn process_source(&self, uri: Url, permanent: bool)
    -> Result<MediaPackage, MediaPmError>;

    /// Reconciles declared media state to filesystem/materialization state.
    async fn sync_library(&self) -> Result<SyncSummary, MediaPmError>;
}

/// Generic media service over a pluggable conductor implementation.
pub struct MediaPmService<C>
where
    C: ConductorApi,
{
    conductor: C,
}

impl<C> MediaPmService<C>
where
    C: ConductorApi,
{
    /// Creates a media service using the provided conductor implementation.
    pub fn new(conductor: C) -> Self {
        Self { conductor }
    }
}

impl MediaPmService<SimpleConductor<InMemoryCas>> {
    /// Creates an in-memory stack suitable for local tests and bootstrap flows.
    pub fn new_in_memory() -> Self {
        let cas = InMemoryCas::new();
        let conductor = SimpleConductor::new(cas);
        Self::new(conductor)
    }
}

#[async_trait]
impl<C> MediaPmApi for MediaPmService<C>
where
    C: ConductorApi,
{
    async fn process_source(
        &self,
        uri: Url,
        permanent: bool,
    ) -> Result<MediaPackage, MediaPmError> {
        match uri.scheme() {
            "http" | "https" | "file" => Ok(MediaPackage { source_uri: uri, permanent }),
            _ => Err(MediaPmError::InvalidSource(
                "phase-3 currently supports http(s) and file schemes".to_string(),
            )),
        }
    }

    async fn sync_library(&self) -> Result<SyncSummary, MediaPmError> {
        let summary = self
            .conductor
            .run_workflow(Path::new("mediapm.user.cue"), Path::new("mediapm.machine.cue"))
            .await?;

        Ok(SyncSummary { executed_instances: summary.executed_instances })
    }
}

/// Returns built-in tool ids that phase 3 expects to be available.
pub fn registered_builtin_ids() -> [&'static str; 3] {
    [
        mediapm_conductor_builtin_fs_ops::TOOL_ID,
        mediapm_conductor_builtin_import::TOOL_ID,
        mediapm_conductor_builtin_zip::TOOL_ID,
    ]
}
