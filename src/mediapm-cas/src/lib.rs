//! # mediapm-cas — Content-Addressable Storage
//!
//! A minimal, content-addressed blob store with delta-compression hints.
//!
//! ## Quick start
//!
//! ```
//! # use mediapm_cas::storage::in_memory::new_in_memory_cas;
//! # use mediapm_cas::api::CasApi;
//! # use bytes::Bytes;
//! #
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let cas = new_in_memory_cas();
//!
//! // Store some data.
//! let data = Bytes::from_static(b"hello content-addressable world");
//! let hash = cas.put(data.clone()).await?;
//!
//! // Retrieve it by hash.
//! let retrieved = cas.get(hash).await?;
//! assert_eq!(retrieved, data);
//!
//! // Get metadata.
//! let info = cas.stat(hash).await?;
//! assert_eq!(info.len, data.len() as u64);
//!
//! // Delete.
//! cas.delete(hash).await?;
//! assert!(cas.get(hash).await.is_err());
//! # Ok(())
//! # }
//! ```

pub mod api;
pub mod background;
pub mod config;
pub mod defaults;
pub(crate) mod delta;
pub mod error;
pub mod hash;
pub mod storage;
pub(crate) mod verify;

#[cfg(feature = "cli")]
pub mod cli;

// Re-export the most important types at crate root for convenience.
pub use api::{
    CasApi, CasMaintenanceApi, ConstraintApi, ConstraintPatch, ObjectEncoding, ObjectMeta,
    OptimizeReport, PruneReport, VerifyTriggerStrategy,
};
pub use background::BackgroundMaintenanceGuard;
pub use config::{
    CasConfig, CasIntegrityConfig, CasLocatorParseOptions, CasStorageLocator, ConfiguredCas,
};
pub use error::CasError;
pub use hash::{Hash, HashParseError};
pub use storage::file_system::FileSystemCas;
pub use storage::in_memory::{InMemoryCas, new_in_memory_cas};
pub use storage::store::CasStore;
