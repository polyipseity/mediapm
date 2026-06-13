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
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
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
//! #
//! # let rt = tokio::runtime::Runtime::new().unwrap();
//! # rt.block_on(example()).unwrap();
//! ```

pub mod api;
pub(crate) mod delta;
pub mod error;
pub mod hash;
pub mod storage;

#[cfg(feature = "cli")]
pub mod cli;
#[cfg(feature = "cli")]
pub mod cli_visualization;

// Re-export the most important types at crate root for convenience.
pub use api::{
    CasApi, CasApiStreaming, CasMaintenanceApi, ConstraintApi, ObjectEncoding, ObjectMeta,
};
pub use error::CasError;
pub use hash::{Hash, HashParseError};
pub use storage::in_memory::new_in_memory_cas;
pub use storage::store::CasStore;
