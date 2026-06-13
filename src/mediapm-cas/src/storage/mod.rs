//! Storage backends: WAL, ObjectIndex, MetadataIndex, BackgroundEngine,
//! and the composed CasStore handle.

#[macro_use]
mod macros;

pub mod bg_engine;
pub mod file_system;
pub mod in_memory;
pub mod metadata_index;
pub mod object_index;
pub(crate) mod read_view;
pub mod store;
pub mod wal;
