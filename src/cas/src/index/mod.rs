//! Persistent index model and graph invariants for CAS.
//!
//! `mod.rs` is intentionally thin. Runtime model types live in `state.rs`,
//! depth/graph logic in `graph.rs`, persistence in `db.rs`, schema definitions
//! and row/schema version envelopes in `versions/`.
//!
//! ## DO NOT REMOVE: external versions boundary guard
//!
//! Files outside `index/versions/` must stay version-agnostic and interact
//! through this module's unversioned facade APIs, never through
//! `index::versions::*` paths directly.

mod db;
mod graph;
mod state;
mod versions;

#[cfg(test)]
pub(super) use versions::schema_marker_needs_initialization;
pub(super) use versions::{
    decode_bloom_payload, decode_primary_object_meta, encode_bloom_payload,
    encode_current_schema_marker, encode_primary_object_meta, latest_schema_marker,
    migrate_index_state_to_version,
};
pub(super) use versions::{hash_from_index_key, index_key_from_hash};
pub(super) use versions::{
    initialize_tables, open_constraints_table_read, open_constraints_table_write,
    open_primary_table_read, open_primary_table_write, read_bloom_payload_from_table,
    read_schema_marker_value_from_metadata, write_bloom_payload_to_table,
    write_schema_marker_to_metadata,
};

pub(crate) use db::{BatchOperation, CasIndexDb};
pub(crate) use graph::{recalculate_depths, resolve_object_depth};
pub(crate) use state::{
    DELTA_PROMOTION_DEPTH, IndexState, MAX_DELTA_DEPTH, ObjectEncoding, ObjectMeta,
    ensure_empty_record,
};
pub(crate) use versions::HASH_STORAGE_KEY_BYTES;
