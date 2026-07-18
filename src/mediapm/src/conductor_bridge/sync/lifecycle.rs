//! Tool lifecycle transitions.
//!
//! This module manages content-map hash validation and internal launcher
//! file regeneration for managed tools.

use mediapm_conductor::NickelDocument;

/// Returns true when the tool name identifies a builtin source-ingest
/// tool that requires special content-ingestion handling.
#[must_use]
pub(super) fn is_builtin_source_ingest_requirement(tool_name: &str) -> bool {
    tool_name.eq_ignore_ascii_case("import")
}

/// Checks whether a content-map hash value is still referenced by any
/// tool runtime in the document.
#[allow(dead_code)]
#[must_use]
pub(super) fn is_hash_in_tool_content_maps(hash: &str, document: &NickelDocument) -> bool {
    document.tools.values().any(|spec| spec.runtime.content_map.values().any(|v| v == hash))
}

/// Stores a CAS lock marker for a provisioned tool registry version.
///
/// Creates a deterministic marker payload `registry-locks/<tool_id>/<version>`
/// and stores it in CAS. The returned hex hash uniquely identifies the locked
/// version — identical inputs produce the same hash, making the operation
/// idempotent.
///
/// Returns `None` when `identity` is empty.
#[allow(dead_code)]
pub(super) async fn lock_registry_version(
    cas: &impl mediapm_cas::CasApi,
    tool_id: &str,
    identity: &str,
) -> Option<String> {
    if identity.is_empty() {
        return None;
    }
    let marker = format!("registry-locks/{tool_id}/{identity}");
    let hash = cas.put(bytes::Bytes::from(marker)).await.ok()?;
    Some(hash.to_hex())
}
