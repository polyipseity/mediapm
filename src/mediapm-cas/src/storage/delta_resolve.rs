//! Shared delta-chain resolution for reconstructing full bytes from delta
//! encodings.
//!
//! Both [`ComposedReadView::fetch_inner`](super::read_view::ComposedReadView)
//! and [`BgEngine::read_full_bytes`](super::bg_engine::BgEngine) walk the
//! same delta chain. This module extracts the common loop so both call sites
//! share one implementation.

use bytes::Bytes;

use crate::api::ObjectEncoding;
use crate::error::CasError;
use crate::hash::Hash;

use super::blob_store::BlobStore;
use super::index::Index;

/// Reconstruct full bytes for `hash` by walking its delta chain.
///
/// Callers provide the starting `base_hash` from the object's encoding and
/// context strings for error messages. Returns `Ok(full_bytes)` on success.
pub(super) async fn resolve_delta_chain<I: Index, B: BlobStore>(
    hash: &Hash,
    base_hash: Hash,
    index: &I,
    blob_store: &B,
    self_ref_msg: &str,
    base_not_found_msg: &str,
) -> Result<Bytes, CasError> {
    let mut chain: Vec<(Hash, Bytes)> = Vec::new();
    let mut current = *hash;
    let mut base = base_hash;

    loop {
        if current == base {
            return Err(CasError::CorruptObject {
                hash: Some(current),
                details: self_ref_msg.into(),
            });
        }
        let delta_data = blob_store.read_delta(&current).await?;
        chain.push((current, delta_data));
        current = base;

        match index.get(&current).await? {
            Some(base_entry) => match base_entry.encoding {
                ObjectEncoding::Full => {
                    let base_data = blob_store.read(&current).await?;
                    return crate::delta::delta::resolve_delta_chain(
                        base_data, &mut chain, current,
                    );
                }
                ObjectEncoding::Delta { base_hash: next_base } => {
                    base = next_base;
                }
            },
            None => {
                return Err(CasError::CorruptObject {
                    hash: Some(current),
                    details: format!("{base_not_found_msg}: base {current} not found"),
                });
            }
        }
    }
}
