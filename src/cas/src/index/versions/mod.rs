//! Versioned persistence envelopes for CAS index rows.
//!
//! The runtime source-of-truth is `ObjectMeta` + explicit constraints in
//! `index/mod.rs`. Version modules own row wire formats and migration bridges.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - `vX.rs` files must never import unversioned structs outside `versions/`.
//! - A `vX` file may only reference the most recent previous version, and only
//!   for version-to-version isomorphism/migration.
//! - This `mod.rs` is the only place where latest version state is bridged to
//!   unversioned runtime state.
//! - Files outside `index/versions/` must interact with versioned index
//!   envelopes only through this `mod.rs`; never import `versions::vX` directly.
//! - Do not directly re-export `versions::vX` structs/types from this module.
//!   Expose unversioned APIs here and keep versioned internals encapsulated.

use fp_library::brands::RcBrand;
use fp_library::types::optics::IsoPrime;
use redb::TableDefinition;

use crate::index::{IndexState, ObjectEncoding, ObjectMeta};
use crate::{CasError, Hash};

pub(crate) mod v1;

/// Latest-version bindings.
///
/// Keep all explicit latest-version (`vX`) references centralized in this
/// module so schema upgrades are less error-prone.
// BEGIN latest-version bindings
mod latest {
    use super::v1;
    use crate::{CasError, Hash};

    pub(super) const SCHEMA_MARKER: u32 = v1::schema_version_v1();
    pub(super) const HASH_KEY_BYTES: usize = v1::HASH_STORAGE_KEY_BYTES;
    pub(super) type ObjectMeta = v1::ObjectMetaV1;

    pub(super) const fn is_schema_marker(marker: u32) -> bool {
        v1::is_schema_version_v1(marker)
    }

    pub(super) fn index_key_from_hash(hash: Hash) -> [u8; HASH_KEY_BYTES] {
        v1::index_key_from_hash(hash)
    }

    pub(super) fn hash_from_index_key(key: &[u8]) -> Result<Hash, CasError> {
        v1::hash_from_index_key(key)
    }

    pub(super) const fn object_meta_full(
        payload_len: u64,
        content_len: u64,
        depth: u32,
    ) -> ObjectMeta {
        v1::ObjectMetaV1::full(payload_len, content_len, depth)
    }

    pub(super) fn object_meta_delta(
        payload_len: u64,
        content_len: u64,
        depth: u32,
        base_hash: Hash,
    ) -> ObjectMeta {
        v1::ObjectMetaV1::delta(payload_len, content_len, depth, base_hash)
    }
}
// END latest-version bindings

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Internal dispatched persisted-index layout marker.
enum PersistedLayoutVersion {
    V1,
}

/// Fixed-width multihash key size for persisted index rows.
pub(crate) const HASH_STORAGE_KEY_BYTES: usize = latest::HASH_KEY_BYTES;

/// Fixed schema metadata table containing only one field: schema `version`.
const INDEX_METADATA_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("metadata");
/// Schema metadata field key storing the persisted schema marker.
const SCHEMA_VERSION_KEY: &[u8] = b"version";

/// Encodes one hash into the fixed-width primary-key representation.
#[must_use]
pub(crate) fn index_key_from_hash(hash: Hash) -> [u8; HASH_STORAGE_KEY_BYTES] {
    latest::index_key_from_hash(hash)
}

/// Decodes one fixed-width primary key back into a hash.
pub(crate) fn hash_from_index_key(key: &[u8]) -> Result<Hash, CasError> {
    latest::hash_from_index_key(key)
}

/// Isomorphism between latest version-local object metadata and runtime metadata.
fn object_meta_iso() -> IsoPrime<'static, RcBrand, latest::ObjectMeta, ObjectMeta> {
    IsoPrime::new(
        |versioned: latest::ObjectMeta| {
            if versioned.is_full() {
                ObjectMeta::full(versioned.payload_len, versioned.content_len, versioned.depth())
            } else {
                let base_hash = versioned
                    .base_hash()
                    .expect("versioned delta metadata must contain a multihash-encoded base hash");
                ObjectMeta::delta(
                    versioned.payload_len,
                    versioned.content_len,
                    versioned.depth(),
                    base_hash,
                )
            }
        },
        |runtime: ObjectMeta| match runtime.encoding() {
            ObjectEncoding::Full => {
                latest::object_meta_full(runtime.payload_len, runtime.content_len, runtime.depth())
            }
            ObjectEncoding::Delta { base_hash } => latest::object_meta_delta(
                runtime.payload_len,
                runtime.content_len,
                runtime.depth(),
                base_hash,
            ),
        },
    )
}

/// Decodes persisted primary-row bytes into unversioned runtime object metadata.
pub(crate) fn decode_primary_object_meta(
    schema_marker: u32,
    bytes: &[u8],
) -> Result<ObjectMeta, CasError> {
    match dispatch_schema_marker(schema_marker)? {
        PersistedLayoutVersion::V1 => {
            let versioned =
                v1::PrimaryHeaderV1::decode(bytes, super::MAX_DELTA_DEPTH)?.to_object_meta_v1();
            Ok(object_meta_iso().from(versioned))
        }
    }
}

/// Encodes unversioned runtime object metadata into persisted primary-row bytes.
pub(crate) fn encode_primary_object_meta(
    schema_marker: u32,
    meta: ObjectMeta,
) -> Result<Vec<u8>, CasError> {
    match dispatch_schema_marker(schema_marker)? {
        PersistedLayoutVersion::V1 => {
            let versioned = object_meta_iso().to(meta);
            Ok(v1::PrimaryHeaderV1::encode(versioned, super::MAX_DELTA_DEPTH)?.to_vec())
        }
    }
}

/// Returns the latest supported persisted schema marker.
#[must_use]
pub(crate) const fn latest_schema_marker() -> u32 {
    latest::SCHEMA_MARKER
}

/// ## DO NOT REMOVE: latest-first version dispatch guard
///
/// Version checks shoud always start checking from the latest version to ensure performance.
fn dispatch_schema_marker(marker: u32) -> Result<PersistedLayoutVersion, CasError> {
    if latest::is_schema_marker(marker) {
        Ok(PersistedLayoutVersion::V1)
    } else {
        Err(CasError::corrupt_index(format!(
            "unsupported index schema marker {marker}; expected {}",
            latest_schema_marker()
        )))
    }
}

/// Migrates one full runtime index snapshot from one schema marker to another.
///
/// Migration is whole-index and atomic at caller level: no partial per-row
/// migration is performed through this API.
pub(crate) fn migrate_index_state_to_version(
    state: IndexState,
    from_schema_marker: u32,
    target_schema_marker: u32,
) -> Result<IndexState, CasError> {
    let from_layout = dispatch_schema_marker(from_schema_marker)?;
    let to_layout = dispatch_schema_marker(target_schema_marker)?;

    match (from_layout, to_layout) {
        (PersistedLayoutVersion::V1, PersistedLayoutVersion::V1) => Ok(state),
    }
}

/// Opens the primary object metadata table for a read transaction.
pub(crate) fn open_primary_table_read(
    read: &redb::ReadTransaction,
    schema_marker: u32,
) -> Result<redb::ReadOnlyTable<&'static [u8], &'static [u8]>, CasError> {
    match dispatch_schema_marker(schema_marker)? {
        PersistedLayoutVersion::V1 => v1::open_primary_table_read_v1(read),
    }
}

/// Opens the primary object metadata table for a write transaction.
pub(crate) fn open_primary_table_write<'txn>(
    write: &'txn redb::WriteTransaction,
    schema_marker: u32,
) -> Result<redb::Table<'txn, &'static [u8], &'static [u8]>, CasError> {
    match dispatch_schema_marker(schema_marker)? {
        PersistedLayoutVersion::V1 => v1::open_primary_table_write_v1(write),
    }
}

/// Opens the constraints table for a read transaction.
pub(crate) fn open_constraints_table_read(
    read: &redb::ReadTransaction,
    schema_marker: u32,
) -> Result<redb::ReadOnlyMultimapTable<&'static [u8], &'static [u8]>, CasError> {
    match dispatch_schema_marker(schema_marker)? {
        PersistedLayoutVersion::V1 => v1::open_constraints_table_read_v1(read),
    }
}

/// Opens the constraints table for a write transaction.
pub(crate) fn open_constraints_table_write<'txn>(
    write: &'txn redb::WriteTransaction,
    schema_marker: u32,
) -> Result<redb::MultimapTable<'txn, &'static [u8], &'static [u8]>, CasError> {
    match dispatch_schema_marker(schema_marker)? {
        PersistedLayoutVersion::V1 => v1::open_constraints_table_write_v1(write),
    }
}

/// Initializes all index persistence tables for one persisted schema marker.
pub(crate) fn initialize_tables(
    write: &redb::WriteTransaction,
    schema_marker: u32,
) -> Result<(), CasError> {
    match dispatch_schema_marker(schema_marker)? {
        PersistedLayoutVersion::V1 => v1::initialize_data_tables_v1(write)?,
    }
    let _ = write.open_table(INDEX_METADATA_TABLE).map_err(CasError::redb)?;
    Ok(())
}

/// Reads raw schema marker bytes from metadata storage.
pub(crate) fn read_schema_marker_from_metadata(
    read: &redb::ReadTransaction,
) -> Result<Option<Vec<u8>>, CasError> {
    let metadata = read.open_table(INDEX_METADATA_TABLE).map_err(CasError::redb)?;
    let Some(value) = metadata.get(SCHEMA_VERSION_KEY).map_err(CasError::redb)? else {
        return Ok(None);
    };

    Ok(Some(value.value().to_vec()))
}

/// Decodes persisted schema marker bytes from metadata storage.
///
/// This validates that the decoded marker belongs to one supported schema
/// layout and returns the marker value for fast caller-side caching.
pub(crate) fn read_schema_marker_value_from_metadata(
    read: &redb::ReadTransaction,
) -> Result<Option<u32>, CasError> {
    let Some(bytes) = read_schema_marker_from_metadata(read)? else {
        return Ok(None);
    };

    let marker = decode_schema_marker_bytes(bytes.as_slice())?;
    let _ = dispatch_schema_marker(marker)?;
    Ok(Some(marker))
}

/// Writes schema marker bytes to metadata storage.
pub(crate) fn write_schema_marker_to_metadata(
    write: &redb::WriteTransaction,
    encoded_schema_marker: &[u8; 4],
) -> Result<(), CasError> {
    let mut metadata = write.open_table(INDEX_METADATA_TABLE).map_err(CasError::redb)?;
    metadata
        .insert(SCHEMA_VERSION_KEY, encoded_schema_marker.as_slice())
        .map_err(CasError::redb)?;
    Ok(())
}

/// Reads raw bloom payload bytes from persisted bloom table.
pub(crate) fn read_bloom_payload_from_table(
    read: &redb::ReadTransaction,
    schema_marker: u32,
) -> Result<Option<Vec<u8>>, CasError> {
    match dispatch_schema_marker(schema_marker)? {
        PersistedLayoutVersion::V1 => v1::read_bloom_payload_from_table_v1(read),
    }
}

/// Writes raw bloom payload bytes to persisted bloom table.
pub(crate) fn write_bloom_payload_to_table(
    write: &redb::WriteTransaction,
    schema_marker: u32,
    payload: &[u8],
) -> Result<(), CasError> {
    match dispatch_schema_marker(schema_marker)? {
        PersistedLayoutVersion::V1 => v1::write_bloom_payload_to_table_v1(write, payload),
    }
}

/// Returns whether schema metadata should be initialized for an empty index.
///
/// All schema-version matching logic is centralized in `versions/mod.rs`.
#[cfg(test)]
pub(crate) fn schema_marker_needs_initialization(
    persisted: Option<&[u8]>,
    has_data: bool,
) -> Result<bool, CasError> {
    match persisted {
        Some(bytes) => {
            let marker = decode_schema_marker_bytes(bytes)?;
            let _ = dispatch_schema_marker(marker)?;
            Ok(false)
        }
        None if has_data => Err(CasError::corrupt_index(
            "index contains data but missing schema metadata for current unreleased format",
        )),
        None => Ok(true),
    }
}

/// Encodes current schema metadata bytes for persistence.
pub(crate) fn encode_current_schema_marker(schema_marker: u32) -> Result<[u8; 4], CasError> {
    let _ = dispatch_schema_marker(schema_marker)?;
    Ok(schema_marker.to_le_bytes())
}

/// Decodes metadata schema marker bytes into a marker value.
///
/// This helper intentionally validates only byte width. Supported-marker
/// validation remains in `dispatch_schema_marker` so callers can separate
/// decode and compatibility checks when needed.
pub(crate) fn decode_schema_marker_bytes(bytes: &[u8]) -> Result<u32, CasError> {
    let array: [u8; 4] = bytes.try_into().map_err(|_| {
        CasError::corrupt_index(format!(
            "invalid schema metadata width: expected 4, got {}",
            bytes.len()
        ))
    })?;
    Ok(u32::from_le_bytes(array))
}

/// Decodes persisted bloom payload bytes.
///
/// Returns `(bit_len, raw_words_bytes)` where `raw_words_bytes` is a borrowed
/// slice over packed little-endian `u64` words.
pub(crate) fn decode_bloom_payload(
    schema_marker: u32,
    bytes: &[u8],
) -> Result<(usize, &[u8]), CasError> {
    match dispatch_schema_marker(schema_marker)? {
        PersistedLayoutVersion::V1 => v1::decode_bloom_payload_v1(bytes),
    }
}

/// Encodes bloom payload bytes using the latest format.
pub(crate) fn encode_bloom_payload(
    schema_marker: u32,
    bit_len: usize,
    raw_words: &[u64],
) -> Result<Vec<u8>, CasError> {
    match dispatch_schema_marker(schema_marker)? {
        PersistedLayoutVersion::V1 => v1::encode_bloom_payload_v1(bit_len, raw_words),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    /// Collects `vN.rs` files and parsed numeric version markers in ascending order.
    fn collect_versioned_files(dir: &Path) -> Vec<(PathBuf, u32)> {
        let mut files = fs::read_dir(dir)
            .unwrap_or_else(|err| panic!("failed to read versions dir '{}': {err}", dir.display()))
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| {
                let path = entry.path();
                if !path.is_file() {
                    return None;
                }

                let file_name = path.file_name()?.to_string_lossy();
                let version = file_name
                    .strip_prefix('v')
                    .and_then(|tail| tail.strip_suffix(".rs"))
                    .and_then(|digits| digits.parse::<u32>().ok())?;
                Some((path, version))
            })
            .collect::<Vec<_>>();
        files.sort_unstable_by_key(|(_, version)| *version);
        files
    }

    /// Extracts `::vN::` style module references from source text.
    fn extract_version_module_refs(content: &str) -> Vec<u32> {
        let bytes = content.as_bytes();
        let mut refs = Vec::new();
        let mut idx = 0usize;

        while idx + 3 < bytes.len() {
            if bytes[idx] != b'v' {
                idx += 1;
                continue;
            }

            let mut end = idx + 1;
            while end < bytes.len() && bytes[end].is_ascii_digit() {
                end += 1;
            }

            let looks_like_path_segment = end > idx + 1
                && end + 1 < bytes.len()
                && bytes[end] == b':'
                && bytes[end + 1] == b':'
                && idx >= 2
                && bytes[idx - 1] == b':'
                && bytes[idx - 2] == b':';

            if looks_like_path_segment && let Ok(version) = content[idx + 1..end].parse::<u32>() {
                refs.push(version);
            }

            idx = end;
        }

        refs
    }

    /// Recursively collects Rust source files under `dir`.
    fn collect_rs_files_recursive(dir: &Path, out: &mut Vec<PathBuf>) {
        for entry in fs::read_dir(dir)
            .unwrap_or_else(|err| panic!("failed to read source dir '{}': {err}", dir.display()))
        {
            let entry = entry.unwrap_or_else(|err| {
                panic!("failed reading source dir entry in '{}': {err}", dir.display())
            });
            let path = entry.path();
            if path.is_dir() {
                collect_rs_files_recursive(&path, out);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                out.push(path);
            }
        }
    }

    /// Returns `true` when `path` is inside one of the versions directories.
    fn path_is_inside_versions_dir(path: &Path) -> bool {
        let normalized = path.to_string_lossy().replace('\\', "/");
        normalized.contains("/codec/versions/") || normalized.contains("/index/versions/")
    }

    /// Detects direct `versions::vN` path usage in non-versions files.
    fn has_direct_versions_vx_path(content: &str) -> bool {
        let bytes = content.as_bytes();
        let needle = b"versions::v";
        let mut idx = 0usize;

        while idx + needle.len() < bytes.len() {
            let Some(rel) = bytes[idx..].windows(needle.len()).position(|w| w == needle) else {
                break;
            };
            let pos = idx + rel;
            let mut end = pos + needle.len();
            while end < bytes.len() && bytes[end].is_ascii_digit() {
                end += 1;
            }

            if end > pos + needle.len() {
                let boundary_ok = end == bytes.len()
                    || matches!(
                        bytes[end],
                        b':' | b';' | b',' | b' ' | b'\n' | b'\r' | b'{' | b'(' | b')'
                    );
                if boundary_ok {
                    return true;
                }
            }

            idx = end;
        }

        false
    }

    /// Detects leaked versioned type tokens outside version boundaries.
    fn has_known_versioned_type_leak(content: &str) -> bool {
        const LEAKED_TOKENS: &[&str] =
            &["IndexStateV", "ObjectMetaV", "PrimaryHeaderV", "V1Envelope", "DeltaStateV"];
        LEAKED_TOKENS.iter().any(|token| content.contains(token))
    }

    #[test]
    /// Enforces version-folder policy guard and vN reference boundaries.
    fn versioned_files_keep_policy_guard_and_boundary_rules() {
        let versions_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/index/versions");
        let files = collect_versioned_files(&versions_dir);
        assert!(
            !files.is_empty(),
            "expected at least one versioned file under {}",
            versions_dir.display()
        );

        for (path, version) in files {
            let content = fs::read_to_string(&path).unwrap_or_else(|err| {
                panic!("failed reading versioned file '{}': {err}", path.display())
            });

            assert!(
                content.contains("## DO NOT REMOVE: versions policy guard"),
                "{} must include the non-removable versions policy guard docstring",
                path.display()
            );

            assert!(
                !content.contains("use crate::index::") && !content.contains("crate::index::"),
                "{} must not import unversioned index runtime structs directly",
                path.display()
            );

            let referenced_versions = extract_version_module_refs(&content);
            for referenced in referenced_versions {
                if referenced == version {
                    continue;
                }

                if version == 1 {
                    panic!(
                        "{} (v1) must not reference any other version module; found v{}",
                        path.display(),
                        referenced
                    );
                }

                assert_eq!(
                    referenced,
                    version - 1,
                    "{} (v{}) may reference only v{}; found v{}",
                    path.display(),
                    version,
                    version - 1,
                    referenced
                );
            }
        }
    }

    #[test]
    /// Ensures non-version files never directly import `versions::vN` symbols.
    fn non_versions_files_never_import_versions_vx_directly() {
        let src_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut rs_files = Vec::new();
        collect_rs_files_recursive(&src_dir, &mut rs_files);

        for path in rs_files {
            if path_is_inside_versions_dir(&path) {
                continue;
            }

            let content = fs::read_to_string(&path).unwrap_or_else(|err| {
                panic!("failed reading source file '{}': {err}", path.display())
            });

            assert!(
                !has_direct_versions_vx_path(&content),
                "{} must not directly import or reference versions::vX; route through versions/mod.rs",
                path.display()
            );

            assert!(
                !has_known_versioned_type_leak(&content),
                "{} must not depend on leaked versioned type names; keep non-versions files on unversioned runtime models",
                path.display()
            );
        }
    }

    #[test]
    /// Ensures this module does not directly re-export version-specific symbols.
    fn versions_mod_must_not_reexport_versioned_symbols() {
        let mod_file = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/index/versions/mod.rs");
        let content = fs::read_to_string(&mod_file)
            .unwrap_or_else(|err| panic!("failed reading '{}': {err}", mod_file.display()));
        let has_direct_reexport = content.lines().any(|line| {
            let line = line.trim_start();
            line.starts_with("pub(crate) use v") || line.starts_with("pub use v")
        });

        assert!(
            !has_direct_reexport,
            "{} must not directly re-export vX symbols; expose unversioned APIs instead",
            mod_file.display()
        );
    }

    #[test]
    /// Enforces index non-version modules avoid explicit versions paths/markers.
    fn index_non_versions_modules_must_not_import_versions_paths() {
        let index_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/index");
        let mut rs_files = Vec::new();
        collect_rs_files_recursive(&index_dir, &mut rs_files);

        for path in rs_files {
            let normalized = path.to_string_lossy().replace('\\', "/");
            if normalized.ends_with("/index/mod.rs") || normalized.contains("/index/versions/") {
                continue;
            }

            let content = fs::read_to_string(&path)
                .unwrap_or_else(|err| panic!("failed reading '{}': {err}", path.display()));

            let non_comment_code = content
                .lines()
                .filter(|line| {
                    let trimmed = line.trim_start();
                    !trimmed.starts_with("//")
                })
                .collect::<Vec<_>>()
                .join("\n");

            assert!(
                !non_comment_code.contains("use super::versions")
                    && !non_comment_code.contains("use crate::index::versions")
                    && !non_comment_code.contains("use self::versions"),
                "{} must not import from versions paths; use index/mod.rs unversioned facade symbols",
                path.display()
            );

            assert!(
                !non_comment_code.contains("version:")
                    && !non_comment_code.contains("INDEX_SCHEMA_VERSION")
                    && !non_comment_code.contains("BLOOM_ENCODING_VERSION")
                    && !non_comment_code.contains("decode_schema_version")
                    && !non_comment_code.contains("ensure_schema_version")
                    && !non_comment_code.contains("write_schema_version")
                    && !non_comment_code.contains("read_schema_version"),
                "{} must not contain version fields or explicit version-checking code",
                path.display()
            );
        }
    }

    #[test]
    /// Ensures latest-first dispatch performance guard text remains present.
    fn versions_mod_keeps_latest_first_dispatch_docstring() {
        let mod_file = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/index/versions/mod.rs");
        let content = fs::read_to_string(&mod_file)
            .unwrap_or_else(|err| panic!("failed reading '{}': {err}", mod_file.display()));

        assert!(
            content.contains("Version checks shoud always start checking from the latest version to ensure performance."),
            "{} must keep the latest-first version dispatch performance guard docstring",
            mod_file.display()
        );
    }

    #[test]
    /// Ensures latest-version binding block remains centralized in one section.
    fn versions_mod_centralizes_latest_bindings_block() {
        let mod_file = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/index/versions/mod.rs");
        let full_content = fs::read_to_string(&mod_file)
            .unwrap_or_else(|err| panic!("failed reading '{}': {err}", mod_file.display()));
        let content = full_content
            .split("\n#[cfg(test)]")
            .next()
            .expect("split always yields at least one segment");

        let begin = "// BEGIN latest-version bindings";
        let end = "// END latest-version bindings";
        let Some(begin_idx) = content.find(begin) else {
            panic!(
                "{} must include '{}' marker to centralize latest bindings",
                mod_file.display(),
                begin
            );
        };
        let Some(end_idx) = content.find(end) else {
            panic!(
                "{} must include '{}' marker to centralize latest bindings",
                mod_file.display(),
                end
            );
        };
        assert!(
            begin_idx < end_idx,
            "{} latest bindings markers are out of order",
            mod_file.display()
        );

        let mut outside = String::with_capacity(content.len());
        outside.push_str(&content[..begin_idx]);
        outside.push_str(&content[end_idx + end.len()..]);

        assert!(
            content.contains(
                "pub(crate) const HASH_STORAGE_KEY_BYTES: usize = latest::HASH_KEY_BYTES;"
            ),
            "{} must centralize latest hash-key-width binding through latest module",
            mod_file.display()
        );

        let allowed_functions = [
            "decode_primary_object_meta",
            "encode_primary_object_meta",
            "open_primary_table_read",
            "open_primary_table_write",
            "open_constraints_table_read",
            "open_constraints_table_write",
            "initialize_tables",
            "read_bloom_payload_from_table",
            "write_bloom_payload_to_table",
            "decode_bloom_payload",
            "encode_bloom_payload",
        ];

        let mut current_fn: Option<String> = None;
        for line in outside.lines() {
            let trimmed = line.trim_start();
            if trimmed.starts_with("fn ") || trimmed.starts_with("pub(crate) fn ") {
                let after_kw = if let Some(rest) = trimmed.strip_prefix("pub(crate) fn ") {
                    rest
                } else if let Some(rest) = trimmed.strip_prefix("fn ") {
                    rest
                } else {
                    continue;
                };

                let name = after_kw
                    .split('(')
                    .next()
                    .unwrap_or_default()
                    .split('<')
                    .next()
                    .unwrap_or_default()
                    .trim()
                    .to_string();
                current_fn = Some(name);
            }

            if trimmed.contains("v1::") {
                let fn_name = current_fn.as_deref().unwrap_or("<module>");
                assert!(
                    allowed_functions.contains(&fn_name),
                    "{} must use direct v1:: references only in dispatch-conditioned functions; found in {}: {}",
                    mod_file.display(),
                    fn_name,
                    trimmed
                );
            }
        }
    }
}
