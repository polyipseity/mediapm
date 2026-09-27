//! Nickel schema sync-prevention tests for the mediapm schema (`v1.ncl`, `v2.ncl`).
//!
//! These tests pin the shape the live `mediapm.ncl` decoder actually depends
//! on, from both sides of the boundary:
//!
//! - the Rust runtime model `MediaPmDocument` (the type every `mediapm.ncl`
//!   resolves to), and
//! - the `v1.ncl` / `v2.ncl` contract sources themselves.
//!
//! The versioned contracts and every migration live in Nickel, not Rust (see
//! `src/config/versions/mod.rs` for why). `parity_rust_side_owns_no_migration_dispatcher`
//! pins that: if a Rust decode/migrate mirror of the Nickel ladder is ever re-introduced, it will
//! have to be justified against a live caller rather than drift silently beside the Nickel one.
//! The two `parity_*_control` tests are regression controls for the scoping those parity tests
//! rely on; each seeds the drift a file-wide substring or a single-file scan cannot see, so a
//! future refactor that re-widens either one fails here rather than silently.

use mediapm::MediaPmDocument;

/// Extracts the body of the Nickel record contract `let <contract> = { ... }`.
///
/// Returns `None` when the contract is absent, or when its closing brace is
/// not written at column 0 — a `None` is a test failure, never a silent skip,
/// so a reindented or re-nested record cannot quietly turn a scoped field
/// assertion into a no-op.
///
/// Scoping matters because a bare `schema.contains("field | {")` cannot tell
/// *which* contract the field was declared in. A field that moved out of
/// `ToolRequirementV1` while an unrelated record kept a same-named field
/// would leave the file-wide substring intact; see
/// [`parity_scoped_lookup_catches_field_removed_from_tool_requirement`].
fn nickel_record_body<'a>(schema: &'a str, contract: &str) -> Option<&'a str> {
    let (_, after_open) = schema.split_once(&format!("let {contract} = {{"))?;
    let (body, _) = after_open.split_once("\n}\n")?;
    Some(body)
}

/// Every Rust file in `src/config/versions/`, paired with its repo-relative path.
///
/// The "no Rust migration dispatcher" rule governs the whole
/// `config/versions/` directory, not `mod.rs` alone: a dispatcher written into
/// `v1.rs` is exactly the drift the rule exists to prevent, and a
/// `mod.rs`-only scan cannot see it.
const RUST_VERSIONS_TREE: &[(&str, &str)] = &[
    ("config/versions/document.rs", include_str!("../../src/config/versions/document.rs")),
    ("config/versions/mod.rs", include_str!("../../src/config/versions/mod.rs")),
    ("config/versions/v_latest.rs", include_str!("../../src/config/versions/v_latest.rs")),
];

/// Rust-side symbols that would constitute a second migration ladder beside the Nickel one.
///
/// A per-version wire envelope counts too: `versions/vX.rs` is allowed to name
/// the immediately previous version module, so a Rust envelope added there
/// would satisfy the `mod_policy_guard` import rule while still mirroring the
/// ladder the Nickel registry owns.
const RUST_MIGRATION_DISPATCHER_SYMBOLS: &[&str] = &[
    "decode_mediapm_document_value",
    "encode_mediapm_document_value",
    "trait Migrate",
    "MediaPmDocumentEnvelopeV1",
    "MediaPmDocumentEnvelopeV2",
];

/// Returns the first `(path, symbol)` pair where a forbidden dispatcher symbol appears in `files`.
///
/// `files` is a parameter rather than a direct read of [`RUST_VERSIONS_TREE`] so that
/// [`parity_tree_scan_catches_dispatcher_outside_mod_rs`] can feed it a synthetic tree.
fn first_rust_migration_dispatcher<'a>(
    files: &'a [(&'a str, &'a str)],
) -> Option<(&'a str, &'a str)> {
    for (path, source) in files {
        for symbol in RUST_MIGRATION_DISPATCHER_SYMBOLS {
            if source.contains(*symbol) {
                return Some((*path, *symbol));
            }
        }
    }
    None
}

/// Validates that the Rust struct serialization shape matches the expected
/// V1 schema contract invariants.
#[test]
fn parity_mediapm_document_serialization_invariants() {
    let doc = MediaPmDocument::default();
    let json = serde_json::to_value(&doc).unwrap();
    let obj = json.as_object().expect("MediaPmDocument must serialize to a JSON object");

    // --- MUST be present at top level ---
    assert!(
        obj.contains_key("version"),
        "version must be a top-level field in MediaPmDocument (must be in V1 schema)"
    );
    assert!(
        obj.contains_key("media"),
        "media must be a top-level field in MediaPmDocument (must be in V1 schema)"
    );
    assert!(
        obj.contains_key("hierarchy"),
        "hierarchy must be a top-level field in MediaPmDocument (must be in V1 schema)"
    );
    assert!(
        obj.contains_key("tools"),
        "tools must be a top-level field in MediaPmDocument (must be in V1 schema)"
    );
    assert!(
        obj.contains_key("runtime"),
        "runtime must be a top-level field in MediaPmDocument (must be in V1 schema)"
    );

    // --- MUST NOT be present at top level ---
    assert!(
        !obj.contains_key("conductor"),
        "conductor must NOT be a top-level field in MediaPmDocument (removed from V1 schema)"
    );

    // --- tools MUST NOT be inside runtime ---
    if let Some(runtime) = obj.get("runtime").and_then(|v| v.as_object()) {
        assert!(
            !runtime.contains_key("tools"),
            "tools must NOT be inside runtime in MediaPmDocument (tools moved to top level)"
        );
    }
}

/// Validates that the V1 Nickel schema (`v1.ncl`) contains the expected
/// contracts and omits removed ones.
#[test]
fn parity_v1_nickel_schema_structure() {
    let schema = include_str!("../../src/config/versions/v1.ncl");

    // --- MUST define ToolRequirementV1 contract ---
    assert!(
        schema.contains("let ToolRequirementV1 = {"),
        "v1.ncl must define ToolRequirementV1 contract for the top-level tools field"
    );
    assert!(
        nickel_record_body(schema, "ToolRequirementV1").is_some_and(
            |body| body.contains("version_spec\n    | ConfigVersionSpecV1\n    | optional,")
        ),
        "ToolRequirementV1 must have version_spec field typed ConfigVersionSpecV1"
    );
    assert!(
        nickel_record_body(schema, "ToolRequirementV1")
            .is_some_and(|body| body.contains("dependencies | {")),
        "ToolRequirementV1 must have dependencies field"
    );

    // --- MUST have top-level tools with ToolRequirementV1 ---
    assert!(
        schema.contains("tools | { _ : ToolRequirementV1 } | default = {}"),
        "v1.ncl must declare top-level tools using ToolRequirementV1"
    );

    // --- MUST accept the legacy optional state field ---
    assert!(
        schema.contains("state | MediaPmStateV1 | optional"),
        "v1.ncl must accept the legacy optional state field (state is managed via state.json)"
    );

    // --- MUST NOT have conductor field ---
    assert!(
        !schema.contains("conductor | { .. } | optional"),
        "v1.ncl must NOT have a conductor field"
    );
    assert!(
        !schema.contains("conductor |"),
        "v1.ncl must NOT have any conductor field (double-check)"
    );

    // --- MUST NOT have tools inside runtime ---
    assert!(
        !schema.contains("tools | { .. } | optional"),
        "v1.ncl must NOT have tools inside runtime (tools is now top-level)"
    );
}

/// Validates that the V2 Nickel schema (`v2.ncl`) contains the expected
/// contracts and omits the dropped state surface.
#[test]
fn parity_v2_nickel_schema_structure() {
    let schema = include_str!("../../src/config/versions/v2.ncl");

    // --- MUST define ToolRequirementV2 contract ---
    assert!(
        schema.contains("let ToolRequirementV2 = {"),
        "v2.ncl must define ToolRequirementV2 contract for the top-level tools field"
    );
    assert!(
        nickel_record_body(schema, "ToolRequirementV2").is_some_and(
            |body| body.contains("version_spec\n    | ConfigVersionSpecV2\n    | optional,")
        ),
        "ToolRequirementV2 must have version_spec field typed ConfigVersionSpecV2"
    );
    assert!(
        nickel_record_body(schema, "ToolRequirementV2")
            .is_some_and(|body| body.contains("dependencies | {")),
        "ToolRequirementV2 must have dependencies field"
    );

    // --- MUST have top-level tools with ToolRequirementV2 ---
    assert!(
        schema.contains("tools | { _ : ToolRequirementV2 } | default = {}"),
        "v2.ncl must declare top-level tools using ToolRequirementV2"
    );

    // --- MUST NOT have a state field (dropped in v2; state is state.json) ---
    assert!(
        !schema.contains("state |"),
        "v2.ncl must NOT have a state field (state is managed via state.json)"
    );
    assert!(
        !schema.contains("MediaPmStateV"),
        "v2.ncl must NOT reference MediaPmState contracts (no V1 or V2 state shim)"
    );

    // --- MUST NOT have conductor field ---
    assert!(!schema.contains("conductor |"), "v2.ncl must NOT have any conductor field");

    // --- MUST NOT have tools inside runtime ---
    assert!(
        !schema.contains("tools | { .. } | optional"),
        "v2.ncl must NOT have tools inside runtime (tools is top-level)"
    );

    // --- STRICT VERSION SEPARATION: no *V1 contract bindings ---
    assert!(
        !schema.contains("V1 ="),
        "v2.ncl must NOT define any *V1 contract names (strict version separation)"
    );
}

/// Pins that the versioned contracts and the migrations live in Nickel, and
/// that no Rust-side dispatcher mirrors them.
///
/// Every `mediapm.ncl` on disk is read through `mod.ncl`'s `migrate_to`, so a
/// second Rust implementation of the same ladder would have no caller while
/// still looking like a supported format. This test fails if one reappears
/// anywhere in `src/config/versions/`, not just in `mod.rs` — see
/// [`RUST_VERSIONS_TREE`] and
/// [`parity_tree_scan_catches_dispatcher_outside_mod_rs`].
#[test]
fn parity_rust_side_owns_no_migration_dispatcher() {
    let v1_ncl = include_str!("../../src/config/versions/v1.ncl");
    let v2_ncl = include_str!("../../src/config/versions/v2.ncl");
    let registry_ncl = include_str!("../../src/config/versions/mod.ncl");

    // The Nickel registry must remain the dispatcher: it owns the supported
    // set, the current version, and both migration edges.
    assert!(registry_ncl.contains("migrate_to = migrate_to_fn"), "mod.ncl must own migrate_to");
    assert!(
        registry_ncl.contains("migrate_v1_to_v2"),
        "v2.ncl's migration must be reachable through the registry"
    );
    assert!(v2_ncl.contains("migrate_v1_to_v2"), "v2.ncl must own the migration INTO v2");
    assert!(v1_ncl.contains("migrate_v2_to_v1"), "v1.ncl must own the migration INTO v1");

    // The Rust side must carry no version-dispatch surface anywhere in the
    // versions tree. The `Debug` form of the `Option` names the offending
    // file and symbol, so a hit is diagnosable without a second lookup.
    assert_eq!(
        first_rust_migration_dispatcher(RUST_VERSIONS_TREE),
        None,
        "Rust must not mirror the Nickel migration ladder"
    );
}

/// Regression control for the scoped field assertions in the two parity tests above.
///
/// Seeds the drift a file-wide substring cannot see — `dependencies` leaves
/// `ToolRequirementV1` while an unrelated record in the same file keeps a
/// same-named field — and proves the scoped lookup reports the field as
/// missing where the file-wide substring still matched.
#[test]
fn parity_scoped_lookup_catches_field_removed_from_tool_requirement() {
    let schema = include_str!("../../src/config/versions/v1.ncl");
    let defective =
        schema.replacen("  dependencies | { _ : ConfigVersionSpecV1 } | default = {},\n", "", 1)
            + "\nlet UnrelatedHolderV1 = {\n  dependencies | { _ : String } | default = {},\n}\n";

    // Setup sanity: the contract survives, the field inside it does not.
    assert!(
        nickel_record_body(&defective, "ToolRequirementV1")
            .is_some_and(|body| !body.contains("dependencies | {")),
        "defect setup must remove `dependencies` from ToolRequirementV1 only"
    );
    assert!(
        defective.contains("dependencies | {"),
        "the file-wide substring the old assertion used must still match, \
         which is exactly why it could not see this defect"
    );

    assert!(
        !nickel_record_body(&defective, "ToolRequirementV1")
            .is_some_and(|body| body.contains("dependencies | {")),
        "the scoped lookup must report the field as missing once it leaves the contract"
    );
}

/// Regression control for [`RUST_VERSIONS_TREE`] covering the files a
/// `mod.rs`-only scan would miss.
///
/// The synthetic tree holds the dispatcher in `v1.rs` and keeps `mod.rs`
/// clean, which is precisely the shape a drifted `versions/v1.rs` would take:
/// a version file naming the adjacent version module is permitted by the
/// `mod_policy_guard` import rule, so nothing else in the suite would object.
#[test]
fn parity_tree_scan_catches_dispatcher_outside_mod_rs() {
    let synthetic: &[(&str, &str)] = &[
        ("config/versions/mod.rs", "pub fn registry_marker() {}"),
        ("config/versions/v1.rs", "fn decode_mediapm_document_value() {}"),
    ];

    assert_eq!(
        first_rust_migration_dispatcher(synthetic).map(|(path, _)| path),
        Some("config/versions/v1.rs"),
        "the tree scan must name the file holding the dispatcher, not report None"
    );
}
