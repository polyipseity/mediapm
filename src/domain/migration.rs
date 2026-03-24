//! Sidecar schema migration engine.
//!
//! Migrations are executed as explicit version hops (`vN -> vN+1`) and each
//! applied hop is recorded in sidecar provenance.
//!
//! We use version hops (instead of one giant conversion function) so each step
//! stays understandable, testable, and independently auditable.

use anyhow::{Result, anyhow};
use serde_json::{Value, json};

use crate::{
    domain::model::{LATEST_SCHEMA_VERSION, MigrationProvenance},
    support::util::now_rfc3339,
};

type MigrationFn = fn(Value) -> Result<Value>;

#[derive(Clone, Copy)]
struct VersionHop {
    from_version: u32,
    to_version: u32,
    migration_id: &'static str,
    apply: MigrationFn,
}

fn migration_hops() -> Vec<VersionHop> {
    vec![VersionHop {
        from_version: 0,
        to_version: 1,
        migration_id: "bootstrap-v1",
        apply: migrate_v0_to_v1,
    }]
}

/// Migrate an arbitrary sidecar JSON value to the latest schema.
///
/// Returns the migrated value plus provenance records describing every applied
/// version hop.
///
/// This function is intentionally tolerant of missing/old fields and strict on
/// structural invalidity (e.g. non-object root), which balances forward motion
/// with safety.
pub fn migrate_to_latest(mut value: Value) -> Result<(Value, Vec<MigrationProvenance>)> {
    let mut provenance = Vec::new();

    let mut version =
        value.get("schema_version").and_then(Value::as_u64).map(|value| value as u32).unwrap_or(0);

    while version < LATEST_SCHEMA_VERSION {
        let Some(hop) = migration_hops().into_iter().find(|hop| hop.from_version == version) else {
            return Err(anyhow!("no migration hop found from schema_version={version}"));
        };

        value = (hop.apply)(value)?;
        value["schema_version"] = Value::from(hop.to_version);

        provenance.push(MigrationProvenance {
            migration_id: hop.migration_id.to_owned(),
            from_version: hop.from_version,
            to_version: hop.to_version,
            timestamp: now_rfc3339()?,
        });

        version = hop.to_version;
    }

    Ok((value, provenance))
}

fn migrate_v0_to_v1(mut value: Value) -> Result<Value> {
    let object =
        value.as_object_mut().ok_or_else(|| anyhow!("sidecar root must be a JSON object"))?;

    if !object.contains_key("canonical_uri")
        && let Some(uri_value) = object.remove("uri")
    {
        object.insert("canonical_uri".to_owned(), uri_value);
    }

    let now = now_rfc3339()?;
    object.entry("created_at").or_insert_with(|| Value::String(now.clone()));
    object.entry("updated_at").or_insert_with(|| Value::String(now));

    object.entry("variants").or_insert_with(|| Value::Array(Vec::new()));
    object.entry("edits").or_insert_with(|| Value::Array(Vec::new()));
    object.entry("migration_provenance").or_insert_with(|| Value::Array(Vec::new()));
    object
        .entry("provider_enrichment")
        .or_insert_with(|| json!({ "musicbrainz": { "matches": [], "applied": {} } }));

    if !object.contains_key("original") {
        let original_variant_hash = object
            .get("variants")
            .and_then(Value::as_array)
            .and_then(|variants| variants.first())
            .and_then(|first| first.get("variant_hash"))
            .and_then(Value::as_str)
            .unwrap_or("0000000000000000000000000000000000000000000000000000000000000000");

        object.insert(
            "original".to_owned(),
            json!({
                "original_variant_hash": original_variant_hash,
                "original_metadata": {},
            }),
        );
    }

    Ok(value)
}
