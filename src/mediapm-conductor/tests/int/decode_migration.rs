//! Decode + migration integration tests.
//!
//! Exercises the Nickel migration pipeline (`decode_document`) that the
//! conductor uses for config loading. Replaces earlier standalone
//! reproduction tests (`test_repro_migration_error*`).
//!
//! Regression: the pipeline must not throw a `MissingFieldDef` error on
//! `migrate_to` or any other record field. Nickel `rec` records (the default)
//! bind field names in their own scope, so a field `x = x` creates a
//! self-reference (not a reference to the let-bound `x`). Therefore all
//! let-bound variables exported by a record must use names distinct from the
//! record field keys. The parser additionally does not elaborate shorthand
//! `{ x, }` to `{ x = x }`, but that is a secondary concern — even explicit
//! `{ x = x }` fails in a `rec` record.

use mediapm_conductor::config::versions::decode_document;

/// Helper: runs `decode_document` on `input`, returns `true` if the error (if
/// any) is *not* a MissingFieldDef on `migrate_to`.
fn check_no_migrate_to_error(label: &str, input: &str) -> bool {
    let result = decode_document(input.as_bytes());
    match &result {
        Ok(_) => {
            eprintln!("  {label}: OK");
            true
        }
        Err(err) => {
            let msg = format!("{err}");
            if msg.contains("migrate_to") || msg.contains("MissingFieldDef") {
                eprintln!("  {label}: UNEXPECTED MissingFieldDef: {err}");
                false
            } else {
                eprintln!("  {label}: expected validation error: {err}");
                true
            }
        }
    }
}

/// Minimal v1 document — only `version = 1`, everything else missing.
const V1_MINIMAL: &str = r#"{ version = 1 }"#;

/// Realistic v1 machine-state header.
const V1_HEADER: &str = r#"{
    runtime = { tool_configs = {} },
    tools = {},
    workflows = {},
    external_data = {},
    version = 1,
}"#;

fn make_large_v1(extra_fields: usize) -> String {
    let extra: String = (0..extra_fields).map(|i| format!("  _field_{i} = null,\n")).collect();
    format!(
        r#"{{
    runtime = {{ tool_configs = {{}} }},
    tools = {{}},
    workflows = {{}},
    external_data = {{}},
    version = 1,
{extra}}}"#
    )
}

fn make_large_v1_workflows(n: usize) -> String {
    let workflows: String = (0..n)
        .map(|i| {
            format!(
                r#"    "wf.{i}" = {{
      description = m%%""dummy workflow {i}""%%,
      name = "wf.{i}",
      steps = [
        {{
          depends_on = [],
          id = "step-{i}",
          inputs = {{}},
          outputs = [],
          tool = "dummy",
        }},
      ],
    }},
"#
            )
        })
        .collect();
    format!(
        r#"{{
    runtime = {{ tool_configs = {{}} }},
    tools = {{}},
    workflows = {{
{workflows}    }},
    external_data = {{}},
    version = 1,
}}"#
    )
}

/// The nickel migration pipeline must not throw MissingFieldDef on `migrate_to`
/// even for minimal input documents.
#[test]
fn minimal_document_does_not_trigger_migrate_to_missing_def() {
    let ok = check_no_migrate_to_error("v1_minimal", V1_MINIMAL);
    assert!(ok, "Minimal v1 doc triggered MissingFieldDef on migrate_to");
}

/// Various realistic v1 documents must survive migration without the shorthand
/// defect.
#[test]
fn various_inputs_do_not_trigger_migrate_to_missing_def() {
    assert!(check_no_migrate_to_error("v1_header", V1_HEADER));
    assert!(check_no_migrate_to_error("v1_large_10_fields", &make_large_v1(10)));
    assert!(check_no_migrate_to_error("v1_large_100_fields", &make_large_v1(100)));
    assert!(check_no_migrate_to_error("v1_large_500_fields", &make_large_v1(500)));

    let with_ml_string = r#"{
    runtime = {
        tool_configs = {
            "test" = {
                command = m%%"
multiline string here
"%%,
            },
        },
    },
    tools = {},
    workflows = {},
    external_data = {},
    version = 1,
}"#;
    assert!(check_no_migrate_to_error("v1_multiline", with_ml_string));
    assert!(check_no_migrate_to_error("v1_10_workflows", &make_large_v1_workflows(10)));
    assert!(check_no_migrate_to_error("v1_50_workflows", &make_large_v1_workflows(50)));
    assert!(check_no_migrate_to_error("v1_100_workflows", &make_large_v1_workflows(100)));
}
