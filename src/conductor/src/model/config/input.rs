//! Tool input declaration types and workflow-step input-binding parser.
//!
//! These types represent declared tool input contracts and the interpolation
//! engine that resolves live binding strings at planning time.

use std::str::FromStr;

use mediapm_cas::Hash;
use serde::{Deserialize, Serialize};

use crate::error::ConductorError;

/// Tool input declaration entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ToolInputKind {
    /// Scalar string input (default).
    #[default]
    String,
    /// Ordered list of string arguments.
    ///
    /// Runtime treats this as list data and allows command-argument unpacking
    /// only through standalone unpack tokens in executable command templates.
    StringList,
}

/// Returns whether a value equals its type default for serde skip checks.
fn is_default_value<T>(value: &T) -> bool
where
    T: Default + PartialEq,
{
    value == &T::default()
}

/// Tool input declaration entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ToolInputSpec {
    /// Declared value kind for this input.
    ///
    /// When omitted, runtime defaults to [`ToolInputKind::String`].
    #[serde(default, skip_serializing_if = "is_default_value")]
    pub kind: ToolInputKind,
}

/// Prefix for `${external_data.<hash>}` interpolation expression bodies.
const INPUT_BINDING_EXTERNAL_DATA_PREFIX: &str = "external_data.";

/// Prefix for `${step_output.<step_id>.<output_name>}` expression bodies.
const INPUT_BINDING_STEP_OUTPUT_PREFIX: &str = "step_output.";

/// Prefix for `${env.<VAR_NAME>}` expression bodies.
const INPUT_BINDING_ENV_PREFIX: &str = "env.";

/// Token-start marker for interpolation spans in workflow-step input bindings.
const INPUT_BINDING_TOKEN_START: &str = "${";

/// Parsed token segment in one workflow-step input binding string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ParsedInputBindingSegment<'a> {
    /// Plain literal text payload.
    Literal(&'a str),
    /// Interpolated external-data reference looked up from top-level
    /// `external_data`.
    ExternalData {
        /// External-data reference hash key.
        hash: Hash,
    },
    /// Interpolated prior-step output reference.
    StepOutput {
        /// Dependency step id that produced the output.
        step_id: &'a str,
        /// Output name on the dependency step.
        output: &'a str,
        /// Optional ZIP member selector extracted from output bytes.
        zip_member: Option<&'a str>,
    },
    /// Interpolated environment-variable placeholder.
    ///
    /// Runtime keeps this as a literal `${env.<VAR_NAME>}` placeholder at
    /// input-binding resolution time so resolved-input persistence does not
    /// materialize host-secret values.
    Env {
        /// Environment-variable name token after the `env.` prefix.
        name: &'a str,
    },
}

/// Parses one optional trailing `:zip(<member>)` selector.
fn parse_optional_zip_selector(expression: &str) -> Result<(&str, Option<&str>), ConductorError> {
    if !expression.contains(":zip(") {
        return Ok((expression, None));
    }

    let Some(without_suffix) = expression.strip_suffix(')') else {
        return Err(ConductorError::Workflow(format!(
            "invalid input binding expression '${{{expression}}}'; malformed :zip(...) selector"
        )));
    };

    let Some((selector, member)) = without_suffix.rsplit_once(":zip(") else {
        return Err(ConductorError::Workflow(format!(
            "invalid input binding expression '${{{expression}}}'; malformed :zip(...) selector"
        )));
    };

    let member = member.trim();
    if member.is_empty() {
        return Err(ConductorError::Workflow(format!(
            "invalid input binding expression '${{{expression}}}'; :zip(...) requires one non-empty member key"
        )));
    }
    if member.contains('/') || member.contains('\\') {
        return Err(ConductorError::Workflow(format!(
            "invalid input binding expression '${{{expression}}}'; :zip(...) member key must be flat and must not contain path separators"
        )));
    }

    if selector.trim().is_empty() {
        return Err(ConductorError::Workflow(format!(
            "invalid input binding expression '${{{expression}}}'; :zip(...) requires one non-empty selector prefix"
        )));
    }

    Ok((selector.trim(), Some(member)))
}

/// Parses one `${...}` expression body from a workflow-step input binding.
fn parse_input_binding_expression<'a>(
    expression: &'a str,
    binding: &str,
) -> Result<ParsedInputBindingSegment<'a>, ConductorError> {
    if expression.contains(":file(") || expression.contains(":folder(") {
        return Err(ConductorError::Workflow(format!(
            "unsupported input binding expression '${{{expression}}}' in '{binding}'; supported interpolation forms are '${{external_data.<hash>}}', '${{step_output.<step_id>.<output_name>}}', and '${{env.<VAR_NAME>}}'. Input bindings do not support materialization directives like ':file(...)' or ':folder(...)'"
        )));
    }

    let (selector_expression, zip_member) = parse_optional_zip_selector(expression)?;

    if let Some(hash_text) = selector_expression.strip_prefix(INPUT_BINDING_EXTERNAL_DATA_PREFIX) {
        let hash_text = hash_text.trim();
        if hash_text.is_empty() {
            return Err(ConductorError::Workflow(
                "input binding '${external_data.<hash>}' requires a non-empty <hash>".to_string(),
            ));
        }
        let hash = Hash::from_str(hash_text).map_err(|error| {
            ConductorError::Workflow(format!(
                "input binding '${{external_data.{hash_text}}}' must reference a valid CAS hash key: {error}"
            ))
        })?;
        if zip_member.is_some() {
            return Err(ConductorError::Workflow(format!(
                "unsupported input binding expression '${{{expression}}}' in '{binding}'; :zip(...) is currently supported only for step_output references"
            )));
        }
        return Ok(ParsedInputBindingSegment::ExternalData { hash });
    }

    if let Some(selector) = selector_expression.strip_prefix(INPUT_BINDING_STEP_OUTPUT_PREFIX) {
        let Some((step_id, output)) = selector.split_once('.') else {
            return Err(ConductorError::Workflow(
                "input binding '${step_output.<step_id>.<output_name>}' requires both step id and output name"
                    .to_string(),
            ));
        };
        if step_id.trim().is_empty() || output.trim().is_empty() {
            return Err(ConductorError::Workflow(
                "input binding '${step_output.<step_id>.<output_name>}' requires non-empty step id and output name"
                    .to_string(),
            ));
        }
        return Ok(ParsedInputBindingSegment::StepOutput { step_id, output, zip_member });
    }

    if let Some(name) = selector_expression.strip_prefix(INPUT_BINDING_ENV_PREFIX) {
        let name = name.trim();
        if name.is_empty() {
            return Err(ConductorError::Workflow(
                "input binding '${env.<VAR_NAME>}' requires a non-empty <VAR_NAME>".to_string(),
            ));
        }
        if zip_member.is_some() {
            return Err(ConductorError::Workflow(format!(
                "unsupported input binding expression '${{{expression}}}' in '{binding}'; :zip(...) is currently supported only for step_output references"
            )));
        }
        return Ok(ParsedInputBindingSegment::Env { name });
    }

    Err(ConductorError::Workflow(format!(
        "unsupported input binding expression '${{{expression}}}' in '{binding}'; supported interpolation forms are '${{external_data.<hash>}}', '${{step_output.<step_id>.<output_name>}}', '${{step_output.<step_id>.<output_name>:zip(<member>)}}', and '${{env.<VAR_NAME>}}'. Input bindings do not support materialization directives like ':file(...)' or ':folder(...)'"
    )))
}

/// Parses one workflow-step input binding string into interpolation segments.
///
/// Rules:
/// - plain text outside `${...}` tokens is preserved as literal content,
/// - supported interpolation expressions are `${external_data.<hash>}`,
///   `${step_output.<step_id>.<output_name>}`, and `${env.<VAR_NAME>}`,
/// - `${step_output.<step_id>.<output_name>:zip(<member>)}` additionally
///   selects one ZIP member from the referenced output bytes,
/// - unsupported `${...}` expressions fail fast with explicit errors,
/// - `${...` without a closing `}` fails fast.
pub(crate) fn parse_input_binding(
    binding: &str,
) -> Result<Vec<ParsedInputBindingSegment<'_>>, ConductorError> {
    let mut segments = Vec::new();
    let mut cursor = 0usize;

    while let Some(start_relative) = binding[cursor..].find(INPUT_BINDING_TOKEN_START) {
        let token_start = cursor + start_relative;
        if token_start > cursor {
            segments.push(ParsedInputBindingSegment::Literal(&binding[cursor..token_start]));
        }

        let expression_start = token_start + INPUT_BINDING_TOKEN_START.len();
        let Some(end_relative) = binding[expression_start..].find('}') else {
            return Err(ConductorError::Workflow(format!(
                "input binding '{binding}' contains '${{' without a matching closing '}}'"
            )));
        };
        let expression_end = expression_start + end_relative;
        let expression = &binding[expression_start..expression_end];
        segments.push(parse_input_binding_expression(expression, binding)?);
        cursor = expression_end + 1;
    }

    if cursor < binding.len() {
        segments.push(ParsedInputBindingSegment::Literal(&binding[cursor..]));
    }

    if segments.is_empty() {
        segments.push(ParsedInputBindingSegment::Literal(binding));
    }

    Ok(segments)
}

#[cfg(test)]
mod tests {
    use super::{ParsedInputBindingSegment, parse_input_binding};

    /// Verifies `${env.<VAR_NAME>}` parses as one dedicated env segment.
    #[test]
    fn parse_input_binding_supports_env_segment() {
        let parsed = parse_input_binding("${env.RUNTIME_TOOL_DIR}").expect("env binding parses");
        assert_eq!(parsed, vec![ParsedInputBindingSegment::Env { name: "RUNTIME_TOOL_DIR" }]);
    }

    /// Verifies env interpolation can be mixed with surrounding literal text.
    #[test]
    fn parse_input_binding_supports_mixed_env_and_literals() {
        let parsed = parse_input_binding("prefix-${env.RUNTIME_TOOL_DIR}/bin")
            .expect("mixed env binding parses");
        assert_eq!(
            parsed,
            vec![
                ParsedInputBindingSegment::Literal("prefix-"),
                ParsedInputBindingSegment::Env { name: "RUNTIME_TOOL_DIR" },
                ParsedInputBindingSegment::Literal("/bin"),
            ]
        );
    }

    /// Verifies empty env names fail fast with actionable diagnostics.
    #[test]
    fn parse_input_binding_rejects_empty_env_name() {
        let error = parse_input_binding("${env.}").expect_err("empty env name should fail");
        let message = error.to_string();
        assert!(message.contains("${env.<VAR_NAME>}"));
        assert!(message.contains("non-empty"));
    }
}
