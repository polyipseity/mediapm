//! Full template interpolation engine for step input resolution.
//!
//! Supports `${step_output.<id>.<name>}`, `${external_data.<hash>}`,
//! `${env.<VAR>}`, `${*<token>}`, `${inputs.<name>}`, platform conditionals,
//! exists ternaries (`${ref ? true | false}`), comparison operators, escape
//! sequences, and post-processing selectors (`:zip(member)`, `:file(path)`,
//! `:folder(path)`).
//!
//! # Resolution order
//!
//! 1. Escape `\${` → literal `$`.
//! 2. Extract `${...}` segments with proper brace nesting.
//! 3. For each segment, determine whether it is a:
//!    - **Platform conditional** — `context.os <op> "value" ? ... : ...`
//!    - **Exists ternary** — `ref_expr ? true | false`
//!    - **Base reference** — `step_output.<id>.<name>`, `external_data.<hash>`,
//!      `env.<VAR>`, `*<token>`, or `inputs.<name>`.
//! 4. Apply chained post-processing selectors (`:zip`, `:file`, `:folder`)
//!    from right to left.
//! 5. Resolve and stringify (for [`resolve_template`]) or preserve as bytes
//!    (for [`resolve_content`]).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use mediapm_cas::Hash;

use crate::error::ConductorError;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Context injected into every template resolution call.
///
/// Generic parameter `C` is the concrete CAS type (see [`mediapm_cas::CasApi`]).
pub struct TemplateContext<'a, C> {
    /// Optional CAS handle for fetching external data and step-output content.
    pub cas: Option<&'a C>,
    /// Step outputs: map from step_id → (output_name → content hash).
    pub step_outputs: &'a BTreeMap<String, BTreeMap<String, Hash>>,
    /// Host environment variables.
    pub env_vars: &'a BTreeMap<String, String>,
    /// Unpack tokens: token name → raw binary data (archives, blobs).
    pub tokens: &'a BTreeMap<String, Vec<u8>>,
    /// Sandbox directory for materialization directives (`:file`, `:folder`).
    pub sandbox_dir: Option<&'a Path>,
    /// Host OS string for platform conditional evaluation
    /// (e.g. `"macos"`, `"linux"`, `"windows"`).
    pub host_os: &'a str,
    /// Resolved input values for the current step.
    /// Populated for command-parts resolution; empty during input resolution.
    pub inputs: &'a BTreeMap<String, String>,
}

/// Result of resolving a single template reference.
#[derive(Debug, Clone)]
pub enum ResolvedValue {
    /// Plain string value (most common — step output, env var, etc.).
    String(String),
    /// Binary data (from external_data, zip extraction, or unpack token).
    Bytes(Vec<u8>),
    /// File materialized to the sandbox at the given path.
    MaterializedFile(PathBuf),
    /// Folder materialized to the sandbox at the given path.
    MaterializedFolder(PathBuf),
}

impl ResolvedValue {
    /// Convert to `String` for interpolation into a template result.
    fn into_string(self) -> Result<String, ConductorError> {
        match self {
            ResolvedValue::String(s) => Ok(s),
            ResolvedValue::Bytes(b) => String::from_utf8(b).map_err(|e| {
                ConductorError::Workflow(format!(
                    "template: binary content is not valid UTF-8: {e}",
                ))
            }),
            ResolvedValue::MaterializedFile(p) | ResolvedValue::MaterializedFolder(p) => {
                Ok(p.to_string_lossy().to_string())
            }
        }
    }

    /// Convert to `Vec<u8>` for binary content resolution.
    fn into_bytes(self) -> Result<Vec<u8>, ConductorError> {
        match self {
            ResolvedValue::String(s) => Ok(s.into_bytes()),
            ResolvedValue::Bytes(b) => Ok(b),
            ResolvedValue::MaterializedFile(_) | ResolvedValue::MaterializedFolder(_) => {
                Err(ConductorError::Workflow(
                    "template: cannot convert materialized path to raw bytes".to_string(),
                ))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Parsed reference types
// ---------------------------------------------------------------------------

/// The kind of a base reference (before post-processing selectors).
#[derive(Debug, Clone, PartialEq)]
enum BaseRef {
    /// `${step_output.<step_id>.<output_name>}`
    StepOutput { step_id: String, output: String },
    /// `${external_data.<hash>}`
    ExternalData(Hash),
    /// `${env.<VAR>}`
    Env(String),
    /// `${*<token>}` — unpack token for archive expansion.
    UnpackToken(String),
    /// `${inputs.<name>}` — resolved step input value.
    Input(String),
    /// `${*inputs.<name>}` — splat a string_list input into multiple command
    /// parts (JSON-decodes `Vec<String>` from the resolved value).
    UnpackInput(String),
}

/// A post-processing selector applied to a resolved value.
#[derive(Debug, Clone, PartialEq)]
enum PostSelector {
    /// `:zip(<member>)` — extract a member from ZIP binary data.
    Zip(String),
    /// `:file(<path>)` — write resolved content to sandbox, return path.
    File(String),
    /// `:folder(<path>)` — extract archive to sandbox folder, return path.
    Folder(String),
}

/// A fully parsed reference expression.
#[derive(Debug, Clone, PartialEq)]
struct ParsedReference {
    /// Optional conditional guard (comparison or exists).
    conditional: Option<ConditionalExpr>,
    /// Base reference (filled when there is no conditional).
    base: Option<BaseRef>,
    /// Chained post-processing selectors (applied left-to-right).
    selectors: Vec<PostSelector>,
}

/// A parsed platform conditional `context.os <op> "value" ? <true_val> : <false_val>`.
#[derive(Debug, Clone, PartialEq)]
struct ParsedConditional {
    /// Left-hand side of the comparison (e.g. `context.os`).
    lhs: String,
    /// Comparison operator.
    op: ComparisonOp,
    /// Right-hand side literal value (quoted string).
    rhs: String,
    /// True branch expression (may contain nested `${}`, resolved recursively).
    true_expr: String,
    /// False branch expression.
    false_expr: String,
}

/// A parsed exists ternary `${ref_expr ? true_branch | false_branch}`.
///
/// The `ref_expr` is resolved; if non-empty, the true branch is used,
/// otherwise the false branch.
#[derive(Debug, Clone, PartialEq)]
struct ParsedExistsTernary {
    /// Reference expression to check for existence (may include selectors).
    ref_expr: String,
    /// Expression when the reference resolves to a non-empty value.
    true_expr: String,
    /// Expression when the reference is empty or absent.
    false_expr: String,
}

/// Types of conditional expressions in templates.
#[derive(Debug, Clone, PartialEq)]
enum ConditionalExpr {
    /// Comparison conditional: `context.os == "value" ? true_branch : false_branch`.
    Comparison(ParsedConditional),
    /// Exists ternary: `ref_expr ? true_branch | false_branch`.
    Exists(ParsedExistsTernary),
}

/// Supported comparison operators.
#[derive(Debug, Clone, Copy, PartialEq)]
enum ComparisonOp {
    Eq,  // ==
    Neq, // !=
    Lt,  // <
    Le,  // <=
    Gt,  // >
    Ge,  // >=
}

impl ComparisonOp {
    fn evaluate(&self, lhs: &str, rhs: &str) -> bool {
        match self {
            ComparisonOp::Eq => lhs == rhs,
            ComparisonOp::Neq => lhs != rhs,
            ComparisonOp::Lt => lhs < rhs,
            ComparisonOp::Le => lhs <= rhs,
            ComparisonOp::Gt => lhs > rhs,
            ComparisonOp::Ge => lhs >= rhs,
        }
    }
}

// ---------------------------------------------------------------------------
// Parsing constants
// ---------------------------------------------------------------------------

/// Sentinel used to represent escaped `\${` during interpolation.
const ESCAPED_SENTINEL: &str = "\x00ESC_DOLLAR_BRACE\x00";

// ---------------------------------------------------------------------------
// Main entry points
// ---------------------------------------------------------------------------

/// Resolves a template string, returning the fully interpolated result.
///
/// All references are resolved and stringified. Binary content from CAS is
/// decoded as UTF-8.
pub async fn resolve_template<C: mediapm_cas::CasApi + Send + Sync>(
    template: &str,
    ctx: &TemplateContext<'_, C>,
) -> Result<String, ConductorError> {
    // 1. Handle escape sequences.
    let processed = template.replace("\\${", ESCAPED_SENTINEL);

    // 2. Parse into segments.
    let segments = parse_into_segments(&processed)?;

    // 3. Resolve each reference segment.
    let mut result = String::new();
    for segment in &segments {
        match segment {
            Segment::Text(text) => result.push_str(text),
            Segment::Reference(parsed) => {
                let value = resolve_parsed_reference(parsed, ctx).await?;
                result.push_str(&value.into_string()?);
            }
        }
    }

    // 4. Restore escape sentinels.
    Ok(result.replace(ESCAPED_SENTINEL, "${"))
}

/// Resolves a template string into raw binary content.
///
/// For references that resolve to binary (external_data, zip members, unpack
/// tokens), the raw bytes are returned. For string values, UTF-8 encoding
/// is used.
pub async fn resolve_content<C: mediapm_cas::CasApi + Send + Sync>(
    template: &str,
    ctx: &TemplateContext<'_, C>,
) -> Result<Vec<u8>, ConductorError> {
    let processed = template.replace("\\${", ESCAPED_SENTINEL);
    let segments = parse_into_segments(&processed)?;

    // If the template is a single reference, return its bytes directly.
    if segments.len() == 1 {
        if let Segment::Reference(parsed) = &segments[0] {
            let value = resolve_parsed_reference(parsed, ctx).await?;
            return value.into_bytes();
        }
    }

    // Otherwise, resolve as string and encode.
    let string_result = resolve_template(template, ctx).await?;
    Ok(string_result.into_bytes())
}

/// Resolves a slice of command parts, expanding splat references
/// (`${*inputs.<name>}`) into their constituent elements.
///
/// Each part is resolved as a template.  If the raw part matches the splat
/// pattern `${*inputs.<name>}`, the named input is JSON-decoded as
/// `Vec<String>` and its elements are inserted in place.
pub async fn resolve_command_parts<C: mediapm_cas::CasApi + Send + Sync>(
    command_parts: &[String],
    ctx: &TemplateContext<'_, C>,
) -> Result<Vec<String>, ConductorError> {
    let mut resolved = Vec::with_capacity(command_parts.len());

    for part in command_parts {
        let trimmed = part.trim();

        // Detect splat reference: `${*inputs.<name>}` (possibly with
        // whitespace around the braces).
        if let Some(splat_inner) = trimmed
            .strip_prefix("${")
            .and_then(|s| s.strip_suffix('}'))
            .map(|s| s.trim())
            .and_then(|s| s.strip_prefix("*inputs."))
        {
            let name = splat_inner.trim().to_string();
            if name.is_empty() {
                return Err(ConductorError::Workflow(
                    "template: empty inputs splat in command parts '${{*inputs.}}'".to_string(),
                ));
            }
            let raw = ctx.inputs.get(&name).ok_or_else(|| {
                ConductorError::Workflow(format!(
                    "template: input '${{*inputs.{name}}}' in command parts not found",
                ))
            })?;
            let parts: Vec<String> = serde_json::from_str(raw).map_err(|e| {
                ConductorError::Workflow(format!(
                    "template: input '${{*inputs.{name}}}' is not a valid \
                     JSON array of strings: {e}",
                ))
            })?;
            resolved.extend(parts);
        } else {
            let value = resolve_template(part, ctx).await?;
            resolved.push(value);
        }
    }

    Ok(resolved)
}

// ---------------------------------------------------------------------------
// Segment parsing
// ---------------------------------------------------------------------------

/// A segment of a parsed template.
#[derive(Debug, Clone)]
enum Segment {
    /// Literal text (pass through).
    Text(String),
    /// A `${...}` reference.
    Reference(ParsedReference),
}

/// Parses a template string into segments of text and `${...}` references.
fn parse_into_segments(template: &str) -> Result<Vec<Segment>, ConductorError> {
    let bytes = template.as_bytes();
    let len = bytes.len();
    let mut segments: Vec<Segment> = Vec::new();
    let mut pos = 0;

    while pos < len {
        // Look for `${`
        if pos + 1 < len && bytes[pos] == b'$' && bytes[pos + 1] == b'{' {
            let open = pos;
            pos += 2; // skip past `${`

            // Find matching `}` with proper nesting.
            let mut depth: u32 = 1;
            let start = pos;
            while pos < len && depth > 0 {
                match bytes[pos] {
                    b'{' => depth += 1,
                    b'}' => depth -= 1,
                    _ => {}
                }
                if depth > 0 {
                    pos += 1;
                }
            }

            if depth != 0 {
                return Err(ConductorError::Workflow(format!(
                    "template: unclosed '${{' at position {open}",
                )));
            }

            let expr = &template[start..pos];
            let parsed = parse_reference_expr(expr)?;
            segments.push(Segment::Reference(parsed));
            pos += 1; // skip past `}`
        } else {
            // Accumulate literal text.
            let text_start = pos;
            while pos < len && !(bytes[pos] == b'$' && pos + 1 < len && bytes[pos + 1] == b'{') {
                pos += 1;
            }
            segments.push(Segment::Text(template[text_start..pos].to_string()));
        }
    }

    Ok(segments)
}

// ---------------------------------------------------------------------------
// Expression parsing
// ---------------------------------------------------------------------------

/// Parses the content inside `${...}` (after the opening `${` and before the
/// matching `}`).
fn parse_reference_expr(expr: &str) -> Result<ParsedReference, ConductorError> {
    let expr = expr.trim();

    // Check for platform conditional: `context.os <op> "value" ? ... : ...`
    if let Some(cond) = try_parse_conditional(expr) {
        return Ok(ParsedReference {
            conditional: Some(ConditionalExpr::Comparison(cond)),
            base: None,
            selectors: Vec::new(),
        });
    }

    // Check for exists ternary: `ref_expr ? true_branch | false_branch`
    if let Some(exists) = try_parse_exists_ternary(expr) {
        return Ok(ParsedReference {
            conditional: Some(ConditionalExpr::Exists(exists)),
            base: None,
            selectors: Vec::new(),
        });
    }

    // Split off post-processing selectors (`:zip(...)`, `:file(...)`, `:folder(...)`).
    let (base_str, selectors) = parse_selectors(expr)?;

    // Parse the base reference.
    let base = parse_base_ref(&base_str)?;

    Ok(ParsedReference { conditional: None, base: Some(base), selectors })
}

/// Tries to parse an exists ternary expression.
///
/// Format: `<ref_expr> ? <true_branch> | <false_branch>`
///
/// The `<ref_expr>` is resolved and checked for non-empty existence.  If the
/// reference expression contains a `?` at depth zero (from selectors like
/// `:file(...)`) this returns `None` and the caller falls through to normal
/// reference parsing.
fn try_parse_exists_ternary(expr: &str) -> Option<ParsedExistsTernary> {
    let trimmed = expr.trim();

    // Must not start with `context.` (that's a comparison conditional).
    if trimmed.starts_with("context.") {
        return None;
    }

    // Find the first unbraced `?`.
    let bytes = trimmed.as_bytes();
    let _len = bytes.len();
    let mut depth = 0u32;
    let mut q_pos = None;

    for (i, &b) in bytes.iter().enumerate() {
        match b {
            b'{' => depth += 1,
            b'}' => depth = depth.saturating_sub(1),
            b'?' if depth == 0 => {
                q_pos = Some(i);
                break;
            }
            _ => {}
        }
    }

    let q_pos = q_pos?;

    // No `?` at position 0 — there must be a ref expression before it.
    if q_pos == 0 {
        return None;
    }

    let ref_expr = trimmed[..q_pos].trim().to_string();
    let rest = trimmed[q_pos + 1..].trim();

    // Find the first unbraced `|` in the rest (branch separator).
    let bytes2 = rest.as_bytes();
    let _len2 = bytes2.len();
    let mut depth2 = 0u32;
    let mut pipe_pos = None;

    for (i, &b) in bytes2.iter().enumerate() {
        match b {
            b'{' => depth2 += 1,
            b'}' => depth2 = depth2.saturating_sub(1),
            b'|' if depth2 == 0 => {
                pipe_pos = Some(i);
                break;
            }
            _ => {}
        }
    }

    let pipe_pos = pipe_pos?;
    let true_expr = rest[..pipe_pos].trim().to_string();
    let false_expr = rest[pipe_pos + 1..].trim().to_string();

    Some(ParsedExistsTernary { ref_expr, true_expr, false_expr })
}

/// Tries to parse a platform conditional expression.
///
/// Format: `context.os <op> "value" ? <true_expr> : <false_expr>`
fn try_parse_conditional(expr: &str) -> Option<ParsedConditional> {
    let trimmed = expr.trim();
    if !trimmed.starts_with("context.") {
        return None;
    }

    // Find the `?` that separates condition from branches.
    let bytes = trimmed.as_bytes();
    let len = bytes.len();
    let mut q_pos = None;
    let mut depth = 0u32;

    for (i, &b) in bytes.iter().enumerate() {
        match b {
            b'{' => depth += 1,
            b'}' => depth = depth.saturating_sub(1),
            b'?' if depth == 0 => {
                q_pos = Some(i);
                break;
            }
            _ => {}
        }
    }

    let q_pos = q_pos?;
    if q_pos + 1 >= len {
        return None;
    }

    // Split condition from the rest.
    let condition_part = trimmed[..q_pos].trim();
    let rest = trimmed[q_pos + 1..].trim();

    // Find the `:` that separates true/false branches (with brace awareness).
    let bytes2 = rest.as_bytes();
    let mut colon_pos = None;
    let mut depth2 = 0u32;

    for (i, &b) in bytes2.iter().enumerate() {
        match b {
            b'{' => depth2 += 1,
            b'}' => depth2 = depth2.saturating_sub(1),
            b':' if depth2 == 0 => {
                colon_pos = Some(i);
                break;
            }
            _ => {}
        }
    }

    let colon_pos = colon_pos?;
    let true_expr = rest[..colon_pos].trim().to_string();
    let false_expr = rest[colon_pos + 1..].trim().to_string();

    // Parse the condition part: `lhs <op> "rhs"`
    let (lhs, op_str, quoted_rhs) = parse_condition_expression(condition_part)?;
    let rhs = unquote_string(quoted_rhs)?;

    let op = match op_str {
        "==" => ComparisonOp::Eq,
        "!=" => ComparisonOp::Neq,
        "<" => ComparisonOp::Lt,
        "<=" => ComparisonOp::Le,
        ">" => ComparisonOp::Gt,
        ">=" => ComparisonOp::Ge,
        _ => return None,
    };

    Some(ParsedConditional {
        lhs: lhs.to_string(),
        op,
        rhs: rhs.to_string(),
        true_expr,
        false_expr,
    })
}

/// Parses the condition part `context.os == "value"` into (lhs, op, quoted_rhs).
fn parse_condition_expression(s: &str) -> Option<(&str, &str, &str)> {
    // Try each operator in descending length order to match `<=`, `>=`, `!=` first.
    for op_str in &["<=", ">=", "!=", "==", "<", ">"] {
        if let Some(pos) = s.find(op_str) {
            let lhs = s[..pos].trim();
            let rhs = s[pos + op_str.len()..].trim();
            if !lhs.is_empty() && !rhs.is_empty() {
                return Some((lhs, op_str, rhs));
            }
        }
    }
    None
}

/// Strips surrounding quotes from a string value. Returns `None` if not quoted.
fn unquote_string(s: &str) -> Option<&str> {
    let s = s.trim();
    if (s.starts_with('"') && s.ends_with('"')) || (s.starts_with('\'') && s.ends_with('\'')) {
        Some(&s[1..s.len() - 1])
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Selector parsing
// ---------------------------------------------------------------------------

/// Splits an expression into a base reference string and a list of post-processing
/// selectors (parsed right-to-left).  Returns `("base_expr", [PostSelector])`.
fn parse_selectors(expr: &str) -> Result<(String, Vec<PostSelector>), ConductorError> {
    let expr = expr.trim();
    let mut remaining = expr;
    let mut selectors: Vec<PostSelector> = Vec::new();

    // Selectors are appended right-to-left: `base:zip(a):file(b)`.
    loop {
        let trimmed = remaining.trim_end();
        if let Some((sel, rest)) = split_last_selector(trimmed) {
            selectors.push(sel);
            remaining = rest;
        } else {
            break;
        }
    }

    selectors.reverse(); // Restore application order (leftmost selector first).
    Ok((remaining.to_string(), selectors))
}

/// Parses the last `:name(args)` suffix from an expression.
fn split_last_selector(expr: &str) -> Option<(PostSelector, &str)> {
    for (prefix, ctor) in &SEL_PREFIXES {
        if let Some((arg, remaining)) = split_suffix_by_prefix(expr, prefix) {
            let selector = ctor(arg.to_string());
            return Some((selector, remaining));
        }
    }
    None
}

/// Static list of supported selector prefixes and their constructors.
static SEL_PREFIXES: [(&str, fn(String) -> PostSelector); 3] = [
    (":zip(", |s| PostSelector::Zip(s)),
    (":file(", |s| PostSelector::File(s)),
    (":folder(", |s| PostSelector::Folder(s)),
];

/// Splits `expr` at the last occurrence of `prefix`, returning the argument
/// inside `prefix...)` and the expression before `prefix`.
///
/// Handles nested parentheses by tracking depth from the end.
fn split_suffix_by_prefix<'a>(expr: &'a str, prefix: &str) -> Option<(&'a str, &'a str)> {
    if !expr.ends_with(')') {
        return None;
    }

    // Find the matching '(' by scanning backwards from the end.
    let bytes = expr.as_bytes();
    let len = bytes.len();
    let mut depth = 0u32;
    let mut paren_open = None;

    for i in (0..len).rev() {
        match bytes[i] {
            b')' => depth += 1,
            b'(' => {
                if depth == 0 {
                    return None; // unbalanced
                }
                depth -= 1;
                if depth == 0 {
                    paren_open = Some(i);
                    break;
                }
            }
            _ => {}
        }
    }

    let open_pos = paren_open?;

    // Check that the prefix immediately precedes the '('.
    let prefix_len = prefix.len();
    if open_pos + 1 < prefix_len {
        return None;
    }
    let prefix_start = open_pos + 1 - prefix_len; // position of ':' in `:name(`
    if expr[prefix_start..=open_pos] != *prefix {
        return None;
    }

    let arg = &expr[open_pos + 1..len - 1]; // inside the parens
    let remaining = &expr[..prefix_start];
    Some((arg, remaining))
}

// ---------------------------------------------------------------------------
// Base reference parsing
// ---------------------------------------------------------------------------

fn parse_base_ref(s: &str) -> Result<BaseRef, ConductorError> {
    let s = s.trim();

    // `${step_output.<step_id>.<output_name>}`
    if let Some(rest) = s.strip_prefix("step_output.") {
        let parts: Vec<&str> = rest.splitn(2, '.').collect();
        if parts.len() != 2 || parts[0].is_empty() || parts[1].is_empty() {
            return Err(ConductorError::Workflow(format!(
                "template: invalid step_output reference '${{step_output.{rest}}}' — \
                 expected 'step_output.<step_id>.<output_name>'",
            )));
        }
        return Ok(BaseRef::StepOutput {
            step_id: parts[0].to_string(),
            output: parts[1].to_string(),
        });
    }

    // `${external_data.<hash>}`
    if let Some(rest) = s.strip_prefix("external_data.") {
        let hash = rest.trim().parse::<Hash>().map_err(|e| {
            ConductorError::Workflow(format!(
                "template: invalid external_data hash '${{external_data.{rest}}}': {e}",
            ))
        })?;
        return Ok(BaseRef::ExternalData(hash));
    }

    // `${env.<VAR>}`
    if let Some(var_name) = s.strip_prefix("env.") {
        if var_name.is_empty() {
            return Err(ConductorError::Workflow(
                "template: empty env variable name '${{env.}}'".to_string(),
            ));
        }
        return Ok(BaseRef::Env(var_name.to_string()));
    }

    // `${*inputs.<name>}` — must check before generic `*` to avoid misparse.
    if let Some(name) = s.strip_prefix("*inputs.") {
        if name.is_empty() {
            return Err(ConductorError::Workflow(
                "template: empty inputs splat '${{*inputs.}}'".to_string(),
            ));
        }
        return Ok(BaseRef::UnpackInput(name.to_string()));
    }

    // `${*<token>}`
    if let Some(token) = s.strip_prefix('*') {
        if token.is_empty() {
            return Err(ConductorError::Workflow(
                "template: empty unpack token '${{*}}'".to_string(),
            ));
        }
        return Ok(BaseRef::UnpackToken(token.to_string()));
    }

    // `${inputs.<name>}`
    if let Some(name) = s.strip_prefix("inputs.") {
        if name.is_empty() {
            return Err(ConductorError::Workflow(
                "template: empty inputs reference '${{inputs.}}'".to_string(),
            ));
        }
        return Ok(BaseRef::Input(name.to_string()));
    }

    Err(ConductorError::Workflow(format!(
        "template: unrecognized reference '${{{s}}}' — expected step_output, \
         external_data, env, inputs, or *token",
    )))
}

// ---------------------------------------------------------------------------
// Reference resolution
// ---------------------------------------------------------------------------

/// Resolves a parsed reference to a [`ResolvedValue`].
async fn resolve_parsed_reference<C: mediapm_cas::CasApi + Send + Sync>(
    parsed: &ParsedReference,
    ctx: &TemplateContext<'_, C>,
) -> Result<ResolvedValue, ConductorError> {
    // Handle conditional first (comparison or exists ternary).
    if let Some(cond) = &parsed.conditional {
        let resolved = match cond {
            ConditionalExpr::Comparison(c) => evaluate_comparison_conditional(c, ctx).await?,
            ConditionalExpr::Exists(e) => evaluate_exists_ternary(e, ctx).await?,
        };
        return Ok(ResolvedValue::String(resolved));
    }

    // Resolve base reference.
    let base = parsed.base.as_ref().ok_or_else(|| {
        ConductorError::Internal(
            "template: parsed reference without conditional or base".to_string(),
        )
    })?;

    let mut value = resolve_base_ref(base, ctx).await?;

    // Apply post-processing selectors in order.
    for selector in &parsed.selectors {
        value = apply_selector(value, selector, ctx).await?;
    }

    Ok(value)
}

/// Resolves a base reference to a [`ResolvedValue`].
async fn resolve_base_ref<C: mediapm_cas::CasApi + Send + Sync>(
    base: &BaseRef,
    ctx: &TemplateContext<'_, C>,
) -> Result<ResolvedValue, ConductorError> {
    match base {
        BaseRef::StepOutput { step_id, output } => resolve_step_output(step_id, output, ctx),
        BaseRef::ExternalData(hash) => resolve_external_data(*hash, ctx).await,
        BaseRef::Env(var_name) => resolve_env(var_name, ctx),
        BaseRef::UnpackToken(token) => resolve_unpack_token(token, ctx),
        BaseRef::Input(name) => resolve_input(name, ctx),
        BaseRef::UnpackInput(name) => resolve_unpack_input(name, ctx),
    }
}

/// Resolves `${step_output.<step_id>.<output>}`.
fn resolve_step_output<C: mediapm_cas::CasApi + Send + Sync>(
    step_id: &str,
    output: &str,
    ctx: &TemplateContext<'_, C>,
) -> Result<ResolvedValue, ConductorError> {
    let hash =
        ctx.step_outputs.get(step_id).and_then(|outputs| outputs.get(output)).ok_or_else(|| {
            ConductorError::Workflow(format!(
                "template: step output '${{step_output.{step_id}.{output}}}' not found \
                 (step id '{step_id}' or output '{output}' missing)",
            ))
        })?;

    Ok(ResolvedValue::String(hash.to_string()))
}

/// Resolves `${external_data.<hash>}` by fetching from CAS.
async fn resolve_external_data<C: mediapm_cas::CasApi + Send + Sync>(
    hash: Hash,
    ctx: &TemplateContext<'_, C>,
) -> Result<ResolvedValue, ConductorError> {
    let cas = ctx.cas.ok_or_else(|| {
        ConductorError::Workflow(format!(
            "template: external_data reference '${{external_data.{hash}}}' requires \
             a CAS handle but none was provided",
        ))
    })?;

    let data = cas.get(hash).await.map_err(|e| {
        ConductorError::Workflow(format!(
            "template: external_data hash '{hash}' not found in CAS: {e}",
        ))
    })?;

    Ok(ResolvedValue::Bytes(data.to_vec()))
}

/// Resolves `${env.<VAR>}` from host environment variables.
fn resolve_env<C: mediapm_cas::CasApi + Send + Sync>(
    var_name: &str,
    ctx: &TemplateContext<'_, C>,
) -> Result<ResolvedValue, ConductorError> {
    let value = ctx.env_vars.get(var_name).ok_or_else(|| {
        ConductorError::Workflow(format!(
            "template: environment variable '${{env.{var_name}}}' not found",
        ))
    })?;

    Ok(ResolvedValue::String(value.clone()))
}

/// Resolves `${*<token>}` from the tokens map.
fn resolve_unpack_token<C: mediapm_cas::CasApi + Send + Sync>(
    token: &str,
    ctx: &TemplateContext<'_, C>,
) -> Result<ResolvedValue, ConductorError> {
    let data = ctx.tokens.get(token).ok_or_else(|| {
        ConductorError::Workflow(format!("template: unpack token '${{*{token}}}' not found",))
    })?;

    Ok(ResolvedValue::Bytes(data.clone()))
}

/// Resolves `${inputs.<name>}` from the resolved step inputs.
fn resolve_input<C: mediapm_cas::CasApi + Send + Sync>(
    name: &str,
    ctx: &TemplateContext<'_, C>,
) -> Result<ResolvedValue, ConductorError> {
    let value = ctx.inputs.get(name).ok_or_else(|| {
        ConductorError::Workflow(format!("template: input '${{inputs.{name}}}' not found",))
    })?;

    Ok(ResolvedValue::String(value.clone()))
}

/// Resolves `${*inputs.<name>}` — fetches the input value and JSON-decodes it
/// as `Vec<String>` for command-part splatting.
fn resolve_unpack_input<C: mediapm_cas::CasApi + Send + Sync>(
    name: &str,
    ctx: &TemplateContext<'_, C>,
) -> Result<ResolvedValue, ConductorError> {
    let raw = ctx.inputs.get(name).ok_or_else(|| {
        ConductorError::Workflow(format!("template: input '${{*inputs.{name}}}' not found",))
    })?;

    // Parse as JSON array of strings.
    let parts: Vec<String> = serde_json::from_str(raw).map_err(|e| {
        ConductorError::Workflow(format!(
            "template: input '${{*inputs.{name}}}' is not a valid JSON array of strings: {e}",
        ))
    })?;

    Ok(ResolvedValue::String(serde_json::to_string(&parts).map_err(|e| {
        ConductorError::Workflow(format!(
            "template: failed to serialize splat parts for '${{*inputs.{name}}}': {e}",
        ))
    })?))
}

// ---------------------------------------------------------------------------
// Conditional evaluation
// ---------------------------------------------------------------------------

/// Evaluates an exists ternary and returns the resolved branch value.
///
/// Format: `ref_expr ? true_branch | false_branch`.
/// The `ref_expr` is resolved as a template; if the result is non-empty,
/// `true_branch` is used, otherwise `false_branch`.
async fn evaluate_exists_ternary<C: mediapm_cas::CasApi + Send + Sync>(
    cond: &ParsedExistsTernary,
    ctx: &TemplateContext<'_, C>,
) -> Result<String, ConductorError> {
    // Resolve the reference expression.
    // Box::pin breaks the async recursion cycle.
    let resolved_ref = Box::pin(resolve_template(&cond.ref_expr, ctx)).await?;

    let branch = if resolved_ref.is_empty() { &cond.false_expr } else { &cond.true_expr };

    // The branch may itself contain `${...}` references — resolve recursively.
    Box::pin(resolve_template(branch, ctx)).await
}

/// Evaluates a parsed comparison conditional and returns the resolved branch value.
async fn evaluate_comparison_conditional<C: mediapm_cas::CasApi + Send + Sync>(
    cond: &ParsedConditional,
    ctx: &TemplateContext<'_, C>,
) -> Result<String, ConductorError> {
    // Determine the left-hand side value.
    let lhs_value = resolve_conditional_operand(&cond.lhs, ctx).await?;

    // The right-hand side is always a quoted string literal.
    let rhs_value = &cond.rhs;

    let branch =
        if cond.op.evaluate(&lhs_value, rhs_value) { &cond.true_expr } else { &cond.false_expr };

    // The branch may itself contain `${...}` references — resolve recursively.
    // Box::pin breaks the async recursion cycle:
    // evaluate_comparison_conditional -> resolve_template -> resolve_parsed_reference
    // -> evaluate_comparison_conditional -> ...
    Box::pin(resolve_template(branch, ctx)).await
}

/// Resolves a conditional operand to a string for comparison.
///
/// Supports `context.os` (maps to ctx.host_os) and literal strings.
async fn resolve_conditional_operand<C: mediapm_cas::CasApi + Send + Sync>(
    operand: &str,
    ctx: &TemplateContext<'_, C>,
) -> Result<String, ConductorError> {
    let operand = operand.trim();

    // Check for context properties.
    if operand == "context.os" {
        return Ok(ctx.host_os.to_string());
    }

    // Check for quoted string literal.
    if (operand.starts_with('"') && operand.ends_with('"'))
        || (operand.starts_with('\'') && operand.ends_with('\''))
    {
        return Ok(operand[1..operand.len() - 1].to_string());
    }

    // Check for nested template reference.
    if operand.starts_with("${") && operand.ends_with('}') {
        // Use Box::pin to break the async recursion cycle:
        // resolve_template -> resolve_parsed_reference -> evaluate_conditional
        // -> resolve_conditional_operand -> resolve_template.
        return Box::pin(resolve_template(operand, ctx)).await;
    }

    // Fallback: treat as literal string.
    Ok(operand.to_string())
}

// ---------------------------------------------------------------------------
// Post-processing selectors
// ---------------------------------------------------------------------------

/// Applies a single post-processing selector to a resolved value.
async fn apply_selector<C: mediapm_cas::CasApi + Send + Sync>(
    value: ResolvedValue,
    selector: &PostSelector,
    ctx: &TemplateContext<'_, C>,
) -> Result<ResolvedValue, ConductorError> {
    match selector {
        PostSelector::Zip(member) => apply_zip_selector(value, member).await,
        PostSelector::File(path) => apply_file_selector(value, path, ctx).await,
        PostSelector::Folder(path) => apply_folder_selector(value, path, ctx).await,
    }
}

/// `:zip(member)` — extracts a member from ZIP binary content.
async fn apply_zip_selector(
    value: ResolvedValue,
    member: &str,
) -> Result<ResolvedValue, ConductorError> {
    let data = value.into_bytes()?;
    let extracted = extract_zip_member(&data, member)?;
    Ok(ResolvedValue::Bytes(extracted))
}

/// `:file(path)` — writes resolved content to sandbox and returns the path.
async fn apply_file_selector<C: mediapm_cas::CasApi + Send + Sync>(
    value: ResolvedValue,
    path: &str,
    ctx: &TemplateContext<'_, C>,
) -> Result<ResolvedValue, ConductorError> {
    let sandbox = ctx.sandbox_dir.ok_or_else(|| {
        ConductorError::Workflow(
            "template: ':file' selector requires a sandbox directory".to_string(),
        )
    })?;

    let data = value.into_bytes()?;
    let target_path = sandbox.join(path);

    // Create parent directories if needed.
    if let Some(parent) = target_path.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(|source| {
            ConductorError::io("template: create parent directory for :file", parent, source)
        })?;
    }

    tokio::fs::write(&target_path, &data).await.map_err(|source| {
        ConductorError::io("template: write :file content", &target_path, source)
    })?;

    Ok(ResolvedValue::MaterializedFile(target_path))
}

/// `:folder(path)` — extracts archive content to sandbox folder and returns path.
async fn apply_folder_selector<C: mediapm_cas::CasApi + Send + Sync>(
    value: ResolvedValue,
    path: &str,
    ctx: &TemplateContext<'_, C>,
) -> Result<ResolvedValue, ConductorError> {
    let sandbox = ctx.sandbox_dir.ok_or_else(|| {
        ConductorError::Workflow(
            "template: ':folder' selector requires a sandbox directory".to_string(),
        )
    })?;

    let data = value.into_bytes()?;
    let target_dir = sandbox.join(path);

    tokio::fs::create_dir_all(&target_dir).await.map_err(|source| {
        ConductorError::io("template: create target directory for :folder", &target_dir, source)
    })?;

    extract_archive_to_folder(&data, &target_dir).await?;

    Ok(ResolvedValue::MaterializedFolder(target_dir))
}

// ---------------------------------------------------------------------------
// ZIP extraction
// ---------------------------------------------------------------------------

/// Extracts a single member from ZIP archive data.
fn extract_zip_member(data: &[u8], member: &str) -> Result<Vec<u8>, ConductorError> {
    #[cfg(feature = "tool-presets")]
    {
        use std::io::Read;
        let mut archive = zip::ZipArchive::new(std::io::Cursor::new(data)).map_err(|e| {
            ConductorError::Workflow(format!(
                "template: failed to open ZIP archive for ':zip({member})': {e}",
            ))
        })?;

        let mut file = archive.by_name(member).map_err(|e| {
            ConductorError::Workflow(format!("template: ZIP member '{member}' not found: {e}",))
        })?;

        let mut buf = Vec::new();
        file.read_to_end(&mut buf).map_err(|e| {
            ConductorError::Workflow(
                format!("template: failed to read ZIP member '{member}': {e}",),
            )
        })?;
        Ok(buf)
    }

    #[cfg(not(feature = "tool-presets"))]
    {
        let _ = (data, member);
        Err(ConductorError::Workflow(
            "template: ZIP extraction requires the 'tool-presets' feature".to_string(),
        ))
    }
}

/// Extracts an entire archive (ZIP) into a target directory.
async fn extract_archive_to_folder(data: &[u8], target_dir: &Path) -> Result<(), ConductorError> {
    #[cfg(feature = "tool-presets")]
    {
        use std::io::Read;
        let mut archive = zip::ZipArchive::new(std::io::Cursor::new(data)).map_err(|e| {
            ConductorError::Workflow(format!(
                "template: failed to open ZIP archive for folder extraction: {e}",
            ))
        })?;

        for i in 0..archive.len() {
            let mut file = archive.by_index(i).map_err(|e| {
                ConductorError::Workflow(format!("template: failed to access ZIP entry #{i}: {e}",))
            })?;

            let name = file.mangled_name().to_string_lossy().to_string();

            // Skip directory entries.
            if file.is_dir() || name.ends_with('/') {
                let dir_path = target_dir.join(&name);
                tokio::fs::create_dir_all(&dir_path).await.map_err(|source| {
                    ConductorError::io(
                        "template: create directory for ZIP entry",
                        &dir_path,
                        source,
                    )
                })?;
                continue;
            }

            let entry_path = target_dir.join(&name);
            if let Some(parent) = entry_path.parent() {
                tokio::fs::create_dir_all(parent).await.map_err(|source| {
                    ConductorError::io("template: create parent for ZIP entry", parent, source)
                })?;
            }

            let mut buf = Vec::new();
            file.read_to_end(&mut buf).map_err(|e| {
                ConductorError::Workflow(format!(
                    "template: failed to read ZIP entry '{name}': {e}",
                ))
            })?;

            tokio::fs::write(&entry_path, &buf).await.map_err(|source| {
                ConductorError::io("template: write ZIP entry", &entry_path, source)
            })?;
        }

        Ok(())
    }

    #[cfg(not(feature = "tool-presets"))]
    {
        let _ = (data, target_dir);
        Err(ConductorError::Workflow(
            "template: archive extraction requires the 'tool-presets' feature".to_string(),
        ))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use mediapm_cas::CasApi;
    use mediapm_cas::storage::in_memory::InMemoryCas;
    use std::sync::LazyLock;

    /// Helper to build a context with CAS + env + tokens + inputs.
    fn full_ctx<'a>(
        cas: Option<&'a InMemoryCas>,
        step_outputs: &'a BTreeMap<String, BTreeMap<String, Hash>>,
        env_vars: &'a BTreeMap<String, String>,
        tokens: &'a BTreeMap<String, Vec<u8>>,
        sandbox_dir: Option<&'a Path>,
        host_os: &'a str,
    ) -> TemplateContext<'a, InMemoryCas> {
        static EMPTY_INPUTS: LazyLock<BTreeMap<String, String>> = LazyLock::new(BTreeMap::new);
        TemplateContext {
            cas,
            step_outputs,
            env_vars,
            tokens,
            sandbox_dir,
            host_os,
            inputs: &EMPTY_INPUTS,
        }
    }

    // -----------------------------------------------------------------------
    // Basic resolution: step_output
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn resolve_step_output_simple() {
        let mut step_outputs = BTreeMap::new();
        let mut outputs = BTreeMap::new();
        outputs.insert("result".to_string(), Hash::from_content(b"hello"));
        step_outputs.insert("step-1".to_string(), outputs);
        let env_vars = BTreeMap::new();
        let tokens = BTreeMap::new();
        let ctx = TemplateContext::<InMemoryCas> {
            cas: None,
            step_outputs: &step_outputs,
            env_vars: &env_vars,
            tokens: &tokens,
            sandbox_dir: None,
            host_os: "macos",
            inputs: &BTreeMap::new(),
        };
        let result = resolve_template("${step_output.step-1.result}", &ctx).await.unwrap();
        assert_eq!(result, Hash::from_content(b"hello").to_string());
    }

    #[tokio::test]
    async fn resolve_step_output_missing_step_id() {
        let step_outputs = BTreeMap::new();
        let env_vars = BTreeMap::new();
        let tokens = BTreeMap::new();
        let ctx = TemplateContext::<InMemoryCas> {
            cas: None,
            step_outputs: &step_outputs,
            env_vars: &env_vars,
            tokens: &tokens,
            sandbox_dir: None,
            host_os: "macos",
            inputs: &BTreeMap::new(),
        };
        let result = resolve_template("${step_output.missing.output}", &ctx).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[tokio::test]
    async fn resolve_step_output_missing_output() {
        let mut step_outputs = BTreeMap::new();
        step_outputs.insert("step-1".to_string(), BTreeMap::new());
        let env_vars = BTreeMap::new();
        let tokens = BTreeMap::new();
        let ctx = TemplateContext::<InMemoryCas> {
            cas: None,
            step_outputs: &step_outputs,
            env_vars: &env_vars,
            tokens: &tokens,
            sandbox_dir: None,
            host_os: "macos",
            inputs: &BTreeMap::new(),
        };
        let result = resolve_template("${step_output.step-1.output}", &ctx).await;
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // Basic resolution: env
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn resolve_env_var() {
        let env_vars = BTreeMap::from([("HOME".to_string(), "/home/user".to_string())]);
        let step_outputs = BTreeMap::new();
        let tokens = BTreeMap::new();
        let ctx = full_ctx(None, &step_outputs, &env_vars, &tokens, None, "macos");
        let result = resolve_template("${env.HOME}", &ctx).await.unwrap();
        assert_eq!(result, "/home/user");
    }

    #[tokio::test]
    async fn resolve_env_missing() {
        let step_outputs = BTreeMap::new();
        let env_vars = BTreeMap::new();
        let tokens = BTreeMap::new();
        let ctx = full_ctx(None, &step_outputs, &env_vars, &tokens, None, "macos");
        let result = resolve_template("${env.MISSING_VAR}", &ctx).await;
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // Basic resolution: external_data
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn resolve_external_data_with_cas() {
        let cas = mediapm_cas::storage::in_memory::new_in_memory_cas();
        let data = b"hello from CAS";
        let hash = cas.put(bytes::Bytes::from_static(data)).await.unwrap();

        let step_outputs = BTreeMap::new();
        let env_vars = BTreeMap::new();
        let tokens = BTreeMap::new();
        let ctx = full_ctx(Some(&cas), &step_outputs, &env_vars, &tokens, None, "macos");
        let result = resolve_template(&format!("${{external_data.{hash}}}"), &ctx).await.unwrap();
        assert_eq!(result, "hello from CAS");
    }

    #[tokio::test]
    async fn resolve_external_data_no_cas() {
        let hash = Hash::from_content(b"data");
        let step_outputs = BTreeMap::new();
        let env_vars = BTreeMap::new();
        let tokens = BTreeMap::new();
        let ctx = full_ctx(None, &step_outputs, &env_vars, &tokens, None, "macos");
        let result = resolve_template(&format!("${{external_data.{hash}}}"), &ctx).await;
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // Basic resolution: unpack token
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn resolve_unpack_token() {
        let tokens = BTreeMap::from([("my-token".to_string(), b"token-data".to_vec())]);
        let step_outputs = BTreeMap::new();
        let env_vars = BTreeMap::new();
        let ctx = full_ctx(None, &step_outputs, &env_vars, &tokens, None, "macos");
        let result = resolve_template("${*my-token}", &ctx).await.unwrap();
        assert_eq!(result, "token-data");
    }

    #[tokio::test]
    async fn resolve_unpack_token_missing() {
        let step_outputs = BTreeMap::new();
        let env_vars = BTreeMap::new();
        let tokens = BTreeMap::new();
        let ctx = full_ctx(None, &step_outputs, &env_vars, &tokens, None, "macos");
        let result = resolve_template("${*missing}", &ctx).await;
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // Escape sequences
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn escape_literal_dollar_brace() {
        let step_outputs = BTreeMap::new();
        let env_vars = BTreeMap::new();
        let tokens = BTreeMap::new();
        let ctx = TemplateContext::<InMemoryCas> {
            cas: None,
            step_outputs: &step_outputs,
            env_vars: &env_vars,
            tokens: &tokens,
            sandbox_dir: None,
            host_os: "macos",
            inputs: &BTreeMap::new(),
        };
        let result = resolve_template("\\${not_a_reference}", &ctx).await.unwrap();
        assert_eq!(result, "${not_a_reference}");
    }

    #[tokio::test]
    async fn mixed_escaped_and_real() {
        let mut step_outputs = BTreeMap::new();
        let mut outputs = BTreeMap::new();
        outputs.insert("val".to_string(), Hash::from_content(b"42"));
        step_outputs.insert("s1".to_string(), outputs);
        let env_vars = BTreeMap::new();
        let tokens = BTreeMap::new();
        let ctx = TemplateContext::<InMemoryCas> {
            cas: None,
            step_outputs: &step_outputs,
            env_vars: &env_vars,
            tokens: &tokens,
            sandbox_dir: None,
            host_os: "macos",
            inputs: &BTreeMap::new(),
        };
        let result =
            resolve_template("\\${escaped} and ${step_output.s1.val}", &ctx).await.unwrap();
        assert_eq!(result, format!("${{escaped}} and {}", Hash::from_content(b"42")));
    }

    // -----------------------------------------------------------------------
    // Platform conditionals
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn conditional_macos_true() {
        let step_outputs = BTreeMap::new();
        let env_vars = BTreeMap::new();
        let tokens = BTreeMap::new();
        let ctx = TemplateContext::<InMemoryCas> {
            cas: None,
            step_outputs: &step_outputs,
            env_vars: &env_vars,
            tokens: &tokens,
            sandbox_dir: None,
            host_os: "macos",
            inputs: &BTreeMap::new(),
        };
        let result =
            resolve_template("${context.os == \"macos\" ? hello : world}", &ctx).await.unwrap();
        assert_eq!(result, "hello");
    }

    #[tokio::test]
    async fn conditional_macos_false() {
        let step_outputs = BTreeMap::new();
        let env_vars = BTreeMap::new();
        let tokens = BTreeMap::new();
        let ctx = TemplateContext::<InMemoryCas> {
            cas: None,
            step_outputs: &step_outputs,
            env_vars: &env_vars,
            tokens: &tokens,
            sandbox_dir: None,
            host_os: "linux",
            inputs: &BTreeMap::new(),
        };
        let result =
            resolve_template("${context.os == \"macos\" ? hello : world}", &ctx).await.unwrap();
        assert_eq!(result, "world");
    }

    #[tokio::test]
    async fn conditional_not_equal() {
        let step_outputs = BTreeMap::new();
        let env_vars = BTreeMap::new();
        let tokens = BTreeMap::new();
        let ctx = TemplateContext::<InMemoryCas> {
            cas: None,
            step_outputs: &step_outputs,
            env_vars: &env_vars,
            tokens: &tokens,
            sandbox_dir: None,
            host_os: "linux",
            inputs: &BTreeMap::new(),
        };
        let result =
            resolve_template("${context.os != \"macos\" ? hello : world}", &ctx).await.unwrap();
        assert_eq!(result, "hello");
    }

    #[tokio::test]
    async fn conditional_with_step_output_branches() {
        let mut step_outputs = BTreeMap::new();
        let mut outs = BTreeMap::new();
        outs.insert("a".to_string(), Hash::from_content(b"apple"));
        outs.insert("b".to_string(), Hash::from_content(b"banana"));
        step_outputs.insert("s1".to_string(), outs);
        let env_vars = BTreeMap::new();
        let tokens = BTreeMap::new();
        let ctx = TemplateContext::<InMemoryCas> {
            cas: None,
            step_outputs: &step_outputs,
            env_vars: &env_vars,
            tokens: &tokens,
            sandbox_dir: None,
            host_os: "macos",
            inputs: &BTreeMap::new(),
        };
        let result = resolve_template(
            "${context.os == \"macos\" ? ${step_output.s1.a} : ${step_output.s1.b}}",
            &ctx,
        )
        .await
        .unwrap();
        assert_eq!(result, Hash::from_content(b"apple").to_string());
    }

    #[tokio::test]
    async fn conditional_nested_braces() {
        let step_outputs = BTreeMap::new();
        let env_vars = BTreeMap::new();
        let tokens = BTreeMap::new();
        let ctx = TemplateContext::<InMemoryCas> {
            cas: None,
            step_outputs: &step_outputs,
            env_vars: &env_vars,
            tokens: &tokens,
            sandbox_dir: None,
            host_os: "windows",
            inputs: &BTreeMap::new(),
        };
        let result =
            resolve_template("${context.os == \"windows\" ? win : other}", &ctx).await.unwrap();
        assert_eq!(result, "win");
    }

    // -----------------------------------------------------------------------
    // Comparison operators
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn comparison_less_than() {
        let step_outputs = BTreeMap::new();
        let env_vars = BTreeMap::new();
        let tokens = BTreeMap::new();
        let ctx = TemplateContext::<InMemoryCas> {
            cas: None,
            step_outputs: &step_outputs,
            env_vars: &env_vars,
            tokens: &tokens,
            sandbox_dir: None,
            host_os: "macos",
            inputs: &BTreeMap::new(),
        };
        let result = resolve_template("${context.os <= \"macos\" ? yes : no}", &ctx).await.unwrap();
        // "macos" <= "macos" → true
        assert_eq!(result, "yes");
    }

    #[tokio::test]
    async fn comparison_greater_than() {
        let step_outputs = BTreeMap::new();
        let env_vars = BTreeMap::new();
        let tokens = BTreeMap::new();
        let ctx = TemplateContext::<InMemoryCas> {
            cas: None,
            step_outputs: &step_outputs,
            env_vars: &env_vars,
            tokens: &tokens,
            sandbox_dir: None,
            host_os: "macos",
            inputs: &BTreeMap::new(),
        };
        let result = resolve_template("${context.os > \"linux\" ? yes : no}", &ctx).await.unwrap();
        assert_eq!(result, "yes");
    }

    // -----------------------------------------------------------------------
    // Template with mixed content
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn mixed_text_and_references() {
        let mut step_outputs = BTreeMap::new();
        let mut outs = BTreeMap::new();
        outs.insert("val".to_string(), Hash::from_content(b"42"));
        step_outputs.insert("step-1".to_string(), outs);

        let env_vars = BTreeMap::from([("USER".to_string(), "alice".to_string())]);
        let tokens = BTreeMap::new();
        let ctx = full_ctx(None, &step_outputs, &env_vars, &tokens, None, "macos");
        let result = resolve_template("user=${env.USER}, result=${step_output.step-1.val}", &ctx)
            .await
            .unwrap();
        assert_eq!(result, format!("user=alice, result={}", Hash::from_content(b"42")),);
    }

    #[tokio::test]
    async fn no_references_passthrough() {
        let step_outputs = BTreeMap::new();
        let env_vars = BTreeMap::new();
        let tokens = BTreeMap::new();
        let ctx = TemplateContext::<InMemoryCas> {
            cas: None,
            step_outputs: &step_outputs,
            env_vars: &env_vars,
            tokens: &tokens,
            sandbox_dir: None,
            host_os: "macos",
            inputs: &BTreeMap::new(),
        };
        let result = resolve_template("hello world", &ctx).await.unwrap();
        assert_eq!(result, "hello world");
    }

    // -----------------------------------------------------------------------
    // Error cases
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn unclosed_brace_error() {
        let step_outputs = BTreeMap::new();
        let env_vars = BTreeMap::new();
        let tokens = BTreeMap::new();
        let ctx = TemplateContext::<InMemoryCas> {
            cas: None,
            step_outputs: &step_outputs,
            env_vars: &env_vars,
            tokens: &tokens,
            sandbox_dir: None,
            host_os: "macos",
            inputs: &BTreeMap::new(),
        };
        let result = resolve_template("${unclosed", &ctx).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn unrecognized_reference_error() {
        let step_outputs = BTreeMap::new();
        let env_vars = BTreeMap::new();
        let tokens = BTreeMap::new();
        let ctx = TemplateContext::<InMemoryCas> {
            cas: None,
            step_outputs: &step_outputs,
            env_vars: &env_vars,
            tokens: &tokens,
            sandbox_dir: None,
            host_os: "macos",
            inputs: &BTreeMap::new(),
        };
        let result = resolve_template("${foo.bar}", &ctx).await;
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // Content resolution (resolve_content)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn resolve_content_from_cas() {
        let cas = mediapm_cas::storage::in_memory::new_in_memory_cas();
        let data = b"binary data";
        let hash = cas.put(bytes::Bytes::from_static(data)).await.unwrap();

        let step_outputs = BTreeMap::new();
        let env_vars = BTreeMap::new();
        let tokens = BTreeMap::new();
        let ctx = full_ctx(Some(&cas), &step_outputs, &env_vars, &tokens, None, "macos");
        let result = resolve_content(&format!("${{external_data.{hash}}}"), &ctx).await.unwrap();
        assert_eq!(result, b"binary data");
    }

    #[tokio::test]
    async fn resolve_content_string_is_utf8() {
        let env_vars = BTreeMap::from([("MSG".to_string(), "hello".to_string())]);
        let step_outputs = BTreeMap::new();
        let tokens = BTreeMap::new();
        let ctx = full_ctx(None, &step_outputs, &env_vars, &tokens, None, "macos");
        let result = resolve_content("${env.MSG}", &ctx).await.unwrap();
        assert_eq!(result, b"hello");
    }

    // -----------------------------------------------------------------------
    // :file materialization selector
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn file_selector_materializes_content() {
        let cas = mediapm_cas::storage::in_memory::new_in_memory_cas();
        let data = b"hello file content";
        let hash = cas.put(bytes::Bytes::from_static(data)).await.unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let step_outputs = BTreeMap::new();
        let env_vars = BTreeMap::new();
        let tokens = BTreeMap::new();
        let ctx =
            full_ctx(Some(&cas), &step_outputs, &env_vars, &tokens, Some(tmp.path()), "macos");
        let result = resolve_template(&format!("${{external_data.{hash}:file(output.txt)}}"), &ctx)
            .await
            .unwrap();

        // Result should be the materialized file path as string.
        let expected_path = tmp.path().join("output.txt").to_string_lossy().to_string();
        assert_eq!(result, expected_path);

        // File content should be on disk.
        let on_disk = tokio::fs::read(tmp.path().join("output.txt")).await.unwrap();
        assert_eq!(on_disk, b"hello file content");
    }

    #[tokio::test]
    async fn file_selector_no_sandbox_error() {
        let env_vars = BTreeMap::from([("X".to_string(), "val".to_string())]);
        let step_outputs = BTreeMap::new();
        let tokens = BTreeMap::new();
        let ctx = full_ctx(None, &step_outputs, &env_vars, &tokens, None, "macos");
        let result = resolve_template("${env.X:file(test.txt)}", &ctx).await;
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // Edge cases
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn empty_template() {
        let step_outputs = BTreeMap::new();
        let env_vars = BTreeMap::new();
        let tokens = BTreeMap::new();
        let ctx = TemplateContext::<InMemoryCas> {
            cas: None,
            step_outputs: &step_outputs,
            env_vars: &env_vars,
            tokens: &tokens,
            sandbox_dir: None,
            host_os: "macos",
            inputs: &BTreeMap::new(),
        };
        let result = resolve_template("", &ctx).await.unwrap();
        assert_eq!(result, "");
    }

    #[tokio::test]
    async fn empty_env_var_name_error() {
        let step_outputs = BTreeMap::new();
        let env_vars = BTreeMap::new();
        let tokens = BTreeMap::new();
        let ctx = full_ctx(None, &step_outputs, &env_vars, &tokens, None, "macos");
        let result = resolve_template("${env.}", &ctx).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn empty_unpack_token_name_error() {
        let step_outputs = BTreeMap::new();
        let env_vars = BTreeMap::new();
        let tokens = BTreeMap::new();
        let ctx = full_ctx(None, &step_outputs, &env_vars, &tokens, None, "macos");
        let result = resolve_template("${*}", &ctx).await;
        assert!(result.is_err());
    }
}
