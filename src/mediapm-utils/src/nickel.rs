//! Deterministic Nickel source rendering for serializable Rust types.
//!
//! This module converts a `serde::Serialize` value into syntactically valid
//! Nickel source.  Field names that collide with Nickel reserved keywords are
//! automatically quoted with double quotes to prevent parse errors.
//!
//! # Design
//!
//! - Pure string formatting — no `nickel-lang-core` dependency.
//! - Output is deterministic (sorted map keys).
//! - `description` fields are rendered as Nickel multiline string literals
//!   (`m%%"..."%%`).
//!
//! # Feature gate
//!
//! This module requires the `nickel` feature on `mediapm-utils`, which enables
//! the `serde` and `serde_json` optional dependencies.

use serde::Serialize;
use serde_json::Value;

/// Nickel reserved keywords that cannot be used as bare identifiers.
const NICKEL_KEYWORDS: &[&str] = &[
    "array", "bool", "default", "doc", "fail", "forall", "fun", "if", "import", "in", "let",
    "match", "merge", "null", "number", "open", "rec", "string", "then", "type", "var", "with",
];

/// Returns `true` when `s` is a Nickel reserved keyword.
fn is_nickel_keyword(s: &str) -> bool {
    NICKEL_KEYWORDS.contains(&s)
}

/// Returns whether `key` can be emitted as a bare Nickel identifier.
///
/// A bare identifier must start with a letter (after optional leading
/// underscores), contain only alphanumeric ASCII, underscore, hyphen, or
/// single-quote characters, and must not be a reserved Nickel keyword.
fn is_bare_nickel_identifier(key: &str) -> bool {
    if is_nickel_keyword(key) {
        return false;
    }

    let mut chars = key.chars().peekable();

    while matches!(chars.peek(), Some('_')) {
        let _ = chars.next();
    }

    let Some(head) = chars.next() else {
        return false;
    };

    if !head.is_ascii_alphabetic() {
        return false;
    }

    chars.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '\''))
}

/// Renders one field name in Nickel record syntax.
///
/// Non-bare identifiers (empty, starting with non-alpha, or matching a Nickel
/// keyword) are emitted as quoted strings via `serde_json`.
fn render_field_name(name: &str) -> String {
    if is_bare_nickel_identifier(name) {
        name.to_string()
    } else {
        serde_json::to_string(name).unwrap_or_else(|_| format!("\"{name}\""))
    }
}

/// Renders `text` as a Nickel multiline string literal using 2-percent delimiters.
///
/// Uses `m%%"..."%%` so content that contains the 1-percent closing sequence
/// `"%` is still safe.  Literal `%{` interpolation markers are escaped to `%%{`.
fn render_nickel_multiline_string(text: &str) -> String {
    let escaped = text.replace("%{", "%%{");
    format!("m%%\"\n{escaped}\n\"%%")
}

/// Renders a serde JSON value as deterministic Nickel source.
fn render_nickel_value(value: &Value, indent: usize) -> String {
    let pad = " ".repeat(indent);
    let next_pad = " ".repeat(indent + 2);

    match value {
        Value::Null => "null".to_string(),
        Value::Bool(boolean) => boolean.to_string(),
        Value::Number(number) => number.to_string(),
        Value::String(text) => serde_json::to_string(text).unwrap_or_else(|_| "\"\"".to_string()),
        Value::Array(items) => {
            if items.is_empty() {
                "[]".to_string()
            } else {
                let rendered_items = items
                    .iter()
                    .map(|item| format!("{next_pad}{},", render_nickel_value(item, indent + 2)))
                    .collect::<Vec<_>>()
                    .join("\n");
                format!("[\n{rendered_items}\n{pad}]")
            }
        }
        Value::Object(entries) => {
            if entries.is_empty() {
                "{}".to_string()
            } else {
                let mut ordered_entries = entries.iter().collect::<Vec<_>>();
                ordered_entries.sort_by_key(|(key, _)| *key);
                let rendered_entries = ordered_entries
                    .into_iter()
                    .map(|(key, entry_value)| {
                        let rendered_value = match (key.as_str(), entry_value) {
                            ("description", Value::String(text)) => {
                                render_nickel_multiline_string(text)
                            }
                            _ => render_nickel_value(entry_value, indent + 2),
                        };
                        format!("{next_pad}{} = {},", render_field_name(key), rendered_value)
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                format!("{{\n{rendered_entries}\n{pad}}}")
            }
        }
    }
}

/// Renders one serializable Rust structure as Nickel source.
///
/// The value is serialized to `serde_json::Value` first, then rendered as
/// deterministic Nickel source syntax.  The output includes a trailing newline.
///
/// # Errors
///
/// Returns an error message when `serde_json::to_value` fails (typically a
/// programming error such as a non-string map key).
pub fn render_document_as_nickel<T>(document: &T, document_kind: &str) -> Result<Vec<u8>, String>
where
    T: Serialize,
{
    let value = serde_json::to_value(document)
        .map_err(|err| format!("serializing {document_kind} to intermediate value: {err}"))?;
    let rendered = format!("{}\n", render_nickel_value(&value, 0));
    Ok(rendered.into_bytes())
}
