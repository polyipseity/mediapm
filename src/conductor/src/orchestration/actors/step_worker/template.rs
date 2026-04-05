//! Template rendering and ZIP-selector parsing helpers for step workers.
//!
//! This module isolates `${...}` interpolation mechanics from process
//! execution plumbing so `step_worker.rs` can stay focused on actor/runtime
//! orchestration.

use std::collections::BTreeMap;
use std::path::Path;

use mediapm_cas::CasApi;

use crate::error::ConductorError;
use crate::model::state::ResolvedInput;

use super::{ExtractedZipSelection, StepWorkerExecutor, TemplateFileWrite, TemplateSelectorSource};

impl<C> StepWorkerExecutor<C>
where
    C: CasApi + Send + Sync + 'static,
{
    /// Renders a map of template values using the resolved input scope.
    pub(super) fn render_templates(
        &self,
        templates: &BTreeMap<String, String>,
        inputs: &BTreeMap<String, ResolvedInput>,
        pending_file_writes: &mut Vec<TemplateFileWrite>,
    ) -> Result<BTreeMap<String, String>, ConductorError> {
        templates
            .iter()
            .map(|(key, value)| {
                self.render_template_value(value, inputs, pending_file_writes)
                    .map(|rendered| (key.clone(), rendered))
            })
            .collect()
    }

    /// Renders one template string using JavaScript-like `${...}` interpolation
    /// rules over the step input scope.
    pub(super) fn render_template_value(
        &self,
        template: &str,
        inputs: &BTreeMap<String, ResolvedInput>,
        pending_file_writes: &mut Vec<TemplateFileWrite>,
    ) -> Result<String, ConductorError> {
        let mut rendered = String::with_capacity(template.len());
        let mut index = 0usize;

        while index < template.len() {
            let tail = &template[index..];
            if tail.starts_with(r"\${") {
                rendered.push_str("${");
                index += 3;
                continue;
            }
            if tail.starts_with("${") {
                let after = &template[index + 2..];
                let Some(end) = after.find('}') else {
                    return Err(ConductorError::Workflow(format!(
                        "unterminated template expression in '{template}'"
                    )));
                };
                let token = &after[..end];
                rendered.push_str(&self.resolve_template_token(
                    token,
                    inputs,
                    pending_file_writes,
                )?);
                index += 2 + end + 1;
                continue;
            }
            if tail.starts_with('\\') {
                let (decoded, consumed) =
                    self.decode_js_escape(&template[index + 1..], template)?;
                rendered.push_str(&decoded);
                index += 1 + consumed;
                continue;
            }

            let Some(ch) = tail.chars().next() else {
                return Err(ConductorError::Workflow(format!(
                    "invalid template scanning state for '{template}'"
                )));
            };
            rendered.push(ch);
            index += ch.len_utf8();
        }

        Ok(rendered)
    }

    /// Resolves one `${...}` token body against the current input scope.
    ///
    /// Supported trailing directives are:
    /// - `:zip(<entry>)` to select a ZIP entry from input bytes,
    /// - `:file(<relative_path>)` to defer writing selected bytes as one file,
    /// - `:folder(<relative_path>)` to defer writing selected ZIP-directory
    ///   descendants as files under one destination folder.
    ///
    /// `:folder(...)` is only valid when combined with `:zip(...)`, and ZIP
    /// selectors that resolve to directories must use `:folder(...)`.
    fn resolve_template_token(
        &self,
        token: &str,
        inputs: &BTreeMap<String, ResolvedInput>,
        pending_file_writes: &mut Vec<TemplateFileWrite>,
    ) -> Result<String, ConductorError> {
        /// One optional trailing materialization directive.
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        enum TemplateMaterializationDirective<'a> {
            /// Defer materialization to one concrete file path.
            File(&'a str),
            /// Defer materialization to one concrete directory path.
            Folder(&'a str),
        }

        let token = token.trim();
        if token.is_empty() {
            return Err(ConductorError::Workflow(
                "template expression cannot be empty".to_string(),
            ));
        }

        if let Some(conditional_rendered) =
            self.resolve_os_conditional_token(token, inputs, pending_file_writes)?
        {
            return Ok(conditional_rendered);
        }

        let mut selector = token;
        let mut materialization_directive: Option<TemplateMaterializationDirective<'_>> = None;
        if let Some((selector_prefix, argument)) =
            self.split_trailing_template_directive(selector, "file")?
        {
            materialization_directive = Some(TemplateMaterializationDirective::File(argument));
            selector = selector_prefix;
        } else if let Some((selector_prefix, argument)) =
            self.split_trailing_template_directive(selector, "folder")?
        {
            materialization_directive = Some(TemplateMaterializationDirective::Folder(argument));
            selector = selector_prefix;
        }

        if self.split_trailing_template_directive(selector, "file")?.is_some()
            || self.split_trailing_template_directive(selector, "folder")?.is_some()
        {
            return Err(ConductorError::Workflow(format!(
                "template expression '${{{token}}}' supports at most one trailing materialization directive"
            )));
        }

        let mut zip_entry_path: Option<&str> = None;
        if let Some((selector_prefix, argument)) =
            self.split_trailing_template_directive(selector, "zip")?
        {
            zip_entry_path = Some(argument);
            selector = selector_prefix;
        }

        let TemplateSelectorSource::Input(input_key) = self.resolve_template_selector(selector)?;
        let input = inputs.get(&input_key).ok_or_else(|| {
            ConductorError::Workflow(format!("template references missing input '{input_key}'"))
        })?;

        let plain_content = if let Some(entry_path) = zip_entry_path {
            match self.extract_zip_entry_from_input(&input_key, &input.plain_content, entry_path)? {
                ExtractedZipSelection::File(file_content) => {
                    if let Some(TemplateMaterializationDirective::Folder(_)) =
                        materialization_directive
                    {
                        return Err(ConductorError::Workflow(format!(
                            "template zip selector for input '{input_key}' resolved '{entry_path}' to a file; expected a directory for :folder(...)"
                        )));
                    }
                    file_content
                }
                ExtractedZipSelection::Directory(directory_files) => {
                    let Some(TemplateMaterializationDirective::Folder(relative_path)) =
                        materialization_directive
                    else {
                        return Err(ConductorError::Workflow(format!(
                            "template zip selector for input '{input_key}' resolved '{entry_path}' to a directory; use :folder(<relative_path>) to materialize directory entries"
                        )));
                    };

                    let normalized_relative = self.normalized_relative_tool_path(
                        relative_path,
                        "template folder materialization",
                    )?;
                    for (entry_relative_path, entry_content) in directory_files {
                        pending_file_writes.push(TemplateFileWrite {
                            relative_path: normalized_relative.join(entry_relative_path),
                            plain_content: entry_content,
                        });
                    }
                    return Ok(normalized_relative.to_string_lossy().to_string());
                }
            }
        } else {
            if let Some(TemplateMaterializationDirective::Folder(_)) = materialization_directive {
                return Err(ConductorError::Workflow(format!(
                    "template folder materialization only supports ZIP selectors on input values, not '${{{token}}}'"
                )));
            }
            input.plain_content.clone()
        };

        if let Some(TemplateMaterializationDirective::File(relative_path)) =
            materialization_directive
        {
            let normalized_relative =
                self.normalized_relative_tool_path(relative_path, "template file materialization")?;
            pending_file_writes.push(TemplateFileWrite {
                relative_path: normalized_relative.clone(),
                plain_content,
            });
            Ok(normalized_relative.to_string_lossy().to_string())
        } else {
            Ok(String::from_utf8_lossy(&plain_content).to_string())
        }
    }

    /// Splits one token suffix formatted as `:<directive>(<argument>)`.
    ///
    /// Returns the token prefix before the directive plus the trimmed argument
    /// when the trailing suffix matches exactly; otherwise returns `None`.
    fn split_trailing_template_directive<'a>(
        &self,
        token: &'a str,
        directive: &str,
    ) -> Result<Option<(&'a str, &'a str)>, ConductorError> {
        let token = token.trim_end();
        if !token.ends_with(')') {
            return Ok(None);
        }

        let mut depth = 0usize;
        let mut open_index = None;
        for (index, character) in token.char_indices().rev() {
            match character {
                ')' => depth = depth.saturating_add(1),
                '(' => {
                    if depth == 0 {
                        return Err(ConductorError::Workflow(format!(
                            "invalid template expression '${{{token}}}'"
                        )));
                    }
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        open_index = Some(index);
                        break;
                    }
                }
                _ => {}
            }
        }

        let Some(open_index) = open_index else {
            return Err(ConductorError::Workflow(format!(
                "invalid template expression '${{{token}}}'"
            )));
        };

        let prefix_with_directive = token[..open_index].trim_end();
        let directive_prefix = format!(":{directive}");
        if !prefix_with_directive.ends_with(&directive_prefix) {
            return Ok(None);
        }

        let argument = token[open_index + 1..token.len() - 1].trim();
        if argument.is_empty() {
            return Err(ConductorError::Workflow(format!(
                "template directive '{directive}' requires one non-empty argument in '${{{token}}}'"
            )));
        }

        let selector = prefix_with_directive
            [..prefix_with_directive.len() - directive_prefix.len()]
            .trim_end();
        if selector.is_empty() {
            return Err(ConductorError::Workflow(format!(
                "template directive '{directive}' requires one selector in '${{{token}}}'"
            )));
        }

        Ok(Some((selector, argument)))
    }

    /// Resolves `${os.<target>?<value>}` conditionals.
    ///
    /// Supported `<target>` values are `windows`, `linux`, and `macos`.
    ///
    /// When current host OS matches `<target>`, `<value>` is resolved
    /// recursively and supports the same selector/materialization special forms
    /// accepted by other template tokens (for example
    /// `inputs.payload:file(payload.txt)`). Otherwise the token renders as
    /// empty content.
    fn resolve_os_conditional_token(
        &self,
        token: &str,
        inputs: &BTreeMap<String, ResolvedInput>,
        pending_file_writes: &mut Vec<TemplateFileWrite>,
    ) -> Result<Option<String>, ConductorError> {
        let Some(remainder) = token.strip_prefix("os.") else {
            return Ok(None);
        };

        let Some((target_os, value)) = remainder.split_once('?') else {
            return Err(ConductorError::Workflow(format!(
                "invalid os-conditional template expression '${{{token}}}'; expected '${{os.<target>?<value>}}'"
            )));
        };

        let normalized_target = target_os.trim().to_ascii_lowercase();
        let current_os = std::env::consts::OS;
        let matches_target = match normalized_target.as_str() {
            "windows" => current_os == "windows",
            "linux" => current_os == "linux",
            "macos" | "darwin" => current_os == "macos",
            other => {
                return Err(ConductorError::Workflow(format!(
                    "unsupported os-conditional target '{other}' in template expression '${{{token}}}'"
                )));
            }
        };

        if !matches_target {
            return Ok(Some(String::new()));
        }

        let conditional_value = value.trim();
        if conditional_value.is_empty() {
            return Ok(Some(String::new()));
        }

        if conditional_value.contains("${") || conditional_value.contains(r"\${") {
            return self
                .render_template_value(conditional_value, inputs, pending_file_writes)
                .map(Some);
        }

        let should_attempt_selector_resolution = conditional_value.contains(':')
            || conditional_value.contains('.')
            || conditional_value.contains('[')
            || conditional_value.contains(']')
            || conditional_value.contains('(')
            || conditional_value.contains(')')
            || conditional_value.chars().any(char::is_whitespace)
            || inputs.contains_key(conditional_value);

        if !should_attempt_selector_resolution {
            return Ok(Some(conditional_value.to_string()));
        }

        match self.resolve_template_token(conditional_value, inputs, pending_file_writes) {
            Ok(rendered) => Ok(Some(rendered)),
            Err(ConductorError::Workflow(message))
                if message.contains("unsupported template expression") =>
            {
                Ok(Some(conditional_value.to_string()))
            }
            Err(err) => Err(err),
        }
    }

    /// Extracts one ZIP entry from bytes provided by one resolved input.
    ///
    /// Extraction is delegated to the builtin archive runtime so workflow
    /// behavior stays aligned with builtin ZIP semantics.
    fn extract_zip_entry_from_input(
        &self,
        input_key: &str,
        input_bytes: &[u8],
        entry_path: &str,
    ) -> Result<ExtractedZipSelection, ConductorError> {
        let normalized_entry =
            self.normalized_relative_tool_path(entry_path, "template zip entry selector")?;

        let zip_workspace =
            tempfile::Builder::new().prefix("zip-entry-").tempdir().map_err(|source| {
                ConductorError::Io {
                    operation: "creating temporary ZIP extraction workspace".to_string(),
                    path: std::env::temp_dir(),
                    source,
                }
            })?;

        let extraction_root = zip_workspace.path().join("extracted");
        std::fs::create_dir_all(&extraction_root).map_err(|source| ConductorError::Io {
            operation: "creating temporary ZIP extraction directory".to_string(),
            path: extraction_root.clone(),
            source,
        })?;

        mediapm_conductor_builtin_archive::unpack_zip_bytes_to_directory(
            input_bytes,
            &extraction_root,
        )
        .map_err(|err| {
            ConductorError::Workflow(format!(
                "template zip extraction for input '{input_key}' failed: {err}"
            ))
        })?;

        let selected_entry = extraction_root.join(&normalized_entry);
        if !selected_entry.exists() {
            return Err(ConductorError::Workflow(format!(
                "template zip selector for input '{input_key}' could not find entry '{}': extracted archive has no such entry",
                normalized_entry.to_string_lossy()
            )));
        }

        if selected_entry.is_dir() {
            let directory_files = self.collect_directory_file_payloads(&selected_entry)?;
            return Ok(ExtractedZipSelection::Directory(directory_files));
        }

        std::fs::read(&selected_entry).map(ExtractedZipSelection::File).map_err(|source| {
            ConductorError::Io {
                operation: format!(
                    "reading extracted ZIP entry '{}' from template input",
                    normalized_entry.to_string_lossy()
                ),
                path: selected_entry,
                source,
            }
        })
    }

    /// Collects all regular descendant files under one extracted ZIP directory.
    fn collect_directory_file_payloads(
        &self,
        directory_path: &Path,
    ) -> Result<BTreeMap<std::path::PathBuf, Vec<u8>>, ConductorError> {
        let mut file_payloads = BTreeMap::new();
        self.collect_directory_file_payloads_recursive(
            directory_path,
            directory_path,
            &mut file_payloads,
        )?;
        Ok(file_payloads)
    }

    /// Recursively collects one directory tree into relative-file payloads.
    fn collect_directory_file_payloads_recursive(
        &self,
        root_directory: &Path,
        current_directory: &Path,
        file_payloads: &mut BTreeMap<std::path::PathBuf, Vec<u8>>,
    ) -> Result<(), ConductorError> {
        for entry in std::fs::read_dir(current_directory).map_err(|source| ConductorError::Io {
            operation: "enumerating extracted ZIP directory entries".to_string(),
            path: current_directory.to_path_buf(),
            source,
        })? {
            let entry = entry.map_err(|source| ConductorError::Io {
                operation: "reading extracted ZIP directory entry".to_string(),
                path: current_directory.to_path_buf(),
                source,
            })?;
            let entry_path = entry.path();
            let entry_type = entry.file_type().map_err(|source| ConductorError::Io {
                operation: "reading extracted ZIP entry type".to_string(),
                path: entry_path.clone(),
                source,
            })?;

            if entry_type.is_dir() {
                self.collect_directory_file_payloads_recursive(
                    root_directory,
                    &entry_path,
                    file_payloads,
                )?;
                continue;
            }

            if !entry_type.is_file() {
                return Err(ConductorError::Workflow(format!(
                    "template zip directory extraction encountered unsupported entry '{}'; expected regular files only",
                    entry_path.to_string_lossy()
                )));
            }

            let relative_path = entry_path
                .strip_prefix(root_directory)
                .map_err(|_| {
                    ConductorError::Internal(format!(
                        "failed deriving relative ZIP directory entry for '{}' under '{}'",
                        entry_path.to_string_lossy(),
                        root_directory.to_string_lossy()
                    ))
                })?
                .to_path_buf();

            let entry_path_for_error = entry_path.clone();
            let payload = std::fs::read(&entry_path).map_err(|source| ConductorError::Io {
                operation: "reading extracted ZIP directory file".to_string(),
                path: entry_path_for_error,
                source,
            })?;
            file_payloads.insert(relative_path, payload);
        }

        Ok(())
    }

    /// Resolves one interpolation selector to an input key.
    fn resolve_template_selector(
        &self,
        selector: &str,
    ) -> Result<TemplateSelectorSource, ConductorError> {
        let selector = selector.trim();
        if selector.is_empty() {
            return Err(ConductorError::Workflow("template selector cannot be empty".to_string()));
        }
        if let Some(key) = selector.strip_prefix("inputs.") {
            let key = key.trim();
            if key.is_empty() {
                return Err(ConductorError::Workflow(format!(
                    "unsupported template expression '{selector}'"
                )));
            }
            return Ok(TemplateSelectorSource::Input(key.to_string()));
        }
        if let Some(index) = selector.strip_prefix("inputs[") {
            if let Some(inner) = index.strip_suffix(']') {
                let inner = inner.trim();
                if (inner.starts_with('"') && inner.ends_with('"'))
                    || (inner.starts_with('\'') && inner.ends_with('\''))
                {
                    let key = self.decode_js_quoted_string(inner, selector)?;
                    if !key.is_empty() {
                        return Ok(TemplateSelectorSource::Input(key));
                    }
                }
            }
            return Err(ConductorError::Workflow(format!(
                "unsupported template expression '{selector}'"
            )));
        }

        let looks_like_expression = selector.contains('.')
            || selector.contains('(')
            || selector.contains(')')
            || selector.contains('[')
            || selector.contains(']')
            || selector.contains(':')
            || selector.chars().any(char::is_whitespace);
        if looks_like_expression {
            return Err(ConductorError::Workflow(format!(
                "unsupported template expression '{selector}'"
            )));
        }
        Ok(TemplateSelectorSource::Input(selector.to_string()))
    }

    /// Decodes one quoted JavaScript-like selector string.
    fn decode_js_quoted_string(
        &self,
        quoted: &str,
        selector: &str,
    ) -> Result<String, ConductorError> {
        let quote = quoted.chars().next().ok_or_else(|| {
            ConductorError::Workflow(format!("unsupported template expression '{selector}'"))
        })?;
        let Some(body) = quoted.strip_prefix(quote).and_then(|text| text.strip_suffix(quote))
        else {
            return Err(ConductorError::Workflow(format!(
                "unsupported template expression '{selector}'"
            )));
        };

        let mut decoded = String::with_capacity(body.len());
        let mut index = 0usize;
        while index < body.len() {
            let tail = &body[index..];
            if tail.starts_with('\\') {
                let (part, consumed) = self.decode_js_escape(&body[index + 1..], selector)?;
                decoded.push_str(&part);
                index += 1 + consumed;
                continue;
            }
            let Some(ch) = tail.chars().next() else {
                break;
            };
            decoded.push(ch);
            index += ch.len_utf8();
        }
        Ok(decoded)
    }

    /// Decodes one JavaScript-like escape sequence from a template or selector.
    fn decode_js_escape(
        &self,
        escaped_tail: &str,
        context: &str,
    ) -> Result<(String, usize), ConductorError> {
        let Some(first) = escaped_tail.chars().next() else {
            return Err(ConductorError::Workflow(format!(
                "trailing escape in template expression '{context}'"
            )));
        };
        let first_len = first.len_utf8();
        match first {
            '\\' => Ok(("\\".to_string(), first_len)),
            '\'' => Ok(("'".to_string(), first_len)),
            '"' => Ok(("\"".to_string(), first_len)),
            '`' => Ok(("`".to_string(), first_len)),
            '$' => Ok(("$".to_string(), first_len)),
            'n' => Ok(("\n".to_string(), first_len)),
            'r' => Ok(("\r".to_string(), first_len)),
            't' => Ok(("\t".to_string(), first_len)),
            'b' => Ok(("\u{0008}".to_string(), first_len)),
            'f' => Ok(("\u{000C}".to_string(), first_len)),
            'v' => Ok(("\u{000B}".to_string(), first_len)),
            '0' => {
                if let Some(next) = escaped_tail[first_len..].chars().next()
                    && next.is_ascii_digit()
                {
                    return Err(ConductorError::Workflow(format!(
                        "unsupported octal escape in template expression '{context}'"
                    )));
                }
                Ok(("\0".to_string(), first_len))
            }
            '\n' => Ok((String::new(), first_len)),
            '\r' => {
                if escaped_tail[first_len..].starts_with('\n') {
                    Ok((String::new(), first_len + 1))
                } else {
                    Ok((String::new(), first_len))
                }
            }
            'x' => {
                let hex = escaped_tail[first_len..].get(..2).ok_or_else(|| {
                    ConductorError::Workflow(format!(
                        "invalid hex escape in template expression '{context}'"
                    ))
                })?;
                let value = u8::from_str_radix(hex, 16).map_err(|_| {
                    ConductorError::Workflow(format!(
                        "invalid hex escape in template expression '{context}'"
                    ))
                })?;
                Ok(((value as char).to_string(), first_len + 2))
            }
            'u' => {
                let rest = &escaped_tail[first_len..];
                if let Some(braced) = rest.strip_prefix('{') {
                    let end = braced.find('}').ok_or_else(|| {
                        ConductorError::Workflow(format!(
                            "invalid unicode escape in template expression '{context}'"
                        ))
                    })?;
                    let digits = &braced[..end];
                    if digits.is_empty() || digits.len() > 6 {
                        return Err(ConductorError::Workflow(format!(
                            "invalid unicode escape in template expression '{context}'"
                        )));
                    }
                    let value = u32::from_str_radix(digits, 16).map_err(|_| {
                        ConductorError::Workflow(format!(
                            "invalid unicode escape in template expression '{context}'"
                        ))
                    })?;
                    let ch = char::from_u32(value).ok_or_else(|| {
                        ConductorError::Workflow(format!(
                            "invalid unicode escape in template expression '{context}'"
                        ))
                    })?;
                    Ok((ch.to_string(), first_len + 1 + end + 1))
                } else {
                    let hex = rest.get(..4).ok_or_else(|| {
                        ConductorError::Workflow(format!(
                            "invalid unicode escape in template expression '{context}'"
                        ))
                    })?;
                    let value = u32::from_str_radix(hex, 16).map_err(|_| {
                        ConductorError::Workflow(format!(
                            "invalid unicode escape in template expression '{context}'"
                        ))
                    })?;
                    let ch = char::from_u32(value).ok_or_else(|| {
                        ConductorError::Workflow(format!(
                            "invalid unicode escape in template expression '{context}'"
                        ))
                    })?;
                    Ok((ch.to_string(), first_len + 4))
                }
            }
            other => Err(ConductorError::Workflow(format!(
                "unsupported escape sequence '\\{other}' in template expression '{context}'"
            ))),
        }
    }

    /// Renders a list of template strings in order.
    pub(super) fn render_template_list(
        &self,
        templates: &[String],
        inputs: &BTreeMap<String, ResolvedInput>,
        pending_file_writes: &mut Vec<TemplateFileWrite>,
    ) -> Result<Vec<String>, ConductorError> {
        templates
            .iter()
            .map(|value| self.render_template_value(value, inputs, pending_file_writes))
            .collect()
    }

    /// Renders a command template list and removes OS-conditional omissions.
    pub(super) fn render_template_command(
        &self,
        templates: &[String],
        inputs: &BTreeMap<String, ResolvedInput>,
        pending_file_writes: &mut Vec<TemplateFileWrite>,
    ) -> Result<Vec<String>, ConductorError> {
        let mut rendered = self.render_template_list(templates, inputs, pending_file_writes)?;
        rendered.retain(|entry| !entry.is_empty());
        Ok(rendered)
    }
}
