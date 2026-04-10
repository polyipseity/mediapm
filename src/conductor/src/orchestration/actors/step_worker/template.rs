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

/// Supported comparison operators for `${<left> <op> <right> ? ... | ...}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TemplateComparisonOperator {
    /// Equality (`==`).
    Equal,
    /// Inequality (`!=`).
    NotEqual,
    /// Lexicographic less-than (`<`).
    LessThan,
    /// Lexicographic less-than-or-equal (`<=`).
    LessThanOrEqual,
    /// Lexicographic greater-than (`>`).
    GreaterThan,
    /// Lexicographic greater-than-or-equal (`>=`).
    GreaterThanOrEqual,
}

impl<C> StepWorkerExecutor<C>
where
    C: CasApi + Send + Sync + 'static,
{
    /// Parses one standalone command-argument unpack token.
    ///
    /// Supported form is exactly `${*<selector>}` where `<selector>` resolves
    /// to an input key. The token must occupy the entire command argument.
    /// List inputs unpack to multiple arguments, while scalar inputs unpack to
    /// one argument when non-empty.
    fn parse_command_unpack_token<'a>(&self, template: &'a str) -> Option<&'a str> {
        let prefix = "${*";
        if !template.starts_with(prefix) || !template.ends_with('}') {
            return None;
        }
        let selector = &template[prefix.len()..template.len() - 1];
        if selector.trim().is_empty() {
            return None;
        }
        Some(selector)
    }

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
        if token.starts_with('*') {
            return Err(ConductorError::Workflow(format!(
                "unpack expression '${{{token}}}' is only valid as a standalone executable command argument token"
            )));
        }

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
            self.resolve_comparison_conditional_token(token, inputs, pending_file_writes)?
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

        let plain_content = match self.resolve_template_selector(selector)? {
            TemplateSelectorSource::Input(input_key) => {
                let input = inputs.get(&input_key).ok_or_else(|| {
                    ConductorError::Workflow(format!(
                        "template references missing input '{input_key}'"
                    ))
                })?;

                if input.string_list.is_some() {
                    return Err(ConductorError::Workflow(format!(
                        "template expression '${{{token}}}' references list input '{input_key}', but list inputs are only valid in standalone command unpack tokens like '${{*inputs.{input_key}}}'"
                    )));
                }

                if let Some(entry_path) = zip_entry_path {
                    match self.extract_zip_entry_from_input(
                        &input_key,
                        &input.plain_content,
                        entry_path,
                    )? {
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
                    input.plain_content.clone()
                }
            }
            TemplateSelectorSource::ContextOs => {
                if zip_entry_path.is_some() {
                    return Err(ConductorError::Workflow(format!(
                        "template expression '${{{token}}}' cannot apply :zip(...) to 'context.os'"
                    )));
                }
                self.current_os_text().as_bytes().to_vec()
            }
        };

        if let Some(TemplateMaterializationDirective::Folder(_)) = materialization_directive {
            return Err(ConductorError::Workflow(format!(
                "template folder materialization only supports ZIP selectors on input values, not '${{{token}}}'"
            )));
        }

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

    /// Resolves one `${<left> <op> <right> ? <true> | <false>}` comparison.
    ///
    /// Supported operators are `==`, `!=`, `<`, `<=`, `>`, and `>=` and
    /// operands compare using lexicographic string ordering.
    fn resolve_comparison_conditional_token(
        &self,
        token: &str,
        inputs: &BTreeMap<String, ResolvedInput>,
        pending_file_writes: &mut Vec<TemplateFileWrite>,
    ) -> Result<Option<String>, ConductorError> {
        let Some((condition_expression, true_branch, false_branch)) =
            self.split_conditional_token_branches(token)?
        else {
            return Ok(None);
        };

        let (left_operand, operator, right_operand) =
            self.parse_conditional_comparison(condition_expression, token)?;
        let left_value = self.resolve_conditional_operand(left_operand, inputs)?;
        let right_value = self.resolve_conditional_operand(right_operand, inputs)?;

        let matches = match operator {
            TemplateComparisonOperator::Equal => left_value == right_value,
            TemplateComparisonOperator::NotEqual => left_value != right_value,
            TemplateComparisonOperator::LessThan => left_value < right_value,
            TemplateComparisonOperator::LessThanOrEqual => left_value <= right_value,
            TemplateComparisonOperator::GreaterThan => left_value > right_value,
            TemplateComparisonOperator::GreaterThanOrEqual => left_value >= right_value,
        };

        let selected_branch = if matches { true_branch } else { false_branch };
        self.resolve_conditional_branch_value(selected_branch, inputs, pending_file_writes)
            .map(Some)
    }

    /// Returns host platform value used by `context.os` selectors.
    #[must_use]
    fn current_os_text(&self) -> &'static str {
        match std::env::consts::OS {
            "windows" => "windows",
            "linux" => "linux",
            "macos" => "macos",
            other => other,
        }
    }

    /// Splits one conditional token into condition + true/false branches.
    fn split_conditional_token_branches<'a>(
        &self,
        token: &'a str,
    ) -> Result<Option<(&'a str, &'a str, &'a str)>, ConductorError> {
        let mut quote: Option<char> = None;
        let mut escaped = false;
        let mut paren_depth = 0usize;
        let mut bracket_depth = 0usize;
        let mut brace_depth = 0usize;
        let mut condition_separator_index = None;
        let mut branch_separator_index = None;

        for (index, character) in token.char_indices() {
            if let Some(active_quote) = quote {
                if escaped {
                    escaped = false;
                    continue;
                }
                if character == '\\' {
                    escaped = true;
                    continue;
                }
                if character == active_quote {
                    quote = None;
                }
                continue;
            }

            match character {
                '\'' | '"' => quote = Some(character),
                '(' => paren_depth = paren_depth.saturating_add(1),
                ')' => paren_depth = paren_depth.saturating_sub(1),
                '[' => bracket_depth = bracket_depth.saturating_add(1),
                ']' => bracket_depth = bracket_depth.saturating_sub(1),
                '{' => brace_depth = brace_depth.saturating_add(1),
                '}' => brace_depth = brace_depth.saturating_sub(1),
                '?' if paren_depth == 0 && bracket_depth == 0 && brace_depth == 0 => {
                    if condition_separator_index.is_none() {
                        condition_separator_index = Some(index);
                    }
                }
                '|' if paren_depth == 0 && bracket_depth == 0 && brace_depth == 0 => {
                    if condition_separator_index.is_some() {
                        branch_separator_index = Some(index);
                        break;
                    }
                }
                _ => {}
            }
        }

        let Some(condition_separator_index) = condition_separator_index else {
            return Ok(None);
        };
        let Some(branch_separator_index) = branch_separator_index else {
            return Err(ConductorError::Workflow(format!(
                "invalid conditional template expression '${{{token}}}'; expected '${{<left> <op> <right>?<true>|<false>}}'"
            )));
        };

        let condition = token[..condition_separator_index].trim();
        let true_branch = token[condition_separator_index + 1..branch_separator_index].trim();
        let false_branch = token[branch_separator_index + 1..].trim();

        if condition.is_empty() || true_branch.is_empty() || false_branch.is_empty() {
            return Err(ConductorError::Workflow(format!(
                "invalid conditional template expression '${{{token}}}'; condition and both branches must be non-empty"
            )));
        }

        Ok(Some((condition, true_branch, false_branch)))
    }

    /// Parses one conditional comparison expression into operands + operator.
    fn parse_conditional_comparison<'a>(
        &self,
        condition_expression: &'a str,
        token: &str,
    ) -> Result<(&'a str, TemplateComparisonOperator, &'a str), ConductorError> {
        let mut quote: Option<char> = None;
        let mut escaped = false;
        let mut paren_depth = 0usize;
        let mut bracket_depth = 0usize;
        let mut brace_depth = 0usize;

        for (index, character) in condition_expression.char_indices() {
            if let Some(active_quote) = quote {
                if escaped {
                    escaped = false;
                    continue;
                }
                if character == '\\' {
                    escaped = true;
                    continue;
                }
                if character == active_quote {
                    quote = None;
                }
                continue;
            }

            match character {
                '\'' | '"' => {
                    quote = Some(character);
                    continue;
                }
                '(' => {
                    paren_depth = paren_depth.saturating_add(1);
                    continue;
                }
                ')' => {
                    paren_depth = paren_depth.saturating_sub(1);
                    continue;
                }
                '[' => {
                    bracket_depth = bracket_depth.saturating_add(1);
                    continue;
                }
                ']' => {
                    bracket_depth = bracket_depth.saturating_sub(1);
                    continue;
                }
                '{' => {
                    brace_depth = brace_depth.saturating_add(1);
                    continue;
                }
                '}' => {
                    brace_depth = brace_depth.saturating_sub(1);
                    continue;
                }
                _ => {}
            }

            if paren_depth != 0 || bracket_depth != 0 || brace_depth != 0 {
                continue;
            }

            let tail = &condition_expression[index..];
            let (operator_token, operator) = if tail.starts_with("==") {
                ("==", TemplateComparisonOperator::Equal)
            } else if tail.starts_with("!=") {
                ("!=", TemplateComparisonOperator::NotEqual)
            } else if tail.starts_with(">=") {
                (">=", TemplateComparisonOperator::GreaterThanOrEqual)
            } else if tail.starts_with("<=") {
                ("<=", TemplateComparisonOperator::LessThanOrEqual)
            } else if tail.starts_with('>') {
                (">", TemplateComparisonOperator::GreaterThan)
            } else if tail.starts_with('<') {
                ("<", TemplateComparisonOperator::LessThan)
            } else {
                continue;
            };

            let left_operand = condition_expression[..index].trim();
            let right_operand = condition_expression[index + operator_token.len()..].trim();
            if left_operand.is_empty() || right_operand.is_empty() {
                return Err(ConductorError::Workflow(format!(
                    "invalid conditional template expression '${{{token}}}'; comparison operands must be non-empty"
                )));
            }

            return Ok((left_operand, operator, right_operand));
        }

        Err(ConductorError::Workflow(format!(
            "invalid conditional template expression '${{{token}}}'; expected one comparison operator among ==, !=, <, <=, >, >="
        )))
    }

    /// Resolves one conditional operand into its comparable string value.
    fn resolve_conditional_operand(
        &self,
        operand: &str,
        inputs: &BTreeMap<String, ResolvedInput>,
    ) -> Result<String, ConductorError> {
        if (operand.starts_with('"') && operand.ends_with('"'))
            || (operand.starts_with('\'') && operand.ends_with('\''))
        {
            return self.decode_js_quoted_string(operand, operand);
        }

        if operand == "context.os" {
            return Ok(self.current_os_text().to_string());
        }

        let should_attempt_selector = operand.starts_with("inputs.")
            || operand.starts_with("inputs[")
            || inputs.contains_key(operand);
        if !should_attempt_selector {
            return Ok(operand.to_string());
        }

        match self.resolve_template_selector(operand)? {
            TemplateSelectorSource::Input(input_key) => {
                let input = inputs.get(&input_key).ok_or_else(|| {
                    ConductorError::Workflow(format!(
                        "conditional expression references missing input '{input_key}'"
                    ))
                })?;
                if input.string_list.is_some() {
                    return Err(ConductorError::Workflow(format!(
                        "conditional expression references list input '{input_key}', but comparison operands must be scalar"
                    )));
                }
                Ok(String::from_utf8_lossy(&input.plain_content).to_string())
            }
            TemplateSelectorSource::ContextOs => Ok(self.current_os_text().to_string()),
        }
    }

    /// Resolves one conditional branch into rendered output content.
    fn resolve_conditional_branch_value(
        &self,
        branch: &str,
        inputs: &BTreeMap<String, ResolvedInput>,
        pending_file_writes: &mut Vec<TemplateFileWrite>,
    ) -> Result<String, ConductorError> {
        if branch.contains("${") || branch.contains(r"\${") {
            return self.render_template_value(branch, inputs, pending_file_writes);
        }

        if (branch.starts_with('"') && branch.ends_with('"'))
            || (branch.starts_with('\'') && branch.ends_with('\''))
        {
            return self.decode_js_quoted_string(branch, branch);
        }

        let should_attempt_selector_resolution = branch.contains(':')
            || branch.contains('.')
            || branch.contains('[')
            || branch.contains(']')
            || branch.contains('(')
            || branch.contains(')')
            || branch.chars().any(char::is_whitespace)
            || branch == "context.os"
            || inputs.contains_key(branch);

        if !should_attempt_selector_resolution {
            return Ok(branch.to_string());
        }

        match self.resolve_template_token(branch, inputs, pending_file_writes) {
            Ok(rendered) => Ok(rendered),
            Err(ConductorError::Workflow(message))
                if message.contains("unsupported template expression") =>
            {
                Ok(branch.to_string())
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

    /// Resolves one interpolation selector to an input key or context value.
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

        if selector == "context.os" {
            return Ok(TemplateSelectorSource::ContextOs);
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

    /// Renders a command template list and removes empty conditional results.
    pub(super) fn render_template_command(
        &self,
        templates: &[String],
        inputs: &BTreeMap<String, ResolvedInput>,
        pending_file_writes: &mut Vec<TemplateFileWrite>,
    ) -> Result<Vec<String>, ConductorError> {
        let mut rendered = Vec::new();
        for template in templates {
            if let Some(selector) = self.parse_command_unpack_token(template) {
                let input_key = match self.resolve_template_selector(selector)? {
                    TemplateSelectorSource::Input(input_key) => input_key,
                    TemplateSelectorSource::ContextOs => {
                        return Err(ConductorError::Workflow(format!(
                            "command unpack token '${{*{selector}}}' only supports step inputs, not 'context.os'"
                        )));
                    }
                };
                let input = inputs.get(&input_key).ok_or_else(|| {
                    ConductorError::Workflow(format!(
                        "command unpack token '${{*{selector}}}' references missing input '{input_key}'"
                    ))
                })?;
                if let Some(values) = input.string_list.as_ref() {
                    rendered.extend(values.iter().cloned());
                } else {
                    let scalar = String::from_utf8_lossy(&input.plain_content).to_string();
                    if !scalar.is_empty() {
                        rendered.push(scalar);
                    }
                }
                continue;
            }

            let value = self.render_template_value(template, inputs, pending_file_writes)?;
            if !value.is_empty() {
                rendered.push(value);
            }
        }
        Ok(rendered)
    }
}
