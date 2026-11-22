//! The live tool set, derived from the generated document at the point of use.
//!
//! The sync used to carry a `BTreeMap<String, ToolRuntime>` beside the document
//! it wrote, so a tool's policy existed twice and only the document's copy
//! reached a step. One derivation replaces the second copy.

use std::collections::BTreeMap;

use mediapm_conductor::{NickelDocument, ToolSpec};

use super::{ToolDeployments, find_active_tool_spec};

/// The specs of the tools this run provisioned, keyed by generated-document key.
///
/// `provisioned` is the run's own record of what it provisioned (its tool ids),
/// because the document keeps pruned keys beside the active ones and cannot
/// distinguish them by itself. Each id is resolved through
/// [`find_active_tool_spec`], which is the same preference the materializer
/// uses, so both name one spec.
#[must_use]
pub(crate) fn live_tool_specs<'a>(
    doc: &'a NickelDocument,
    deployments: &ToolDeployments,
    provisioned: &[String],
) -> BTreeMap<String, &'a ToolSpec> {
    let mut live = BTreeMap::new();
    for tool_id in provisioned {
        if let Some((key, spec)) = find_active_tool_spec(doc, deployments, tool_id) {
            live.insert(key.clone(), spec);
        }
    }
    live
}

#[cfg(test)]
mod tests {
    use super::*;
    use mediapm_conductor::{ToolKindSpec, ToolRuntime, ToolSpec};

    /// A spec whose content map marks it active, named `name`.
    fn spec(name: &str, content_map: &[(&str, &str)]) -> ToolSpec {
        ToolSpec {
            kind: ToolKindSpec::Executable {
                command: vec![name.to_string()],
                env_vars: BTreeMap::new(),
                success_codes: vec![0],
            },
            name: name.to_string(),
            version: None,
            inputs: BTreeMap::new(),
            default_inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
            runtime: ToolRuntime {
                content_map: content_map
                    .iter()
                    .map(|(path, hash)| ((*path).to_string(), (*hash).to_string()))
                    .collect(),
                ..ToolRuntime::default()
            },
        }
    }

    /// The live set names the active spec of every provisioned tool, and only
    /// those: a pruned key in the document must not resurrect a tool the run did
    /// not provision.
    #[test]
    fn only_provisioned_tools_are_live() {
        let mut doc = NickelDocument::default();
        // A stale/pruned key kept its name but no longer names an active payload;
        // the active key is the one with a content map.
        doc.tools.insert("yt-dlp@v1".to_string(), spec("yt-dlp", &[]));
        doc.tools.insert("yt-dlp@v2".to_string(), spec("yt-dlp", &[("linux/yt-dlp", "new")]));
        doc.tools.insert("ffmpeg@v1".to_string(), spec("ffmpeg", &[("linux/ffmpeg", "kept")]));

        let live = live_tool_specs(&doc, &ToolDeployments::empty(), &["yt-dlp".to_string()]);
        assert_eq!(live.keys().collect::<Vec<_>>(), vec!["yt-dlp@v2"], "the active spec wins");
        assert!(
            !live.contains_key("ffmpeg@v1"),
            "a tool the run did not provision must not be live just because the document names it"
        );
    }

    /// A no-payload tool resolves through the fallback, so a builtin registered
    /// under a bare id is still live.
    #[test]
    fn a_payload_free_tool_still_resolves() {
        let mut doc = NickelDocument::default();
        doc.tools.insert("import".to_string(), spec("import", &[]));

        let live = live_tool_specs(&doc, &ToolDeployments::empty(), &["import".to_string()]);
        assert_eq!(live.keys().collect::<Vec<_>>(), vec!["import"]);
    }

    /// A provisioned tool the document does not describe is absent rather than
    /// invented: the derivation reports the document, it does not build a spec.
    #[test]
    fn a_tool_without_a_spec_is_not_live() {
        let doc = NickelDocument::default();
        assert!(
            live_tool_specs(&doc, &ToolDeployments::empty(), &["missing".to_string()]).is_empty()
        );
    }
}
