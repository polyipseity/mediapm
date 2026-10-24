//! Conversions from the extracted CLI definition into library types.
//!
//! The clap types themselves live in `mediapm-cli`, so a build script can build
//! the command tree without depending on this package. Two conversions cannot
//! live there. `From<AddInsertPositionArg> for AddInsertPosition` and
//! `From<HierarchyPresetArg> for MediaHierarchyPreset` each name a library type,
//! and an impl has to be written in a crate that owns one of the two types:
//! `mediapm-cli` owns the argument but cannot name the target.
//!
//! So they live here, where the target is local. Nothing else belongs in this
//! module — it is the seam between the extracted definition and the library, and
//! every item in it exists to satisfy that orphan rule.

use mediapm_cli::{AddInsertPositionArg, HierarchyPresetArg};

use crate::{AddInsertPosition, MediaHierarchyPreset};

impl From<AddInsertPositionArg> for AddInsertPosition {
    fn from(value: AddInsertPositionArg) -> Self {
        match value {
            AddInsertPositionArg::Sorted => AddInsertPosition::Sorted,
            AddInsertPositionArg::Beginning => AddInsertPosition::Beginning,
            AddInsertPositionArg::End => AddInsertPosition::End,
        }
    }
}

impl From<HierarchyPresetArg> for MediaHierarchyPreset {
    fn from(value: HierarchyPresetArg) -> Self {
        match value {
            HierarchyPresetArg::Local => MediaHierarchyPreset::Local,
            HierarchyPresetArg::YtDlp => MediaHierarchyPreset::YtDlpChannel,
        }
    }
}

#[cfg(all(test, feature = "media-tagger"))]
mod tests {
    use mediapm_cli::command;

    /// Returns the `[default: ...]` text clap renders for one option.
    ///
    /// The value is read out of the rendered help rather than from
    /// [`clap::Arg::get_default_values`] on purpose: the declared value is what
    /// the crate wrote down, and what a user sees is the rendering. Only the
    /// second one is the surface the extraction has to preserve.
    fn displayed_default(help: &str, long: &str) -> String {
        let option_line = format!("      --{long}");
        let mut lines = help.lines().peekable();
        while let Some(line) = lines.next() {
            if !line.starts_with(&option_line) {
                continue;
            }
            // The description runs until the next option, and may wrap.
            let mut description = String::new();
            while lines.peek().is_some_and(|next| !next.trim_start().starts_with('-')) {
                description.push_str(lines.next().expect("peeked line"));
                description.push(' ');
            }
            let start = description
                .find("[default: ")
                .unwrap_or_else(|| panic!("`--{long}` renders no default: {description:?}"))
                + "[default: ".len();
            let end = start
                + description[start..].find(']').unwrap_or_else(|| {
                    panic!("`--{long}` has an unterminated default: {description:?}")
                });
            return description[start..end].to_string();
        }
        panic!("`--{long}` is not in the rendered help");
    }

    /// The builtin's help renders the same defaults the runtime reads.
    ///
    /// The CLI crate carries literal copies of these values because a clap
    /// attribute is evaluated in the crate that declares the field, and that
    /// crate cannot name the builtin's constants. This is the test that keeps
    /// the copies honest, and it compares the rendering rather than the two
    /// constants, so a value that never reaches the help text fails here.
    #[test]
    fn builtin_media_tagger_help_renders_the_runtime_defaults() {
        use crate::builtins::media_tagger as tagger;

        let command = command();
        let builtin =
            command.find_subcommand("builtin").expect("`builtin` must be a declared subcommand");
        let media_tagger = builtin
            .find_subcommand("media-tagger")
            .expect("`builtin media-tagger` must be a declared subcommand");
        let help = media_tagger.clone().render_long_help().to_string();

        let cases: [(&str, String); 15] = [
            ("acoustid-endpoint", tagger::DEFAULT_ACOUSTID_ENDPOINT.to_string()),
            ("musicbrainz-endpoint", tagger::DEFAULT_MUSICBRAINZ_ENDPOINT.to_string()),
            ("caa-endpoint", tagger::DEFAULT_CAA_ENDPOINT.to_string()),
            ("cache-expiry-seconds", tagger::DEFAULT_CACHE_EXPIRY_SECONDS.to_string()),
            ("save-images-to-tags", tagger::DEFAULT_SAVE_IMAGES_TO_TAGS.to_string()),
            ("embed-only-one-front-image", tagger::DEFAULT_EMBED_ONLY_ONE_FRONT_IMAGE.to_string()),
            ("ca-providers", tagger::DEFAULT_CA_PROVIDERS.to_string()),
            ("caa-image-types", tagger::DEFAULT_CAA_IMAGE_TYPES.to_string()),
            ("caa-image-size", tagger::DEFAULT_CAA_IMAGE_SIZE.to_string()),
            ("caa-approved-only", tagger::DEFAULT_CAA_APPROVED_ONLY.to_string()),
            ("preserve-images", tagger::DEFAULT_PRESERVE_IMAGES.to_string()),
            ("clear-existing-tags", tagger::DEFAULT_CLEAR_EXISTING_TAGS.to_string()),
            ("enable-tag-saving", tagger::DEFAULT_ENABLE_TAG_SAVING.to_string()),
            ("release-ars", tagger::DEFAULT_RELEASE_ARS.to_string()),
            ("cover-art-slot-count", tagger::DEFAULT_COVER_ART_SLOT_COUNT.to_string()),
        ];

        for (option, expected) in cases {
            assert_eq!(
                displayed_default(&help, option),
                expected,
                "`--{option}` must render the default the runtime reads"
            );
        }
    }
}
