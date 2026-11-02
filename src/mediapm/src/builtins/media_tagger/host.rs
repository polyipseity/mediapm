//! `media-tagger` as a host builtin.
//!
//! The tagging step declares no command parts and names the `media-tagger`
//! builtin id, so the conductor dispatches it to [`MediaTaggerBuiltin`]
//! in-process instead of spawning a program. The runner's whole job is to turn a
//! step's resolved inputs into one [`InternalMediaTaggerOptions`]: fold the
//! sandbox into the paths the pipeline works on, read the option values the
//! step bound, and hand back what the pipeline produced.
//!
//! The option names are the ones the synthesized step writes (see
//! [`build_media_tagger_metadata_inputs`](crate::tools::workflows::media_tagger));
//! the defaults below are taken from the builtin's own constants, because the
//! tool definition declares no default inputs, and the three booleans the
//! builtin has no constant for are stated once below. The `builtin media-tagger`
//! subcommand's clap definition is a second home for the same values: it lives
//! in `mediapm-cli`, which is behind this crate's optional `cli` feature while
//! this module is compiled under `media-tagger`, so the copies cannot be
//! imported from here.
//!
//! An option the retired command mapping consumed — `option_args`,
//! `leading_args`, `trailing_args` and `enable_acoustid` — is refused by name
//! rather than ignored: see [`RETIRED_COMMAND_OPTIONS`].
//!
//! A value the step does not bind is absent from the arguments, which is how a
//! run that says nothing about an option gets the builtin's default. An empty
//! value is not the same thing: it is what an empty option (a disabled cache
//! directory, for instance) resolves to, so the readers here treat blank text as
//! unset only where the builtin itself does.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use mediapm_conductor::error::ConductorError;
use mediapm_conductor::tools::{
    HostBuiltin, HostBuiltinCas, HostBuiltinOutput, HostBuiltinRequest,
};

use crate::conductor_bridge::constants::INPUT_CONTENT;
use crate::tools::workflows::media_tagger::{MEDIA_TAGGER_BUILTIN_ID, MEDIA_TAGGER_OUTPUT_FILE};

use super::{
    DEFAULT_ACOUSTID_ENDPOINT, DEFAULT_CA_PROVIDERS, DEFAULT_CAA_APPROVED_ONLY,
    DEFAULT_CAA_ENDPOINT, DEFAULT_CAA_IMAGE_SIZE, DEFAULT_CAA_IMAGE_TYPES,
    DEFAULT_CACHE_EXPIRY_SECONDS, DEFAULT_CLEAR_EXISTING_TAGS, DEFAULT_COVER_ART_SLOT_COUNT,
    DEFAULT_EMBED_ONLY_ONE_FRONT_IMAGE, DEFAULT_ENABLE_TAG_SAVING, DEFAULT_MUSICBRAINZ_ENDPOINT,
    DEFAULT_PRESERVE_IMAGES, DEFAULT_RELEASE_ARS, DEFAULT_SAVE_IMAGES_TO_TAGS,
    InternalMediaTaggerOptions, run_internal_media_tagger,
};

/// The sandbox file the step's `content` input is staged at.
///
/// The path is sandbox-relative, the form every workflow selector uses, and it
/// is what the retired command template staged the input at: a capture or a
/// downstream step that named this file keeps naming the same one.
const SANDBOX_INPUT_FILE: &str = "inputs/input.media";

/// A library default the tagger's own definition spells with a literal.
///
/// `strict_identification`, `write_all_tags` and `write_all_images` are the
/// three options the builtin reads whose default has no constant of its own, so
/// the value is stated here once instead of at each read.
const DEFAULT_STRICT_IDENTIFICATION: bool = true;
/// See [`DEFAULT_STRICT_IDENTIFICATION`].
const DEFAULT_WRITE_ALL_TAGS: bool = true;
/// See [`DEFAULT_STRICT_IDENTIFICATION`].
const DEFAULT_WRITE_ALL_IMAGES: bool = true;

/// Step options the retired command mapping consumed and this runner does not.
///
/// The tagging step used to be executed through a command built by the deleted
/// `build_media_tagger_command`: `option_args`, `leading_args` and
/// `trailing_args` were rendered into its argv, and `enable_acoustid` became
/// `--enable-acoustid`, a flag `mediapm-cli` does not declare (it declares only
/// `--acoustid-api-key` and `--acoustid-endpoint`). The step renders no command
/// any more, so a binding of one of these names reaches nothing — while the
/// value still decides the step's instance key. A step that binds one is refused
/// instead, so a library that still sets it hears that it must stop rather than
/// getting the quiet difference.
const RETIRED_COMMAND_OPTIONS: [&str; 4] =
    ["option_args", "leading_args", "trailing_args", "enable_acoustid"];

/// The `media-tagger` builtin runner mediapm registers on every run.
///
/// Stateless: everything a run needs arrives with the step, so one runner serves
/// every workflow of every run.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct MediaTaggerBuiltin;

#[async_trait]
impl HostBuiltin for MediaTaggerBuiltin {
    fn id(&self) -> &str {
        MEDIA_TAGGER_BUILTIN_ID
    }

    /// Runs one tagging step and writes the artifacts the step captures.
    ///
    /// # Errors
    ///
    /// Returns [`ConductorError::Workflow`] when a bound option cannot be read
    /// as the type it feeds, when the step's content hash names no object in the
    /// store, or when the tagging pipeline fails. A failed step is reported the
    /// way the run reports any failed step, rather than as a successful one with
    /// no artifacts.
    async fn run(
        &self,
        request: HostBuiltinRequest<'_>,
    ) -> Result<HostBuiltinOutput, ConductorError> {
        let input_path = stage_content(request.args, request.cas, request.sandbox_dir).await?;
        let options = options_from_args(request.args, input_path, request.sandbox_dir)?;

        run_internal_media_tagger(options)
            .await
            .map_err(|error| ConductorError::Workflow(format!("media-tagger failed: {error:#}")))?;

        Ok(HostBuiltinOutput::default())
    }
}

/// Writes the bytes the step's content input names into the sandbox.
///
/// The input carries a content hash rather than a path, because a workflow binds
/// bytes and the running builtin is what turns a hash into a file it can read.
/// A step that binds no content has nothing to stage: the pipeline needs an
/// input file only for the fingerprint path, which fails on its own when the
/// step supplies no recording identity.
async fn stage_content(
    args: &BTreeMap<String, String>,
    cas: &dyn HostBuiltinCas,
    sandbox_dir: &Path,
) -> Result<Option<PathBuf>, ConductorError> {
    let Some(hash_text) = args.get(INPUT_CONTENT).map(String::as_str).map(str::trim) else {
        return Ok(None);
    };
    if hash_text.is_empty() {
        return Ok(None);
    }

    let hash = hash_text.parse::<mediapm_cas::Hash>().map_err(|error| {
        ConductorError::Workflow(format!(
            "media-tagger input '{INPUT_CONTENT}' is not a content hash ('{hash_text}'): {error}"
        ))
    })?;
    let bytes = cas.get(hash).await.map_err(|error| {
        ConductorError::Workflow(format!(
            "media-tagger could not read input '{INPUT_CONTENT}' from the store ('{hash}'): {error}"
        ))
    })?;

    let staged = sandbox_dir.join(SANDBOX_INPUT_FILE);
    if let Some(parent) = staged.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(|error| {
            ConductorError::Workflow(format!(
                "media-tagger could not create the staging directory '{}': {error}",
                parent.display()
            ))
        })?;
    }
    tokio::fs::write(&staged, &bytes).await.map_err(|error| {
        ConductorError::Workflow(format!(
            "media-tagger could not stage its input at '{}': {error}",
            staged.display()
        ))
    })?;
    Ok(Some(staged))
}

/// Builds the pipeline options one step describes.
///
/// `sandbox_dir` is where the pipeline writes: the `FFmetadata` document at the
/// sandbox-relative path the step captures, and the cover-art folder beside it
/// under the sandbox root.
///
/// # Errors
///
/// Returns [`ConductorError::Workflow`] when the step binds an option this
/// runner has no field for because the retired command mapping used to consume
/// it ([`RETIRED_COMMAND_OPTIONS`]), when a boolean is neither `true` nor
/// `false`, or when a number does not parse.
fn options_from_args(
    args: &BTreeMap<String, String>,
    input_path: Option<PathBuf>,
    sandbox_dir: &Path,
) -> Result<InternalMediaTaggerOptions, ConductorError> {
    reject_retired_command_options(args)?;

    Ok(InternalMediaTaggerOptions {
        input_path,
        output_path: sandbox_dir.join(MEDIA_TAGGER_OUTPUT_FILE),
        acoustid_api_key: optional_text(args, "acoustid_api_key"),
        acoustid_endpoint: text(args, "acoustid_endpoint", DEFAULT_ACOUSTID_ENDPOINT),
        musicbrainz_endpoint: text(args, "musicbrainz_endpoint", DEFAULT_MUSICBRAINZ_ENDPOINT),
        caa_endpoint: text(args, "caa_endpoint", DEFAULT_CAA_ENDPOINT),
        cache_dir: optional_text(args, "cache_dir").map(PathBuf::from),
        cache_expiry_seconds: number(args, "cache_expiry_seconds", DEFAULT_CACHE_EXPIRY_SECONDS)?,
        strict_identification: flag(args, "strict_identification", DEFAULT_STRICT_IDENTIFICATION)?,
        write_all_tags: flag(args, "write_all_tags", DEFAULT_WRITE_ALL_TAGS)?,
        write_all_images: flag(args, "write_all_images", DEFAULT_WRITE_ALL_IMAGES)?,
        save_images_to_tags: flag(args, "save_images_to_tags", DEFAULT_SAVE_IMAGES_TO_TAGS)?,
        embed_only_one_front_image: flag(
            args,
            "embed_only_one_front_image",
            DEFAULT_EMBED_ONLY_ONE_FRONT_IMAGE,
        )?,
        cover_art_slot_count: number(args, "cover_art_slot_count", DEFAULT_COVER_ART_SLOT_COUNT)?,
        ca_providers: text(args, "ca_providers", DEFAULT_CA_PROVIDERS),
        caa_image_types: text(args, "caa_image_types", DEFAULT_CAA_IMAGE_TYPES),
        caa_image_size: text(args, "caa_image_size", DEFAULT_CAA_IMAGE_SIZE),
        caa_approved_only: flag(args, "caa_approved_only", DEFAULT_CAA_APPROVED_ONLY)?,
        preserve_images: flag(args, "preserve_images", DEFAULT_PRESERVE_IMAGES)?,
        clear_existing_tags: flag(args, "clear_existing_tags", DEFAULT_CLEAR_EXISTING_TAGS)?,
        enable_tag_saving: flag(args, "enable_tag_saving", DEFAULT_ENABLE_TAG_SAVING)?,
        release_ars: flag(args, "release_ars", DEFAULT_RELEASE_ARS)?,
        recording_mbid: optional_text(args, "recording_mbid"),
        release_mbid: optional_text(args, "release_mbid"),
    })
}

/// Refuses a step that binds an option the retired command mapping consumed.
///
/// # Errors
///
/// Returns [`ConductorError::Workflow`] naming the option and the value when the
/// step binds one of [`RETIRED_COMMAND_OPTIONS`]: no field of the pipeline
/// options answers for it, so the value would be dropped while still deciding
/// the step's instance key.
fn reject_retired_command_options(args: &BTreeMap<String, String>) -> Result<(), ConductorError> {
    for key in RETIRED_COMMAND_OPTIONS {
        if let Some(value) = args.get(key) {
            return Err(ConductorError::Workflow(format!(
                "media-tagger option '{key}' is '{value}', but the tagging step renders no command, so no field of it reads the option; remove it"
            )));
        }
    }
    Ok(())
}

/// Returns the text option `key` binds, or `default` when the step binds none.
fn text(args: &BTreeMap<String, String>, key: &str, default: &str) -> String {
    args.get(key)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map_or_else(|| default.to_string(), ToString::to_string)
}

/// Returns the text option `key` binds, or `None` when the step binds none.
///
/// Blank text is `None`: an unset option and one set to nothing both mean the
/// pipeline should decide, which is what the sentinel values it accepts
/// (`auto`, an absent key) also resolve to.
fn optional_text(args: &BTreeMap<String, String>, key: &str) -> Option<String> {
    args.get(key)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

/// Returns the boolean option `key` binds, or `default` when the step binds none.
///
/// # Errors
///
/// Returns [`ConductorError::Workflow`] naming the option and the value when the
/// bound text is neither `true` nor `false`: silently reading it as `false`
/// would turn a mistyped option into a policy the user never asked for.
fn flag(args: &BTreeMap<String, String>, key: &str, default: bool) -> Result<bool, ConductorError> {
    let Some(value) = args.get(key).map(|value| value.trim()).filter(|value| !value.is_empty())
    else {
        return Ok(default);
    };
    match value.to_ascii_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(ConductorError::Workflow(format!(
            "media-tagger option '{key}' is '{value}', which is neither 'true' nor 'false'"
        ))),
    }
}

/// Returns the numeric option `key` binds, or `default` when the step binds none.
///
/// # Errors
///
/// Returns [`ConductorError::Workflow`] naming the option and the value when the
/// bound text does not parse as the number the pipeline reads.
fn number<T: std::str::FromStr>(
    args: &BTreeMap<String, String>,
    key: &str,
    default: T,
) -> Result<T, ConductorError> {
    let Some(value) = args.get(key).map(|value| value.trim()).filter(|value| !value.is_empty())
    else {
        return Ok(default);
    };
    value.parse::<T>().map_err(|_| {
        ConductorError::Workflow(format!(
            "media-tagger option '{key}' is '{value}', which is not a number in the form it reads"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::Bytes;
    use mediapm_cas::CasApi;

    /// The arguments a synthesized tagging step resolves to, as the conductor
    /// hands them to a host builtin.
    ///
    /// The names are the ones `build_media_tagger_metadata_inputs` writes, which
    /// is the contract between the two modules: a rename on either side has to
    /// fail here rather than silently drop the option.
    fn step_args() -> BTreeMap<String, String> {
        BTreeMap::from([
            (INPUT_CONTENT.to_string(), "staged-by-this-test".to_string()),
            ("cache_dir".to_string(), "/cache".to_string()),
            ("cache_expiry_seconds".to_string(), "120".to_string()),
            ("strict_identification".to_string(), "false".to_string()),
            ("write_all_tags".to_string(), "false".to_string()),
            ("write_all_images".to_string(), "true".to_string()),
            ("musicbrainz_endpoint".to_string(), "http://127.0.0.1:1/ws/2".to_string()),
            ("cover_art_slot_count".to_string(), "4".to_string()),
            ("recording_mbid".to_string(), "8f3471b5-7e6a-48da-86a9-c1c07a0f47ae".to_string()),
            // A step option the pipeline has no field for: it must be ignored,
            // not rejected.
            ("ffmpeg_bin".to_string(), "/usr/bin/ffmpeg".to_string()),
        ])
    }

    /// The step's bindings become the options the pipeline reads.
    ///
    /// The values that differ from the library default are the evidence: a
    /// reader that ignored its argument, or read a name the step does not bind,
    /// would answer with the default and the assertions would hold either way
    /// only by coincidence.
    #[test]
    fn the_step_bindings_become_the_options_the_pipeline_reads() {
        let sandbox = std::path::Path::new("/sandbox");
        let options =
            options_from_args(&step_args(), Some("/sandbox/inputs/input.media".into()), sandbox)
                .expect("a step that binds valid values must map");

        assert_eq!(
            options.output_path,
            sandbox.join("metadata").join("output.ffmeta"),
            "the document is written where the step's `file:` capture reads it",
        );
        assert_eq!(
            options.input_path.as_deref(),
            Some(std::path::Path::new("/sandbox/inputs/input.media"))
        );
        assert_eq!(options.cache_dir.as_deref(), Some(std::path::Path::new("/cache")));
        assert_eq!(options.cache_expiry_seconds, 120);
        assert_eq!(options.cover_art_slot_count, 4);
        assert!(!options.strict_identification, "a bound 'false' must reach the pipeline");
        assert!(!options.write_all_tags);
        assert!(options.write_all_images);
        assert_eq!(options.musicbrainz_endpoint, "http://127.0.0.1:1/ws/2");
        assert_eq!(options.recording_mbid.as_deref(), Some("8f3471b5-7e6a-48da-86a9-c1c07a0f47ae"),);
    }

    /// An option the step does not bind gets the library default.
    ///
    /// The tool definition declares no default inputs, so this is the only place
    /// a run that says nothing about an option gets a value at all.
    #[test]
    fn an_unbound_option_gets_the_library_default() {
        let options = options_from_args(&BTreeMap::new(), None, std::path::Path::new("/sandbox"))
            .expect("an empty step maps to the defaults");

        assert!(options.strict_identification);
        assert!(options.write_all_tags);
        assert!(options.write_all_images);
        assert_eq!(options.musicbrainz_endpoint, super::DEFAULT_MUSICBRAINZ_ENDPOINT);
        assert_eq!(options.caa_endpoint, super::DEFAULT_CAA_ENDPOINT);
        assert_eq!(options.acoustid_endpoint, super::DEFAULT_ACOUSTID_ENDPOINT);
        assert_eq!(options.cache_expiry_seconds, super::DEFAULT_CACHE_EXPIRY_SECONDS);
        assert_eq!(options.cover_art_slot_count, super::DEFAULT_COVER_ART_SLOT_COUNT);
        assert_eq!(options.acoustid_api_key, None);
        assert_eq!(options.recording_mbid, None);
        // The assertions above compare against the same constants the readers
        // take their defaults from, so they hold even if one of those constants
        // is wrong. These are the literals: the cache expiry, the slot count and
        // the three endpoints a run reaches when the step says nothing.
        assert_eq!(
            (
                super::DEFAULT_CACHE_EXPIRY_SECONDS,
                super::DEFAULT_COVER_ART_SLOT_COUNT,
                super::DEFAULT_ACOUSTID_ENDPOINT,
                super::DEFAULT_MUSICBRAINZ_ENDPOINT,
                super::DEFAULT_CAA_ENDPOINT,
            ),
            (
                86_400,
                16,
                "https://api.acoustid.org/v2/lookup",
                "https://musicbrainz.org/ws/2",
                "https://coverartarchive.org",
            ),
            "the defaults are the builtin's shipped values, not whatever a constant drifted to",
        );
    }

    /// An option the retired command mapping consumed fails the step by name.
    ///
    /// The tagging step renders no command, so these four values reach nothing;
    /// a library that still sets one would be ignored while the value kept
    /// deciding the step's instance key, which is the silent difference this
    /// refusal replaces.
    #[test]
    fn a_binding_of_a_retired_command_option_fails_the_step() {
        for key in ["option_args", "leading_args", "trailing_args", "enable_acoustid"] {
            let args = BTreeMap::from([(key.to_string(), "--quiet -v".to_string())]);
            let error = options_from_args(&args, None, std::path::Path::new("/sandbox"))
                .expect_err("an option no field of the pipeline reads must not be ignored");

            assert!(
                error.to_string().contains(key) && error.to_string().contains("--quiet -v"),
                "the failure must name the option '{key}' and its value, got: {error}",
            );
        }
    }

    /// A boolean that is neither `true` nor `false` fails the step.
    ///
    /// Reading it as `false` would apply a policy the user never wrote, and the
    /// run would look successful while doing it.
    #[test]
    fn a_bound_boolean_that_is_neither_value_fails_the_step() {
        let args = BTreeMap::from([("strict_identification".to_string(), "yes".to_string())]);
        let error = options_from_args(&args, None, std::path::Path::new("/sandbox"))
            .expect_err("a mistyped boolean must not be read as a value");

        assert!(
            error.to_string().contains("strict_identification")
                && error.to_string().contains("yes"),
            "the failure must name the option and the value, got: {error}",
        );
    }

    /// The step's content hash is staged where the pipeline reads it.
    ///
    /// A workflow binds bytes as a content hash; the running builtin is what
    /// turns them into a file. The bytes and the path both matter: the pipeline
    /// reads `inputs/input.media` under the sandbox, which is where the retired
    /// command template staged the same input.
    #[tokio::test]
    async fn the_content_hash_is_staged_where_the_pipeline_reads_it() {
        let cas = mediapm_cas::InMemoryCas::new();
        let hash = cas.put(Bytes::from_static(b"media bytes")).await.expect("store the bytes");
        let sandbox = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let args = BTreeMap::from([(INPUT_CONTENT.to_string(), hash.to_string())]);

        let staged = stage_content(&args, &cas, sandbox.path())
            .await
            .expect("a hash the store holds must stage")
            .expect("the step bound content, so a file must exist");

        assert_eq!(staged, sandbox.path().join("inputs").join("input.media"));
        assert_eq!(
            std::fs::read(&staged).expect("the staged file must be readable"),
            b"media bytes"
        );
    }

    /// A step that binds no content stages nothing.
    ///
    /// The tagging pipeline reaches the input file only on the fingerprint path,
    /// which a step with a recording identity never takes, so an absent binding
    /// is not an error.
    #[tokio::test]
    async fn a_step_that_binds_no_content_stages_nothing() {
        let cas = mediapm_cas::InMemoryCas::new();
        let sandbox = mediapm_utils::temp::artifact_dir().expect("artifact dir");

        let staged = stage_content(&BTreeMap::new(), &cas, sandbox.path())
            .await
            .expect("no binding is not a failure");

        assert_eq!(staged, None);
        assert!(
            !sandbox.path().join("inputs").exists(),
            "nothing was asked for, so nothing may be created",
        );
    }
}
