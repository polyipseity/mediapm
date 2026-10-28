//! Clap command definition for the `mediapm` binary.
//!
//! The definition lives in its own crate so that a build script can construct
//! the command tree. A build script cannot depend on the package it builds, so
//! a definition that lived in `mediapm` would be unreachable from `mediapm`'s
//! own build script. Nothing in this crate depends on `mediapm`, which is what
//! keeps the tree reachable from there.
//!
//! The crate declares types and nothing else: no storage, no I/O, no dispatch.
//! `mediapm` parses with these types and dispatches the result.
//!
//! Every doc comment on a clap type here is a user-facing string: clap renders
//! it into the help text, and a multi-paragraph one additionally becomes
//! `long_about`, which switches the whole command to long-help formatting. They
//! are kept as the definition carried them, and the reasoning that would
//! otherwise go in them belongs in this module's docs instead.

use std::path::PathBuf;

use clap::{Args, CommandFactory, Parser, Subcommand, ValueEnum};
use clap_complete::Shell;

/// The default values the `builtin media-tagger` options carry.
///
/// These duplicate values that also live in `mediapm`'s builtin, because a clap
/// attribute is evaluated in the crate that defines the field, so the
/// definition cannot name the library's constants there. They are literals
/// rather than the fields being narrowed to `Option` on purpose: narrowing
/// would stop `builtin media-tagger --help` printing `[default: ...]`, which is
/// a user-visible change to the surface this extraction has to preserve. The
/// duplication is held in step by a parity test in `mediapm` that renders the
/// help text and compares each displayed default against the constant the
/// runtime actually reads.
#[cfg(feature = "media-tagger")]
mod defaults {
    /// `AcoustID` lookup endpoint.
    pub const DEFAULT_ACOUSTID_ENDPOINT: &str = "https://api.acoustid.org/v2/lookup";
    /// `MusicBrainz` web-service endpoint.
    pub const DEFAULT_MUSICBRAINZ_ENDPOINT: &str = "https://musicbrainz.org/ws/2";
    /// `Cover Art Archive` endpoint.
    pub const DEFAULT_CAA_ENDPOINT: &str = "https://coverartarchive.org";
    /// How long a cache entry stays valid, in seconds.
    pub const DEFAULT_CACHE_EXPIRY_SECONDS: i64 = 24 * 60 * 60;
    /// Whether identified images are written into the file's tags.
    pub const DEFAULT_SAVE_IMAGES_TO_TAGS: bool = true;
    /// Whether only one front image is embedded.
    pub const DEFAULT_EMBED_ONLY_ONE_FRONT_IMAGE: bool = false;
    /// Cover-art providers consulted, in order.
    pub const DEFAULT_CA_PROVIDERS: &str = "caa_release,url_relationships,caa_release_group";
    /// Cover-art image types kept.
    pub const DEFAULT_CAA_IMAGE_TYPES: &str = "all,-matrix/runout,-raw/unedited,-watermark";
    /// Cover-art image size requested.
    pub const DEFAULT_CAA_IMAGE_SIZE: &str = "full";
    /// Whether only approved cover-art entries are used.
    pub const DEFAULT_CAA_APPROVED_ONLY: bool = false;
    /// Whether existing embedded images are preserved.
    pub const DEFAULT_PRESERVE_IMAGES: bool = false;
    /// Whether existing textual tags are cleared first.
    pub const DEFAULT_CLEAR_EXISTING_TAGS: bool = false;
    /// Whether tag writing is enabled.
    pub const DEFAULT_ENABLE_TAG_SAVING: bool = true;
    /// Whether release relationships are processed.
    pub const DEFAULT_RELEASE_ARS: bool = true;
    /// How many cover-art slots are written.
    pub const DEFAULT_COVER_ART_SLOT_COUNT: usize = 16;
}

#[cfg(feature = "media-tagger")]
use defaults::{
    DEFAULT_ACOUSTID_ENDPOINT, DEFAULT_CA_PROVIDERS, DEFAULT_CAA_APPROVED_ONLY,
    DEFAULT_CAA_ENDPOINT, DEFAULT_CAA_IMAGE_SIZE, DEFAULT_CAA_IMAGE_TYPES,
    DEFAULT_CACHE_EXPIRY_SECONDS, DEFAULT_CLEAR_EXISTING_TAGS, DEFAULT_COVER_ART_SLOT_COUNT,
    DEFAULT_EMBED_ONLY_ONE_FRONT_IMAGE, DEFAULT_ENABLE_TAG_SAVING, DEFAULT_MUSICBRAINZ_ENDPOINT,
    DEFAULT_PRESERVE_IMAGES, DEFAULT_RELEASE_ARS, DEFAULT_SAVE_IMAGES_TO_TAGS,
};

/// `mediapm` orchestration CLI.
#[derive(Debug, Parser)]
#[command(name = "mediapm", author, version, about = "mediapm orchestration CLI")]
pub struct Cli {
    /// Workspace root that hosts `mediapm.ncl` and `.mediapm/` runtime state.
    #[arg(long, default_value = ".", env = "MEDIAPM_ROOT")]
    pub root: PathBuf,
    /// Overrides `runtime.mediapm_dir` for this command invocation.
    #[arg(long, env = "MEDIAPM_DIR")]
    pub mediapm_dir: Option<PathBuf>,
    /// Override for `runtime.hierarchy_root_dir`.
    #[arg(long, env = "MEDIAPM_HIERARCHY_ROOT_DIR")]
    pub hierarchy_root_dir: Option<PathBuf>,
    /// Override for `runtime.conductor_config`.
    #[arg(long, env = "MEDIAPM_CONDUCTOR_CONFIG")]
    pub conductor_config: Option<PathBuf>,
    /// Override for `runtime.conductor_generated_config`.
    #[arg(long, env = "MEDIAPM_CONDUCTOR_GENERATED_CONFIG")]
    pub conductor_generated_config: Option<PathBuf>,
    /// Override for `runtime.conductor_state_config`.
    #[arg(long, env = "MEDIAPM_CONDUCTOR_STATE_CONFIG")]
    pub conductor_state_config: Option<PathBuf>,
    /// Override for `runtime.env_file`.
    #[arg(long, env = "MEDIAPM_ENV_FILE")]
    pub env_file: Option<PathBuf>,
    /// Override for `runtime.env_generated_file`.
    #[arg(long, env = "MEDIAPM_ENV_GENERATED_FILE")]
    pub env_generated_file: Option<PathBuf>,
    /// Override for `runtime.mediapm_schema_dir`.
    #[arg(long, env = "MEDIAPM_MEDIAPM_SCHEMA_DIR")]
    pub mediapm_schema_dir: Option<PathBuf>,
    /// Override for `runtime.media_state_config`.
    #[arg(long, env = "MEDIAPM_MEDIA_STATE_CONFIG")]
    pub media_state_config: Option<PathBuf>,
    /// Enables `CorruptObject` retry for impure workflow steps.
    #[arg(long, env = "MEDIAPM_RETRY_IMPURE")]
    pub retry_impure: bool,
    /// Top-level command selector.
    #[command(subcommand)]
    pub command: Command,
}

/// Top-level `mediapm` commands.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Reconciles desired state and materializes the media library.
    ///
    /// Default policy checks remote updates for tag-only selectors unless
    /// `--no-check-tag-updates` is provided.
    Sync(SyncArgs),
    /// Tool lifecycle commands.
    #[command(name = "tool", visible_alias = "tools")]
    Tool {
        /// Tool subcommand selector.
        #[command(subcommand)]
        command: ToolCommand,
    },
    /// Media-source registry commands.
    Media {
        /// Media subcommand selector.
        #[command(subcommand)]
        command: MediaCommand,
    },
    /// Hierarchy registry commands.
    Hierarchy {
        /// Hierarchy subcommand selector.
        #[command(subcommand)]
        command: HierarchyCommand,
    },
    /// User-scoped global `mediapm` directory management commands.
    Global {
        /// Global subcommand selector.
        #[command(subcommand)]
        command: GlobalCommand,
    },
    /// Builtin command implementations exposed by the `mediapm` executable.
    #[command(name = "builtin", visible_alias = "builtins")]
    Builtin {
        /// Builtin subcommand selector.
        #[command(subcommand)]
        command: Box<BuiltinCommand>,
    },
    /// Passthrough to the `mediapm-cas` CLI.
    Cas(PassthroughArgs),
    /// Passthrough to the conductor CLI.
    Conductor(PassthroughArgs),
    /// Generates shell completion scripts for the `mediapm` CLI.
    Completions {
        /// Target shell for completion script generation.
        shell: Shell,
    },
}

/// Tool lifecycle commands.
#[derive(Debug, Subcommand)]
pub enum ToolCommand {
    /// Adds one tool requirement entry to `mediapm.ncl` by logical name.
    ///
    /// The tool must appear in the built-in downloader catalog. If a
    /// requirement for this name already exists, the command is a no-op.
    /// After adding, run `tool sync` to download and register the tool.
    Add {
        /// Logical tool name (e.g. `yt-dlp`, `ffmpeg`, `deno`, `rsgain`,
        /// `media-tagger`, `sd`).
        name: String,
    },
    /// Reconciles desired tool requirements only (no workflow/materialization
    /// run).
    ///
    /// Default policy checks remote updates for tag-only selectors unless
    /// `--no-check-tag-updates` is provided.
    Sync(SyncArgs),
    /// Lists registered tools and binary status.
    List,
    /// Removes one tool requirement entry from `mediapm.ncl`.
    ///
    /// This updates desired tool state only.
    Remove {
        /// Logical tool name.
        name: String,
    },
    /// Removes one installed tool binary while keeping metadata.
    ///
    /// Pass `--metadata` to also remove tool metadata from the machine
    /// document and the tool registry entirely.
    Prune {
        /// Immutable tool id.
        id: String,
        /// Also remove tool metadata from the machine document and registry.
        #[arg(long)]
        metadata: bool,
    },
    /// Runs one managed tool binary directly.
    Run {
        /// Immutable tool id or logical tool name.
        tool: String,
        /// Trailing arguments passed to the managed tool executable.
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// Refreshes machine-managed runtime path defaults and dotenv scaffolding.
    RefreshRuntime,
}

/// Media-source registry commands.
#[derive(Debug, Subcommand)]
pub enum MediaCommand {
    /// Adds one media source registered in `mediapm.ncl`.
    Add(MediaAddArgs),
    /// Removes one media source from `mediapm.ncl` by id.
    Remove {
        /// Media source id to remove.
        media_id: String,
    },
    /// Invalidates cached tool output for a specific media step.
    ///
    /// After invalidation, run `mediapm sync` to regenerate outputs.
    Invalidate(MediaInvalidateArgs),
}

/// Arguments for `media add <preset> <source>`.
#[derive(Debug, Args)]
pub struct MediaAddArgs {
    /// Preset defining which toolchain to use.
    #[arg(value_enum)]
    pub preset: MediaAddPreset,
    /// Media source URL or local file path.
    pub source: String,
    /// Optional human-readable title for the media source.
    #[arg(long)]
    pub title: Option<String>,
    /// Optional artist name override.
    #[arg(long)]
    pub artist: Option<String>,
    /// Optional description for the media source.
    #[arg(long)]
    pub description: Option<String>,
    /// Optional recording MBID for music-related sources.
    #[arg(long)]
    pub recording_mbid: Option<String>,
    /// Optional release MBID for music-related sources.
    #[arg(long)]
    pub release_mbid: Option<String>,
    /// Overwrite existing media source if media id already exists.
    #[arg(long)]
    pub overwrite: bool,
    /// Insert position for the new entry.
    #[arg(long, default_value = "sorted")]
    pub insert_position: AddInsertPositionArg,
    /// Optional album name override.
    #[arg(long)]
    pub album: Option<String>,
    /// ffprobe command path (local preset only).
    #[arg(long)]
    pub ffprobe_command: Option<String>,
}

/// Preset for media add operations.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum MediaAddPreset {
    /// Use yt-dlp to download and process media.
    YtDlp,
    /// Add a local file as a media source.
    Local,
}

/// Insertion position for media/hierarchy add operations.
#[derive(Debug, Clone, Copy, Default, ValueEnum)]
pub enum AddInsertPositionArg {
    /// Insert in sorted (alphabetical) position.
    #[default]
    Sorted,
    /// Insert at the beginning of the list.
    Beginning,
    /// Insert at the end of the list.
    End,
}

/// Arguments for `media invalidate <media-id> <step-index>`.
///
/// The command sets one marker: the recorded instances of the named step miss
/// the probe, so the next sync re-executes them. `--undo` clears the same
/// marker. `--invalidate-calls` and `--regenerate` stay accepted because they
/// named the two halves of the command that used to differ — clearing recorded
/// progress and queuing the run — and neither half exists any more: there is no
/// progress to clear, and the marker is not a queue.
#[derive(Debug, Args)]
pub struct MediaInvalidateArgs {
    /// Media source id that owns the step.
    pub media_id: String,
    /// Zero-based step index to mark (or unmark with `--undo`).
    ///
    /// The index addresses this media's own workflow, so marking step 0 here
    /// never marks another media's step 0, even when both workflows number
    /// their steps the same way.
    pub step_index: usize,
    /// Clear the marker instead of setting it.
    #[arg(long, conflicts_with_all = ["invalidate_calls", "regenerate"])]
    pub undo: bool,
    /// Accepted for compatibility; marking is what this command does.
    #[arg(long)]
    pub invalidate_calls: bool,
    /// Accepted for compatibility; marking is what this command does.
    #[arg(long)]
    pub regenerate: bool,
}

/// Hierarchy registry commands.
#[derive(Debug, Subcommand)]
pub enum HierarchyCommand {
    /// Adds one predefined hierarchy preset entry.
    Add(HierarchyAddArgs),
    /// Removes one hierarchy node by media id.
    Remove {
        /// Hierarchy preset to remove from.
        #[arg(long, value_enum)]
        preset: Option<HierarchyPresetArg>,
        /// Root folder for hierarchy nodes.
        #[arg(long, default_value = "media/")]
        root_folder: String,
        /// Media id of the hierarchy node to remove.
        media_id: String,
    },
}

/// Arguments for `hierarchy add --preset <preset>`.
#[derive(Debug, Args)]
pub struct HierarchyAddArgs {
    /// Predefined hierarchy preset.
    #[arg(value_enum)]
    pub preset: HierarchyPresetArg,
    /// Root folder name for hierarchy nodes.
    #[arg(long, default_value = "media/")]
    pub root_folder: String,
    /// Overwrite existing hierarchy node.
    #[arg(long)]
    pub overwrite: bool,
    /// Insert position for the new entry.
    #[arg(long, default_value = "sorted")]
    pub insert_position: AddInsertPositionArg,
    /// Optional media id to associate with the hierarchy node.
    pub media_id: Option<String>,
}

/// Hierarchy preset selector with user-friendly names.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum HierarchyPresetArg {
    /// Local-file library.
    Local,
    /// yt-dlp channel-based library.
    #[value(name = "yt-dlp")]
    YtDlp,
}

/// Global directory management commands.
#[derive(Debug, Subcommand)]
pub enum GlobalCommand {
    /// Print global directory paths.
    Path,
    /// Creates the global directory layout if absent.
    Init,
    /// Tool-cache management commands.
    ToolCache {
        /// Tool-cache subcommand selector.
        #[command(subcommand)]
        command: GlobalToolCacheCommand,
    },
}

/// Global tool-cache management commands.
#[derive(Debug, Subcommand)]
pub enum GlobalToolCacheCommand {
    /// Print tool-cache status information.
    Status,
    /// Prune expired entries from the global tool cache.
    Prune,
    /// Clear the global tool cache entirely.
    Clear,
}

/// Builtin command implementations exposed by the `mediapm` executable.
#[derive(Debug, Subcommand)]
pub enum BuiltinCommand {
    /// Run internal media-tagger (Picard-based tagging pipeline).
    #[cfg(feature = "media-tagger")]
    MediaTagger(InternalMediaTaggerArgs),
}

/// Arguments for `builtin media-tagger`.
#[cfg(feature = "media-tagger")]
#[derive(Debug, Args)]
#[expect(clippy::struct_excessive_bools, reason = "media-tagger has many option toggles")]
pub struct InternalMediaTaggerArgs {
    /// Input media file path.
    #[arg(short, long)]
    pub input: String,
    /// Output file path.
    #[arg(short, long)]
    pub output: String,
    /// `AcoustID` API key.
    #[arg(long)]
    pub acoustid_api_key: Option<String>,
    /// `AcoustID` endpoint URL.
    #[arg(long, default_value = DEFAULT_ACOUSTID_ENDPOINT)]
    pub acoustid_endpoint: String,
    /// `MusicBrainz` endpoint URL.
    #[arg(long, default_value = DEFAULT_MUSICBRAINZ_ENDPOINT)]
    pub musicbrainz_endpoint: String,
    /// `Cover Art Archive` endpoint URL.
    #[arg(long, default_value = DEFAULT_CAA_ENDPOINT)]
    pub caa_endpoint: String,
    /// Cache directory path.
    #[arg(long)]
    pub cache_dir: Option<String>,
    /// Cache expiry in seconds.
    #[arg(long, default_value_t = DEFAULT_CACHE_EXPIRY_SECONDS as u64)]
    pub cache_expiry_seconds: u64,
    /// Enable strict identification.
    #[arg(long, action = clap::ArgAction::Set, default_value_t = true)]
    pub strict_identification: bool,
    /// Write all tags.
    #[arg(long, action = clap::ArgAction::Set, default_value_t = true)]
    pub write_all_tags: bool,
    /// Write all images.
    #[arg(long, action = clap::ArgAction::Set, default_value_t = true)]
    pub write_all_images: bool,
    /// Save images to tags.
    #[arg(long, action = clap::ArgAction::Set, default_value_t = DEFAULT_SAVE_IMAGES_TO_TAGS)]
    pub save_images_to_tags: bool,
    /// Embed only one front image.
    #[arg(long, action = clap::ArgAction::Set, default_value_t = DEFAULT_EMBED_ONLY_ONE_FRONT_IMAGE)]
    pub embed_only_one_front_image: bool,
    /// Cover art providers.
    #[arg(long, default_value_t = DEFAULT_CA_PROVIDERS.to_string())]
    pub ca_providers: String,
    /// CAA image types filter.
    #[arg(long, default_value_t = DEFAULT_CAA_IMAGE_TYPES.to_string())]
    pub caa_image_types: String,
    /// CAA image size.
    #[arg(long, default_value_t = DEFAULT_CAA_IMAGE_SIZE.to_string())]
    pub caa_image_size: String,
    /// Restrict CAA entries to approved-only.
    #[arg(long, action = clap::ArgAction::Set, default_value_t = DEFAULT_CAA_APPROVED_ONLY)]
    pub caa_approved_only: bool,
    /// Preserve existing embedded images.
    #[arg(long, action = clap::ArgAction::Set, default_value_t = DEFAULT_PRESERVE_IMAGES)]
    pub preserve_images: bool,
    /// Clear existing textual tags.
    #[arg(long, action = clap::ArgAction::Set, default_value_t = DEFAULT_CLEAR_EXISTING_TAGS)]
    pub clear_existing_tags: bool,
    /// Enable tag writing.
    #[arg(long, action = clap::ArgAction::Set, default_value_t = DEFAULT_ENABLE_TAG_SAVING)]
    pub enable_tag_saving: bool,
    /// Release relationship processing.
    #[arg(long, action = clap::ArgAction::Set, default_value_t = DEFAULT_RELEASE_ARS)]
    pub release_ars: bool,
    /// Cover art slot count.
    #[arg(long, default_value_t = DEFAULT_COVER_ART_SLOT_COUNT)]
    pub cover_art_slot_count: usize,
    /// Recording MBID override.
    #[arg(long)]
    pub recording_mbid: Option<String>,
    /// Release MBID override.
    #[arg(long)]
    pub release_mbid: Option<String>,
}

/// Generic passthrough argument holder.
#[derive(Debug, Args)]
pub struct PassthroughArgs {
    /// Trailing passthrough arguments.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub args: Vec<String>,
}

/// Optional recheck policy override flags for sync commands.
#[derive(Debug, Args, Clone, Copy, Default)]
pub struct RecheckPolicyArgs {
    /// Force re-resolve all version selectors from remote sources.
    #[arg(long, conflicts_with = "no_force_re_resolve")]
    pub force_re_resolve: bool,
    /// Use cached metadata; do not force re-resolve.
    #[arg(long)]
    pub no_force_re_resolve: bool,
}

/// Optional verify-materialization override flags for sync commands.
#[derive(Debug, Args, Clone, Copy, Default)]
pub struct VerifyMaterializationArgs {
    /// Enables materialization verification.
    #[arg(long, conflicts_with = "no_verify_materialization")]
    pub verify_materialization: bool,
    /// Disables materialization verification.
    #[arg(long)]
    pub no_verify_materialization: bool,
}

impl VerifyMaterializationArgs {
    /// Resolves effective verify-materialization policy using command-specific
    /// default.
    #[must_use]
    pub fn resolve(self, default_value: bool) -> bool {
        if self.verify_materialization {
            true
        } else if self.no_verify_materialization {
            false
        } else {
            default_value
        }
    }
}

/// Arguments for top-level `mediapm sync`.
#[derive(Debug, Args, Clone, Copy, Default)]
pub struct SyncArgs {
    /// Optional override for tool recheck policy.
    #[command(flatten)]
    pub recheck_policy: RecheckPolicyArgs,
    /// Optional override for materialization verification.
    #[command(flatten)]
    pub verify_materialization: VerifyMaterializationArgs,
    /// Suppress all progress bar output (for headless/scripted use).
    #[arg(long, env = "MEDIAPM_NO_PROGRESS")]
    pub no_progress: bool,
    /// Re-execute every workflow step this run, even when its instance is
    /// cached.
    ///
    /// Instance keys are content-derived, so elapsed time never expires a
    /// step; this flag is the explicit way to re-run one whose external
    /// circumstances changed. It applies to every workflow this run executes
    /// and leaves nothing behind.
    ///
    /// A step marked by `mediapm media invalidate` re-executes on a plain sync
    /// instead: the marker sits on the recorded instance, so the probe misses
    /// without any run-wide flag. The scope here is the whole run, which is
    /// wider than that command's one step.
    #[arg(long)]
    pub force_rerun: bool,
}

/// Builds the `mediapm` command tree.
///
/// This is the definition a build script renders completion scripts from, and
/// the binary parses with the same types, so the two cannot describe different
/// commands. The name is pinned on [`Cli`] because clap would otherwise derive
/// it from this crate's package name.
///
/// The return type is spelled `clap::Command` rather than imported, because
/// this module also defines a [`Command`] of its own.
#[must_use]
pub fn command() -> clap::Command {
    Cli::command()
}
