//! Builtin command implementations exposed by the `mediapm` executable.
//!
//! These handlers are invoked through `mediapm builtin ...`.

#[cfg(feature = "media-tagger")]
pub mod media_tagger;
